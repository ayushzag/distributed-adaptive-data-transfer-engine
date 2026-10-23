import asyncio
import hashlib

import pytest

from transfer_engine.protocol import read_message, sha256_file, sha256_range, write_message
from transfer_engine.sender import (
    TransferError,
    _send_range,
    build_parser,
    make_transfer_id,
    probe_rtt,
    transfer_file,
)


def test_transfer_id_is_stable_for_same_inputs(tmp_path):
    path = tmp_path / "a.bin"
    path.write_bytes(b"same")
    stat = path.stat()
    one = make_transfer_id(path, stat.st_size, stat.st_mtime_ns, sha256_file(path), path.name)
    two = make_transfer_id(path, stat.st_size, stat.st_mtime_ns, sha256_file(path), path.name)
    assert one == two
    assert len(one) == 32


@pytest.mark.parametrize("samples", [0, -1])
def test_probe_rejects_nonpositive_samples(samples):
    with pytest.raises(ValueError):
        asyncio.run(probe_rtt("127.0.0.1", 5050, samples=samples))


def test_probe_reports_unreachable_receiver():
    async def scenario():
        # Port 1 is normally not listening in isolated test environments.
        with pytest.raises(TransferError, match="could not probe receiver"):
            await probe_rtt("127.0.0.1", 1, samples=1, timeout=0.2)
    asyncio.run(scenario())


def test_sender_validation_errors(tmp_path):
    missing = tmp_path / "missing.bin"
    with pytest.raises(FileNotFoundError):
        asyncio.run(transfer_file(missing, "127.0.0.1"))

    source = tmp_path / "x.bin"
    source.write_bytes(b"x")
    for kwargs in [
        {"requested_streams": 0},
        {"retries": -1},
        {"loss_pct": -1},
        {"loss_pct": 101},
        {"filename": "../escape.bin"},
        {"transfer_id": "short"},
    ]:
        with pytest.raises(ValueError):
            asyncio.run(transfer_file(source, "127.0.0.1", **kwargs))


def test_sender_detects_source_changed_during_checksum(tmp_path, monkeypatch):
    import transfer_engine.sender as sender_module

    source = tmp_path / "mutating.bin"
    source.write_bytes(b"initial")
    original_hash = sender_module.sha256_file

    def mutate_after_hash(path):
        result = original_hash(path)
        source.write_bytes(b"changed content")
        return result

    monkeypatch.setattr(sender_module, "sha256_file", mutate_after_hash)
    with pytest.raises(TransferError, match="source file changed"):
        asyncio.run(transfer_file(source, "127.0.0.1"))


def test_sender_parser_parses_options():
    args = build_parser().parse_args(
        ["input.bin", "127.0.0.1", "--port", "5051", "--streams", "3", "--loss-pct", "1.5"]
    )
    assert args.source == "input.bin"
    assert args.port == 5051
    assert args.streams == 3
    assert args.loss_pct == 1.5


def test_range_sender_retries_after_peer_disconnect(tmp_path):
    async def scenario():
        source = tmp_path / "retry.bin"
        payload = b"retry-this-range" * 100
        source.write_bytes(payload)
        full_hash = hashlib.sha256(payload).hexdigest()
        range_hash = sha256_range(source, 0, len(payload))
        attempts = 0

        async def fake_receiver(reader, writer):
            nonlocal attempts
            try:
                header = await read_message(reader)
                attempts += 1
                if attempts == 1:
                    writer.close()
                    await writer.wait_closed()
                    return
                await write_message(writer, {"status": "ready"})
                await reader.readexactly(header["end"] - header["start"])
                await write_message(writer, {"status": "complete"})
            finally:
                if not writer.is_closing():
                    writer.close()
                    await writer.wait_closed()

        server = await asyncio.start_server(fake_receiver, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        try:
            result = await _send_range(
                source=source,
                host="127.0.0.1",
                port=port,
                transfer_id="range-retry-test-01",
                filename=source.name,
                size=len(payload),
                checksum=full_hash,
                start=0,
                end=len(payload),
                range_checksum=range_hash,
                chunk_size=4096,
                retries=1,
                timeout=3,
            )
            assert result.attempts == 2
            assert result.bytes_sent == len(payload)
            assert attempts == 2
        finally:
            server.close()
            await server.wait_closed()

    asyncio.run(scenario())


def test_range_sender_fails_after_retry_budget(tmp_path):
    async def scenario():
        async def always_close(reader, writer):
            writer.close()
            await writer.wait_closed()

        server = await asyncio.start_server(always_close, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        source = tmp_path / "x.bin"
        source.write_bytes(b"x")
        try:
            with pytest.raises(TransferError, match="failed after 1 attempt"):
                await _send_range(
                    source=source,
                    host="127.0.0.1",
                    port=port,
                    transfer_id="range-failure-test-01",
                    filename=source.name,
                    size=1,
                    checksum=sha256_file(source),
                    start=0,
                    end=1,
                    range_checksum=sha256_range(source, 0, 1),
                    chunk_size=4096,
                    retries=0,
                    timeout=2,
                )
        finally:
            server.close()
            await server.wait_closed()

    asyncio.run(scenario())


def test_sender_can_resume_a_fully_completed_transfer(tmp_path):
    async def scenario():
        from transfer_engine.receiver import TransferReceiver

        source = tmp_path / "done.bin"
        source.write_bytes(b"done" * 256)
        receiver = TransferReceiver(tmp_path / "received")
        _, port = await receiver.start("127.0.0.1", 0)
        try:
            first = await transfer_file(source, "127.0.0.1", port, requested_streams=2, timeout=3)
            second = await transfer_file(
                source, "127.0.0.1", port, requested_streams=2, transfer_id=first.transfer_id, timeout=3
            )
            assert second.bytes_sent_this_run == 0
            assert second.resumed_ranges >= 1
            assert sha256_file(tmp_path / "received" / source.name) == sha256_file(source)
        finally:
            await receiver.close()

    asyncio.run(scenario())

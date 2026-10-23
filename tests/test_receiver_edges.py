import asyncio
import hashlib
import json
import os
import threading

import pytest

from transfer_engine.protocol import ProtocolError, read_message, sha256_file, write_message
from transfer_engine.receiver import TransferReceiver, TransferStore, _write_at
from transfer_engine.sender import TransferError, finalize_transfer, transfer_file


def _header(transfer_id="receiver-test-001", filename="x.bin", data=b"abc"):
    return {
        "type": "range",
        "transfer_id": transfer_id,
        "filename": filename,
        "size": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
        "start": 0,
        "end": len(data),
        "range_sha256": hashlib.sha256(data).hexdigest(),
    }


def test_transfer_store_validates_header_fields(tmp_path):
    async def scenario():
        store = TransferStore(tmp_path)
        base = _header()
        invalid_headers = []
        for changes in [
            {"transfer_id": "short"},
            {"size": True},
            {"size": -1},
            {"sha256": "xyz"},
            {"filename": "../x.bin"},
        ]:
            invalid = dict(base)
            invalid.update(changes)
            invalid_headers.append(invalid)
        for header in invalid_headers:
            with pytest.raises(ProtocolError):
                await store.get_or_create(header)
    asyncio.run(scenario())


def test_transfer_id_cannot_be_reused_for_different_contents(tmp_path):
    async def scenario():
        store = TransferStore(tmp_path)
        await store.get_or_create(_header())
        other = _header(data=b"xyz")
        with pytest.raises(ProtocolError, match="different file contents"):
            await store.get_or_create(other)
    asyncio.run(scenario())


def test_bad_persisted_metadata_is_rejected(tmp_path):
    async def scenario():
        store = TransferStore(tmp_path)
        metadata = store.state_dir / "broken-transfer-01.json"
        metadata.write_text("not-json", encoding="utf-8")
        header = _header(transfer_id="broken-transfer-01")
        with pytest.raises(ProtocolError, match="unreadable"):
            await store.get_or_create(header)
    asyncio.run(scenario())


def test_missing_partial_file_resets_saved_completed_ranges(tmp_path):
    async def scenario():
        store = TransferStore(tmp_path)
        state = await store.get_or_create(_header())
        state.completed["0:3"] = hashlib.sha256(b"abc").hexdigest()
        await store.save(state)
        state.part_path.unlink()
        store._states.clear()
        loaded = await store.get_or_create(_header())
        assert loaded.completed == {}
        assert loaded.part_path.exists()
        assert loaded.part_path.stat().st_size == 3
    asyncio.run(scenario())


def test_partial_file_with_wrong_size_is_reset(tmp_path):
    async def scenario():
        store = TransferStore(tmp_path)
        state = await store.get_or_create(_header())
        state.part_path.write_bytes(b"larger-than-three")
        state.completed["0:3"] = "a" * 64
        await store.save(state)
        store._states.clear()
        loaded = await store.get_or_create(_header())
        assert loaded.completed == {}
        assert loaded.part_path.stat().st_size == 3
    asyncio.run(scenario())


def test_get_existing_returns_none_if_transfer_not_started(tmp_path):
    async def scenario():
        store = TransferStore(tmp_path)
        assert await store.get_existing(_header()) is None
    asyncio.run(scenario())


def test_receiver_rejects_too_small_io_chunk(tmp_path):
    with pytest.raises(ValueError):
        TransferReceiver(tmp_path, io_chunk_size=100)


def test_write_at_uses_pwrite_and_fallback(tmp_path, monkeypatch):
    path = tmp_path / "write.bin"
    path.write_bytes(b"\0" * 8)
    fd = os.open(path, os.O_WRONLY)
    try:
        _write_at(fd, path, 2, b"AB", threading.Lock())
    finally:
        os.close(fd)
    assert path.read_bytes() == b"\0\0AB\0\0\0\0"

    fallback_path = tmp_path / "fallback.bin"
    fallback_path.write_bytes(b"\0" * 8)
    monkeypatch.delattr(os, "pwrite", raising=False)
    _write_at(-1, fallback_path, 3, b"XY", threading.Lock())
    assert fallback_path.read_bytes() == b"\0\0\0XY\0\0\0"


def test_write_at_raises_when_pwrite_fails(tmp_path, monkeypatch):
    path = tmp_path / "failed.bin"
    path.write_bytes(b"\0" * 2)
    fd = os.open(path, os.O_WRONLY)
    monkeypatch.setattr(os, "pwrite", lambda *args: 0)
    try:
        with pytest.raises(OSError):
            _write_at(fd, path, 0, b"x", threading.Lock())
    finally:
        os.close(fd)


def test_unknown_protocol_message_gets_error(tmp_path):
    async def scenario():
        receiver = TransferReceiver(tmp_path)
        _, port = await receiver.start("127.0.0.1", 0)
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            await write_message(writer, {"type": "unknown"})
            response = await read_message(reader)
            assert response["status"] == "error"
            assert response["error"] == "unknown message type"
            writer.close()
            await writer.wait_closed()
        finally:
            await receiver.close()
    asyncio.run(scenario())


@pytest.mark.parametrize(
    "changes",
    [
        {"start": -1},
        {"end": 4},
        {"start": True},
        {"end": "3"},
        {"range_sha256": "not-a-hash"},
    ],
)
def test_invalid_range_header_is_rejected(tmp_path, changes):
    async def scenario():
        data = b"abc"
        header = _header(data=data)
        header.update(changes)
        receiver = TransferReceiver(tmp_path)
        _, port = await receiver.start("127.0.0.1", 0)
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            await write_message(writer, header)
            response = await read_message(reader)
            assert response["status"] == "error"
            writer.close()
            await writer.wait_closed()
        finally:
            await receiver.close()
    asyncio.run(scenario())


def test_range_hash_mismatch_is_rejected(tmp_path):
    async def scenario():
        data = b"abc"
        header = _header(data=data)
        header["range_sha256"] = hashlib.sha256(b"not abc").hexdigest()
        receiver = TransferReceiver(tmp_path)
        _, port = await receiver.start("127.0.0.1", 0)
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            await write_message(writer, header)
            ready = await read_message(reader)
            assert ready["status"] == "ready"
            writer.write(data)
            await writer.drain()
            response = await read_message(reader)
            assert response["status"] == "error"
            assert "SHA-256 mismatch" in response["error"]
            writer.close()
            await writer.wait_closed()
        finally:
            await receiver.close()
    asyncio.run(scenario())


def test_finalize_without_any_ranges_is_rejected(tmp_path):
    async def scenario():
        source = tmp_path / "src.bin"
        source.write_bytes(b"abc")
        receiver = TransferReceiver(tmp_path / "output")
        _, port = await receiver.start("127.0.0.1", 0)
        try:
            with pytest.raises(TransferError, match="no saved ranges"):
                await finalize_transfer(
                    "127.0.0.1", port, "missing-transfer-01", "src.bin", 3, sha256_file(source), timeout=3
                )
        finally:
            await receiver.close()
    asyncio.run(scenario())


def test_final_checksum_mismatch_clears_range_checkpoints(tmp_path):
    async def scenario():
        source_data = b"correct-data"
        checksum = hashlib.sha256(source_data).hexdigest()
        receiver = TransferReceiver(tmp_path / "output")
        state = await receiver.store.get_or_create(
            {"transfer_id": "checksum-test-01", "filename": "data.bin", "size": len(source_data), "sha256": checksum}
        )
        state.part_path.write_bytes(b"incorrect!!")
        state.completed["0:12"] = "b" * 64
        await receiver.store.save(state)
        _, port = await receiver.start("127.0.0.1", 0)
        try:
            with pytest.raises(TransferError, match="whole-file SHA-256 mismatch"):
                await finalize_transfer("127.0.0.1", port, state.transfer_id, state.filename, state.size, checksum, timeout=3)
            assert state.completed == {}
        finally:
            await receiver.close()
    asyncio.run(scenario())


def test_preexisting_different_destination_is_not_overwritten_by_default(tmp_path):
    async def scenario():
        output = tmp_path / "received"
        output.mkdir()
        (output / "payload.bin").write_bytes(b"older data")
        source = tmp_path / "payload.bin"
        source.write_bytes(b"new contents")
        receiver = TransferReceiver(output)
        _, port = await receiver.start("127.0.0.1", 0)
        try:
            with pytest.raises(TransferError, match="destination exists"):
                await transfer_file(source, "127.0.0.1", port, retries=0, timeout=3)
            assert (output / "payload.bin").read_bytes() == b"older data"
        finally:
            await receiver.close()
    asyncio.run(scenario())


def test_receiver_can_overwrite_when_explicitly_enabled(tmp_path):
    async def scenario():
        output = tmp_path / "received"
        output.mkdir()
        (output / "payload.bin").write_bytes(b"older data")
        source = tmp_path / "payload.bin"
        source.write_bytes(b"new contents")
        receiver = TransferReceiver(output, allow_overwrite=True)
        _, port = await receiver.start("127.0.0.1", 0)
        try:
            await transfer_file(source, "127.0.0.1", port, retries=0, timeout=3)
            assert (output / "payload.bin").read_bytes() == b"new contents"
        finally:
            await receiver.close()
    asyncio.run(scenario())


def test_same_content_existing_destination_is_accepted(tmp_path):
    async def scenario():
        output = tmp_path / "received"
        source = tmp_path / "payload.bin"
        source.write_bytes(b"same contents")
        output.mkdir()
        (output / source.name).write_bytes(source.read_bytes())
        receiver = TransferReceiver(output)
        _, port = await receiver.start("127.0.0.1", 0)
        try:
            result = await transfer_file(
                source, "127.0.0.1", port, transfer_id="same-content-transfer-01", retries=0, timeout=3
            )
            assert result.completed_path == str(output / source.name)
            assert (output / source.name).read_bytes() == source.read_bytes()
        finally:
            await receiver.close()
    asyncio.run(scenario())


def test_receiver_close_without_start_is_safe(tmp_path):
    async def scenario():
        receiver = TransferReceiver(tmp_path)
        await receiver.close()
        assert receiver._server is None
    asyncio.run(scenario())


def test_store_reloads_finalized_transfer(tmp_path):
    async def scenario():
        output = tmp_path / "output"
        store = TransferStore(output)
        data = b"complete"
        header = {"transfer_id": "finalized-test-01", "filename": "x.bin", "size": len(data), "sha256": hashlib.sha256(data).hexdigest()}
        state = await store.get_or_create(header)
        state.part_path.write_bytes(data)
        final = output / "x.bin"
        os.replace(state.part_path, final)
        state.finalized = True
        state.final_path = str(final)
        await store.save(state)
        store._states.clear()
        reloaded = await store.get_or_create(header)
        assert reloaded.finalized is True
        assert reloaded.final_path == str(final)
    asyncio.run(scenario())

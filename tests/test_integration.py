import asyncio
import hashlib

from transfer_engine.protocol import sha256_file, sha256_range
from transfer_engine.receiver import TransferReceiver
from transfer_engine.sender import _send_range, transfer_file


def test_small_file_transfer_is_verified(tmp_path):
    async def scenario():
        output = tmp_path / "received"
        source = tmp_path / "source.bin"
        source.write_bytes((b"distributed-tcp-transfer-" * 7000) + b"done")
        receiver = TransferReceiver(output_dir=output, io_chunk_size=64 * 1024)
        _, port = await receiver.start("127.0.0.1", 0)
        try:
            result = await transfer_file(source, "127.0.0.1", port, requested_streams=4, retries=1, timeout=5)
            destination = output / source.name
            assert destination.exists()
            assert sha256_file(destination) == sha256_file(source)
            assert result.sha256 == sha256_file(source)
            assert result.active_streams >= 1
            assert result.rtt_ms >= 0
        finally:
            await receiver.close()

    asyncio.run(scenario())


def test_transfer_resumes_a_previously_verified_range(tmp_path):
    async def scenario():
        output = tmp_path / "received"
        source = tmp_path / "source.bin"
        source.write_bytes(bytes(range(256)) * 1024)
        receiver = TransferReceiver(output_dir=output, io_chunk_size=64 * 1024)
        _, port = await receiver.start("127.0.0.1", 0)
        transfer_id = "manual-resume-test-01"
        try:
            checksum = await asyncio.to_thread(sha256_file, source)
            size = source.stat().st_size
            split = size // 2
            range_checksum = await asyncio.to_thread(sha256_range, source, 0, split)
            first = await _send_range(
                source=source,
                host="127.0.0.1",
                port=port,
                transfer_id=transfer_id,
                filename=source.name,
                size=size,
                checksum=checksum,
                start=0,
                end=split,
                range_checksum=range_checksum,
                chunk_size=64 * 1024,
                retries=1,
                timeout=5,
            )
            assert first.bytes_sent == split
            # The full sender should skip the durable first half and send the second half.
            result = await transfer_file(
                source,
                "127.0.0.1",
                port,
                requested_streams=2,
                retries=1,
                transfer_id=transfer_id,
                timeout=5,
            )
            destination = output / source.name
            assert destination.exists()
            assert sha256_file(destination) == checksum
            assert result.bytes_sent_this_run == size - split
            assert result.resumed_ranges >= 1
        finally:
            await receiver.close()

    asyncio.run(scenario())


def test_empty_file_transfer(tmp_path):
    async def scenario():
        output = tmp_path / "received"
        source = tmp_path / "empty.bin"
        source.write_bytes(b"")
        receiver = TransferReceiver(output_dir=output)
        _, port = await receiver.start("127.0.0.1", 0)
        try:
            await transfer_file(source, "127.0.0.1", port, requested_streams=2, retries=0, timeout=5)
            destination = output / source.name
            assert destination.exists()
            assert destination.stat().st_size == 0
            assert sha256_file(destination) == hashlib.sha256(b"").hexdigest()
        finally:
            await receiver.close()

    asyncio.run(scenario())

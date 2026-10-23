"""Async TCP receiver with persistent range checkpoints and final SHA-256 validation."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .protocol import (
    DEFAULT_IO_CHUNK_SIZE,
    ProtocolError,
    read_message,
    safe_filename,
    sha256_file,
    write_message,
)

_TRANSFER_ID_RE = re.compile(r"^[A-Za-z0-9_-]{8,128}$")
_SHA256_RE = re.compile(r"^[a-fA-F0-9]{64}$")


@dataclass(slots=True)
class TransferState:
    transfer_id: str
    filename: str
    size: int
    sha256: str
    part_path: Path
    metadata_path: Path
    completed: dict[str, str] = field(default_factory=dict)
    finalized: bool = False
    final_path: str | None = None
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    disk_lock: threading.Lock = field(default_factory=threading.Lock)


class TransferStore:
    """Manage in-progress transfer files and atomically saved resume metadata."""

    def __init__(self, output_dir: str | Path, allow_overwrite: bool = False) -> None:
        self.output_dir = Path(output_dir).expanduser().resolve()
        self.state_dir = self.output_dir / ".transfer-engine"
        self.allow_overwrite = allow_overwrite
        self._states: dict[str, TransferState] = {}
        self._states_lock = asyncio.Lock()
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.state_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _validate_header(header: dict[str, Any]) -> tuple[str, str, int, str]:
        transfer_id = header.get("transfer_id")
        filename = safe_filename(header.get("filename"))
        size = header.get("size")
        checksum = header.get("sha256")
        if not isinstance(transfer_id, str) or not _TRANSFER_ID_RE.fullmatch(transfer_id):
            raise ProtocolError("transfer_id must be 8-128 letters, digits, underscores, or hyphens")
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            raise ProtocolError("size must be a non-negative integer")
        if not isinstance(checksum, str) or not _SHA256_RE.fullmatch(checksum):
            raise ProtocolError("sha256 must contain exactly 64 hexadecimal characters")
        return transfer_id, filename, size, checksum.lower()

    async def get_or_create(self, header: dict[str, Any]) -> TransferState:
        transfer_id, filename, size, checksum = self._validate_header(header)
        async with self._states_lock:
            state = self._states.get(transfer_id)
            if state is not None:
                self._assert_same_transfer(state, filename, size, checksum)
                return state

            metadata_path = self.state_dir / f"{transfer_id}.json"
            part_path = self.state_dir / f"{transfer_id}.part"
            completed: dict[str, str] = {}
            finalized = False
            final_path: str | None = None

            if metadata_path.exists():
                try:
                    saved = json.loads(metadata_path.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError) as exc:
                    raise ProtocolError("saved transfer metadata is unreadable") from exc
                if (saved.get("filename"), saved.get("size"), str(saved.get("sha256", "")).lower()) != (
                    filename,
                    size,
                    checksum,
                ):
                    raise ProtocolError("transfer_id already belongs to different file contents")
                completed = {str(k): str(v) for k, v in saved.get("completed", {}).items()}
                final_path = saved.get("final_path")
                finalized = bool(saved.get("finalized")) and bool(final_path) and Path(final_path).is_file()

            if not finalized:
                if not part_path.exists():
                    completed = {}
                    fd = os.open(part_path, os.O_CREAT | os.O_RDWR, 0o600)
                    try:
                        os.ftruncate(fd, size)
                    finally:
                        os.close(fd)
                else:
                    current_size = part_path.stat().st_size
                    if current_size != size:
                        completed = {}
                        fd = os.open(part_path, os.O_CREAT | os.O_RDWR, 0o600)
                        try:
                            os.ftruncate(fd, size)
                        finally:
                            os.close(fd)

            state = TransferState(
                transfer_id=transfer_id,
                filename=filename,
                size=size,
                sha256=checksum,
                part_path=part_path,
                metadata_path=metadata_path,
                completed=completed,
                finalized=finalized,
                final_path=final_path,
            )
            self._states[transfer_id] = state
            if not metadata_path.exists() or not finalized:
                await self.save(state)
            return state

    async def get_existing(self, header: dict[str, Any]) -> TransferState | None:
        transfer_id, filename, size, checksum = self._validate_header(header)
        async with self._states_lock:
            state = self._states.get(transfer_id)
        if state is None:
            metadata_path = self.state_dir / f"{transfer_id}.json"
            if not metadata_path.exists():
                return None
            state = await self.get_or_create(header)
        self._assert_same_transfer(state, filename, size, checksum)
        return state

    @staticmethod
    def _assert_same_transfer(state: TransferState, filename: str, size: int, checksum: str) -> None:
        if (state.filename, state.size, state.sha256) != (filename, size, checksum):
            raise ProtocolError("transfer_id already belongs to different file contents")

    async def save(self, state: TransferState) -> None:
        payload = {
            "transfer_id": state.transfer_id,
            "filename": state.filename,
            "size": state.size,
            "sha256": state.sha256,
            "completed": state.completed,
            "finalized": state.finalized,
            "final_path": state.final_path,
            "updated_at": time.time(),
        }
        temp_path = state.metadata_path.with_suffix(".json.tmp")
        await asyncio.to_thread(temp_path.write_text, json.dumps(payload, indent=2), encoding="utf-8")
        await asyncio.to_thread(os.replace, temp_path, state.metadata_path)


def _write_at(fd: int, path: Path, offset: int, data: bytes, disk_lock: threading.Lock) -> None:
    if hasattr(os, "pwrite"):
        written = 0
        while written < len(data):
            count = os.pwrite(fd, data[written:], offset + written)
            if count <= 0:
                raise OSError("failed to write received bytes")
            written += count
        return
    with disk_lock:  # pragma: no cover - fallback for platforms without pwrite
        with open(path, "r+b") as file_obj:
            file_obj.seek(offset)
            file_obj.write(data)
            file_obj.flush()


class TransferReceiver:
    def __init__(
        self,
        output_dir: str | Path = "received",
        io_chunk_size: int = DEFAULT_IO_CHUNK_SIZE,
        allow_overwrite: bool = False,
    ) -> None:
        if io_chunk_size < 4096:
            raise ValueError("io_chunk_size must be at least 4096 bytes")
        self.io_chunk_size = io_chunk_size
        self.store = TransferStore(output_dir, allow_overwrite=allow_overwrite)
        self._server: asyncio.AbstractServer | None = None

    async def start(self, host: str = "0.0.0.0", port: int = 5050) -> tuple[str, int]:
        self._server = await asyncio.start_server(self.handle_client, host, port, limit=64 * 1024)
        socket_info = self._server.sockets[0].getsockname()
        return str(socket_info[0]), int(socket_info[1])

    async def close(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    async def serve_forever(self, host: str = "0.0.0.0", port: int = 5050) -> None:
        await self.start(host, port)
        assert self._server is not None
        async with self._server:
            await self._server.serve_forever()

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            header = await read_message(reader)
            message_type = header.get("type")
            if message_type == "probe":
                await write_message(writer, {"status": "pong", "server_time_ns": time.monotonic_ns()})
                return
            if message_type == "range":
                await self._handle_range(header, reader, writer)
                return
            if message_type == "finalize":
                await self._handle_finalize(header, writer)
                return
            await write_message(writer, {"status": "error", "error": "unknown message type"})
        except (ProtocolError, OSError, ValueError, asyncio.IncompleteReadError) as exc:
            try:
                await write_message(writer, {"status": "error", "error": str(exc)[:300]})
            except (ConnectionError, OSError, RuntimeError):
                pass
        except Exception as exc:  # keep one malformed connection from killing the listener
            try:
                await write_message(writer, {"status": "error", "error": f"receiver internal error: {str(exc)[:200]}"})
            except (ConnectionError, OSError, RuntimeError):
                pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass

    async def _handle_range(
        self,
        header: dict[str, Any],
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        state = await self.store.get_or_create(header)
        start, end = header.get("start"), header.get("end")
        expected_range_hash = header.get("range_sha256")
        if isinstance(start, bool) or not isinstance(start, int) or isinstance(end, bool) or not isinstance(end, int):
            raise ProtocolError("start and end must be integers")
        if start < 0 or end < start or end > state.size or (start == end and state.size != 0):
            raise ProtocolError("invalid byte range")
        if not isinstance(expected_range_hash, str) or not _SHA256_RE.fullmatch(expected_range_hash):
            raise ProtocolError("range_sha256 must be a SHA-256 hex digest")
        expected_range_hash = expected_range_hash.lower()
        range_key = f"{start}:{end}"

        async with state.lock:
            if state.finalized:
                await write_message(writer, {"status": "already_complete", "sha256": state.sha256})
                return
            if state.completed.get(range_key) == expected_range_hash:
                await write_message(writer, {"status": "already_complete", "range": range_key})
                return

        await write_message(writer, {"status": "ready", "range": range_key})
        remaining = end - start
        offset = start
        digest = hashlib.sha256()
        fd = os.open(state.part_path, os.O_WRONLY)
        try:
            while remaining:
                block = await reader.read(min(self.io_chunk_size, remaining))
                if not block:
                    raise ProtocolError(f"connection ended with {remaining} range bytes still missing")
                digest.update(block)
                await asyncio.to_thread(_write_at, fd, state.part_path, offset, block, state.disk_lock)
                offset += len(block)
                remaining -= len(block)
        finally:
            os.close(fd)

        received_hash = digest.hexdigest()
        if received_hash != expected_range_hash:
            raise ProtocolError("range SHA-256 mismatch; sender should retry this range")

        async with state.lock:
            state.completed[range_key] = received_hash
            await self.store.save(state)
        await write_message(writer, {"status": "complete", "range": range_key, "range_sha256": received_hash})

    async def _handle_finalize(self, header: dict[str, Any], writer: asyncio.StreamWriter) -> None:
        state = await self.store.get_existing(header)
        if state is None:
            raise ProtocolError("transfer has no saved ranges; send the file ranges first")
        async with state.lock:
            if state.finalized and state.final_path and Path(state.final_path).is_file():
                await write_message(
                    writer,
                    {"status": "complete", "path": state.final_path, "sha256": state.sha256, "resumed": True},
                )
                return
            actual_hash = await asyncio.to_thread(sha256_file, state.part_path)
            if actual_hash != state.sha256:
                state.completed.clear()
                await self.store.save(state)
                await write_message(
                    writer,
                    {"status": "error", "error": "whole-file SHA-256 mismatch; retry the transfer ranges"},
                )
                return

            destination = self.store.output_dir / state.filename
            same_content = False
            if destination.exists() and destination.resolve() != state.part_path.resolve():
                try:
                    same_content = (await asyncio.to_thread(sha256_file, destination)) == state.sha256
                except OSError:
                    same_content = False
                if not same_content and not self.store.allow_overwrite:
                    await write_message(
                        writer,
                        {"status": "error", "error": "destination exists; choose another filename or enable overwrite"},
                    )
                    return
            if destination.exists() and same_content:
                await asyncio.to_thread(state.part_path.unlink, missing_ok=True)
            else:
                await asyncio.to_thread(os.replace, state.part_path, destination)
            state.finalized = True
            state.final_path = str(destination)
            await self.store.save(state)
            await write_message(
                writer,
                {"status": "complete", "path": str(destination), "sha256": actual_hash, "resumed": False},
            )


async def _serve(args: argparse.Namespace) -> None:
    receiver = TransferReceiver(
        output_dir=args.output_dir,
        io_chunk_size=args.io_chunk_size,
        allow_overwrite=args.allow_overwrite,
    )
    host, port = await receiver.start(args.host, args.port)
    print(f"TCP receiver listening on {host}:{port}; output directory: {receiver.store.output_dir}", flush=True)
    assert receiver._server is not None
    try:
        async with receiver._server:
            await receiver._server.serve_forever()
    finally:
        await receiver.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Adaptive TCP file-transfer receiver")
    parser.add_argument("--host", default="0.0.0.0", help="bind address (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=5050, help="TCP port (default: 5050)")
    parser.add_argument("--output-dir", default="received", help="directory for completed files")
    parser.add_argument("--io-chunk-size", type=int, default=DEFAULT_IO_CHUNK_SIZE, help="receiver read block size")
    parser.add_argument("--allow-overwrite", action="store_true", help="allow completed files to overwrite same-name files")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    try:
        asyncio.run(_serve(args))
    except KeyboardInterrupt:
        print("Receiver stopped.")


if __name__ == "__main__":
    main()

"""Small, newline-delimited JSON protocol helpers for transfer control frames."""

from __future__ import annotations

import asyncio
import hashlib
import json
from pathlib import Path
from typing import Any

MAX_HEADER_BYTES = 64 * 1024
DEFAULT_IO_CHUNK_SIZE = 1024 * 1024


class ProtocolError(ValueError):
    """Raised when a peer sends an invalid or incomplete protocol frame."""


def encode_message(message: dict[str, Any]) -> bytes:
    return json.dumps(message, separators=(",", ":"), ensure_ascii=True).encode("utf-8") + b"\n"


async def read_message(reader: Any) -> dict[str, Any]:
    try:
        raw = await reader.readline()
    except (ValueError, asyncio.LimitOverrunError) as exc:
        raise ProtocolError("control frame exceeded the configured limit") from exc
    if not raw:
        raise ProtocolError("peer closed before sending a control frame")
    if len(raw) > MAX_HEADER_BYTES:
        raise ProtocolError("control frame is too large")
    try:
        decoded = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProtocolError("control frame is not valid UTF-8 JSON") from exc
    if not isinstance(decoded, dict):
        raise ProtocolError("control frame must be a JSON object")
    return decoded


async def write_message(writer: Any, message: dict[str, Any]) -> None:
    writer.write(encode_message(message))
    await writer.drain()


def sha256_file(path: str | Path, chunk_size: int = DEFAULT_IO_CHUNK_SIZE) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as file_obj:
        while True:
            block = file_obj.read(chunk_size)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


def sha256_range(path: str | Path, start: int, end: int, chunk_size: int = DEFAULT_IO_CHUNK_SIZE) -> str:
    if start < 0 or end < start:
        raise ValueError("invalid byte range")
    digest = hashlib.sha256()
    remaining = end - start
    with open(path, "rb") as file_obj:
        file_obj.seek(start)
        while remaining:
            block = file_obj.read(min(chunk_size, remaining))
            if not block:
                raise OSError("unexpected end of source file while hashing range")
            digest.update(block)
            remaining -= len(block)
    return digest.hexdigest()


def split_ranges(file_size: int, count: int) -> list[tuple[int, int]]:
    """Split [0, file_size) into non-empty, balanced ranges (except empty files)."""
    if file_size < 0:
        raise ValueError("file_size cannot be negative")
    if count < 1:
        raise ValueError("count must be at least 1")
    if file_size == 0:
        return [(0, 0)]
    count = min(count, file_size)
    ranges: list[tuple[int, int]] = []
    for index in range(count):
        start = (file_size * index) // count
        end = (file_size * (index + 1)) // count
        ranges.append((start, end))
    return ranges


def safe_filename(filename: str) -> str:
    """Reject paths; the receiver only accepts a plain output filename."""
    if not isinstance(filename, str) or not filename or filename in {".", ".."}:
        raise ProtocolError("filename must be a non-empty basename")
    if Path(filename).name != filename or "/" in filename or "\\" in filename:
        raise ProtocolError("filename must not contain a directory path")
    if "\x00" in filename:
        raise ProtocolError("filename contains a NUL byte")
    return filename

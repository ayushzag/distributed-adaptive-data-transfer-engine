import asyncio
import hashlib
import json

import pytest

from transfer_engine.protocol import (
    ProtocolError,
    encode_message,
    read_message,
    safe_filename,
    sha256_file,
    sha256_range,
    split_ranges,
)


@pytest.mark.parametrize(
    ("size", "count", "expected"),
    [
        (0, 1, [(0, 0)]),
        (1, 1, [(0, 1)]),
        (2, 1, [(0, 2)]),
        (2, 2, [(0, 1), (1, 2)]),
        (3, 2, [(0, 1), (1, 3)]),
        (4, 2, [(0, 2), (2, 4)]),
        (5, 2, [(0, 2), (2, 5)]),
        (7, 3, [(0, 2), (2, 4), (4, 7)]),
        (8, 4, [(0, 2), (2, 4), (4, 6), (6, 8)]),
        (10, 3, [(0, 3), (3, 6), (6, 10)]),
        (11, 5, [(0, 2), (2, 4), (4, 6), (6, 8), (8, 11)]),
        (9, 20, [(i, i + 1) for i in range(9)]),
    ],
)
def test_split_ranges(size, count, expected):
    assert split_ranges(size, count) == expected


@pytest.mark.parametrize("size", [-1, -10])
def test_split_ranges_rejects_negative_size(size):
    with pytest.raises(ValueError):
        split_ranges(size, 2)


@pytest.mark.parametrize("count", [0, -1])
def test_split_ranges_rejects_invalid_count(count):
    with pytest.raises(ValueError):
        split_ranges(10, count)


@pytest.mark.parametrize("filename", ["file.bin", "report-2026.csv", "folder name.txt", "a_b-c.dat"])
def test_safe_filename_accepts_basenames(filename):
    assert safe_filename(filename) == filename


@pytest.mark.parametrize("filename", ["", ".", "..", "../secret", "/tmp/file", "dir/file", r"dir\\file", "bad\x00name"])
def test_safe_filename_rejects_paths_and_invalid_names(filename):
    with pytest.raises(ProtocolError):
        safe_filename(filename)


def test_encode_message_is_one_json_line():
    encoded = encode_message({"type": "probe", "text": "hello"})
    assert encoded.endswith(b"\n")
    assert json.loads(encoded) == {"type": "probe", "text": "hello"}


class FakeReader:
    def __init__(self, data: bytes):
        self.data = data

    async def readline(self):
        return self.data


@pytest.mark.parametrize("payload", [b'{"ok":true}\n', b'{"n":1}\n', b'{"list":[]}\n'])
def test_read_message_decodes_json_objects(payload):
    parsed = asyncio.run(read_message(FakeReader(payload)))
    assert isinstance(parsed, dict)


@pytest.mark.parametrize("payload", [b"", b"not-json\n", b"[]\n", b"\xff\n"])
def test_read_message_rejects_bad_payloads(payload):
    with pytest.raises(ProtocolError):
        asyncio.run(read_message(FakeReader(payload)))


def test_sha256_file_and_range_match_expected(tmp_path):
    target = tmp_path / "payload.bin"
    target.write_bytes(b"abcdef012345")
    assert sha256_file(target) == hashlib.sha256(b"abcdef012345").hexdigest()
    assert sha256_range(target, 2, 8) == hashlib.sha256(b"cdef01").hexdigest()
    assert sha256_range(target, 4, 4) == hashlib.sha256(b"").hexdigest()


def test_sha256_range_rejects_invalid_range(tmp_path):
    target = tmp_path / "x.bin"
    target.write_bytes(b"x")
    with pytest.raises(ValueError):
        sha256_range(target, 2, 1)


def test_sha256_range_rejects_short_file_read(tmp_path):
    target = tmp_path / "x.bin"
    target.write_bytes(b"x")
    with pytest.raises(OSError):
        sha256_range(target, 0, 2)


class RaisingReader:
    async def readline(self):
        raise ValueError("stream limit exceeded")


@pytest.mark.parametrize("reader", [RaisingReader()])
def test_read_message_wraps_stream_limit_error(reader):
    with pytest.raises(ProtocolError, match="configured limit"):
        asyncio.run(read_message(reader))


def test_read_message_rejects_oversized_line():
    from transfer_engine.protocol import MAX_HEADER_BYTES

    with pytest.raises(ProtocolError, match="too large"):
        asyncio.run(read_message(FakeReader(b"x" * (MAX_HEADER_BYTES + 1))))

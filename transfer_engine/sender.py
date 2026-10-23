"""Adaptive parallel TCP sender with retryable ranges and receiver-side resume."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import statistics
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from .adaptive import AdaptivePolicy, NetworkMetrics, TransferPlan
from .protocol import (
    DEFAULT_IO_CHUNK_SIZE,
    ProtocolError,
    read_message,
    sha256_file,
    sha256_range,
    split_ranges,
    write_message,
)


class TransferError(RuntimeError):
    """Raised when a transfer cannot complete after its retry budget."""


@dataclass(frozen=True, slots=True)
class RangeResult:
    start: int
    end: int
    bytes_sent: int
    attempts: int
    resumed: bool


@dataclass(frozen=True, slots=True)
class TransferStats:
    transfer_id: str
    filename: str
    size_bytes: int
    sha256: str
    rtt_ms: float
    chunk_size_bytes: int
    requested_streams: int
    active_streams: int
    estimated_loss_pct: float
    elapsed_seconds: float
    bytes_sent_this_run: int
    throughput_mbps_this_run: float
    effective_file_rate_mbps: float
    completed_path: str
    retries: int
    resumed_ranges: int


async def probe_rtt(host: str, port: int, samples: int = 3, timeout: float = 10.0) -> float:
    """Measure TCP connect + one request/response round-trip; return median milliseconds."""
    if samples < 1:
        raise ValueError("samples must be at least 1")
    measurements: list[float] = []
    for _ in range(samples):
        writer: asyncio.StreamWriter | None = None
        try:
            started = time.perf_counter()
            reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=timeout)
            await write_message(writer, {"type": "probe", "client_time_ns": time.monotonic_ns()})
            response = await asyncio.wait_for(read_message(reader), timeout=timeout)
            elapsed = (time.perf_counter() - started) * 1000
            if response.get("status") != "pong":
                raise TransferError(f"receiver probe failed: {response.get('error', response)}")
            measurements.append(elapsed)
        except (OSError, asyncio.TimeoutError, ProtocolError) as exc:
            raise TransferError(f"could not probe receiver {host}:{port}: {exc}") from exc
        finally:
            if writer is not None:
                writer.close()
                try:
                    await writer.wait_closed()
                except (ConnectionError, OSError):
                    pass
    return statistics.median(measurements)


def make_transfer_id(source: Path, size: int, mtime_ns: int, checksum: str, filename: str) -> str:
    identity = f"{source.resolve()}|{size}|{mtime_ns}|{checksum}|{filename}".encode("utf-8")
    return hashlib.sha256(identity).hexdigest()[:32]


async def _send_range(
    *,
    source: Path,
    host: str,
    port: int,
    transfer_id: str,
    filename: str,
    size: int,
    checksum: str,
    start: int,
    end: int,
    range_checksum: str,
    chunk_size: int,
    retries: int,
    timeout: float,
) -> RangeResult:
    last_error: Exception | None = None
    range_label = f"{start}:{end}"
    for attempt in range(1, retries + 2):
        writer: asyncio.StreamWriter | None = None
        bytes_sent = 0
        try:
            reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=timeout)
            header = {
                "type": "range",
                "transfer_id": transfer_id,
                "filename": filename,
                "size": size,
                "sha256": checksum,
                "start": start,
                "end": end,
                "range_sha256": range_checksum,
            }
            await write_message(writer, header)
            response = await asyncio.wait_for(read_message(reader), timeout=timeout)
            status = response.get("status")
            if status == "already_complete":
                return RangeResult(start, end, 0, attempt, True)
            if status != "ready":
                raise TransferError(f"receiver rejected range {range_label}: {response.get('error', response)}")

            remaining = end - start
            with open(source, "rb") as file_obj:
                file_obj.seek(start)
                while remaining:
                    block = file_obj.read(min(chunk_size, remaining))
                    if not block:
                        raise OSError("source file ended unexpectedly during transfer")
                    writer.write(block)
                    await writer.drain()
                    bytes_sent += len(block)
                    remaining -= len(block)

            ack = await asyncio.wait_for(read_message(reader), timeout=max(timeout, 30.0))
            if ack.get("status") != "complete":
                raise TransferError(f"receiver did not commit range {range_label}: {ack.get('error', ack)}")
            return RangeResult(start, end, bytes_sent, attempt, False)
        except (OSError, ConnectionError, asyncio.TimeoutError, ProtocolError, TransferError) as exc:
            last_error = exc
            if attempt <= retries:
                await asyncio.sleep(min(0.25 * (2 ** (attempt - 1)), 3.0))
        finally:
            if writer is not None:
                writer.close()
                try:
                    await writer.wait_closed()
                except (ConnectionError, OSError):
                    pass
    raise TransferError(f"range {range_label} failed after {retries + 1} attempt(s): {last_error}")


async def finalize_transfer(
    host: str,
    port: int,
    transfer_id: str,
    filename: str,
    size: int,
    checksum: str,
    timeout: float = 60.0,
) -> tuple[str, bool]:
    writer: asyncio.StreamWriter | None = None
    try:
        reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=timeout)
        await write_message(
            writer,
            {
                "type": "finalize",
                "transfer_id": transfer_id,
                "filename": filename,
                "size": size,
                "sha256": checksum,
            },
        )
        response = await asyncio.wait_for(read_message(reader), timeout=timeout)
        if response.get("status") != "complete":
            raise TransferError(f"receiver could not finalize transfer: {response.get('error', response)}")
        return str(response["path"]), bool(response.get("resumed", False))
    except (OSError, asyncio.TimeoutError, ProtocolError) as exc:
        raise TransferError(f"finalization failed: {exc}") from exc
    finally:
        if writer is not None:
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass


async def transfer_file(
    source: str | Path,
    host: str,
    port: int = 5050,
    *,
    requested_streams: int = 4,
    retries: int = 3,
    loss_pct: float = 0.0,
    bandwidth_mbps: float | None = None,
    previous_throughput_mbps: float | None = None,
    transfer_id: str | None = None,
    filename: str | None = None,
    timeout: float = 30.0,
) -> TransferStats:
    path = Path(source).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(f"source file does not exist: {path}")
    if requested_streams < 1:
        raise ValueError("requested_streams must be at least 1")
    if retries < 0:
        raise ValueError("retries cannot be negative")
    if not 0 <= loss_pct <= 100:
        raise ValueError("loss_pct must be between 0 and 100")
    target_filename = filename or path.name
    if (not target_filename or target_filename in {".", ".."} or "/" in target_filename or "\\" in target_filename or "\x00" in target_filename):
        raise ValueError("filename must be a plain file name, not a path")

    initial_stat = path.stat()
    checksum = await asyncio.to_thread(sha256_file, path)
    current_stat = path.stat()
    if (initial_stat.st_size, initial_stat.st_mtime_ns) != (current_stat.st_size, current_stat.st_mtime_ns):
        raise TransferError("source file changed while its checksum was being calculated")
    size = current_stat.st_size
    effective_id = transfer_id or make_transfer_id(path, size, current_stat.st_mtime_ns, checksum, target_filename)
    if not (8 <= len(effective_id) <= 128) or not all(
        ch in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for ch in effective_id
    ):
        raise ValueError("transfer_id must be 8-128 letters, digits, underscores, or hyphens")

    rtt_ms = await probe_rtt(host, port, timeout=timeout)
    metrics = NetworkMetrics(
        rtt_ms=rtt_ms,
        loss_pct=loss_pct,
        bandwidth_mbps=bandwidth_mbps,
        previous_throughput_mbps=previous_throughput_mbps,
    )
    plan: TransferPlan = AdaptivePolicy(max_concurrency=max(8, requested_streams)).choose(
        metrics, requested_concurrency=requested_streams
    )
    ranges = split_ranges(size, plan.concurrency)
    active_streams = len(ranges)
    range_hashes = await asyncio.gather(
        *(asyncio.to_thread(sha256_range, path, start, end) for start, end in ranges)
    )

    print(
        "Adaptive plan: "
        f"RTT={rtt_ms:.2f} ms, loss-scenario={loss_pct:.2f}%, "
        f"block={plan.chunk_size / 1024:.0f} KiB, streams={active_streams}",
        flush=True,
    )
    for reason in plan.rationale:
        print(f"  - {reason}", flush=True)

    started = time.perf_counter()
    results = await asyncio.gather(
        *(
            _send_range(
                source=path,
                host=host,
                port=port,
                transfer_id=effective_id,
                filename=target_filename,
                size=size,
                checksum=checksum,
                start=start,
                end=end,
                range_checksum=range_hash,
                chunk_size=plan.chunk_size,
                retries=retries,
                timeout=timeout,
            )
            for (start, end), range_hash in zip(ranges, range_hashes, strict=True)
        )
    )
    completed_path, was_finalized_before = await finalize_transfer(
        host, port, effective_id, target_filename, size, checksum, timeout=max(timeout, 60.0)
    )
    elapsed = max(time.perf_counter() - started, 1e-9)
    bytes_sent = sum(result.bytes_sent for result in results)
    range_resumes = sum(result.resumed for result in results)
    retries_used = sum(max(0, result.attempts - 1) for result in results)
    throughput = (bytes_sent * 8 / 1_000_000) / elapsed
    file_rate = (size * 8 / 1_000_000) / elapsed
    return TransferStats(
        transfer_id=effective_id,
        filename=target_filename,
        size_bytes=size,
        sha256=checksum,
        rtt_ms=rtt_ms,
        chunk_size_bytes=plan.chunk_size,
        requested_streams=requested_streams,
        active_streams=active_streams,
        estimated_loss_pct=loss_pct,
        elapsed_seconds=elapsed,
        bytes_sent_this_run=bytes_sent,
        throughput_mbps_this_run=throughput,
        effective_file_rate_mbps=file_rate,
        completed_path=completed_path,
        retries=retries_used,
        resumed_ranges=range_resumes,
    )


async def _run(args: argparse.Namespace) -> None:
    stats = await transfer_file(
        args.source,
        args.host,
        args.port,
        requested_streams=args.streams,
        retries=args.retries,
        loss_pct=args.loss_pct,
        bandwidth_mbps=args.bandwidth_mbps,
        previous_throughput_mbps=args.previous_throughput_mbps,
        transfer_id=args.transfer_id,
        filename=args.filename,
        timeout=args.timeout,
    )
    print("\nTransfer completed and verified:")
    print(json.dumps(asdict(stats), indent=2))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Adaptive parallel TCP file sender")
    parser.add_argument("source", help="path to the file to send")
    parser.add_argument("host", help="receiver hostname or IP address")
    parser.add_argument("--port", type=int, default=5050, help="receiver TCP port (default: 5050)")
    parser.add_argument("--streams", type=int, default=4, help="maximum parallel TCP streams (default: 4)")
    parser.add_argument("--retries", type=int, default=3, help="retries per range after a failure (default: 3)")
    parser.add_argument("--loss-pct", type=float, default=0.0, help="known/test-scenario packet-loss percentage")
    parser.add_argument("--bandwidth-mbps", type=float, default=None, help="optional estimated link bandwidth")
    parser.add_argument("--previous-throughput-mbps", type=float, default=None, help="optional prior throughput measurement")
    parser.add_argument("--transfer-id", default=None, help="reuse an ID to explicitly resume a transfer")
    parser.add_argument("--filename", default=None, help="output filename on receiver")
    parser.add_argument("--timeout", type=float, default=30.0, help="connection/probe timeout in seconds")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    try:
        asyncio.run(_run(args))
    except (TransferError, OSError, ValueError) as exc:
        raise SystemExit(f"transfer failed: {exc}") from exc
    except KeyboardInterrupt:
        raise SystemExit("transfer interrupted; rerun with the same file/ID to resume") from None


if __name__ == "__main__":
    main()

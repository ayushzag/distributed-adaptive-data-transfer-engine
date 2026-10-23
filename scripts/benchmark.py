#!/usr/bin/env python3
"""Run one transfer benchmark and append a result row to CSV.

Apply any Linux tc/netem impairment to a dedicated test interface before running
this command. The script records observed RTT/throughput; --loss-pct informs the
adaptive policy but does not itself modify the network.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import uuid
from dataclasses import asdict
from pathlib import Path

from transfer_engine.sender import transfer_file

FIELDS = [
    "scenario",
    "filename",
    "transfer_id",
    "size_bytes",
    "sha256",
    "rtt_ms",
    "estimated_loss_pct",
    "chunk_size_bytes",
    "requested_streams",
    "active_streams",
    "elapsed_seconds",
    "bytes_sent_this_run",
    "throughput_mbps_this_run",
    "effective_file_rate_mbps",
    "retries",
    "resumed_ranges",
    "completed_path",
]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Append one transfer benchmark result to CSV")
    parser.add_argument("source", help="file to transfer")
    parser.add_argument("host", help="receiver host/IP")
    parser.add_argument("--port", type=int, default=5050)
    parser.add_argument("--scenario-label", required=True, help="label for current netem/network conditions")
    parser.add_argument("--loss-pct", type=float, default=0.0, help="packet-loss scenario input for adaptive policy")
    parser.add_argument("--bandwidth-mbps", type=float, default=None)
    parser.add_argument("--streams", type=int, default=4)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--csv", default="results.csv", help="CSV output path (default: results.csv)")
    return parser


async def _run(args: argparse.Namespace) -> None:
    source = Path(args.source).expanduser().resolve()
    # Unique destination names prevent collisions between repeated benchmark trials.
    suffix = uuid.uuid4().hex[:8]
    destination_name = f"{source.name}.{args.scenario_label}.{suffix}"
    stats = await transfer_file(
        source,
        args.host,
        args.port,
        requested_streams=args.streams,
        retries=args.retries,
        loss_pct=args.loss_pct,
        bandwidth_mbps=args.bandwidth_mbps,
        filename=destination_name,
    )
    row = {"scenario": args.scenario_label, **asdict(stats)}
    output = Path(args.csv).expanduser()
    output.parent.mkdir(parents=True, exist_ok=True)
    needs_header = not output.exists() or output.stat().st_size == 0
    with output.open("a", newline="", encoding="utf-8") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=FIELDS, extrasaction="ignore")
        if needs_header:
            writer.writeheader()
        writer.writerow(row)
    print(f"Appended benchmark result to {output.resolve()}")


def main() -> None:
    args = build_parser().parse_args()
    asyncio.run(_run(args))


if __name__ == "__main__":
    main()

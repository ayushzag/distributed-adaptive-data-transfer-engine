"""Adaptive transfer-policy decisions based on observed network metrics."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class NetworkMetrics:
    """Network measurements used for the initial transfer plan.

    ``rtt_ms`` is actively probed by the sender. ``loss_pct`` and
    ``bandwidth_mbps`` are scenario inputs because portable TCP sockets do not
    expose a reliable cross-platform packet-loss measurement. Actual throughput
    is measured after the transfer and reported in ``TransferStats``.
    """

    rtt_ms: float
    loss_pct: float = 0.0
    bandwidth_mbps: float | None = None
    previous_throughput_mbps: float | None = None


@dataclass(frozen=True, slots=True)
class TransferPlan:
    chunk_size: int
    concurrency: int
    rationale: tuple[str, ...]


class AdaptivePolicy:
    """Choose sender read/write block size and parallel TCP stream count."""

    MIN_CHUNK_SIZE = 64 * 1024
    MAX_CHUNK_SIZE = 4 * 1024 * 1024

    def __init__(self, max_concurrency: int = 8) -> None:
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be at least 1")
        self.max_concurrency = max_concurrency

    def choose(self, metrics: NetworkMetrics, requested_concurrency: int = 4) -> TransferPlan:
        if metrics.rtt_ms < 0:
            raise ValueError("rtt_ms cannot be negative")
        if not 0 <= metrics.loss_pct <= 100:
            raise ValueError("loss_pct must be between 0 and 100")
        if requested_concurrency < 1:
            raise ValueError("requested_concurrency must be at least 1")
        if metrics.bandwidth_mbps is not None and metrics.bandwidth_mbps < 0:
            raise ValueError("bandwidth_mbps cannot be negative")
        if metrics.previous_throughput_mbps is not None and metrics.previous_throughput_mbps < 0:
            raise ValueError("previous_throughput_mbps cannot be negative")

        reasons: list[str] = []
        rtt = metrics.rtt_ms
        if rtt < 20:
            chunk_size = 64 * 1024
            reasons.append("low RTT: use small blocks to keep buffering modest")
        elif rtt < 80:
            chunk_size = 256 * 1024
            reasons.append("moderate RTT: increase block size to reduce Python I/O overhead")
        elif rtt < 180:
            chunk_size = 1024 * 1024
            reasons.append("high RTT: use 1 MiB blocks to reduce per-write overhead")
        else:
            chunk_size = 2 * 1024 * 1024
            reasons.append("very high RTT: use larger blocks to reduce per-write overhead")

        if metrics.bandwidth_mbps is not None:
            if metrics.bandwidth_mbps >= 500:
                chunk_size *= 2
                reasons.append("high estimated bandwidth: increase block size")
            elif metrics.bandwidth_mbps < 25:
                chunk_size //= 2
                reasons.append("low estimated bandwidth: reduce block size")

        chunk_size = max(self.MIN_CHUNK_SIZE, min(self.MAX_CHUNK_SIZE, chunk_size))
        concurrency = min(requested_concurrency, self.max_concurrency)
        reasons.append(f"start with {concurrency} stream(s), capped by the requested limit")

        if metrics.loss_pct >= 5:
            concurrency = 1
            reasons.append("packet-loss scenario >= 5%: use one stream to limit contention")
        elif metrics.loss_pct >= 3:
            concurrency = max(1, concurrency // 2)
            reasons.append("packet-loss scenario >= 3%: halve parallelism")
        elif metrics.loss_pct >= 1:
            concurrency = max(1, (concurrency * 3) // 4)
            reasons.append("packet-loss scenario >= 1%: reduce parallelism conservatively")

        if metrics.previous_throughput_mbps is not None:
            if metrics.previous_throughput_mbps < 10 and concurrency > 1:
                concurrency = max(1, concurrency // 2)
                reasons.append("low previous throughput: reduce parallelism")
            elif metrics.previous_throughput_mbps >= 500:
                chunk_size = min(self.MAX_CHUNK_SIZE, chunk_size * 2)
                reasons.append("high previous throughput: increase block size")

        return TransferPlan(chunk_size=chunk_size, concurrency=concurrency, rationale=tuple(reasons))

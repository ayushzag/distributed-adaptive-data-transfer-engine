import pytest

from transfer_engine.adaptive import AdaptivePolicy, NetworkMetrics


@pytest.mark.parametrize(
    ("rtt_ms", "expected"),
    [
        (0, 64 * 1024),
        (1, 64 * 1024),
        (5.5, 64 * 1024),
        (19.99, 64 * 1024),
        (20, 256 * 1024),
        (35, 256 * 1024),
        (50, 256 * 1024),
        (79.99, 256 * 1024),
        (80, 1024 * 1024),
        (100, 1024 * 1024),
        (150, 1024 * 1024),
        (179.99, 1024 * 1024),
        (180, 2 * 1024 * 1024),
        (200, 2 * 1024 * 1024),
        (500, 2 * 1024 * 1024),
    ],
)
def test_rtt_selects_expected_block_size(rtt_ms, expected):
    plan = AdaptivePolicy().choose(NetworkMetrics(rtt_ms=rtt_ms), requested_concurrency=4)
    assert plan.chunk_size == expected


@pytest.mark.parametrize(
    ("loss_pct", "expected_concurrency"),
    [(0, 4), (0.5, 4), (1, 3), (2.99, 3), (3, 2), (4.9, 2), (5, 1), (10, 1), (100, 1)],
)
def test_packet_loss_scenario_reduces_parallelism(loss_pct, expected_concurrency):
    plan = AdaptivePolicy().choose(NetworkMetrics(rtt_ms=50, loss_pct=loss_pct), requested_concurrency=4)
    assert plan.concurrency == expected_concurrency


@pytest.mark.parametrize(
    ("bandwidth", "expected"),
    [(0.1, 128 * 1024), (10, 128 * 1024), (24.9, 128 * 1024), (25, 256 * 1024), (100, 256 * 1024), (500, 512 * 1024), (1000, 512 * 1024)],
)
def test_bandwidth_estimate_adjusts_block_size(bandwidth, expected):
    plan = AdaptivePolicy().choose(
        NetworkMetrics(rtt_ms=50, bandwidth_mbps=bandwidth), requested_concurrency=4
    )
    assert plan.chunk_size == expected


@pytest.mark.parametrize("requested", [1, 2, 3, 4, 6, 8])
def test_requested_concurrency_caps_plan(requested):
    plan = AdaptivePolicy(max_concurrency=8).choose(NetworkMetrics(rtt_ms=5), requested_concurrency=requested)
    assert plan.concurrency == requested


def test_policy_has_explanations():
    plan = AdaptivePolicy().choose(NetworkMetrics(rtt_ms=120, loss_pct=3), requested_concurrency=4)
    assert plan.rationale
    assert any("parallelism" in reason for reason in plan.rationale)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"rtt_ms": -1},
        {"rtt_ms": 1, "loss_pct": -0.1},
        {"rtt_ms": 1, "loss_pct": 100.1},
        {"rtt_ms": 1, "bandwidth_mbps": -1},
        {"rtt_ms": 1, "previous_throughput_mbps": -1},
    ],
)
def test_invalid_metrics_are_rejected(kwargs):
    with pytest.raises(ValueError):
        AdaptivePolicy().choose(NetworkMetrics(**kwargs))


@pytest.mark.parametrize("max_concurrency", [0, -1, -5])
def test_invalid_policy_concurrency_is_rejected(max_concurrency):
    with pytest.raises(ValueError):
        AdaptivePolicy(max_concurrency=max_concurrency)


def test_low_previous_throughput_reduces_concurrency():
    plan = AdaptivePolicy().choose(
        NetworkMetrics(rtt_ms=40, previous_throughput_mbps=5), requested_concurrency=4
    )
    assert plan.concurrency == 2


def test_high_previous_throughput_increases_block_size():
    plan = AdaptivePolicy().choose(
        NetworkMetrics(rtt_ms=5, previous_throughput_mbps=600), requested_concurrency=4
    )
    assert plan.chunk_size == 128 * 1024


def test_requested_concurrency_must_be_positive():
    with pytest.raises(ValueError, match="requested_concurrency"):
        AdaptivePolicy().choose(NetworkMetrics(rtt_ms=10), requested_concurrency=0)

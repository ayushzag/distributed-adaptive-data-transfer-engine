# ⚡ Adaptive Data Transfer Engine

**A Python-based distributed file-transfer engine built with asyncio and TCP sockets.** It transfers large files over parallel connections, resumes verified byte ranges after interruption, retries failed ranges, and validates the reconstructed file with SHA-256.

> This repository is a portfolio/reference implementation. The TCP data plane is functional; the optional control-plane registry is a lightweight in-memory demo, not a production scheduler.

## Highlights

- **Adaptive transfer plan:** probes round-trip latency and selects an application I/O block size and stream count using RTT plus optional bandwidth and packet-loss scenario inputs.
- **Parallel TCP transfer:** splits a file into byte ranges and transfers them concurrently over independent TCP connections.
- **Resume support:** the receiver persists completed-range metadata. Re-running the same source file can skip ranges that were already received and verified.
- **Retry handling:** failed or interrupted ranges are retried with exponential backoff.
- **Integrity checks:** checks SHA-256 for every range and verifies the complete file before publishing it to the output directory.
- **Measured metrics:** reports RTT, bytes sent, elapsed time, effective file-transfer rate, retry count, and resume information.
- **Control plane:** provides HTTP endpoints to register nodes and track transfer metadata and status.
- **Container/deployment examples:** includes Docker, Docker Compose, and Kubernetes manifests.

## Architecture

```text
                         Optional HTTP Control Plane
                         ┌──────────────────────────┐
                         │ FastAPI                  │
                         │ /nodes/register          │
                         │ /nodes                   │
                         │ /transfers               │
                         └──────────────────────────┘

┌───────────────────────┐       Parallel TCP ranges       ┌────────────────────────┐
│ Sender                │ ═══════════════════════════════> │ Receiver               │
│                       │   range 1 / range 2 / ...        │                        │
│ RTT probe             │ <──────── TCP probe ──────────── │ range checkpoint store │
│ adaptive policy       │                                  │ per-range SHA-256       │
│ asyncio workers       │                                  │ whole-file SHA-256     │
└───────────────────────┘                                  └────────────┬───────────┘
                                                                       │
                                                            verified file is published
                                                                       │
                                                            ┌──────────▼───────────┐
                                                            │ Output directory     │
                                                            └──────────────────────┘
```

The TCP sender/receiver work without the control plane. The control plane is for lightweight node registration and transfer-status tracking; it does not schedule or proxy the data sockets.

## Tech Stack

`Python` · `asyncio` · `TCP Sockets` · `SHA-256` · `FastAPI` · `pytest` · `Docker` · `Kubernetes` · `Linux tc/netem`

## Quick Start

### 1. Install

Python 3.10 or newer is recommended.

```bash
git clone https://github.com/ayushzag/adaptive-tcp-data-transfer-engine.git
cd adaptive-tcp-data-transfer-engine
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
python -m pip install -e ".[dev]"
```

### 2. Start the receiver

```bash
python -m transfer_engine.receiver --host 0.0.0.0 --port 5050 --output-dir ./received
```

### 3. Send a file

Open another terminal in the project directory:

```bash
python -m transfer_engine.sender ./path/to/large-file.bin 127.0.0.1 --port 5050 --streams 4
```

The receiver saves the verified output in `./received/`. By default, a repeat run derives the same transfer ID from the source path, size, modification time, checksum, and destination name. To explicitly resume with a chosen ID, provide `--transfer-id` and reuse that same value on subsequent runs.

Use `--filename output.bin` to choose the destination filename. The receiver avoids overwriting a different existing file by default. Set `--allow-overwrite` on the receiver only when that behavior is intended.

## Adaptive Policy and Network Metrics

The sender takes three RTT probe samples and uses their median. The initial policy picks a block size and number of parallel streams based on latency, an optional estimated bandwidth, a previous throughput measurement, and a packet-loss scenario.

```bash
python -m transfer_engine.sender ./large-file.bin 10.0.0.20 \
  --port 5050 \
  --streams 4 \
  --loss-pct 3 \
  --bandwidth-mbps 100
```

Important measurement detail: **RTT is actively probed; throughput is measured during the transfer; `--loss-pct` is a scenario input, not a direct packet-loss measurement taken from the TCP socket.** Portable TCP sockets do not provide a reliable cross-platform loss percentage. Use it to tell the policy which `tc/netem` scenario you are testing. `--bandwidth-mbps` and `--previous-throughput-mbps` are optional estimates/inputs.

The block size is an application read/write buffer size; it does not change TCP's segment size or MTU. The kernel's TCP congestion control continues to manage packet delivery.

### Example test matrix

These examples assume network emulation has already been applied to a test interface:

| Scenario | Suggested sender inputs |
|---|---|
| Baseline | `--loss-pct 0` |
| 50 ms configured delay, 1% loss | `--loss-pct 1` |
| 100 ms configured delay, 3% loss | `--loss-pct 3` |
| 200 ms configured delay, 5% loss | `--loss-pct 5` |

The sender measures the resulting RTT. Record the output for each run when comparing throughput, retries, and effective file rate. Results depend on the host, link, storage, congestion control, and how the impairment is applied; no fixed throughput guarantee is implied.

### Optional Linux `tc/netem` helper

`scripts/netem-run.sh` applies a temporary impairment to one Linux network interface while a command runs:

```bash
./scripts/netem-run.sh veth-test 50ms 1% \
  python -m transfer_engine.sender ./large-file.bin 10.0.0.20 --port 5050 --streams 4 --loss-pct 1
```

Use a **dedicated test interface** (such as a veth pair between test containers or VMs). The helper removes the interface's root qdisc on exit; do not use it on an interface whose existing qdisc configuration must be preserved. Applying netem to loopback can be noisy and is not a substitute for a controlled multi-host or veth test.

For repeatable CSV measurements, start the receiver and run the benchmark command once per network scenario. Each run appends observed RTT, throughput, retries, chosen block size, and active stream count to `results.csv`:

```bash
./scripts/netem-run.sh veth-test 50ms 1% \
  .venv/bin/python scripts/benchmark.py ./large-file.bin 10.0.0.20 \
  --scenario-label rtt50ms_loss1 --loss-pct 1 --csv results.csv
```

Repeat with the other delay/loss settings, changing `--scenario-label` each time. The helper configures the interface; the benchmark script records the measurements and policy inputs.

## Control Plane API

Start the API in a separate terminal:

```bash
python -m control_plane.app --host 0.0.0.0 --port 8000
```

Interactive API docs: `http://127.0.0.1:8000/docs`

| Method | Endpoint | Purpose |
|---|---|---|
| `GET` | `/healthz` | Health check |
| `POST` | `/nodes/register` | Register/update a node |
| `GET` | `/nodes` | List registered nodes |
| `POST` | `/transfers` | Register a transfer record |
| `GET` | `/transfers` | List transfer records |
| `GET` | `/transfers/{transfer_id}` | Fetch transfer details |
| `PATCH` | `/transfers/{transfer_id}` | Update lifecycle status and metrics |

The registry is stored in process memory and is cleared when the control-plane process restarts. This demo API has no authentication; do not expose it to the public internet as-is.

## Tests

```bash
pytest
pytest --cov=transfer_engine --cov=control_plane --cov-report=term-missing
```

Integration tests start a receiver on a local ephemeral port, send a file, and verify that the destination SHA-256 matches the source. The latest local run completed **144 tests with 91% combined coverage** using the coverage command above. Network-emulation performance trials should be run separately on an isolated Linux test interface.

## Docker Compose

```bash
docker compose up --build
```

This starts the TCP receiver on port `5050` and the optional control plane on port `8000`. Completed files and resume metadata are stored under `./received`. To run a sender from your host, install the package locally and target `127.0.0.1:5050`.

To allow overwriting an existing destination file in Compose, add `--allow-overwrite` to the receiver command in `docker-compose.yml` after checking that this is desired.

## Kubernetes

The `k8s/` folder contains a single-replica receiver with persistent storage and a single-replica control-plane example. Build the images (then push them or load them into your local cluster) before applying the manifests:

```bash
docker build -t adaptive-data-transfer-engine:1.0.0 -f Dockerfile .
docker build -t adaptive-transfer-control-plane:1.0.0 -f Dockerfile.control-plane .
# For kind: kind load docker-image adaptive-data-transfer-engine:1.0.0
# For kind: kind load docker-image adaptive-transfer-control-plane:1.0.0
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/receiver.yaml
kubectl apply -f k8s/control-plane.yaml
# To connect from your workstation:
kubectl port-forward -n adaptive-transfer service/transfer-receiver 5050:5050
kubectl port-forward -n adaptive-transfer service/transfer-control-plane 8000:8000
```

The receiver example uses a persistent volume claim for partial-transfer checkpoints and completed files. It intentionally uses one receiver replica because its checkpoint store is local to that mounted volume. The control plane is also one replica because its demo registry is in memory.

## Limitations and Next Steps

- No TLS or authentication is implemented for the raw TCP protocol. Run it only on a trusted network or add transport security before broader use.
- The control-plane registry is in-memory and does not orchestrate the TCP transfer.
- The loss percentage is provided as a scenario input; it is not directly inferred from a socket statistic.
- For production use, add authenticated peers, encrypted transport, durable control-plane storage, quotas, cancellation, observability, and explicit protocol-version negotiation.

## License

MIT. See [LICENSE](LICENSE).

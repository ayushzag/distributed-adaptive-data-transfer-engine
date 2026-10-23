#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 4 ]]; then
  echo "Usage: $0 <TEST_INTERFACE> <DELAY> <LOSS> <COMMAND> [ARGS...]" >&2
  echo "Example: $0 veth-test 50ms 1% python -m transfer_engine.sender file.bin 10.0.0.2 --loss-pct 1" >&2
  exit 2
fi

iface="$1"
delay="$2"
loss="$3"
shift 3

if [[ $# -eq 0 ]]; then
  echo "A command to run is required." >&2
  exit 2
fi

if ! command -v tc >/dev/null 2>&1; then
  echo "Linux tc (iproute2) is required." >&2
  exit 2
fi

applied=0
cleanup() {
  if [[ "$applied" -eq 1 ]]; then
    sudo tc qdisc del dev "$iface" root >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT INT TERM

# WARNING: use only on a disposable test interface; cleanup removes its root qdisc.
sudo tc qdisc replace dev "$iface" root netem delay "$delay" loss "$loss"
applied=1
"$@"

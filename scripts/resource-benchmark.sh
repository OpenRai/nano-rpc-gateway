#!/usr/bin/env bash
set -euo pipefail

gateway_bin="${GATEWAY_BIN:-target/release/nano-rpc-gateway}"
listen_port="${LISTEN_PORT:-18080}"
gateway_url="http://127.0.0.1:${listen_port}"
tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/nano-rpc-gateway-resource.XXXXXX")"
config_file="$tmp_dir/gateway.yaml"
log_file="$tmp_dir/gateway.log"
gateway_pid=""

cleanup() {
  if [[ -n "$gateway_pid" ]]; then
    kill "$gateway_pid" 2>/dev/null || true
    wait "$gateway_pid" 2>/dev/null || true
  fi
  rm -rf "$tmp_dir"
}
trap cleanup EXIT

if [[ ! -x "$gateway_bin" ]]; then
  echo "gateway binary is not executable: $gateway_bin" >&2
  exit 2
fi

# The upstream ports are deliberately unreachable: this probe measures the
# gateway process' startup and idle footprint, not a Nano node.
cat >"$config_file" <<EOF
listen: "127.0.0.1:${listen_port}"
node_rpc_url: "http://127.0.0.1:9"
node_ws_url: "ws://127.0.0.1:9"
profile: "nano-node/V28.2"
allow_work: false
allow_control: false
auth_public_key: null
enable_discovery: true
EOF

start_ns="$(date +%s%N)"
"$gateway_bin" serve --config "$config_file" >"$log_file" 2>&1 &
gateway_pid=$!

healthy=0
for _ in $(seq 1 100); do
  if curl --fail --silent "$gateway_url/health" >/dev/null; then
    healthy=1
    break
  fi
  sleep 0.01
done

if [[ "$healthy" != 1 ]]; then
  echo "gateway did not become healthy" >&2
  sed -n '1,80p' "$log_file" >&2
  exit 1
fi

ready_ns="$(date +%s%N)"
startup_ms=$(( (ready_ns - start_ns) / 1000000 ))
rss_kb="$(ps -o rss= -p "$gateway_pid" | awk '{print $1}')"

printf 'gateway=%s startup_ms=%s idle_rss_kb=%s upstream=unreachable_probe\n' \
  "$gateway_bin" "$startup_ms" "$rss_kb"

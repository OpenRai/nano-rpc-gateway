#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${NANO_RPC_URL:-}" ]]; then
  echo 'NANO_RPC_URL is required, for example NANO_RPC_URL=http://127.0.0.1:7076' >&2
  exit 2
fi

gateway_binary="${GATEWAY_BINARY:-./target/release/nano-rpc-gateway}"
gateway_listen="${GATEWAY_LISTEN:-127.0.0.1:8090}"
gateway_url="${GATEWAY_URL:-http://${gateway_listen}/rpc}"
gateway_health_url="${GATEWAY_HEALTH_URL:-http://${gateway_listen}/health}"
playground_url="${PLAYGROUND_URL:-http://127.0.0.1:8080/}"
tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/nano-rpc-gateway-playground.XXXXXX")"
gateway_log="$tmp_dir/gateway.log"
playground_log="$tmp_dir/playground.log"
gateway_pid=''
playground_pid=''

cleanup() {
  trap - EXIT INT TERM
  [[ -z "$playground_pid" ]] || kill "$playground_pid" 2>/dev/null || true
  [[ -z "$gateway_pid" ]] || kill "$gateway_pid" 2>/dev/null || true
  [[ -z "$playground_pid" ]] || wait "$playground_pid" 2>/dev/null || true
  [[ -z "$gateway_pid" ]] || wait "$gateway_pid" 2>/dev/null || true
  rm -rf "$tmp_dir"
}
trap cleanup EXIT INT TERM

if [[ ! -x "$gateway_binary" ]]; then
  echo "gateway binary not found at $gateway_binary; run 'make build' first" >&2
  exit 1
fi

NANO_RPC_URL="$NANO_RPC_URL" NANO_WS_URL="${NANO_WS_URL:-}" \
  GATEWAY_BINARY="$gateway_binary" GATEWAY_LISTEN="$gateway_listen" \
  ./scripts/run-local-gateway.sh >"$gateway_log" 2>&1 &
gateway_pid=$!

gateway_ready=0
for _ in $(seq 1 "${GATEWAY_ATTEMPTS:-30}"); do
  if curl --fail --silent "$gateway_health_url" >/dev/null; then
    gateway_ready=1
    break
  fi
  if ! kill -0 "$gateway_pid" 2>/dev/null; then
    break
  fi
  sleep 1
done
if [[ "$gateway_ready" != 1 ]]; then
  echo 'gateway did not become healthy' >&2
  sed -n '1,120p' "$gateway_log" >&2
  exit 1
fi

PLAYGROUND_LISTEN="${PLAYGROUND_LISTEN:-tcp://127.0.0.1:8080}" \
  ./scripts/serve-openrpc-playground.sh >"$playground_log" 2>&1 &
playground_pid=$!

playground_ready=0
for _ in $(seq 1 "${PLAYGROUND_ATTEMPTS:-60}"); do
  if curl --fail --silent "$playground_url" >/dev/null; then
    playground_ready=1
    break
  fi
  if ! kill -0 "$playground_pid" 2>/dev/null; then
    break
  fi
  sleep 1
done
if [[ "$playground_ready" != 1 ]]; then
  echo 'OpenRPC Playground did not become available' >&2
  sed -n '1,120p' "$playground_log" >&2
  exit 1
fi

schema_gateway_url="${gateway_url%/}"
playground_link="$($gateway_binary playground --gateway-url "$schema_gateway_url")"
echo "Gateway: $gateway_url"
echo "Playground: $playground_link"
if [[ "${OPEN_PLAYGROUND:-1}" == 1 && "$(uname -s)" == Darwin && -x "$(command -v open 2>/dev/null || true)" ]]; then
  open "$playground_link"
fi
echo 'Press Ctrl-C to stop both services.'
wait "$gateway_pid"

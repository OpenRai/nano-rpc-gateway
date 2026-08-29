#!/usr/bin/env bash
set -euo pipefail

gateway_url="${GATEWAY_URL:-http://127.0.0.1:8090}"
attempts="${ATTEMPTS:-60}"
account="${NANO_ACCOUNT:-nano_3e3j5tkog48pnny9dmfzj1r16pg8t1e76dz5tmac6iq689wyjfpiij4txtdo}"
start_devnet="${START_DEVNET:-0}"
compose_file="${COMPOSE_FILE:-docker-compose.dev.yml}"

if [[ "$start_devnet" == "1" ]]; then
  docker compose -f "$compose_file" up -d --build
  cleanup() { docker compose -f "$compose_file" down; }
  trap cleanup EXIT
fi

healthy=0
for _ in $(seq 1 "$attempts"); do
  if curl --fail --silent --show-error "$gateway_url/health" >/dev/null; then
    healthy=1
    break
  fi
  sleep 2
done

if [[ "$healthy" != 1 ]]; then
  echo "gateway did not become healthy after ${attempts} attempts" >&2
  exit 1
fi

curl --fail --silent --show-error "$gateway_url/health"
curl --fail --silent --show-error "$gateway_url/openrpc.json" | jq -e '.openrpc == "1.3.2"'
curl --fail --silent --show-error \
  -H 'content-type: application/json' \
  --data '{"jsonrpc":"2.0","method":"rpc.discover","id":1}' \
  "$gateway_url/rpc" | jq -e '.result.openrpc == "1.3.2"'
account_request="$(jq -cn --arg account "$account" '{jsonrpc:"2.0",method:"account_info",params:{account:$account},id:2}')"
account_ready=0
for _ in $(seq 1 "$attempts"); do
  if curl --fail --silent --show-error \
    -H 'content-type: application/json' \
    --data "$account_request" \
    "$gateway_url/rpc" | jq -e '.jsonrpc == "2.0" and .result != null' >/dev/null; then
    account_ready=1
    break
  fi
  sleep 2
done
if [[ "$account_ready" != 1 ]]; then
  echo "gateway did not reach the Nano RPC upstream after ${attempts} attempts" >&2
  exit 1
fi

# Exercise the process path with a deliberately invalid block. A fresh devnet
# does not contain a reusable signed block, so this verifies translation and
# native error handling without embedding private keys in the fixture.
curl --fail --silent --show-error \
  -H 'content-type: application/json' \
  --data '{"jsonrpc":"2.0","method":"process","params":{"block":{"type":"state","account":"nano_3e3j5tkog48pnny9dmfzj1r16pg8t1e76dz5tmac6iq689wyjfpiij4txtdo"}},"id":3}' \
  "$gateway_url/rpc" | jq -e '.jsonrpc == "2.0" and .error.code == -32000'

# Confirm the public stream is available with the browser-native SSE media
# type. max-time is expected because an idle stream has no data frame.
sse_headers="$(mktemp "${TMPDIR:-/tmp}/nano-rpc-gateway-sse-headers.XXXXXX")"
cleanup_sse() { rm -f "$sse_headers"; }
trap 'cleanup_sse; if [[ "$start_devnet" == "1" ]]; then docker compose -f "$compose_file" down; fi' EXIT
curl --silent --show-error --max-time 3 -D "$sse_headers" "$gateway_url/events/confirmations" >/dev/null || true
grep -iq '^content-type: text/event-stream' "$sse_headers"
curl --fail --silent --show-error "$gateway_url/readyz" | jq -e '.status == "ready"'

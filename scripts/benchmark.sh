#!/usr/bin/env bash
set -euo pipefail

gateway_url="${GATEWAY_URL:-http://127.0.0.1:8090}"
requests="${REQUESTS:-100}"

printf 'gateway=%s requests=%s\n' "$gateway_url" "$requests"
printf 'health: '
curl --fail --silent --show-error "$gateway_url/health"
printf '\n'

for _ in $(seq 1 "$requests"); do
  curl --fail --silent --show-error \
    -H 'content-type: application/json' \
    --data '{"jsonrpc":"2.0","method":"account_info","params":{"account":"nano_test"},"id":1}' \
    "$gateway_url/rpc" >/dev/null
done

printf 'completed=%s\n' "$requests"

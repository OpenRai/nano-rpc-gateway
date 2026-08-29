#!/usr/bin/env bash
set -euo pipefail

gateway_url="${GATEWAY_URL:-http://127.0.0.1:8090}"
attempts="${ATTEMPTS:-60}"
account="${NANO_ACCOUNT:-nano_3e3j5tkog48pnny9dmfzj1r16pg8t1e76dz5tmac6iq689wyjfpiij4txtdo}"

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
curl --fail --silent --show-error \
  -H 'content-type: application/json' \
  --data "$(jq -cn --arg account "$account" '{jsonrpc:"2.0",method:"account_info",params:{account:$account},id:2}')" \
  "$gateway_url/rpc" | jq -e '.jsonrpc == "2.0" and .result != null'

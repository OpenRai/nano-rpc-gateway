#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${NANO_RPC_URL:-}" ]]; then
  echo 'NANO_RPC_URL is required, for example NANO_RPC_URL=http://127.0.0.1:7076' >&2
  exit 2
fi

gateway_binary="${GATEWAY_BINARY:-./target/release/nano-rpc-gateway}"
gateway_listen="${GATEWAY_LISTEN:-127.0.0.1:8090}"
tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/nano-rpc-gateway-local.XXXXXX")"
config_file="$tmp_dir/gateway.yaml"
gateway_pid=''

cleanup() {
  [[ -z "$gateway_pid" ]] || kill "$gateway_pid" 2>/dev/null || true
  [[ -z "$gateway_pid" ]] || wait "$gateway_pid" 2>/dev/null || true
  rm -rf "$tmp_dir"
}
on_signal() {
  cleanup
  exit 143
}
trap cleanup EXIT
trap on_signal INT TERM

if [[ ! -x "$gateway_binary" ]]; then
  echo "gateway binary not found at $gateway_binary; run 'make build' first" >&2
  exit 1
fi

derive_ws_url() {
  local rpc_url="$1"
  local scheme authority host port ws_scheme
  rpc_url="${rpc_url%/}"
  scheme="${rpc_url%%://*}"
  authority="${rpc_url#*://}"
  if [[ "$authority" == */* || "$authority" == "$rpc_url" ]]; then
    return 1
  fi
  host="${authority%:*}"
  port="${authority##*:}"
  if [[ -z "$host" || "$host" == "$authority" || "$port" != 7076 ]]; then
    return 1
  fi
  case "$scheme" in
    http) ws_scheme=ws ;;
    https) ws_scheme=wss ;;
    *) return 1 ;;
  esac
  printf '%s://%s:7078\n' "$ws_scheme" "$host"
}

if [[ -n "${NANO_WS_URL:-}" ]]; then
  node_ws_url="$NANO_WS_URL"
elif ! node_ws_url="$(derive_ws_url "$NANO_RPC_URL")"; then
  echo 'NANO_WS_URL is required when NANO_RPC_URL is not an http(s)://host:7076 URL' >&2
  exit 2
fi

{
  printf 'listen: "%s"\n' "$gateway_listen"
  printf 'node_rpc_url: "%s"\n' "$NANO_RPC_URL"
  printf 'node_ws_url: "%s"\n' "$node_ws_url"
  printf 'profile: "nano-node/V28.2"\n'
  printf 'allow_work: false\nallow_control: false\n'
  printf 'auth_public_key: null\nenable_discovery: true\n'
  printf 'tls_cert: null\ntls_key: null\n'
} >"$config_file"

echo "Gateway: http://${gateway_listen}/rpc"
echo "Backend: $NANO_RPC_URL"
"$gateway_binary" serve --config "$config_file" &
gateway_pid=$!
if wait "$gateway_pid"; then
  status=0
else
  status=$?
fi
gateway_pid=''
exit "$status"

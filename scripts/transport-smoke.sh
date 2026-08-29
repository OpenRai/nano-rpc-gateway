#!/usr/bin/env bash
set -euo pipefail

# Exercise the public TLS listener with both protocol preferences. This is a
# deployment smoke check: the gateway must already be running with a complete
# certificate/key pair, and curl must have HTTP/2 support.
gateway_url="${GATEWAY_URL:-https://127.0.0.1:8090}"
curl_tls_args=()
if [[ "${INSECURE_TLS:-0}" == "1" ]]; then
  curl_tls_args+=(--insecure)
fi

probe() {
  local protocol="$1"
  curl --fail --silent --show-error "${curl_tls_args[@]}" \
    "--${protocol}" -D - -o /dev/null "$gateway_url/health" \
    | awk -v expected="$protocol" 'tolower($1) == (expected == "http1.1" ? "http/1.1" : "http/2") { print $1; found=1; exit } END { if (!found) exit 1 }'
}

http1="$(probe http1.1)"
http2="$(probe http2)"
printf 'http1=%s http2=%s\n' "$http1" "$http2"

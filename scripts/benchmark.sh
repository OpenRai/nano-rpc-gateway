#!/usr/bin/env bash
set -euo pipefail

gateway_url="${GATEWAY_URL:-http://127.0.0.1:8090}"
requests="${REQUESTS:-100}"
output_file="${OUTPUT_FILE:-}"
protocol="${PROTOCOL:-http1.1}"
account="${ACCOUNT:-nano_test}"
tls_args=()
if [[ "${INSECURE_TLS:-0}" == "1" ]]; then
  tls_args+=(--insecure)
fi

case "$protocol" in
  http1.1) protocol_args=(--http1.1) ;;
  http2) protocol_args=(--http2) ;;
  *) echo "unsupported PROTOCOL=$protocol (expected http1.1 or http2)" >&2; exit 2 ;;
esac

if [[ -z "$output_file" ]]; then
  output_file="$(mktemp "${TMPDIR:-/tmp}/nano-rpc-gateway-benchmark.XXXXXX")"
  cleanup_output=true
else
  cleanup_output=false
  : >"$output_file"
fi
trap 'if [[ "$cleanup_output" == true ]]; then rm -f "$output_file"; fi' EXIT

printf 'gateway=%s protocol=%s requests=%s raw=%s\n' "$gateway_url" "$protocol" "$requests" "$output_file"
printf 'health: '
curl "${protocol_args[@]}" "${tls_args[@]}" --fail --silent --show-error "$gateway_url/health"
printf '\n'

for _ in $(seq 1 "$requests"); do
  curl "${protocol_args[@]}" "${tls_args[@]}" --fail --silent --show-error \
    -H 'content-type: application/json' \
    -w '%{time_total}\n' \
    --data "$(jq -cn --arg account "$account" '{jsonrpc:"2.0",method:"account_info",params:{account:$account},id:1}')" \
    "$gateway_url/rpc" -o /dev/null >>"$output_file"
done

awk -v requests="$requests" '
  { values[NR] = $1; sum += $1; sumsq += $1 * $1 }
  END {
    if (NR == 0) { exit 1 }
    for (i = 1; i <= NR; i++) sorted[i] = values[i]
    for (i = 1; i <= NR; i++) for (j = i + 1; j <= NR; j++) if (sorted[j] < sorted[i]) { t = sorted[i]; sorted[i] = sorted[j]; sorted[j] = t }
    p50 = sorted[int((NR + 1) * 0.50)]; p95 = sorted[int((NR + 1) * 0.95)]
    mean = sum / NR; variance = (sumsq / NR) - (mean * mean)
    if (variance < 0) variance = 0
    printf "completed=%d p50_seconds=%.6f p95_seconds=%.6f min_seconds=%.6f max_seconds=%.6f mean_seconds=%.6f stddev_seconds=%.6f\n", NR, p50, p95, sorted[1], sorted[NR], mean, sqrt(variance)
  }
' "$output_file"

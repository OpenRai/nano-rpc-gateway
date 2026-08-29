#!/usr/bin/env bash
set -euo pipefail

compose_file="${COMPOSE_FILE:-docker-compose.dev.yml}"
gateway_url="${GATEWAY_URL:-http://127.0.0.1:8090}"
attempts="${ATTEMPTS:-120}"
requests="${REQUESTS:-20}"
account="${ACCOUNT:-nano_3e3j5tkog48pnny9dmfzj1r16pg8t1e76dz5tmac6iq689wyjfpiij4txtdo}"
tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/nano-rpc-gateway-container.XXXXXX")"
benchmark_raw="${BENCHMARK_OUTPUT_FILE:-$tmp_dir/rpc.raw}"
gateway_id=""
nano_id=""

cleanup() {
  docker compose -f "$compose_file" down >/dev/null 2>&1 || true
  rm -rf "$tmp_dir"
}
trap cleanup EXIT

start_ns="$(date +%s%N)"
docker compose -f "$compose_file" up -d --build
compose_ns="$(date +%s%N)"

healthy=0
for _ in $(seq 1 "$attempts"); do
  if curl --fail --silent "$gateway_url/health" >/dev/null; then
    healthy=1
    break
  fi
  sleep 1
done

if [[ "$healthy" != 1 ]]; then
  echo "gateway did not become healthy" >&2
  docker compose -f "$compose_file" logs --no-color --tail=80 >&2
  exit 1
fi

ready=0
for _ in $(seq 1 "$attempts"); do
  if curl --fail --silent "$gateway_url/readyz" >/dev/null; then
    ready=1
    break
  fi
  sleep 1
done

if [[ "$ready" != 1 ]]; then
  echo "gateway did not become ready" >&2
  docker compose -f "$compose_file" logs --no-color --tail=80 >&2
  exit 1
fi

ready_ns="$(date +%s%N)"
compose_ms=$(( (compose_ns - start_ns) / 1000000 ))
ready_ms=$(( (ready_ns - start_ns) / 1000000 ))
nano_id="$(docker compose -f "$compose_file" ps -q nano)"
gateway_id="$(docker compose -f "$compose_file" ps -q gateway)"

printf 'compose_up_ms=%s ready_ms=%s gateway=%s requests=%s\n' \
  "$compose_ms" "$ready_ms" "$gateway_url" "$requests"
REQUESTS="$requests" ACCOUNT="$account" OUTPUT_FILE="$benchmark_raw" GATEWAY_URL="$gateway_url" \
  PROTOCOL="${PROTOCOL:-http1.1}" ./scripts/benchmark.sh
printf 'container_stats:\n'
docker stats --no-stream --format '{{.Name}}\t{{.MemUsage}}\t{{.CPUPerc}}' \
  "$nano_id" "$gateway_id"
printf 'rpc_raw=%s\n' "$benchmark_raw"

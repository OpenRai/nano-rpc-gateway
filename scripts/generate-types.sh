#!/usr/bin/env bash
set -euo pipefail

# The OpenRPC document is the authority. Keep the generator version pinned so
# a schema release produces reviewable, reproducible TypeScript changes.
schema_url="${1:-http://127.0.0.1:8090/openrpc.json}"
output_dir="${2:-generated}"
mkdir -p "$output_dir"
npx --yes "@open-rpc/generator@2.1.1" generate -t client -l typescript -n nanoNodeClient -d "$schema_url" -o "$output_dir"
generated_client="$output_dir/client/typescript/src/index.ts"
if [[ ! -s "$generated_client" ]]; then
  echo "OpenRPC generator produced no TypeScript client at $generated_client" >&2
  exit 1
fi
published_client="$output_dir/nano-node-v28.2.client.ts"
cp -f "$generated_client" "$published_client"
perl -0pi -e 's/public rpc\.discover:/public ["rpc.discover"]:/g' "$published_client"
perl -pi -e 's/[ \t]+$//' "$published_client"
if grep -q 'public rpc\.discover:' "$published_client"; then
  echo "generated TypeScript client contains an invalid rpc.discover member" >&2
  exit 1
fi
# The generated client imports runtime peer dependencies that belong to its
# consumer, not this Rust project. Use the pinned compiler to reject syntax
# errors in the published artifact without resolving those peer modules.
npx --yes --package typescript@5.9.2 tsc --noEmit --noCheck --target ES2020 --module commonjs "$published_client"

#!/usr/bin/env bash
set -euo pipefail

# The OpenRPC document is the authority. Keep the generator version pinned so
# a schema release produces reviewable, reproducible TypeScript changes.
#
# Generation is offline: the document is the committed OpenRPC artifact, not a
# running gateway, and the generator and compiler come from the pinned
# docs devDependencies. Run `npm --prefix docs ci` once to install them.
repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
schema_document="${1:-$repo_dir/docs/api-docs/openrpc.json}"
output_dir="${2:-$repo_dir/generated}"
generator="$repo_dir/docs/node_modules/.bin/open-rpc-generator"
compiler="$repo_dir/docs/node_modules/.bin/tsc"
if [[ ! -x "$generator" ]]; then
  echo "open-rpc generator is not installed; run: npm --prefix docs ci" >&2
  exit 1
fi
if [[ ! -s "$schema_document" ]]; then
  echo "OpenRPC document not found at $schema_document; run: make contracts" >&2
  exit 1
fi
mkdir -p "$output_dir"
work_dir="$(mktemp -d)"
trap 'rm -rf "$work_dir"' EXIT
"$generator" generate -t client -l typescript -n nanoNodeClient -d "$schema_document" -o "$work_dir"
generated_client="$work_dir/client/typescript/src/index.ts"
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
"$compiler" --noEmit --noCheck --target ES2020 --module commonjs "$published_client"

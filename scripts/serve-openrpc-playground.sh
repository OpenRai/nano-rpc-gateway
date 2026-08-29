#!/usr/bin/env bash
set -euo pipefail

listen_address="${PLAYGROUND_LISTEN:-tcp://127.0.0.1:8080}"
tmpdir="$(mktemp -d "${TMPDIR:-/tmp}/nano-rpc-playground.XXXXXX")"
trap 'rm -rf "$tmpdir"' EXIT INT TERM

# The published Playground package contains the built static site, while its
# historical npm bin entry is not present in every published tarball. Fetch
# that pinned artifact and serve the static dist directory explicitly.
npm pack --silent "@open-rpc/playground@1.1.2" --pack-destination "$tmpdir" >/dev/null
tar -xzf "$tmpdir/open-rpc-playground-1.1.2.tgz" -C "$tmpdir"

exec npx --yes serve@14 "$tmpdir/package/dist" --listen "$listen_address"

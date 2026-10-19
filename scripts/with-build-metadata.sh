#!/usr/bin/env bash
# Resolve authoritative inputs once, then share them with Docker or Compose.
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ $# == 0 ]]; then
  echo 'Usage: scripts/with-build-metadata.sh COMMAND [ARGS...]' >&2
  exit 2
fi
package="$(cargo metadata --manifest-path "$repo_dir/Cargo.toml" --no-deps --locked --format-version 1 | jq -ce '.packages[] | select(.name == "nano-rpc-gateway")')"
NANO_GATEWAY_VERSION="$(jq -er '.version' <<<"$package")"
NANO_GATEWAY_LICENSE="$(jq -er '.license' <<<"$package")"
NANO_GATEWAY_SOURCE="${NANO_GATEWAY_SOURCE:-$(git -C "$repo_dir" remote get-url origin)}"
NANO_GATEWAY_GIT_DESCRIBE="$(git -C "$repo_dir" describe --tags --always --dirty)"
NANO_GATEWAY_GIT_REVISION="$(git -C "$repo_dir" rev-parse HEAD)"
NANO_GATEWAY_GIT_DIRTY=false
if [[ "$NANO_GATEWAY_GIT_DESCRIBE" == *-dirty ]]; then
  NANO_GATEWAY_GIT_DIRTY=true
fi
export NANO_GATEWAY_VERSION NANO_GATEWAY_LICENSE NANO_GATEWAY_SOURCE
export NANO_GATEWAY_GIT_DESCRIBE NANO_GATEWAY_GIT_REVISION NANO_GATEWAY_GIT_DIRTY
exec "$@"

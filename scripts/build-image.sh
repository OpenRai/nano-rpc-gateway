#!/usr/bin/env bash
set -euo pipefail
repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
exec "$repo_dir/scripts/with-build-metadata.sh" docker buildx build \
  --build-arg NANO_GATEWAY_VERSION \
  --build-arg NANO_GATEWAY_GIT_DESCRIBE \
  --build-arg NANO_GATEWAY_GIT_REVISION \
  --build-arg NANO_GATEWAY_GIT_DIRTY \
  --build-arg NANO_GATEWAY_SOURCE \
  --build-arg NANO_GATEWAY_LICENSE \
  "$@" "$repo_dir"

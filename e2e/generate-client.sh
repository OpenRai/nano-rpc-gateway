#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
uv run python generate_client.py

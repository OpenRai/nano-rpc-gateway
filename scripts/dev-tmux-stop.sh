#!/usr/bin/env bash
set -euo pipefail

session="${NANO_RPC_GATEWAY_TMUX_SESSION:-nano-rpc-gateway}"

if ! command -v tmux >/dev/null 2>&1; then
  echo "dev-tmux-stop requires tmux; install it and try again" >&2
  exit 1
fi

if tmux has-session -t "$session" 2>/dev/null; then
  tmux kill-session -t "$session"
  echo "Stopped Nano RPC Gateway tmux session '$session'."
else
  echo "No Nano RPC Gateway tmux session '$session' is running."
fi

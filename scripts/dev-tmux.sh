#!/usr/bin/env bash
set -euo pipefail

session="${NANO_RPC_GATEWAY_TMUX_SESSION:-nano-rpc-gateway}"

if ! command -v tmux >/dev/null 2>&1; then
  echo "dev-tmux requires tmux; install it and try again" >&2
  exit 1
fi

root_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
# Keep the development launcher usable from a fresh checkout. The operator
# config at gateway.yaml is intentionally local-only and commonly contains
# unset credential placeholders; select it explicitly when those credentials
# are configured.
config="${NANO_GATEWAY_CONFIG:-gateway.dev.yaml}"
config_path="$config"
if [[ "$config_path" != /* ]]; then
  config_path="$root_dir/$config_path"
fi

if tmux has-session -t "$session" 2>/dev/null; then
  echo "tmux session '$session' already exists. Attach with: tmux attach -t $session" >&2
  exit 1
fi

echo "tmux reminder: Ctrl-b d detaches without stopping the gateway; stop it with: make dev-tmux-stop"
tmux new-session -d -s "$session" -n gateway -c "$root_dir"

# A long-lived tmux server may predate credentials exported in this shell.
# Give the new pane only variables the chosen gateway config interpolates, then
# remove them from tmux after the child process has inherited its environment.
config_variables=()
if [[ -f "$config_path" ]]; then
  while IFS= read -r variable; do
    if [[ -v "$variable" ]]; then
      tmux set-environment -t "$session" "$variable" "${!variable}"
      config_variables+=("$variable")
    fi
  done < <(grep -oE '\$\{[A-Za-z_][A-Za-z0-9_]*\}' "$config_path" | tr -d '${}' | sort -u || true)
fi

printf -v cargo_config '%q' "$config"
tmux respawn-pane -k -t "$session:gateway.0" \
  "exec cargo run -- serve --config $cargo_config"
for variable in "${config_variables[@]}"; do
  tmux set-environment -t "$session" -u "$variable"
done
gateway_pane="$(tmux display-message -p -t "$session:gateway.0" '#{pane_id}')"

tmux set-window-option -t "$session:gateway" pane-border-status top
tmux set-window-option -t "$session:gateway" pane-border-format ' #[fg=cyan]#{pane_title}#[default] | #{pane_current_command} '
tmux set-window-option -t "$session:gateway" pane-border-style 'fg=colour244'
tmux set-window-option -t "$session:gateway" pane-active-border-style 'fg=cyan'
tmux select-pane -t "$gateway_pane" -T 'GATEWAY'

echo "Started gateway in tmux session '$session' using $config."
echo "Attach later with: tmux attach -t $session"
echo "Stop it with: make dev-tmux-stop"
if [[ -t 0 && -t 1 ]]; then
  exec tmux attach-session -t "$session"
fi

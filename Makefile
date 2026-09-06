.PHONY: check test clippy build generate-types contracts docs-install docs-validate docs-generate docs-check docs-model-smoke docs-preview require-nano-rpc gateway playground gateway-playground benchmark resource-benchmark container-benchmark transport-smoke devnet-up devnet-smoke devnet-down dev-tmux dev-tmux-start dev-tmux-stop
check:
	cargo check --locked
test:
	cargo test --locked
clippy:
	cargo clippy --all-targets --all-features --locked -- -D warnings
build:
	cargo build --release --locked
generate-types:
	./scripts/generate-types.sh "$${OPENRPC_SCHEMA_URL:-http://127.0.0.1:8090/openrpc.json}" generated
contracts:
	cargo run --quiet -- contracts --output-dir docs/api-docs
	cp -f docs/api-docs/openrpc.json generated/nano-node-v28.2.openrpc.json
docs-install:
	npm --prefix docs ci --ignore-scripts
docs-validate: docs-install docs-generate
	npm --prefix docs run validate
docs-generate: docs-install
	npm --prefix docs run generate
docs-check: docs-install
	npm --prefix docs run check
docs-model-smoke: docs-install docs-generate
	npm --prefix docs run models
docs-preview: docs-install docs-generate
	npm --prefix docs run preview
require-nano-rpc:
	@test -n "$${NANO_RPC_URL:-}" || (echo 'NANO_RPC_URL is required, for example NANO_RPC_URL=http://127.0.0.1:7076' >&2; exit 2)
gateway: require-nano-rpc build
	NANO_RPC_URL="$${NANO_RPC_URL:-}" NANO_WS_URL="$${NANO_WS_URL:-}" ./scripts/run-local-gateway.sh
dev-tmux:
	./scripts/dev-tmux.sh
dev-tmux-start: dev-tmux
dev-tmux-stop:
	./scripts/dev-tmux-stop.sh
playground:
	cargo run -- playground --gateway-url "$${GATEWAY_URL:-http://127.0.0.1:8090/rpc}" --serve --launch
gateway-playground: require-nano-rpc build
	NANO_RPC_URL="$${NANO_RPC_URL:-}" NANO_WS_URL="$${NANO_WS_URL:-}" ./scripts/run-local-gateway-playground.sh
benchmark:
	./scripts/benchmark.sh
resource-benchmark:
	./scripts/resource-benchmark.sh
container-benchmark:
	./scripts/container-benchmark.sh
transport-smoke:
	./scripts/transport-smoke.sh
devnet-up:
	docker compose -f docker-compose.dev.yml up -d --build
devnet-smoke:
	START_DEVNET=1 ./scripts/devnet-smoke.sh
devnet-down:
	docker compose -f docker-compose.dev.yml down

.PHONY: check test clippy build require-nano-rpc gateway playground gateway-playground benchmark resource-benchmark container-benchmark transport-smoke devnet-up devnet-smoke devnet-down
check:
	cargo check --locked
test:
	cargo test --locked
clippy:
	cargo clippy --all-targets --all-features --locked -- -D warnings
build:
	cargo build --release --locked
require-nano-rpc:
	@test -n "$${NANO_RPC_URL:-}" || (echo 'NANO_RPC_URL is required, for example NANO_RPC_URL=http://127.0.0.1:7076' >&2; exit 2)
gateway: require-nano-rpc build
	NANO_RPC_URL="$${NANO_RPC_URL:-}" NANO_WS_URL="$${NANO_WS_URL:-}" ./scripts/run-local-gateway.sh
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

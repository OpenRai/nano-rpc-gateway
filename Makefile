.PHONY: check test clippy playground benchmark resource-benchmark transport-smoke devnet-up devnet-smoke devnet-down
check:
	cargo check --locked
test:
	cargo test --locked
clippy:
	cargo clippy --all-targets --all-features --locked -- -D warnings
playground:
	cargo run -- playground --gateway-url "$${GATEWAY_URL:-http://127.0.0.1:8090/rpc}" --serve --launch
benchmark:
	./scripts/benchmark.sh
resource-benchmark:
	./scripts/resource-benchmark.sh
transport-smoke:
	./scripts/transport-smoke.sh
devnet-up:
	docker compose -f docker-compose.dev.yml up -d --build
devnet-smoke:
	START_DEVNET=1 ./scripts/devnet-smoke.sh
devnet-down:
	docker compose -f docker-compose.dev.yml down

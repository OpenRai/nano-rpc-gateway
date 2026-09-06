# Nano RPC Gateway

Nano RPC Gateway is an exploratory sidecar that aims to present a modern,
discoverable integration surface in front of an unmodified Nano or RsNano
node.

The proposed external surface combines:

- JSON-RPC 2.0 for commands and queries;
- OpenRPC discovery for tools and generated clients; and
- Server-Sent Events (SSE) for filtered node event streams.

The gateway translates these interfaces to the node's native action-based RPC
and WebSocket interfaces. The v0.1 implementation accepts JSON-RPC 2.0 at
`POST /rpc`, serves callable-method discovery at `/openrpc.json`, serves the
receive-only SSE contract at `/asyncapi.json`, and emits confirmations at
`/events/confirmations`. Every SSE `data:` value is a JSON-RPC 2.0 notification;
its `method` matches the SSE `event:` name and it has no `id` member.

The Clean V28.2 profile is authority-reviewed in
[SCHEMA_POLICY.md](SCHEMA_POLICY.md), exposes a stable artifact digest, and
ships generated TypeScript bindings in [generated/](generated/). Regenerate
bindings with `make generate-types` against the reviewed schema endpoint.

The committed reference in [docs/api-docs/](docs/api-docs/) combines RPC and Events
tabs, global search, deep links, and raw OpenRPC/AsyncAPI downloads. Run
`make docs-install`, then `make docs-generate`, `make docs-validate`, or
`make docs-preview`. `make docs-model-smoke` proves the AsyncAPI contract can
produce typed event models without shipping a subscriber library from this
gateway.

Run `cargo run -- serve`; a missing `gateway.yaml` is created with safe
defaults (`127.0.0.1:8090` for the gateway, leaving Nano's conventional native
RPC port `7076` available). Base reads are unauthenticated, while work
generation is disabled until a PASETO public key and explicit work policy are
configured. Common methods, including `process`, require PASETO by default;
set `require_common_auth: false` for an intentionally public Common profile.

Upstream selection and failure events are always logged with a redacted
`upstream_url`, for both RPC and WebSocket transports. Detailed JSON-RPC and SSE
lifecycle diagnostics remain opt-in: set `log_rpc: true` in the config or run
`cargo run -- --log-rpc serve`. These detailed logs include request receipt,
upstream status/duration, and subscription filter counts, but never request
parameters, account/hash values, tokens, or provider credentials.

For local inspection, enable `enable_inspector` in the gateway config and open
`/inspector/`. The embedded inspector is dynamically pointed at the gateway's
own `/openrpc.json` and `/rpc` endpoints. It is disabled by default;
external OpenRPC tools can still consume the same schema.

Operational deployment and isolation guidance is in [OPERATIONS.md](OPERATIONS.md);
the reproducible request harness is [scripts/benchmark.sh](scripts/benchmark.sh),
with the first local-only result in [benchmarks/v0.1-loopback.md](benchmarks/v0.1-loopback.md).
Current compatibility evidence is recorded in [compatibility/v0.1.md](compatibility/v0.1.md).
For gateway-only startup and idle-RSS sampling, use `make resource-benchmark`;
the measured local result is in [benchmarks/v0.1-resource.md](benchmarks/v0.1-resource.md).
On a Docker-capable host, `make container-benchmark` runs the same checks beside
the pinned dev node and records container memory/CPU plus RPC samples; set
`BENCHMARK_OUTPUT_FILE` to retain the raw request timings.

On a host with Docker, `make devnet-smoke` pulls the pinned official
`nanocurrency/nano:V28.2` image, starts it with the runtime `--network dev`
option and the Dev-network selector file, then starts the gateway, checks the
public RPC/OpenRPC/SSE paths, and tears the stack down when the smoke run exits. Use
`make devnet-up` and
`./scripts/devnet-smoke.sh` when you need to keep the stack running between
checks.

Read [DESIGN.md](DESIGN.md) for the complete design, evidence boundaries, open
questions, and proposed implementation sequence.

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
`POST /rpc`, serves `/openrpc.json`, and emits confirmations at
`/events/confirmations`.

Run `cargo run -- serve`; a missing `gateway.yaml` is created with safe
defaults (`127.0.0.1:8090` for the gateway, leaving Nano's conventional native
RPC port `7076` available). Base reads are unauthenticated, while work
generation is disabled until a PASETO public key and explicit work policy are
configured.

For the shortest local path, set the Nano node's native RPC URL and run
`NANO_RPC_URL=http://127.0.0.1:7076 make gateway-playground`. This builds the
release gateway, starts it against that backend, serves the pinned stock
OpenRPC Playground on `127.0.0.1:8080`, prints the preconfigured Playground
URL, and opens it on macOS. When the node uses a nonstandard WebSocket URL, add
`NANO_WS_URL=ws://host:port`; the conventional `http(s)://host:7076` URL is
mapped to `ws(s)://host:7078` automatically. Press Ctrl-C to stop both
processes. Use `NANO_RPC_URL=... make gateway` when you only want the gateway.

Run `make playground` to serve and open the stock `@open-rpc/playground`
against the local gateway and schema. Playground is a development companion,
not part of the gateway process or production image; omit `--serve` to print a
hosted Playground URL instead.

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

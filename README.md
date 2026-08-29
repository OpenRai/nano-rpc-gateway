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

Run `make playground` to serve and open the stock `@open-rpc/playground`
against the local gateway and schema. Playground is a development companion,
not part of the gateway process or production image; omit `--serve` to print a
hosted Playground URL instead.

Operational deployment and isolation guidance is in [OPERATIONS.md](OPERATIONS.md);
the reproducible request harness is [scripts/benchmark.sh](scripts/benchmark.sh),
with the first local-only result in [benchmarks/v0.1-loopback.md](benchmarks/v0.1-loopback.md).
Current compatibility evidence is recorded in [compatibility/v0.1.md](compatibility/v0.1.md).

Read [DESIGN.md](DESIGN.md) for the complete design, evidence boundaries, open
questions, and proposed implementation sequence.

# Nano RPC Gateway

Nano RPC Gateway is an exploratory sidecar that aims to present a modern,
discoverable integration surface in front of an unmodified Nano or RsNano
node.

The proposed external surface combines:

- JSON-RPC 2.0 for commands and queries;
- OpenRPC discovery for tools and generated clients; and
- Server-Sent Events (SSE) for filtered node event streams.

The gateway translates these interfaces to the node's native action-based RPC
and WebSocket interfaces. It is not yet implemented. No interface in this
repository is stable.

Read [DESIGN.md](DESIGN.md) for the complete design, evidence boundaries, open
questions, and proposed implementation sequence.

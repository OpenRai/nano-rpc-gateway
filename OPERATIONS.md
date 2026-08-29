# Operating the v0.1 gateway

The gateway is a sidecar. Keep the Nano node's native HTTP RPC and WebSocket
listeners on loopback or an isolated network, and expose only the gateway
listener to integrators.

The `/metrics` endpoint is deliberately small and credential-free. It exposes
`nano_gateway_up`, `nano_gateway_upstream_ready`, request/error totals,
request-duration sum/count, active SSE streams, replay hit/miss/reset totals,
bounded-overflow resets, and upstream reconnect totals. These counters describe
the gateway process; they are not Nano ledger or consensus metrics. No account,
hash, request ID, token, or raw error text is used as a metric label.

1. Copy `gateway.yaml.example` to the mounted configuration directory.
2. Set `node_rpc_url` and `node_ws_url` to the private node listeners.
3. Leave `allow_work` and `allow_control` disabled unless the corresponding
   policy and PASETO verification key have been reviewed.
4. Start `nano-rpc-gateway serve --config /etc/nano-rpc-gateway/gateway.yaml`.
5. Check `/health`, `/readyz`, and `/metrics` before routing traffic. `/health`
   is process liveness; `/readyz` returns 200 only after the native
   confirmation subscription is connected and returns 503 while it is down or
   reconnecting. The metrics endpoint exposes the same state as
   `nano_gateway_upstream_ready`.

For TLS, provide both `tls_cert` and `tls_key`. When either is absent the
development server uses HTTP; terminate TLS at a trusted reverse proxy for
production if certificate management is external to the sidecar.

`/events/confirmations` is bounded gateway-local replay, not durable storage.
After `nano.stream_reset`, reconcile state with JSON-RPC before applying new
events. Rotate PASETO signing keys by replacing the configured verification
key and issuing short-lived tokens; the gateway stores no token revocation DB.

For a local developer UI, run `make playground`. This serves the pinned stock
OpenRPC Playground package on loopback:8080 and prints/opens a URL targeting
the gateway's `/openrpc.json`; it is not included in the gateway image.

For a disposable compatibility smoke on a Docker host, run `make devnet-smoke`.
It builds the pinned V28.2 source with `NANO_NETWORK=dev`, starts the node and
gateway, verifies account lookup, process error translation, SSE headers, and
native WebSocket readiness, then removes the containers. The native node ports
are loopback-mapped for development and are not the public gateway interface.

`make resource-benchmark` measures release-binary startup and idle RSS with
unreachable upstreams. Treat it as gateway-only data; it says nothing about
Nano-node memory, container overhead, or SSE event fan-out.

`make container-benchmark` is the Docker-capable counterpart: it builds the
development stack, waits for gateway readiness, runs the RPC benchmark, and
prints one `docker stats` sample for the node and gateway before teardown.

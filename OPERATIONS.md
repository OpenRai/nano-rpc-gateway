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
2. Set parallel `node_rpc_urls` and `node_ws_urls` lists to the private node listeners.
   Public providers can be used when their terms and rate limits are suitable:
   Nanswap accepts `https://nodes.nanswap.com/XNO?api_key=${NANSWAP_COM_API_KEY}` and
   `wss://nodes.nanswap.com/ws/?ticker=XNO&api_key=${NANSWAP_COM_API_KEY}`;
   Nano.to accepts `https://${NANO_TO_API_KEY}:@rpc.nano.to/` for HTTP and
   `wss://ws.nano.to` for WebSocket. The gateway loads an optional `.env` beside
   the chosen configuration file, expands `${NAME}` values before parsing YAML,
   and lets explicitly exported environment variables take precedence.
   The gateway strips Nano.to URL userinfo before issuing the request and sends
   it as empty-password Basic authentication. Keep these key-bearing values in
   a deployment secret or untracked local config, never in a committed example.
3. Leave `allow_work` and `allow_control` disabled unless the corresponding
   policy and PASETO verification key have been reviewed. Set
   `require_common_auth: false` only when the public Common profile is
   intentional; this makes `process` callable without a token.
   Upstream selection and failure events are always emitted for RPC and
   WebSocket transports. They include a redacted `upstream_url` so failover,
   disconnect, and upstream-error records identify the affected provider.
   For temporary request diagnostics, set `log_rpc: true` or pass `--log-rpc`
   to `serve`. This additionally emits structured `rpc_request`,
   `rpc_upstream_response`, `sse_session_created`, `sse_session_closed`,
   `upstream_ws_connecting`, and `upstream_ws_connected` events. These detailed
   events do not log request
   parameters, account or hash values, tokens, or upstream credentials. Keep
   detailed diagnostics disabled for normal production operation.
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

For live RPC testing, use the OpenRPC Playground link in the generated
reference and configure its endpoint from the published schema. Use AsyncAPI
Studio to inspect the receive-only event contract. The gateway exposes the
machine-readable contracts and runtime endpoints; it does not embed a custom
inspector UI.

For a disposable compatibility smoke on a Docker host, run `make devnet-smoke`.
It pulls the pinned official V28.2 image, starts it with the runtime Dev-network
selection, starts the gateway, verifies account lookup, process error
translation, SSE headers, and native WebSocket readiness, then removes the
containers. The native node ports are loopback-mapped for development and are
not the public gateway interface.

`make resource-benchmark` measures release-binary startup and idle RSS with
unreachable upstreams. Treat it as gateway-only data; it says nothing about
Nano-node memory, container overhead, or SSE event fan-out.

`make container-benchmark` is the Docker-capable counterpart: it builds the
development stack, waits for gateway readiness, runs the RPC benchmark, and
prints one `docker stats` sample for the node and gateway before teardown.

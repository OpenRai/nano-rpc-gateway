# Operating the v0.1 gateway

The gateway is a sidecar. Keep the Nano node's custom JSON RPC HTTP interface
and WebSocket listeners on loopback or an isolated network. Expose only the
gateway listener to integrators. Integrators call the gateway with JSON-RPC 2.0.

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
   Nano.to accepts `https://${GATEWAY_RPC_UPSTREAM_API_KEY}:@rpc.nano.to/` for HTTP and
   `wss://ws.nano.to` for WebSocket. The API key is supplied through generic
   configuration interpolation in the URL; there is no provider-specific gateway
   setting. The gateway loads an optional `.env` beside
   the chosen configuration file, expands `${NAME}` values before parsing YAML,
   and lets explicitly exported environment variables take precedence.
   Expansion is textual over the whole file, so a `${NAME}` inside a comment is
   expanded too, and an unset variable is a hard startup error rather than an
   empty string. Comment out a placeholder instead of naming an undefined one.
   The gateway strips URL userinfo before issuing the request and sends the
   username as Basic authentication with an empty password. Keep key-bearing values in
   a deployment secret or untracked local config, never in a committed example.
   The authenticated Nano.to `work_generate` endpoint has been observed returning
   `frontier` instead of the `hash` field shown in its current docs and required by
   Nano RPC V28. The gateway renames `frontier` to `hash` only when it exactly matches
   the requested work root, and logs that compatibility remap. A mismatched value is
   still rejected. Continue to ask Nano.to to make the live response match its docs.
3. Leave `allow_work` and `allow_control` disabled unless the corresponding
   policy and PASETO verification key have been reviewed. These flags enable
   Work-scope `work_generate` and Control-scope `work_cancel`; `work_validate`
   follows the Common-method authentication policy. Set
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
4. Set `public_url` to the externally reachable JSON-RPC endpoint whenever a
   reverse proxy or tunnel fronts the gateway. It is what `/openrpc.json` and
   `/asyncapi.json` advertise as `servers`, so published contracts, generated
   clients, and the hosted OpenRPC and AsyncAPI tools resolve the reachable
   address. It must be an absolute `http`/`https` URL, and it is independent of
   `listen`, so TLS may terminate upstream of the gateway. Left null, the
   advertised URL is derived from `listen` and the local TLS setting, which is
   correct only for a directly reachable loopback listener.
5. Review `cors_origins`. The default `*` is deliberate: CORS is enforced by
   browsers, and every endpoint is already callable by any non-browser client,
   so the allowlist is not an access-control boundary. It exists to stop a page
   on an unrelated origin from using a visitor's browser as a relay. Narrow it
   to the known web origins for a deployment that has them, and expect a
   browser to report a bare CORS failure with a `200` status when an origin is
   missing — the status code alone does not indicate success. A wildcard is
   incompatible with cookie credentials; `Authorization`-header tokens are
   unaffected, so PASETO authentication keeps working under `*`.
6. Start `nano-rpc-gateway serve --config /etc/nano-rpc-gateway/gateway.yaml`.
7. Check `/health`, `/readyz`, and `/metrics` before routing traffic. `/health`
   is process liveness; `/readyz` returns 200 only after the native
   confirmation subscription is connected and returns 503 while it is down or
   reconnecting. The metrics endpoint exposes the same state as
   `nano_gateway_upstream_ready`.

For TLS, provide both `tls_cert` and `tls_key`. When either is absent the
development server uses HTTP; terminate TLS at a trusted reverse proxy for
production if certificate management is external to the sidecar. When TLS
terminates upstream, set `public_url` to the `https` address so the published
contracts do not advertise the loopback listener.

`/events/confirmations` is a live, non-replayable stream. When it closes,
open a replacement stream, buffer its notifications, and reconcile state with
`accounts_frontiers` and `account_history` before applying buffered or new events.
Each stream belongs to one acknowledged upstream WebSocket session. Every loss
of that session closes its streams, including heartbeat and confirmation-delivery
timeouts; a fast failover does not preserve the old streams across the gap.
This follows the node's disconnect and reconciliation semantics:
<https://docs.nano.org/integration-guides/websockets/>.

While no upstream subscription is acknowledged, valid GET and POST stream
requests return HTTP 503 with the same JSON body as `/readyz`, `Retry-After: 2`,
and `Cache-Control: no-store`. Retry with backoff; these requests do not alter
the background WS provider rotation or cooldowns. A native browser `EventSource`
stops reconnecting after a non-200 response, so its owner must recreate it after
an error with `readyState === EventSource.CLOSED`. Fetch-based clients must
likewise retry non-200 responses and reconcile after an interruption. A response
that was already streaming ends without an invented error event or replay cursor.
With `log_rpc` enabled, `generation` links stream creation/closure to the upstream
session and `close_reason` distinguishes upstream loss, subscriber lag, and client
disconnects.

For a controlled deployment check, build the optional `ws-fault-relay` Docker
target and run it privately on the gateway's Compose network. Pass the original
WS URLs as repeated `--upstream` arguments, and temporarily replace them in a
copy of the gateway configuration with `ws://nano-as3-ws-relay:8080/0`, `/1`, etc.
Do not publish relay ports: port 8081 has unauthenticated test-only pause/resume
controls. Keep the original configuration available for restoration.

Run `python3 scripts/verify-sse-continuity.py --help` for the deployed verification
arguments. The script requires `curl`, SSH to the Docker host, and `xno-skills`;
it sends exactly three transfers of 0.00000001 XNO between the supplied local
agent wallets. It verifies public HTTP/1.1 and HTTP/2 delivery, stream termination
during a five-second relay outage, retryable 503s, unaffected RPC, recovery, and
receive confirmations. Local PoW time is reported separately from SSE latency.
If a run stopped after its first send was verified, `--resume-send-hash` reuses
that send instead of spending the amount again; retain the original test log.
Afterward restore the original WS configuration, recreate only the gateway, and
remove the private relay. The default gateway image contains no relay executable.

Rotate PASETO signing
keys by replacing the configured verification key and issuing short-lived
tokens; the gateway stores no token revocation DB.

For live JSON-RPC testing, use the OpenRPC Playground link in the generated
reference. It loads the gateway's OpenRPC schema and sends JSON-RPC 2.0 requests.
Use AsyncAPI Studio to inspect the receive-only event contract. The gateway exposes the
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

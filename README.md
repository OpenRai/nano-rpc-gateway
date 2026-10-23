# Nano Gateway

This sidecar gives clients a discoverable API in front of an unmodified Nano
or RsNano node. Clients call the gateway with JSON-RPC 2.0. For ledger methods,
the gateway translates those calls into the Nano node's custom JSON RPC
actions. The gateway reads confirmation events from the node's WebSocket
interface and publishes them through SSE.

The gateway exposes a JSON-RPC 2.0 API, an OpenRPC schema for that API, and an SSE event stream:

- JSON-RPC 2.0 for commands and queries;
- OpenRPC discovery for tools and generated clients; and
- Server-Sent Events (SSE) for filtered node event streams.

The gateway accepts JSON-RPC 2.0 requests at `POST /rpc`. `/openrpc.json`
describes that API. Nano nodes use a separate custom JSON RPC interface
upstream; the gateway translates ledger methods between the two protocols.
The gateway publishes node confirmations through SSE at
`/events/confirmations`; `/asyncapi.json` describes that event stream. Every SSE
`data:` value is a JSON-RPC 2.0 notification. Its `method` matches the SSE
`event:` name, and it has no `id` member.

The Clean V28.2 profile is authority-reviewed in
[SCHEMA_POLICY.md](SCHEMA_POLICY.md), exposes a stable artifact digest, and
ships generated TypeScript bindings in [generated/](generated/). They are
regenerated offline from the reviewed OpenRPC document with `make client`
(after `make docs-install`); `make docs-check` fails when they drift.

The committed reference in [docs/api-docs/](docs/api-docs/) combines gateway
JSON-RPC 2.0 and SSE tabs, global search, deep links, and raw OpenRPC/AsyncAPI
downloads. Run
`make docs-install`, then `make docs-generate`, `make docs-validate`, or
`make docs-preview`. `make docs-model-smoke` proves the AsyncAPI contract can
produce typed event models without shipping a subscriber library from this
gateway. The reference links to the hosted OpenRPC Playground and AsyncAPI
Studio; those tools load the published JSON contracts and handle live
exploration separately from this site.

Browser pages are disabled by default. Set `enable_browser_ui: true` to serve
the landing page at `/api-landing-page`, the reference at `/docs/`, and
redirect `/` to the landing page for requests that accept HTML. The hosted
viewers fetch schemas and make JSON-RPC 2.0 requests directly to the gateway,
so deployments with restricted `cors_origins` must allow
`https://playground.open-rpc.org` and `https://studio.asyncapi.com`.

Run `cargo run -- serve`; a missing `gateway.yaml` is created with safe
defaults (`127.0.0.1:8090` for the gateway, leaving Nano node's custom JSON RPC
port `7076` available). Base reads are unauthenticated, while work
generation is disabled until a PASETO public key and explicit work policy are
configured. Common methods, including `process`, require PASETO by default;
set `require_common_auth: false` for an intentionally public Common profile.

Token issuance defaults to a one-hour lifetime (`issue --ttl 3600`). For a trusted
service deployment, `issue --secret '<operator signing key>' --scope work
--no-expiry --subject nanosweeper-worker` issues a token with no `exp` claim.
`--no-expiry` and `--ttl` cannot be combined. Signature, audience, and scope checks
still apply. Revoke a non-expiring token by rotating the gateway signing key;
this revokes every token signed by the old key and requires replacing them.

Browser CORS defaults to `*`. The gateway is a public read surface and CORS is
browser-enforced rather than access control, since non-browser clients already
reach every endpoint regardless of the allowlist. Narrow `cors_origins` to the
known web origins for a deployment that has them; a wildcard does not weaken
`Authorization`-header authentication.

`public_url` declares the externally reachable JSON-RPC endpoint and is what
`/openrpc.json` and `/asyncapi.json` advertise under `servers`. Set it whenever
a reverse proxy or tunnel fronts the gateway, including when TLS terminates
upstream of it. Left unset, the advertised URL is derived from the local bind
address, which is correct only for a directly reachable listener.

For the tmux development launcher, use `make tmux-preview`. It starts with the
local credential-free `gateway.preview.yaml` (created from built-in safe defaults);
select a local credentialed
configuration explicitly with `NANO_GATEWAY_CONFIG=gateway.yaml make tmux-preview`.

Work generation is independently configured with `work_provider`. Use `local`
to generate with the embedded GPU-first `nano-rspow` provider, or `remote` to
forward work to `work_peer_urls`. An empty peer list follows `node_rpc_urls`.
The development deployment uses remote work peers; local preview uses local work.
In local mode, Nano requests that explicitly select peers or ask the node to derive
a block threshold use the work-peer pool. A provider error never triggers a switch
between local and remote modes.

Upstream selection and failure events are always logged with a redacted
`upstream_url`, for both RPC and WebSocket transports. Detailed JSON-RPC and SSE
lifecycle diagnostics remain opt-in: set `log_rpc: true` in the config or run
`cargo run -- --log-rpc serve`. These detailed logs include request receipt,
upstream status/duration, and subscription filter counts, but never request
parameters, account/hash values, tokens, or provider credentials.

For live JSON-RPC testing, open the OpenRPC Playground link in the generated
reference. It loads the gateway's OpenRPC schema and sends JSON-RPC 2.0
requests to the gateway. Open the AsyncAPI Studio link to inspect the
confirmation-event contract. The gateway serves both schemas and event streams.

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

## Build identity

`Cargo.toml` owns the application release number. Executables, OpenRPC and AsyncAPI
`info.version`, user agents, and OCI version metadata derive it automatically.
Git adds source provenance; the image digest identifies the exact package,
including its platform and base image. Rebuilding the same source against a newer
base image can change the digest without changing the application release.

```sh
cargo run -- --version
cargo run -- build-info
cargo run --example ws_fault_relay -- --version
cargo run --example ws_fault_relay -- --build-info
```

The JSON-RPC `version` method reports the upstream Nano node's version.

The display stamp combines the release and `git describe --tags --always --dirty`.
A matching clean release tag is displayed once. `build-info` retains the full
commit SHA, Git description, and dirty status as separate fields. As with Git's
`--dirty`, untracked files are excluded; a dirty stamp does not identify the exact
modified bytes. Source archives without Git report unknown provenance. Runtime
environment variables cannot change the identity embedded in an executable.

When browser UI is enabled, `/api-landing-page` includes that same stamp directly
in its HTML, alongside the configured Nano profile. No script or JSON-RPC request
is needed. The startup banner and JSON startup logs also report the build.
Generated contracts contain the release version, but no per-build Git metadata.

OCI builds require host metadata because `.git` is excluded from their context.
The wrappers need Cargo, Git, jq, and Docker; they read the release and license
through Cargo metadata and capture source provenance before invoking Docker:

```sh
./scripts/build-image.sh --load --tag nano-rpc-gateway:local
./scripts/build-image.sh --load --target ws-fault-relay --tag ws-fault-relay:local
./scripts/with-build-metadata.sh docker compose -f docker-compose.dev.yml up -d --build
# make devnet-up, make devnet-smoke, and make container-benchmark use the helper.
docker run --rm --entrypoint /usr/local/bin/nano-rpc-gateway nano-rpc-gateway:local build-info
docker image inspect nano-rpc-gateway:local --format '{{json .Config.Labels}}'
docker image inspect nano-rpc-gateway:local --format '{{json .RepoDigests}}'
```

Both images carry standard OCI `version`, `revision`, `source`, and `licenses`
labels, plus `com.github.openrai.nano-rpc-gateway.source.dirty` and
`com.github.openrai.nano-rpc-gateway.source.description`. The source URL defaults
to the Git origin; set `NANO_GATEWAY_SOURCE` to a public repository URL when needed.
Raw Dockerfile builds must supply the six `NANO_GATEWAY_*` build arguments listed
in `scripts/build-image.sh`; missing metadata or a release differing from Cargo
fails the build. The Dockerfile makes no independent release-number assignment.

For releases, use a clean checkout with complete tags, verify a `v<package.version>`
tag, and tag the image with that release. Deploy using the published digest rather
than a mutable tag. Publishing can use the same wrapper with
`--push --provenance=mode=max --sbom=true --tag REGISTRY/IMAGE:RELEASE` instead of
`--load`; provenance and SBOM attestations describe how the package was built and
what it contains. Loading attestations locally requires a compatible containerd
image store. No publishing workflow is installed by this change. See the
[OCI metadata definitions](https://github.com/opencontainers/image-spec/blob/main/annotations.md)
and [Docker attestation documentation](https://docs.docker.com/build/metadata/attestations/).

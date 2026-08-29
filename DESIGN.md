# Nano RPC Gateway: exploratory software design

Status: working design; the v0.1 sidecar implementation is present, but its
external contract remains subject to the evidence and compatibility gates below

Last revised: 2026-08-29

Primary readers: implementers, integrators, reviewers, and coding agents

Current phase: v0.1 implementation and evidence collection

This document is the source of truth for the current design exploration. Every
part remains open to refinement when implementation, protocol inspection, or
tests produce better evidence. A statement marked **proposed** describes the
current preferred direction. A statement marked **required** records a design
constraint from the discussion, but can still change through an explicit
decision record.

This is not a committee specification. The project will advance through narrow,
working vertical slices. Each slice must improve developer experience, expose
new evidence, and leave the design easier to revise.

## 1. Read this document progressively

Readers do not need to consume the entire document before acting.

1. Read sections 2–5 to understand the purpose, boundaries, and proposed shape.
2. Read sections 6–9 before designing a public interface.
3. Read sections 10–14 before implementing translation or event delivery.
4. Read sections 15–19 before planning production deployment.
5. Read sections 20–24 before claiming compatibility or completing a milestone.
6. Use the appendices for provenance, terminology, and unresolved research.

## 2. One-minute orientation

Nano RPC Gateway is a standalone, sidecar-deployable integration gateway for
Nano nodes. It aims to give modern clients one coherent HTTP surface:

- standards-conforming JSON-RPC 2.0 requests and responses;
- standard `rpc.discover` discovery backed by an OpenRPC document;
- filtered Server-Sent Events for node notifications;
- shared authentication, errors, versioning, observability, and compatibility
  metadata; and
- explicit profiles for differences between Nano, RsNano, and hosted providers.

The gateway will connect to existing node interfaces instead of requiring node
changes:

```text
Integrator or developer
  ├── JSON-RPC 2.0 over HTTP
  ├── rpc.discover / OpenRPC
  └── Server-Sent Events
                 │
          Nano RPC Gateway
                 │
  ├── native Nano action RPC over HTTP or IPC
  └── native Nano WebSocket subscriptions
                 │
          Nano or RsNano node
```

The gateway is not a byte-preserving proxy. It is a protocol adapter with a
normalized public contract. That distinction is essential because native Nano
RPC does not use the JSON-RPC 2.0 envelope.

## 3. Guiding intent

The project starts from two equally important developer experiences.

### 3.1 Core developer experience

Core developers should gain:

- executable descriptions of current node behavior;
- visible compatibility differences between implementations and releases;
- bounded, deterministic conformance scenarios;
- localized translation logic instead of changes spread across clients;
- clear evidence when documentation, source, tests, and deployed behavior
  disagree; and
- a place to experiment without forcing premature changes into node cores.

### 3.2 Integrator developer experience

Integrators should gain:

- a familiar JSON-RPC 2.0 request model;
- runtime discovery through `rpc.discover`;
- machine-readable request, result, and error shapes;
- generated or validated clients where the schema evidence supports them;
- browser-friendly, HTTP-native event delivery through SSE;
- consistent authentication and diagnostics; and
- explicit recovery behavior after disconnects or event loss.

Neither experience is subordinate to the other. A pleasant façade that hides
incorrect Nano semantics is a failure. A precise compatibility laboratory that
remains unpleasant to integrate is also a failure.

## 4. Problem statement

Nano exposes a mature action-based RPC interface and a separate WebSocket event
interface. These interfaces work, but they leave integrators to discover
methods through documentation, interpret loosely structured errors, maintain
transport-specific code, and reconstruct recovery behavior.

The project will determine whether one deployable gateway can preserve Nano
semantics while presenting a discoverable and internally consistent integration
surface. The gateway must make translation limits visible instead of claiming
false equivalence.

## 5. Current design position

The current recommendation is a single gateway with two public interfaces that
share one internal Nano translation boundary.

```text
                         Public seam
          ┌────────────────────────────────┐
          │ JSON-RPC adapter   SSE adapter │
          └───────────┬────────────┬───────┘
                      │            │
              operation model  event model
                      │            │
          ┌───────────┴────────────┴───────┐
          │ node RPC adapter  node WS adapter │
          └────────────────────────────────┘
                         Node seam
```

The translation boundary turns native Nano HTTP replies and WebSocket messages
into the gateway's public method results, errors, and events. Both JSON-RPC
calls and SSE updates use it, so they do not acquire separate meanings for the
same error, confirmation, overflow, or reconnect. The HTTP-facing adapters can
change without changing those public meanings.

## 6. Scope

### 6.1 Proposed initial scope

- Run as a separate process or container beside a Nano-compatible node.
- Connect through configured HTTP RPC, IPC where feasible, and WebSocket URLs.
- Expose JSON-RPC 2.0 over HTTP.
- Optionally implement standard `rpc.discover` on the JSON-RPC endpoint.
- Publish an OpenRPC document derived from reviewed contracts.
- Expose filtered event streams through SSE.
- Normalize a small wallet- and integrator-relevant method set.
- Preserve provider-specific additions through named profiles or extensions.
- Emit explicit overflow, reset, and reconciliation signals.
- Provide health, readiness, metrics, structured logs, and safe diagnostics.
- Test Nano and RsNano against the same deterministic scenarios.

### 6.2 Explicitly outside the first slice

- Full coverage of every Nano RPC action.
- Replacement or deprecation of node-native interfaces.
- Changes to Nano or RsNano core repositories.
- A public multi-tenant hosted gateway.
- Durable event retention across process restarts.
- Transparent support for every node administration or wallet-control action.
- JSON-RPC batching before partial-failure and resource-limit semantics exist.
- Automatic schema generation treated as normative without review.
- Claims that SSE provides durable delivery.
- Claims that the gateway is a transparent one-to-one proxy.

### 6.3 Candidate first vertical slice

The first useful slice should cover:

- `account_info`;
- `process`;
- `work_generate`;
- confirmation events over SSE;
- disconnect, reconnect, reset, and state reconciliation;
- `rpc.discover`; and
- one compatibility report covering Nano and RsNano.

This slice is intentionally small but must support a real client flow. A plumbing
demonstration that cannot submit and observe a block does not prove enough.

## 7. Terminology

### 7.1 Native Nano RPC

The action-based request and response protocol exposed by Nano-compatible nodes.
A representative request has this shape:

```json
{
  "action": "account_info",
  "account": "nano_..."
}
```

Despite common informal usage, this envelope is not JSON-RPC 2.0.

### 7.2 Gateway JSON-RPC

The proposed standards-conforming JSON-RPC 2.0 interface exposed by the gateway.
A representative request has this shape:

```json
{
  "jsonrpc": "2.0",
  "method": "account_info",
  "params": {
    "account": "nano_..."
  },
  "id": 42
}
```

### 7.3 OpenRPC document

The machine-readable description of the gateway JSON-RPC interface. It describes
the gateway contract, not the native Nano envelope.

### 7.4 Event stream

An HTTP `text/event-stream` response containing normalized gateway events. The
initial design uses SSE for server-to-client delivery only.

### 7.5 Profile

A named set of behavior or schema differences associated with a node
implementation, provider, release range, or enabled feature.

### 7.6 Reconciliation

An authoritative query performed after event continuity becomes uncertain.
Reconciliation repairs client state from node RPC rather than inferring state
from an incomplete event stream.

## 8. Why combine JSON-RPC, OpenRPC, and SSE

A JSON-RPC translator alone improves request syntax but leaves event consumers
on a separate, stateful WebSocket protocol. Adding SSE allows the gateway to
present one HTTP-oriented integration surface with shared operational policy.

The combination has leverage only when the gateway also owns the difficult
parts:

- schema authority and provenance;
- error mapping;
- compatibility profiles;
- event identity and ordering;
- overflow and loss signaling;
- authentication and authorization;
- method and event filtering;
- observability; and
- recovery instructions.

If the gateway only renames fields and forwards bytes, its public interface will
be nearly as complex as both native interfaces combined. Such a shallow module
would not justify its maintenance cost.

## 9. Naming

`Nano RPC Gateway` is the current working name.

The words `proxy` and `1:1 translator` remain useful informal descriptions, but
they imply transparent equivalence. The implementation will normalize errors,
method names, event shapes, and recovery behavior. `Gateway` better communicates
that responsibility.

The repository name is `nano-rpc-gateway`. Naming remains open to refinement.

## 10. JSON-RPC interface

### 10.1 Endpoint

The proposed endpoint is:

```http
POST /rpc
Content-Type: application/json
```

The exact base path remains provisional.

### 10.2 Method namespace

The first gateway profile uses native action names at its base endpoint, such
as `account_info` and `process`. The endpoint and its profile already identify
the Nano API, so adding `nano.` to every method name is redundant and does not
improve JSON-RPC compatibility.

System methods retain the reserved `rpc.` namespace where a standard defines
them, including optional `rpc.discover`.

Potential future evolution, not part of the first implementation, is to split
the surface into four separately versioned endpoints and schemas:

1. a common integration endpoint for the small set of account, block, and
   payment operations most applications and client-side wallets use;
2. a node-state and observability endpoint for statistics, diagnostics, and
   other operations especially useful to explorers and history trackers;
3. a custodial-wallet endpoint; and
4. a proof-of-work endpoint.

This would avoid making ordinary integrations carry a large node-operator
surface simply because they share the legacy RPC port. `enable_control`-gated
operations belong in the second, node-state and observability group. Each
endpoint could retain unprefixed method names. No such endpoint split is part
of the POC; `control.` remains only a possible explicit boundary if control
operations ever must share an endpoint with non-control methods.

### 10.3 Parameter mapping

Native top-level action fields become JSON-RPC `params` members. The gateway must
not assume that all fields are optional merely because the native handler reads
them conditionally.

Each method contract must record:

- required and optional parameters;
- types and accepted string encodings;
- units;
- conditional requirements;
- defaults applied by each upstream implementation;
- configuration-dependent behavior;
- authorization requirements;
- side effects and idempotency; and
- implementation or provider extensions.

### 10.4 Result mapping

Successful native responses become JSON-RPC `result` values. The gateway should
preserve lossless Nano values, including raw amounts and hashes, unless a reviewed
contract defines a canonical transformation.

Illustrative response:

```json
{
  "jsonrpc": "2.0",
  "result": {
    "frontier": "...",
    "balance": "1000000000000000000000000000000"
  },
  "id": 42
}
```

The fields above are illustrative. Each implemented method requires evidence
from source, documentation, tagged behavior, and tests.

### 10.5 Error mapping

Native Nano errors are commonly strings embedded in JSON responses. JSON-RPC
requires structured error objects. The gateway therefore needs a maintained
error taxonomy rather than ad hoc string wrapping.

Each error mapping must define:

- stable gateway code;
- gateway message;
- original upstream error and safe metadata;
- retryability;
- whether the request may have taken effect;
- reconciliation action when outcome is ambiguous; and
- implementation and release applicability.

Unknown upstream errors must remain observable. The gateway must not incorrectly
classify an unknown error as safe to retry.

### 10.6 Request identifiers

The gateway must echo valid JSON-RPC identifiers. The upstream native request
does not provide equivalent identifier semantics, so the gateway owns request
correlation.

### 10.7 Notifications

JSON-RPC notifications omit an identifier and do not receive a response. The
first implementation should reject notifications for methods with side effects
unless tests establish safe and useful semantics.

### 10.8 Batch requests

Batch support is deferred. Before implementation, the project must decide:

- concurrency and ordering;
- per-item authorization;
- resource limits;
- partial failures;
- side-effect ordering;
- cancellation after client disconnect; and
- upstream request amplification limits.

## 11. OpenRPC discovery

### 11.1 Standard discovery

The gateway can honestly implement standard `rpc.discover` because its public
endpoint uses JSON-RPC 2.0. It is an optional runtime convenience, not a
requirement for using the gateway: generated clients can bundle the same
versioned OpenRPC document at build time.

Illustrative request:

```json
{
  "jsonrpc": "2.0",
  "method": "rpc.discover",
  "params": [],
  "id": 1
}
```

When enabled, the result returns the gateway's OpenRPC document. Operators may
disable this method to avoid serving reflection on a performance-sensitive
deployment; the matching versioned document must still be published with the
gateway release. Native Nano `{"action":"rpc.discover"}` is explicitly not
part of this design.

### 11.2 Document authority

The OpenRPC document is a reviewed projection of executable gateway contracts.
It must not be generated solely from source AST inspection or example fixtures.

The authority hierarchy remains a research item. At minimum, every contract
must record evidence from the applicable combination of:

1. upstream protocol intent;
2. behavior of supported tagged releases;
3. implementation source;
4. upstream tests and release notes;
5. public documentation;
6. provider behavior; and
7. gateway compatibility decisions.

When evidence conflicts, the project must record the conflict and the selected
gateway behavior. It must not silently merge incompatible interpretations.

### 11.3 Version and provenance

Each published schema bundle should identify:

- gateway version;
- supported node implementation and release range;
- source commit or build provenance where known;
- profile identifiers;
- contract lifecycle status;
- compatibility classification; and
- generation and review date.

### 11.4 Static Nano-native discovery

A future `/.well-known/nano-rpc` resource could advertise profiles, schema URLs,
and endpoint locations for native integrations. It is separate from OpenRPC and
must not claim OpenRPC conformance.

## 12. SSE interface

### 12.1 Purpose

SSE provides HTTP-native server-to-client event delivery. It can simplify
wallet, browser, command-line, and backend integrations that do not need a
bidirectional socket after subscription creation.

SSE does not automatically provide durable delivery, authoritative ordering, or
replay across gateway restarts.

### 12.2 Direct filtered streams

The proposed first interface creates a stream with one HTTP request:

```http
GET /events/confirmations?accounts=nano_...,nano_...
Accept: text/event-stream
Last-Event-ID: 182736
```

Illustrative event:

```text
Content-Type: text/event-stream

id: 182737
event: nano.confirmation
data: {"account":"nano_...","hash":"...","amount":"..."}
```

Exact paths, filters, event names, fields, and identifier semantics require
implementation evidence and contract tests.

### 12.3 Managed subscriptions

Large or dynamically changing filters may later use managed subscription
resources:

```text
POST   /subscriptions
GET    /subscriptions/{subscription_id}/events
PATCH  /subscriptions/{subscription_id}
DELETE /subscriptions/{subscription_id}
```

This model replaces WebSocket subscribe, update, and unsubscribe control
messages with ordinary HTTP requests. It introduces server-side state,
authorization rules, expiration, quotas, and cleanup obligations. It is deferred
until direct streams prove insufficient.

### 12.4 Event identity

Every resumable SSE event needs an identifier meaningful within a documented
scope. Candidate scopes include:

- one gateway process lifetime;
- one upstream connection generation;
- one retained event buffer; or
- a durable gateway event log.

The first implementation should prefer a monotonically increasing gateway
sequence within one retained buffer. The stream must identify a new generation
after restart or upstream resubscription.

### 12.5 Ordering

The gateway must document whether ordering applies:

- globally;
- per upstream subscription;
- per account;
- per event topic; or
- only in gateway observation order.

The gateway must not imply ledger finality or causal ordering from arrival order
unless tests and node guarantees support that claim.

### 12.6 Overflow and loss

Subscriber overflow must remain visible. The gateway must not silently discard
an upstream overflow signal or a local buffer overrun.

Proposed control events include:

- `nano.stream_ready`;
- `nano.stream_reset`;
- `nano.stream_overflow`; and
- `nano.upstream_reconnected`.

Names remain provisional. Each event must tell the client whether reconciliation
is required and which state must be queried.

### 12.7 Reconnect and replay

The proposed initial contract is bounded replay with explicit reset:

1. The client reconnects with `Last-Event-ID`.
2. The gateway replays retained events when the identifier remains available.
3. The gateway emits `nano.stream_reset` when continuity cannot be proven.
4. The client queries authoritative state through JSON-RPC.
5. The client resumes event processing from the new stream generation.

This model promises a convenient live notification channel, not a durable event
broker.

### 12.8 Heartbeats and idle behavior

The gateway should emit SSE comments or a documented heartbeat event often
enough to keep supported proxies and clients from treating a healthy stream as
idle. The interval must be configurable and tested through the intended reverse
proxy path.

### 12.9 Backpressure

Each client needs a bounded output queue. When a client cannot consume events,
the gateway must terminate or reset that stream using explicit policy. An
unbounded queue is not acceptable.

## 13. Internal operation model

The internal operation model separates Nano semantics from transport syntax.

Conceptual interface:

```text
execute(operation, context) -> result | classified error
```

The interface includes more than a function signature. It also defines:

- supported operations;
- canonical input values;
- configuration and authorization context;
- side-effect and idempotency properties;
- timeout and cancellation behavior;
- ambiguous-outcome handling;
- normalized results; and
- preserved upstream evidence.

The implementation may contain internal adapters for Nano, RsNano, fixtures, or
hosted providers. The public JSON-RPC adapter must not branch on raw upstream
strings throughout its code.

## 14. Internal event model

The internal event model receives native WebSocket messages and emits normalized
events plus stream-control events.

Conceptual interface:

```text
subscribe(filter, cursor?) -> ordered event stream | reset requirement
```

The event model owns:

- upstream subscription sharing;
- filter normalization;
- event validation;
- gateway sequence allocation;
- connection generation;
- bounded buffering;
- overflow propagation;
- client backpressure;
- deduplication policy where evidence supports it;
- reconnection; and
- reconciliation instructions.

The event model must retain the original node event or a redacted diagnostic
representation when normalization fails.

## 15. Compatibility profiles

The gateway should distinguish at least these categories:

- core-compatible behavior;
- additive extensions;
- action-name compatible but response-incompatible behavior;
- configuration-dependent behavior;
- unavailable capability; and
- unknown or untested behavior.

Profiles may apply to:

- Nano releases;
- RsNano releases;
- hosted RPC providers;
- work providers;
- enabled node features; and
- gateway policy modes.

The project must not use one provider's extension as evidence of a universal
Nano contract. Nondeterministic fields require normalization rules before
cross-provider comparison.

## 16. Authentication and authorization

The gateway should apply authentication at its public HTTP seam. Credentials
must not be inserted into the native Nano JSON request body.

The design must separate:

- transport authentication;
- method authorization;
- event-filter authorization;
- upstream node credentials; and
- redaction of credentials and sensitive request data.

ORIS-010 is relevant prior work for bearer authentication over HTTPS, but its
current status and exact applicability must be verified before implementation.

Administrative, wallet-control, and other dangerous node actions should be
disabled by default. Method exposure needs an explicit allowlist or capability
policy.

## 17. Deployment model

The gateway should support:

- a process beside a locally installed node;
- a sidecar container in the same deployment unit;
- a separate container on a private network; and
- development against remote or hosted endpoints where policy permits it.

During phase-in, the gateway may run alongside the native node RPC and
WebSocket endpoints so existing clients remain unaffected. In a hardened
deployment, an operator may keep those native ports private and expose only
the gateway's selected public interfaces. This gateway does not change the
node-to-node P2P protocol; that would require separate protocol and migration
work.

Configuration should identify exact upstream URLs, credentials, implementation
profiles, timeouts, body limits, connection limits, and exposed methods. Secret
values must not appear in logs, metrics labels, discovery documents, or error
responses.

## 18. Operational behavior

### 18.1 Health

Liveness should report whether the gateway process can serve requests.
Readiness should require configured upstream capabilities needed by the enabled
public surface.

Traffic alone does not prove readiness. Readiness should include bounded probes
that validate the expected native Nano protocol and WebSocket subscription path.

### 18.2 Observability

The gateway should expose:

- request counts and latency by normalized method;
- upstream latency and failures by profile;
- active SSE clients and subscriptions;
- upstream WebSocket reconnects;
- replay hits and misses;
- overflow and reset counts;
- rejected requests by classification; and
- schema/profile/version information.

Metrics must avoid unbounded labels such as account, hash, request ID, or raw
error text.

### 18.3 Logging

Structured logs should correlate public requests with upstream operations while
preserving secret safety. Logs should distinguish gateway rejection, upstream
rejection, transport failure, timeout, ambiguous outcome, and normalization
failure.

### 18.4 Limits

The first production-capable version needs explicit limits for:

- HTTP request bodies;
- JSON nesting and parameter sizes;
- concurrent upstream requests;
- SSE clients;
- accounts or hashes per filter;
- per-client event queues;
- replay buffer size;
- heartbeat interval; and
- request and upstream timeouts.

Exact defaults remain unresolved.

## 19. Security model

The gateway increases the reachable surface in front of a node. Security review
must include:

- accidental exposure of dangerous node actions;
- server-side request forgery through configurable upstreams;
- request amplification and JSON-RPC batch abuse;
- denial of service through filters, streams, or slow consumers;
- schema and error leakage;
- credential forwarding and logging;
- cross-origin browser access;
- HTTP caching of sensitive results;
- event-stream authorization after credential expiry;
- dependency and container provenance; and
- divergence between the advertised and enforced method allowlist.

The gateway must fail closed when authorization configuration is invalid. It
must not silently expose every native action discovered from an upstream node.

## 20. Contract evidence and conformance

Static source extraction, schema validation, and scenario coverage answer
different questions. The project must track them separately.

### 20.1 Static inventory

Static inspection can identify action names, field accesses, and implementation
changes. It cannot establish complete semantics, conditional requiredness,
formats, authorization, retryability, errors, or branch coverage.

### 20.2 Schema validation

Schema validation proves that selected examples conform to a declared shape. It
does not prove that the shape covers every valid or invalid behavior.

### 20.3 Scenario coverage

Deterministic scenarios exercise concrete behavior against implementations.
Each method needs a coverage manifest listing exercised and unexercised cases.

### 20.4 Compatibility report

A versioned report should state:

- tested gateway revision;
- node implementation, version, and build provenance;
- enabled configuration;
- scenario fixture provenance;
- pass, fail, and not-tested results;
- normalized nondeterministic fields;
- observed incompatibilities; and
- limitations of the conclusion.

The report must not convert partial scenario coverage into a universal
compatibility claim.

## 21. Proposed implementation sequence

Each stage should end in runnable evidence and may revise later stages.

### Stage 0: Evidence harness

- Capture native requests and responses for the first methods.
- Create deterministic fixtures for Nano and RsNano.
- Record version and configuration provenance.
- Define a coverage manifest format.
- Prove one hosted-provider divergence if remote testing is authorized.

Exit evidence: repeatable observations without a public gateway.

### Stage 1: One read-only JSON-RPC method

- Implement JSON-RPC parsing and request correlation.
- Implement `account_info` or another bounded read method.
- Preserve raw upstream evidence in test fixtures.
- Define structured errors for observed failure cases.
- Publish the first reviewed OpenRPC fragment.

Exit evidence: a client can discover and call one read-only method.

### Stage 2: Side effects and ambiguous outcomes

- Add `process` and `work_generate`.
- Specify idempotency and retry behavior.
- Test timeout-before-send, timeout-after-send, and stale-state outcomes.
- Compare Nano and RsNano behavior.

Exit evidence: a client can submit a valid block and interpret failures safely.

### Stage 3: Confirmation SSE

- Translate native confirmation subscriptions.
- Add filters, event IDs, bounded buffering, and heartbeats.
- Propagate upstream and local overflow.
- Test disconnect, replay hit, replay miss, reset, and reconciliation.

Exit evidence: a client can submit a block, observe confirmation, reconnect, and
recover when continuity is lost.

### Stage 4: Discovery and compatibility publication

- Return the complete current document through `rpc.discover`.
- Publish schema provenance and profiles.
- Generate a compatibility report from deterministic scenarios.
- Verify at least one independent client or tool against discovery.

Exit evidence: discovery has a real consumer and matches enforced behavior.

### Stage 5: Deployment hardening

- Add container and process deployment paths.
- Add authentication, allowlists, limits, metrics, and structured logs.
- Test reverse proxies and SSE idle behavior.
- Perform security and failure-injection review.

Exit evidence: an operator can deploy, diagnose, limit, upgrade, and roll back the
gateway without exposing the native node interfaces.

## 22. Verification strategy

The interface is the primary test seam. Tests should exercise the same JSON-RPC
and SSE surfaces used by integrators.

Required test layers are proposed as follows:

| Layer | Purpose | Initial evidence |
| --- | --- | --- |
| Contract tests | Validate public envelopes and errors | JSON fixtures and schema checks |
| Adapter tests | Validate native-to-normalized mapping | Recorded and synthetic node responses |
| Scenario tests | Validate behavior on deterministic ledgers | Nano and RsNano development networks |
| Stream tests | Validate ordering, overflow, reconnect, and reset | Controlled WebSocket source and slow clients |
| Discovery tests | Compare OpenRPC with enforced behavior | Method inventory and negative calls |
| Deployment tests | Validate container/process behavior | Loopback and reverse-proxy environments |
| Security tests | Validate exposure and limits | Denied methods, malformed input, resource pressure |

Probabilistic proof-of-work timing must not determine compatibility conclusions.
Tests should control work-provider outcomes or use deterministic fixtures where
possible.

## 23. Decision log

### D-001: Build a gateway, not a native `rpc.discover` action

Status: proposed

Reason: OpenRPC discovery belongs to a genuine JSON-RPC 2.0 endpoint. A native
Nano action named `rpc.discover` would not satisfy that contract.

### D-002: Combine request and event modernization

Status: proposed

Reason: JSON-RPC plus OpenRPC improves only half of the integration experience if
clients must still implement the separate native WebSocket protocol.

### D-003: Use SSE for outward event delivery

Status: proposed

Reason: SSE offers a browser-friendly, HTTP-native, server-to-client stream. The
gateway can replace bidirectional subscription control with HTTP request
parameters or later managed subscription resources.

### D-004: Promise live notifications before durable events

Status: proposed

Reason: bounded replay plus explicit reset provides honest recovery without
turning the first sidecar into a durable event broker.

### D-005: Normalize behind an internal model

Status: proposed

Reason: translation, errors, profiles, ordering, and recovery must remain local.
A pass-through adapter would leak both legacy interfaces into every client.

### D-006: Develop through useful vertical slices

Status: required guiding constraint

Reason: implementation evidence should refine the design. Large up-front method
coverage would delay feedback and make unsupported assumptions harder to remove.

## 24. Open questions and research backlog

These questions are intentionally unresolved. An implementation decision should
cite evidence, tests, or a recorded trade-off.

### Public contract

- What evidence would justify a future endpoint split for node, custodial
  wallet, proof-of-work, or control operations?
- Which JSON-RPC version details and OpenRPC tooling interoperate in practice?
- Should positional `params` be rejected or normalized?
- Which methods can safely support JSON-RPC notifications?
- When, if ever, should batch requests be enabled?
- How are provider extensions named without fragmenting clients?

### Schema authority

- What is the exact authority order when source, docs, tests, and behavior differ?
- Which upstream release ranges deserve simultaneous support?
- How are conditional response variants represented?
- How does the project review generated schema changes?
- Which contract artifacts are handwritten, generated, or hybrid?

### Events

- Which native topics provide enough stable semantics for the first release?
- What ordering does each node implementation actually guarantee?
- How does each node report subscriber overflow?
- Can a stable cursor derive from native data, or must it remain gateway-local?
- How large should the replay buffer be?
- What exact JSON-RPC reconciliation recipe corresponds to each reset?
- When do managed subscriptions become necessary?
- Does durable event retention ever belong in this project?

### Operations and security

- Which runtime and implementation language best fit the target deployments?
- Which node connection transports must the first release support?
- What are safe default method and topic allowlists?
- How are authentication and authorization configured and rotated?
- Which reverse proxies and hosted environments must SSE support?
- What limits prevent slow-client and filter-amplification attacks?
- What state, if any, may persist across restarts?

### Product and ecosystem

- Which real wallet, explorer, exchange, or SDK will consume discovery first?
- Is generated client code valuable before broader method coverage?
- Which hosted provider divergence best demonstrates the gateway's value?
- Would Nano or RsNano maintainers consume compatibility reports?
- What maintenance ceiling should terminate or narrow the effort?

## 25. Change discipline

This design is provisional, but changes must remain legible.

When evidence changes the design:

1. Update the affected canonical section.
2. Add or revise a decision-log entry.
3. Record the evidence or test that caused the change.
4. Update affected acceptance criteria and open questions.
5. Avoid preserving obsolete alternatives as if they remain current.

Implementation convenience alone is evidence of cost, not evidence of correct
protocol semantics.

## 26. Milestone acceptance criteria

### 26.1 First read-only method

- A standards-valid JSON-RPC request reaches one configured node.
- The gateway returns a standards-valid result or structured error.
- `rpc.discover` describes the method accurately.
- Contract tests reject malformed and unsupported requests.
- The result records applicable node versions and profiles.

### 26.2 First complete wallet flow

- A client queries account state.
- A client obtains or submits work through the selected policy.
- A client submits a signed state block.
- A client observes confirmation through SSE.
- A disconnected client either replays retained events or receives an explicit
  reset.
- A reset client reconciles authoritative state through JSON-RPC.
- The same scenarios run against Nano and RsNano with visible differences.

### 26.3 First deployable release

- Existing native clients remain usable during phase-in; hardened deployments
  may later expose only gateway endpoints.
- Authentication and method allowlists fail closed.
- Logs and metrics do not expose secrets or unbounded identifiers.
- Resource limits cover requests, streams, filters, buffers, and upstream calls.
- Operators can distinguish liveness, readiness, upstream failure, and stream
  degradation.
- Deployment and rollback procedures are tested.
- Published discovery matches enforced behavior.

## 27. Known failure modes

| Failure | Required observable behavior | Recovery direction |
| --- | --- | --- |
| Upstream RPC unavailable | Classified gateway error and readiness degradation | Retry only safe operations |
| Upstream request times out | Report whether outcome is ambiguous | Reconcile before side-effect retry |
| Native response violates profile | Normalization error with safe upstream evidence | Update profile or fix implementation |
| Upstream WebSocket disconnects | Emit or force stream reset when continuity is unknown | Reconnect and reconcile |
| Upstream subscriber overflow | Preserve overflow as a gateway control event | Reconcile affected state |
| Local SSE client is slow | Apply bounded queue policy | Disconnect or reset the client |
| Replay cursor expired | Emit explicit reset | Query authoritative state |
| Gateway restarts | Start a new stream generation | Reconcile unless durable evidence exists |
| Discovery drifts from runtime | Fail conformance or publication checks | Correct contract or implementation |
| Unauthorized method requested | Reject before upstream forwarding | Change policy only through configuration |

## 28. Evidence provenance from prior discussions

The design incorporates ideas developed across earlier Nano interoperability
work. These references are context, not permanent authority.

### 28.1 OpenRai schema roadmap and discovery

- Session: `01a03e05-31ef-78d1-a014-093f610617dc`
- Workspace: `/Users/conny/Developer/nano/OpenRai`
- Date: 2026-08-26
- Topics: OpenRPC, JSON-RPC 2.0, native Nano RPC, `rpc.discover`, schema
  authority, compatibility profiles, `work_generate`, and minimum deliverables.

### 28.2 ORIS transport and authentication layering

- Session: `01a00933-267d-7061-868f-ab4848f3bce0`
- Workspace: `/Users/conny/Developer/nano/OpenRai/Standards`
- Date: 2026-08-16
- Topics: Nano RPC semantics versus JSON-RPC, HTTP, gRPC, and transport-specific
  authentication placement.

### 28.3 RsNano gRPC interoperability exploration

- Sessions: `019fd548-3c7e-7f21-9c88-d1befad977b4`,
  `019fdbb4-2706-7ae3-ab46-beb7214acfbf`,
  `019fdd43-ba10-7732-98e6-2cff85bf9503`,
  `019fe018-9edd-7da2-a099-a3c7bb95a94d`,
  `019fe067-3291-77e1-b808-0d2caff391e0`, and
  `019fe0c5-82ec-7a53-ab41-9c78f26e2400`
- Workspace: `/Users/conny/Developer/nano/rsnano-grpc`
- Dates: 2026-08-06 through 2026-08-08
- Topics: typed replacement surfaces, JSON-RPC behavioral parity, deterministic
  scenarios, event overflow, wallet-developer experience, and clean adapters.

### 28.4 `work_generate` integration work

- Sessions: `01a02288-b44c-7d63-9a24-5f4eb976584f`,
  `01a02288-b4f0-7cf3-b90e-f50ec23e5ad6`,
  `01a0287e-127b-7783-b9bc-7223501d9b62`,
  `01a028c6-2117-7b23-b48f-848c527942a7`, and
  `01a028c6-21df-79d2-9c2d-ec1bdda1b8e7`
- Workspace: `/Users/conny/Developer/CasualSecurityInc/nano-rspow`
- Dates: 2026-08-21 through 2026-08-22
- Topics: proxying `work_generate`, validating request and response shapes,
  provider behavior, work-server mode, and benchmark tooling.

### 28.5 Current design conversation

- Date: 2026-08-28
- Workspace: `/Users/conny/Developer/nano/OpenRai`
- Topics: a standalone sidecar gateway, JSON-RPC translation, OpenRPC discovery,
  replacement of the native WebSocket client surface with SSE, live versus
  durable events, progressive design, and dual developer-experience goals.

## Appendix A: Candidate artifact layout

This layout is illustrative and should follow the selected implementation
language rather than constrain it prematurely.

```text
README.md
DESIGN.md
docs/
  decisions/
  compatibility/
  operations/
contracts/
  openrpc.json
  methods/
  events/
fixtures/
  nano/
  rsnano/
  providers/
src/
tests/
```

## Appendix B: Candidate first method contract

`work_generate` remains a strong interoperability pilot because node and hosted
provider variants have previously shown meaningful request and response
differences.

A future contract unit should specify:

- root or hash meaning;
- difficulty encoding and default;
- work version or algorithm selection;
- multiplier behavior;
- synchronous and asynchronous behavior;
- cached work behavior;
- response fields;
- cancellation and timeout behavior;
- provider authentication;
- validation of returned work; and
- compatibility with strict existing Nano clients.

No field in this appendix is normative until verified against supported
implementations and providers.

## Appendix C: Design principles for agents

An agent working in this repository should apply these principles:

1. Inspect actual node behavior before strengthening a contract claim.
2. Separate observed facts, proposed policy, assumptions, and unresolved items.
3. Keep the first implementation slice narrow and end-to-end useful.
4. Do not silently convert native Nano RPC into JSON-RPC terminology.
5. Preserve upstream errors and event-loss evidence before normalizing them.
6. Keep public adapters thin and the internal operation/event model deep.
7. Test through public interfaces and deterministic upstream adapters.
8. Treat OpenRPC as a description of enforced gateway behavior.
9. Treat SSE as lossy unless continuity is proved for the requested cursor.
10. Record design changes when implementation evidence contradicts this document.
11. Prefer explicit unsupported behavior over plausible but unverified support.
12. Improve both core and integrator developer experience in every meaningful
    milestone.

# A Machine-Readable Nano RPC Schema: Strategic Roadmap

**Status:** Draft for ORIS consideration
**Scope:** nano-node (C++, canonical) and rsnano-node (Rust port)
**Core thesis:** The primary value of this effort is not client/server code generation — with only two server implementations in existence, and possibly never more, that's a minor win. The primary value is **CI-enforced drift detection**: a mechanism that makes the build fail the instant the RPC surface changes in a way that isn't reflected in a versioned, machine-readable schema.

---

## 1. Where things stand today

### 1.1 No machine-readable schema exists, anywhere, officially

Neither `nanocurrency/nano-node`, nor `rsnano-node/rsnano-node`, nor `nanocurrency/nano-docs` defines a machine-readable schema for the RPC surface. `nano-docs`' [RPC Protocol reference](https://docs.nano.org/commands/rpc-protocol/) is a hand-maintained Markdown page — prose plus example request/response JSON, organized by section. There is no OpenAPI file, no OpenRPC document, no JSON Schema bundle, generated or otherwise, anywhere in the official project surface.

This isn't a documentation-browsing failure — it's a real gap. Third parties have had to reverse-engineer the surface themselves (see §1.3).

### 1.2 The canonical source of truth: `json_handler.cpp`, not `rpc_handler.cpp`

The per-action request/response logic does **not** live where the file name suggests. `nano/rpc/rpc_handler.cpp` is transport/depth-limiting plumbing (233 lines). The real logic is in **`nano/node/json_handler.cpp`** (5,727 lines):

- **120 actions**, each a `void nano::json_handler::<action_name> ()` member function.
- Dispatched via a single static map (`no_arg_funcs`, built by `create_ipc_json_handler_no_arg_func_map()`), one `emplace("action_name", &nano::json_handler::function_name)` line per action — action name and function name occasionally differ (aliasing), which any automated tool must resolve via this table, not by string-matching function names.
- Parameters are read via `boost::property_tree`: `request.get<T>("key", default)`. Response fields are written via `response_l.put("key", value)`.

**Parameter-extraction census** (of all 120 dispatched actions, resolved against the authoritative action→function dispatch table):

| Pattern | Count | Share |
|---|---|---|
| Route ≥1 param through a shared `_impl()` helper | 74 | 62% |
| Use *only* inline `request.get<>()`, no shared helper | 19 | 16% |
| Use *both* a shared helper and inline `get<>()` | 39 | 32% |
| Take no parameters at all (pure no-arg / state queries) | 27 | 22% |

There are exactly **16 distinct `_impl()` helper functions** in the file. Two dominate: `account_impl` and `wallet_impl`, each used by 32 handlers — together covering the bulk of the wallet and account-query surface. This means:

- A naive per-function scanner (looking only at each named handler's own body) would **miss parameters on well over half the API**, including bread-and-butter actions like `account_info`, `account_balance`, and most `wallet_*` calls.
- But the helper surface is small and closed — 16 known functions, not arbitrary indirection. A one-time resolution of what each helper pulls from `request`, spliced into every caller, closes the gap without open-ended complexity.
- Response shape sometimes branches on input at runtime (e.g. `account_info`'s `pending`/`receivable` vs. `confirmed_pending`/`confirmed_receivable` aliasing, gated on an `include_confirmed` flag). This needs a human judgment call per action to model as `oneOf`/conditional JSON Schema — not something a call-site scanner alone can infer.
- Types are runtime-coerced (`request.get<bool>(...)`), not statically declared, and Nano's RPC is famous for accepting numbers-as-strings at the boundary — schema `type` fields need cross-checking against `nano-docs`' worked examples, not just the C++ template parameter.

### 1.3 Prior art: `mehlium/nano-rpc-api` → `nanobox-cc/openapi-nano-spec`

An independent attempt exists. [`mehlium/nano-rpc-api`](https://github.com/mehlium/nano-rpc-api) is archived, with a pointer: the project moved to [`nanobox-cc/openapi-nano-spec`](https://github.com/nanobox-cc/openapi-nano-spec) (same effort, same author community, new home).

**Coverage, per that repo's own tracked checklist:**

| Surface | Done | Total | Share |
|---|---|---|---|
| Node RPCs | 33 | 68 | ~49% |
| Wallet RPCs | 3 | 37 | ~8% |
| Unit conversion RPCs | 0 | 6 | 0% |
| **Overall** | **~36** | **~111** | **~32%** |

It ships a Redoc reference site, a Swagger UI, raw `openapi.json`/`openapi.yaml` artifacts, and a generated TypeScript client (`nanobox-cc/nano-rpc-typescript`) — real infrastructure for the actions it covers, abandoned mid-way, never merged upstream, using aging tooling (Travis CI).

**What it almost certainly is, structurally:** given the tooling used (Redocly, Swagger UI — both REST/OpenAPI-native), it's very likely the [documented "fake OpenAPI schema" pattern](https://dev.to/vearutop/json-rpc-2-0-with-swagger-ui-2h3g) for hosting JSON-RPC-shaped APIs in Swagger tooling: one artificial OpenAPI `path` per action (`/account_balance`, `/block_info`, ...), each a `post` operation, even though every one of those "paths" is really the same single HTTP endpoint disambiguated by an `action` field in the body.

**What's salvageable vs. not:**
- ✅ The request/response **payload shapes** for the ~33 modeled node actions are real, reusable JSON Schema (OpenAPI's `components.schemas` *is* JSON Schema under the hood) — a legitimate seed for a `schemas/` directory.
- ⚠️ If the source is OpenAPI 3.0.x (likely, given era/tooling), its schema dialect isn't quite standard JSON Schema — `nullable: true` instead of `type: ["string","null"]`, no `$id`, occasional OpenAPI-only keywords. A normalization pass (or a 3.0→3.1 migration, since 3.1 adopted JSON Schema 2020-12 directly) should happen before treating these as drop-in.
- ❌ The REST **envelope** itself (paths, verbs, "resources") earns nothing and should not be perpetuated — see §2.

---

## 2. The envelope question: Nano's RPC is not JSON-RPC of any vintage

### 2.1 Origin: homegrown from day one, unrelated to any JSON-RPC lineage

Nano's `{"action": "...", ...params}` envelope traces directly back to the original [RaiBlocks C++ implementation](https://github.com/clemahieu/raiblocks) — present already in the [earliest surviving wiki snapshots](https://github.com/BitDesert/raiblocks-wiki/blob/master/RPC-protocol.md), with old documentation referencing the source file directly (`rai/node/rpc.cpp`, the direct ancestor of today's `json_handler.cpp`).

It is **not** a degenerate or early-era JSON-RPC dialect. JSON-RPC 1.0 (2005, Dojo-era) already used `{"method": ..., "params": [...], "id": ...}` — the same field-naming lineage 2.0 later formalized. Nano's `action` key shares no naming lineage with either. It's a flat, single-level object where the dispatch key sits as a peer to the parameters, rather than a nested envelope separating protocol metadata from payload. A meaningfully different design, not a version skew.

Supporting evidence this is a self-contained, independent design rather than a degraded standard:
- The `version` action returns an `rpc_version` field — easy to mistake for `"jsonrpc":"2.0"`, but it's Nano's own internal protocol-revision counter, unrelated to any JSON-RPC spec versioning.
- The only `"id"` field found in the source (`json_handler.cpp:4092`) is scoped to the `send` action alone — an auto-generated idempotency key for safe retries, **not** a JSON-RPC request/response correlation id. Any translation layer must synthesize its own correlation ids without colliding with this pre-existing, semantically unrelated field.

### 2.2 Why this matters for schema fidelity

OpenRPC formally assumes the JSON-RPC 2.0 transport envelope: request as `{"jsonrpc":"2.0","method":X,"params":Y,"id":Z}`, response as `{"jsonrpc":"2.0","result":...,"id":Z}` or a spec-shaped `error` object with a numeric `code`. Nano's wire format has none of this — no `jsonrpc` marker, no `id` echo, no `params` sub-object, ad hoc `{"error": "..."}` strings instead of spec-shaped error objects.

**A literal, wire-faithful OpenRPC document for Nano cannot be written honestly without a decision:**
- **(A)** Write an OpenRPC-*shaped* document, explicitly annotated as non-literal/idealized — reuses the method/params/result vocabulary and the shared JSON Schema payloads, but openly documents that it does not describe the actual wire envelope.
- **(B)** Build a translation adapter that makes the envelope claim literally true (see Phase 3, §4).

---

## 3. Competitive posture: how Nano compares to other chains

The split is generational, not qualitative — chains that shipped before or right at JSON-RPC 2.0's 2010 finalization rolled their own dialect; everything built after adopted 2.0 outright, for free, because there was no legacy install base pulling the other way.

| Chain | Launch era | Wire envelope | Native 2.0? | Machine-readable schema? | CI-validated? |
|---|---|---|---|---|---|
| **Nano** (RaiBlocks) | 2015 | Bespoke `{"action": ...}` | No | No (official) | No |
| **Bitcoin Core** | 2009 (pre-dates 2.0 spec) | Blended 1.0/1.1/2.0 | Opt-in since v27 (2023–24) | No | No |
| **Ethereum** | 2015 (post-2.0) | Native JSON-RPC 2.0 | Yes, since inception | **Yes — OpenRPC, canonical** | **Yes** |
| **Solana** | 2020 (post-2.0) | Native JSON-RPC 2.0 | Yes, since inception | Docs-only (no OpenRPC found) | Not found |

**Bitcoin — the closest precedent, and evidence this is achievable non-disruptively.** [Bitcoin Core's own maintainers describe](https://github.com/bitcoin/bitcoin/pull/27101) their historic RPC server as "a special blend of 1.0, 1.1, and 2.0 behaviors" — non-200 HTTP codes for RPC errors, both `error` and `result` present simultaneously, inconsistent batch handling. A first attempt at strict 2.0 compliance ([PR #12435](https://github.com/bitcoin/bitcoin/pull/27101)) was abandoned. It took until 2023–24 to ship real, **opt-in, version-detected** 2.0 support: [the server now recognizes 2.0 only when `"jsonrpc":"2.0"` is present in the request, otherwise falling back to the legacy protocol](https://github.com/bitcoin-core/gui/blob/master/doc/JSON-RPC-interface.md) that was the only option through v27.0. This is direct, close-to-home precedent that a legacy chain can add real 2.0 conformance non-disruptively, 15 years post-launch, without breaking its existing client ecosystem — via exactly the version-negotiation pattern Phase 3 (§4) would need.

**Ethereum — the reference architecture to emulate.** [`ethereum/execution-apis`](https://github.com/ethereum/execution-apis) is the existence proof for the entire plan in this document, already running at scale:
- The spec is [continuously validated against the OpenRPC metaschema as part of the build](https://github.com/ethereum/execution-apis).
- [Every method and schema is defined natively in OpenRPC format](https://deepwiki.com/ethereum/execution-apis), compiled into machine-readable JSON Schema and human docs, used as the single source of truth by Geth, Nethermind, Besu, Erigon, consensus clients, and library authors (web3.js, ethers.js, viem) alike.
- **Crucially — this is the CI drift-detection mechanism this roadmap wants, already proven**: [test cases live alongside the spec as literal recorded request/response pairs](https://deepwiki.com/ethereum/execution-apis/4-json-rpc-methods) (`>>` request / `<<` response), validated against the specification by a dedicated `speccheck` tool. This is functionally identical to Phase 4 (§4) below, at a mature, multi-implementation scale.

**Solana** — native JSON-RPC 2.0 [from inception](https://github.com/solana-foundation/developer-content/blob/main/docs/rpc/http/index.mdx), unsurprising given it postdates the spec by a decade with no earlier convention to be grandfathered into. No public OpenRPC document was found in this research, despite the clean envelope — a reminder that envelope conformance and schema formalization are separate axes; Nano is behind on both, Solana is behind on one.

---

## 4. Roadmap — in order of effort / payoff

### Phase 0 — Framing decision (no cost, do first)

Decide whether the near-term deliverable is **(A)** an annotated, honestly-non-literal OpenRPC document, or **(B)** a real translation proxy making OpenRPC conformance literally true. Recommendation: **defer this decision** — Phase 1 delivers standalone value regardless of which path is chosen, so build it first and decide the envelope question once Phase 1 exists.

### Phase 1 — Deterministic, versioned JSON Schema extraction *(highest priority — do regardless of everything else)*

- **Tooling:** a **Clang LibTooling AST matcher**, not a naive regex/grep pass — required specifically because of the 62%/16-helper finding in §1.2. It must resolve the closed set of 16 `_impl()` helper functions once, then splice their extracted fields into every calling action.
- **Cross-reference against `nano-docs`**, method by method, as a second ground-truth source — flag discrepancies between what the C++ actually does and what the hand-maintained prose claims.
- **Externalize** each action's request/response shape as standalone, `$ref`-able JSON Schema files (`schemas/AccountInfoResponse.json`, `schemas/Amount.json`, etc.) — seed this directory with `nanobox-cc`'s already-modeled ~33 actions, after a dialect-normalization pass (OpenAPI 3.0→3.1 / standard JSON Schema).
- **Version and tag the output per nano-node release** (`git tag V28.1` ⇔ schema `V28.1`) — this alone satisfies the "reliable, deterministic, versioned" goal at the payload level, independent of anything else in this document.
- The 27 no-arg actions are free wins — no extraction needed, just confirm against `nano-docs`.

### Phase 2 — Assemble the OpenRPC document

- One `methods[]` entry per action (120 total), `params`/`result` fields pointing via `$ref` at the Phase 1 schema files — both an eventual OpenAPI view (if ever wanted) and the OpenRPC document share the *same* files, guaranteeing type consistency without duplication.
- Explicitly document (per the Phase 0 decision) that this describes an idealized method/params/result mapping onto Nano's actual flat, action-keyed wire format, unless/until Phase 3 exists to make that claim literal.
- Validate the document itself against the [OpenRPC metaschema](https://spec.open-rpc.org/) in CI — mirroring Ethereum's `speccheck` pattern from day one, even before Phase 4's drift detection exists.

### Phase 3 — Bidirectional JSON-RPC 2.0 ⇄ Nano-action translation proxy

- A thin adapter service (Rust is a natural fit, could sit alongside existing `nano-grpc` gateway work) that:
  - Unwraps `{"jsonrpc":"2.0","method":X,"params":Y,"id":N}` → `{"action":X, ...flattened params}`, forwards to a real node.
  - Wraps the flat response back into `{"jsonrpc":"2.0","result":{...},"id":N}` or a spec-shaped `error` object.
  - Synthesizes/tracks its **own** correlation ids, kept separate from the pre-existing `send`-action idempotency `id` field (§2.1) so the two never collide.
- Once this exists, the Phase 2 OpenRPC document becomes **literally true** of real traffic through the proxy — unlocking the entire off-the-shelf OpenRPC ecosystem (Playground, Inspector, generic client generators, conformance tooling) with zero Nano-specific tooling required. This is the point at which Nano reaches functional parity with Ethereum's setup (§3), mediated by an adapter rather than native support.
- Existing action-based clients are completely unaffected — this is purely additive infrastructure sitting in front of, not inside, the node.

### Phase 4 — CI-enforced drift detection *(the actual headline win)*

This is the reframing this document exists to capture: **the dominant value of this entire effort is not code generation.** There are two server implementations today — `nano-node` (C++, canonical) and `rsnano-node` (Rust port) — and per your own framing, there may never be more. Generating new clients/servers from the schema is a minor, almost incidental benefit next to this:

- Build a recorded request/response test-vector corpus, mirroring Ethereum's `>>`/`<<` format, exercised against a live node on every commit/release.
- The Phase 1 AST-matcher is re-run continuously, not just once — it becomes a **live source-of-truth generator**, diffed against the versioned schema on every CI run.
- **Any of the following breaks the build immediately:**
  - A field appears in live/source behavior that isn't present in the schema (undocumented addition).
  - A schema-declared field is no longer present, or has been silently renamed (undocumented removal/rename).
  - A type coercion changes (e.g. a field silently switches from string-encoded to native JSON number).
- **This converts "a downstream consumer eventually notices in production" into "the PR fails automatically, same day."** That's the confidence win: not a hypothetical interoperability improvement, but a concrete regression alarm for the two people/teams who actually maintain the two implementations, plus every exchange, wallet, and explorer downstream of them.
- **A valuable side effect:** running this corpus against *both* `nano-node` and `rsnano-node` in CI *is* the cross-implementation conformance check that RsNano's public repo does not currently appear to have (per earlier research into its testing setup — its "nullable infrastructure" unit tests exercise the Rust implementation's own behavior, but no public evidence of automated behavioral diffing against the C++ original was found). Phase 4 closes that gap as a natural byproduct, for free.

### Phase 5 — Formalize as an ORIS artifact; long-shot upstream conversation

- Publish the schema files, OpenRPC document, proxy (if built), and CI drift-checker as ORIS-sanctioned infrastructure — explicitly positioned as additive, requiring no changes to either server implementation's own source, and not affecting any existing action-based client.
- Treat native upstream adoption (a `--rpc-json-rpc2` compatibility mode in `nano-node` itself) as a **stretch goal, not a dependency** for anything above. Bitcoin's precedent (§3) is grounds for optimism that it's *achievable* non-disruptively — but a working proxy + schema + CI gate, already delivering value on their own, is a far easier upstream pitch later ("here's a working reference implementation") than a speculative ask today.

---

## 5. Summary table — effort vs. payoff

| Phase | Deliverable | Depends on | Payoff type |
|---|---|---|---|
| 0 | Framing decision | — | Unblocks everything else |
| 1 | Versioned JSON Schema per action | Clang AST matcher + nano-docs cross-check | Standalone: reliable, versioned payload schema |
| 2 | OpenRPC document | Phase 1 | Standard-shaped docs, OpenRPC metaschema validation |
| 3 | JSON-RPC 2.0 translation proxy | Phase 2 | Unlocks generic OpenRPC tooling ecosystem |
| 4 | CI drift-detection gate | Phase 1 (re-run continuously) | **Primary win: automatic regression alarm; incidentally closes the C++/Rust conformance gap** |
| 5 | ORIS RFC + optional upstream pitch | Phases 1–4 | Ecosystem legitimacy; long-shot native adoption |

---

## References

- Nano RPC documentation: [docs.nano.org/commands/rpc-protocol](https://docs.nano.org/commands/rpc-protocol/) · [nano-docs source](https://github.com/nanocurrency/nano-docs/blob/main/docs/commands/rpc-protocol.md) · [original nano-node wiki](https://github.com/nanocurrency/nano-node/wiki/RPC-protocol)
- `rsnano-node`: [repository](https://github.com/rsnano-node/rsnano-node) · [RsNano announcement](https://nano.org/en/news/rsnano-a-working-nano-node-written-in-100-rust--53016a8c) · [rsnano.com](https://rsnano.com/)
- Prior art: [mehlium/nano-rpc-api](https://github.com/mehlium/nano-rpc-api) (archived) → [nanobox-cc/openapi-nano-spec](https://github.com/nanobox-cc/openapi-nano-spec)
- OpenRPC: [spec.open-rpc.org](https://spec.open-rpc.org/) · [open-rpc.org](https://www.open-rpc.org/) · [generator-client (Rust client support)](https://www.npmjs.com/package/@open-rpc/generator-client)
- OpenAPI-as-JSON-RPC hack pattern: [dev.to/vearutop](https://dev.to/vearutop/json-rpc-2-0-with-swagger-ui-2h3g)
- Historical origin: [BitDesert/raiblocks-wiki RPC-protocol.md](https://github.com/BitDesert/raiblocks-wiki/blob/master/RPC-protocol.md) · [raiblocks-py docs](https://raiblocks-py.readthedocs.io/en/latest/_modules/raiblocks/rpc.html) (references original `rai/node/rpc.cpp`)
- Bitcoin Core JSON-RPC 2.0 support: [PR #27101](https://github.com/bitcoin/bitcoin/pull/27101) · [PR #30238](https://github.com/bitcoin/bitcoin/pull/30238) · [JSON-RPC-interface.md](https://github.com/bitcoin-core/gui/blob/master/doc/JSON-RPC-interface.md)
- Ethereum: [ethereum/execution-apis](https://github.com/ethereum/execution-apis) · [DeepWiki overview](https://deepwiki.com/ethereum/execution-apis) · [JSON-RPC methods / test format](https://deepwiki.com/ethereum/execution-apis/4-json-rpc-methods) · [ethereum.org JSON-RPC API](https://ethereum.org/developers/docs/apis/json-rpc/) · [etclabscore/ethereum-json-rpc-specification](https://github.com/etclabscore/ethereum-json-rpc-specification)
- Solana: [developer-content RPC HTTP docs](https://github.com/solana-foundation/developer-content/blob/main/docs/rpc/http/index.mdx) · [solana-com RPC reference](https://github.com/solana-foundation/solana-com/blob/main/apps/docs/content/docs/en/rpc/http/index.mdx)
- JSON-RPC 2.0 specification: [jsonrpc.org](https://www.jsonrpc.org/specification)
- Source census in this document (§1.2) was produced by scripted analysis of `nano/node/json_handler.cpp` at `nanocurrency/nano-node` `master` (cloned during this research; not separately citable by URL — reproducible against any current checkout).

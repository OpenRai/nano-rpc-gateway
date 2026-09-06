# OpenRPC schema authority and release policy

The published V28.2 OpenRPC document is a reviewed artifact, not an automatic
dump of every native Nano action.

## V28.2 feature disposition

| Disposition | Gateway behavior |
| --- | --- |
| Supported | `version`, `block_count`, `account_info`, `receivable`, `account_balance`, `accounts_balances`, `account_history`, `block_info`, `blocks_info`, `process`, and `rpc.discover` when discovery is enabled. `accounts_balances` is the native per-account batch response; the gateway does not aggregate it. |
| Rejected | JSON-RPC request batches, unknown methods, deprecated aliases, and methods outside the published profile return the standard JSON-RPC method/parameter error. |
| Deferred | Control, custodial-wallet, and threshold-discovery operations are not part of this profile and are not advertised or routed. |

The conformance suite checks both the positive method inventory and negative
calls for unsupported/deprecated surface. A native node accepting an action is
not evidence that the gateway supports it.

## Authority order

1. The pinned gateway contract and its acceptance tests define the public
   method names, envelopes, and normalized fields.
2. Official Nano V28.2 source and release documentation define native request
   and response semantics.
3. Versioned fixtures and live conformance runs provide observed evidence.
4. Provider behavior is evidence for a named profile only; it cannot silently
   change the V28.2 profile.

When sources disagree, the discrepancy is recorded in the profile and the
schema keeps the narrowest shape supported by the contract. Unknown or
conditional native fields stay below the gateway boundary unless the contract
promotes them explicitly.

## Approval and evolution

Every schema change requires a reviewed OpenRPC diff, updated generated
bindings, and passing contract tests. A maintainer approves the change after
checking the authority order and fixture provenance.

Removing a method, renaming a field, changing requiredness, or changing an
error meaning is a new profile/version (for example nano-node/V28.3); it is
not represented by a compatibility alias in V28.2. Additive optional fields
may remain in the same profile only when they are documented, deterministic,
and covered by tests. Deprecated native actions are never advertised merely
because an upstream node still accepts them.

The artifact digest in rpc.discover and /openrpc.json identifies the exact
canonical profile semantics. Runtime server URLs do not change that digest.

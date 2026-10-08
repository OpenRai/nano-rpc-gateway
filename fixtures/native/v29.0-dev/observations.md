# Live V29.0-DEV observations

Captured 2026-10-08 against `https://rainstorm.city/api`, which reports
`node_vendor: "Nano V29.0-DEV"`, `network: "live"`. This is a public provider on
the main network, not a development ledger, so it is evidence about a provider and
about the node build, not about the pinned V28.2 profile.

Purpose: settle the questions the docs leave open. Everything here is an
observation, not a contract. The pinned profile remains `nano-node/V28.2`.

## Confirmed by observation

- `confirmed` is the **string** `"true"`, not a boolean, on `block_info`,
  `blocks_info` and `account_history` entries. The gateway previously coerced it.
- `successor` and `local_timestamp` are present on `block_info` when the block is
  known. The gateway previously dropped them.
- `contents` is a **JSON string** when `json_block` is omitted, and a JSON subtree
  when `json_block` is `"true"`.
- `block_info` has **no `hash` key**. The caller supplied it.
- `include_linked_account` returns a real account for a send block, not `"0"`.
- `account_history` **head is inclusive**: with
  `head=<send block>, reverse=true, raw=true, count=2` the head block is entry 0.
  Backwards from `head` the head block is also entry 0 and `previous` is returned.
  `reverse` renames `previous` to `next`, and both appear only when truncated.
- `subtype` is present on **state blocks only**. In a 281-block raw history the 56
  legacy blocks (types `send`, `open`, `change`) carried no `subtype` at all.
- `pending` and `active_difficulty` both still work and both return a
  `deprecated: true` field, confirming both are deprecated.

## Undocumented, observed

- `block_info` returns **`topo_height`**, which the RPC protocol page does not
  document. It passes through; the gateway does not declare it.
- `version` returns `requestsLimit`, `requestsRemaining` and `requestLimitReset`
  alongside the documented fields. All three are provider-added rate-limit
  headers-as-fields. The gateway does not declare them.
- `account_balance` returns `pending` even though it was not requested, matching
  the documented backwards-compatibility behaviour. It therefore passes through
  under the gateway's pass-through rule even though the schema omits it.

## `receivable` result shape selection, observed

The page shows three shapes and says nothing about combinations. Observed on
`nano_1ipx847tk8o46pwxt5qjdbncjqcbwcc1rrmqnkztrfjy5k7z4imsrata9est`:

| parameters | shape |
| --- | --- |
| none | array of hashes |
| `source` | map, hash to `{amount, source}` |
| `threshold` | map, hash to amount |
| `sorting` alone | **map, hash to amount** |
| `min_version` alone | array of hashes |
| `include_active` alone | array of hashes |
| `threshold` + `source` | map, hash to `{amount, source}` |
| `sorting` + `source` | map, hash to `{amount, source}` |
| `sorting` + `threshold` | map, hash to amount |

Two rules the page does not state:

1. **`source` wins** over `threshold` and `sorting`.
2. **`sorting` alone changes the shape** to hash-to-amount, because sorting needs
   the amounts to sort by.

A third field `min_version` appeared inside the source shape under some
combinations and not others. It is not documented and was not reproducible, so it
is recorded as unstable rather than as a rule.

## Provider action policy, observed

This provider refuses several documented actions with
`{"error": "Action <name> not allowed"}`:

- `accounts_receivable`
- `confirmation_history`
- `representatives`
- `accounts_representatives`
- `telemetry`

while allowing `accounts_balances`, `accounts_frontiers`, `accounts_receivable`'s
single-account sibling `receivable`, `block_confirm` and `account_history`. This
is a provider allowlist, not `enable_control`: nothing custodial is involved.

The gateway's error contract handles this correctly without special-casing. The
node reports the refusal as its own `error`, so the gateway returns the node's
string verbatim under the single fixed code. A caller cannot distinguish a
provider policy refusal from any other node error, which is faithful and is also a
real limitation for Scenario 3 on this provider.

# Nano RPC Gateway E2E suite

This is an opt-in live suite for the gateway's current Clean V28.2 surface. It
uses `uv` and `pytest`, and its JSON-RPC calls go through the Python client
generated from `../generated/nano-node-v28.2.openrpc.json` by
`openrpcclientgenerator`.

## First-time setup (manual, outside the suite)

Create two dedicated OWS wallets and fund wallet A separately. The suite does
not create wallets, export secrets, fund accounts, or automatically receive
funds:

```bash
ows wallet create --name nano-gateway-e2e-a
ows wallet create --name nano-gateway-e2e-b
xno-skills send --wallet <funded-wallet> \
  --to <wallet-a-address> --amount-xno 0.001
xno-skills receive --wallet nano-gateway-e2e-a
```

Wallet names and addresses are supplied to the test process through
environment variables; no mnemonic or private key belongs in this repository.

## Generate and run

```bash
./e2e/generate-client.sh
cd e2e
uv sync --locked
uv run pytest -m 'not live'
```

The generated client is checked in for review. Regenerate it after a reviewed
OpenRPC artifact change with `./e2e/generate-client.sh` and inspect the diff.
The generator's current model cannot parse valid `$ref` entries in method
`errors`, so the generation wrapper removes only those descriptors in memory;
all methods, parameters, result schemas, and the source artifact digest remain
derived from the checked-in document. It also removes unused parameter-schema
imports emitted by the generator, preserving the generated method signatures.

To run read-only live coverage:

```bash
NANO_E2E_RUN_LIVE=1 uv run pytest -m live
```

To permit the send/receive journey, explicitly add mutation opt-in and the two
wallet names (defaults match the setup above):

```bash
NANO_E2E_RUN_LIVE=1 NANO_E2E_RUN_MUTATIONS=1 \
  NANO_E2E_WALLET_A=nano-gateway-e2e-a \
  NANO_E2E_WALLET_B=nano-gateway-e2e-b \
  uv run pytest -m 'live and mutation'
```

The mutation test requires the gateway's configured `node_ws_urls` to deliver
confirmation frames for the same Nano network as its `node_rpc_urls`. Pair
each RPC URL with its corresponding WebSocket URL. Health and readiness do not
prove confirmation delivery; if the exact send or receive event does not
arrive, the test fails after its timeout. Reconcile pending funds before
retrying a failed mutation run.

Useful overrides are `NANO_E2E_RPC_URL` (default
`http://127.0.0.1:8090/rpc`), `NANO_E2E_EVENTS_URL` (default derived from the
RPC URL), `NANO_E2E_WALLET_RPC_URL` (optional Nano node RPC URL passed to
`xno-skills`), `NANO_E2E_TRANSFER_XNO` (default `0.000001`), and
`NANO_E2E_BEARER_TOKEN` for a gateway that protects `process`.

Tests skip with an explicit reason when live mode, mutation mode, wallets, or
the configured gateway is unavailable. They never treat SSE alone as balance
proof: after each event the test reconciles through `account_info`,
`account_balance`, `receivable`, and `account_history`.

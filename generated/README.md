# Generated V28.2 client artifacts

These files are generated from the gateway's Clean V28.2 OpenRPC document with
the pinned @open-rpc/generator@2.1.1 client generator. Do not edit the client
source manually.

Run `make client` after reviewing a schema release; it regenerates offline from
`docs/api-docs/openrpc.json` with the pinned generator and compiler installed by
`npm --prefix docs ci`. `make docs-check` fails if these files drift from the
schema. The generated client expects the official OpenRPC client-js runtime and
its normal TypeScript dependencies.

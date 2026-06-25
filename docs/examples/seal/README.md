# Encrypting data with Seal and Walrus

A minimal, runnable example showing the full round trip: encrypt with
[Seal](https://seal-docs.wal.app/), store the ciphertext on Walrus, read it
back, and decrypt it.

The narrative walkthrough lives in the Walrus docs under **Encrypting data with
Seal**. This directory holds the code that page imports.

## Files

- `config.ts` — network, policy IDs, and the shared Sui/Seal/Walrus clients.
- `funded-keypair.ts` — a Testnet keypair with SUI and WAL for the example.
- `encrypt-and-store.ts` — encrypt a message and store it on Walrus.
- `read-and-decrypt.ts` — read it back and decrypt it.

## Prerequisites

1. Deploy an access-control policy that exposes a `seal_approve` function. This
   example uses the `allowlist` pattern from the Seal repository:
   <https://github.com/MystenLabs/seal/blob/main/examples/move/sources/allowlist.move>.
   Publish it, create an allowlist, and add your address to it.
2. Put the published package ID and allowlist object ID into `config.ts`.
3. Acquire Testnet SUI (faucet) and WAL (`walrus get-wal`).

## Run

```sh
npm install
SUI_PRIVATE_KEY=suiprivkey1... npm run encrypt
SUI_PRIVATE_KEY=suiprivkey1... npm run decrypt -- <blobId>
```

> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

Use when the client must handle embedding calls and local SEAL operations. The relayer still handles
upload relay, vector registration, search, and restore.

This is the recommended path for Web3-native users who want to minimize trust in the relayer, it never sees your plaintext data.

## What the client handles vs. what the relayer handles

| Operation | Client (MemWalManual) | Relayer |
|-----------|----------------------|---------|
| Embedding | Client calls OpenAI/compatible API |, |
| SEAL encryption | Client encrypts locally |, |
| Walrus upload |, | Server uploads through sidecar (server pays gas) |
| Vector registration |, | Server stores `{blob_id, vector}` in PostgreSQL |
| Recall search |, | Server searches vectors, returns `{blob_id, distance}` |
| Walrus download | Client downloads from aggregator |, |
| SEAL decryption | Client decrypts locally (SessionKey) |, |

## Setup

```ts
import { MemWalManual } from "@mysten-incubation/memwal/manual";

const manual = MemWalManual.create({
  key: "<your-ed25519-private-key>",
  serverUrl: "https://your-relayer-url.com",
  suiPrivateKey: "<your-sui-private-key>",    // OR walletSigner
  embeddingApiKey: "<your-openai-api-key>",
  packageId: "<memwal-package-id>",
  accountId: "<memwal-account-id>",
  namespace: "chatbot-prod",
});
```

## Core methods

```ts
// Embed locally, encrypt locally, relay encrypted payload + vector
await manual.rememberManual("User prefers dark mode.");

// Embed locally, search via relayer, download and decrypt locally
const result = await manual.recallManual("What do we know?", 5);
for (const memory of result.results) {
  console.log(memory.text, memory.distance);
}

// Same relayer restore endpoint
await manual.restore("chatbot-prod", 10);

// Check if using a connected wallet signer
console.log(manual.isWalletMode);
```

## Remember flow (under the hood)

1. Client generates embedding through OpenAI-compatible API
2. Client SEAL-encrypts the plaintext locally (no wallet signature needed)
3. Client sends `{encrypted_data (base64), vector}` to the relayer
4. Relayer uploads encrypted bytes to Walrus through upload-relay sidecar (server pays gas)
5. Relayer stores `{blob_id, vector, owner, namespace}` in PostgreSQL

## Recall flow (under the hood)

1. Client generates query embedding through OpenAI-compatible API
2. Client sends the vector to the relayer
3. Relayer searches PostgreSQL and returns `{blob_id, distance}` hits
4. Client downloads all matching encrypted blobs from Walrus concurrently
5. Client creates a single SEAL SessionKey (one wallet popup in browser mode)
6. Client decrypts each blob locally using the shared session key

## Browser integration (wallet signer)

Use `walletSigner` instead of `suiPrivateKey` when integrating with a connected wallet (for example, , `@mysten/dapp-kit`):

```ts
const manual = MemWalManual.create({
  key: "<your-ed25519-delegate-key>",
  walletSigner: {
    address: walletAddress,
    signAndExecuteTransaction: signAndExecuteTransaction,
    signPersonalMessage: signPersonalMessage,
  },
  embeddingApiKey: "<your-openai-api-key>",
  packageId: "<memwal-package-id>",
  accountId: "<memwal-account-id>",
});
```

## Config notes

- `suiNetwork` defaults to `mainnet`
- `sealServerConfigs` lets the client configure independent or committee SEAL servers; committee entries require `aggregatorUrl`
- `sealKeyServers` remains supported as a legacy independent key server object ID override
- Walrus publisher, aggregator, and upload relay defaults follow `suiNetwork`
- `embeddingModel` defaults to `text-embedding-3-small` (or `openai/text-embedding-3-small` for OpenRouter)
- `walrusEpochs` defaults to `50` (storage duration)
- All `@mysten/*` peer dependencies are loaded dynamically, users who only use the default `MemWal` client don't need them installed
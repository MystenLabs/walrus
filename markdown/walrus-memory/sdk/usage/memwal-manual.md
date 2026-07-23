> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

Use when the client must handle embedding calls and local Seal operations. The relayer still handles
upload relay, vector registration, search, and restore.

This is the recommended path for Web3-native users who want to minimize trust in the relayer, because it never sees your plaintext data.

## What the client handles versus what the relayer handles

| Operation | Client (MemWalManual) | Relayer |
|-----------|----------------------|---------|
| Embedding | Client calls OpenAI/compatible API | |
| Seal encryption | Client encrypts locally | |
| Walrus upload | | Server uploads through a sidecar (server pays gas) |
| Vector registration | | Server stores `{blob_id, vector}` in PostgreSQL |
| Recall search | | Server searches vectors, returns `{blob_id, distance}` |
| Walrus download | Client downloads from aggregator | |
| Seal decryption | Client decrypts locally (SessionKey) | |

## Setup

[Source: sdk/usage/memwal-manual.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/sdk/usage/memwal-manual.md)

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

[Source: sdk/usage/memwal-manual.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/sdk/usage/memwal-manual.md)

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
2. Client Seal-encrypts the plaintext locally (no wallet signature needed)
3. Client sends `{encrypted_data (base64), vector}` to the relayer
4. Relayer uploads encrypted bytes to Walrus through upload-relay sidecar (server pays gas)
5. Relayer stores `{blob_id, vector, owner, namespace}` in PostgreSQL

## Recall flow (under the hood)

1. Client generates query embedding through OpenAI-compatible API
2. Client sends the vector to the relayer
3. Relayer searches PostgreSQL and returns `{blob_id, distance}` hits
4. Client downloads all matching encrypted blobs from Walrus concurrently
5. Client creates a single Seal SessionKey (one wallet popup in browser mode)
6. Client decrypts each blob locally using the shared session key

## Browser integration (wallet signer)

Use `walletSigner` instead of `suiPrivateKey` when integrating with a connected wallet (for example, `@mysten/dapp-kit`):

[Source: sdk/usage/memwal-manual.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/sdk/usage/memwal-manual.md)

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
- `sealServerConfigs` lets the client configure independent or committee Seal servers; committee entries require `aggregatorUrl`
- `sealKeyServers` remains supported as a legacy independent key server object ID override
- Walrus publisher, aggregator, and upload relay defaults follow `suiNetwork`
- `embeddingModel` defaults to `text-embedding-3-small` (or `openai/text-embedding-3-small` for OpenRouter)
- `walrusEpochs` defaults to `50` (storage duration)
- All `@mysten/*` peer dependencies are loaded dynamically, so users who only use the default `MemWal` client do not need them installed

## Agent state: holding your own keys

Every blob on Walrus is public, so you must encrypt private agent state before storing it. For an autonomous agent that has no human to approve a wallet popup, `MemWalManual` is the path that keeps the agent in control of its own key material: encryption happens client-side, and the relayer never receives plaintext.

A headless agent signs Seal and Walrus operations with its own Sui key rather than a connected wallet. Provide `suiPrivateKey` instead of `walletSigner`, set `suiNetwork` for your environment, and load every secret from the environment:

[Source: sdk/usage/memwal-manual.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/sdk/usage/memwal-manual.md)

```ts
import { MemWalManual } from "@mysten-incubation/memwal/manual";

const manual = MemWalManual.create({
  key: process.env.MEMWAL_KEY!,
  accountId: process.env.MEMWAL_ACCOUNT_ID!,
  packageId: process.env.MEMWAL_PACKAGE_ID!,
  serverUrl: "https://relayer-staging.memory.walrus.xyz",
  suiPrivateKey: process.env.SUI_PRIVATE_KEY!,
  embeddingApiKey: process.env.OPENAI_API_KEY!,
  suiNetwork: "testnet",
  namespace: "agent-state",
});

// Encrypts locally, then relays only ciphertext to the server.
await manual.rememberManual("Private: internal risk score for account 0xabc is 0.82.");

const hits = await manual.recallManual("risk score for account 0xabc", 5);
for (const memory of hits.results) {
  console.log(memory.text, memory.distance);
}
```

The agent holds two distinct secrets:

- The **delegate key** (`key`) authenticates the agent to the relayer.
- The **Sui key** (`suiPrivateKey`) signs the Seal and Walrus operations that encrypt and store the data.

> **Warning**
>
> With client-managed encryption the agent owns its key material. If the Sui key is lost, you cannot recover the encrypted memories, so treat it as a production secret and rotate the delegate key through the dashboard if it might be exposed.
For the full headless write, confirm, and recall loop that builds on this, see [Agent Storage Loop](/walrus-memory/sdk/agent-storage-loop).
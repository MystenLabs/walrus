> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

See also:

- [Configuration](/walrus-memory/reference/configuration)
- [Relayer API](/walrus-memory/relayer/api-reference)

## `MemWal.create(config)`

[Source: sdk/api-reference.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/sdk/api-reference.md)

```ts
MemWal.create(config: MemWalConfig): MemWal
```

Config:

| Property | Type | Required | Default | Notes |
| --- | --- | --- | --- | --- |
| `key` | `string` | Yes | | Ed25519 delegate private key in hex |
| `accountId` | `string` | Yes | | MemWalAccount object ID on Sui |
| `serverUrl` | `string` | No | `https://relayer.memory.walrus.xyz` | Relayer URL |
| `namespace` | `string` | No | `"default"` | Default namespace for memory isolation |

For the full config surface, see [Configuration](/walrus-memory/reference/configuration).

### `remember(text, namespace?): Promise<RememberAcceptedResult>`

Submit one memory through the relayer. The method returns after the relayer creates a background job; embedding, Seal encryption, Walrus upload, and vector indexing continue asynchronously.

**Returns:**

[Source: sdk/api-reference.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/sdk/api-reference.md)

```ts
{
  job_id: string; // Polling id
  status: string; // Usually "running"
}
```

### `rememberAndWait(text, namespace?, opts?): Promise<RememberResult>`

Submit one memory and poll until the background job completes.

**Returns:**

[Source: sdk/api-reference.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/sdk/api-reference.md)

```ts
{
  id: string;        // Stable job id/vector row id
  job_id: string;    // Polling id
  blob_id: string;   // Walrus blob ID
  owner: string;     // Owner Sui address
  namespace: string; // Namespace used
}
```

### `waitForRememberJob(jobId, opts?): Promise<RememberResult>`

Poll a previously accepted remember job until it reaches `done` or `failed`.

### `rememberBulk(items): Promise<RememberBulkAcceptedResult>`

Submit up to 20 memories in one request and return the accepted job IDs immediately.

**Returns:**

[Source: sdk/api-reference.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/sdk/api-reference.md)

```ts
{
  job_ids: string[];
  total: number;
  status: string; // Usually "running"
}
```

### `rememberBulkAndWait(items, opts?): Promise<RememberBulkResult>`

Submit a bulk remember request and wait until every job reaches a terminal state.

### `recall(params): Promise<RecallResult>`

Search for memories matching a natural language query, scoped to `owner + namespace`.

- Preferred form: `recall({ query, limit?, topK?, namespace?, maxDistance? })`
- `limit` defaults to `10`; `topK` is an alias and wins when both are set
- Legacy positional forms still work: `recall(query)`, `recall(query, limit)`, `recall(query, limit, namespace)`, and `recall(query, options)`
- `maxDistance` filters weak matches client-side by dropping results where `distance >= maxDistance`

**Returns:**

[Source: sdk/api-reference.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/sdk/api-reference.md)

```ts
{
  results: Array<{
    blob_id: string;   // Walrus blob ID
    text: string;      // Decrypted plaintext
    distance: number;  // Cosine distance (lower = more similar)
  }>;
  total: number;
}
```

`distance` is cosine distance, lower is more similar.

### `analyze(text, namespace?): Promise<AnalyzeResult>`

Extract memorable facts from text using an LLM, then return accepted background jobs for storing each fact.

**Returns:**

[Source: sdk/api-reference.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/sdk/api-reference.md)

```ts
{
  job_ids: string[];
  facts: Array<{
    text: string;     // Extracted fact
    id: string;       // Same value as job_id
    job_id: string;   // Polling id
  }>;
  fact_count: number;
  status: string;     // Usually "pending"
  owner: string;
}
```

Use `analyzeAndWait(text, namespace?, opts?)` to wait for every extracted fact job to finish and return per-job storage results.

### `restore(namespace, limit?): Promise<RestoreResult>`

Rebuild missing indexed entries for one namespace from Walrus. Incremental, only re-indexes blobs that aren't already in the local database.

- `limit` defaults to `10`

**Returns:**

[Source: sdk/api-reference.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/sdk/api-reference.md)

```ts
{
  restored: number;   // Entries newly indexed
  skipped: number;    // Entries already in DB
  total: number;      // Total blobs found on-chain
  namespace: string;
  owner: string;
}
```

### `health(): Promise<HealthResult>`

Check relayer health. Does not require authentication.

**Returns:** `{ status: string, version: string, relayerVersion?: string, apiVersion?: string, minSupportedSdk?: ... }`

### `compatibility(): Promise<RelayerVersionMetadata>`

Fetch and validate the relayer compatibility contract from `/version`. Protected SDK calls run this check before signing the first request and raise `MemWalCompatibilityError` when the SDK/relayer pair is unsupported.

### `getPublicKeyHex(): Promise<string>`

Return the hex-encoded public key for the current delegate key.

### Lower-level methods

These exist on the `MemWal` class for advanced use cases:

| Method | Description |
|--------|-------------|
| `rememberManual({ blobId, vector, namespace? })` | Register a pre-uploaded blob ID with a pre-computed vector |
| `recallManual({ vector, limit?, namespace? })` | Search with a pre-computed query vector (returns blob IDs, no decryption) |
| `embed(text)` | Generate an embedding vector for text (no storage) |

## `MemWalManual`

[Source: sdk/api-reference.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/sdk/api-reference.md)

```ts
import { MemWalManual } from "@mysten-incubation/memwal/manual";
```

See [MemWalManual usage](/walrus-memory/sdk/usage/memwal-manual) for the full setup and flow details.

### `rememberManual(text, namespace?): Promise<RememberManualResult>`

Embed locally, Seal encrypt locally, send encrypted payload + vector to relayer for Walrus upload and vector registration.

### `recallManual(query, limit?, namespace?): Promise<RecallManualResult>`

Embed locally, search through relayer, download from Walrus, Seal decrypt locally. Returns decrypted text results.

### `restore(namespace, limit?): Promise<RestoreResult>`

Same as `MemWal.restore()`, delegates to the relayer.

### `isWalletMode: boolean`

Whether this client uses a connected wallet signer (vs. raw keypair).

### Config notes

- `suiNetwork` defaults to `mainnet`
- `sealServerConfigs` lets the client configure independent or committee Seal servers; committee entries require `aggregatorUrl`
- `sealKeyServers` remains supported as a legacy independent key server object ID override
- All `@mysten/*` peer dependencies are loaded dynamically, only needed if you use `MemWalManual`

## `withMemWal`

[Source: sdk/api-reference.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/sdk/api-reference.md)

```ts
import { withMemWal } from "@mysten-incubation/memwal/ai";
```

Wraps a Vercel AI SDK model with automatic memory recall and save.

**Before generation:**
- Reads the last user message
- Runs `recall()` against Walrus Memory
- Filters by minimum relevance (`minRelevance`, default `0.3`)
- Injects matching memories into the prompt as a system message

**After generation:**
- Optionally runs `analyze()` on the user message (fire-and-forget)
- Saves extracted facts asynchronously

**Options** (extends `MemWalConfig`):

| Option | Default | Description |
|--------|---------|-------------|
| `maxMemories` | `5` | Max memories to inject per request |
| `autoSave` | `true` | Auto-save new facts from conversation |
| `minRelevance` | `0.3` | Minimum similarity score (0–1) to include a memory |
| `debug` | `false` | Enable debug logging |

See [Configuration](/walrus-memory/reference/configuration) for all options.

## Account management

[Source: sdk/api-reference.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/sdk/api-reference.md)

```ts
import {
  createAccount,
  addDelegateKey,
  removeDelegateKey,
  generateDelegateKey,
} from "@mysten-incubation/memwal/account";
```

| Function | Description |
|----------|-------------|
| `generateDelegateKey()` | Generate a new Ed25519 keypair (returns `privateKey`, `publicKey`, `suiAddress`) |
| `createAccount(opts)` | Create a new MemWalAccount onchain (one per Sui address) |
| `addDelegateKey(opts)` | Add a delegate key to an account (owner only) |
| `removeDelegateKey(opts)` | Remove a delegate key from an account (owner only) |

## Utility functions

[Source: sdk/api-reference.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/sdk/api-reference.md)

```ts
import { delegateKeyToSuiAddress, delegateKeyToPublicKey } from "@mysten-incubation/memwal";
```

| Function | Description |
|----------|-------------|
| `delegateKeyToSuiAddress(privateKeyHex)` | Derive the Sui address from a delegate private key |
| `delegateKeyToPublicKey(privateKeyHex)` | Get the 32-byte public key from a delegate private key |
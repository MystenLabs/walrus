> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

The recommended default client. The relayer handles embeddings, SEAL encryption, Walrus upload, and vector indexing.

## How it works

1. The SDK signs each request with your delegate key
2. The relayer verifies delegate access
3. `remember` returns an accepted job while the relayer encrypts, uploads, and indexes in the background
4. `recall` searches by Memory Space and returns decrypted matches

```ts
import { MemWal } from "@mysten-incubation/memwal";

const memwal = MemWal.create({
  key: "<your-ed25519-private-key>",
  accountId: "<your-memwal-account-id>",
  serverUrl: "https://your-relayer-url.com",
  namespace: "chatbot-prod",
});
```

## Core methods

```ts
// Store a memory
const job = await memwal.remember("User prefers dark mode and works in TypeScript.");
await memwal.waitForRememberJob(job.job_id);

// Recall relevant memories
const result = await memwal.recall({ query: "What do we know about this user?", limit: 5 });

// Extract and store facts from longer text
const analyzed = await memwal.analyze(
  "I live in Hanoi, prefer dark mode, and usually work late at night."
);
console.log(analyzed.job_ids);

// Check relayer health
await memwal.health();
```

## Restore

Rebuild missing indexed entries for one namespace. Incremental, namespace-scoped, and meant to
repair PostgreSQL vector state from Walrus-backed memory.

```ts
const result = await memwal.restore("chatbot-prod", 10);
```

## Lower-level methods

Use these when you already have a vector or encrypted payload:

- `rememberManual({ blobId, vector, namespace? })`
- `recallManual({ vector, limit?, namespace? })`
- `embed(text)`
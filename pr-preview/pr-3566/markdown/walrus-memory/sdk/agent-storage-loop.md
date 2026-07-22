> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

This guide walks an autonomous agent through the complete storage loop on Testnet: set up the client with no interactive steps, batch many small writes, encrypt agent state, confirm each write landed before depending on it, then recall. Every step uses the real SDK surface, so you can lift the final script straight into a server or agent runtime.

The loop has four stages:

1. **Set up** the client from environment variables, with no browser or prompt in the path.
2. **Write** many small state blobs in one batched call.
3. **Confirm** each write reached a durable `done` state before the agent acts on it.
4. **Recall** the stored context back into the agent.

## Set up in an agent runtime

An agent runtime has no human to click through a wallet or paste a key at a prompt. Generate the account ID and delegate key once at [staging.memory.walrus.xyz](https://staging.memory.walrus.xyz) for Testnet, store them as secrets, and load them from the environment at startup.

```ts agent.ts
import { MemWal } from "@mysten-incubation/memwal";

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}

const memwal = MemWal.create({
  key: requireEnv("MEMWAL_KEY"),
  accountId: requireEnv("MEMWAL_ACCOUNT_ID"),
  // Testnet relayer. Use https://relayer.memory.walrus.xyz for Mainnet.
  serverUrl: "https://relayer-staging.memory.walrus.xyz",
  namespace: "agent-state",
});

// Fail fast at boot if the relayer is unreachable or the key is rejected,
// rather than discovering it on the first write mid-run.
await memwal.health();
```

> **Note**
>
> Read every credential from the environment. Never hardcode an account ID copied from docs or another project: recall is scoped per **account plus namespace**, so a shared ID puts your agent's memories in a space other readers can see.
The `MemWal.create` config takes four fields:

| Property | Type | Required | Description |
| --- | --- | --- | --- |
| `key` | `string` | Yes | Ed25519 delegate private key in hex |
| `accountId` | `string` | Yes | MemWalAccount object ID on Sui |
| `serverUrl` | `string` | No | Relayer URL for your network |
| `namespace` | `string` | No | Default namespace, falls back to `"default"` |

## Batch many small writes with quilt

Agents tend to produce many small state blobs: observations, intermediate results, per-step notes. Writing them one at a time means a separate round trip and Sui transaction set for each. The SDK batches them instead, backed by Walrus Quilt under the hood.

`rememberBulk` accepts up to 20 items per call. The relayer embeds and Seal-encrypts every item concurrently and uploads them to Walrus in parallel, so the whole batch shares far fewer Sui transactions than writing each item on its own would. For an agent writing dozens of small memories a minute, that is the difference between a workable cost profile and an unworkable one.

```ts
const items = [
  { text: "Observed: user prefers concise summaries." },
  { text: "Step 1 result: parsed 42 records, 3 flagged." },
  { text: "Hypothesis: the spike correlates with the Tuesday deploy." },
];

// Fire the batch and wait for every item to finish.
const result = await memwal.rememberBulkAndWait(items);
```

> **Note**
>
> Keep each batch at 20 items or fewer. If you have more, chunk the array and call `rememberBulkAndWait` per chunk. To fan out without blocking, use `rememberBulkAsync` and collect the returned `job_ids` to confirm later.
## Encrypt agent state

All blobs on Walrus are public, so you must encrypt private agent state. There are two models, and which one you want depends on who should hold the keys.

1. **Relayer-managed encryption (default):** With the standard `MemWal` client used above, the relayer encrypts every item with Seal before it reaches Walrus. You do not manage keys, and this is the right default for most agents.

2. **Client-managed encryption.** When the agent must hold its own keys and never delegate decryption to the relayer, use the manual entry point. `MemWalManual` performs Seal encryption client-side, so plaintext never leaves the agent process.

```ts
import { MemWalManual } from "@mysten-incubation/memwal/manual";

const manual = MemWalManual.create({
  key: requireEnv("MEMWAL_KEY"),
  accountId: requireEnv("MEMWAL_ACCOUNT_ID"),
  packageId: requireEnv("MEMWAL_PACKAGE_ID"),
  serverUrl: "https://relayer-staging.memory.walrus.xyz",
  // The agent signs SEAL and Walrus operations with its own Sui key, no wallet popup.
  suiPrivateKey: requireEnv("SUI_PRIVATE_KEY"),
  embeddingApiKey: requireEnv("OPENAI_API_KEY"),
  suiNetwork: "testnet",
  namespace: "agent-state",
});

// Encrypts locally, then relays the ciphertext. The relayer never sees plaintext.
await manual.rememberManual("Private: internal risk score for account 0xabc is 0.82.");
```

> **Warning**
>
> Client-managed encryption means the agent is responsible for its key material. If the delegate key is lost, you cannot recover the encrypted memories. Treat the key as you would any production secret, and rotate it through the dashboard if it might be exposed.
For the full client-managed flow, including local embeddings and recall, see [MemWalManual](/walrus-memory/sdk/usage/memwal-manual).

## Confirm a write before depending on it

An autonomous agent should not act on a memory it only believes it wrote. Before the agent reads state back or makes a decision that assumes the write persisted, confirm that the write reached a durable state.

The `*AndWait` helpers already block until the relayer reports each job `done`, which is the signal that the relayer embedded, encrypted, and uploaded the memory to Walrus. When you need to write without blocking and confirm later, capture the job IDs and wait on them explicitly:

```ts
const accepted = await memwal.rememberBulkAsync(items);

// ... agent does other work ...

// Block until every job reaches `done` before relying on the memories.
const settled = await memwal.waitForRememberJobs(accepted.job_ids);
const failed = settled.results.filter((r) => r.status !== "done");
if (failed.length > 0) {
  throw new Error(`${failed.length} memories did not persist; do not proceed.`);
}
```

> **Note**
>
> A dedicated `verify()` helper that reconstructs a memory from its onchain blob object is on the roadmap. Until it ships, a job reaching `done` is the durability signal to gate on, and the memory's Walrus blob object on Sui is the onchain record of the write.
## Recall the stored context

After you confirm the writes, recall pulls the relevant memories back by semantic similarity. The relayer verifies the request, embeds the query, searches, downloads from Walrus, decrypts, and returns plaintext.

```ts
const recalled = await memwal.recall({
  query: "What do we know about the Tuesday spike?",
  limit: 5,
});

for (const memory of recalled.results) {
  console.log(memory.distance.toFixed(3), memory.text);
}
```

Recall is scoped to the client's namespace by default. Pass `namespace` to read from a different one, and `limit` to cap how many memories return.

## The full loop

Putting the four stages together, here is a complete headless agent that runs end to end on Testnet:

```ts agent.ts
import { MemWal } from "@mysten-incubation/memwal";

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}

async function main() {
  // 1. Set up from the environment, no interactive steps.
  const memwal = MemWal.create({
    key: requireEnv("MEMWAL_KEY"),
    accountId: requireEnv("MEMWAL_ACCOUNT_ID"),
    serverUrl: "https://relayer-staging.memory.walrus.xyz",
    namespace: "agent-state",
  });
  await memwal.health();

  // 2. Batch many small state blobs in one call.
  const items = [
    { text: "Observed: user prefers concise summaries." },
    { text: "Step 1 result: parsed 42 records, 3 flagged." },
    { text: "Hypothesis: the spike correlates with the Tuesday deploy." },
  ];

  // 3. Write and confirm every item reached `done` before continuing.
  const settled = await memwal.rememberBulkAndWait(items);
  const failed = settled.results.filter((r) => r.status !== "done");
  if (failed.length > 0) {
    throw new Error(`${failed.length} memories did not persist; aborting.`);
  }

  // 4. Recall the stored context.
  const recalled = await memwal.recall({
    query: "What do we know about the Tuesday spike?",
    limit: 5,
  });
  for (const memory of recalled.results) {
    console.log(memory.distance.toFixed(3), memory.text);
  }
}

main().catch((err) => {
  console.error("Agent storage loop failed:", err);
  process.exit(1);
});
```

## Next steps

- [Walrus Memory client](/walrus-memory/sdk/usage/memwal): full method signatures for the default client
- [MemWalManual](/walrus-memory/sdk/usage/memwal-manual): the client-managed encryption flow
- [Public relayer](/walrus-memory/relayer/public-relayer): managed Mainnet and Testnet relayer endpoints
- [API Reference](/walrus-memory/sdk/api-reference): every config field and return type
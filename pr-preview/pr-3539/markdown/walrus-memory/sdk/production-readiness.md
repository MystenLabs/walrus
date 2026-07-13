> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

The SDK gives you the storage primitives. Running them in a long-lived agent, where there is no human to retry a failed write or notice a runaway bill, takes a few patterns on top. This guide collects the ones that matter most.

Several of these are patterns you implement around the client today. Where a capability is on the roadmap as a native feature, this guide says so, so you know which code is permanent and which is a stopgap.

## Make writes idempotent

The relayer does not deduplicate writes. A `remember` call that retries after a network blip, or fired twice by an at-least-once job queue, stores the same text twice and pollutes later recall. Until content-based deduplication is available natively, gate writes on a key your agent controls so a repeat is a no-op.

```ts
import { createHash } from "crypto";

const written = new Set<string>(); // back this with Redis or a DB in production

async function rememberOnce(memwal: MemWal, text: string, namespace?: string) {
  const id = createHash("sha256").update(`${namespace ?? "default"}:${text}`).digest("hex");
  if (written.has(id)) return; // already stored this exact memory

  const job = await memwal.remember(text, namespace);
  await memwal.waitForRememberJob(job.job_id);
  written.add(id);
}
```

> **Note**
>
> Persist the idempotency set outside the process (Redis, a database row, a Sui object), not in memory. An in-memory set resets on restart, which is exactly when a retry storm is most likely.
## Retry with backoff, but only retryable failures

The client does not retry for you. Wrap calls in exponential backoff, and be careful to retry only failures that a retry can fix. A transient network error or relayer timeout is worth retrying. A `401 AUTH_REJECTED` is a configuration problem that fails identically on every attempt, so retrying it just delays the real fix.

```ts
async function withRetry<T>(fn: () => Promise<T>, attempts = 4): Promise<T> {
  let lastErr: unknown;
  for (let i = 0; i < attempts; i++) {
    try {
      return await fn();
    } catch (err) {
      const status = (err as { status?: number }).status;
      // Do not retry auth or client errors; they will not succeed on a retry.
      if (status === 401 || (status && status >= 400 && status < 500)) throw err;
      lastErr = err;
      await new Promise((r) => setTimeout(r, 2 ** i * 500 + Math.random() * 250));
    }
  }
  throw lastErr;
}

await withRetry(() => rememberOnce(memwal, "Observed: deploy at 14:03 UTC."));
```

Pairing retry with the idempotency guard above is deliberate: a retry that fires after the write actually succeeded server-side stays safe, because `rememberOnce` short-circuits on the key.

> **Note**
>
> Built-in retry and backoff inside the client is on the roadmap. When it ships, you can drop the wrapper for the calls it covers and keep only the idempotency guard.
## Confirm durability before acting on a memory

An autonomous agent should not make a decision that assumes a write persisted until it confirms the write reached a terminal state. The `*AndWait` helpers block until each job reports `done`; when you write without blocking, capture the job IDs and wait on them before depending on the data.

```ts
const accepted = await memwal.rememberBulkAsync(items);
const settled = await memwal.waitForRememberJobs(accepted.job_ids);

if (settled.failed > 0) {
  const bad = settled.results.filter((r) => r.status !== "done");
  throw new Error(`${settled.failed} writes did not persist: ${bad.map((b) => b.status).join(", ")}`);
}
```

> **Warning**
>
> A job reaching `done` confirms that the relayer stored the memory, but the vector index can briefly lag behind that signal, so a `recall` fired in the same instant might not return the memory yet. For read-after-write critical paths, tolerate a short delay or re-query rather than treating an empty first result as a missing memory.
For the full write, confirm, and recall sequence in context, see [Agent Storage Loop](/walrus-memory/sdk/agent-storage-loop).

## Bound cost

Every write registers storage on Walrus and costs gas and WAL. An agent in a tight loop can run up spend quickly, so put ceilings in the agent, not just in your head.

- **Batch with Quilt.** `rememberBulkAndWait` (up to 20 items) collapses many small writes into far fewer transactions. Buffer small state blobs and flush them as a batch instead of writing one at a time.
- **Do not store what you never recall.** Ephemeral scratch state that never feeds a future `recall` does not belong in durable storage. Keep it in process memory.
- **Cap writes per cycle.** Give the agent loop a budget, for example a maximum number of memories per run or per hour, and have it drop or summarize past the cap rather than write unbounded.

```ts
let writesThisCycle = 0;
const MAX_WRITES_PER_CYCLE = 50;

async function budgetedRemember(memwal: MemWal, text: string) {
  if (writesThisCycle >= MAX_WRITES_PER_CYCLE) return false;
  await rememberOnce(memwal, text);
  writesThisCycle++;
  return true;
}
```

## Custody the agent's keys

A headless agent holds secrets no human rotates by hand, so treat them with production discipline.

- The **delegate key** authenticates the agent to the relayer.
- For client-managed encryption, the **Sui key** signs the Seal and Walrus operations. See [holding your own keys](/walrus-memory/sdk/usage/memwal-manual#agent-state-holding-your-own-keys).

Load both from a secret manager, never from source or logs. Scope each agent to its own delegate key so you can revoke one without taking down the rest, and rotate through the dashboard if a key might be exposed. If the Sui key behind client-managed encryption is lost, you cannot recover the encrypted memories, so back it up with the same care as any data-encryption key.

## Degrade gracefully when memory is unavailable

A memory layer that is down should slow your agent, not stop it. Check reachability at startup and guard the memory paths so the agent keeps serving when the relayer is unreachable for a cycle.

```ts
let memory: MemWal | null = null;
try {
  memory = MemWal.create({ key, accountId, serverUrl, namespace });
  await memory.health();
} catch (err) {
  console.log("Memory unavailable, continuing without it this cycle:", err);
  memory = null;
}

// Guard every read and write on memory being live.
if (memory) {
  await budgetedRemember(memory, "...");
}
```

This is the same defensive pattern the [Cloudflare Workers guide](/walrus-memory/sdk/cloudflare-workers) uses to keep a Worker healthy when the memory dependency has a bad moment.

## Mind the recall result cap

`recall` returns up to `limit` results, defaulting to `10`. When more memories match than the cap, the relayer does not return the extra results, with no signal that it truncated the set. If a decision depends on seeing every match, set `limit` explicitly to a value above what you expect, and treat a result count equal to `limit` as a sign there might be more.

```ts
const result = await memwal.recall({ query: "open incidents", limit: 50 });
if (result.results.length === 50) {
  console.warn("Recall hit the limit; there may be more matches than returned.");
}
```

## Next steps

- [Agent Storage Loop](/walrus-memory/sdk/agent-storage-loop): the write, confirm, and recall loop these patterns harden
- [MemWalManual](/walrus-memory/sdk/usage/memwal-manual): client-managed encryption and key custody
- [Troubleshooting](/walrus-memory/troubleshooting/overview): auth, timeout, and read-after-write symptoms
- [Public relayer](/walrus-memory/relayer/public-relayer): managed Mainnet and Testnet endpoints
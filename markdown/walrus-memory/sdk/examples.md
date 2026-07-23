> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

## Basic: store and recall

The shortest working Walrus Memory example using the default relayer-backed SDK.

[Source: sdk/examples.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/sdk/examples.md)

```ts
import { MemWal } from "@mysten-incubation/memwal";

const memwal = MemWal.create({
  key: process.env.MEMWAL_PRIVATE_KEY!,
  accountId: process.env.MEMWAL_ACCOUNT_ID!,
  serverUrl: process.env.MEMWAL_SERVER_URL,
  namespace: "demo",
});

await memwal.health();

const accepted = await memwal.remember(
  "User prefers dark mode and works in TypeScript."
);
const stored = await memwal.waitForRememberJob(accepted.job_id);

const recalled = await memwal.recall({
  query: "What do we know about this user?",
  limit: 5,
});

console.log(stored.blob_id);
console.log(recalled.results);
```

What you should see:

- `health()` succeeds
- `remember()` returns a `job_id` immediately
- `waitForRememberJob()` returns a `blob_id`
- `recall()` returns plaintext results for the same namespace

### Manual registration

Use `rememberManual()` when you already have an encrypted payload plus vector, and `recallManual()`
when you already have a query vector.

### Fact extraction

Use `analyze()` when you want the relayer to extract facts from longer text and store them as
memories.

[Source: sdk/examples.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/sdk/examples.md)

```ts
const analyzed = await memwal.analyze(
  "I live in Hanoi, prefer dark mode, and usually work late at night."
);
console.log(analyzed.facts, analyzed.job_ids);
```

### AI Middleware

Use `withMemWal` when you want recall before generation and optional auto-save after generation.
See [AI Integration](/walrus-memory/sdk/ai-integration) for the full setup.

## Research app pattern

Use this when you want to store structured research findings and recall them in later sessions.

1. Submit a structured summary with `remember()` and wait for completion when immediate recall is needed
2. Generate targeted queries later
3. Use `recall()` to pull relevant findings back into context

Structured summaries usually recall better than raw transcripts because they keep the signal high.
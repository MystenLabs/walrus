> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

AI coding assistants like Cursor appear to remember a codebase because they retrieve relevant context at query time rather than holding an entire repository in the model's context window. You can build the same persistent, cross-session memory on Walrus Memory with the TypeScript SDK: store code and project context as memories, then recall the pieces that matter for each question.

## How it fits together

The loop has three parts:

1. **Scope** memory to a repository with a namespace, so one assistant can hold many codebases without mixing them.
2. **Store** chunks of code and project context as memories.
3. **Recall** the most relevant chunks by semantic similarity whenever the assistant needs context, and pass them to the model.

Set up the client first. In an editor extension or a backend service, initialize it headlessly from the environment, as described in [Headless SDK Setup](/walrus-memory/sdk/headless-setup).

## Scope memory per repository

Give each repository its own namespace so recall never crosses codebases. Derive the namespace deterministically from the repository identity, so the same repo always maps to the same space:

```ts
import { MemWal } from "@mysten-incubation/memwal";

function repoNamespace(owner: string, name: string): string {
  return `repo-${owner}-${name}`.toLowerCase();
}

const memwal = MemWal.create({
  key: process.env.MEMWAL_PRIVATE_KEY!,
  accountId: process.env.MEMWAL_ACCOUNT_ID!,
  serverUrl: "https://relayer.memory.walrus.xyz",
  namespace: repoNamespace("acme", "billing-service"),
});
```

## Store codebase context

Break code and project knowledge into focused chunks, then write them in batches. Each chunk should be small and self-contained: a function with a short note on what it does, a module summary, an architectural decision, or a convention the team follows. Include the file path and symbol name in the text so recall can match on them.

`rememberBulk` accepts up to 20 items per call, so chunk your input and send it in batches:

```ts
const chunks = [
  { text: "src/auth/session.ts - createSession(userId): issues a signed session token, 30 min TTL. Uses jose for signing." },
  { text: "src/billing/invoice.ts - Invoice totals are computed in cents as integers to avoid float drift." },
  { text: "Convention: all API handlers validate input with zod schemas in src/schemas before touching the database." },
];

// Batches of 20 or fewer. Chunk larger inputs and call once per batch.
const result = await memwal.rememberBulkAndWait(chunks);
```

For longer material such as a design document or a pull request description, `analyze` extracts discrete facts from the text and stores them as separate memories, which recall better than one large blob:

```ts
await memwal.analyzeAndWait(pullRequestDescription);
```

## Recall relevant context at query time

When the assistant is about to answer, recall the most relevant memories for the current question and add them to the prompt. `recall` embeds the query, searches the repository's namespace, and returns the closest matches by cosine distance, where a lower distance means a closer match:

```ts
const recalled = await memwal.recall({
  query: "How are session tokens issued and how long do they last?",
  limit: 5,
  // Drop weak matches so only genuinely relevant context reaches the model.
  maxDistance: 0.5,
});

const context = recalled.results.map((m) => m.text).join("\n");
// Prepend `context` to the model prompt.
```

Each result carries the matched `text`, its `distance`, and the `blob_id` that durably addresses the stored chunk. Tune `limit` and `maxDistance` to balance recall against prompt size.

## Organizing without tags

Walrus Memory scopes and groups memories by **namespace**. There is no separate tags parameter. When you need finer-grained organization than one namespace per repository, you have two options:

- **Sub-namespaces:** Encode the category into the namespace, for example `repo-acme-billing:tests` or `repo-acme-billing:api`, and pass the specific namespace on the `recall` call. This keeps categories in fully separate search spaces.
- **Keywords in the text:** Because recall is semantic, category words you include in a memory's text, for example a leading `[tests]` or `[security]`, are searchable through the query itself.

Choose sub-namespaces when categories should never be searched together, and keywords when you want one search to span them.

## When to use the SDK instead of the MCP server

Building codebase memory yourself with the SDK is the right path when you are writing an assistant or an editor integration. If instead you want to give an existing tool like Cursor access to Walrus Memory without writing code, connect the Walrus Memory MCP server. See [Cursor](/walrus-memory/mcp/cursor) for that setup. The two paths can coexist: build repository memory with the SDK, and expose it to an editor through MCP.

## References

- [Headless SDK Setup](/walrus-memory/sdk/headless-setup)
- [Walrus Memory client](/walrus-memory/sdk/usage/memwal)
- [Memory space](/walrus-memory/fundamentals/concepts/memory-space)
- [Multi-tenant cookbook](/walrus-memory/sdk/cookbook-multi-tenant)
- [Cursor MCP server](/walrus-memory/mcp/cursor)
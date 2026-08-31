> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

Your Walrus Memory account accumulates memories over time. See what you have stored, organize it with namespaces, renew memories before they expire, and delete the ones you no longer want, from the dashboard or through the SDK.

- [x] A Sui wallet that owns the memories you want to manage.
- [x] For SDK access, a delegate key and account ID from the dashboard. See [SDK Quickstart](/walrus-memory/sdk/quick-start).

Connect your wallet to open the dashboard:

| **Network** | **Dashboard** |
| --- | --- |
| Mainnet | [memory.walrus.xyz](https://memory.walrus.xyz) |
| Testnet | [staging.memory.walrus.xyz](https://staging.memory.walrus.xyz) |

## Browse and search

You do not browse memories by name or folder. You search them by meaning: give a natural language query, and recall returns the closest matches, scoped to your memory space. Recall works the same whether an agent runs it or you run it to check what you stored.

[Source: guides/manage-your-memory.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/guides/manage-your-memory.md)

```ts
const result = await memwal.recall({
  query: "What are the user's food preferences?",
  limit: 10,
});

for (const memory of result.results) {
  console.log(memory.distance.toFixed(3), memory.text);
}
```

Each result carries a `distance`, where a smaller number means a closer match. Raise `limit` to see more results per query. Recall searches only the client's namespace unless you pass a different `namespace`.

### Preview stored content

From code, `recall` returns the decrypted `text` of each match, so you can read stored content programmatically. Preview a memory before deleting it, because deletion cannot be undone; the [Delete old memories](/walrus-memory/guides/delete-old-memories) guide covers previewing content in the dashboard delete flow.

## Organize with namespaces

A namespace is a label you assign to group related memories. One account can hold many namespaces, and each one is a separate memory space that recall and restore treat independently.

Use namespaces to keep unrelated memory apart:

- `personal` for preferences, notes, and personal context.
- `work` for work knowledge and conversations.
- `research` for findings and references.

You set the namespace when you create the client, and every write and recall on that client uses it by default:

[Source: guides/manage-your-memory.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/guides/manage-your-memory.md)

```ts
const memwal = MemWal.create({
  key: process.env.MEMWAL_PRIVATE_KEY!,
  accountId: process.env.MEMWAL_ACCOUNT_ID!,
  serverUrl: process.env.MEMWAL_SERVER_URL,
  namespace: "personal",
});
```

Storing into one namespace never affects another, and recall in `personal` never returns a `work` memory. For the full model, including how the app ID adds a second isolation boundary, see [Memory Space](/walrus-memory/fundamentals/concepts/memory-space).

> **Tip**
>
> Choose namespaces before you write at scale. Because recall and restore match a namespace exactly, splitting or merging memories across namespaces later means re-writing them. A small, stable set of namespaces is easier to manage than many overlapping ones.
## Renew memories

A memory persists on Walrus for the number of epochs you paid for. An epoch is about 2 weeks on Mainnet and about 1 day on Testnet. When the epochs run out, Walrus drops the blob and the memory disappears. Renewal extends the memory's underlying Walrus `Blob` object for more epochs: the blob keeps its blob ID and its place in the relayer's index, and only its expiry epoch moves forward, so renewal never re-uploads the content.

Renewal covers two cases:

1. **Console uploads:** New assets that you upload through the console renew automatically.
2. **Self-managed and agent-owned assets:** Renew at the storage layer by extending the `Blob` object on Walrus. For how expiry and extension work, and how an autonomous agent runs an extend-before-expiry loop, see [Tracking Agent-Owned Blobs and Storage](/walrus-memory/fundamentals/architecture/tracking-agent-storage) and [How an Agent Funds Walrus Storage](/walrus-memory/fundamentals/architecture/funding-storage).

> **Warning**
>
> Renew before the expiry epoch, not after. Once a blob lapses, you cannot recover its content, and you cannot renew a lapsed memory. Track expiry epochs and renew with a margin to spare.
## Delete memories

Deletion permanently removes a memory from Walrus Memory. You cannot undo it, so preview a memory before you delete it. Delete from the dashboard or from code:

1. **Dashboard:** Review and delete stored memories from the dashboard delete flow. See [Delete old memories](/walrus-memory/guides/delete-old-memories).
2. **Programmatic:** The Security Delete API finds memories older than a cutoff, prepares a sponsored transaction, and deletes them in batches after a dry run. See [Delete memories programmatically](/walrus-memory/guides/delete-memories-programmatically).

> **Warning**
>
> Both deletion paths are permanent. Start with a preview in the dashboard or a dry run in the API, review every blob ID, and only then delete.
## Rebuild the index

The search index lives in the relayer's database, not in your browser, so switching devices does not lose it. If recall does not return memories you know you stored, the relayer's index might lack rows for those blobs, for example after a database loss or reset, or when you point a fresh self-hosted relayer at your account. Walrus holds the permanent record, so restore rebuilds the index from it.

Restore rediscovers the blobs your account owns in a namespace and re-indexes any the relayer does not already have:

[Source: guides/manage-your-memory.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/guides/manage-your-memory.md)

```ts
const result = await memwal.restore("personal");
console.log(`restored=${result.restored} skipped=${result.skipped} total=${result.total}`);
```

Restore inspects your onchain blobs newest-first, up to `limit` (default 10), so `total` is the number of blobs it inspected in that call, not a full count of the namespace. Restore has no pagination cursor, so repeating a call at the same limit re-inspects the same newest blobs and returns nothing new. To rebuild a large namespace, rerun restore with a progressively higher `limit` until `restored` stops increasing. Restore is safe to run more than once, because it skips blobs the relayer already indexed. For the full restore flow, see [How Storage Works](/walrus-memory/fundamentals/architecture/how-storage-works).
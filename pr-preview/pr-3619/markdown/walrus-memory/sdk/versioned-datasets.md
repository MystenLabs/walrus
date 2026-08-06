> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

When an agent stores a dataset, a model artifact, or a snapshot of its own state, it usually needs more than the latest copy. It needs the history: which version it acted on, what changed between revisions, and proof that a past version has not been altered. On Walrus, that history comes for free from how storage works, with no version table to maintain.

## How content-addressing gives versioning for free

Every write to Walrus produces a **blob ID derived from the stored bytes**. Two properties follow directly:

- **Blobs are immutable.** You never overwrite a blob in place. Storing a changed version creates a new blob with a new ID, and the previous blob keeps its own ID and its own bytes. Nothing is lost on update, so the full version history is retained automatically.
- **The blob ID is a verifiable handle.** Because the ID is derived from the content, it pins down exactly those bytes. A blob ID that resolves is proof the bytes were not altered after the fact, which is what makes the lineage tamper-evident.

Put together, you get an append-only version history without building one: each revision is a separate, permanent, independently addressable object, and the chain of blob IDs is the lineage.

> **Warning**
>
> Memories are Seal-encrypted before they reach Walrus, so the blob ID addresses the **ciphertext**, not the plaintext. Treat every write as a new immutable version, and do not assume identical input produces an identical blob ID. Automatic deduplication of identical content is a separate capability that is not available today, so the content-addressing here gives you version history and lineage, not free dedup.
## Capturing a version handle

`rememberAndWait` returns the blob ID once the write is durable, and every recalled memory carries the blob ID it came from. Those are the two places you collect the permanent handle for a version.

[Source: sdk/versioned-datasets.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/sdk/versioned-datasets.md)

```ts
// On write: capture the blob ID as the version's permanent handle.
const v1 = await memwal.rememberAndWait(JSON.stringify(datasetV1), "dataset");
console.log(v1.blob_id); // immutable handle for version 1

// On recall: each hit reports which blob it came from.
const result = await memwal.recall({ query: "latest dataset snapshot", namespace: "dataset" });
for (const memory of result.results) {
  console.log(memory.blob_id, memory.text);
}
```

## Worked example: a versioned agent dataset

This agent revises a dataset over time and keeps an auditable lineage. Each revision is written as a new immutable blob, and the agent records a small lineage entry that links the new version to its parent. The dataset content lives in one namespace; the lineage records live in another, so the history is queryable on its own.

[Source: sdk/versioned-datasets.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/sdk/versioned-datasets.md)

```ts
import { MemWal } from "@mysten-incubation/memwal";

type LineageEntry = {
  version: number;
  blobId: string;
  parentBlobId: string | null;
  storedAt: string;
};

class VersionedDataset {
  private version = 0;
  private head: string | null = null;

  constructor(private memwal: MemWal) {}

  // Store a new revision and append a lineage record pointing at the parent.
  async commit(content: unknown): Promise<LineageEntry> {
    const stored = await this.memwal.rememberAndWait(JSON.stringify(content), "dataset");

    const entry: LineageEntry = {
      version: ++this.version,
      blobId: stored.blob_id,
      parentBlobId: this.head,
      storedAt: new Date().toISOString(),
    };

    // Persist the lineage entry as its own memory so the history is durable too.
    await this.memwal.rememberAndWait(JSON.stringify(entry), "dataset-lineage");
    this.head = stored.blob_id;
    return entry;
  }
}

// A configured client — see the SDK setup for how to supply credentials.
const memwal = MemWal.create({
  key: process.env.MEMWAL_KEY!,
  accountId: process.env.MEMWAL_ACCOUNT_ID!,
});

const dataset = new VersionedDataset(memwal);
const v1 = await dataset.commit({ rows: 100, schema: "v1" });
const v2 = await dataset.commit({ rows: 142, schema: "v1" });

// v2.parentBlobId === v1.blobId, so the chain is recorded, and both
// versions remain permanently addressable by their blob IDs.
```

Because each `blobId` permanently and verifiably addresses one exact revision, an agent (or an auditor reviewing it later) can point to the precise dataset a decision was made against, and confirm it has not changed since.

> **Note**
>
> Keep the lineage index wherever your agent already keeps durable state. Storing it as a memory, as shown here, keeps everything inside Walrus Memory. A database row or a Sui object works equally well, because the blob IDs are the durable handles either way.
## When this pattern fits

- **Dataset and artifact versioning:** retain every revision of a training set, prompt library, or config without a version-control system.
- **Provenance and audit:** prove which exact input an agent acted on at a point in time.
- **Reproducibility:** pin a workflow to a specific blob ID so a rerun reads the same bytes, not a mutated current copy.

## Next steps

- [How storage works](/walrus-memory/fundamentals/architecture/how-storage-works): where blob IDs come from in the write path
- [Walrus Memory client](/walrus-memory/sdk/usage/memwal): full signatures for `rememberAndWait` and `recall`
- [API Reference](/walrus-memory/sdk/api-reference): return types, including the blob ID on every result
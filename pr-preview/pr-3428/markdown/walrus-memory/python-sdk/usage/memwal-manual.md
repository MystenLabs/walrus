> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

> **Note**
>
> Unlike the TypeScript SDK there is **no separate `MemWalManual` class** in Python. The Python SDK is relayer-backed: the relayer always handles embedding, Seal encryption, and Walrus storage. The "manual" methods are lower-level entry points on the same `MemWal` / `MemWalSync` client for callers that already have a vector or a pre-uploaded blob.
Use these when you want to control indexing or do your own vector math. For the standard flow, prefer [`remember` / `recall`](/walrus-memory/python-sdk/usage/memwal).

## `embed`

Compute the embedding vector for text without storing anything.

```python
from memwal import MemWal

memwal = MemWal.create(key="...", account_id="0x...", env="prod")

vec = await memwal.embed("User prefers dark mode.")
print(len(vec.vector))
```

## `remember_manual`

Register a pre-uploaded Walrus blob with a pre-computed vector. The relayer stores the `{blob_id, vector, owner, namespace}` mapping; it does not upload for you here.

```python
from memwal import RememberManualOptions

result = await memwal.remember_manual(
    RememberManualOptions(
        blob_id="<walrus-blob-id>",
        vector=vec.vector,
        namespace="chatbot-prod",   # optional; falls back to client default
    )
)
print(result.id, result.blob_id, result.owner, result.namespace)
```

## `recall_manual`

Search with a pre-computed query vector. Returns `{blob_id, distance}` hits only, no decrypted text (you fetch/decrypt the blobs yourself).

```python
from memwal import RecallManualOptions

q = await memwal.embed("What do we know about this user?")
hits = await memwal.recall_manual(
    RecallManualOptions(vector=q.vector, limit=5, namespace="chatbot-prod")
)
for hit in hits.results:
    print(hit.blob_id, hit.distance)
```

## When to use which

| You have… | Use |
| --- | --- |
| Plain text, want it stored | `remember` / `remember_and_wait` |
| Plain text, want only the vector | `embed` |
| A vector + an already-uploaded blob | `remember_manual` |
| A query vector, want raw hits | `recall_manual` |
| Plain query text, want decrypted matches | `recall` |

All four manual entry points exist on both `MemWal` (async) and `MemWalSync` (sync) with identical signatures.
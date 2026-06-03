> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

The recommended default client. The relayer handles embeddings, Seal encryption, Walrus upload, and vector indexing. The SDK only signs requests and sends text.

## How it works

1. The SDK signs each request with your delegate key (Ed25519 through PyNaCl)
2. The relayer verifies delegate access
3. `remember` returns an accepted job while the relayer encrypts, uploads, and indexes in the background
4. `recall` searches by namespace and returns decrypted matches

```python
from memwal import MemWal, RecallParams

memwal = MemWal.create(
    key="<your-ed25519-private-key>",
    account_id="<your-memwal-account-id>",
    env="prod",            # or server_url="https://your-relayer-url.com"
    namespace="chatbot-prod",
)
```

`MemWalSync.create(...)` takes the same arguments for synchronous code.

## Core methods

```python
# Store a memory (async-accept; poll to completion)
done = await memwal.remember_and_wait("User prefers dark mode and works in Python.")
print(done.blob_id)

# Recall relevant memories
result = await memwal.recall(RecallParams(query="What do we know about this user?", limit=5))
for memory in result.results:
    print(memory.text, memory.distance)

# Extract and store facts from longer text
analyzed = await memwal.analyze(
    "I live in Hanoi, prefer dark mode, and usually work late at night."
)
print(analyzed.job_ids)

# Ask a question answered using your memories
answer = await memwal.ask("Where does this user live?")
print(answer.answer, answer.memories_used)

# Check relayer health (no auth)
await memwal.health()
```

Every memory method accepts an optional `namespace=` override that wins over the client default for that call.

## Restore

Rebuild missing indexed entries for one namespace from Walrus. Incremental and namespace-scoped, meant to repair PostgreSQL vector state from Walrus-backed memory.

```python
result = await memwal.restore("chatbot-prod", limit=10)
print(result.restored, result.skipped, result.total)
```

## Lower-level methods

Use these when you already have a vector or a pre-uploaded blob, see [Manual methods](/walrus-memory/python-sdk/usage/memwal-manual):

- `remember_manual(RememberManualOptions(blob_id=..., vector=..., namespace=...))`
- `recall_manual(RecallManualOptions(vector=..., limit=..., namespace=...))`
- `embed(text)`, embedding vector only, no storage
- `get_public_key_hex()`, the delegate public key

## Errors

| Exception | Raised when |
| --- | --- |
| `MemWalError` | Base class for all SDK errors (also raised on a failed `health()`) |
| `MemWalRememberJobNotFound` | A polled `job_id` is unknown or not owned by the caller |
| `MemWalRememberJobFailed` | An async remember job reached terminal `status=failed` |
| `MemWalRememberJobTimeout` | A polling loop exceeded its `timeout_ms` budget |

Transient statuses (connection drop, `429`, `5xx`) are retried inside the polling loops rather than surfaced.
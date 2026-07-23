> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

The Python SDK exposes one relayer-backed client in two forms, plus middleware:

| Entry point | Import | When to use |
| --- | --- | --- |
| `MemWal` | `from memwal import MemWal` | **Recommended default**, async-native; relayer handles embeddings, Seal, and storage |
| `MemWalSync` | `from memwal import MemWalSync` | Same API surface, synchronous, scripts, notebooks, non-async apps |
| `with_memwal_langchain` / `with_memwal_openai` | `from memwal import ...` | Automatic recall + save as middleware around an existing LLM client |

Detailed pages:

- [Walrus Memory](/walrus-memory/python-sdk/usage/memwal), the default async/sync client and its core methods
- [Manual methods](/walrus-memory/python-sdk/usage/memwal-manual), lower-level `remember_manual` / `recall_manual` / `embed`
- [with_memwal](/walrus-memory/python-sdk/usage/with-memwal), LangChain and OpenAI middleware

## Async vs sync

`MemWal` is async-native. Every API method is a coroutine:

[Source: python-sdk/usage.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/python-sdk/usage.md)

```python
from memwal import MemWal, RecallParams

memwal = MemWal.create(key="...", account_id="0x...", env="prod")
done = await memwal.remember_and_wait("User prefers dark mode.")
result = await memwal.recall(RecallParams(query="preferences"))
await memwal.close()
```

`MemWalSync` wraps it through `asyncio.run()`, identical methods, no `await`. It is notebook-safe (detects a running loop and offloads to a worker thread):

[Source: python-sdk/usage.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/python-sdk/usage.md)

```python
from memwal import MemWalSync, RecallParams

client = MemWalSync.create(key="...", account_id="0x...", env="prod")
client.remember_and_wait("User prefers dark mode.")
result = client.recall(RecallParams(query="preferences"))
client.close()
```

Both support context managers (`async with MemWal.create(...)` / `with MemWalSync.create(...)`) which close the underlying HTTP client on exit.

## Namespace rules

- Set a default namespace in `create(...)` when one app or agent uses one boundary
- Pass `namespace=` per call when one client needs multiple boundaries
- If omitted, namespace falls back to the client config, then to `"default"`

Good namespace examples: `todo`, `personal`, `password`, `project-x`. Avoid keeping everything in `"default"` after early testing.

## Async remember model

`remember` returns a `RememberAcceptedResult` (`job_id`, `status`) as soon as the relayer accepts the request, the Walrus upload and onchain commit happen in a background worker. Three ways to consume it:

[Source: python-sdk/usage.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/python-sdk/usage.md)

```python
# 1. Fire and forget the wait — poll later if you need the blob_id
accepted = await memwal.remember("Fact A")

# 2. Submit + poll to completion in one call
done = await memwal.remember_and_wait("Fact B")
print(done.blob_id)

# 3. Poll an earlier job yourself
done = await memwal.wait_for_remember_job(accepted.job_id)
```

Bulk (up to 20 items per call) follows the same pattern with `remember_bulk_async`, `wait_for_remember_jobs`, and `remember_bulk_and_wait`. Polling uses jittered exponential backoff (1.5× capped at 10s, ±25%) to stay relayer-friendly at scale.
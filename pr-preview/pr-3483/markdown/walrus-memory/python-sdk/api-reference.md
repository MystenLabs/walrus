> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

See also:

- [Configuration](/walrus-memory/reference/configuration)
- [Relayer API](/walrus-memory/relayer/api-reference)

## `MemWal.create(...)`

```python
MemWal.create(
    key: str,
    account_id: str,
    server_url: str = "http://localhost:8000",
    namespace: str = "default",
    env: str | None = None,
) -> MemWal
```

`MemWalSync.create(...)` has the same signature and returns a synchronous wrapper.

| Argument | Type | Required | Default | Notes |
| --- | --- | --- | --- | --- |
| `key` | `str` | Yes | | Ed25519 delegate private key in hex |
| `account_id` | `str` | Yes | | Walrus Memory account object ID on Sui |
| `server_url` | `str` | No | `http://localhost:8000` | Explicit relayer URL, wins over `env` |
| `namespace` | `str` | No | `"default"` | Default namespace for memory isolation |
| `env` | `str` | No | | Hosted preset: `staging` for testing or `prod` for production. Unknown → `ValueError` |

You might also build a `MemWalConfig` and call `MemWal(config)` directly; `env` resolution happens in `MemWalConfig.__post_init__`.

## `MemWal` methods

All methods are coroutines on `MemWal`; `MemWalSync` exposes the same names without `await`.

### `remember(text, namespace=None) -> RememberAcceptedResult`

Submit one memory. Returns after the relayer accepts a background job; embedding, Seal encryption, Walrus upload, and indexing continue asynchronously. `remember_async` is an alias.

```python
RememberAcceptedResult(job_id: str, status: str)
```

### `remember_and_wait(text, namespace=None, poll_interval_ms=1500, timeout_ms=60_000) -> RememberResult`

Submit one memory and poll until the job completes.

```python
RememberResult(id: str, blob_id: str, owner: str, namespace: str)
```

### `wait_for_remember_job(job_id, poll_interval_ms=1500, timeout_ms=60_000) -> RememberResult`

Poll a previously accepted job until `done` or `failed`. Raises `MemWalRememberJobNotFound` / `MemWalRememberJobFailed` / `MemWalRememberJobTimeout`.

### `remember_bulk_async(items) -> RememberBulkAcceptedResult`

Submit up to 20 `RememberBulkItem` in one request. `remember_bulk` is an alias.

```python
RememberBulkAcceptedResult(job_ids: list[str], total: int, status: str)
```

### `get_remember_bulk_status(job_ids) -> RememberBulkStatusResult`

One `RememberBulkStatusItem(job_id, status, blob_id?, error?)` per requested job.

### `wait_for_remember_jobs(job_ids, opts=None) -> RememberBulkResult`

Poll a batch until every job is terminal. `opts` is a `RememberBulkOptions(poll_interval_ms=1500, timeout_ms=120_000)`.

```python
RememberBulkResult(
    results: list[RememberBulkItemResult],  # preserves input order
    total: int, succeeded: int, failed: int, timed_out: int,
)
```

### `remember_bulk_and_wait(items, opts=None) -> RememberBulkResult`

`remember_bulk_async` + `wait_for_remember_jobs` in one call.

### `recall(RecallParams(...)) -> RecallResult`

Search memories matching a natural-language query, scoped to `owner + namespace`.
When `max_distance` is set, the client drops weak matches where `distance >= max_distance`.

Preferred form:

```python
from memwal import RecallParams

result = await memwal.recall(
    RecallParams(query="food allergies", limit=10, namespace="profile")
)
```

The legacy positional form `recall(query, limit=10, namespace=None, max_distance=None)` remains supported for backwards compatibility.

```python
RecallResult(
    results: list[RecallMemory],  # RecallMemory(blob_id, text, distance)
    total: int,
)
```

`distance` is cosine distance, lower is more similar.

### `analyze(text, namespace=None) -> AnalyzeResult`

Extract memorable facts through an LLM and enqueue one background remember job per fact.

```python
AnalyzeResult(
    facts: list[AnalyzedFact],  # AnalyzedFact(text, id, blob_id)
    fact_count: int,
    job_ids: list[str],
    status: str,
    owner: str,
)
```

`AnalyzeResult.total` is a backward-compat alias for `fact_count`. Use `analyze_and_wait(text, namespace=None, opts=None) -> AnalyzeWaitResult` to wait for every fact job to settle.

### `ask(question, limit=5, namespace=None) -> AskResult`

Answer a question using your memories as context.

```python
AskResult(
    answer: str,
    memories_used: int,
    memories: list[AskMemory],  # AskMemory(blob_id, text, distance)
)
```

### `restore(namespace, limit=10) -> RestoreResult`

Rebuild missing indexed entries for one namespace from Walrus. Incremental.

- `limit` defaults to `10` and caps the inspected blob set, newest-first
- `restored` counts blobs re-indexed in this call; `skipped` counts blobs already in the local index
- There is no pagination cursor; use a larger `limit` for larger one-shot restores

```python
RestoreResult(restored: int, skipped: int, total: int, namespace: str, owner: str)
```

### `health() -> HealthResult`

Check relayer health. No authentication. Raises `MemWalError` on non-200.

```python
HealthResult(
    status: str,
    version: str,
    relayer_version: str | None = None,
    api_version: str | None = None,
    min_supported_sdk: dict[str, str] | None = None,
)
```

### `compatibility() -> dict`

Fetch and validate the relayer compatibility contract from `/version`. Protected SDK calls run this check before signing the first request and raise `MemWalCompatibilityError` when the SDK/relayer pair is unsupported.

### `get_public_key_hex() -> str`

Hex-encoded public key for the current delegate key.

### `close()` / context managers

`await memwal.close()` closes the underlying `httpx` client. `MemWal` supports `async with`; `MemWalSync` supports `with`.

### Lower-level methods

| Method | Description |
| --- | --- |
| `remember_manual(RememberManualOptions)` | Register a pre-uploaded blob with a pre-computed vector → `RememberManualResult` |
| `recall_manual(RecallManualOptions)` | Search with a pre-computed query vector → `RecallManualResult` (blob_id + distance only) |
| `embed(text)` | Embedding vector for text, no storage → `EmbedResult(vector)` |

See [Manual methods](/walrus-memory/python-sdk/usage/memwal-manual).

## Middleware

```python
from memwal import with_memwal_langchain, with_memwal_openai
```

Both wrap an existing LLM client with automatic recall + save and accept: `key`, `account_id`, `server_url`, `namespace`, `env`, `max_memories` (5), `auto_save` (True), `min_relevance` (0.3), `debug` (False). The alias `withMemWal` maps to `with_memwal_langchain`. See [with_memwal](/walrus-memory/python-sdk/usage/with-memwal).

## Exceptions

```python
from memwal import (
    MemWalError,
    MemWalRememberJobNotFound,
    MemWalRememberJobFailed,
    MemWalRememberJobTimeout,
)
```

| Exception | `.status` | Meaning |
| --- | --- | --- |
| `MemWalError` | | Base class for all SDK errors |
| `MemWalRememberJobNotFound` | `404` | Job unknown or not owned by the caller |
| `MemWalRememberJobFailed` | `500` | Job reached terminal `failed` (`.error`) |
| `MemWalRememberJobTimeout` | `504` | Polling exceeded `.timeout_ms` |

## Utility functions

```python
from memwal import delegate_key_to_sui_address, delegate_key_to_public_key
```

| Function | Description |
| --- | --- |
| `delegate_key_to_sui_address(private_key_hex)` | Derive the `0x`-prefixed Sui address, `blake2b-256(0x00 ‖ pubkey)` |
| `delegate_key_to_public_key(private_key_hex)` | 32-byte Ed25519 public key bytes |

## Authentication

Every request is signed with Ed25519 (PyNaCl). Canonical message:

```
{timestamp}.{method}.{path_and_query}.{body_sha256}.{nonce}.{account_id}
```

Signed requests send `x-public-key`, `x-signature`, `x-timestamp`, `x-nonce` (UUID v4), and `x-account-id`. Relayer-mode requests also send `x-seal-session`; manual-mode requests omit decrypt credentials. SDKs that omit `x-nonce` are rejected by the server with `426 Upgrade Required`.
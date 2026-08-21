> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

The Rust relayer exposes these routes. The route table lives in `services/server/src/main.rs`.

See also:

- [Environment Variables](/walrus-memory/reference/environment-variables)
- [Configuration](/walrus-memory/reference/configuration)
- [Versioning and Compatibility](/walrus-memory/relayer/versioning-and-compatibility)

## Authentication

The routes below require signed headers, except the [MCP transports](#mcp-transports), which use bearer authentication. The SDK handles the signing automatically.

Not every `/api/*` route works this way. Account existence checks need no authentication, the security-delete families use their own challenge and bearer flows, and `/api/admin/*` requires an `ADMIN_API_KEY`. Those families stay out of scope here.

### Required headers

| **Header** | **Description** |
|--------|-------------|
| `x-public-key` | Hex-encoded Ed25519 public key (32 bytes) |
| `x-signature` | Hex-encoded Ed25519 signature (64 bytes) |
| `x-timestamp` | Unix timestamp in seconds (5-minute validity window) |
| `x-nonce` | UUID v4 nonce. The relayer records it in Redis for replay protection |

### Optional headers

| **Header** | **Description** |
|--------|-------------|
| `x-account-id` | MemWalAccount object ID hint. Official SDKs always send it and include it in the canonical signature |
| `x-seal-session` | Base64-encoded Seal SessionKey for relayer-managed decrypt flows. The TypeScript and Python SDKs use it |
| `x-delegate-key` | Legacy delegate private key credential for relayer-managed decrypt flows. Deprecated; use `x-seal-session` where supported |

### Signature format

The signed message is:

[Source: relayer/api-reference.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/relayer/api-reference.md)

```text
{timestamp}.{method}.{path_and_query}.{body_sha256}.{nonce}.{account_id}
```

For `GET` requests, `body_sha256` is the SHA-256 of an empty byte string. If a raw client omits `x-account-id`, it must sign the empty string in the final `account_id` position. Official SDKs send `x-account-id`.

The relayer verifies the Ed25519 signature, then resolves the owner by looking up the public key in onchain `MemWalAccount.delegate_keys`.

## Public routes

These routes require no authentication.

### `GET /health`

Service health check.

**Response:**

[Source: relayer/api-reference.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/relayer/api-reference.md)

```json
{
  "status": "ok",
  "version": "0.1.0",
  "relayerVersion": "0.1.0",
  "apiVersion": "1.0.0",
  "minSupportedSdk": {
    "typescript": "0.0.4",
    "python": "0.1.0",
    "mcp": "0.0.1"
  },
  "featureFlags": {
    "auth.accountBoundNonce": true,
    "auth.sealSessionHeader": true,
    "runtime.versionEndpoint": true
  },
  "deprecations": [],
  "build": {},
  "mode": "production",
  "prompt_versions": {
    "extract": "extract.v1",
    "ask": "ask.v1"
  }
}
```

### `GET /version`

Stable relayer/API compatibility metadata.

**Response:** the compatibility object documented in [Versioning and Compatibility](/walrus-memory/relayer/versioning-and-compatibility#runtime-metadata).

### `GET /config`

Public deployment parameters that the SDK reads to build a Seal SessionKey client-side. The endpoint returns no secrets.

**Response:**

[Source: relayer/api-reference.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/relayer/api-reference.md)

```json
{
  "packageId": "0x...",
  "network": "testnet",
  "suiRpcUrl": "https://fullnode.testnet.sui.io",
  "suiGrpcUrl": "https://fullnode.testnet.sui.io",
  "suiTransport": "grpc",
  "rateLimitDisabled": false
}
```

This example shows the fields a client needs to select a transport and build a session key. A deployment can return more, so read fields by name rather than assuming the shape is exhaustive.

`suiGrpcUrl` is the preferred gRPC endpoint, and `suiTransport` names the transport the relayer prefers for Sui reads. Newer SDKs read both; a client might fall back to `suiRpcUrl` while a deployment migrates. `rateLimitDisabled` mirrors the server's benchmark-bypass setting so benchmark scripts can pre-flight the configuration.

### `GET /metrics`

Prometheus metrics for scraping. See [Observability](/walrus-memory/relayer/observability) for the exported series and how to wire a scraper.

### `POST /sponsor`

Proxy to the Seal/Walrus sidecar's `/sponsor` endpoint for sponsored transactions. The request must include `authTimestamp`, a UUID-v4 `authNonce`, and `authSignature`: a Sui personal-message signature over the sender, transaction-kind hash, timestamp, and nonce. Only one allowlisted Walrus Memory `account` call qualifies for sponsorship.

### `POST /sponsor/execute`

Proxy to the sidecar's `/sponsor/execute` endpoint. `sender` must match the short-lived, one-time Redis binding that the authenticated `/sponsor` call creates.

## Protected routes

Every route below requires the signed headers described in [Authentication](#authentication).

### `POST /api/remember`

Submit text as an encrypted memory job. The relayer returns after creating a background job; embedding, Seal encryption, Walrus upload, and vector indexing continue asynchronously.

**Request:**

[Source: relayer/api-reference.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/relayer/api-reference.md)

```json
{
  "text": "User prefers dark mode",
  "namespace": "demo"
}
```

`namespace` defaults to `"default"` if omitted and is limited to 255 UTF-8 bytes.

**Response:** `202 Accepted`

[Source: relayer/api-reference.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/relayer/api-reference.md)

```json
{
  "job_id": "uuid",
  "status": "running"
}
```

### `GET /api/remember/:job_id`

Poll a remember job. `status` is one of `pending`, `running`, `uploaded`, `done`, or `failed`. Failed jobs include an `error` message. Unknown job IDs and jobs that belong to another owner return `404`, so callers cannot enumerate job IDs.

**Response:**

[Source: relayer/api-reference.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/relayer/api-reference.md)

```json
{
  "job_id": "uuid",
  "status": "done",
  "owner": "0x...",
  "namespace": "demo",
  "blob_id": "walrus-blob-id"
}
```

### `POST /api/remember/bulk`

Submit up to 20 memories in one request. `job_ids[i]` corresponds to `items[i]`. This route accepts request bodies up to 2 MB.

**Request:**

[Source: relayer/api-reference.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/relayer/api-reference.md)

```json
{
  "items": [
    { "text": "User prefers dark mode", "namespace": "demo" },
    { "text": "User works in TypeScript", "namespace": "demo" }
  ]
}
```

**Response:** `202 Accepted`

[Source: relayer/api-reference.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/relayer/api-reference.md)

```json
{
  "job_ids": ["uuid-1", "uuid-2"],
  "total": 2,
  "status": "running"
}
```

### `POST /api/remember/bulk/status`

Poll a batch of remember jobs. Unknown job IDs come back with status `not_found`, and failed items include an `error` message.

**Request:**

[Source: relayer/api-reference.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/relayer/api-reference.md)

```json
{
  "job_ids": ["uuid-1", "uuid-2"]
}
```

**Response:**

[Source: relayer/api-reference.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/relayer/api-reference.md)

```json
{
  "results": [
    { "job_id": "uuid-1", "status": "done", "blob_id": "walrus-blob-id" },
    { "job_id": "uuid-2", "status": "running" }
  ]
}
```

### `POST /api/recall`

Search for memories matching a natural language query. Returns decrypted plaintext results.

**Request:**

[Source: relayer/api-reference.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/relayer/api-reference.md)

```json
{
  "query": "What do we know about this user?",
  "limit": 10,
  "namespace": "demo",
  "scoring_weights": {
    "semantic": 1.0,
    "recency": 0.3,
    "recency_half_life_days": 30,
    "importance": 0.2
  }
}
```

`limit` defaults to `10`; the server caps it at `100`. `namespace` defaults to `"default"`. `scoring_weights` takes an optional object; omit it to keep the plain cosine-distance order.

#### Scoring weights

The optional `scoring_weights` object turns on composite ranking. The same object works on `/api/recall`, `/api/recall/manual`, and `/api/ask`.

| **Field** | **Default** | **Description** |
|--------|-------------|-------------|
| `semantic` | `1.0` | Weight for cosine similarity between the query and each memory |
| `recency` | `0` | Weight for how recently the relayer indexed each memory |
| `recency_half_life_days` | `30` | Half-life in days for the recency decay |
| `importance` | `0` | Weight for the per-fact importance score that the extractor assigns at save time |

**Response:**

[Source: relayer/api-reference.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/relayer/api-reference.md)

```json
{
  "results": [
    {
      "blob_id": "walrus-blob-id",
      "text": "User prefers dark mode",
      "distance": 0.15,
      "score": 0.91
    }
  ],
  "total": 1,
  "dropped_count": 0
}
```

`score` only appears when `scoring_weights` sets a nonzero `recency` or `importance` weight. A request that sets only the `semantic` weight keeps the plain cosine order, and the relayer omits `score`. `dropped_count` only appears when at least one match dropped out because its blob download or decryption failed; the relayer omits those matches from `results`.

### `POST /api/remember/manual`

Register a client-encrypted payload. The client sends Seal-encrypted data (base64) and a precomputed embedding vector. The relayer uploads the encrypted bytes to Walrus and stores the vector mapping.

**Request:**

[Source: relayer/api-reference.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/relayer/api-reference.md)

```json
{
  "encrypted_data": "base64-encoded-seal-encrypted-bytes",
  "vector": [0.01, -0.02, ...],
  "namespace": "demo"
}
```

**Response:**

[Source: relayer/api-reference.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/relayer/api-reference.md)

```json
{
  "id": "uuid",
  "blob_id": "walrus-blob-id",
  "owner": "0x...",
  "namespace": "demo"
}
```

### `POST /api/recall/manual`

Search with a precomputed query vector. Returns index hits only; the client handles downloading and decrypting. The request accepts the same optional `scoring_weights` object as [`POST /api/recall`](#scoring-weights), and the server applies the same `limit` cap of `100`.

**Request:**

[Source: relayer/api-reference.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/relayer/api-reference.md)

```json
{
  "vector": [0.01, -0.02, ...],
  "limit": 10,
  "namespace": "demo"
}
```

**Response:**

[Source: relayer/api-reference.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/relayer/api-reference.md)

```json
{
  "results": [
    {
      "blob_id": "walrus-blob-id",
      "distance": 0.15,
      "created_at": "2026-07-23T12:00:00Z",
      "importance": 0.5
    }
  ],
  "total": 1
}
```

`created_at` is the time the relayer indexed the entry. `importance` carries the per-fact importance score that the extractor assigns at save time.

### `POST /api/analyze`

Extract facts from text using an LLM, then enqueue each fact as a separate memory job.

**Request:**

[Source: relayer/api-reference.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/relayer/api-reference.md)

```json
{
  "text": "I live in Hanoi and prefer dark mode.",
  "namespace": "demo",
  "occurred_at": "2026-07-01T00:00:00Z"
}
```

`occurred_at` is an optional RFC 3339 timestamp that anchors the extracted facts in time, for example when you import older conversations. The extractor writes the date into the fact text itself; the relayer stores no separate event-time metadata and cannot filter or rank by event time.

**Response:** `202 Accepted`

[Source: relayer/api-reference.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/relayer/api-reference.md)

```json
{
  "job_ids": ["uuid-1", "uuid-2"],
  "facts": [
    { "text": "User lives in Hanoi", "id": "uuid-1", "job_id": "uuid-1" },
    { "text": "User prefers dark mode", "id": "uuid-2", "job_id": "uuid-2" }
  ],
  "fact_count": 2,
  "status": "pending",
  "owner": "0x..."
}
```

### `POST /api/ask`

Recall memories, inject them into an LLM prompt, and return an AI-generated answer with the context used.

**Request:**

[Source: relayer/api-reference.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/relayer/api-reference.md)

```json
{
  "question": "What do you know about my preferences?",
  "limit": 5,
  "namespace": "demo"
}
```

`limit` defaults to `5` and caps at `100`. `namespace` defaults to `"default"`. The request accepts the same optional `scoring_weights` object as [`POST /api/recall`](#scoring-weights).

**Response:**

[Source: relayer/api-reference.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/relayer/api-reference.md)

```json
{
  "answer": "Based on your memories, you prefer dark mode and live in Hanoi.",
  "memories_used": 2,
  "memories": [
    {
      "blob_id": "walrus-blob-id",
      "text": "User prefers dark mode",
      "distance": 0.12
    }
  ]
}
```

### `POST /api/restore`

Rebuild missing vector entries for one namespace. Queries onchain blobs by owner and namespace, downloads from Walrus, decrypts, re-embeds, and re-indexes only the entries missing from the index.

**Request:**

[Source: relayer/api-reference.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/relayer/api-reference.md)

```json
{
  "namespace": "demo",
  "limit": 10
}
```

`limit` defaults to `10` and caps the onchain query itself, newest blobs first. Raise it to cover a larger namespace.

**Response:**

[Source: relayer/api-reference.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/relayer/api-reference.md)

```json
{
  "restored": 3,
  "skipped": 7,
  "total": 10,
  "namespace": "demo",
  "owner": "0x...",
  "truncated": false
}
```

`truncated` is `true` when this restore cannot complete: either more onchain blobs were missing locally than `limit` allowed this call to restore, or the sidecar's raw onchain candidate fetch (bounded per owner, shared across all of the owner's namespaces, hard-capped independent of `limit`) hit its own cap before this namespace's blobs were even filtered out of that set. The second case can produce `truncated: true` even when `total` is `0` for this namespace, because a cap hit elsewhere can starve this namespace's fetch entirely. Raising `limit` only helps with the first case. Past the sidecar's cap, only a cursor or pagination-based restore would help.

### `POST /api/forget`

Delete every vector index row for one namespace. The Walrus blobs persist, so a later `POST /api/restore` call can re-index them. The relayer resolves the owner from the signed headers and only deletes that owner's rows.

**Request:**

[Source: relayer/api-reference.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/relayer/api-reference.md)

```json
{
  "namespace": "demo"
}
```

`namespace` defaults to `"default"`.

**Response:**

[Source: relayer/api-reference.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/relayer/api-reference.md)

```json
{
  "deleted": 12,
  "namespace": "demo",
  "owner": "0x..."
}
```

`deleted` is the number of index rows the relayer removed.

### `POST /api/stats`

Return the memory count and stored byte total for one namespace, scoped to the authenticated owner.

**Request:**

[Source: relayer/api-reference.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/relayer/api-reference.md)

```json
{
  "namespace": "demo"
}
```

`namespace` defaults to `"default"`.

**Response:**

[Source: relayer/api-reference.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/relayer/api-reference.md)

```json
{
  "memory_count": 42,
  "storage_bytes": 1048576,
  "namespace": "demo",
  "owner": "0x..."
}
```

## MCP Transports

The relayer also proxies Model Context Protocol traffic to its Node sidecar. `GET`, `POST`, `DELETE`, and `OPTIONS` on `/api/mcp` serve the Streamable HTTP transport, and `GET /api/mcp/sse` plus `POST /api/mcp/messages` serve the legacy SSE transport. These routes use bearer authentication instead of signed headers. See [Reference](/walrus-memory/mcp/reference) for transport details and client configuration.
> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

## Overview

Registered routes (colon form used by the docs freshness check):

- `GET /v1/owners/:owner/namespaces`
- `GET /v1/owners/:owner/memories`
- `GET /v1/owners/:owner/agents`
- `GET /v1/owners/:owner/_token_probe`

Owner-scoped, cursor-based, read-only. All three endpoints accept either of
two auth mechanisms (see Authentication below); either way, the `{owner}`
path segment must equal the authenticated identity or the request is
rejected with 403.

> This document covers response shapes, auth mechanics, and error/rate-limit
> behavior for the three endpoints below. A formal OpenAPI/JSON-Schema spec
> is a follow-up, not included in this pass.

## Authentication

These three routes accept **either** of two independent, parallel auth
mechanisms, dispatched by `auth::verify_read_api_auth`
(`services/server/src/auth.rs`) based on whether the request carries an
`Authorization` header:

1. **Ed25519 signed headers**, the same `verify_signature` middleware
   `/api/restore` and every other protected route use. This is the
   mechanism direct SDK/dashboard delegate-key callers use; see "Required
   headers" and "Canonical signing string" below.
2. **Owner-scoped bearer token** (`Authorization: Bearer <token>`), the
   mechanism for Console, which structurally never holds a delegate key and
   so can never produce an Ed25519 signature. See `docs/api/owner-token-auth.md`
   for how Console obtains a token; once obtained, a request here is just
   `Authorization: Bearer <token>` with no other auth headers. The token's
   `permissions` must include `memories.read` (the only scope currently
   minted) or the request is rejected `403`.

Presence of an `Authorization` header selects the bearer-token path; its
absence selects the Ed25519 path. A request cannot use both.

### Required headers

| Header | Description |
|---|---|
| `x-public-key` | Hex-encoded Ed25519 public key (32 bytes) |
| `x-signature` | Hex-encoded Ed25519 signature (64 bytes) over the canonical message below |
| `x-timestamp` | Unix timestamp in seconds. Must be within ±300s (5 minutes) of server time |
| `x-nonce` | UUID v4, used once for replay protection (tracked in Redis for 10 minutes) |
| `x-account-id` | Walrus Memory account object ID. Effectively required: it is signed into the canonical message, so omitting it signs an empty string and does not match a real account on Testnet |

### Canonical signing string

The client signs (Ed25519) the following pipe-free, dot-joined string and
sends the signature in `x-signature`:

[Source: api/memory-read-api.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/api/memory-read-api.md)

```
{timestamp}.{method}.{path_and_query}.{body_sha256}.{nonce}.{account_id}
```

- `timestamp`: the exact value sent in `x-timestamp`.
- `method`: HTTP method, for example `GET`.
- `path_and_query`: the request's path plus query string exactly as sent,
  for example `/v1/owners/0xabc.../memories?limit=50`. Mismatched query params
  (including a tampered `updated_after` cursor) invalidate the signature.
- `body_sha256`: hex-encoded SHA-256 of the request body. For these
  GET-only, bodyless endpoints this is the hash of an empty byte string.
- `nonce`: the `x-nonce` value.
- `account_id`: the `x-account-id` value (empty string if omitted, which
  does not match a real signed request).

Server-side verification flow (`auth.rs::verify_signature`): validate the
Ed25519 signature against the canonical string → check the nonce hasn't
been seen before (Redis, fail-closed on Redis outage) → resolve the account
(cache, then the `x-account-id` hint, then a bounded onchain registry scan
as a last resort) → verify the public key is a registered delegate key on
that account's onchain `MemWalAccount.delegate_keys` (cached in
`delegate_key_cache`, revoked entries evicted).

### 401 responses

**Ed25519 path** (no `Authorization` header on the request), any of the
following returns a bare `401 Unauthorized` (no JSON body, constant ~100ms
delay on signature/timestamp/nonce failures to prevent timing side-channels
distinguishing failure reasons):

- Missing/malformed `x-public-key`, `x-signature`, or `x-timestamp`.
- `x-timestamp` outside the ±300s window.
- Ed25519 signature verification failure (including a tampered path, query
  string, or body).
- Nonce already seen (replay): or the Redis nonce check itself failing
  (fail-closed).
- Public key not found among the resolved account's onchain delegate keys,
  or the account is deactivated.

A request missing `x-nonce` entirely gets `426 Upgrade Required` instead of
`401`, signaling an unsupported legacy SDK version rather than an auth
failure.

**Bearer-token path** (`Authorization: Bearer <token>` present), also a
bare `401`, with no distinction in the response between causes, for:

- An expired, tampered, wrongly-signed, or wrong-audience token.
- `OWNER_TOKEN_SECRET` unconfigured on this deployment.
- The token's `owner_address` no longer resolves to a `MemWalAccount`
  (for example, deleted between mint and use).

Once authenticated (either path), the three endpoints below additionally
return `403` if the `{owner}` path segment does not equal the resolved
identity, or, on the bearer-token path, if the token's `permissions` don't
include `memories.read`.

## Rate limiting

These three endpoints run on their own router (`read_api_routes` in
`main.rs`), separate from the write path's `protected_routes`, behind
`read_api_rate_limit_middleware` (`services/server/src/rate_limit.rs`)
instead of the write path's `rate_limit_middleware`. They do **not** share
the write path's budget, a routine pagination loop over this API can no
longer trip, or contend with, the 30/min per-delegate-key budget that
exists to bound the write path's spend-risk (Walrus upload, LLM calls,
gas).

There is a single sliding-window layer, keyed by delegate key under its
own Redis prefix (`rate:read:dk:{public_key}`, distinct from the write
path's `rate:dk:{public_key}`), no separate per-account burst/sustained
tiers on top. Default limit is **200 weighted-requests/min per delegate
key** (`ReadApiRateLimitConfig::per_delegate_key_per_minute`), overridable
through the `READ_API_RATE_LIMIT_PER_MINUTE` env var. Weights for this API:

| Endpoint | Weight | Why |
|---|---|---|
| `GET /v1/owners/{owner}/namespaces` | 1 | DB read only |
| `GET /v1/owners/{owner}/memories` | 1 | DB read only |
| `GET /v1/owners/{owner}/agents` | 2 | Makes a live onchain `sui_getObject` RPC call (short-TTL cached, but uncached on a cold/expired cache entry) |

Exceeding the limit returns `429 Too Many Requests`:

[Source: api/memory-read-api.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/api/memory-read-api.md)

```json
{
  "error": "Rate limit exceeded",
  "layer": "read_delegate_key",
  "limit": "200 weighted-requests/min",
  "retry_after_seconds": 60
}
```

with a `Retry-After: 60` header. If the rate limiter itself is
unavailable (Redis unreachable), requests fail closed with `503 Service
Unavailable` and a `Retry-After: 30` header rather than being allowed
through unmetered, there is no in-memory fallback for this middleware,
unlike the write path's deliberately-fallback-enabled limiter.

## `GET /v1/owners/:owner/namespaces`

`updated_after`, like `memories`' below, must be the opaque `next_cursor`
value returned by a previous call, not a raw timestamp or namespace name;
omit it for the first page. It base64 (URL-safe, unpadded) encodes the
JSON watermark `{"updated_at": ..., "namespace": ...}`: rows are ordered
and filtered by the rollup's `(MAX(updated_at), namespace)`, mirroring
`memories`' `(updated_at, id)` keyset. `limit` defaults to 100, max 500,
`400` for non-positive/non-integer values, same convention as `memories`.

**Breaking change from an earlier version of this endpoint:** namespaces
are now returned ordered by recency (`(MAX(updated_at), namespace)`), not
alphabetically by name. A client that wants an alphabetical list needs to
buffer and sort every page itself rather than relying on response order.

Because the cursor is a recency watermark rather than a name, it is a real
incremental-sync token: a namespace you already synced comes back on the
next poll if any of its memories were **created or updated** after that
watermark, however early its name sorts. Namespaces untouched since the
watermark are not re-sent.

Hard deletes write a row to `memory_tombstones` and remove the live
`vector_entries` row in one statement. Incremental `/namespaces` computes the
watermark as GREATEST(live MAX(updated_at), tombstone MAX(deleted_at)), so a
namespace whose last memory was deleted still resurfaces with a smaller
`memory_count`. Incremental `/memories` never puts tombstones in `memories[]`.
They arrive in the additive `deleted` array (`memory_id`, `namespace_id`,
`deleted_at`). Old Console clients ignore that key. Tombstones are kept for
30 days; a cursor older than that returns `must_resync: true`.
`snapshot_version` stays 2.

Response:
[Source: api/memory-read-api.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/api/memory-read-api.md)

```json
{
  "namespaces": [
    {
      "id": "work",
      "name": "work",
      "memory_count": 12,
      "storage_used": 48213,
      "updated_at": "2026-08-04T10:00:00Z"
    }
  ],
  "next_cursor": "eyJ1cGRhdGVkX2F0IjouLi4sIm5hbWVzcGFjZSI6IndvcmsifQ",
  "has_more": false,
  "snapshot_version": 2
}
```

`updated_at` is `MAX(updated_at)` across the namespace's memories, the same
value the cursor is built from.

## `GET /v1/owners/:owner/memories`

`updated_after` must be the opaque `next_cursor` value returned by a
previous call, not a raw timestamp. `limit` defaults to 100, max 500.

Response:
[Source: api/memory-read-api.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/api/memory-read-api.md)

```json
{
  "memories": [
    {
      "memory_id": "abc123",
      "namespace_id": "work",
      "blob_id": "blob-xyz",
      "created_at": "2026-08-04T10:00:00Z",
      "updated_at": "2026-08-04T11:30:00Z",
      "size": 2048,
      "agent_id": "agent-abc",
      "package_id": "0xpkg",
      "status": "active",
      "end_epoch": 900,
      "expires_at": "2026-09-15T00:00:00Z",
      "importance": 0.5
    }
  ],
  "deleted": [],
  "must_resync": false,
  "next_cursor": "eyJ1cGRhdGVkX2F0IjouLi59",
  "has_more": false,
  "snapshot_version": 2
}
```

`updated_at` is the row's own last-modified time, the same value this
page's cursor is built from.

`status` is `"expired"` if `expires_at` is in the past and `"active"`
otherwise (including when `end_epoch` /
`expires_at` are still null because the expiry sweep has not run yet).
There is no `"deleted"` status: a deleted memory leaves `memories[]`
entirely and appears once in `deleted[]`. That array is the only deletion
signal on this endpoint.

The first time the sweep resolves a row's `end_epoch`/`expires_at` from
`null` to a real value, that row's `updated_at` advances too, so a client
that already synced the row while it was still unsynced sees the
populated values on its next incremental poll, rather than being stuck
with `null` forever. A later routine re-verification that reconfirms an
unchanged value does *not* advance `updated_at` (this would otherwise make
every synced memory reappear roughly once a day regardless of whether
anything changed); only a genuine change does.

### Cursor semantics (both paginated endpoints)

`next_cursor` is **always** returned for a non-empty page, including the
final page of a traversal and a result that fits entirely in one page. It
is the watermark of the last row in that page, and it is what you pass as
`updated_after` on your next poll.

An empty page returns `next_cursor: null` in exactly one case: the very
first page of a fresh walk (no `updated_after` on the request) that
matches nothing at all, there is no prior cursor and no row to build a
watermark from. Every other empty page, a continuation page reached with
an incoming cursor, which can happen when every remaining row raced past
the walk's snapshot boundary between pages, still returns a non-null
`next_cursor`: the same position as the incoming cursor, but with a fresh
snapshot boundary. Use that new cursor rather than the one you already
held; the one you held is now stale, it's exactly what the reset exists
to replace.

So `next_cursor: null` does **not** mean "end of data", use the separate
`has_more` boolean for that instead. Keep paginating (pass back the latest
`next_cursor`) while `has_more` is `true`; stop once you see `has_more:
false`.

**Do not infer end-of-data from page length.** `limit` is silently clamped
to each endpoint's max (500), a request for `limit=1000` that happens to
match exactly 500 real rows returns a page exactly as long as the (clamped)
`limit`, and a request for more than the actual remaining data returns a
page shorter than `limit` while more data still exists elsewhere for that
owner. `has_more` is correct in both cases; page-length heuristics are not.

## `GET /v1/owners/:owner/agents`

Live onchain read of `MemWalAccount.delegate_keys`, short-TTL cached
per-account (same 30s window `sui/client.rs::walrus_epoch()` uses) so
repeated calls within the TTL window don't re-hit the chain.

Response:
[Source: api/memory-read-api.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/api/memory-read-api.md)

```json
{
  "agents": [
    { "label": "cli", "sui_address": "0xdelegate1" }
  ],
  "snapshot_version": 2
}
```

## Errors

All errors from these three endpoints (except the shared auth middleware's
bare `401`/`426`, and the shared rate limiter's `429`/`503`, see
Authentication and Rate limiting above) use the envelope
`{ "error": "<message>" }`:

| Status | Cause |
|---|---|
| `401` | Auth failed, either an invalid/expired Ed25519 signature/nonce/timestamp, or (bearer-token path) an invalid/expired/unresolvable owner token (see Authentication) |
| `403` | `{owner}` path segment does not match the authenticated identity, or (bearer-token path) the token's `permissions` lack `memories.read` |
| `400` | Invalid/malformed cursor, or non-positive/non-integer `limit` |
| `429` | Rate limit exceeded (see Rate limiting) |
| `500` | Internal error, including an onchain RPC failure on `/agents` |

An empty list (owner with no memories/namespaces, or no delegate keys) is
a valid `200`, not a `404`.
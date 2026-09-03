# Admin API

The Admin API lets app operators manage accounts, API keys, and S3 access
keys. All admin endpoints require **admin-key** authentication (long-lived
per-app Bearer tokens issued through `oysterd app issue-admin-key`;
see [Authentication](authentication.md)).

An app can only manage accounts it created. Attempting to access another
app's accounts returns **403 Forbidden**.

## Accounts

### Create Account

```
POST /api/v1/accounts
```

Creates a new account owned by the authenticated app. An initial API key
is generated automatically.

**Request body** (optional):

```json
{
  "name": "my-app-user",
  "max_unencoded_bytes": 5000000000,
  "avg_blob_size": 10000000
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | no | Human-readable account name; defaults to the account ID if omitted |
| `max_unencoded_bytes` | integer | no | Per-account storage cap, in *unencoded* bytes. Defaults to `5_000_000_000` (5 × 10⁹) when omitted. Must be strictly positive; `0` and negative values are rejected with `400` |
| `avg_blob_size` | integer | no | Assumed average blob size, in *unencoded* bytes. Turns `max_unencoded_bytes` into a **lower** bound on storable capacity for blobs of this size (see [Lower-bound semantics](#lower-bound-semantics-avg_blob_size) below). Defaults to the server's `OYSTER_DEFAULT_AVG_BLOB_SIZE` (**10 MB**) when omitted. `0` disables inflation (the historical upper-bound behavior); negative values are rejected with `400`; an oversized value is accepted as a silent no-op |

**Example:**

```bash
curl -s -X POST \
  -H "Authorization: Bearer $ADMIN_KEY" \
  -H "Content-Type: application/json" \
  -d '{"name": "my-app-user"}' \
  "$OYSTER_URL/api/v1/accounts" | jq
```

**Response** (`201 Created`):

```json
{
  "account_id": "550e8400-e29b-41d4-a716-446655440000",
  "api_key": {
    "id": "b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e",
    "prefix": "a1b2c3d4",
    "bearer_token": "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2",
    "created_at": "2025-01-15T10:30:00Z"
  }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `account_id` | string | UUID of the new account |
| `api_key.id` | string | Unique key identifier |
| `api_key.prefix` | string | First 8 characters of the raw key (for identification) |
| `api_key.bearer_token` | string | The full API key, **shown only once** |
| `api_key.created_at` | string | ISO 8601 timestamp |

> The `bearer_token` is returned only at creation time. A lost
> key cannot be recovered; create a new one instead.

**Errors:**

| Status | Condition |
|--------|-----------|
| `400` | `max_unencoded_bytes` must be a positive integer, or `avg_blob_size` is negative |
| `401` | Missing or invalid admin key |

### Update Storage Cap

```
PUT /api/v1/accounts/{account_id}/max-storage
```

Raises or lowers the per-account `max_unencoded_bytes` cap. Lowering
the cap below the account's current onchain encoded usage is
rejected; lowering between current usage and current pool capacity
submits an onchain shrink transaction to release the freed reserve
back to the Pearl-derived wallet.

**Path parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `account_id` | string | UUID of the account |

**Request body:**

```json
{ "max_unencoded_bytes": 10000000000, "avg_blob_size": 10000000 }
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `max_unencoded_bytes` | integer | yes | New per-account cap, in *unencoded* bytes. Must be strictly positive |
| `avg_blob_size` | integer | no | New assumed average blob size, in *unencoded* bytes (see [Lower-bound semantics](#lower-bound-semantics-avg_blob_size)). When **omitted, the account's existing `avg_blob_size` is retained** and the orphan/shrink threshold is recomputed against it. `0` disables inflation; negative values are rejected with `400`; an oversized value is a silent no-op |

**Example:**

```bash
curl -s -X PUT \
  -H "Authorization: Bearer $ADMIN_KEY" \
  -H "Content-Type: application/json" \
  -d '{"max_unencoded_bytes": 10000000000}' \
  "$OYSTER_URL/api/v1/accounts/550e8400-e29b-41d4-a716-446655440000/max-storage" | jq
```

**Response** (`200 OK`):

```json
{
  "account_id": "550e8400-e29b-41d4-a716-446655440000",
  "max_unencoded_bytes": 10000000000,
  "avg_blob_size": 10000000,
  "pool": {
    "reserved_encoded_bytes": 8000000000,
    "used_encoded_bytes": 4123456789
  },
  "shrink_tx_digest": "5jK...digest"
}
```

| Field | Type | Description |
|-------|------|-------------|
| `account_id` | string | The account whose cap was updated |
| `max_unencoded_bytes` | integer | The new cap, in *unencoded* bytes |
| `avg_blob_size` | integer | The effective assumed average blob size after the update. Echoes the request value when supplied, otherwise the account's retained value |
| `pool` | object or null | Onchain `StoragePool` snapshot after the (optional) shrink. `null` when the account has never lazy-created a pool (DB-only fast path; no onchain read was performed) |
| `pool.reserved_encoded_bytes` | integer | `storage.storage_size`: encoded bytes reserved by the pool |
| `pool.used_encoded_bytes` | integer | Encoded bytes currently consumed by registered blobs |
| `shrink_tx_digest` | string or null | Digest of the submitted `decrease_storage_pool_capacity_by_size` PTB, or `null` when no shrink was needed |

When the account has no pool yet (no upload has lazy-created one),
the cap is updated in the DB only (`pool` and `shrink_tx_digest`
are both `null`). When the new cap covers the existing pool's reserved
bytes, the same DB-only path runs (`shrink_tx_digest` is `null`,
`pool` is populated from the onchain read).

### Lower-bound semantics (`avg_blob_size`)

`max_unencoded_bytes` is stated in *unencoded* bytes, but Walrus tracks
usage in *encoded* bytes, and the encoding `f` carries a large fixed
per-blob metadata overhead. By default (`avg_blob_size = 0`) the cap
therefore acts as an **upper** bound: an account storing many small
blobs pays that overhead per blob and hits the cap well below its
stated unencoded budget.

Setting `avg_blob_size = s` flips this into a **lower** bound *for blobs
averaging `s`*: Oyster inflates the encoded admission ceiling by the
per-blob expansion factor `f(s)/s`, guaranteeing that at least
`max_unencoded_bytes` unencoded bytes are storable when the account's
blobs average ≥ `s`. The expansion factor shrinks as blobs grow:
roughly `66034×` at 1 KB, `70×` at 1 MB, `11×` at 10 MB, `5.1×` at
100 MB, asymptoting to ~`4.5×`. A 10 MB `avg_blob_size` (the
default) sets the ceiling at about `11 ×` the cap's bare encoded value.
Blobs smaller than `s` carry more overhead and so reach the ceiling
before the unencoded total reaches `max_unencoded_bytes`.

This only raises the *admission ceiling*; it does **not** pre-reserve or
pre-pay capacity. Onchain pool capacity still grows incrementally per
upload (pay-as-you-go for actual encoded usage), so a non-zero
`avg_blob_size` costs nothing for normal workloads. Setting
`avg_blob_size = 0` reproduces the historical upper-bound behavior
byte-for-byte; accounts created before this feature default to `0`.

**Onchain shrink semantics.** When the new cap is lower than the
pool's current reserved capacity and at least one encoded byte can
be freed without orphaning data, Oyster submits a Pearl-signed
`system::decrease_storage_pool_capacity_by_size` PTB. The contract
extracts a `Storage` object covering the freed bytes and transfers
it back to the pool's owner (the Pearl-managed sender). Over time
these extracted `Storage` objects accumulate in the wallet[^orphan].

[^orphan]: Tooling for absorbing accumulated `Storage` objects back
    into a pool is planned but not yet present. See the project's
    `PLAN.md` for the "Recycle orphaned Storage objects" item.

**Errors:**

| Status | Condition |
|--------|-----------|
| `400` | Body invalid (`max_unencoded_bytes` ≤ 0 or `avg_blob_size` < 0), `would_orphan` (the new cap is below the account's current onchain encoded usage), or `shrink_aborted` (a concurrent upload re-consumed the freed capacity between the onchain read and the PTB submission) |
| `401` | Missing or invalid admin key |
| `403` | Account does not belong to the authenticated app |
| `404` | Account not found |
| `503` | Pearl or Sui RPC unavailable while performing the onchain read or PTB submission |

**`would_orphan` body**: emitted when lowering would drop the cap
below current onchain usage:

```json
{
  "error": "max-storage update would orphan stored data: ...",
  "would_orphan": {
    "max_unencoded_bytes": 1000000000,
    "used_encoded_bytes": 4123456789,
    "threshold_encoded": 1500000000
  }
}
```

**`shrink_aborted` body**: emitted when the shrink PTB aborted
because another replica's upload raced and re-consumed the
capacity Oyster was about to extract:

```json
{
  "error": "max-storage shrink aborted: ...",
  "shrink_aborted": {
    "move_abort_description": "EInsufficientCapacity in storage_pool::extract_storage",
    "max_unencoded_bytes": 2000000000,
    "extract_size": 1234567890
  }
}
```

Both `would_orphan` and `shrink_aborted` are safe to retry after
the underlying state has settled (for example, delete some blobs to lower
`used_encoded_bytes`, or wait for the concurrent upload to finish).

A successful cap change writes an `account.max_storage_updated`
audit event.

## API Keys

### Create API Key

```
POST /api/v1/accounts/{account_id}/api-keys
```

Creates a new API key for an existing account.

**Path parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `account_id` | string | UUID of the account |

**Example:**

```bash
curl -s -X POST \
  -H "Authorization: Bearer $ADMIN_KEY" \
  "$OYSTER_URL/api/v1/accounts/550e8400-e29b-41d4-a716-446655440000/api-keys" | jq
```

**Response** (`201 Created`):

```json
{
  "id": "b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e",
  "prefix": "a1b2c3d4",
  "bearer_token": "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2",
  "created_at": "2025-01-15T10:30:00Z"
}
```

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Unique key identifier |
| `prefix` | string | First 8 characters of the raw key |
| `bearer_token` | string | The full API key, **shown only once** |
| `created_at` | string | ISO 8601 timestamp |

**Errors:**

| Status | Condition |
|--------|-----------|
| `401` | Missing or invalid admin key |
| `403` | Account does not belong to the authenticated app |
| `404` | Account not found |

### Revoke API Key

```
DELETE /api/v1/accounts/{account_id}/api-keys/{key_id}
```

Revokes an API key. The key immediately stops working for authentication.

**Path parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `account_id` | string | UUID of the account |
| `key_id` | string | ID of the API key to revoke |

**Example:**

```bash
curl -s -X DELETE \
  -H "Authorization: Bearer $ADMIN_KEY" \
  "$OYSTER_URL/api/v1/accounts/550e8400-e29b-41d4-a716-446655440000/api-keys/b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e"
```

**Response:** `204 No Content`

**Errors:**

| Status | Condition |
|--------|-----------|
| `401` | Missing or invalid admin key |
| `403` | Account does not belong to the authenticated app |
| `404` | API key not found or already revoked |

## S3 Access Keys

These endpoints manage S3-compatible access keys for accounts. See
[S3 Access Keys](access-keys.md) for key format details and limits.

### Create Access Key

```
POST /api/v1/accounts/{account_id}/access-keys
```

Creates a new S3 access key pair. The secret is returned **only once**, so
save it immediately. Each account can have up to **3 active access keys**.

**Path parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `account_id` | string | UUID of the account |

**Example:**

```bash
curl -s -X POST \
  -H "Authorization: Bearer $ADMIN_KEY" \
  "$OYSTER_URL/api/v1/accounts/550e8400-e29b-41d4-a716-446655440000/access-keys" | jq
```

**Response** (`201 Created`):

```json
{
  "access_key_id": "OYAK1234567890ABCDEF",
  "secret_access_key": "abcdef1234567890abcdef1234567890abcdef12",
  "created_at": "2025-01-15T10:30:00Z"
}
```

| Field | Type | Description |
|-------|------|-------------|
| `access_key_id` | string | 20-character key ID (starts with `OYAK`) |
| `secret_access_key` | string | 40-character hex secret, **shown only once** |
| `created_at` | string | ISO 8601 timestamp |

**Errors:**

| Status | Condition |
|--------|-----------|
| `401` | Missing or invalid admin key |
| `403` | Account does not belong to the authenticated app |
| `404` | Account not found |
| `409` | Access key limit reached (max 3 active keys) |

### List Access Keys

```
GET /api/v1/accounts/{account_id}/access-keys
```

Returns all S3 access keys for the account, including revoked ones.
Secrets are **never** included in list responses.

**Path parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `account_id` | string | UUID of the account |

**Example:**

```bash
curl -s \
  -H "Authorization: Bearer $ADMIN_KEY" \
  "$OYSTER_URL/api/v1/accounts/550e8400-e29b-41d4-a716-446655440000/access-keys" | jq
```

**Response** (`200 OK`):

```json
[
  {
    "access_key_id": "OYAK1234567890ABCDEF",
    "created_at": "2025-01-15T10:30:00Z",
    "revoked_at": null
  },
  {
    "access_key_id": "OYAKFEDCBA0987654321",
    "created_at": "2025-01-10T08:00:00Z",
    "revoked_at": "2025-01-14T12:00:00Z"
  }
]
```

| Field | Type | Description |
|-------|------|-------------|
| `access_key_id` | string | 20-character key ID |
| `created_at` | string | ISO 8601 timestamp |
| `revoked_at` | string or null | ISO 8601 timestamp if revoked, `null` if active |

**Errors:**

| Status | Condition |
|--------|-----------|
| `401` | Missing or invalid admin key |
| `403` | Account does not belong to the authenticated app |
| `404` | Account not found |

### Revoke Access Key

```
DELETE /api/v1/accounts/{account_id}/access-keys/{access_key_id}
```

Revokes an S3 access key. Any S3 requests using this key stop
working immediately. Revoked keys no longer count toward the
3-key active limit.

**Path parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `account_id` | string | UUID of the account |
| `access_key_id` | string | The 20-character access key ID to revoke |

**Example:**

```bash
curl -s -X DELETE \
  -H "Authorization: Bearer $ADMIN_KEY" \
  "$OYSTER_URL/api/v1/accounts/550e8400-e29b-41d4-a716-446655440000/access-keys/OYAK1234567890ABCDEF"
```

**Response:** `204 No Content`

**Errors:**

| Status | Condition |
|--------|-----------|
| `401` | Missing or invalid admin key |
| `403` | Account does not belong to the authenticated app |
| `404` | Access key not found or already revoked |

## App

### Get App

```
GET /api/v1/admin/app
```

Returns the authenticated app, including the current webhook URL and
the base64-encoded Ed25519 public key paired with it. Use this endpoint
when you need to retrieve the response from `PUT /admin/app/webhook`.

**Response** (`200 OK`):

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "name": "my-app",
  "contact_email": "admin@example.com",
  "webhook_url": "https://example.com/oyster/webhook",
  "webhook_public_key": "base64-encoded-32-byte-key",
  "created_at": "2025-01-15T10:30:00Z"
}
```

| Field | Type | Description |
|-------|------|-------------|
| `webhook_url` | string or null | Currently configured webhook URL, or `null` when none |
| `webhook_public_key` | string or null | Base64-encoded 32-byte Ed25519 public key, or `null` when no webhook is configured |

**Errors:**

| Status | Condition |
|--------|-----------|
| `401` | Missing or invalid admin key |

### Set Webhook URL

```
PUT /api/v1/admin/app/webhook
```

Registers or rotates the webhook URL for the authenticated app. Each
call generates a fresh Ed25519 keypair; the response is the only
opportunity to capture the public key for verification. Subsequent
deliveries are signed with the corresponding private key. See
[Webhooks](../guides/webhooks.md) for the signature format.

**Request body:**

```json
{ "webhook_url": "https://example.com/oyster/webhook" }
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `webhook_url` | string | yes | Receiver URL. Must be `https://`, ≤ 2048 chars, must not embed credentials, must have a host |

**Response** (`200 OK`): same shape as `GET /admin/app` above.

**Errors:**

| Status | Condition |
|--------|-----------|
| `400` | Webhook URL invalid (bad scheme, embedded credentials, oversize, host-less, malformed) |
| `401` | Missing or invalid admin key |

### Clear Webhook URL

```
DELETE /api/v1/admin/app/webhook
```

Clears the webhook URL and discards the keypair. Subsequent
extension failures do not deliver a webhook.

**Response** (`200 OK`): the updated app row with all three webhook
fields nulled.

**Errors:**

| Status | Condition |
|--------|-----------|
| `401` | Missing or invalid admin key |

## Server Commands

`oysterd` is the server binary. Besides the `oysterd app` subcommands below
(for managing apps and admin keys), it runs the service itself.

### Running the Server

```bash
oysterd serve     # run the HTTP + S3 server (default when no subcommand given)
oysterd extend     # run the background blob-extension service only
```

`oysterd serve` is the default; running `oysterd` with no subcommand is
equivalent. `oysterd extend` runs only the background task that renews
expiring storage pools (see [Blob Lifecycle](../guides/blob-lifecycle.md)),
without serving the API.

The global `--pearl-service-secret-file <PATH>` flag reads the Pearl service
secret from a file instead of the `PEARL_SERVICE_SECRET` environment variable,
which is useful for mounting the secret as a file (for example, a Kubernetes secret). It applies
to any `oysterd` invocation.

Both services are otherwise configured through environment variables; see the
[README](https://github.com/MystenLabs/oyster#configuration) for the full
env-var reference.

### Create App

```bash
oysterd app new --name <NAME> --contact_email <EMAIL> [--no-key]
```

Creates a new app, prints its UUID, and (by default) auto-issues a first
admin key alongside. Webhook URLs are configured by the app builder
using the self-service `PUT /admin/app/webhook` endpoint above (or
`oyster app webhook set <URL>`).

| Flag | Required | Description |
|------|----------|-------------|
| `--name` | yes | Human-readable app name |
| `--contact_email` | yes | Contact email for the app owner |
| `--no-key` | no | Skip the auto-issued first admin key |

**Example:**

```bash
oysterd app new --name "my-app" --contact_email "admin@example.com"
# 550e8400-e29b-41d4-a716-446655440000
# 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
```

### Issue Admin Key

```bash
oysterd app issue-admin-key <app_id>
```

Generates a fresh admin key for the given app. Multiple admin keys per
app are supported with no cap; use this for AWS-style two-key rotation.

**stdout** carries the raw admin key as a single line; this is the only
machine-readable output, suitable for capturing in a variable or piping. The
key id and 8-char prefix are **not** printed to stdout; they appear in a
`tracing::info!` structured log line (fields `app_id`, `key_id`, `prefix`,
message `issued admin key`) written to **stderr**. That line is emitted at the
`info` level, so it shows with the default log filter but is suppressed if
`RUST_LOG` raises the threshold above `info`. It is a human-readable log line,
not a stable machine-readable string. To recover a key id reliably, use
[List Admin Keys](#list-admin-keys).

**Example:**

```bash
oysterd app issue-admin-key 550e8400-e29b-41d4-a716-446655440000
# stdout: 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
# stderr (info log): ... issued admin key app_id=550e8400-... key_id=<key_id> prefix=01234567
```

The printed key can be used directly in the `Authorization` header:

```bash
curl -H "Authorization: Bearer $(oysterd app issue-admin-key $APP_ID)" ...
```

### List Admin Keys

```bash
oysterd app list-admin-keys <app_id>
```

Lists all admin keys for the given app in tab-separated format, including
revoked ones (so an operator can confirm what is currently live). There is no
header row; columns are `id`, `prefix`, `created_at`, `revoked_at`. The
`revoked_at` column is the **empty string** for active keys (so a line for an
active key ends with a trailing tab).

**Example output** (`→` marks a tab):

```
b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e→01234567→2026-04-15T10:30:00Z→
a1b2c3d4-e5f6-7890-abcd-ef0123456789→89abcdef→2026-03-01T08:00:00Z→2026-04-15T10:31:00Z
```

### Revoke Admin Key

```bash
oysterd app revoke-admin-key <key_id>
```

Marks an admin key as revoked. Subsequent requests using that key are
rejected with `401`. Revocation is by `key_id` (globally unique), not by
the raw key value.

**Example:**

```bash
oysterd app revoke-admin-key b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e
```

### List Apps

```bash
oysterd app list
```

Lists all registered apps in tab-separated format.

**Example output:**

```
ID	NAME	CONTACT_EMAIL
550e8400-e29b-41d4-a716-446655440000	my-app	admin@example.com
```

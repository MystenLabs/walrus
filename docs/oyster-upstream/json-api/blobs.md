# Blobs

Blobs are binary objects stored inside buckets. Each blob is identified by a
user-chosen **key** (like a file path) and has a content-addressed **blob ID**
computed from its contents.

Key properties:
- **Reads are public**: no authentication needed to download a blob
- **Writes require auth**: uploading, updating, and deleting need a Bearer
  token
- **Overwrite semantics**: uploading to an existing key replaces the blob
- **Content-addressed**: identical content is stored only once
- **Reference-counted deletion**: underlying data is removed only when no
  keys reference it
- **Pool-scoped expiration**: blobs share their account's `StoragePool`
  lifetime; a background extension service renews each pool before it
  expires (see [Blob Lifecycle](../guides/blob-lifecycle.md))

## Store Blob

```
PUT /api/v1/buckets/{bucket_name}/blobs/{key}
```

Uploads binary data to the specified bucket and key. If a blob already
exists at that key, it is replaced.

**Path parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `bucket_name` | string | Target bucket |
| `key` | string | Object key (for example, `images/photo.png`) |

**Request headers:**

| Header | Default | Description |
|--------|---------|-------------|
| `Content-Type` | `application/octet-stream` | MIME type stored with the blob |
| `If-Match` | — | Only overwrite if the existing blob's ETag matches (412 otherwise) |
| `If-None-Match` | — | Set to `*` to create only if the key doesn't exist (412 otherwise) |
| `x-oyster-tag` | — | Attach a tag as `key=value` (percent-decoded). Repeatable; send the header once per tag. See [Blob Tags](#blob-tags) |

**Request body:** Raw binary data (max **1 GB**)

**Example: upload a string:**

```bash
curl -s -X PUT \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: text/plain" \
  --data-binary "Hello, Oyster!" \
  "$OYSTER_URL/api/v1/buckets/my-bucket/blobs/hello.txt" | jq
```

**Example: upload a file:**

```bash
curl -s -X PUT \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: image/png" \
  --data-binary @photo.png \
  "$OYSTER_URL/api/v1/buckets/my-bucket/blobs/images/photo.png" | jq
```

**Example: create only (fail if key exists):**

```bash
curl -s -X PUT \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: text/plain" \
  -H "If-None-Match: *" \
  --data-binary "Hello, Oyster!" \
  "$OYSTER_URL/api/v1/buckets/my-bucket/blobs/hello.txt" | jq
```

**Example: safe overwrite (only if ETag matches):**

```bash
curl -s -X PUT \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: text/plain" \
  -H 'If-Match: "9a0364b9e99bb480dd25e1f0284c8555"' \
  --data-binary "Updated content" \
  "$OYSTER_URL/api/v1/buckets/my-bucket/blobs/hello.txt" | jq
```

**Example: upload with tags:**

```bash
curl -s -X PUT \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: text/plain" \
  -H "x-oyster-tag: env=prod" \
  -H "x-oyster-tag: team=platform" \
  --data-binary "Hello, Oyster!" \
  "$OYSTER_URL/api/v1/buckets/my-bucket/blobs/hello.txt" | jq
```

Each `x-oyster-tag` header carries exactly one `key=value` pair (percent-decoded;
no `&`-joined pairs). Tags are **replaced** on every PUT, so re-uploading a key
without any `x-oyster-tag` headers clears its tags. The same caps as the
[Blob Tags](#blob-tags) endpoints apply. See [Tag rules](#tag-rules).

**Response** (`201 Created`):

```json
{
  "key": "hello.txt",
  "blob_id": "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
  "size": 14,
  "md5": "9a0364b9e99bb480dd25e1f0284c8555",
  "sui_object_id": null,
  "created_at": "2025-01-15T10:31:00Z"
}
```

| Field | Type | Description |
|-------|------|-------------|
| `key` | string | The object key |
| `blob_id` | string | Content-addressed hash of the blob data |
| `size` | integer | Size in bytes |
| `md5` | string | Hex-encoded MD5 digest (used as S3 ETag) |
| `sui_object_id` | string or null | Onchain Sui object ID (if stored on Walrus) |
| `created_at` | string | ISO 8601 timestamp |

The response includes an `ETag` header containing the quoted MD5 digest
(for example, `"9a0364b9e99bb480dd25e1f0284c8555"`).

**Errors:**

| Status | Condition |
|--------|-----------|
| `400` | Upload would push the account past its per-account `max_unencoded_bytes` cap (body carries a `cap_exceeded` block) |
| `401` | Missing or invalid API key |
| `402` | Pearl-derived wallet lacks WAL/SUI to fund the upload (body carries a `funding_required` block; see [Cross-Cutting Error Contracts](README.md#cross-cutting-error-contracts)) |
| `404` | Bucket not found |
| `412` | `If-Match` or `If-None-Match` condition failed |
| `413` | Payload exceeds 1 GB, or exceeds the Walrus encoder's per-blob ceiling for the network's `n_shards` (body carries a `payload_too_large` block) |

When the cap is exceeded the response body looks like:

```json
{
  "error": "storage cap exceeded: ...",
  "cap_exceeded": {
    "max_unencoded_bytes": 5000000000,
    "used_encoded_bytes": 4998123456,
    "new_unencoded_bytes": 16384,
    "admin_endpoint": "PUT /api/v1/accounts/{account_id}/max-storage"
  }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `max_unencoded_bytes` | integer | Configured per-account cap, in *unencoded* bytes |
| `used_encoded_bytes` | integer | Onchain encoded usage observed at check time |
| `new_unencoded_bytes` | integer | Unencoded size of the rejected upload |
| `admin_endpoint` | string | Admin route that can raise the cap |

The cap is enforced in unencoded bytes against onchain encoded usage
through the same `f = encoded_blob_length_for_n_shards` that the upload
path uses to project the post-upload encoded total, so the
short-circuit fires before any onchain work. Raise (or lower) the
cap through the admin
[Update Storage Cap](admin.md#update-storage-cap) endpoint.

## Read Blob by Key

```
GET /api/v1/buckets/{bucket_name}/blobs/{key}
```

Downloads a blob's contents. **No authentication required.**

**Path parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `bucket_name` | string | Bucket containing the blob |
| `key` | string | Object key |

**Example:**

```bash
curl -s "$OYSTER_URL/api/v1/buckets/my-bucket/blobs/hello.txt"
```

**Conditional headers:**

| Header | Effect |
|--------|--------|
| `If-Match: "<etag>"` | Return the blob only if its ETag matches; otherwise `412` |
| `If-None-Match: "<etag>"` | Return the blob only if its ETag differs; otherwise `304` |

**Example: cache validation:**

```bash
curl -s -o /dev/null -w "%{http_code}" \
  -H 'If-None-Match: "9a0364b9e99bb480dd25e1f0284c8555"' \
  "$OYSTER_URL/api/v1/buckets/my-bucket/blobs/hello.txt"
# Returns 304 if unchanged, 200 with body if changed
```

**Response** (`200 OK`):
- **Body:** Raw binary blob data
- **Content-Type:** The MIME type set during upload
- **ETag:** Quoted MD5 digest (for example, `"9a0364b9e99bb480dd25e1f0284c8555"`)
- **Content-Disposition:** `attachment`. Because reads are public and
  serve a caller-supplied Content-Type, blobs are returned as downloads
  so a `text/html`/SVG payload can't execute as a page on the Oyster
  origin. The response also carries `X-Content-Type-Options: nosniff`
  and `Content-Security-Policy: default-src 'none'; sandbox`. Embedding
  a blob as a subresource (`<img>`, `<video>`, `<script src>`, `fetch`)
  is unaffected; only direct top-level navigation downloads instead of
  rendering.

**Errors:**

| Status | Condition |
|--------|-----------|
| `304` | `If-None-Match` matched: blob has not changed |
| `404` | Blob not found |
| `412` | `If-Match` condition failed |

## Read Blob by Blob ID

```
GET /api/v1/blobs/by-blob-id/{blob_id}
```

Downloads a blob by its content-addressed hash. Useful when you know the
blob ID but not which bucket or key it's stored under.
**No authentication required.**

**Path parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `blob_id` | string | Content-addressed blob hash |

**Example:**

```bash
curl -s "$OYSTER_URL/api/v1/blobs/by-blob-id/2cf24dba5fb0a30e..."
```

**Response** (`200 OK`):
- **Body:** Raw binary blob data
- **Content-Type:** `application/octet-stream`

**Errors:**

| Status | Condition |
|--------|-----------|
| `404` | Blob ID not found |

## List Blobs

```
GET /api/v1/buckets/{bucket_name}/blobs
```

Returns a paginated list of blobs in a bucket.

**Path parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `bucket_name` | string | Bucket to list |

**Query parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `cursor` | string | — | Opaque cursor from a previous `next_cursor` |
| `limit` | integer | 20 | Items per page (max: 100) |

**Example:**

```bash
curl -s -H "Authorization: Bearer $API_KEY" \
  "$OYSTER_URL/api/v1/buckets/my-bucket/blobs?limit=50" | jq
```

**Response** (`200 OK`):

```json
{
  "data": [
    {
      "key": "hello.txt",
      "blob_id": "2cf24dba5fb0a30e...",
      "bucket_name": "my-bucket",
      "account_id": "550e8400-e29b-41d4-a716-446655440000",
      "content_type": "text/plain",
      "size": 14,
      "md5": "9a0364b9e99bb480...",
      "sui_object_id": null,
      "created_at": "2025-01-15T10:31:00Z"
    }
  ],
  "next_cursor": null
}
```

| Field | Type | Description |
|-------|------|-------------|
| `key` | string | Object key |
| `blob_id` | string | Content-addressed hash |
| `bucket_name` | string | Containing bucket |
| `account_id` | string | Owning account UUID |
| `content_type` | string | MIME type |
| `size` | integer | Size in bytes |
| `md5` | string | Hex-encoded MD5 digest |
| `sui_object_id` | string or null | Onchain Sui object ID |
| `created_at` | string | ISO 8601 timestamp |

## Update Blob Metadata

```
PATCH /api/v1/buckets/{bucket_name}/blobs/{key}/metadata
```

Updates metadata for an existing blob. Currently only `content_type` can be
changed.

**Path parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `bucket_name` | string | Bucket containing the blob |
| `key` | string | Object key |

**Request body:**

```json
{
  "content_type": "image/png"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `content_type` | string | yes | New MIME type for the blob |

**Example:**

```bash
curl -s -X PATCH \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"content_type": "image/png"}' \
  "$OYSTER_URL/api/v1/buckets/my-bucket/blobs/photo.png/metadata" | jq
```

**Response** (`200 OK`): Full blob metadata (same shape as items in
[List Blobs](#list-blobs)).

**Errors:**

| Status | Condition |
|--------|-----------|
| `400` | `content_type` not provided |
| `401` | Missing or invalid API key |
| `404` | Blob not found |

## Delete Blob

```
DELETE /api/v1/buckets/{bucket_name}/blobs/{key}
```

Deletes a blob by key. The underlying data is only removed from storage
when no other keys reference the same content (reference-counted deletion).

**Path parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `bucket_name` | string | Bucket containing the blob |
| `key` | string | Object key to delete |

**Conditional headers:**

| Header | Effect |
|--------|--------|
| `If-Match: "<etag>"` | Delete only if ETag matches; otherwise `412` |
| `If-None-Match: "<etag>"` | Delete only if ETag differs; otherwise `412` |

**Example:**

```bash
curl -s -X DELETE \
  -H "Authorization: Bearer $API_KEY" \
  "$OYSTER_URL/api/v1/buckets/my-bucket/blobs/hello.txt"
```

**Example: delete only if ETag matches:**

```bash
curl -s -X DELETE \
  -H "Authorization: Bearer $API_KEY" \
  -H 'If-Match: "9a0364b9e99bb480dd25e1f0284c8555"' \
  "$OYSTER_URL/api/v1/buckets/my-bucket/blobs/hello.txt"
```

**Response:** `204 No Content`

**Errors:**

| Status | Condition |
|--------|-----------|
| `401` | Missing or invalid API key |
| `402` | Insufficient onchain balance to clear the `PooledBlob`; the DB row is left intact for retry |
| `404` | Blob not found |
| `412` | `If-Match` or `If-None-Match` condition failed |

A `402` carries the same `funding_required` block as the upload path:

```json
{
  "error": "insufficient balance: ...",
  "funding_required": {
    "wal_frost": "1234567890",
    "sui_mist": "98765432"
  }
}
```

`wal_frost` and `sui_mist` are decimal strings (Pearl-derived
wallet's owed top-up); inspect your wallet through
[Get Wallet Address](wallet.md). When `delete_blob` returns `402`,
the DB row is left intact on purpose so the client can fund the
Pearl-derived wallet and retry the same `DELETE`. Other onchain
delete errors are still swallowed to preserve idempotent-delete
semantics.

## Blob Tags

Each blob can carry a small set of arbitrary `key=value` tags, stored in
Oyster's database (independent of the underlying blob content). Tags set through
this JSON API and tags set through the [S3 Object Tagging](../s3-api/objects.md#object-tagging)
operations share the same backing store. A tag written through one API is
visible through the other.

All tag endpoints live under a blob's `/tags` path, require Bearer auth, and
return `404` if the blob does not exist or is not owned by the authenticated
account.

### Tag rules

| Limit | Value |
|-------|-------|
| Max tags per blob | 10 |
| Max tag key length | 128 bytes |
| Max tag value length | 256 bytes |
| Max total set size | 2048 bytes (sum of all keys + values) |

Allowed characters in keys and values: ASCII alphanumerics plus space and
`+ - = . _ : / @`. Keys must be non-empty; values might be empty. Duplicate keys
are rejected. Any request whose resulting tag set violates these rules returns
`400`.

### Get Tags

```
GET /api/v1/buckets/{bucket_name}/blobs/{key}/tags
```

Returns all tags on the blob.

**Example:**

```bash
curl -s -H "Authorization: Bearer $API_KEY" \
  "$OYSTER_URL/api/v1/buckets/my-bucket/blobs/hello.txt/tags" | jq
```

**Response** (`200 OK`):

```json
{
  "tags": {
    "env": "prod",
    "team": "platform"
  }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `tags` | object | Map of tag keys to values (empty object if the blob has no tags) |

### Replace Tags

```
PUT /api/v1/buckets/{bucket_name}/blobs/{key}/tags
```

Replaces the blob's **entire** tag set with the supplied map. Tags not present
in the request are removed.

**Request body:**

```json
{ "tags": { "env": "prod", "team": "platform" } }
```

**Example:**

```bash
curl -s -X PUT \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"tags": {"env": "prod", "team": "platform"}}' \
  "$OYSTER_URL/api/v1/buckets/my-bucket/blobs/hello.txt/tags"
```

**Response:** `204 No Content`

**Errors:** `400` if the tag set violates [tag rules](#tag-rules); `401` if
unauthenticated; `404` if the blob is not found.

### Merge Tags

```
PATCH /api/v1/buckets/{bucket_name}/blobs/{key}/tags
```

Merges the supplied map into the blob's existing tags (upsert per key). Keys not
mentioned in the request are left untouched.

**Request body:**

```json
{ "tags": { "team": "storage" } }
```

**Example:**

```bash
curl -s -X PATCH \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"tags": {"team": "storage"}}' \
  "$OYSTER_URL/api/v1/buckets/my-bucket/blobs/hello.txt/tags"
```

**Response:** `204 No Content`

**Errors:** `400` if the **merged** set would violate [tag rules](#tag-rules)
(for example, exceed the 10-tag cap); `401` if unauthenticated; `404` if the blob is not
found.

### Delete All Tags

```
DELETE /api/v1/buckets/{bucket_name}/blobs/{key}/tags
```

Clears every tag on the blob.

**Example:**

```bash
curl -s -X DELETE \
  -H "Authorization: Bearer $API_KEY" \
  "$OYSTER_URL/api/v1/buckets/my-bucket/blobs/hello.txt/tags"
```

**Response:** `204 No Content`

### Set a Single Tag

```
PUT /api/v1/buckets/{bucket_name}/blobs/{key}/tags/{tag_key}
```

Upserts a single tag. The request body is the raw tag value as
**`text/plain`** (not JSON).

**Example:**

```bash
curl -s -X PUT \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: text/plain" \
  --data-binary "prod" \
  "$OYSTER_URL/api/v1/buckets/my-bucket/blobs/hello.txt/tags/env"
```

**Response:** `204 No Content`

**Errors:** `400` if adding the tag would exceed the 10-tag cap or the
value/key violates [tag rules](#tag-rules); `401` if unauthenticated; `404` if
the blob is not found.

### Delete a Single Tag

```
DELETE /api/v1/buckets/{bucket_name}/blobs/{key}/tags/{tag_key}
```

Deletes a single tag by key. Idempotent: deleting a tag that doesn't exist
still returns `204`.

**Example:**

```bash
curl -s -X DELETE \
  -H "Authorization: Bearer $API_KEY" \
  "$OYSTER_URL/api/v1/buckets/my-bucket/blobs/hello.txt/tags/env"
```

**Response:** `204 No Content`

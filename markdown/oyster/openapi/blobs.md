> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

# Blobs Endpoints

Blob storage and retrieval

Base URL: `https://oyster.testnet.mystenlabs.com/api/v1`

> **Info**
>
> This page is auto-generated from the [OpenAPI spec](/oyster/openapi.json).
> For an interactive explorer, see the [Interactive API Reference](../api-reference.mdx).
## GET `/blobs/by-blob-id/{blob_id}`

**Read a blob's content by its content-addressed blob ID. No authentication required.**

**Parameters:**

| Name | In | Type | Required | Description |
|------|-----|------|----------|-------------|
| `blob_id` | path | string | Yes | Blob content-hash ID |

**Responses:**

- **200**: Blob data
- **404**: Blob not found

---

## GET `/buckets/{bucket_name}/blobs`

**List all blobs in a bucket, with cursor-based pagination.**

**Authentication:** Required

**Parameters:**

| Name | In | Type | Required | Description |
|------|-----|------|----------|-------------|
| `bucket_name` | path | string | Yes | Bucket name |
| `cursor` | query | string,null | No | Opaque cursor from a previous response. |
| `limit` | query | integer,null | No | Maximum number of items to return. |

**Responses:**

- **200**: List of blobs
- **401**: Unauthorized
- **404**: Bucket not found

---

## GET `/buckets/{bucket_name}/blobs/{key}`

**Read a blob's content by bucket name and key. No authentication required.**

**Parameters:**

| Name | In | Type | Required | Description |
|------|-----|------|----------|-------------|
| `bucket_name` | path | string | Yes | Bucket name |
| `key` | path | string | Yes | Object key |

**Responses:**

- **200**: Blob data
- **404**: Blob not found

---

## PUT `/buckets/{bucket_name}/blobs/{key}`

**Upload a blob into a bucket at the given key. The request body is the raw binary content. Uploading to the same key overwrites.**

**Authentication:** Required

**Parameters:**

| Name | In | Type | Required | Description |
|------|-----|------|----------|-------------|
| `bucket_name` | path | string | Yes | Bucket name |
| `key` | path | string | Yes | Object key |

**Request body:** `application/octet-stream`

```json
[null]
```

**Responses:**

- **201**: Blob stored
- **400**: Upload would push the account past its per-account `max_unencoded_bytes` cap. Body carries a `cap_exceeded` block; the admin can raise the cap via `PUT /api/v1/accounts/{account_id}/max-storage`.
- **401**: Unauthorized
- **402**: Insufficient on-chain balance to fund the upload. Body carries a `funding_required` block hinting at how much WAL (FROST) and SUI (MIST) the Pearl-derived wallet needs.
- **404**: Bucket not found
- **409**: Bucket was deleted concurrently while the upload was in flight. The on-chain `PooledBlob` is best-effort compensated before this response is returned; see `oyster_post_store_compensation_total` for the success/failure outcome.
- **413**: Payload too large. Either the body exceeded the static MAX_BLOB_SIZE cap (no structured body) or the upload exceeded the Walrus encoder's per-blob ceiling for this network's n_shards (body carries a `payload_too_large` block).

---

## DELETE `/buckets/{bucket_name}/blobs/{key}`

**Delete a blob by its bucket name and key. The underlying data is only removed when no other objects reference it.**

**Authentication:** Required

**Parameters:**

| Name | In | Type | Required | Description |
|------|-----|------|----------|-------------|
| `bucket_name` | path | string | Yes | Bucket name |
| `key` | path | string | Yes | Object key |

**Responses:**

- **204**: Blob deleted
- **401**: Unauthorized
- **402**: Insufficient on-chain balance to clear the PooledBlob. Body carries a `funding_required` block.
- **404**: Blob not found

---

## PATCH `/buckets/{bucket_name}/blobs/{key}/metadata`

**Update a blob's content type.**

**Authentication:** Required

**Parameters:**

| Name | In | Type | Required | Description |
|------|-----|------|----------|-------------|
| `bucket_name` | path | string | Yes | Bucket name |
| `key` | path | string | Yes | Object key |

**Request body:** `application/json`

```json
{
  "content_type": "<string,null>"
}
```

**Responses:**

- **200**: Metadata updated
- **400**: Bad request
- **401**: Unauthorized
- **404**: Blob not found

---

## GET `/buckets/{bucket_name}/blobs/{key}/tags`

**List all tags on a blob.**

**Authentication:** Required

**Parameters:**

| Name | In | Type | Required | Description |
|------|-----|------|----------|-------------|
| `bucket_name` | path | string | Yes | Bucket name |
| `key` | path | string | Yes | Object key |

**Responses:**

- **200**: Tags for the blob
- **401**: Unauthorized
- **404**: Blob not found

---

## PUT `/buckets/{bucket_name}/blobs/{key}/tags`

**Replace the entire tag set on a blob (S3 `PutObjectTagging`-equivalent).**

**Authentication:** Required

**Parameters:**

| Name | In | Type | Required | Description |
|------|-----|------|----------|-------------|
| `bucket_name` | path | string | Yes | Bucket name |
| `key` | path | string | Yes | Object key |

**Request body:** `application/json`

```json
{
  "tags": "<object>"
}
```

**Responses:**

- **204**: Tag set replaced
- **400**: Bad request
- **401**: Unauthorized
- **404**: Blob not found

---

## DELETE `/buckets/{bucket_name}/blobs/{key}/tags`

**Remove every tag from a blob (S3 `DeleteObjectTagging`-equivalent).**

**Authentication:** Required

**Parameters:**

| Name | In | Type | Required | Description |
|------|-----|------|----------|-------------|
| `bucket_name` | path | string | Yes | Bucket name |
| `key` | path | string | Yes | Object key |

**Responses:**

- **204**: All tags removed
- **401**: Unauthorized
- **404**: Blob not found

---

## PATCH `/buckets/{bucket_name}/blobs/{key}/tags`

**Merge a partial tag set into a blob's existing tags (upsert per key).**

**Authentication:** Required

**Parameters:**

| Name | In | Type | Required | Description |
|------|-----|------|----------|-------------|
| `bucket_name` | path | string | Yes | Bucket name |
| `key` | path | string | Yes | Object key |

**Request body:** `application/json`

```json
{
  "tags": "<object>"
}
```

**Responses:**

- **204**: Tags merged
- **400**: Bad request
- **401**: Unauthorized
- **404**: Blob not found

---

## PUT `/buckets/{bucket_name}/blobs/{key}/tags/{tag_key}`

**Upsert a single tag on a blob (body is the raw value).**

**Authentication:** Required

**Parameters:**

| Name | In | Type | Required | Description |
|------|-----|------|----------|-------------|
| `bucket_name` | path | string | Yes | Bucket name |
| `key` | path | string | Yes | Object key |
| `tag_key` | path | string | Yes | Tag key |

**Request body:** `text/plain`

**Responses:**

- **204**: Tag upserted
- **400**: Bad request
- **401**: Unauthorized
- **404**: Blob not found

---

## DELETE `/buckets/{bucket_name}/blobs/{key}/tags/{tag_key}`

**Delete a single tag from a blob. Idempotent.**

**Authentication:** Required

**Parameters:**

| Name | In | Type | Required | Description |
|------|-----|------|----------|-------------|
| `bucket_name` | path | string | Yes | Bucket name |
| `key` | path | string | Yes | Object key |
| `tag_key` | path | string | Yes | Tag key |

**Responses:**

- **204**: Tag removed (or did not exist)
- **401**: Unauthorized
- **404**: Blob not found

---
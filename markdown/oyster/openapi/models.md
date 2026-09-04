> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

# API Models

Request and response schemas used by the Walrus Oyster API.

> **Info**
>
> This page is auto-generated from the [OpenAPI spec](/oyster/openapi.json).
> For an interactive explorer, see the [Interactive API Reference](../api-reference.mdx).
## AccessKey

An S3 access key record (without the secret).

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `access_key_id` | string | Yes | The 20-character access key ID (e.g. "OYAK..."). |
| `created_at` | string | Yes | ISO 8601 creation timestamp. |
| `revoked_at` | string,null | No | ISO 8601 revocation timestamp, if revoked. |

## AccessKeyWithSecret

A newly created S3 access key, including the secret (shown only once).

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `access_key_id` | string | Yes | The 20-character access key ID. |
| `created_at` | string | Yes | ISO 8601 creation timestamp. |
| `secret_access_key` | string | Yes | The 40-character hex secret access key. |

## AccountId

A strongly-typed Oyster account identifier (UUID v4).

## AccountSummary

One-row summary of an account, returned by `GET /accounts`.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `active_api_key_count` | integer | Yes | Number of active (non-revoked) API keys on this account. |
| `created_at` | string | Yes | ISO 8601 creation timestamp. |
| `id` | AccountId | Yes | Unique identifier. |
| `name` | string | Yes | Human-readable account name. |

## ApiKeyMetadata

Public API key metadata. Never includes the bearer secret.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `created_at` | string | Yes | ISO 8601 creation timestamp. |
| `id` | string | Yes | Unique identifier. |
| `note` | string | Yes | Human-readable note (defaults to "api"). |
| `prefix` | string | Yes | First 8 characters of the raw key. |
| `revoked_at` | string,null | No | ISO 8601 revocation timestamp, if revoked. |

## ApiKeyWithBearerToken

A newly created API key, including the Bearer token (shown only once).

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `bearer_token` | string | Yes | The Bearer token (shown only once). |
| `created_at` | string | Yes | ISO 8601 creation timestamp. |
| `id` | string | Yes | Unique identifier. |
| `prefix` | string | Yes | First 8 characters of the raw key. |

## App

An Oyster app — organizational trust boundary for account creation.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `contact_email` | string | Yes | Contact email for the app owner. |
| `created_at` | string | Yes | ISO 8601 creation timestamp. |
| `id` | AppId | Yes | Unique identifier. |
| `name` | string | Yes | Human-readable app name. |
| `webhook_public_key` | string,null | No | Base64-encoded Ed25519 public key paired with the active webhook URL, or `None` when no webhook is configured. |
| `webhook_url` | string,null | No | Optional webhook URL for extension failure notifications. |

## AppId

A strongly-typed Oyster app identifier (UUID).

## AppWithPublicKey

Response shape for app endpoints that return the public key alongside
the standard `App` fields.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `contact_email` | string | Yes | Contact email for the app owner. |
| `created_at` | string | Yes | ISO 8601 creation timestamp. |
| `id` | AppId | Yes | Unique identifier. |
| `name` | string | Yes | Human-readable app name. |
| `webhook_public_key` | string,null | No | Base64-encoded Ed25519 public key for verifying webhook deliveries, or `None` when no webhook is configured. |
| `webhook_url` | string,null | No | Webhook URL, or `None` when no webhook is configured. |

## BlobMetadata

Metadata for a stored blob.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `account_id` | AccountId | Yes | Owning account ID. |
| `blob_id` | string | Yes | Content-addressed blob identifier. |
| `bucket_name` | string | Yes | Containing bucket name. |
| `content_type` | string | Yes | MIME content type. |
| `created_at` | string | Yes | ISO 8601 creation timestamp. |
| `encoded_size` | integer,null | No | Walrus-encoded size in bytes, if registered on-chain. `None` for local-store blobs and for dedup-skipped duplicate rows (the original registering row still carries the encoded size). |
| `key` | string | Yes | User-chosen object key (like a file path). |
| `md5` | string | Yes | Hex-encoded MD5 digest (S3 ETag). |
| `pooled_blob_object_id` | string,null | No | On-chain Sui object ID of the `PooledBlob`, if stored on Walrus. |
| `size` | integer | Yes | Size in bytes. |

## BlobTagsResponse

Response body for listing a blob's tags.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `tags` | object | Yes | Tags for the blob. Deterministically ordered for stable responses. |

## Bucket

A named container for blobs within an account.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `account_id` | AccountId | Yes | Owning account ID. |
| `created_at` | string | Yes | ISO 8601 creation timestamp. |
| `name` | string | Yes | Globally unique bucket name (primary key). |

## CapExceededDetails

Details accompanying a 400 from the storage-cap path.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `admin_endpoint` | string | Yes | Admin route that can raise the cap. |
| `max_unencoded_bytes` | integer | Yes | Configured per-account cap, in *unencoded* bytes. |
| `new_unencoded_bytes` | integer | Yes | Unencoded size of the rejected upload. |
| `used_encoded_bytes` | integer | Yes | On-chain encoded usage observed at check time. |

## CapExceededErrorResponse

Body returned with a 400 Bad Request when an upload would push the
account past its `max_unencoded_bytes` cap. Emitted by `store_blob`
(JSON) and `put_object` (S3 surface mirrors the message text). The
`cap_exceeded` block points the caller at the admin endpoint that
can raise the cap.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `cap_exceeded` | CapExceededDetails | Yes | Structured details for the cap-exceeded case. |
| `error` | string | Yes | Human-readable error message. |

## CreateAccountRequest

Request body for creating a new account.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `avg_blob_size` | integer,null | No | Optional assumed average blob size, in *unencoded* bytes, used to turn `max_unencoded_bytes` into a *lower* bound on storable capacity (see [`Account::avg_blob_size`]). Defaults to `OYSTER_DEFAULT_AVG_BLOB_SIZE` (10 MB) when omitted. `0` disables inflation; negative values are rejected with 400; an oversized value is accepted as a silent no-op. |
| `max_unencoded_bytes` | integer,null | No | Optional per-account storage cap, in *unencoded* bytes. Defaults to `5_000_000_000` (5 × 10⁹ bytes) when omitted. Must be strictly positive; `0` and negative values are rejected with 400. |
| `name` | string,null | No | Human-readable account name. Defaults to the account ID if omitted. |
| `note` | string,null | No | Optional note attached to the auto-issued initial API key. Defaults to "api" when omitted. |

## CreateAccountResponse

Response after creating a new account.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `account_id` | AccountId | Yes | The new account ID. |
| `api_key` | ApiKeyWithBearerToken | Yes | The initial API key (with bearer token). |

## CreateApiKeyRequest

Request body for `POST /accounts/{id}/api-keys`.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `note` | string,null | No | Optional note attached to the new API key. Defaults to "api" when omitted. |

## CreateBucketRequest

Request body for creating a new bucket.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Desired bucket name. |

## ErrorResponse

Generic error response body.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `error` | string | Yes | Human-readable error message. |

## ExtendRequestResponse

Response for a user-requested storage-pool extension retry.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `pool_end_epoch` | integer | Yes | The pool's current end epoch as tracked by Oyster; advances once the retried extension lands. |
| `status` | string | Yes | Always `"scheduled"`: the background extension worker will retry on its next cycle (typically within a minute). |

## FundingAmount

WAL/SUI top-up estimate attached to both the synchronous 402 response
body and the `account.funding_required` webhook payload.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `sui_mist` | string | Yes | Required SUI, in MIST units (1 SUI = 1e9 MIST). |
| `wal_frost` | string | Yes | Required WAL, in FROST units (1 WAL = 1e9 FROST). |

## InsufficientBalanceErrorResponse

Body returned with a 402 Payment Required when the account's Pearl
wallet can't cover the requested operation. `funding_required` is
`None` when the price-lookup itself failed.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `error` | string | Yes | Human-readable error message. |
| `funding_required` | object | No |  |

## PaginatedResponse_BlobMetadata

A paginated list response.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `data` | array | Yes | The items in this page. |
| `next_cursor` | string,null | No | Opaque cursor for fetching the next page, if more results exist. |

## PaginatedResponse_Bucket

A paginated list response.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `data` | array | Yes | The items in this page. |
| `next_cursor` | string,null | No | Opaque cursor for fetching the next page, if more results exist. |

## PatchTagsRequest

Request body for a partial tag-set merge.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `tags` | object | Yes | Tags to upsert onto the existing set. |

## PayloadTooLargeDetails

Details accompanying a 413 from the encoder-ceiling path.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `max_unencoded_bytes_for_network` | integer | Yes | Per-network maximum unencoded blob size at this `n_shards`. |
| `n_shards` | integer | Yes | Number of shards in the network's encoding config. |
| `unencoded_size_bytes` | integer | Yes | Size of the rejected upload, in unencoded bytes. |

## PayloadTooLargeErrorResponse

Body returned with a 413 Payload Too Large when an upload
exceeds the network's per-blob encoder ceiling. Distinct from
the static `MAX_BLOB_SIZE` body-limit 413 (which has no structured
`payload_too_large` block).

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `error` | string | Yes | Human-readable error message. |
| `payload_too_large` | PayloadTooLargeDetails | Yes | Structured details for the encoder-ceiling case. |

## PoolOnChainState

On-chain `StoragePool` counters surfaced in the
[`UpdateMaxStorageResponse`].

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `reserved_encoded_bytes` | integer | Yes | `storage.storage_size` — encoded bytes reserved by the pool. |
| `used_encoded_bytes` | integer | Yes | `used_encoded_bytes` — encoded bytes currently consumed by registered blobs. |

## PutTagsRequest

Request body for a full tag-set replace.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `tags` | object | Yes | Complete replacement set of tags. |

## SetWebhookUrlRequest

Request body for `PUT /admin/app/webhook`.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `webhook_url` | string | Yes | The webhook URL to register. Must be `https://`, must not embed credentials, must have a host. Each call generates a fresh keypair. |

## StoreBlobResponse

Response after successfully storing a blob.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `blob_id` | string | Yes | Content-addressed blob ID. |
| `created_at` | string | Yes | ISO 8601 creation timestamp. |
| `key` | string | Yes | User-chosen object key. |
| `md5` | string | Yes | Hex-encoded MD5 digest (S3 ETag). |
| `pooled_blob_object_id` | string,null | No | On-chain Sui object ID of the `PooledBlob`, if applicable. |
| `size` | integer | Yes | Size in bytes. |

## UpdateBlobMetadataRequest

Request body for updating blob metadata.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `content_type` | string,null | No | New MIME content type. |

## UpdateMaxStorageRequest

Request body for `PUT /admin/accounts/{account_id}/max-storage`.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `avg_blob_size` | integer,null | No | Optional new assumed average blob size, in *unencoded* bytes (see [`Account::avg_blob_size`]). When omitted, the account's existing `avg_blob_size` is retained and the orphan/shrink threshold is recomputed against it. `0` disables inflation; negative values are rejected with 400; an oversized value is a silent no-op. |
| `max_unencoded_bytes` | integer | Yes | New per-account cap, in *unencoded* bytes. Must be strictly positive; `0` and negative values are rejected with 400. |

## UpdateMaxStorageResponse

Response body for `PUT /admin/accounts/{account_id}/max-storage`.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `account_id` | AccountId | Yes | The account whose cap was updated. |
| `avg_blob_size` | integer | Yes | The effective assumed average blob size after the update (see [`Account::avg_blob_size`]). Echoes the request value when supplied, otherwise the account's retained value. |
| `max_unencoded_bytes` | integer | Yes | The new cap, in *unencoded* bytes. |
| `pool` | object | No |  |
| `shrink_tx_digest` | string,null | No | Digest of the submitted shrink PTB, or `None` when no shrink was needed. |

## WalletResponse

Wallet information for an account.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `address` | string | Yes | Sui address of the wallet. |
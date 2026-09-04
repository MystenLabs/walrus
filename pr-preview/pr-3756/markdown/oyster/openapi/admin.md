> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

# Admin Endpoints

Admin endpoints (admin-key authenticated)

Base URL: `https://oyster.testnet.mystenlabs.com/api/v1`

> **Info**
>
> This page is auto-generated from the [OpenAPI spec](/oyster/openapi.json).
> For an interactive explorer, see the [Interactive API Reference](../api-reference.mdx).
## GET `/accounts`

**List accounts owned by the authenticated app, with active API key counts.**

**Authentication:** Required

**Responses:**

- **200**: Accounts owned by the authenticated app
- **401**: Unauthorized

---

## POST `/accounts`

**Create a new account owned by the authenticated app.**

**Authentication:** Required

**Request body:** `application/json`

```json
{
  "avg_blob_size": "<integer,null>",
  "max_unencoded_bytes": "<integer,null>",
  "name": "<string,null>",
  "note": "<string,null>"
}
```

**Responses:**

- **201**: Account created
- **400**: Invalid request (e.g. non-positive max_unencoded_bytes)
- **401**: Unauthorized

---

## GET `/accounts/{account_id}/access-keys`

**List S3 access keys for an account owned by the authenticated app.**

**Authentication:** Required

**Parameters:**

| Name | In | Type | Required | Description |
|------|-----|------|----------|-------------|
| `account_id` | path |  | Yes | Account ID |

**Responses:**

- **200**: List of access keys
- **401**: Unauthorized
- **403**: Forbidden
- **404**: Account not found

---

## POST `/accounts/{account_id}/access-keys`

**Create a new S3 access key for an account owned by the authenticated app.**

**Authentication:** Required

**Parameters:**

| Name | In | Type | Required | Description |
|------|-----|------|----------|-------------|
| `account_id` | path |  | Yes | Account ID |

**Responses:**

- **201**: Access key created
- **401**: Unauthorized
- **403**: Forbidden
- **404**: Account not found
- **409**: Access key limit reached

---

## DELETE `/accounts/{account_id}/access-keys/{access_key_id}`

**Revoke an S3 access key for an account owned by the authenticated app.**

**Authentication:** Required

**Parameters:**

| Name | In | Type | Required | Description |
|------|-----|------|----------|-------------|
| `account_id` | path |  | Yes | Account ID |
| `access_key_id` | path | string | Yes | Access key ID to revoke |

**Responses:**

- **204**: Access key revoked
- **401**: Unauthorized
- **403**: Forbidden
- **404**: Access key not found

---

## GET `/accounts/{account_id}/api-keys`

**List API key metadata for an account owned by the authenticated app.
Never returns the bearer secret.**

**Authentication:** Required

**Parameters:**

| Name | In | Type | Required | Description |
|------|-----|------|----------|-------------|
| `account_id` | path |  | Yes | Account ID |

**Responses:**

- **200**: API keys for the account
- **401**: Unauthorized
- **403**: Forbidden
- **404**: Account not found

---

## POST `/accounts/{account_id}/api-keys`

**Create a new API key for an account owned by the authenticated app.**

**Authentication:** Required

**Parameters:**

| Name | In | Type | Required | Description |
|------|-----|------|----------|-------------|
| `account_id` | path |  | Yes | Account ID |

**Request body:** `application/json`

```json
{
  "note": "<string,null>"
}
```

**Responses:**

- **201**: API key created
- **401**: Unauthorized
- **403**: Forbidden
- **404**: Account not found
- **409**: API key limit reached

---

## DELETE `/accounts/{account_id}/api-keys/{key_id}`

**Revoke an API key for an account owned by the authenticated app.**

**Authentication:** Required

**Parameters:**

| Name | In | Type | Required | Description |
|------|-----|------|----------|-------------|
| `account_id` | path |  | Yes | Account ID |
| `key_id` | path | string | Yes | API key ID to revoke |

**Responses:**

- **204**: API key revoked
- **401**: Unauthorized
- **403**: Forbidden
- **404**: API key not found

---

## PUT `/accounts/{account_id}/max-storage`

**Update an account's per-account `max_unencoded_bytes` cap.**

Logic:
1. If the account has no on-chain `StoragePool` yet, just update the DB
   cap — no on-chain action is needed because future uploads enforce the
   new cap before any lazy-create.
2. Otherwise read the on-chain pool's
   `reserved_encoded_capacity_bytes` / `used_encoded_bytes` and compute
   `threshold = f(new_cap)` (same `f` as the upload-side cap check).
3. If `used_encoded > threshold`, reject 400 — lowering the cap would
   orphan currently-stored data.
4. If `reserved_encoded > threshold`, submit a Pearl-signed
   `decrease_storage_pool_capacity_by_size` PTB. The contract's own
   assertion guarantees the chain refuses to cut into `used_encoded`,
   so a concurrent race surfaces as a structured 400 with the chain's
   MoveAbort context.
5. Persist the new cap and reconcile DB pool counters to the
   post-shrink on-chain truth only when the shrink succeeded (or none
   was needed).

**Authentication:** Required

**Parameters:**

| Name | In | Type | Required | Description |
|------|-----|------|----------|-------------|
| `account_id` | path |  | Yes | Account ID |

**Request body:** `application/json`

```json
{
  "avg_blob_size": "<integer,null>",
  "max_unencoded_bytes": "<integer>"
}
```

**Responses:**

- **200**: Cap updated; response carries the post-update on-chain pool snapshot and the shrink tx digest when a shrink was submitted
- **400**: Invalid request, would orphan data, or shrink aborted
- **401**: Unauthorized
- **403**: Forbidden
- **404**: Account not found
- **503**: On-chain shrink unavailable in this configuration

---

## GET `/admin/app`

**Return the authenticated app, including its current webhook URL and
public key. Useful when an admin lost the response from `set_webhook_url`.**

**Authentication:** Required

**Responses:**

- **200**: The authenticated app, including the webhook public key
- **401**: Unauthorized

---

## PUT `/admin/app/webhook`

**Register or rotate the webhook URL for the authenticated app. Each call
generates a fresh Ed25519 keypair; the returned public key is the only
way to verify subsequent webhook deliveries.**

**Authentication:** Required

**Request body:** `application/json`

```json
{
  "webhook_url": "<string>"
}
```

**Responses:**

- **200**: Webhook URL set; response includes the freshly-generated public key
- **400**: Invalid webhook URL
- **401**: Unauthorized

---

## DELETE `/admin/app/webhook`

**Clear the webhook URL and discard the keypair for the authenticated app.
Subsequent extension failures will not deliver a webhook.**

**Authentication:** Required

**Responses:**

- **200**: Webhook URL cleared
- **401**: Unauthorized

---
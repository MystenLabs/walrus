# Authentication

Oyster uses **Bearer tokens** for authenticated routes plus **public access**
for blob reads and infrastructure probes. There are two tiers of Bearer
token, distinguished by which routes they unlock; both share the same
`Authorization: Bearer <hex>` wire format.

## Authentication Modes at a Glance

| Route pattern | Auth mode | Purpose |
|---|---|---|
| Bucket CRUD, blob write/list/delete, wallet | API Key | Data operations |
| `GET .../blobs/{key}`, `GET /blobs/by-blob-id/...` | Public | Blob reads |
| `POST /accounts`, key management under `/accounts/{id}/...` | Admin Key | Admin operations |
| `/health`, `/ready`, `/metrics`, `/api/docs` | Public | Infrastructure |

> **How Oyster tells them apart:** the URL prefix selects the credential
> table. Admin routes look up the Bearer token in the `app_admin_keys`
> table (one app per key); data routes look it up in the `api_keys` table
> (one account per key). Both tokens are 64-char hex; the hash check
> happens on whichever table the route is statically wired to.

## Bearer Token (API Key) Authentication

Include your API key in the `Authorization` header:

```
Authorization: Bearer <api-key>
```

**Example:**

```bash
curl -s \
  -H "Authorization: Bearer $API_KEY" \
  "$OYSTER_URL/api/v1/buckets"
```

### Key properties

| Property | Value |
|---|---|
| Size | 32 bytes, hex-encoded (64 characters) |
| Hash algorithm | BLAKE2s-256 (only the hash is stored) |
| Prefix | First 8 characters, used to identify keys without exposing the secret |

API keys are provisioned through the Admin API (see
[Admin](admin.md)). The full secret is shown **only once** at
creation time. A lost key cannot be recovered.

### Errors

| Status | Condition |
|---|---|
| `401 Unauthorized` | Missing, malformed, or invalid API key |

## Admin-Key Authentication (for Apps)

Admin endpoints require a per-app **admin key** issued by the server
operator:

```
Authorization: Bearer <admin-key>
```

Admin keys are generated server-side with
`oysterd app issue-admin-key <app_id>`. They are **not** available through
a public API. Multiple admin keys per app are supported (AWS-style two-key
rotation, no cap).

### Key properties

| Property | Value |
|---|---|
| Size | 32 bytes, hex-encoded (64 characters) |
| Hash algorithm | BLAKE2s-256 (only the hash is stored) |
| Prefix | First 8 characters, used to identify keys in listings without exposing the secret |
| Lifetime | Long-lived; no expiry. Rotation is voluntary issue-then-revoke. |

### Account ownership enforcement

An app can only manage accounts it created. Attempting to access another
app's accounts returns **403 Forbidden**:

```json
{ "error": "forbidden: account does not belong to this app" }
```

### Rotation

Admin keys do not expire. The recommended pattern is AWS-style two-key
overlap:

1. Operator issues a new key alongside the old one
   (`oysterd app issue-admin-key <APP_ID>`).
2. Callers swap to the new key.
3. After confirming nothing still uses the old key, the operator revokes
   it (`oysterd app revoke-admin-key <OLD_KEY_ID>`). Revocation takes
   effect immediately, with no caching.

`oysterd app list-admin-keys <APP_ID>` shows all keys (active and
revoked) so an operator can audit before revoking.

### Errors

| Status | Condition |
|---|---|
| `401 Unauthorized` | Missing, malformed, revoked, or unknown admin key |
| `403 Forbidden` | Valid admin key but accessing another app's resources |

## Public Endpoints (No Authentication)

The following routes require no authentication:

- **Blob reads**: `GET /api/v1/buckets/{bucket_name}/blobs/{key}` and
  `GET /api/v1/blobs/by-blob-id/{blob_id}`
- **Infrastructure**: `/health`, `/ready`, `/metrics`, `/api/docs`

**Example:**

```bash
curl -s "$OYSTER_URL/api/v1/buckets/my-bucket/blobs/hello.txt"
```

## Security Notes

- **API keys and admin keys**: Only the BLAKE2s-256 hash is stored.
  A lost key cannot be recovered; issue a new one instead.
- **Admin-key compromise**: A leaked admin key gives full app-admin
  access (account creation, key issuance, S3 access keys) until revoked.
  Treat it like a long-lived service credential and rotate it periodically and
  on personnel changes through `oysterd app issue-admin-key` +
  `oysterd app revoke-admin-key`.
- **TLS**: Always terminate TLS in front of Oyster in production.

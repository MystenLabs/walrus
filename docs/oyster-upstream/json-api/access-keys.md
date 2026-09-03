# S3 Access Keys

S3 access keys let you authenticate with Oyster's
[S3-compatible API](../s3-api/README.md) using standard AWS Signature
Version 4. Each access key consists of an **access key ID** (20 characters,
prefixed with `OYAK`) and a **secret access key** (40 hex characters).

You can have up to **3 active access keys** per account.

## Managing Access Keys

Access keys are provisioned through the [Admin API](admin.md#s3-access-keys).
An app operator uses admin-key authentication to create, list, and revoke
keys for accounts they manage.

| Operation | Endpoint | Description |
|-----------|----------|-------------|
| [Create](admin.md#create-access-key) | `POST /api/v1/accounts/{account_id}/access-keys` | Create a new key pair |
| [List](admin.md#list-access-keys) | `GET /api/v1/accounts/{account_id}/access-keys` | List all keys for an account |
| [Revoke](admin.md#revoke-access-key) | `DELETE /api/v1/accounts/{account_id}/access-keys/{access_key_id}` | Revoke a key |

## Key Format

| Field | Format | Description |
|-------|--------|-------------|
| `access_key_id` | 20 characters, `OYAK` prefix | Identifies the key in S3 requests |
| `secret_access_key` | 40 hex characters | Signs S3 requests, **shown only once** at creation |

Each account can have at most **3 active** access keys. Revoked keys do
not count toward this limit.

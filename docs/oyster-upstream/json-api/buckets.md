# Buckets

Buckets are named containers that hold your blobs. Bucket names are
**globally unique**: no two accounts can have a bucket with the same name.

## Bucket Naming Rules

Bucket names must follow these rules:

- **3–63 characters** long
- Only **lowercase letters**, **digits**, and **hyphens** (`-`)
- Must **start and end** with a letter or digit
- No **consecutive hyphens** (`--`)
- Cannot look like an **IP address** (for example, `192.168.1.1`)
- Cannot use **reserved names**: `health`, `ready`, `metrics`, `api`

**Valid examples:** `my-bucket`, `data-2025`, `images`

**Invalid examples:** `My-Bucket` (uppercase), `a` (too short),
`-bucket` (starts with hyphen), `my--bucket` (consecutive hyphens)

## Create Bucket

```
POST /api/v1/buckets
```

**Request body:**

```json
{
  "name": "my-bucket"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | yes | Globally unique bucket name |

**Example:**

```bash
curl -s -X POST \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"name": "my-bucket"}' \
  "$OYSTER_URL/api/v1/buckets" | jq
```

**Response** (`201 Created`):

```json
{
  "name": "my-bucket",
  "account_id": "550e8400-e29b-41d4-a716-446655440000",
  "created_at": "2025-01-15T10:30:00Z"
}
```

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Bucket name |
| `account_id` | string | UUID of the owning account |
| `created_at` | string | ISO 8601 timestamp |

**Errors:**

| Status | Condition |
|--------|-----------|
| `400` | Invalid bucket name (see naming rules above) |
| `401` | Missing or invalid API key |
| `409` | Bucket name already exists |

## List Buckets

```
GET /api/v1/buckets
```

Returns a paginated list of buckets owned by your account.

**Query parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `cursor` | string | — | Opaque cursor from a previous `next_cursor` |
| `limit` | integer | 20 | Items per page (max: 100) |

**Example:**

```bash
curl -s -H "Authorization: Bearer $API_KEY" \
  "$OYSTER_URL/api/v1/buckets?limit=10" | jq
```

**Response** (`200 OK`):

```json
{
  "data": [
    {
      "name": "my-bucket",
      "account_id": "550e8400-e29b-41d4-a716-446655440000",
      "created_at": "2025-01-15T10:30:00Z"
    },
    {
      "name": "logs-2025",
      "account_id": "550e8400-e29b-41d4-a716-446655440000",
      "created_at": "2025-01-16T08:00:00Z"
    }
  ],
  "next_cursor": null
}
```

When `next_cursor` is not `null`, pass it as the `cursor` query parameter
to fetch the next page.

## Delete Bucket

```
DELETE /api/v1/buckets/{bucket_name}
```

Deletes a bucket. The bucket must be empty; delete all blobs first.

**Path parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `bucket_name` | string | Name of the bucket to delete |

**Example:**

```bash
curl -s -X DELETE \
  -H "Authorization: Bearer $API_KEY" \
  "$OYSTER_URL/api/v1/buckets/my-bucket"
```

**Response:** `204 No Content`

**Errors:**

| Status | Condition |
|--------|-----------|
| `401` | Missing or invalid API key |
| `404` | Bucket not found or not owned by your account |
| `409` | Bucket is not empty |

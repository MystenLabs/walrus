# JSON API Reference

The Oyster JSON API is served under `/api/v1/`. All requests and responses
use JSON (except blob content, which is raw binary). Authenticated endpoints
require a Bearer token in the `Authorization` header.

## Base URL

All API endpoints are prefixed with `/api/v1`:

```
$OYSTER_URL/api/v1/
```

Throughout this reference, `$OYSTER_URL` is set to your Oyster
server address (for example, `http://localhost:3000`).

## Authentication

Most endpoints require a Bearer token:

```
Authorization: Bearer <your-api-key>
```

Endpoints that **do not** require authentication:
- Reading blobs by key or blob ID
- Health, readiness, and metrics probes
- OpenAPI documentation

## Error Responses

All errors return a JSON body with a single `error` field:

```json
{
  "error": "human-readable error message"
}
```

### Status Codes

| Code | Meaning |
|------|---------|
| `200` | Success (GET, PATCH) |
| `201` | Created (POST, PUT) |
| `204` | No Content (DELETE) |
| `304` | Not Modified: `If-None-Match` matched on a GET request |
| `400` | Bad Request: invalid input or validation failure |
| `401` | Unauthorized: missing or invalid API key |
| `404` | Not Found: resource doesn't exist or not owned by your account |
| `409` | Conflict: resource already exists or limit reached |
| `412` | Precondition Failed: `If-Match` or `If-None-Match` condition not met |
| `413` | Payload Too Large: blob exceeds 1 GB |
| `500` | Internal Server Error |
| `501` | Not Implemented: endpoint exists but isn't functional yet |
| `503` | Service Unavailable: a dependent service is unreachable |

`501` is currently produced only by three account stubs that are wired up but
not yet implemented: `PUT /account/billing`, `GET /account/report`, and
`POST /account/transfer`. These are intentionally omitted from the per-endpoint
reference until they are functional.

### Cross-Cutting Error Contracts

A few error bodies are shared across multiple routes and carry a
structured block alongside the standard `error` string. Document
once here; per-route docs link back.

- **`InsufficientBalance` (402)**: the Pearl-derived wallet
  doesn't hold enough WAL or SUI to fund the onchain action.
  Body carries a `funding_required: { wal_frost, sui_mist }`
  block (both decimal strings). Currently fires on
  `PUT /buckets/{bucket}/blobs/{key}` (see
  [Store Blob](blobs.md#store-blob)) and
  `DELETE /buckets/{bucket}/blobs/{key}` (see
  [Delete Blob](blobs.md#delete-blob)). When the lookup itself
  fails, `funding_required` is `null`.
- **`CapExceeded` (400)**: the upload would push the account
  past its per-account `max_unencoded_bytes` cap. Body carries a
  `cap_exceeded` block pointing at the admin endpoint that can
  raise the cap. Currently fires on
  `PUT /buckets/{bucket}/blobs/{key}` (see
  [Store Blob](blobs.md#store-blob)). The cap is an *upper* bound by
  default; a per-account
  [`avg_blob_size`](admin.md#lower-bound-semantics-avg_blob_size)
  turns it into a *lower* bound on storable capacity for blobs of
  that size.

The admin-side onchain shrink endpoint
([`PUT /accounts/{account_id}/max-storage`](admin.md#update-storage-cap))
has its own 400 variants (`would_orphan`, `shrink_aborted`)
documented in the admin reference.

## Pagination

List endpoints use **cursor-based pagination**:

**Query parameters:**
- `cursor` (optional): opaque string from a previous response's `next_cursor`
- `limit` (optional): number of items per page (default: 20, max: 100)

**Response format:**

```json
{
  "data": [ ... ],
  "next_cursor": "opaque-cursor-string"
}
```

When `next_cursor` is `null`, there are no more results. To fetch the next
page, pass the `next_cursor` value as the `cursor` query parameter.

**Example: paginating through buckets:**

```bash
# First page
curl -s -H "Authorization: Bearer $API_KEY" \
  "$OYSTER_URL/api/v1/buckets?limit=10" | jq

# Next page (using next_cursor from previous response)
curl -s -H "Authorization: Bearer $API_KEY" \
  "$OYSTER_URL/api/v1/buckets?limit=10&cursor=eyJjcmVhdGVk..." | jq
```

## Interactive Documentation

Oyster serves an interactive OpenAPI UI at:

```
$OYSTER_URL/api/docs
```

Explore and test all endpoints directly from your browser.

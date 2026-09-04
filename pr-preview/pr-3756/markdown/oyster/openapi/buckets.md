> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

# Buckets Endpoints

Bucket CRUD operations

Base URL: `https://oyster.testnet.mystenlabs.com/api/v1`

> **Info**
>
> This page is auto-generated from the [OpenAPI spec](/oyster/openapi.json).
> For an interactive explorer, see the [Interactive API Reference](../api-reference.mdx).
## GET `/buckets`

**List all buckets owned by the authenticated account, with cursor-based pagination.**

**Authentication:** Required

**Parameters:**

| Name | In | Type | Required | Description |
|------|-----|------|----------|-------------|
| `cursor` | query | string,null | No | Opaque cursor from a previous response. |
| `limit` | query | integer,null | No | Maximum number of items to return. |

**Responses:**

- **200**: List of buckets
- **401**: Unauthorized

---

## POST `/buckets`

**Create a new bucket. Bucket names must be globally unique.**

**Authentication:** Required

**Request body:** `application/json`

```json
{
  "name": "<string>"
}
```

**Responses:**

- **201**: Bucket created
- **400**: Bad request
- **401**: Unauthorized
- **409**: Bucket name already exists

---

## DELETE `/buckets/{bucket_name}`

**Delete an empty bucket. Returns 409 Conflict if the bucket still contains blobs.**

**Authentication:** Required

**Parameters:**

| Name | In | Type | Required | Description |
|------|-----|------|----------|-------------|
| `bucket_name` | path | string | Yes | Bucket name |

**Responses:**

- **204**: Bucket deleted
- **401**: Unauthorized
- **404**: Bucket not found
- **409**: Bucket is not empty

---
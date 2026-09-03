# Object Operations

All S3 operations require authentication (SigV4), including reads. See
[S3 Setup](setup.md) for configuration.

> **Authenticated reads:** Unlike the [JSON API](../json-api/blobs.md), where
> blob reads are public and unauthenticated, all S3 reads require authentication.

## PutObject

Uploads an object to a bucket. If an object with the same key already
exists, it is replaced.

```bash
aws --profile oyster s3api put-object \
  --bucket my-bucket \
  --key hello.txt \
  --body hello.txt
```

**Response:**

```json
{
    "ETag": "\"9a0364b9e99bb480dd25e1f0284c8555\""
}
```

The ETag is the MD5 digest of the uploaded content.

### Setting Content-Type

```bash
aws --profile oyster s3api put-object \
  --bucket my-bucket \
  --key image.png \
  --body photo.png \
  --content-type "image/png"
```

If `--content-type` is omitted, it defaults to `application/octet-stream`.

### Setting Tags on Upload

Attach tags at upload time with `--tagging`, a URL-encoded query string of
`key=value` pairs:

```bash
aws --profile oyster s3api put-object \
  --bucket my-bucket \
  --key hello.txt \
  --body hello.txt \
  --tagging "env=prod&team=platform"
```

Tags set this way share the same store as the [JSON API](../json-api/blobs.md#blob-tags)
and the [Object Tagging](#object-tagging) operations below, and are subject to
the same [tag rules](../json-api/blobs.md#tag-rules).

### Key Behavior

- **Overwrite:** Uploading to an existing key replaces the object
- **Expiration:** Objects share the owning account's `StoragePool`
  lifetime; the background extension service renews the pool before it
  expires (see [Blob Lifecycle](../guides/blob-lifecycle.md))
- **Content-addressed:** Identical content produces the same blob ID
  internally, enabling deduplication

### Conditional Headers

PutObject supports `If-Match` and `If-None-Match` headers for safe writes:

- **`If-None-Match: *`**: upload only if the key doesn't already exist
  (create-only semantics). Returns `412 PreconditionFailed` if the key
  exists.
- **`If-Match: "<etag>"`**: overwrite only if the current object's ETag
  matches. Returns `412 PreconditionFailed` on mismatch.

```bash
# Create-only: fail if the key already exists
aws --profile oyster s3api put-object \
  --bucket my-bucket \
  --key hello.txt \
  --body hello.txt \
  --if-none-match "*"
```

**Errors:**

| S3 Error Code | Condition |
|---------------|-----------|
| `NoSuchBucket` | Bucket doesn't exist |
| `PreconditionFailed` | `If-Match` / `If-None-Match` condition not met |

## GetObject

Downloads an object's contents.

```bash
aws --profile oyster s3api get-object \
  --bucket my-bucket \
  --key hello.txt \
  downloaded.txt
```

**Response metadata:**

```json
{
    "ContentLength": 14,
    "ContentType": "text/plain",
    "ETag": "\"9a0364b9e99bb480dd25e1f0284c8555\"",
    "LastModified": "2025-01-15T10:31:00Z"
}
```

The file contents are written to the output path (`downloaded.txt` in this
example).

### Conditional Headers

GetObject supports `If-Match` and `If-None-Match` for cache validation:

- **`If-Match: "<etag>"`**: return the object only if its ETag matches.
  Returns `412 PreconditionFailed` on mismatch.
- **`If-None-Match: "<etag>"`**: return the object only if its ETag
  differs. Returns `304 NotModified` if the ETag matches (useful for
  cache validation).

```bash
# Only download if the object has changed
aws --profile oyster s3api get-object \
  --bucket my-bucket \
  --key hello.txt \
  --if-none-match '"9a0364b9e99bb480dd25e1f0284c8555"' \
  downloaded.txt
```

**Errors:**

| S3 Error Code | Condition |
|---------------|-----------|
| `NoSuchBucket` | Bucket doesn't exist |
| `NoSuchKey` | Object key doesn't exist |
| `PreconditionFailed` | `If-Match` condition not met |
| `NotModified` | `If-None-Match` matched; object unchanged (304) |

## HeadObject

Retrieves object metadata without downloading the contents. Useful for
checking if an object exists or reading its size and content type.

```bash
aws --profile oyster s3api head-object \
  --bucket my-bucket \
  --key hello.txt
```

**Response:**

```json
{
    "ContentLength": 14,
    "ContentType": "text/plain",
    "ETag": "\"9a0364b9e99bb480dd25e1f0284c8555\"",
    "LastModified": "2025-01-15T10:31:00Z"
}
```

HeadObject supports the same `If-Match` and `If-None-Match` conditional
headers as [GetObject](#getobject). Returns `412 PreconditionFailed` or
`304 NotModified` as appropriate.

**Errors:**

| S3 Error Code | Condition |
|---------------|-----------|
| `NoSuchBucket` | Bucket doesn't exist |
| `NoSuchKey` | Object key doesn't exist |
| `PreconditionFailed` | `If-Match` condition not met |
| `NotModified` | `If-None-Match` matched; object unchanged (304) |

## DeleteObject

Deletes an object from a bucket.

```bash
aws --profile oyster s3api delete-object \
  --bucket my-bucket \
  --key hello.txt
```

Returns no output on success.

This operation is **idempotent**: deleting a key that doesn't exist still
returns success, matching standard S3 behavior.

Deletion is **reference-counted**: the underlying blob data is only removed
from storage when no other keys reference the same content.

### Conditional Headers

DeleteObject supports `If-Match` for safe deletion:

- **`If-Match: "<etag>"`**: delete only if the object's ETag matches.
  Returns `412 PreconditionFailed` on mismatch.

**Errors:**

| S3 Error Code | Condition |
|---------------|-----------|
| `NoSuchBucket` | Bucket doesn't exist |
| `PreconditionFailed` | `If-Match` condition not met |

## ListObjectsV2

Lists objects in a bucket with optional filtering and pagination.

### Basic Listing

```bash
aws --profile oyster s3api list-objects-v2 --bucket my-bucket
```

**Response:**

```json
{
    "Name": "my-bucket",
    "Contents": [
        {
            "Key": "hello.txt",
            "Size": 14,
            "ETag": "\"9a0364b9e99bb480...\"",
            "LastModified": "2025-01-15T10:31:00Z",
            "StorageClass": "STANDARD"
        },
        {
            "Key": "images/photo.png",
            "Size": 204800,
            "ETag": "\"d41d8cd98f00b204...\"",
            "LastModified": "2025-01-15T11:00:00Z",
            "StorageClass": "STANDARD"
        }
    ],
    "KeyCount": 2,
    "MaxKeys": 1000,
    "IsTruncated": false
}
```

### Filtering by Prefix

List only objects under a specific "folder":

```bash
aws --profile oyster s3api list-objects-v2 \
  --bucket my-bucket \
  --prefix "images/"
```

### Simulating Folders with Delimiter

Use `--delimiter "/"` to group objects into virtual folders:

```bash
aws --profile oyster s3api list-objects-v2 \
  --bucket my-bucket \
  --delimiter "/"
```

**Response:**

```json
{
    "Name": "my-bucket",
    "Contents": [
        {
            "Key": "hello.txt",
            "Size": 14,
            "ETag": "\"9a0364b9e99bb480...\"",
            "LastModified": "2025-01-15T10:31:00Z",
            "StorageClass": "STANDARD"
        }
    ],
    "CommonPrefixes": [
        {
            "Prefix": "images/"
        }
    ],
    "KeyCount": 2,
    "MaxKeys": 1000,
    "Delimiter": "/",
    "IsTruncated": false
}
```

Objects directly at the root level appear in `Contents`, while "folders"
(key prefixes before the delimiter) appear in `CommonPrefixes`.

### Combining Prefix and Delimiter

List the contents of a specific "folder":

```bash
aws --profile oyster s3api list-objects-v2 \
  --bucket my-bucket \
  --prefix "images/" \
  --delimiter "/"
```

### Pagination

Limit results and paginate through large listings:

```bash
# First page
aws --profile oyster s3api list-objects-v2 \
  --bucket my-bucket \
  --max-keys 10

# Next page (using NextContinuationToken from previous response)
aws --profile oyster s3api list-objects-v2 \
  --bucket my-bucket \
  --max-keys 10 \
  --starting-token "last-key-from-previous-page"
```

### Supported Parameters

| Parameter | AWS CLI Flag | Description |
|-----------|--------------|-------------|
| Prefix | `--prefix` | Filter keys that start with this string |
| Delimiter | `--delimiter` | Group keys by this separator (for example, `/`) |
| MaxKeys | `--max-keys` | Max objects to return (default: 1000) |
| StartAfter | `--start-after` | Return keys after this value (lexicographic) |
| ContinuationToken | `--starting-token` | Continue from a previous response |

**Errors:**

| S3 Error Code | Condition |
|---------------|-----------|
| `NoSuchBucket` | Bucket doesn't exist |

## Object Tagging

Oyster implements the three S3 object-tagging operations. Tags are stored in
Oyster's database in the same `blob_tags` table used by the
[JSON API tag endpoints](../json-api/blobs.md#blob-tags); a tag set through S3
is visible through the JSON API and vice versa. The same
[tag rules](../json-api/blobs.md#tag-rules) apply (max 10 tags; key ≤ 128 B;
value ≤ 256 B; set ≤ 2048 B; restricted charset).

### PutObjectTagging

Replaces the object's entire tag set.

```bash
aws --profile oyster s3api put-object-tagging \
  --bucket my-bucket \
  --key hello.txt \
  --tagging 'TagSet=[{Key=env,Value=prod},{Key=team,Value=platform}]'
```

### GetObjectTagging

Returns the object's current tags.

```bash
aws --profile oyster s3api get-object-tagging \
  --bucket my-bucket \
  --key hello.txt
```

**Response:**

```json
{
    "TagSet": [
        { "Key": "env", "Value": "prod" },
        { "Key": "team", "Value": "platform" }
    ]
}
```

### DeleteObjectTagging

Removes all tags from the object.

```bash
aws --profile oyster s3api delete-object-tagging \
  --bucket my-bucket \
  --key hello.txt
```

**Errors:**

| S3 Error Code | Condition |
|---------------|-----------|
| `NoSuchBucket` | Bucket doesn't exist |
| `NoSuchKey` | Object key doesn't exist |

# Bucket Operations

All bucket operations require S3 authentication (SigV4). See
[S3 Setup](setup.md) for configuration.

## CreateBucket

Creates a new bucket.

```bash
aws --profile oyster s3api create-bucket --bucket my-bucket
```

**Response:**

```json
{
    "Location": "/my-bucket"
}
```

Bucket names follow the same [naming rules](../json-api/buckets.md#bucket-naming-rules)
as the JSON API: 3–63 characters, lowercase letters/digits/hyphens only,
no consecutive hyphens, no IP address format, and no reserved names
(`health`, `ready`, `metrics`, `api`).

**Errors:**

| S3 Error Code | Condition |
|---------------|-----------|
| `BucketAlreadyOwnedByYou` | A bucket with this name already exists |
| `InvalidBucketName` | Name violates naming rules |

## HeadBucket

Checks if a bucket exists and you have access to it. Returns no body —
only HTTP status.

```bash
aws --profile oyster s3api head-bucket --bucket my-bucket
```

Returns exit code 0 on success (HTTP 200). If the bucket doesn't exist or
isn't owned by your account, the AWS CLI prints an error (HTTP 404).

**Errors:**

| S3 Error Code | Condition |
|---------------|-----------|
| `NoSuchBucket` | Bucket doesn't exist or not owned by your account |

## ListBuckets

Lists all buckets in your account.

```bash
aws --profile oyster s3api list-buckets
```

**Response:**

```json
{
    "Buckets": [
        {
            "Name": "my-bucket",
            "CreationDate": "2025-01-15T10:30:00Z"
        },
        {
            "Name": "logs-2025",
            "CreationDate": "2025-01-16T08:00:00Z"
        }
    ]
}
```

Returns up to 1000 buckets. No pagination is supported for this operation.

## DeleteBucket

Deletes a bucket. The bucket must be empty first.

```bash
aws --profile oyster s3api delete-bucket --bucket my-bucket
```

Returns no output on success (HTTP 204).

**Errors:**

| S3 Error Code | Condition |
|---------------|-----------|
| `NoSuchBucket` | Bucket doesn't exist or not owned by your account |
| `BucketNotEmpty` | Bucket still contains objects |

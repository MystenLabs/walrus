# Getting Started

Follow these steps to complete your first interactions with Oyster. By the end,
you have created a bucket, uploaded a blob, and downloaded it back.

## Prerequisites

- **curl**: for making HTTP requests to the JSON API
- **AWS CLI** (optional): for using the S3-compatible API
  ([install guide](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html))

## Obtaining Credentials

Oyster uses a two-tier auth model: **operators** manage accounts with
long-lived per-app **admin keys**, and **end users** authenticate data
operations with API keys. Both tiers use `Authorization: Bearer <hex>`;
the route prefix selects which credential table is consulted.

### For Operators

The server operator creates an app, gets back a first admin key, and
provisions accounts. See the [Admin API docs](json-api/admin.md) for full
details.

```bash
# 1. Create an app (server operator runs this once). `app new` auto-issues
#    a first admin key by default; pass --no-key to opt out.
oysterd app new --name my-app --contact_email admin@example.com
# Prints: 550e8400-e29b-41d4-a716-446655440000   <- app id
# Prints: <64-char hex admin key>                 <- save this

export ADMIN_KEY="<64-char hex admin key from above>"

# 2. Create an account using the admin key
export OYSTER_URL="http://localhost:3000"
curl -s -X POST \
  -H "Authorization: Bearer $ADMIN_KEY" \
  -H "Content-Type: application/json" \
  -d '{"name": "my-app-user"}' \
  "$OYSTER_URL/api/v1/accounts" | jq
```

The response includes the account ID and an initial API key:

```json
{
  "account_id": "550e8400-e29b-41d4-a716-446655440000",
  "api_key": {
    "id": "b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e",
    "prefix": "a1b2c3d4",
    "bearer_token": "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2",
    "created_at": "2025-01-15T10:30:00Z"
  }
}
```

Save the `account_id` and `bearer_token`. The token is only shown once.

```bash
export ACCOUNT_ID="550e8400-e29b-41d4-a716-446655440000"
export API_KEY="a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2"
```

### For End Users

Your operator provides you with an API key. Store it in an
environment variable for the rest of this guide:

```bash
export OYSTER_URL="http://localhost:3000"
export API_KEY="your-api-key-here"
```

## Create Your First Bucket

Buckets are named containers for your blobs. Create one called `my-bucket`:

```bash
curl -s -X POST \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"name": "my-bucket"}' \
  "$OYSTER_URL/api/v1/buckets" | jq
```

Response:

```json
{
  "name": "my-bucket",
  "account_id": "550e8400-e29b-41d4-a716-446655440000",
  "created_at": "2025-01-15T10:30:00Z"
}
```

## Upload a Blob

Upload a text file to your bucket with the key `hello.txt`:

```bash
curl -s -X PUT \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: text/plain" \
  --data-binary "Hello, Oyster!" \
  "$OYSTER_URL/api/v1/buckets/my-bucket/blobs/hello.txt" | jq
```

Response:

```json
{
  "key": "hello.txt",
  "blob_id": "2cf24dba5fb0a30e...",
  "size": 14,
  "md5": "9a0364b9e99bb480...",
  "sui_object_id": null,
  "created_at": "2025-01-15T10:31:00Z"
}
```

You can also upload a file from disk:

```bash
curl -s -X PUT \
  -H "Authorization: Bearer $API_KEY" \
  --data-binary @photo.png \
  "$OYSTER_URL/api/v1/buckets/my-bucket/blobs/images/photo.png" | jq
```

## Download a Blob

Blob reads are **public** (no authentication needed):

```bash
curl -s "$OYSTER_URL/api/v1/buckets/my-bucket/blobs/hello.txt"
```

Output:

```
Hello, Oyster!
```

## List Blobs in a Bucket

```bash
curl -s -H "Authorization: Bearer $API_KEY" \
  "$OYSTER_URL/api/v1/buckets/my-bucket/blobs" | jq
```

Response:

```json
{
  "data": [
    {
      "key": "hello.txt",
      "blob_id": "2cf24dba5fb0a30e...",
      "bucket_name": "my-bucket",
      "content_type": "text/plain",
      "size": 14,
      "md5": "9a0364b9e99bb480...",
      "created_at": "2025-01-15T10:31:00Z"
    }
  ],
  "next_cursor": null
}
```

## Delete a Blob

```bash
curl -s -X DELETE \
  -H "Authorization: Bearer $API_KEY" \
  "$OYSTER_URL/api/v1/buckets/my-bucket/blobs/hello.txt"
```

Returns HTTP 204 (no content) on success.

## Create Additional API Keys (Operator)

Additional API keys are created by the operator through the Admin API using
admin-key authentication. This requires the `$ADMIN_KEY` and `$ACCOUNT_ID`
variables from the [Obtaining Credentials](#obtaining-credentials) section.

```bash
curl -s -X POST \
  -H "Authorization: Bearer $ADMIN_KEY" \
  "$OYSTER_URL/api/v1/accounts/$ACCOUNT_ID/api-keys" | jq
```

Response:

```json
{
  "id": "b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e",
  "prefix": "a1b2c3d4",
  "bearer_token": "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2",
  "created_at": "2025-01-15T10:32:00Z"
}
```

> **Important:** The `bearer_token` field is only shown once. Save it immediately.

See the [Admin API docs](json-api/admin.md#create-api-key) for full
details including error handling and key revocation.

## Set Up S3 Access Keys (Operator)

To use the AWS CLI or any S3-compatible SDK, the operator creates S3 access
keys through the [Admin API](json-api/admin.md#create-access-key). This
requires the `$ADMIN_KEY` and `$ACCOUNT_ID` variables from the
[Obtaining Credentials](#obtaining-credentials) section.

```bash
curl -s -X POST \
  -H "Authorization: Bearer $ADMIN_KEY" \
  "$OYSTER_URL/api/v1/accounts/$ACCOUNT_ID/access-keys" | jq
```

Response:

```json
{
  "access_key_id": "OYAK1234567890ABCDEF",
  "secret_access_key": "abcdef1234567890abcdef1234567890abcdef12",
  "created_at": "2025-01-15T10:33:00Z"
}
```

> **Important:** The `secret_access_key` is only shown once. Save it
> immediately. You can have up to 3 active S3 access keys per account.

Then configure the AWS CLI:

```bash
aws configure set aws_access_key_id "OYAK1234567890ABCDEF" --profile oyster
aws configure set aws_secret_access_key "abcdef1234567890..." --profile oyster
aws configure set region "us-east-1" --profile oyster
aws configure set endpoint_url "$OYSTER_URL" --profile oyster
```

Now you can use standard S3 commands:

```bash
# Create a bucket
aws --profile oyster s3api create-bucket --bucket my-s3-bucket

# Upload a file
aws --profile oyster s3api put-object \
  --bucket my-s3-bucket --key hello.txt --body hello.txt

# Download a file
aws --profile oyster s3api get-object \
  --bucket my-s3-bucket --key hello.txt downloaded.txt
```

For the full S3 API reference, see **[S3 API Reference](s3-api/README.md)**.

## What's Next

- **[JSON API Reference](json-api/README.md)**: detailed documentation of
  every endpoint.
- **[S3 API Reference](s3-api/README.md)**: complete S3-compatible
  operations and setup.
- **[Guides](guides/README.md)**: CLI quick start, SDK examples, and
  advanced topics.

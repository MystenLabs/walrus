# S3 Setup

Configure the AWS CLI and SDKs to work with Oyster's S3-compatible API using the steps below.

## Prerequisites

- An Oyster API key (Bearer token); see [Getting Started](../getting-started.md)
- [AWS CLI v2](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)
  (for CLI usage)

## Step 1: Create S3 Access Keys

Access keys are created through the [Admin API](../json-api/admin.md#create-access-key)
using admin-key authentication:

```bash
curl -s -X POST \
  -H "Authorization: Bearer $ADMIN_KEY" \
  "$OYSTER_URL/api/v1/accounts/$ACCOUNT_ID/access-keys" | jq
```

Save the `access_key_id` and `secret_access_key` from the response, because the
secret is only shown once.

See [S3 Access Keys](../json-api/access-keys.md) for more on key format
and limits.

## Step 2: Configure the AWS CLI

Set up a named profile pointing at your Oyster instance:

```bash
aws configure set aws_access_key_id "OYAK1234567890ABCDEF" --profile oyster
aws configure set aws_secret_access_key "abcdef1234567890abcdef1234567890abcdef12" --profile oyster
aws configure set region "us-east-1" --profile oyster
aws configure set endpoint_url "$OYSTER_URL" --profile oyster
```

> **Region:** Oyster ignores the region value, but AWS SigV4
> requires one. Use any valid region string, for example `us-east-1`.

## Step 3: Verify Connectivity

```bash
aws --profile oyster s3api list-buckets
```

You should see a JSON response with your buckets (or an empty list if you
haven't created any yet):

```json
{
    "Buckets": []
}
```

## Quick Test

Try a full round-trip:

```bash
# Create a bucket
aws --profile oyster s3api create-bucket --bucket test-bucket

# Upload a file
echo "Hello from S3!" > /tmp/hello.txt
aws --profile oyster s3api put-object \
  --bucket test-bucket --key hello.txt --body /tmp/hello.txt

# Download it back
aws --profile oyster s3api get-object \
  --bucket test-bucket --key hello.txt /tmp/downloaded.txt
cat /tmp/downloaded.txt
```

## SDK Configuration

When using AWS SDKs programmatically, you need to set **path-style
addressing** and a custom endpoint. Here are examples for common SDKs:

### Python (boto3)

```python
import boto3

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:3000",
    aws_access_key_id="OYAK1234567890ABCDEF",
    aws_secret_access_key="abcdef1234567890...",
    region_name="us-east-1",
)

# Path-style is the default for custom endpoints in boto3
s3.list_buckets()
```

### JavaScript / TypeScript (AWS SDK v3)

```javascript
import { S3Client, ListBucketsCommand } from "@aws-sdk/client-s3";

const client = new S3Client({
  endpoint: "http://localhost:3000",
  region: "us-east-1",
  credentials: {
    accessKeyId: "OYAK1234567890ABCDEF",
    secretAccessKey: "abcdef1234567890...",
  },
  forcePathStyle: true,
});

const response = await client.send(new ListBucketsCommand({}));
```

### Rust (aws-sdk-s3)

```rust
use aws_sdk_s3::config::{Credentials, Region};

let creds = Credentials::new(
    "OYAK1234567890ABCDEF",
    "abcdef1234567890...",
    None, None, "oyster",
);

let config = aws_sdk_s3::Config::builder()
    .behavior_version_latest()
    .region(Region::new("us-east-1"))
    .endpoint_url("http://localhost:3000")
    .credentials_provider(creds)
    .force_path_style(true)
    .build();

let client = aws_sdk_s3::Client::from_conf(config);
```

> **Important:** Always set `force_path_style: true` (or equivalent). Oyster
> uses path-style URLs (`endpoint/bucket/key`), not virtual-hosted-style
> (`bucket.endpoint/key`).

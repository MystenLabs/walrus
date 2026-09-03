# AWS SDK Examples

These examples show complete workflows using AWS SDKs with Oyster's
S3-compatible API. For initial SDK setup, see [S3 setup](../s3-api/setup.md).

## Python (boto3)

### Setup

```python
import boto3

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:3000",
    aws_access_key_id="OYAK1234567890ABCDEF",
    aws_secret_access_key="abcdef1234567890abcdef1234567890abcdef12",
    region_name="us-east-1",
)
```

### Create a bucket

```python
s3.create_bucket(Bucket="my-bucket")
```

### Upload a file

```python
# From a file on disk
s3.upload_file("photo.png", "my-bucket", "images/photo.png")

# From a string
s3.put_object(
    Bucket="my-bucket",
    Key="hello.txt",
    Body=b"Hello, Oyster!",
    ContentType="text/plain",
)
```

### Download a file

```python
# To a file on disk
s3.download_file("my-bucket", "hello.txt", "downloaded.txt")

# To memory
response = s3.get_object(Bucket="my-bucket", Key="hello.txt")
content = response["Body"].read()
print(content.decode())  # "Hello, Oyster!"
```

### List objects

```python
# List all objects
response = s3.list_objects_v2(Bucket="my-bucket")
for obj in response.get("Contents", []):
    print(f"{obj['Key']}  {obj['Size']} bytes")

# List with prefix (simulate folder listing)
response = s3.list_objects_v2(
    Bucket="my-bucket",
    Prefix="images/",
    Delimiter="/",
)

# Files directly in "images/"
for obj in response.get("Contents", []):
    print(f"  File: {obj['Key']}")

# "Subfolders" in "images/"
for prefix in response.get("CommonPrefixes", []):
    print(f"  Folder: {prefix['Prefix']}")
```

### Paginate through large listings

```python
paginator = s3.get_paginator("list_objects_v2")
for page in paginator.paginate(Bucket="my-bucket", MaxKeys=100):
    for obj in page.get("Contents", []):
        print(obj["Key"])
```

### Delete an object

```python
s3.delete_object(Bucket="my-bucket", Key="hello.txt")
```

### Delete a bucket

```python
s3.delete_bucket(Bucket="my-bucket")
```

### Check if an object exists

```python
try:
    s3.head_object(Bucket="my-bucket", Key="hello.txt")
    print("Object exists")
except s3.exceptions.ClientError as e:
    if e.response["Error"]["Code"] == "404":
        print("Object not found")
    else:
        raise
```

### Conditional requests

Use `If-Match` and `If-None-Match` headers for safe writes and cache
validation:

```python
# Create-only: fail if the key already exists
try:
    s3.put_object(
        Bucket="my-bucket",
        Key="config.json",
        Body=b'{"version": 1}',
        IfNoneMatch="*",
    )
except s3.exceptions.ClientError as e:
    if e.response["Error"]["Code"] == "PreconditionFailed":
        print("Key already exists — not overwritten")
    else:
        raise

# Safe overwrite: only update if the ETag matches
response = s3.head_object(Bucket="my-bucket", Key="config.json")
current_etag = response["ETag"]

s3.put_object(
    Bucket="my-bucket",
    Key="config.json",
    Body=b'{"version": 2}',
    IfMatch=current_etag,
)

# Cache validation: skip download if unchanged
try:
    s3.get_object(
        Bucket="my-bucket",
        Key="config.json",
        IfNoneMatch=current_etag,
    )
except s3.exceptions.ClientError as e:
    if e.response["Error"]["Code"] == "304":
        print("Not modified — use cached copy")
    else:
        raise
```

### Full workflow

```python
import boto3

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:3000",
    aws_access_key_id="OYAK1234567890ABCDEF",
    aws_secret_access_key="abcdef1234567890...",
    region_name="us-east-1",
)

# Create bucket
s3.create_bucket(Bucket="demo")

# Upload
s3.put_object(Bucket="demo", Key="doc.txt", Body=b"Hello!")
s3.put_object(Bucket="demo", Key="images/a.png", Body=b"\x89PNG...")
s3.put_object(Bucket="demo", Key="images/b.png", Body=b"\x89PNG...")

# List with folder simulation
resp = s3.list_objects_v2(Bucket="demo", Delimiter="/")
print("Root files:", [o["Key"] for o in resp.get("Contents", [])])
print("Folders:", [p["Prefix"] for p in resp.get("CommonPrefixes", [])])
# Root files: ['doc.txt']
# Folders: ['images/']

# Download
obj = s3.get_object(Bucket="demo", Key="doc.txt")
print(obj["Body"].read().decode())  # "Hello!"

# Clean up
s3.delete_object(Bucket="demo", Key="doc.txt")
s3.delete_object(Bucket="demo", Key="images/a.png")
s3.delete_object(Bucket="demo", Key="images/b.png")
s3.delete_bucket(Bucket="demo")
```

## JavaScript / TypeScript (AWS SDK v3)

### Setup

```javascript
import {
  S3Client,
  CreateBucketCommand,
  PutObjectCommand,
  GetObjectCommand,
  HeadObjectCommand,
  ListObjectsV2Command,
  DeleteObjectCommand,
  DeleteBucketCommand,
} from "@aws-sdk/client-s3";

const client = new S3Client({
  endpoint: "http://localhost:3000",
  region: "us-east-1",
  credentials: {
    accessKeyId: "OYAK1234567890ABCDEF",
    secretAccessKey: "abcdef1234567890abcdef1234567890abcdef12",
  },
  forcePathStyle: true,
});
```

### Create a bucket

```javascript
await client.send(new CreateBucketCommand({ Bucket: "my-bucket" }));
```

### Upload an object

```javascript
await client.send(
  new PutObjectCommand({
    Bucket: "my-bucket",
    Key: "hello.txt",
    Body: "Hello, Oyster!",
    ContentType: "text/plain",
  })
);
```

### Upload a file from disk (Node.js)

```javascript
import { createReadStream } from "fs";

await client.send(
  new PutObjectCommand({
    Bucket: "my-bucket",
    Key: "images/photo.png",
    Body: createReadStream("photo.png"),
    ContentType: "image/png",
  })
);
```

### Download an object

```javascript
const response = await client.send(
  new GetObjectCommand({
    Bucket: "my-bucket",
    Key: "hello.txt",
  })
);

const body = await response.Body.transformToString();
console.log(body); // "Hello, Oyster!"
```

### List objects

```javascript
const response = await client.send(
  new ListObjectsV2Command({
    Bucket: "my-bucket",
    Prefix: "images/",
    Delimiter: "/",
  })
);

for (const obj of response.Contents ?? []) {
  console.log(`File: ${obj.Key} (${obj.Size} bytes)`);
}

for (const prefix of response.CommonPrefixes ?? []) {
  console.log(`Folder: ${prefix.Prefix}`);
}
```

### Delete an object

```javascript
await client.send(
  new DeleteObjectCommand({
    Bucket: "my-bucket",
    Key: "hello.txt",
  })
);
```

### Check if an object exists

```javascript
try {
  await client.send(
    new HeadObjectCommand({
      Bucket: "my-bucket",
      Key: "hello.txt",
    })
  );
  console.log("Object exists");
} catch (err) {
  if (err.name === "NotFound") {
    console.log("Object not found");
  } else {
    throw err;
  }
}
```

### Conditional requests

Use `IfMatch` and `IfNoneMatch` parameters for safe writes and cache
validation:

```javascript
// Create-only: fail if the key already exists
try {
  await client.send(
    new PutObjectCommand({
      Bucket: "my-bucket",
      Key: "config.json",
      Body: JSON.stringify({ version: 1 }),
      IfNoneMatch: "*",
    })
  );
} catch (err) {
  if (err.name === "PreconditionFailed") {
    console.log("Key already exists — not overwritten");
  } else {
    throw err;
  }
}

// Safe overwrite: only update if the ETag matches
const head = await client.send(
  new HeadObjectCommand({ Bucket: "my-bucket", Key: "config.json" })
);

await client.send(
  new PutObjectCommand({
    Bucket: "my-bucket",
    Key: "config.json",
    Body: JSON.stringify({ version: 2 }),
    IfMatch: head.ETag,
  })
);
```

### Full workflow

```javascript
import { S3Client, CreateBucketCommand, PutObjectCommand,
  GetObjectCommand, ListObjectsV2Command, DeleteObjectCommand,
  DeleteBucketCommand } from "@aws-sdk/client-s3";

const client = new S3Client({
  endpoint: "http://localhost:3000",
  region: "us-east-1",
  credentials: {
    accessKeyId: "OYAK1234567890ABCDEF",
    secretAccessKey: "abcdef1234567890...",
  },
  forcePathStyle: true,
});

// Create bucket
await client.send(new CreateBucketCommand({ Bucket: "demo" }));

// Upload objects
await client.send(new PutObjectCommand({
  Bucket: "demo", Key: "doc.txt", Body: "Hello!",
}));
await client.send(new PutObjectCommand({
  Bucket: "demo", Key: "images/a.png", Body: Buffer.from([0x89, 0x50]),
}));

// List with folder simulation
const list = await client.send(new ListObjectsV2Command({
  Bucket: "demo", Delimiter: "/",
}));
console.log("Files:", list.Contents?.map(o => o.Key));
console.log("Folders:", list.CommonPrefixes?.map(p => p.Prefix));

// Download
const obj = await client.send(new GetObjectCommand({
  Bucket: "demo", Key: "doc.txt",
}));
console.log(await obj.Body.transformToString()); // "Hello!"

// Clean up
await client.send(new DeleteObjectCommand({ Bucket: "demo", Key: "doc.txt" }));
await client.send(new DeleteObjectCommand({ Bucket: "demo", Key: "images/a.png" }));
await client.send(new DeleteBucketCommand({ Bucket: "demo" }));
```

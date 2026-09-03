# Introduction

Oyster is a Web2-friendly object storage service backed by
[Walrus](https://walrus.xyz/) (decentralized blob storage) and
[Sui](https://sui.io/) (onchain state). It gives you familiar HTTP and S3
APIs while your data is stored on a decentralized network.

## Core Concepts

### Accounts

Every user has an **account**. Your Oyster administrator creates accounts and
issues you an initial API key (Bearer token). With that token you can create
additional API keys, manage buckets and blobs, and generate S3-compatible
access keys.

### API Keys

An API key is a Bearer token used to authenticate JSON API requests. You
include it in the `Authorization` header:

```
Authorization: Bearer <your-api-key>
```

The plaintext key is shown exactly once, at creation time. Store it securely.

### Buckets

Buckets are named containers for your blobs. Bucket names are **globally
unique** and follow S3-style naming rules:

- 3–63 characters long
- Lowercase letters, digits, and hyphens only
- Must start and end with a letter or digit
- No consecutive hyphens
- Cannot look like an IP address (for example, `192.168.1.1`)

### Blobs

A blob is a binary object stored inside a bucket, identified by a
user-chosen **key** (like a file path, for example `images/photo.png`). Blobs are
**content-addressed**: identical content is stored once and can be referenced
by multiple keys.

Key properties of blobs:

- **Public reads**: anyone can download a blob by bucket name and key, or
  by its content-addressed blob ID. No authentication is needed for reads.
- **Authenticated writes**: uploading, deleting, and listing blobs requires
  a valid API key or S3 credentials.
- **Overwrite semantics**: uploading to an existing key replaces the blob.
- **Reference-counted deletion**: deleting a key removes the reference;
  the underlying data is only removed when no other keys point to it.
- **Expiration**: blobs share their account's `StoragePool` lifetime
  rather than expiring individually. A background extension service
  renews each pool before it expires; see
  [Blob Lifecycle](guides/blob-lifecycle.md) for details.

## Two API Surfaces

Oyster exposes two ways to interact with your data:

### JSON API

A RESTful HTTP API under `/api/v1/`. Use it with `curl`, any HTTP client, or
the `oyster-cli` command-line tool. Responses are JSON. This API covers
everything: account management, bucket/blob CRUD, S3 access key management,
wallet info, and more.

### S3-Compatible API

An [AWS S3-compatible](https://docs.aws.amazon.com/s3/) interface that speaks
the same protocol as Amazon S3. Use the AWS CLI, `boto3`, the AWS SDK for
JavaScript, or any S3-compatible client. Authenticate with S3 access keys
(created through the JSON API) using standard AWS Signature Version 4.

Both APIs share the same underlying storage and database. Changes made
through one are immediately visible in the other.

## What's Next

- **[Getting Started](getting-started.md)**: set up credentials and make
  your first API calls.
- **[JSON API Reference](json-api/README.md)**: full endpoint documentation.
- **[S3 API Reference](s3-api/README.md)**: S3-compatible operations and
  AWS CLI setup.
- **[Guides](guides/README.md)**: CLI quick start, SDK examples, and
  deeper topics.

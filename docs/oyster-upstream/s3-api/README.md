# S3 API Reference

Oyster provides an S3-compatible API that works with the AWS CLI, `boto3`,
the AWS SDK for JavaScript, and any other S3-compatible client. It uses
standard AWS Signature Version 4 (SigV4) authentication.

## How It Works

The S3 API runs on the **same HTTP port** as the JSON API. Any request that
doesn't match `/api/v1/`, `/health`, `/ready`, `/metrics`, or `/api/docs`
is routed to the S3-compatible handler.

This means you point your S3 client at the same `$OYSTER_URL`, with no separate
port or endpoint needed.

## Supported Operations

| Category | Operations |
|----------|------------|
| Buckets | CreateBucket, HeadBucket, ListBuckets, DeleteBucket |
| Objects | PutObject, GetObject, HeadObject, DeleteObject, ListObjectsV2 |
| Tagging | GetObjectTagging, PutObjectTagging, DeleteObjectTagging |

See [Limitations](limitations.md) for what's not yet supported compared to
full AWS S3.

## Authentication

S3 requests are authenticated using **S3 access keys** (created through the
[JSON API](../json-api/access-keys.md)). The AWS SDK and CLI handle SigV4
signing automatically, so you just provide your access key ID and secret.

Both APIs share the same database, so buckets and objects created through S3
are visible in the JSON API and vice versa.

## Getting Started

Head to [S3 Setup](setup.md) to configure your AWS CLI or SDK.

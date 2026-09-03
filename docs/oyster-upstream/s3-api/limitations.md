# Limitations

Oyster implements the most commonly used S3 operations. The sections below document
what's different from a full AWS S3 deployment.

## Supported vs. Not Supported

| Feature | Status | Notes |
|---------|--------|-------|
| CreateBucket | Supported | |
| HeadBucket | Supported | |
| ListBuckets | Supported | Max 1000, no pagination |
| DeleteBucket | Supported | Requires empty bucket (same as AWS S3) |
| PutObject | Supported | Single-part only |
| GetObject | Supported | |
| HeadObject | Supported | |
| DeleteObject | Supported | |
| ListObjectsV2 | Supported | Prefix, delimiter, pagination |
| Conditional Requests | Supported | If-Match, If-None-Match on object operations |
| Multipart Upload | Not supported | Use single PutObject (max 1 GB) |
| CopyObject | Not supported | Download and re-upload instead |
| DeleteObjects (batch) | Not supported | Delete one at a time |
| Object Versioning | Not supported | Overwrite replaces the object |
| Bucket Policies | Not supported | |
| ACLs | Not supported | |
| CORS | Not supported | |
| Server-Side Encryption | Not supported | Data is stored unencrypted |
| Object Tagging | Supported | get/put/delete; shares tags with JSON API |
| Custom Metadata Headers | Not supported | Only Content-Type is stored |
| Website Hosting | Not supported | |
| S3 Select | Not supported | |
| Storage Classes | Not supported | All objects are `STANDARD` |
| Transfer Acceleration | Not supported | |
| Inventory / Analytics | Not supported | |
| Object Lock / Legal Hold | Not supported | |
| Lifecycle Rules | Not supported | See automatic expiration below |

## Behavioral Differences

### Object Expiration

All objects in a bucket share the owning account's `StoragePool`
lifetime, with no per-object expiration to set. Oyster runs a
background extension service that renews the pool before it expires,
so objects persist indefinitely as long as the service is running and
the account's wallet stays funded. See
[Blob Lifecycle](../guides/blob-lifecycle.md) for the model.

### Bucket Naming

Oyster's bucket naming rules are slightly **stricter** than AWS S3:

| Rule | AWS S3 | Oyster |
|------|--------|--------|
| Dots (`.`) in names | Allowed | Not allowed |
| Underscores (`_`) in names | Allowed | Not allowed |
| Consecutive hyphens (`--`) | Allowed | Not allowed |
| Reserved names | None | `health`, `ready`, `metrics`, `api` |

### ListBuckets Limit

`ListBuckets` returns a maximum of 1000 buckets with no pagination support.

### Path-Style URLs Only

Oyster only supports **path-style** S3 URLs:

```
http://endpoint/bucket-name/key
```

Virtual-hosted-style URLs (`bucket-name.endpoint/key`) are **not**
supported. Always set `force_path_style: true` in your SDK configuration.

### No Region Semantics

Oyster ignores the region in S3 requests. All data is stored in the same
location. You still need to specify a region for SigV4 signing to work, so
use any valid region string, for example `us-east-1`.

### ETag Format

ETags are always the MD5 digest of the object content, even for large
objects. There is no multipart ETag format, because multipart upload is not supported.

### Conditional Request Headers

`If-Match` and `If-None-Match` headers are supported on PutObject,
GetObject, HeadObject, and DeleteObject (If-Match only). These enable
cache validation (`If-None-Match` returns 304 on GET/HEAD) and safe
concurrent writes (`If-Match` for optimistic locking, `If-None-Match: *`
for create-only semantics). Time-based conditionals (`If-Modified-Since`,
`If-Unmodified-Since`) are not supported.

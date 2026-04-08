# BucketObject

`BucketObject` is the planned object-store layer on top of `blob_bucket`.

`blob_bucket` is intentionally a pooled-storage primitive: it can register, certify, delete,
extend, and resize pooled blobs, but it does not model named objects, object versions, etags,
or conditional updates. `BucketObject` is where those semantics belong.

## Design Goals

- Expose web3-native named object semantics on top of Walrus pooled storage.
- Preserve S3-style object behavior where it maps cleanly to Sui and Walrus.
- Keep expensive storage registration in the bucket layer and object concurrency in the
  shared-object layer.
- Support permissionless upload/certify/finalize flows after the paid registration step.
- Make exact-key resolution on-chain, while leaving listing and rich queries to gateway/indexer
  infrastructure.

## Non-Goals

- Full on-chain `ListObjectsV2` parity.
- On-chain prefix scans or delimiter-based listing.
- On-chain presigned URL logic or HTTP cache negotiation.
- Immediate transport-level multipart parity in the contract.

## Layering

There should be three layers:

1. `blob_bucket`
   Paid pooled storage primitive.
2. `bucket_object`
   Named object semantics, versions, etags, metadata, delete markers, and object-level copy/move.
3. Gateway/indexer
   S3-compatible HTTP APIs, prefix listing, continuation tokens, byte ranges, multipart
   transport, and delegated upload sessions.

## On-Chain Model

The intended end-state model is:

- `BucketObject`
  Shared bucket-level configuration that points at one `BlobBucket`.
- `BucketObjectCap`
  Capability for privileged bucket operations.
- `ObjectEntry`
  Shared object state for a single key.
- `ObjectVersion`
  Immutable version record for a key update.
- `ObjectHeaders`
  Structured HTTP-like headers for object delivery.
- `ObjectTags`
  User-defined object tags.

### Object Entry State

Each key should eventually resolve to an `ObjectEntry` with:

- `current_version_id`
  The live certified version readers should use.
- `pending_version_id`
  A newer registered version that is not live yet.
- `current_etag`
  The object-level etag used for `If-Match` / `If-None-Match`.
- `generation`
  Monotonic object update counter.
- `deleted`
  Whether the current state is a delete marker.

This keeps reads stable while a newer version is still being uploaded and certified.

## Update Flow

The intended write flow is:

1. `put_object_if_absent_and_register` or `update_object_if_match_and_register`
   - serialize on the shared `ObjectEntry`
   - check the object etag / absence precondition
   - register a new pooled blob in the linked `BlobBucket`
   - create a pending object version
2. upload data to Walrus storage nodes
3. certify the pooled blob
4. `finalize_object_if_certified`
   - permissionless
   - promote `pending_version_id` to `current_version_id`

Certification should remain permissionless. The paid, serialized step is registration.

## Planned Contract APIs

### Bucket APIs

- `create_bucket`
- `delete_bucket_if_empty`
- `set_bucket_versioning`
- `set_bucket_cors`
- `set_bucket_website`
- `set_bucket_lifecycle`
- `set_bucket_policy`

### Object Write APIs

- `put_object_if_absent_and_register`
- `update_object_if_match_and_register`
- `delete_object`
- `delete_object_if_match`
- `finalize_object_if_certified`
- `abort_pending_object`

### Object Read APIs

- `head_object`
- `head_object_version`
- `head_pending_object`
- `resolve_object_content`
- `resolve_object_version_content`

### Object Metadata APIs

- `set_object_headers_if_match`
- `set_object_metadata_if_match`
- `set_object_tags_if_match`
- `delete_object_tags_if_match`

### Versioning / Copy APIs

- `restore_object_version`
- `copy_object_if_match`
- `rename_object_if_match`

### Batch / Manifest APIs

- `commit_batch_if_match`
- `put_manifest_if_match`

## S3 Compatibility Boundary

This package should support S3-compatible semantics for:

- `PutObject`
- `HeadObject`
- `DeleteObject`
- `CopyObject`
- object versioning
- `If-Match` / `If-None-Match`
- object metadata and tags

The gateway/indexer should provide:

- `GetObject`
- `ListObjectsV2`
- `ListObjectVersions`
- range reads
- multipart upload transport
- presigned/session auth

## First Slice In This Package

This initial scaffold only introduces the shared `BucketObject` wrapper and capability shape. It
does not yet implement the object-entry table, version records, or update flow. Those should land
in follow-up slices once the package shape is reviewed.

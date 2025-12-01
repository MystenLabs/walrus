# Storage Node BlobManager Design

## Overview

The storage node tracks managed blobs separately from regular blobs, storing the blobs and
blob managers' end epochs separately to avoid bulk updates when BlobManager storage is extended.
This document describes the data structures, event processing, and status computation for managed
blobs.

## Data Structures

### BlobManagerTable

A simple key-value table that stores the current state of each BlobManager:

```rust
struct StoredBlobManagerInfo {
    // The epoch starting from which the blobs won't be available for reads.
    end_epoch: Epoch,
    // The epoch at which the blob data will be deleted: gc_epoch = end_epoch + grace_period.
    gc_epoch: Epoch,
}

struct BlobManagerTable {
    blob_managers: DBMap<ObjectID, StoredBlobManagerInfo>,
}
```

The table is updated when:
- `BlobManagerCreated` event: Insert new entry with initial `end_epoch`.
- `BlobManagerExtended` event: Update `end_epoch` to `new_end_epoch`.
- (TODO): `BlobManagerUpdated` event: Update other blob manager configs, like gc_epoch.

### Aggregate Blob Info (BlobInfo)

A new variant `BlobInfoV2` is added to the existing `BlobInfo` enum. `BlobInfoV2` is a union struct
containing the existing `ValidBlobInfoV1` and a new `ManagedBlobInfo`. This design preserves all
existing logic for regular blobs unchanged.

```rust
enum BlobInfo {
    V1(BlobInfoV1),           // Existing: regular blobs only
    V2(BlobInfoV2),           // New: supports both regular and managed blobs
}

struct BlobInfoV2 {
    Valid(ValidBlobInfoV2),
    Invalid(InvalidBlobInfo),
}

struct ValidBlobInfoV2 {
    regular_blob_info: Option<ValidBlobInfoV1>,  // Existing V1 structure, unchanged
    managed_blob_info: Option<ManagedBlobInfo>,  // New: managed blob tracking
}
```

When both regular and managed blobs exist for the same `blob_id`, both are stored and query
functions return an aggregation over both.

### ManagedBlobInfo

Tracks a single blob (by blob_id) across multiple BlobManagers. A blob can be registered by
multiple BlobManagers independently, each as either deletable or permanent.

```rust
struct ManagedBlobInfo {
    // These two members are duplicates as ValidBlobInfoV1, and this is to make sure minimal
    // changes to the existing data structures and reuse of the APIs.
    is_metadata_stored: bool,
    initial_certified_epoch: Option<Epoch>,

    // Maps BlobManager ObjectID to ManagedBlob ObjectID.
    registered_deletable: HashMap<ObjectID, ObjectID>,
    registered_permanent: HashMap<ObjectID, ObjectID>,

    // BlobManagers that have certified this blob.
    certified_deletable: HashSet<ObjectID>,
    certified_permanent: HashSet<ObjectID>,

    // Populated at read time, NOT serialized.
    #[serde(skip)]
    deletable_end_epoch: Option<Epoch>,
    #[serde(skip)]
    permanent_end_epoch: Option<Epoch>,
}
```

The `end_epoch` fields are not stored in the database. They are computed on-demand by querying
the BlobManagerTable when a blob is read.

### Per-Object Blob Info (PerObjectBlobInfo)

A new variant `V2` is added to the existing `PerObjectBlobInfo` enum. Each entry in the table is
either a regular blob (V1) or a managed blob (V2), so existing logic for regular blobs remains
unchanged.

This info is separate from ManagedBlobInfo due to the following considerations:

1. Minimal changes to the existing code.
2. If this info were stored inside ManagedBlobInfo, the struct's size could become large when many
   blob managers store the same blob.

```rust
enum PerObjectBlobInfo {
    V1(PerObjectBlobInfoV1),  // Existing: regular blobs
    V2(PerObjectBlobInfoV2),  // New: managed blobs
}

struct PerObjectBlobInfoV2 {
    blob_id: BlobId,
    registered_epoch: Epoch,
    certified_epoch: Option<Epoch>,
    blob_manager_id: ObjectID,
    deletable: bool,
    event: EventID,
    deleted: bool,

    // Populated at read time, NOT serialized.
    #[serde(skip)]
    end_epoch: Option<Epoch>,
}
```

## End Epoch Population

The `end_epoch` fields are populated dynamically when reading blob info, not stored in the database.
This design avoids bulk updates when a BlobManager's storage is extended.

**For aggregate blob info** (`get()` method):
1. Read the stored `ManagedBlobInfo` from the database.
2. For each BlobManager in `registered_deletable`, look up its `end_epoch` from `BlobManagerTable`.
3. Set `deletable_end_epoch` to the maximum across all deletable registrations.
4. Repeat for `registered_permanent` to compute `permanent_end_epoch`.

**For per-object blob info** (`get_per_object_info()` method):
1. Read the stored `PerObjectBlobInfoV2` from the database.
2. Look up the BlobManager's `end_epoch` from `BlobManagerTable`.
3. Set `end_epoch` on the returned struct.

This approach ensures:
- Storage extension affects all managed blobs immediately without database writes.
- Queries always return the current `end_epoch` state.
- No synchronization issues between blob records and BlobManager state.

**Optimization**: To avoid frequent reads on the BlobManagerTable, an in-memory LRU cache stores
the blob manager's metadata. For example, a cache of 1 million blob managers can be bounded by
approximately 100MB.

## Event Processing

### BlobManager Events

Separate events are added for managed blobs, so that the existing code is kept unchanged.

Processed via `process_blob_manager_event()`:

| Event | Action |
|-------|--------|
| `BlobManagerCreated` | Insert `(manager_id, end_epoch)` into BlobManagerTable |
| `BlobManagerExtended` | Update `end_epoch` in BlobManagerTable |

### Managed Blob Events

Processed via `update_blob_info()`:

| Event | Aggregate Update | Per-Object Update |
|-------|------------------|-------------------|
| `ManagedBlobRegistered` | Add to `registered_deletable` or `registered_permanent` | Create new V2 entry |
| `ManagedBlobCertified` | Add to `certified_deletable` or `certified_permanent` | Set `certified_epoch` |
| `ManagedBlobDeleted` | Remove from all maps/sets | Set `deleted = true` |

### Status Change Types

```rust
enum BlobStatusChangeType {
    // V1 (regular blobs)
    Register,
    Certify,
    Extend,
    Delete { was_certified: bool },

    // V2 (managed blobs)
    RegisterManaged { blob_manager_id: ObjectID, object_id: ObjectID },
    CertifyManaged { blob_manager_id: ObjectID },
    DeleteManaged { blob_manager_id: ObjectID, was_certified: bool },
}
```

## Status Computation

### Managed Blob Status

The `managed_blob_status()` function computes status from `ManagedBlobInfo`:

1. If all registration and certification maps are empty → `Nonexistent`.
2. Count permanent and deletable registrations/certifications.
3. If any permanent registrations exist:
   - Return `Permanent` with `end_epoch = permanent_end_epoch`.
   - Include deletable counts for reference.
4. Otherwise if any deletable registrations exist:
   - Return `Deletable` with counts.
5. Otherwise → `Nonexistent`.

### Combined Status

The final blob status combines V1 and V2:

```rust
fn to_blob_status(&self, current_epoch: Epoch) -> BlobStatus {
    let v1_status = self.regular_blob_info
        .map(|info| info.to_blob_status(current_epoch))
        .unwrap_or(BlobStatus::Nonexistent);

    let managed_status = self.managed_blob_status();

    v1_status.combine(managed_status)
}
```

The `combine()` method prioritizes `Permanent` over `Deletable`, and merges deletable counts.

## Registration Checks

### is_registered() for Managed Blobs

For per-object blob info:
```rust
fn is_registered(&self, current_epoch: Epoch) -> bool {
    !self.deleted && self.end_epoch.is_some_and(|e| e > current_epoch)
}
```

For aggregate blob info, `is_registered()` checks both V1 and V2 registrations.

### is_certified() for Managed Blobs

```rust
fn is_certified(&self, current_epoch: Epoch) -> bool {
    self.is_registered(current_epoch)
        && self.certified_epoch.is_some_and(|e| e <= current_epoch)
}
```

## Garbage Collection

The GC process iterates over blob info and checks expiration status. For managed blobs, the
`end_epoch` must be populated from the BlobManagerTable during iteration, and a grace period
is applied before deletion.

Flow:
1. Iterate over aggregate blob info.
2. For managed blobs, look up BlobManager info to populate `end_epoch` and `gc_eligible_epoch`.
3. Check `can_data_be_deleted(current_epoch)` which respects the grace period.
4. Only delete blob data after the grace period has passed.

The grace period allows users to extend storage after expiration without losing data.

## Key Design Decisions

1. **Lazy end_epoch population**: Avoids O(n) database updates when storage is extended.

2. **Separate tracking for deletable vs permanent**: Allows accurate status reporting based on
   registration type.

3. **Combined V1/V2 status**: When a blob exists as both regular and managed, query functions
   return an aggregation over both.

## Files

- `blob_manager_info.rs`: `BlobManagerTable` and `StoredBlobManagerInfo`.
- `blob_info.rs`: `ManagedBlobInfo`, `PerObjectBlobInfoV2`, `ValidBlobInfoV2`, status computation.
- `storage.rs`: Event processing via `process_blob_manager_event()` and `update_blob_info()`.

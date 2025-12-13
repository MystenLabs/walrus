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
}

impl StoredBlobManagerInfo {
    // Returns the epoch at which managed blobs become eligible for GC.
    // With no grace period, GC is eligible at end_epoch.
    fn gc_eligible_epoch(&self) -> Epoch {
        self.end_epoch
    }
}

struct BlobManagerTable {
    blob_managers: DBMap<ObjectID, StoredBlobManagerInfo>,
}
```

The table is updated when:
- `BlobManagerCreated` event: Insert new entry with initial `end_epoch`.
- `BlobManagerUpdated` event: Update `end_epoch`.

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
multiple BlobManagers independently, each as either deletable or permanent. All per-blob info
is embedded directly in this struct (no separate per-object table for managed blobs).

```rust
/// Info for a registered (but not yet certified) managed blob.
struct RegisteredManagedBlobInfo {
    blob_object_id: ObjectID,
    registered_epoch: Epoch,
    event: EventID,
}

/// Info for a certified managed blob.
struct CertifiedManagedBlobInfo {
    blob_object_id: ObjectID,
    certified_epoch: Epoch,
    event: EventID,
}

struct ManagedBlobInfo {
    is_metadata_stored: bool,
    initial_certified_epoch: Option<Epoch>,

    // Registered blobs (not yet certified). Key: BlobManager ObjectID.
    registered_deletable: HashMap<ObjectID, RegisteredManagedBlobInfo>,
    registered_permanent: HashMap<ObjectID, RegisteredManagedBlobInfo>,

    // Certified blobs (moved from registered_* on certification). Key: BlobManager ObjectID.
    certified_deletable: HashMap<ObjectID, CertifiedManagedBlobInfo>,
    certified_permanent: HashMap<ObjectID, CertifiedManagedBlobInfo>,

    // Populated at read time, NOT serialized.
    #[serde(skip)]
    deletable_end_epoch: Option<Epoch>,
    #[serde(skip)]
    permanent_end_epoch: Option<Epoch>,
}
```

**Key design: Move-on-certify pattern.** When a blob is certified, its entry is moved from
`registered_*` to `certified_*` map. This saves space since a blob is only in one map at a time
per BlobManager.

The `end_epoch` and `gc_eligible_epoch` fields are not stored in the database. They are computed
on-demand by querying the BlobManagerTable when a blob is read or during GC iteration.

### Per-Object Blob Info (PerObjectBlobInfo)

**Managed blobs do NOT use the per-object table.** The `PerObjectBlobInfo` table is only used for
regular blobs (V1). All managed blob info is embedded directly in `ManagedBlobInfo` within the
aggregate `BlobInfo`.

```rust
enum PerObjectBlobInfo {
    V1(PerObjectBlobInfoV1),  // Regular blobs only
}
```

This consolidation eliminates the need for two table lookups and two GC passes for managed blobs.

## End Epoch Population

The `end_epoch` and `gc_eligible_epoch` fields are populated dynamically when reading blob info,
not stored in the database. This design avoids bulk updates when a BlobManager's storage is
extended.

**For aggregate blob info** (`get()` method):
1. Read the stored `ManagedBlobInfo` from the database.
2. Call `populate_epochs(&BlobManagerTable)` which:
   - For each BlobManager in all 4 maps, looks up its info from `BlobManagerTable`.
   - Sets `deletable_end_epoch` to the max across deletable maps.
   - Sets `permanent_end_epoch` to the max across permanent maps.

```rust
impl ManagedBlobInfo {
    fn populate_epochs(&mut self, blob_managers: &BlobManagerTable) -> Result<(), TypedStoreError> {
        // Find max end_epoch from deletable BlobManagers.
        for manager_id in self.registered_deletable.keys().chain(self.certified_deletable.keys()) {
            if let Some(manager_info) = blob_managers.get(manager_id)? {
                self.deletable_end_epoch = max(self.deletable_end_epoch, manager_info.end_epoch);
            }
        }
        // Similar for permanent maps...
        Ok(())
    }
}
```

This approach ensures:
- Storage extension affects all managed blobs immediately without database writes.
- Queries always return the current `end_epoch` state.
- No synchronization issues between blob records and BlobManager state.

## Event Processing

### BlobManager Events

Separate events are added for managed blobs, so that the existing code is kept unchanged.

Processed via `process_blob_manager_event()`:

| Event | Action |
|-------|--------|
| `BlobManagerCreated` | Insert `(manager_id, end_epoch)` into BlobManagerTable |
| `BlobManagerUpdated` | Update `end_epoch` in BlobManagerTable |

### Managed Blob Events

Processed via `update_blob_info()`. **Only the aggregate table is updated** (no per-object table
writes for managed blobs).

| Event | Action |
|-------|--------|
| `ManagedBlobRegistered` | Add `RegisteredManagedBlobInfo` to `registered_deletable` or `registered_permanent` |
| `ManagedBlobCertified` | Move entry from `registered_*` to `certified_*` (with `CertifiedManagedBlobInfo`) |
| `ManagedBlobDeleted` | Remove entry from the appropriate map (registered or certified) |

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
    CertifyManaged { blob_manager_id: ObjectID, object_id: ObjectID },
    DeleteManaged { blob_manager_id: ObjectID, was_certified: bool },
}
```

### Merge Operators

Atomic updates to `BlobInfo` are performed via RocksDB merge operators:

```rust
enum BlobInfoMergeOperand {
    ChangeStatus { change_type: BlobStatusChangeType, change_info: BlobStatusChangeInfo },
    // ... other variants ...
    RemoveExpiredBlobManagers { expired_blob_manager_ids: Vec<ObjectID> },
}
```

The `RemoveExpiredBlobManagers` operand is used during GC to atomically remove expired BlobManagers
from all 4 maps.

## Status Computation

### Managed Blob Status

The `managed_blob_status()` function computes status from `ManagedBlobInfo`:

1. If all 4 maps are empty → `Nonexistent`.
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

```rust
impl ManagedBlobInfo {
    fn is_registered(&self, current_epoch: Epoch) -> bool {
        // Check all 4 maps since blobs move from registered_* to certified_* on certification.
        let has_deletable = !self.registered_deletable.is_empty()
            || !self.certified_deletable.is_empty();
        let has_permanent = !self.registered_permanent.is_empty()
            || !self.certified_permanent.is_empty();

        // Check if end_epoch is populated and not expired.
        let deletable_registered = self.deletable_end_epoch
            .is_some_and(|e| current_epoch < e && has_deletable);
        let permanent_registered = self.permanent_end_epoch
            .is_some_and(|e| current_epoch < e && has_permanent);

        deletable_registered || permanent_registered
    }
}
```

For aggregate blob info, `is_registered()` checks both V1 and V2 registrations.

### is_certified() for Managed Blobs

```rust
fn is_certified(&self, current_epoch: Epoch) -> bool {
    let has_certified = !self.certified_deletable.is_empty()
        || !self.certified_permanent.is_empty();
    has_certified && self.initial_certified_epoch.is_some_and(|e| e <= current_epoch)
}
```

## Garbage Collection

GC for managed blobs happens in the `delete_expired_blob_data()` function. The key insight is that
expired BlobManagers are removed from the maps via atomic merge operators, and blob data is only
deleted when all maps are empty.

### GC Flow

```rust
fn delete_expired_blob_data(&self, current_epoch: Epoch) {
    for (blob_id, mut blob_info) in self.blob_info.aggregate_blob_info_iter() {
        // For managed blobs, find and remove expired BlobManagers.
        if let Some(managed_info) = blob_info.managed_blob_info_mut() {
            // Find all BlobManagers that have passed their gc_eligible_epoch.
            let expired_blob_manager_ids: Vec<ObjectID> = managed_info
                .all_blob_manager_ids()
                .filter(|manager_id| {
                    self.get_blob_manager_gc_eligible_epoch(manager_id)
                        .is_some_and(|gc_epoch| current_epoch >= gc_epoch)
                })
                .cloned()
                .collect();

            if !expired_blob_manager_ids.is_empty() {
                // Update local in-memory copy.
                managed_info.remove_blob_managers(&expired_blob_manager_ids);

                // Submit atomic merge operator to database.
                self.blob_info.remove_expired_blob_managers(&blob_id, expired_blob_manager_ids);
            }
        }

        // Check if blob data can be deleted (using updated local copy).
        if blob_info.can_data_be_deleted(current_epoch) {
            self.attempt_to_delete_blob_data(&blob_id, current_epoch);
        }
    }
}
```

### can_data_be_deleted()

```rust
impl ManagedBlobInfo {
    // Returns true if all 4 maps are empty.
    fn can_data_be_deleted(&self, _current_epoch: Epoch) -> bool {
        self.registered_deletable.is_empty()
            && self.registered_permanent.is_empty()
            && self.certified_deletable.is_empty()
            && self.certified_permanent.is_empty()
    }
}

impl BlobInfoV2 {
    fn can_data_be_deleted(&self, current_epoch: Epoch) -> bool {
        // For regular blobs, use the default is_registered check.
        let regular_can_delete = self.regular_blob_info
            .map(|info| !info.is_registered(current_epoch))
            .unwrap_or(true);

        // For managed blobs, check if all maps are empty.
        let managed_can_delete = self.managed_blob_info
            .map(|info| info.can_data_be_deleted(current_epoch))
            .unwrap_or(true);

        // Both must be deletable for the data to be deleted.
        regular_can_delete && managed_can_delete
    }
}
```

### Helper Methods for GC

```rust
impl ManagedBlobInfo {
    /// Returns all BlobManager IDs from all 4 maps.
    fn all_blob_manager_ids(&self) -> impl Iterator<Item = &ObjectID> {
        self.registered_deletable.keys()
            .chain(self.registered_permanent.keys())
            .chain(self.certified_deletable.keys())
            .chain(self.certified_permanent.keys())
    }

    /// Removes the specified BlobManagers from all 4 maps.
    fn remove_blob_managers(&mut self, blob_manager_ids: &[ObjectID]) {
        for manager_id in blob_manager_ids {
            self.registered_deletable.remove(manager_id);
            self.registered_permanent.remove(manager_id);
            self.certified_deletable.remove(manager_id);
            self.certified_permanent.remove(manager_id);
        }
    }

    /// Returns true if all 4 maps are empty.
    fn is_empty(&self) -> bool {
        self.registered_deletable.is_empty()
            && self.registered_permanent.is_empty()
            && self.certified_deletable.is_empty()
            && self.certified_permanent.is_empty()
    }
}
```

## Storage Confirmation for Managed Blobs

The storage confirmation API uses a dedicated endpoint for managed blobs. The `BlobPersistenceType`
enum has only two variants (`Permanent` and `Deletable`), and managed blobs use the same variants
as regular blobs.

### API Endpoint

```
GET /v1/blobs/{blob_id}/confirmation/managed/{manager_id}/{deletable}
```

The `manager_id` and `deletable` flag are passed in the URL path. The server:
1. Looks up the `ManagedBlob.object_id` from `ManagedBlobInfo` using `(blob_id, manager_id)`.
2. Returns a confirmation with `Permanent` or `Deletable { object_id }` based on the `deletable` flag.

### Implementation

The `ServiceState` trait defines `compute_storage_confirmation_managed()`:

```rust
fn compute_storage_confirmation_managed(
    &self,
    blob_id: &BlobId,
    blob_manager_id: &ObjectID,
    deletable: bool,
) -> impl Future<Output = Result<StorageConfirmation, ComputeStorageConfirmationError>> + Send;
```

This method:
1. Verifies the blob is registered for the given `(blob_id, blob_manager_id)`.
2. Looks up the `object_id` from `ManagedBlobInfo`.
3. Creates the `BlobPersistenceType` based on `deletable`.
4. Signs and returns the confirmation.

### Rationale for Separate Endpoint

Instead of adding a `Managed` variant to `BlobPersistenceType`, managed blobs use a separate API
endpoint with `manager_id` in the URL. Benefits:
- **Simpler enum**: Only two variants in `BlobPersistenceType`.
- **Cleaner wire format**: Same BCS encoding for managed and regular blobs.
- **Separation of concerns**: The `manager_id` is routing information, not persistence semantics.

## Quorum Verification for Deletable Managed Blobs

For deletable managed blobs, the client doesn't know the `object_id` upfront - it's determined by
the server based on the registered `ManagedBlob`. The client must collect confirmations from
multiple storage nodes and verify that a quorum agrees on the same `object_id`.

### Client-Side Verification

The client uses `execute_weight_mapped()` to group confirmations by `BlobPersistenceType`:

```rust
let completed = weighted_futures.execute_weight_mapped(
    &|weight| weight >= quorum_weight,
    n_concurrent,
    |(_confirmation, persistence_type)| *persistence_type,  // Group by persistence type
).await;
```

This ensures:
- Permanent blobs reach quorum for `BlobPersistenceType::Permanent`.
- Deletable blobs reach quorum for `BlobPersistenceType::Deletable { object_id }` with the same
  `object_id`.

### Verification Method

The `SignedStorageConfirmation::verify_managed_and_extract_persistence_type()` method:
1. Verifies the signature and decodes the confirmation.
2. Checks epoch and blob_id match.
3. Validates the persistence type matches the expected `deletable` flag.
4. Returns `(Confirmation, BlobPersistenceType)` directly.

```rust
pub fn verify_managed_and_extract_persistence_type(
    &self,
    public_key: &PublicKey,
    epoch: Epoch,
    blob_id: BlobId,
    deletable: bool,
) -> Result<(Confirmation, BlobPersistenceType), MessageVerificationError>;
```

## Upload Relay Support for Managed Blobs

The upload relay requires `BlobPersistenceType` with the actual `object_id` for deletable blobs.
For managed blobs, the client queries Sui on-chain to get the `object_id` before calling the
upload relay.

### Query Flow

1. Client calls `get_managed_blob_object_id(blob_id, deletable)` on `BlobManagerClient`.
2. This queries the on-chain `ManagedBlob` from the BlobManager's blob stash.
3. Client constructs `BlobPersistenceType::Deletable { object_id }` with the queried ID.
4. Upload relay proceeds with the correct persistence type.

### Implementation

```rust
// Helper method to get BlobPersistenceType for any blob type.
async fn get_blob_persistence_type(
    &self,
    blob_object: &BlobObject,
) -> ClientResult<BlobPersistenceType> {
    match blob_object {
        BlobObject::Regular(_) => Ok(blob_object.blob_persistence_type()),
        BlobObject::Managed { blob_id, deletable, .. } => {
            if *deletable {
                let object_id = self
                    .get_blob_manager_client()?
                    .get_managed_blob_object_id(*blob_id, *deletable)
                    .await?;
                Ok(BlobPersistenceType::Deletable { object_id: object_id.into() })
            } else {
                Ok(BlobPersistenceType::Permanent)
            }
        }
    }
}
```

This approach keeps the upload relay API unchanged while supporting managed blobs.

## Key Design Decisions

1. **Single-table for managed blobs**: All managed blob info is in `ManagedBlobInfo` within the
   aggregate table. No per-object table entries for managed blobs.

2. **Move-on-certify pattern**: When certified, entries move from `registered_*` to `certified_*`
   maps, saving space.

3. **Lazy end_epoch population**: Avoids O(n) database updates when storage is extended.

4. **Atomic GC with merge operators**: The `RemoveExpiredBlobManagers` merge operator ensures
   atomic removal of expired BlobManagers from all maps.

5. **In-memory simulation for GC decisions**: Update local copy before deciding on deletion to
   avoid re-reading from database.

6. **Simple GC eligibility**: BlobManager's `gc_eligible_epoch = end_epoch`. Blobs become eligible
   for garbage collection when the current epoch reaches the BlobManager's end_epoch.

7. **Combined V1/V2 status**: When a blob exists as both regular and managed, query functions
   return an aggregation over both.

8. **No `Managed` variant in `BlobPersistenceType`**: Managed blobs use `Permanent` or `Deletable`
   like regular blobs, with `manager_id` passed separately via the API endpoint.

## Files

- `blob_manager_info.rs`: `BlobManagerTable` and `StoredBlobManagerInfo`.
- `blob_info.rs`: `ManagedBlobInfo`, `RegisteredManagedBlobInfo`, `CertifiedManagedBlobInfo`,
  `ValidBlobInfoV2`, merge operators, status computation.
- `storage.rs`: Event processing via `process_blob_manager_event()` and `update_blob_info()`,
  GC via `delete_expired_blob_data()`.

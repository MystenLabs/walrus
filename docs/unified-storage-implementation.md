# UnifiedStorage Implementation Guide

## Overview

This document provides implementation-level details for the UnifiedStorage/BlobManager separation refactor. Since BlobManager is not yet deployed, this is a greenfield implementation with no migration requirements.

## Implementation Phases

### Phase 0: Add UnifiedStorage Events (CRITICAL - DO FIRST)

Without these events, storage nodes cannot track UnifiedStorage instances.

#### File: `contracts/walrus/sources/system/events.move`

Add event definitions at line ~127 (after existing events):

```move
// =========================================================================
// UnifiedStorage events
// =========================================================================

public struct UnifiedStorageCreated has copy, drop {
    storage_id: ID,
    end_epoch: u32,
}

public struct UnifiedStorageExtended has copy, drop {
    storage_id: ID,
    new_end_epoch: u32,
}

// Emit functions
public(package) fun emit_unified_storage_created(
    storage_id: ID,
    end_epoch: u32,
) {
    event::emit(UnifiedStorageCreated {
        storage_id,
        end_epoch
    });
}

public(package) fun emit_unified_storage_extended(
    storage_id: ID,
    new_end_epoch: u32,
) {
    event::emit(UnifiedStorageExtended {
        storage_id,
        new_end_epoch
    });
}
```

#### File: `contracts/walrus/sources/unified_storage.move`

Update the `new` function (line ~135) to emit event:

```move
public(package) fun new(
    initial_storage: Storage,
    current_epoch: u32,
    ctx: &mut TxContext,
): UnifiedStorage {
    let uid = object::new(ctx);
    let storage_id = object::uid_to_inner(&uid);
    let end_epoch = storage::end_epoch(&initial_storage);

    // Emit creation event (NEW)
    events::emit_unified_storage_created(storage_id, end_epoch);

    let unified_storage = UnifiedStorage {
        id: uid,
        end_epoch,
        // Keep existing fields from current implementation
    };

    storage::burn(initial_storage);
    unified_storage
}
```

Update `extend_storage` function (line ~249) to emit event:

```move
public fun extend_storage(
    self: &mut UnifiedStorage,
    extension_storage: Storage,
) {
    let new_end_epoch = storage::end_epoch(&extension_storage);
    assert!(new_end_epoch > self.end_epoch, EInvalidEndEpoch);

    self.end_epoch = new_end_epoch;

    // Emit extension event (NEW)
    events::emit_unified_storage_extended(
        object::id(self),
        new_end_epoch
    );

    storage::burn(extension_storage);
}
```

### Phase 1: Move Funding to BlobManager Package

#### File: `contracts/blob_manager/sources/coin_stash.move` (NEW)

Create new module for CoinStash (move from walrus package):

```move
module blob_manager::coin_stash {
    use sui::balance::{Self, Balance};
    use sui::coin::{Self, Coin};
    use wal::wal::WAL;
    use walrus::frost::FROST;

    public struct CoinStash has store {
        wal_balance: Balance<WAL>,
        frost_balance: Balance<FROST>,
    }

    public fun new(): CoinStash {
        CoinStash {
            wal_balance: balance::zero(),
            frost_balance: balance::zero(),
        }
    }

    public fun deposit_wal(stash: &mut CoinStash, coin: Coin<WAL>) {
        balance::join(&mut stash.wal_balance, coin::into_balance(coin));
    }

    public fun deposit_frost(stash: &mut CoinStash, coin: Coin<FROST>) {
        balance::join(&mut stash.frost_balance, coin::into_balance(coin));
    }

    public fun withdraw_wal(stash: &mut CoinStash, amount: u64, ctx: &mut TxContext): Coin<WAL> {
        coin::from_balance(balance::split(&mut stash.wal_balance, amount), ctx)
    }

    public fun withdraw_frost(stash: &mut CoinStash, amount: u64, ctx: &mut TxContext): Coin<FROST> {
        coin::from_balance(balance::split(&mut stash.frost_balance, amount), ctx)
    }

    public fun wal_balance(stash: &CoinStash): u64 {
        balance::value(&stash.wal_balance)
    }

    public fun frost_balance(stash: &CoinStash): u64 {
        balance::value(&stash.frost_balance)
    }
}
```

#### File: `contracts/blob_manager/sources/blob_manager.move`

Update to include CoinStash and capabilities:

```move
module blob_manager::blob_manager {
    use std::vector;
    use sui::object::{Self, ID, UID};
    use sui::tx_context::TxContext;
    use walrus::storage::{Self, Storage};
    use walrus::unified_storage::{Self, UnifiedStorage};
    use walrus::system::{Self, SystemStateInfo};
    use blob_manager::coin_stash::{Self, CoinStash};

    // Error codes
    const EInvalidCapability: u64 = 0;
    const EPermissionDenied: u64 = 1;
    const EInsufficientCapacity: u64 = 2;

    // Permission types
    const PERMISSION_REGISTER: u8 = 1;
    const PERMISSION_DELETE: u8 = 2;
    const PERMISSION_EXTEND: u8 = 3;
    const PERMISSION_ADMIN: u8 = 255;

    public struct BlobManager has key, store {
        id: UID,
        unified_storage: UnifiedStorage,
        coin_stash: CoinStash,
        capacity: u64,
        total_registered_size: u64,
    }

    public struct BlobManagerCap has key, store {
        id: UID,
        blob_manager_id: ID,
        permissions: vector<u8>,
    }

    // Create new BlobManager with its UnifiedStorage
    public fun new(
        system: &SystemStateInfo,
        initial_storage: Storage,
        capacity: u64,
        ctx: &mut TxContext,
    ): (BlobManager, BlobManagerCap) {
        let current_epoch = system::epoch(system);
        let unified_storage = unified_storage::new(initial_storage, current_epoch, ctx);
        let blob_manager_uid = object::new(ctx);
        let blob_manager_id = object::uid_to_inner(&blob_manager_uid);
        let end_epoch = unified_storage::end_epoch(&unified_storage);

        let manager = BlobManager {
            id: blob_manager_uid,
            unified_storage,
            coin_stash: coin_stash::new(),
            capacity,
            total_registered_size: 0,
        };

        // Create admin capability
        let cap = BlobManagerCap {
            id: object::new(ctx),
            blob_manager_id,
            permissions: vector[PERMISSION_ADMIN],
        };

        // Emit creation event with proper current_epoch
        unified_storage::emit_blob_manager_created(
            current_epoch,
            blob_manager_id,
            object::id(&manager.unified_storage),
            end_epoch,
        );

        (manager, cap)
    }

    // Register blob requires capability
    public fun register_blob(
        cap: &BlobManagerCap,
        manager: &mut BlobManager,
        blob_id: vector<u8>,
        size: u64,
        encoding_type: u8,
        ctx: &mut TxContext,
    ) {
        assert!(has_permission(cap, PERMISSION_REGISTER), EPermissionDenied);
        assert!(cap.blob_manager_id == object::id(manager), EInvalidCapability);

        // Check capacity
        assert!(manager.total_registered_size + size <= manager.capacity, EInsufficientCapacity);

        // Register via UnifiedStorage
        unified_storage::register_blob_v2(
            &mut manager.unified_storage,
            blob_id,
            size,
            encoding_type,
            true, // deletable
            ctx,
        );

        manager.total_registered_size = manager.total_registered_size + size;
    }

    // Extend storage requires capability
    public fun extend_storage(
        cap: &BlobManagerCap,
        manager: &mut BlobManager,
        system: &SystemStateInfo,
        extension_storage: Storage,
    ) {
        assert!(has_permission(cap, PERMISSION_EXTEND), EPermissionDenied);
        assert!(cap.blob_manager_id == object::id(manager), EInvalidCapability);

        unified_storage::extend_storage(&mut manager.unified_storage, extension_storage);

        let current_epoch = system::epoch(system);
        let new_end_epoch = unified_storage::end_epoch(&manager.unified_storage);

        // Emit update event with proper current_epoch
        unified_storage::emit_blob_manager_updated(
            current_epoch,
            object::id(manager),
            object::id(&manager.unified_storage),
            new_end_epoch,
        );
    }

    // Create delegated capability
    public fun create_delegated_cap(
        admin_cap: &BlobManagerCap,
        permissions: vector<u8>,
        ctx: &mut TxContext,
    ): BlobManagerCap {
        assert!(has_permission(admin_cap, PERMISSION_ADMIN), EPermissionDenied);

        BlobManagerCap {
            id: object::new(ctx),
            blob_manager_id: admin_cap.blob_manager_id,
            permissions,
        }
    }

    fun has_permission(cap: &BlobManagerCap, permission: u8): bool {
        vector::contains(&cap.permissions, &PERMISSION_ADMIN) ||
        vector::contains(&cap.permissions, &permission)
    }

    // Accessors
    public fun unified_storage(manager: &BlobManager): &UnifiedStorage {
        &manager.unified_storage
    }

    public fun unified_storage_mut(manager: &mut BlobManager): &mut UnifiedStorage {
        &mut manager.unified_storage
    }

    public fun capacity(manager: &BlobManager): u64 {
        manager.capacity
    }

    public fun total_registered_size(manager: &BlobManager): u64 {
        manager.total_registered_size
    }
}
```

### Phase 2: Storage Node Updates

#### File: `crates/walrus-service/src/node/storage/unified_storage_info.rs` (RENAME from blob_manager_info.rs)

```rust
// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Storage for UnifiedStorage metadata in the database.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sui_types::base_types::ObjectID;
use typed_store::{
    TypedStoreError,
    rocks::{DBMap, ReadWriteOptions, RocksDB},
    traits::Map,
};
use walrus_core::Epoch;

use super::constants;

/// Information about a UnifiedStorage stored in the database.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredUnifiedStorageInfo {
    /// The end epoch for this UnifiedStorage.
    pub end_epoch: Epoch,
    // NO blob_manager_id field - clean separation
    // NO owner field - ownership is implicit in Sui
}

impl StoredUnifiedStorageInfo {
    /// Creates a new StoredUnifiedStorageInfo.
    pub fn new(end_epoch: Epoch) -> Self {
        Self { end_epoch }
    }

    /// Returns the epoch at which blobs in this storage become eligible for GC.
    pub fn gc_eligible_epoch(&self) -> Epoch {
        self.end_epoch
    }

    /// Returns true if the given epoch is valid for this storage.
    pub fn is_epoch_valid(&self, epoch: Epoch) -> bool {
        epoch < self.end_epoch
    }

    /// Returns true if data can be garbage collected at the given epoch.
    pub fn can_gc_at(&self, current_epoch: Epoch) -> bool {
        current_epoch >= self.gc_eligible_epoch()
    }
}

/// Table for storing UnifiedStorage information.
#[derive(Debug, Clone)]
pub struct UnifiedStorageTable {
    storages: DBMap<ObjectID, StoredUnifiedStorageInfo>,
}

impl UnifiedStorageTable {
    /// Reopens an existing UnifiedStorageTable from the database.
    pub fn reopen(database: &Arc<RocksDB>) -> Result<Self, TypedStoreError> {
        let storages = DBMap::reopen(
            database,
            Some(constants::unified_storages_cf_name()), // Update constant
            &ReadWriteOptions::default(),
            false,
        )?;

        Ok(Self { storages })
    }

    /// Gets the info for a specific UnifiedStorage.
    pub fn get(
        &self,
        storage_id: &ObjectID,
    ) -> Result<Option<StoredUnifiedStorageInfo>, TypedStoreError> {
        self.storages.get(storage_id)
    }

    /// Inserts or updates a UnifiedStorage's info.
    pub fn insert(
        &self,
        storage_id: ObjectID,
        info: StoredUnifiedStorageInfo,
    ) -> Result<(), TypedStoreError> {
        self.storages.insert(&storage_id, &info)
    }

    /// Updates the end_epoch for a UnifiedStorage.
    pub fn update_end_epoch(
        &self,
        storage_id: &ObjectID,
        new_end_epoch: Epoch,
    ) -> Result<(), TypedStoreError> {
        if let Some(mut info) = self.get(storage_id)? {
            info.end_epoch = new_end_epoch;
            self.storages.insert(storage_id, &info)
        } else {
            Ok(())
        }
    }

    /// Removes a UnifiedStorage from the table.
    #[allow(dead_code)]
    pub fn remove(&self, storage_id: &ObjectID) -> Result<(), TypedStoreError> {
        self.storages.remove(storage_id)
    }

    /// Checks if a UnifiedStorage exists.
    pub fn contains(&self, storage_id: &ObjectID) -> Result<bool, TypedStoreError> {
        self.storages.contains_key(storage_id)
    }

    /// Returns the number of UnifiedStorage instances stored.
    #[allow(dead_code)]
    pub fn count(&self) -> Result<usize, TypedStoreError> {
        Ok(self.storages.safe_iter()?.count())
    }
}
```

#### File: `crates/walrus-service/src/node/storage/blob_manager_index.rs` (NEW - Optional)

```rust
// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Optional index for BlobManager to UnifiedStorage mapping.
//! This is a separate concern from core storage logic.

use std::sync::Arc;

use sui_types::base_types::ObjectID;
use typed_store::{
    TypedStoreError,
    rocks::{DBMap, ReadWriteOptions, RocksDB},
    traits::Map,
};

use super::constants;

/// Optional index mapping BlobManager IDs to UnifiedStorage IDs.
#[derive(Debug, Clone)]
pub struct BlobManagerIndex {
    manager_to_storage: DBMap<ObjectID, ObjectID>,
}

impl BlobManagerIndex {
    /// Creates a new BlobManagerIndex.
    pub fn new(database: &Arc<RocksDB>) -> Result<Self, TypedStoreError> {
        let manager_to_storage = DBMap::reopen(
            database,
            Some(constants::blob_manager_index_cf_name()),
            &ReadWriteOptions::default(),
            false,
        )?;

        Ok(Self { manager_to_storage })
    }

    /// Maps a BlobManager ID to its UnifiedStorage ID.
    pub fn insert(
        &self,
        blob_manager_id: ObjectID,
        storage_id: ObjectID,
    ) -> Result<(), TypedStoreError> {
        self.manager_to_storage.insert(&blob_manager_id, &storage_id)
    }

    /// Gets the UnifiedStorage ID for a BlobManager.
    pub fn get_storage_id(
        &self,
        blob_manager_id: &ObjectID,
    ) -> Result<Option<ObjectID>, TypedStoreError> {
        self.manager_to_storage.get(blob_manager_id)
    }

    /// Removes a BlobManager mapping.
    pub fn remove(&self, blob_manager_id: &ObjectID) -> Result<(), TypedStoreError> {
        self.manager_to_storage.remove(blob_manager_id)
    }
}
```

#### File: `crates/walrus-service/src/node/storage/blob_info.rs`

Rename structs (lines ~2291-2328):

```rust
// OLD: pub struct ManagedBlobInfo
// NEW:
pub struct BlobV2Info {
    pub is_metadata_stored: bool,
    pub initial_certified_epoch: Option<Epoch>,

    // These already use storage_id as keys (no change needed)
    pub registered_deletable: HashMap<ObjectID, RegisteredBlobV2Info>,
    pub registered_permanent: HashMap<ObjectID, RegisteredBlobV2Info>,
    pub certified_deletable: HashMap<ObjectID, CertifiedBlobV2Info>,
    pub certified_permanent: HashMap<ObjectID, CertifiedBlobV2Info>,

    pub deletable_end_epoch: Option<Epoch>,
    pub permanent_end_epoch: Option<Epoch>,
    pub deletable_gc_eligible_epoch: Option<Epoch>,
    pub permanent_gc_eligible_epoch: Option<Epoch>,
}

// Rename info structs:
// RegisteredManagedBlobInfo -> RegisteredBlobV2Info
// CertifiedManagedBlobInfo -> CertifiedBlobV2Info
```

Update `populate_epochs` method (lines ~2336-2387):

```rust
impl BlobV2Info {
    pub fn populate_epochs(&mut self, unified_storage_table: &UnifiedStorageTable) {
        let mut max_deletable_end_epoch = None;
        let mut max_permanent_end_epoch = None;
        let mut min_deletable_gc_eligible_epoch = None;
        let mut min_permanent_gc_eligible_epoch = None;

        // Check deletable storages
        for storage_id in self.registered_deletable.keys().chain(self.certified_deletable.keys()) {
            if let Ok(Some(info)) = unified_storage_table.get(storage_id) {
                max_deletable_end_epoch = max_deletable_end_epoch
                    .map(|e| std::cmp::max(e, info.end_epoch))
                    .or(Some(info.end_epoch));

                let gc_epoch = info.gc_eligible_epoch();
                min_deletable_gc_eligible_epoch = min_deletable_gc_eligible_epoch
                    .map(|e| std::cmp::min(e, gc_epoch))
                    .or(Some(gc_epoch));
            }
        }

        // Check permanent storages
        for storage_id in self.registered_permanent.keys().chain(self.certified_permanent.keys()) {
            if let Ok(Some(info)) = unified_storage_table.get(storage_id) {
                max_permanent_end_epoch = max_permanent_end_epoch
                    .map(|e| std::cmp::max(e, info.end_epoch))
                    .or(Some(info.end_epoch));

                let gc_epoch = info.gc_eligible_epoch();
                min_permanent_gc_eligible_epoch = min_permanent_gc_eligible_epoch
                    .map(|e| std::cmp::min(e, gc_epoch))
                    .or(Some(gc_epoch));
            }
        }

        self.deletable_end_epoch = max_deletable_end_epoch;
        self.permanent_end_epoch = max_permanent_end_epoch;
        self.deletable_gc_eligible_epoch = min_deletable_gc_eligible_epoch;
        self.permanent_gc_eligible_epoch = min_permanent_gc_eligible_epoch;
    }
}
```

#### File: `crates/walrus-sui/src/types/events.rs`

Add UnifiedStorage events (after line ~460):

```rust
/// Event emitted when a UnifiedStorage is created.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnifiedStorageCreated {
    pub storage_id: ObjectID,
    pub end_epoch: Epoch,
}

/// Event emitted when a UnifiedStorage is extended.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnifiedStorageExtended {
    pub storage_id: ObjectID,
    pub new_end_epoch: Epoch,
}
```

Update enum (lines ~614-635):

```rust
// Separate enum for UnifiedStorage lifecycle events
pub enum UnifiedStorageEvent {
    Created(UnifiedStorageCreated),
    Extended(UnifiedStorageExtended),
}

// Keep existing BlobEvent enum with BlobV2 variants
pub enum BlobEvent {
    // Regular blob events
    Registered(BlobRegistered),
    Certified(BlobCertified),
    Deleted(BlobDeleted),

    // BlobV2 events (renamed from ManagedBlob*)
    BlobV2Registered(BlobV2Registered),
    BlobV2Certified(BlobV2Certified),
    BlobV2Deleted(BlobV2Deleted),
    BlobV2MadePermanent(BlobV2MadePermanent),
}

// Keep BlobManagerEvent separate
pub enum BlobManagerEvent {
    Created(BlobManagerCreated),
    Updated(BlobManagerUpdated),
}
```

#### File: `crates/walrus-service/src/node/storage.rs`

Update event processing (lines ~657-688):

```rust
impl Storage {
    /// Process UnifiedStorage lifecycle events.
    pub async fn process_unified_storage_event(&mut self, event: UnifiedStorageEvent) {
        match event {
            UnifiedStorageEvent::Created(e) => {
                let info = StoredUnifiedStorageInfo::new(e.end_epoch);
                if let Err(e) = self.unified_storage_table.insert(e.storage_id, info) {
                    error!("Failed to insert UnifiedStorage: {}", e);
                }
            }
            UnifiedStorageEvent::Extended(e) => {
                if let Err(e) = self.unified_storage_table.update_end_epoch(
                    &e.storage_id,
                    e.new_end_epoch,
                ) {
                    error!("Failed to update UnifiedStorage: {}", e);
                }
            }
        }
    }

    /// Process BlobManager events (optional, for indexing only).
    pub async fn process_blob_manager_event(&mut self, event: BlobManagerEvent) {
        if let Some(ref blob_manager_index) = self.blob_manager_index {
            match event {
                BlobManagerEvent::Created(e) => {
                    if let Err(err) = blob_manager_index.insert(
                        e.blob_manager_id,
                        e.storage_id,
                    ) {
                        error!("Failed to index BlobManager: {}", err);
                    }
                }
                BlobManagerEvent::Updated(_) => {
                    // No index update needed for extensions
                }
            }
        }
    }
}
```

#### File: `crates/walrus-service/src/node/storage/constants.rs`

Update column family names:

```rust
pub fn unified_storages_cf_name() -> &'static str {
    "unified_storages"
}

pub fn blob_manager_index_cf_name() -> &'static str {
    "blob_manager_index"
}

// Remove old:
// pub fn blob_managers_cf_name() -> &'static str
```

### Phase 3: API Updates

#### File: `crates/walrus-service/src/node/api/v2/blobs.rs` (or similar)

Update API to work with storage_id:

```rust
/// Get blob information by storage_id and blob_id.
#[get("/storage/{storage_id}/blob/{blob_id}")]
pub async fn get_blob_by_storage(
    storage_id: web::Path<(String,)>,
    blob_id: web::Path<(String,)>,
    state: web::Data<ApiState>,
) -> Result<HttpResponse, ApiError> {
    let storage_id = ObjectID::from_str(&storage_id.0)?;
    let blob_id = BlobId::from_str(&blob_id.0)?;

    // Check if storage exists
    let storage_info = state.storage.unified_storage_table
        .get(&storage_id)?
        .ok_or(ApiError::NotFound)?;

    // Get blob info
    let blob_info = state.storage.blob_v2_table
        .get(&storage_id, &blob_id)?
        .ok_or(ApiError::NotFound)?;

    Ok(HttpResponse::Ok().json(blob_info))
}

/// Optional: Get storage_id for a BlobManager (backward compatibility).
#[get("/blob-manager/{manager_id}/storage")]
pub async fn get_storage_for_manager(
    manager_id: web::Path<String>,
    state: web::Data<ApiState>,
) -> Result<HttpResponse, ApiError> {
    let manager_id = ObjectID::from_str(&manager_id.0)?;

    let storage_id = state.storage.blob_manager_index
        .as_ref()
        .and_then(|index| index.get_storage_id(&manager_id).ok().flatten())
        .ok_or(ApiError::NotFound)?;

    Ok(HttpResponse::Ok().json(json!({
        "storage_id": storage_id.to_string(),
    })))
}
```

## Testing Strategy

### Unit Tests

#### Test UnifiedStorage Events
```rust
#[test]
fn test_unified_storage_created_event() {
    // Create UnifiedStorage
    // Verify UnifiedStorageCreated event is emitted
    // Verify storage_id and end_epoch in event
}

#[test]
fn test_unified_storage_extended_event() {
    // Extend UnifiedStorage
    // Verify UnifiedStorageExtended event is emitted
    // Verify new_end_epoch in event
}
```

#### Test BlobManager Capabilities
```rust
#[test]
fn test_blob_manager_cap_permissions() {
    // Create BlobManager with admin cap
    // Verify admin can perform all actions
    // Create delegated cap with limited permissions
    // Verify delegated cap can only perform allowed actions
}

#[test]
fn test_invalid_capability() {
    // Try to use cap from different BlobManager
    // Verify EInvalidCapability error
}
```

#### Test Storage Node Event Processing
```rust
#[test]
fn test_process_unified_storage_created() {
    // Send UnifiedStorageCreated event
    // Verify entry in UnifiedStorageTable
    // Verify no BlobManager references
}

#[test]
fn test_blob_v2_populate_epochs() {
    // Create BlobV2Info with multiple storages
    // Call populate_epochs
    // Verify correct max/min epoch values
}
```

### Integration Tests

#### Scenario 1: Pure UnifiedStorage
```rust
#[test]
async fn test_pure_unified_storage_flow() {
    // 1. User creates UnifiedStorage directly
    // 2. User registers BlobV2 (has &mut UnifiedStorage)
    // 3. Storage node processes events
    // 4. Verify storage tracked without BlobManager
}
```

#### Scenario 2: BlobManager-owned UnifiedStorage
```rust
#[test]
async fn test_blob_manager_flow() {
    // 1. Create BlobManager (creates UnifiedStorage)
    // 2. Use BlobManagerCap to register blobs
    // 3. Storage node processes events
    // 4. Optional: verify BlobManagerIndex updated
}
```

#### Scenario 3: Certification by Anyone
```rust
#[test]
async fn test_blob_certification_by_third_party() {
    // 1. Owner registers BlobV2
    // 2. Different user certifies the blob
    // 3. Verify certification succeeds
    // 4. Storage node processes certification event
}
```

## Build and Deployment

### Move Contract Build
```bash
# Build Walrus package first (has UnifiedStorage)
cd contracts/walrus
sui move build

# Build BlobManager package (depends on Walrus)
cd ../blob_manager
sui move build

# Run Move tests
sui move test
```

### Rust Build
```bash
# Build with new features
cargo build --features unified-storage

# Run tests
cargo nextest run --features unified-storage

# Run specific test suites
cargo nextest run unified_storage
cargo nextest run blob_manager
cargo nextest run blob_v2
```

### Deployment Order
1. Deploy updated Walrus package with UnifiedStorage events
2. Deploy BlobManager package
3. Deploy updated storage nodes
4. Verify event processing with test transactions

## Monitoring and Validation

### Key Metrics
- UnifiedStorage creation rate
- BlobV2 registration rate per storage
- Storage utilization (registered size / capacity)
- Event processing lag

### Validation Checks
1. Every BlobV2 references valid UnifiedStorage
2. No orphaned UnifiedStorage entries
3. BlobManagerIndex consistency (if used)
4. Epoch boundaries respected

## Common Pitfalls to Avoid

1. **Don't forget UnifiedStorage events** - Without these, storage nodes can't track anything
2. **Don't mix capability systems** - UnifiedStorage uses ownership, BlobManager uses capabilities
3. **Don't assume BlobManager exists** - UnifiedStorage can exist standalone
4. **Don't validate ownership off-chain** - Trust Sui's on-chain validation

## API Changes Summary

### Deprecated (will remove later)
- `/blob-manager/{id}/blobs` - Use storage-based endpoints

### New Endpoints
- `/storage/{storage_id}/blob/{blob_id}` - Primary blob access
- `/storage/{storage_id}/blobs` - List blobs in storage
- `/blob-manager/{id}/storage` - Get storage for BlobManager (optional)

## Configuration Changes

### Storage Node Config
```yaml
# config.yaml
storage:
  enable_blob_manager_index: false  # Set true only if needed
  column_families:
    - unified_storages
    - blob_v2_info
    # blob_manager_index (optional)
```

## Rollback Plan

Since this is new functionality:
- No rollback needed for contracts (not deployed yet)
- Storage nodes can revert to previous version if issues found
- No data migration to reverse

## Success Criteria

1. ✅ UnifiedStorage events emitted and processed
2. ✅ Storage nodes track by storage_id only
3. ✅ BlobManager capability system works
4. ✅ BlobV2 registration/certification flows work
5. ✅ No BlobManager references in core storage logic
6. ✅ All tests pass
7. ✅ Documentation updated

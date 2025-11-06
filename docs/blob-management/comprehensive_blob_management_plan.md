# Comprehensive Blob Management Migration Plan

## Executive Summary

This document outlines the complete implementation plan for migrating Walrus from individual blob ownership to a managed blob system using BlobManager. The change fundamentally shifts blob lifecycle management from individual ownership to centralized management while maintaining backward compatibility.

## 1. Architecture Overview

### Current System
- **Individual Blobs**: Each blob owns its storage directly
- **Direct Ownership**: Users own blob objects after registration
- **Storage Management**: Per-blob storage allocation and tracking

### New System (ManagedBlob)
- **Centralized Management**: BlobManager owns all managed blobs from registration
- **Direct Ownership**: BlobManager creates and retains blobs throughout lifecycle
- **Pooled Storage**: Single storage pool shared across managed blobs
- **Simplified Access**: Clients use (BlobManagerID, BlobID) for uploads, never need ObjectIDs
- **Key Simplification**: Storage nodes authorize using BlobManagerID + BlobID, ObjectIDs exist internally but are never exposed

## 2. Contract Changes

### 2.1 New Contract Structures

#### ManagedBlob Object
```move
public struct ManagedBlob has key, store {
    id: UID,
    registered_epoch: u32,
    blob_id: u256,
    size: u64,
    encoding_type: u8,
    certified_epoch: option::Option<u32>,
    blob_manager_id: ID,  // Reference to BlobManager, not storage
    deletable: bool,
    is_quilt: bool,  // New: indicates if this blob is a quilt (composite blob)
    // Note: No end_epoch field - this is managed at the BlobManager level
}
```

#### BlobManager Updates
```move
public struct BlobManager has key, store {
    id: UID,
    storage: BlobStorage,  // Unified storage pool
    blob_stash: BlobStash,  // Simple table-based blob storage
    stats: BlobManagerStats,  // New: tracking metrics
}

enum BlobStash {
    simple_blob_stash(SimpleBlobStash),
}

public struct SimpleBlobStash has store {
    blob_ids: Table<u256, vector<ID>>,  // BlobID -> vector of ManagedBlob ObjectIDs
    blobs: Table<ID, ManagedBlob>,      // ObjectID -> ManagedBlob
}

```

### 2.2 New Entry Functions

#### Registration Flow
```move
public entry fun register_managed_blob(
    manager: &mut BlobManager,
    blob_id: u256,
    size: u64,
    encoding_type: u8,
    deletable: bool,
    is_quilt: bool,  // New parameter for quilt blobs
    ctx: &mut TxContext,
) {
    // 1. Check for duplicates in blob_stash (reject if certified blob exists)
    // 2. Verify storage pool has capacity (end_epoch from BlobManager's storage)
    // 3. Create ManagedBlob with is_quilt flag (owned by BlobManager)
    // 4. Store in both tables:
    //    - Add ObjectID to blob_ids[blob_id] vector
    //    - Add ManagedBlob to blobs[ObjectID]
    // 5. Emit ManagedBlobRegistered event (includes object_id for storage node tracking only)
    // Note: Blob stays with manager, client never receives ObjectID
    // Note: end_epoch is determined by BlobManager's storage pool, not per blob
}
```

#### Certification Flow
```move
public entry fun certify_managed_blob(
    manager: &mut BlobManager,
    blob_id: u256,
    deletable: bool,
    proofs: vector<vector<u8>>,
    ctx: &mut TxContext,
) {
    // 1. Look up uncertified blob in blob_stash by blob_id and deletable flag
    // 2. Validate proofs against committee
    // 3. Update ManagedBlob's certified_epoch field
    // 4. Keep blob in manager's tables (already there)
    // 5. Emit ManagedBlobCertified event
    // Note: Client only needs blob_id and deletable, not ObjectID
}
```

### 2.3 New Events

```move
// This should be almost the same as the existing one, without the expired epoch.
public struct ManagedBlobRegistered has copy, drop {
    epoch: u32,
    registered_epoch: u32,
    blob_manager_id: ID,
    blob_id: u256,
    blob_object_id: ID,
    size: u64,
    encoding_type: u8,
    deletable: bool,
    is_quilt: bool,  // New: indicates if this is a quilt blob
}

// Same as above.
public struct ManagedBlobCertified has copy, drop {
    epoch: u32,
    blob_manager_id: ID,
    blob_id: u256,
    blob_object_id: ID,
    deletable: bool,
    is_quilt: bool,  // New: indicates if this is a quilt blob
}

public struct ManagedBlobDeleted has copy, drop {
    epoch: u32,
    blob_manager_id: ID,
    blob_id: u256,
    blob_object_id: ID,
}

public struct BlobManagerExtended has copy, drop {
    blob_manager_id: ID,
    current_end_epoch: u32,
    new_end_epoch: u32,
}
```

## 3. Storage Node Changes

### 3.1 Database Schema Updates

#### Updated ValidBlobInfoV1
```rust
pub struct ValidBlobInfoV2 {
    // Existing fields
    pub is_metadata_stored: bool,
    pub count_deletable_total: u32,
    pub count_deletable_certified: u32,
    pub permanent_total: Option<PermanentBlobInfoV1>,
    pub permanent_certified: Option<PermanentBlobInfoV1>,
    pub initial_certified_epoch: Option<Epoch>,
    pub latest_seen_deletable_registered_end_epoch: Option<Epoch>,
    pub latest_seen_deletable_certified_end_epoch: Option<Epoch>,

    // New fields for managed blobs
    pub blob_managers: Vec<ObjectID>,  // List of managing BlobManagers
    pub max_managed_end_epoch: u32,
}
```

#### Updated PerObjectBlobInfoV1
```rust
pub struct PerObjectBlobInfoV2 {
    pub blob_id: BlobId,
    pub registered_epoch: Epoch,
    pub certified_epoch: Option<Epoch>,
    pub end_epoch: Epoch,
    pub deletable: bool,
    pub event: EventID,
    pub deleted: bool,

    pub blob_manager_id: Option<ObjectID>,  // For managed blobs
    pub is_quilt: bool,  // New: indicates if this is a quilt blob
}

```

### 3.2 Event Processing Updates

```rust
impl EventProcessor {
    fn process_managed_blob_registered(&mut self, event: ManagedBlobRegistered) {
        // 1. Create PerObjectBlobInfoV2 with blob_type = Managed
        // 2. Update ValidBlobInfoV2 to add blob_manager_id
        // 3. Increment managed_blob_count
        // 4. Track in both per_object and aggregate tables
    }

    fn process_managed_blob_certified(&mut self, event: ManagedBlobCertified) {
        // 1. Update per_object info certified_epoch
        // 2. Increment managed_certified_count
        // 3. Update aggregate blob info
    }

    fn process_managed_blob_deleted(&mut self, event: ManagedBlobDeleted) {
        // 1. Mark per_object as deleted
        // 2. Decrement counters
        // 3. Schedule cleanup if all variants deleted
    }

    fn process_managed_blob_extended(&mut self, event: ManagedBlobDeleted) {
    }
}
```

### 3.3 ServiceState Trait Extensions

The existing `ServiceState` trait needs minimal modifications since most functionality remains the same. The key changes are in how blob persistence is determined:

```rust
/// Extended BlobPersistenceType to support managed blobs
pub enum BlobPersistenceType {
    Permanent,
    Deletable { object_id: ObjectID },
    // New variant for managed blobs
    Managed {
        blob_manager_id: ObjectID,
        deletable: bool,
    },
}

// No changes needed to store_sliver or retrieve_sliver signatures
// The blob_manager context is handled through the blob_info lookup

impl ServiceState {
    /// Compute storage confirmation - updated to handle managed blobs
    fn compute_storage_confirmation(
        &self,
        blob_id: &BlobId,
        blob_persistence_type: &BlobPersistenceType,
    ) -> impl Future<Output = Result<StorageConfirmation, ComputeStorageConfirmationError>> + Send {
        match blob_persistence_type {
            BlobPersistenceType::Managed { blob_manager_id, deletable } => {
                // 1. Verify blob is managed by this manager via blob_info
                // 2. Check all slivers are stored
                // 3. Generate confirmation with manager context
                self.compute_managed_blob_confirmation(blob_id, blob_manager_id, *deletable).await
            }
            // Existing cases remain unchanged
            _ => self.compute_regular_confirmation(blob_id, blob_persistence_type).await
        }
    }
}
```

### 3.4 API Route Modifications

The storage node HTTP API routes remain largely unchanged. The main additions are:

```rust
// New endpoint for managed blob confirmations
pub const MANAGED_BLOB_CONFIRMATION_ENDPOINT: &str =
    "/v1/blobs/{blob_id}/confirmation/managed/{blob_manager_id}/{deletable}";

// Route handler for managed blob confirmations
pub async fn get_managed_blob_confirmation<S: SyncServiceState>(
    State(state): State<RestApiState<S>>,
    Path((blob_id, blob_manager_id, deletable)): Path<(BlobIdString, ObjectIdSchema, bool)>,
) -> Result<ApiSuccess<StorageConfirmation>, ComputeStorageConfirmationError> {
    let confirmation = state
        .service
        .compute_storage_confirmation(
            &blob_id.0,
            &BlobPersistenceType::Managed {
                blob_manager_id: blob_manager_id.into(),
                deletable,
            },
        )
        .await?;

    Ok(ApiSuccess::ok(confirmation))
}

// Existing store_sliver endpoint works as-is
// The storage node determines if a blob is managed by checking blob_info
// No API changes needed for sliver upload/download
```

## 4. Client-Side Changes

### 4.1 New Transaction Builders

```rust
impl TransactionBuilder {
    pub async fn register_managed_blob(
        &self,
        blob_manager_id: ObjectID,
        blob_id: BlobId,
        size: u64,
        encoding_type: u8,
        end_epoch: u32,
        deletable: bool,
        is_quilt: bool,  // New: quilt support
    ) -> Result<Transaction> {
        // Build registration transaction with is_quilt flag
    }

    pub async fn certify_managed_blob(
        &self,
        blob_manager_id: ObjectID,
        blob_id: BlobId,
        deletable: bool,
        proofs: Vec<Vec<u8>>,
    ) -> Result<Transaction> {
        // Build certification transaction
    }
}
```

### 4.2 Upload Flow Updates

```rust
impl WalrusClient {
    pub async fn upload_to_blob_manager(
        &self,
        blob_manager_id: ObjectID,
        data: &[u8],
        epochs: u32,
        deletable: bool,
        is_quilt: bool,  // New: quilt support
    ) -> Result<BlobId> {
        // 1. Encode data
        // 2. Call register_managed_blob with is_quilt flag
        // 3. Upload slivers with blob_manager_id
        // 4. Collect certificates
        // 5. Call certify_managed_blob
        // 6. Return blob_id
    }

    // New method specifically for quilt uploads
    pub async fn upload_quilt_to_blob_manager(
        &self,
        blob_manager_id: ObjectID,
        quilt_pieces: Vec<BlobId>,  // References to existing blobs
        epochs: u32,
        deletable: bool,
    ) -> Result<BlobId> {
        // 1. Create quilt manifest from piece BlobIds
        // 2. Encode manifest as blob data
        // 3. Call upload_to_blob_manager with is_quilt=true
    }
}
```

### 4.3 API Response Updates

```rust
pub struct BlobInfo {
    pub blob_id: BlobId,
    pub object_id: ObjectID,
    pub blob_manager_id: Option<ObjectID>,  // New field
    pub is_managed: bool,  // New field
    pub is_quilt: bool,  // New: indicates if this is a quilt
    pub status: BlobStatus,
    // ... other fields
}
```

## 5. Migration Strategy

### 5.1 Compatibility Mode

**Phase 1: Dual Support (3-6 months)**
- Both Blob and ManagedBlob systems active
- New uploads default to ManagedBlob
- Existing Blobs continue to work
- Storage nodes accept both types

**Phase 2: Migration Encouragement (3-6 months)**
- Deprecation notices for Blob
- Migration tools provided
- Cost incentives for ManagedBlob

**Phase 3: Sunset (Final)**
- Blob creation disabled
- Existing Blobs continue until expiry
- Full transition to ManagedBlob

### 5.2 Migration Tools

```move
// Contract migration helper
public entry fun migrate_blob_to_manager(
    manager: &mut BlobManager,
    blob: Blob,
    ctx: &mut TxContext,
) {
    // 1. Extract blob data
    // 2. Create equivalent ManagedBlob
    // 3. Transfer storage to manager
    // 4. Delete original Blob
    // 5. Emit migration event
}
```

### 5.3 Backward Compatibility

- Event structure remains compatible
- Storage nodes auto-detect blob type
- Client SDK supports both APIs
- Gradual deprecation path

## 6. Security Considerations

### 6.1 Attack Vectors

**DoS via Blob Spam**
- **Risk**: Attacker creates many managed blobs to exhaust storage
- **Mitigation**:
  - Limit managed blobs per blob_id (e.g., 1000)
  - Rate limiting per account
  - Storage deposit requirements

**Storage Exhaustion**
- **Risk**: BlobManager runs out of pooled storage
- **Mitigation**:
  - Reserve buffer for operations
  - Dynamic storage expansion
  - Quota enforcement

**Unauthorized Access**
- **Risk**: Access to blobs not owned
- **Mitigation**:
  - Strict ownership checks
  - Phase-based access control
  - Capability-based permissions

### 6.2 Safety Limits

```rust
const MAX_MANAGED_BLOBS_PER_ID: usize = 1000;
const MAX_BLOB_MANAGERS_PER_BLOB: usize = 10;
const MIN_STORAGE_RESERVE: u64 = 1_000_000;  // 1MB minimum
const MAX_BLOB_SIZE: u64 = 10_737_418_240;  // 10GB maximum
```

## 7. Testing Strategy

### 7.1 Unit Tests

**Contract Tests**
- Registration deduplication
- Certification validation
- Storage allocation/deallocation
- Event emission correctness

**Storage Node Tests**
- Event processing for all types
- Database migration
- Sliver acceptance logic
- Cleanup processes

### 7.2 Integration Tests

**End-to-End Scenarios**
1. Upload via BlobManager
2. Mixed Blob/ManagedBlob operations
3. Migration from Blob to ManagedBlob
4. Expiry and cleanup
5. Storage exhaustion handling

### 7.3 Performance Tests

**Benchmarks**
- Throughput: blobs/second
- Latency: registration to certification
- Storage efficiency
- Database query performance
- Memory usage with large blob_managers vectors

### 7.4 Chaos Testing

- Network partitions during certification
- Storage node failures
- Concurrent operations on same blob_id
- Clock skew handling

## 8. Rollout Plan

### Phase 1: Development (Weeks 1-4)
- [ ] Implement contract changes
- [ ] Update storage node event processing
- [ ] Modify client SDK
- [ ] Write comprehensive tests

### Phase 2: Testing (Weeks 5-6)
- [ ] Deploy to testnet
- [ ] Run integration tests
- [ ] Performance benchmarking
- [ ] Security audit

### Phase 3: Staged Rollout (Weeks 7-10)
- [ ] Deploy contracts to mainnet (disabled)
- [ ] Update storage nodes (backward compatible)
- [ ] Release client SDK beta
- [ ] Enable for select partners

### Phase 4: General Availability (Week 11+)
- [ ] Enable for all users
- [ ] Monitor metrics
- [ ] Gather feedback
- [ ] Iterate on improvements

## 9. Monitoring and Metrics

### Key Metrics
```rust
pub struct BlobManagerMetrics {
    // Usage
    total_managed_blobs: Counter,
    active_managed_blobs: Gauge,
    storage_utilization: Gauge,

    // Performance
    registration_latency: Histogram,
    certification_latency: Histogram,
    sliver_upload_latency: Histogram,

    // Errors
    registration_failures: Counter,
    certification_failures: Counter,
    storage_exhaustion_events: Counter,

    // Distribution
    blobs_per_manager: Histogram,
    managers_per_blob: Histogram,
    blob_size_distribution: Histogram,
}
```

### Alerting Thresholds
- Storage utilization > 90%
- Registration failures > 1%
- Certification latency > 10s
- Blob managers vector > 100 entries

## 10. Documentation Updates

### Developer Documentation
- API reference for ManagedBlob
- Migration guide from Blob
- Best practices for BlobManager
- Performance tuning guide

### User Documentation
- How managed blobs work
- Benefits over individual blobs
- Usage examples
- FAQ

## 11. Risk Assessment

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| Migration bugs | High | Medium | Extensive testing, staged rollout |
| Performance degradation | Medium | Low | Performance testing, optimization |
| Storage exhaustion | High | Low | Limits, monitoring, reserves |
| Backward compatibility issues | High | Low | Dual support period |
| User adoption | Medium | Medium | Clear benefits, smooth UX |

## 12. Success Criteria

- ✅ All tests passing
- ✅ < 1% error rate in production
- ✅ Performance within 10% of current system
- ✅ 50% of new uploads use ManagedBlob within 3 months
- ✅ No critical security vulnerabilities
- ✅ Positive user feedback

## Appendix A: Configuration Parameters

```toml
[blob_manager]
max_blobs_per_id = 1000
max_managers_per_blob = 10
default_storage_pool_size = "100GB"
storage_reserve_percentage = 10
enable_auto_expansion = true
max_blob_size = "10GB"
cleanup_interval_seconds = 3600
```

## Appendix B: Database Migration Scripts

```sql
-- Add new columns to blob_info table
ALTER TABLE blob_info ADD COLUMN blob_managers BLOB;
ALTER TABLE blob_info ADD COLUMN managed_blob_count INTEGER DEFAULT 0;
ALTER TABLE blob_info ADD COLUMN managed_certified_count INTEGER DEFAULT 0;

-- Add new columns to per_object_blob_info
ALTER TABLE per_object_blob_info ADD COLUMN blob_type INTEGER DEFAULT 0;
ALTER TABLE per_object_blob_info ADD COLUMN blob_manager_id BLOB;

-- Create indexes for efficient queries
CREATE INDEX idx_blob_manager_id ON per_object_blob_info(blob_manager_id);
CREATE INDEX idx_blob_type ON per_object_blob_info(blob_type);
```

## Appendix C: Event Flow Diagrams

### Registration Flow
```
Client -> BlobManager: register_managed_blob(blob_id, size, ...)
BlobManager -> Storage: allocate_storage()
BlobManager -> ManagedBlob: create() [owned by BlobManager]
BlobManager -> BlobStash: store in both tables
BlobManager -> Event: ManagedBlobRegistered
BlobManager -> Client: success (no object returned)
```

### Certification Flow
```
Client -> StorageNodes: upload_slivers(blob_id, manager_id)
StorageNodes -> Client: return certificates
Client -> BlobManager: certify_managed_blob(blob_id, deletable, certificates)
BlobManager -> BlobStash: find blob by blob_id + deletable
BlobManager -> Validators: verify_signatures()
BlobManager -> ManagedBlob: set_certified()
BlobManager -> Event: ManagedBlobCertified
Client: receives success confirmation
```

---

This comprehensive plan provides a complete roadmap for implementing the managed blob system while maintaining backward compatibility and ensuring a smooth migration path.
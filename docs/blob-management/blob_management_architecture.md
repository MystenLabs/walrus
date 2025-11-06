# Blob Management Architecture Design

## Overview

This document describes the architecture for migrating Walrus from individual blob ownership to centralized blob management through BlobManager, where blobs are owned by the manager from registration through their entire lifecycle.

**Key Design Simplification**: Storage nodes don't need ObjectIDs for blob operations. They only need (BlobManagerID, BlobID) pairs for authorization and tracking. This eliminates the need for clients to know or handle ObjectIDs, greatly simplifying the system.

## 1. Core Architecture Changes

### Current System
- **Individual Ownership**: Each Blob owns its storage directly
- **Direct User Control**: Users own Blob objects after registration
- **Per-Blob Storage**: Each blob has dedicated storage allocation

### New ManagedBlob System
- **Centralized Ownership**: BlobManager owns all blobs from registration
- **Pooled Storage**: Single storage pool shared across all managed blobs
- **Simplified Access**: Clients use (BlobManagerID, BlobID) for uploads, (BlobManagerID, BlobID, deletable) for certification
- **No ObjectID Exposure**: ObjectIDs exist internally but are never exposed to clients

## 2. Contract Architecture

### 2.1 Core Data Structures

```move
// Blob owned and managed by BlobManager
public struct ManagedBlob has key, store {
    id: UID,
    registered_epoch: u32,
    blob_id: u256,
    size: u64,
    encoding_type: u8,
    certified_epoch: option::Option<u32>,
    blob_manager_id: ID,  // Reference to BlobManager
    deletable: bool,
    is_quilt: bool,  // Marks if this is a composite blob
    // Note: No end_epoch - managed at BlobManager level
}

// Central blob management structure
public struct BlobManager has key, store {
    id: UID,
    storage: BlobStorage,  // Unified storage pool
    blob_stash: BlobStash,  // Enum for different stash strategies
    stats: BlobManagerStats,
}

// Enum to support different blob storage strategies
enum BlobStash {
    SimpleBlobStash(SimpleBlobStash),
    // Future: other strategies can be added here
}

// Storage and indexing for blobs
public struct SimpleBlobStash has store {
    blob_ids: Table<u256, vector<ID>>,  // BlobID -> ManagedBlob ObjectIDs
    blobs: Table<ID, ManagedBlob>,      // ObjectID -> ManagedBlob
}
```

### 2.2 Key Operations

#### Registration
```move
public entry fun register_managed_blob(
    manager: &mut BlobManager,
    blob_id: u256,
    size: u64,
    encoding_type: u8,
    deletable: bool,
    is_quilt: bool,
    ctx: &mut TxContext,
    // Note: end_epoch determined by BlobManager's storage
```
- Creates ManagedBlob owned by BlobManager
- Stores in both stash tables for efficient lookup
- Client receives success/failure, not the object

#### Certification
```move
public entry fun certify_managed_blob(
    manager: &mut BlobManager,
    blob_id: u256,
    deletable: bool,
    proofs: vector<vector<u8>>,
    ctx: &mut TxContext,
)
```
- Finds blob using blob_id + deletable flag
- Updates certification status in-place
- No object transfer needed (already owned by manager)

### 2.3 Event Architecture

```move
// Simplified events without end_epoch (managed by BlobManager)
public struct ManagedBlobRegistered has copy, drop {
    epoch: u32,
    registered_epoch: u32,
    blob_manager_id: ID,
    blob_id: u256,
    blob_object_id: ID,  // For storage node tracking
    size: u64,
    encoding_type: u8,
    deletable: bool,
}

public struct ManagedBlobCertified has copy, drop {
    epoch: u32,
    blob_manager_id: ID,
    blob_id: u256,
    blob_object_id: ID,
    deletable: bool,
}

public struct ManagedBlobDeleted has copy, drop {
    epoch: u32,
    blob_manager_id: ID,
    blob_id: u256,
    blob_object_id: ID,
}

// Event for extending BlobManager storage
public struct BlobManagerExtended has copy, drop {
    epoch: u32,
    blob_manager_id: ID,
    additional_storage: u64,
    new_total_storage: u64,
}
```

## 3. Storage Node Architecture

### 3.1 Database Schema

#### Aggregate Blob Information
```rust
pub struct ValidBlobInfoV2 {
    // Existing tracking
    pub is_metadata_stored: bool,
    pub count_deletable_total: u32,
    pub count_deletable_certified: u32,
    pub permanent_total: Option<PermanentBlobInfoV1>,
    pub permanent_certified: Option<PermanentBlobInfoV1>,

    // Epoch tracking
    pub initial_certified_epoch: Option<Epoch>,
    pub latest_seen_deletable_registered_end_epoch: Option<Epoch>,
    pub latest_seen_deletable_certified_end_epoch: Option<Epoch>,

    // New: Managed blob tracking
    pub blob_managers: Vec<ObjectID>,  // All managers for this blob
    pub managed_blob_count: u32,
    pub managed_certified_count: u32,
}
```

#### Per-Object Information
```rust
pub struct PerObjectBlobInfoV2 {
    pub blob_id: BlobId,
    pub registered_epoch: Epoch,
    pub certified_epoch: Option<Epoch>,
    pub end_epoch: Epoch,
    pub deletable: bool,
    pub event: EventID,
    pub deleted: bool,

    // New fields
    pub blob_type: BlobType,  // Individual | Managed
    pub blob_manager_id: Option<ObjectID>,
}
```

### 3.2 Sliver Upload Authorization (No ObjectID Required)

```rust
impl StorageNode {
    async fn handle_sliver_upload(&self, request: SliverUploadRequest) {
        // Storage nodes only need BlobID + BlobManagerID for authorization
        // No ObjectID required - this is the key simplification

        let blob_info = self.get_blob_info(request.blob_id)?;

        match request.blob_manager_id {
            Some(manager_id) => {
                // Managed blob: verify manager is authorized
                // Storage node tracks which BlobManagers manage this BlobID
                ensure!(blob_info.blob_managers.contains(&manager_id));
            }
            None => {
                // Individual blob: verify not managed
                ensure!(blob_info.blob_managers.is_empty());
            }
        }

        // Process sliver - no ObjectID needed anywhere
        self.store_sliver(request.blob_id, request.sliver);
    }
}
```

### 3.3 Event Processing

```rust
impl EventProcessor {
    fn process_managed_blob_registered(&mut self, event: ManagedBlobRegistered) {
        // Create per-object entry
        let per_object = PerObjectBlobInfoV2 {
            blob_id: event.blob_id,
            blob_type: BlobType::Managed,
            blob_manager_id: Some(event.blob_manager_id),
            // ... other fields
        };

        // Update aggregate info
        let mut blob_info = self.get_or_create_blob_info(event.blob_id);
        blob_info.blob_managers.push(event.blob_manager_id);
        blob_info.managed_blob_count += 1;
    }
}
```

## 4. Client Architecture

### 4.1 Upload Flow

```rust
impl WalrusClient {
    pub async fn upload_to_blob_manager(
        &self,
        blob_manager_id: ObjectID,
        data: &[u8],
        epochs: u32,
        deletable: bool,
    ) -> Result<BlobId> {
        // 1. Encode data into blob_id and shards
        let (blob_id, shards) = self.encode_data(data)?;

        // 2. Register with BlobManager (no object returned)
        self.register_managed_blob(
            blob_manager_id,
            blob_id,
            data.len(),
            encoding_type,
            epochs,
            deletable,
        ).await?;

        // 3. Upload shards with manager context
        let certificates = self.upload_shards_with_manager(
            blob_id,
            blob_manager_id,
            shards,
        ).await?;

        // 4. Certify using blob_id (no object needed)
        self.certify_managed_blob(
            blob_manager_id,
            blob_id,
            deletable,
            certificates,
        ).await?;

        Ok(blob_id)
    }
}
```

### 4.2 API Data Structures

```rust
pub struct BlobMetadata {
    pub blob_id: BlobId,
    pub blob_manager_id: Option<ObjectID>,
    pub is_managed: bool,
    pub status: BlobStatus,
    // Note: ObjectID is never exposed for managed blobs
}
```

## 5. Data Flow Diagrams

### Registration Flow
```
Client                BlobManager           Storage              Event Bus
  |                        |                    |                     |
  |--register_blob(id)---->|                    |                     |
  |                        |--allocate--------->|                     |
  |                        |<-storage_ref-------|                     |
  |                        |                    |                     |
  |                        |--create_blob------>|                     |
  |                        |  (manager owned)   |                     |
  |                        |                    |                     |
  |                        |--store_in_stash--->|                     |
  |                        |                    |                     |
  |                        |------------------emit_event------------>|
  |<------success----------|                    |                     |
```

### Sliver Upload Flow
```
Client            StorageNode         BlobManager          Event Bus
  |                    |                   |                   |
  |--upload_sliver---->|                   |                   |
  | (blob_id,          |                   |                   |
  |  manager_id,       |                   |                   |
  |  data)             |                   |                   |
  |                    |                   |                   |
  |                    |--check_auth------->|                   |
  |                    |<-blob_info---------|                   |
  |                    |                   |                   |
  |                    |--validate-------->|                   |
  |                    |                   |                   |
  |<--certificate------|                   |                   |
```

### Certification Flow
```
Client              BlobManager           Validators         Event Bus
  |                      |                     |                 |
  |--certify_blob------->|                     |                 |
  | (blob_id,            |                     |                 |
  |  deletable,          |                     |                 |
  |  certificates)       |                     |                 |
  |                      |                     |                 |
  |                      |--find_blob--------->|                 |
  |                      | (by id+deletable)   |                 |
  |                      |                     |                 |
  |                      |--verify_sigs------->|                 |
  |                      |<-validated----------|                 |
  |                      |                     |                 |
  |                      |--update_blob------->|                 |
  |                      | (set certified)     |                 |
  |                      |                     |                 |
  |                      |---------------emit_event------------>|
  |<----success----------|                     |                 |
```

## 6. Key Architecture Decisions

### 6.1 No ObjectID Exposure for Managed Blobs
- **Decision**: Storage nodes and clients operate using (BlobManagerID, BlobID) pairs only
- **Rationale**: Storage nodes track blobs by BlobID, not ObjectID. Adding ObjectID provides no value and complicates the client interface
- **Impact**:
  - Clients never see or handle ObjectIDs for managed blobs
  - Storage nodes authorize based on BlobManagerID association with BlobID
  - Dramatically simplifies client implementation and API

### 6.2 Single-Phase Ownership
- **Decision**: BlobManager owns blobs immediately upon registration
- **Rationale**: Simplifies lifecycle, eliminates complex ownership transfers
- **Impact**: Client never holds blob objects, uses IDs for all operations

### 6.2 Dual-Table Storage in BlobStash
- **Decision**: Maintain both blob_ids->ObjectIDs and ObjectID->Blob mappings
- **Rationale**: Enables efficient lookup by blob_id while preserving object relationships
- **Impact**: Slight storage overhead for indexing, significant query performance improvement

### 6.3 Manager-Based Authorization
- **Decision**: Storage nodes authorize uploads based on blob_manager_id
- **Rationale**: Centralized control over blob lifecycle and access
- **Impact**: Storage nodes must track manager associations in blob_info

### 6.4 Backward Compatibility Through BlobType
- **Decision**: Add BlobType enum to distinguish Individual vs Managed blobs
- **Rationale**: Allows gradual migration without breaking existing blobs
- **Impact**: Storage nodes handle both types transparently

## 7. Security Considerations

### 7.1 Resource Limits
```rust
const MAX_MANAGED_BLOBS_PER_ID: usize = 1000;  // Per blob_id
const MAX_BLOB_MANAGERS_PER_BLOB: usize = 10;  // Cross-manager limit
const MAX_BLOB_SIZE: u64 = 10_737_418_240;     // 10GB
```

### 7.2 Attack Mitigation
- **Blob Spam**: Limit blobs per blob_id to prevent exhaustion
- **Storage DoS**: Reserve buffer in pool, enforce quotas
- **Manager Compromise**: Blobs bound to specific manager, no cross-access

## 8. Database Versioning Strategy (No Migration Required)

### Lazy Upgrade Approach
V1 and V2 structures coexist in the same DBMap. V1 upgrades to V2 only when a managed blob is registered for that blob_id:

```rust
// Versioned enum for blob info - both versions in same DBMap
#[derive(Serialize, Deserialize)]
pub enum BlobInfo {
    V1(BlobInfoV1),
    V2(BlobInfoV2),
}

impl BlobInfo {
    // Upgrade V1 to V2 only when adding managed storage
    fn ensure_v2_for_managed_blob(
        &mut self,
        blob_manager_id: ObjectID,
    ) -> &mut BlobInfoV2 {
        match self {
            BlobInfo::V1(v1) => {
                // Convert V1 to V2 on first managed blob
                let v2 = BlobInfoV2 {
                    // Copy all V1 fields
                    is_metadata_stored: v1.is_metadata_stored,
                    count_deletable_total: v1.count_deletable_total,
                    count_deletable_certified: v1.count_deletable_certified,
                    permanent_total: v1.permanent_total.clone(),
                    permanent_certified: v1.permanent_certified.clone(),
                    initial_certified_epoch: v1.initial_certified_epoch,
                    latest_seen_deletable_registered_end_epoch:
                        v1.latest_seen_deletable_registered_end_epoch,
                    latest_seen_deletable_certified_end_epoch:
                        v1.latest_seen_deletable_certified_end_epoch,

                    // Initialize new V2 fields
                    blob_managers: vec![blob_manager_id],
                    managed_blob_count: 1,
                    managed_certified_count: 0,
                };
                *self = BlobInfo::V2(v2);

                match self {
                    BlobInfo::V2(v2) => v2,
                    _ => unreachable!(),
                }
            }
            BlobInfo::V2(v2) => {
                // Already V2, just add manager if not present
                if !v2.blob_managers.contains(&blob_manager_id) {
                    v2.blob_managers.push(blob_manager_id);
                }
                v2
            }
        }
    }
}
```

### Storage Implementation
```rust
// Single DBMap handles both versions through enum
aggregate_blob_info: DBMap<BlobId, BlobInfo>,  // Can be V1 or V2

// Processing events - automatic upgrade when needed
impl EventProcessor {
    fn process_managed_blob_registered(&mut self, event: ManagedBlobRegistered) {
        let mut blob_info = self.aggregate_blob_info
            .get(&event.blob_id)
            .unwrap_or(BlobInfo::V2(BlobInfoV2::default()));

        // Automatically upgrades V1 to V2 if needed
        let v2_info = blob_info.ensure_v2_for_managed_blob(event.blob_manager_id);
        v2_info.managed_blob_count += 1;

        self.aggregate_blob_info.insert(&event.blob_id, &blob_info);
    }

    fn process_regular_blob_registered(&mut self, event: BlobRegistered) {
        // Regular blobs can stay as V1
        let blob_info = self.aggregate_blob_info
            .get(&event.blob_id)
            .unwrap_or(BlobInfo::V1(BlobInfoV1::default()));

        // Only upgrade if already V2 (has managed blobs)
        match blob_info {
            BlobInfo::V1(mut v1) => {
                v1.count_deletable_total += 1;
                self.aggregate_blob_info.insert(&event.blob_id, &BlobInfo::V1(v1));
            }
            BlobInfo::V2(mut v2) => {
                v2.count_deletable_total += 1;
                self.aggregate_blob_info.insert(&event.blob_id, &BlobInfo::V2(v2));
            }
        }
    }
}
```

### Benefits of This Approach
- **No migration needed**: V1 and V2 coexist in same database
- **Lazy conversion**: V1 converts to V2 only when managed storage added
- **Memory efficient**: V1 blobs don't carry V2 overhead
- **Backward compatible**: Existing code continues working with V1

## 9. Performance Optimizations

### 9.1 Caching Strategy
- Cache frequently accessed blob_manager associations
- LRU cache for blob_info lookups
- Batch event processing for efficiency

### 9.2 Query Optimization
- Secondary indexes for manager->blob lookups
- Bloom filters for existence checks
- Parallel sliver upload processing

## 10. Future Extensibility

### Potential Extensions
- **Multi-Manager Support**: Blobs managed by multiple managers
- **Hierarchical Management**: Manager delegation chains
- **Storage Quotas**: Per-manager storage limits
- **Access Control Lists**: Fine-grained permissions
- **Storage Classes**: Different storage tiers (hot/cold)

### Interface Stability
- Core BlobManager interface designed for extension
- Event structure supports additional fields
- Database schema versioned for evolution
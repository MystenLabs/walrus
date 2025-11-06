# Blob Management Implementation Status (Updated)

## Completed Components

### 1. Contract Layer (Move)

#### ManagedBlob Module (`system/managed_blob.move`)
✅ Created new ManagedBlob struct with:
- Single-phase ownership (BlobManager owns from registration)
- `blob_manager_id` reference instead of direct storage ownership
- **No `end_epoch` field** - storage validity is managed at BlobManager level
- `is_quilt` field for composite blob support
- No direct storage access (managed by BlobManager)

✅ Key functions:
- `new()` - Creates managed blob owned by BlobManager (no end_epoch parameter)
- `certify_with_certified_msg()` - Certifies using message
- `assert_certified()` - Checks certification (no expiration check here)
- Removed `extend()` function - extensions happen at BlobManager level
- Accessor functions for all fields
- Metadata support functions

#### Events Module (`system/events.move`)
✅ Updated event types:
- `ManagedBlobRegistered` - No end_epoch field
- `ManagedBlobCertified` - No end_epoch field
- `ManagedBlobDeleted` - Emitted on deletion
- All events include `is_quilt` field

#### ManagedBlobStash Module (`managed_blob_stash.move`)
✅ Created storage implementation for managed blobs:
- `SimpleManagedBlobStash` - Single-phase ownership storage
- Dual-table structure for efficient lookups:
  - `blob_id_to_objects`: Maps blob_id → vector<ObjectID>
  - `blobs_by_object_id`: Maps ObjectID → ManagedBlob
- Support for multiple variants (deletable/permanent)
- Functions for finding, storing, and certifying blobs

#### BlobManager Module (`blobmanager.move`)
✅ Extended with managed blob support:
- Added `managed_blob_stash` field for single-phase ownership
- `register_managed_blob()` - No end_epoch parameter, uses BlobManager's storage validity
- `certify_managed_blob()` - Certifies using blob_id + deletable (no ObjectID needed)
- Query functions for managed blobs
- Uses test message construction (TODO: production message handling)

## Key Architecture Decisions Implemented

### 1. Single-Phase Ownership
- BlobManager owns ManagedBlobs immediately upon registration
- No ownership transfer required during certification
- Clients never hold ManagedBlob objects

### 2. No ObjectID Exposure
- Clients use (BlobManagerID, BlobID) pairs only
- Storage nodes authorize based on these pairs
- ObjectIDs exist internally but aren't exposed to clients
- Dramatically simplifies client implementation

### 3. Storage Architecture
- BlobManager has unified storage pool with a single end_epoch
- **All ManagedBlobs under a BlobManager share the same storage validity period**
- ManagedBlobs reference BlobManager, not storage directly
- Storage accounting handled at BlobManager level
- **No per-blob end_epoch tracking** - simplifies management

### 4. Quilt Support
- `is_quilt` field added throughout the system
- Allows marking composite blobs
- Propagated through events for tracking

## Important Design Change: No Per-Blob end_epoch

### Rationale
Since ManagedBlobs don't own storage directly and all blobs under a BlobManager share the same unified storage pool, there's no need for per-blob end_epoch tracking. The storage validity period is determined by the BlobManager's storage pool.

### Benefits
1. **Simplicity**: One end_epoch to manage (at BlobManager level) instead of many
2. **Consistency**: All blobs in a manager have the same validity period
3. **Efficiency**: Less storage overhead per blob
4. **Easier Extensions**: Extending storage extends all blobs at once

### Implementation
- ManagedBlob struct has no `end_epoch` field
- BlobManager's storage pool determines validity for all its blobs
- Storage extensions happen at BlobManager level, affecting all blobs
- Events don't include end_epoch for managed blobs

## Compilation Status
✅ All contracts compile successfully
- Minor warnings only (unused imports, entry functions)
- No structural errors

## Next Steps

### 1. Storage Node Implementation
- [ ] Update BlobInfo structures for V1→V2 lazy migration
- [ ] Implement event processing for ManagedBlob events (without end_epoch)
- [ ] Add (BlobManagerID, BlobID) authorization logic
- [ ] Update sliver upload handling

### 2. Client SDK Updates
- [ ] Implement upload_to_blob_manager function
- [ ] Update API to use (BlobManagerID, BlobID) pairs
- [ ] Remove ObjectID handling for managed blobs
- [ ] Add quilt blob support
- [ ] Remove end_epoch from managed blob operations

### 3. Testing
- [ ] Unit tests for ManagedBlob operations
- [ ] Integration tests for registration/certification flow
- [ ] Migration tests for V1→V2 conversion
- [ ] Quilt blob tests
- [ ] BlobManager storage extension tests

### 4. Production Improvements
- [ ] Replace test message construction with proper certified messages
- [ ] Add proper payment processing
- [ ] Implement storage quota management at BlobManager level
- [ ] Add comprehensive error handling

## Known TODOs in Code
1. `blobmanager.move:426` - Production message construction needed
2. `blobmanager.move:387` - Payment processing integration
3. `blobmanager.move:343` - Storage accounting implementation

## Architecture Benefits Achieved
1. **Simplicity**: No ObjectID exposure, no per-blob end_epoch
2. **Efficiency**: Single-phase ownership, unified storage management
3. **Scalability**: Lazy V1→V2 migration, shared storage pools
4. **Flexibility**: Quilt support, BlobManager-level extensions
5. **Compatibility**: Existing blobs continue working unchanged
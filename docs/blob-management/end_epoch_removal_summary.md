# Summary: Removal of end_epoch from ManagedBlob and Accounting-Based Storage

## Key Changes Made

### 1. ManagedBlob Structure Update

**Removed**: `end_epoch` field from ManagedBlob
**Rationale**: Since ManagedBlobs don't own storage directly and all blobs under a BlobManager share the same unified storage pool, there's no need for per-blob end_epoch tracking. The storage validity period is determined by the BlobManager's storage pool.

**Updated Structure**:
```move
public struct ManagedBlob has key, store {
    id: UID,
    registered_epoch: u32,
    blob_id: u256,
    size: u64,
    encoding_type: u8,
    certified_epoch: option::Option<u32>,
    blob_manager_id: ID,  // Reference to BlobManager
    deletable: bool,
    is_quilt: bool,
    // Note: No end_epoch - managed at BlobManager level
}
```

### 2. Storage Architecture Changes

**Converted to Accounting-Based Storage Management**:

Previously, BlobManager held actual Storage objects and split them for each blob. Now it uses accounting:

```move
/// Unified storage accounting structure.
public struct UnifiedStorage has store {
    available_storage: u64,  // Tracks available capacity
    total_capacity: u64,     // Total capacity
    end_epoch: u32,          // Validity period for ALL blobs
}

public enum BlobStorage has store {
    Unified(UnifiedStorage),
}
```

**Key Functions**:
- `new_unified_blob_storage(initial_storage: Storage)` - Destroys the Storage object, extracts capacity and end_epoch for accounting
- `allocate_storage(&mut self, encoded_size)` - Accounting-only allocation for managed blobs (no Storage object created)
- `prepare_storage_for_blob(&mut self, encoded_size, ctx)` - Creates a Storage object from the pool for individual blobs
- `add_storage(&mut self, storage: Storage)` - Adds more capacity by consuming a Storage object
- `extend_storage(&mut self, extension_storage: Storage)` - Extends validity period by consuming a Storage object

### 3. Event Updates

**ManagedBlobRegistered Event**:
```move
public struct ManagedBlobRegistered has copy, drop {
    epoch: u32,
    blob_manager_id: ID,
    blob_id: u256,
    size: u64,
    encoding_type: u8,
    deletable: bool,
    is_quilt: bool,
    object_id: ID,
    // Note: No end_epoch field
}
```

**ManagedBlobCertified Event**:
```move
public struct ManagedBlobCertified has copy, drop {
    epoch: u32,
    blob_manager_id: ID,
    blob_id: u256,
    deletable: bool,
    is_quilt: bool,
    object_id: ID,
    is_extension: bool,
    // Note: No end_epoch field
}
```

### 4. ManagedBlob Function Updates

**Constructor Signature Changed**:
```move
// Before
public(package) fun new(
    blob_manager_id: ID,
    blob_id: u256,
    root_hash: u256,
    size: u64,
    encoding_type: u8,
    deletable: bool,
    is_quilt: bool,
    registered_epoch: u32,
    end_epoch: u32,  // REMOVED
    ctx: &mut TxContext,
): ManagedBlob

// After
public(package) fun new(
    blob_manager_id: ID,
    blob_id: u256,
    root_hash: u256,
    size: u64,
    encoding_type: u8,
    deletable: bool,
    is_quilt: bool,
    registered_epoch: u32,
    ctx: &mut TxContext,
): ManagedBlob
```

**Functions Removed/Updated**:
- Removed `end_epoch()` accessor
- Removed `extend()` function (extensions happen at BlobManager level)
- Updated `assert_certified_not_expired()` → `assert_certified()` (no expiration check)
- Removed end_epoch validation from `certify_with_certified_msg()`
- Removed end_epoch from `delete()` function

### 5. BlobManager Updates

**Commented Out Managed Blob Functions**:
The managed blob registration and certification functions have been temporarily commented out pending a proper design that integrates with the existing blob_stash infrastructure.

## Benefits of These Changes

### 1. **Simplification**
- One end_epoch to manage (at BlobManager level) instead of many
- Less storage overhead per blob
- Cleaner separation of concerns

### 2. **Consistency**
- All blobs in a BlobManager share the same validity period
- Uniform storage management

### 3. **Efficiency**
- Accounting-based storage reduces object overhead
- Storage objects created only when needed (for individual blobs)
- Easy to extend all blobs at once

### 4. **Flexibility**
- Can add storage capacity without affecting existing blobs
- Can extend validity period for all blobs together
- Supports both accounting (for managed blobs) and object creation (for individual blobs)

## Migration Impact

### Storage Nodes
- No need to track per-blob end_epoch for managed blobs
- Query BlobManager's storage end_epoch for validity checks
- Events no longer include end_epoch for managed blobs

### Clients
- Don't need to specify end_epoch when registering managed blobs
- Query BlobManager for storage validity information
- Simpler API for managed blob operations

## Files Modified

1. **contracts/walrus/sources/system/managed_blob.move**
   - Removed end_epoch field from struct
   - Updated constructor signature
   - Removed/updated functions

2. **contracts/walrus/sources/system/events.move**
   - Removed end_epoch from ManagedBlobRegistered event
   - Removed end_epoch from ManagedBlobCertified event

3. **contracts/walrus/sources/blob_storage.move**
   - Changed to accounting-based storage management
   - Added UnifiedStorage struct
   - Updated BlobStorage enum to use positional variant
   - Added allocate_storage, add_storage, extend_storage functions

4. **contracts/walrus/sources/blobmanager.move**
   - Updated to use new_unified_blob_storage signature
   - Commented out incomplete managed blob functions

5. **Removed**:
   - contracts/walrus/sources/managed_blob_stash.move (no longer needed)

## Next Steps

1. **Implement Proper Managed Blob Storage**
   - Decide whether to extend BlobStash to support ManagedBlobs
   - Or implement table-based storage directly in BlobManager
   - Uncomment and complete managed blob functions

2. **Add BlobManager Operations**
   - Implement add_storage() entry function
   - Implement extend_storage() entry function
   - Add events for these operations

3. **Update Documentation**
   - Update architecture docs with accounting-based storage model
   - Document the end_epoch management at BlobManager level
   - Update API documentation

4. **Testing**
   - Test storage accounting edge cases
   - Test add_storage and extend_storage operations
   - Verify Storage object creation works correctly

## Compilation Status

✅ All contracts compile successfully with the current changes
- Only warnings for unused imports (managed_blob functions are commented out)
- No errors in the core storage management system
# Implementation Plan: Refactor BlobManager Contract

## Overview
1. Change `register_blob` to return nothing (void) instead of `ManagedBlobInfo`
2. Change `certify_blob` to use `blob_id` and `deletable` instead of `blob_object_id`

## Changes Required

### 1. Move Contract Changes (`blobmanager.move`)

#### 1.1 Update `register_blob` function signature
- **Current**: Returns `blob_stash::ManagedBlobInfo`
- **New**: Returns nothing (void)
- **Error handling**: Keep existing error aborts (EBlobAlreadyCertifiedInBlobManager)
- **Removed logic**: Remove all `return blob_stash::get_blob_info_from_stash(...)` calls
- **Note**: Still abort on errors (EBlobAlreadyCertifiedInBlobManager)

#### 1.2 Update `certify_blob` function signature
- **Current**: Takes `blob_object_id: ID` parameter
- **New**: Takes `blob_id: u256` and `deletable: bool` parameters
- **Logic changes**:
  - Remove `blob_stash::verify_and_get_blob_id` call (was using object_id)
  - Add lookup by `blob_id` + `deletable` to find the blob ObjectID
  - Need to add helper function in `blob_stash` to find ObjectID by blob_id + deletable
  - Then borrow the blob mutably for certification

#### 1.3 Add helper function in `blob_stash.move`
- **New function**: `find_blob_object_id_by_blob_id_and_deletable`
  - Takes `blob_id: u256` and `deletable: bool`
  - Returns `Option<ID>`
  - Looks up in `blob_id_to_objects` table, finds matching deletable variant
  - Aborts if blob not found or already certified (similar to existing logic)

### 2. Rust Client Changes

#### 2.1 Update `reserve_and_register_blobs_in_blobmanager` (`client.rs`)
- **Current**: Returns `Vec<ManagedBlobInfo>`
- **New**: Returns `()` (void)
- **Remove**: 
  - `result_args` collection logic
  - `extract_managed_blob_infos_from_return_values` call
  - Return value extraction logic
- **Keep**: Transaction execution and error checking

#### 2.2 Update `register_blob_in_blob_manager` (`transaction_builder.rs`)
- **Current**: Returns `Argument::Result(cmd_index)` to capture return value
- **New**: Returns `()` (void)
- **Remove**: Result argument tracking
- **Keep**: PTB command building

#### 2.3 Update `certify_blob_in_blob_manager` (`transaction_builder.rs`)
- **Current**: Takes `blob_object_id: ObjectID`
- **New**: Takes `blob_id: BlobId` and `deletable: bool`
- **Changes**:
  - Replace `blob_object_id` argument with `blob_id` and `deletable`
  - Update Move call arguments to pass blob_id and deletable instead of object_id

#### 2.4 Update `certify_blobs_in_blobmanager` (`client.rs`)
- **Current**: Takes `blobs_with_certificates: &[(ObjectID, &ConfirmationCertificate)]`
- **New**: Needs to take `blob_id` + `deletable` instead of `ObjectID`
- **Changes**:
  - Update function signature
  - Update call to `certify_blob_in_blob_manager` to pass blob_id + deletable
  - Need to extract blob_id and deletable from `WalrusStoreBlob` or certificate

#### 2.5 Update `register_with_blobmanager` (`resource.rs`)
- **Current**: Extracts `object_id` from `ManagedBlobInfo` and creates `StoreOp::RegisteredInBlobManager`
- **New**: Cannot extract object_id from return value
- **Options**:
  - **Option A**: Extract ObjectID from `Field<ObjectID, Blob>` in transaction response (requires RPC call)
  - **Option B**: Store blob_id + deletable in StoreOp, look up ObjectID later when needed
  - **Option C**: Don't store ObjectID at all, use blob_id + deletable everywhere
- **Recommendation**: Option C - Store blob_id + deletable, look up ObjectID when needed for certification

#### 2.6 Update `StoreOp::RegisteredInBlobManager` (`client_types.rs`)
- **Current**: Contains `object_id: ObjectID`
- **New**: Should contain `blob_id: BlobId` and `deletable: bool`
- **Note**: This is a breaking change to the enum variant

#### 2.7 Update `BlobManagerClient::certify_and_complete_blobs` (`blobmanager_client.rs`)
- **Current**: Uses `object_id` from `StoreOp::RegisteredInBlobManager`
- **New**: Uses `blob_id` + `deletable` from `StoreOp::RegisteredInBlobManager`
- **Changes**:
  - Extract blob_id and deletable instead of object_id
  - Pass blob_id + deletable to `certify_blobs_in_blobmanager`

#### 2.8 Update `certify_and_complete_blobs` call sites
- Check where `certify_blobs_in_blobmanager` is called
- Update to pass blob_id + deletable instead of object_id

#### 2.9 Remove/Update `extract_managed_blob_infos_from_return_values` (`client.rs`)
- **Option**: Remove entirely (no longer needed)
- **Option**: Keep for future use but mark as unused

### 3. Testing Updates

#### 3.1 Update E2E tests (`test_client.rs`)
- **Test**: `test_blob_manager_basic`
  - Update to not expect return values from `register_blob`
  - Update certification to use blob_id + deletable
  - Verify error handling still works (EBlobAlreadyCertifiedInBlobManager)

#### 3.2 Move Tests (`blobmanager.move` tests)
- Update any tests that check return values from `register_blob`
- Update certification tests to use blob_id + deletable

### 4. Data Flow Impact Analysis

#### 4.1 Registration Flow
```
Before: register_blob → ManagedBlobInfo → Extract ObjectID → StoreOp::RegisteredInBlobManager { object_id }
After: register_blob → void → StoreOp::RegisteredInBlobManager { blob_id, deletable }
```

#### 4.2 Certification Flow
```
Before: certify_blob(blob_object_id) → Lookup blob by object_id → Certify
After: certify_blob(blob_id, deletable) → Lookup blob by blob_id + deletable → Certify
```

#### 4.3 Impact on ObjectID Usage
- **Where ObjectID is still needed**:
  - Querying blob info (via `get_blob_info` using object_id)
  - Potentially for debugging/logging
- **Solution**: Query ObjectID when needed using `get_blob_object_ids(blob_id)` and filter by deletable

### 5. Migration Considerations

#### 5.1 Existing BlobManager Data
- Existing blobs already stored with ObjectIDs in tables
- New lookup by blob_id + deletable will work with existing data
- No migration needed

#### 5.2 Backward Compatibility
- This is a breaking change to the contract interface
- All clients using BlobManager will need to update
- Consider versioning if needed

### 6. Implementation Order

1. **Move Contract Changes** (foundational)
   - Update `blob_stash.move` to add lookup function
   - Update `blobmanager.move` `register_blob` (remove return value)
   - Update `blobmanager.move` `certify_blob` (change parameters)

2. **Rust Type Updates**
   - Update `StoreOp::RegisteredInBlobManager` variant
   - Update related type definitions

3. **Transaction Builder Updates**
   - Update `register_blob_in_blob_manager`
   - Update `certify_blob_in_blob_manager`

4. **Client Updates**
   - Update `reserve_and_register_blobs_in_blobmanager`
   - Update `register_with_blobmanager`
   - Update `certify_blobs_in_blobmanager`
   - Update `BlobManagerClient::certify_and_complete_blobs`

5. **Testing**
   - Update E2E tests
   - Update unit tests
   - Verify error handling

### 7. Open Questions

1. **ObjectID storage**: Do we need to store ObjectID anywhere after registration, or can we always look it up?
   - **Answer**: Can look up when needed via `get_blob_object_ids(blob_id)`

2. **Error messages**: Should error messages reference blob_id or object_id?
   - **Answer**: Use blob_id for better user experience

3. **Performance**: Is lookup by blob_id + deletable fast enough?
   - **Answer**: Yes, it's the same table lookup, just different key

### 8. Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Breaking change to contract interface | High | Ensure all call sites updated |
| ObjectID no longer available after registration | Medium | Look up when needed via query function |
| Error handling regression | Medium | Comprehensive testing |
| Performance degradation from lookups | Low | Same table structure, same performance |

## Summary

This refactoring simplifies the API by:
- Removing return value from `register_blob` (cleaner, no need to extract from response)
- Using blob_id + deletable for certification (more intuitive, matches user's mental model)
- Eliminates need for return value extraction logic

Trade-off: Need to look up ObjectID when needed, but this is infrequent and can be cached.


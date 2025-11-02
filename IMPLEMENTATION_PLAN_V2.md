# Implementation Plan V2: BlobManager - Caller Owns Blob Until Certification

## Overview
Change the design so that:
1. After registration, the Blob is **returned to the caller** (caller owns it temporarily)
2. Only the Blob's `object_id` is tracked in `blob_id_to_objects` table
3. During certification, the caller **transfers ownership** of the Blob to BlobManager
4. BlobManager stores the Blob in `blobs_by_object_id` table during certification

This aligns with the standard non-managed blob flow and simplifies the API.

## Changes Required

### 1. Move Contract Changes

#### 1.1 Update `register_blob` in `blobmanager.move`
- **Current**: Returns `void`, stores Blob in table immediately
- **New**: Returns `Blob` object (caller owns it)
- **Logic changes**:
  - Create blob via `system.register_blob()` 
  - Get `object_id` from the blob
  - Add `object_id` to `blob_id_to_objects` table (for tracking/deduplication)
  - **DO NOT** store the blob in `blobs_by_object_id` table yet
  - **Return** the blob object to caller
- **Deduplication handling**:
  - Check if blob with same blob_id + deletable exists in table
  - If exists and certified → Abort with `EBlobAlreadyCertifiedInBlobManager`
  - If exists and NOT certified → Need to check if caller owns it or if it's in table
    - If in table → Abort (already managed)
    - If not in table but tracked → Could allow reuse (TODO: clarify behavior)

#### 1.2 Update `certify_blob` in `blobmanager.move`
- **Current**: Takes `blob_id: u256` and `deletable: bool`
- **New**: Takes `blob: Blob` object (caller transfers ownership)
- **Logic changes**:
  - Extract `blob_id` and `deletable` from the blob object
  - Verify the blob's object_id is tracked in `blob_id_to_objects`
  - Verify the blob is NOT already in `blobs_by_object_id` (not yet managed)
  - Transfer blob ownership: store blob in `blobs_by_object_id` table
  - Certify the blob (it's now owned by BlobManager)

#### 1.3 Update `blob_stash.move` helper functions
- **Add new function**: `add_blob_object_id_only` - adds only object_id to tracking, doesn't store blob
- **Update**: `add_blob_to_stash` - keep for certification step (stores full blob)
- **Update**: `find_matching_blob_id` - check if blob is in table OR just tracked by object_id
- **Add new function**: `is_blob_in_table` - check if blob object_id exists in `blobs_by_object_id`
- **Add new function**: `verify_blob_not_in_table` - verify blob isn't already stored before certification

### 2. Rust Client Changes

#### 2.1 Update `register_blob_in_blob_manager` in `transaction_builder.rs`
- **Current**: Returns `()` (void)
- **New**: Returns `Argument::Result(cmd_index)` to capture Blob return value
- **Changes**: 
  - Capture result argument from Move call
  - Return it so caller can extract Blob objects

#### 2.2 Update `reserve_and_register_blobs_in_blobmanager` in `client.rs`
- **Current**: Returns `Vec<ObjectID>` (extracted from Field objects)
- **New**: Returns `Vec<Blob>` (extracted from return values)
- **Changes**:
  - Capture result arguments from each `register_blob` call
  - Extract Blob objects from transaction response (via return values)
  - Use `get_created_sui_object_ids_by_type` to get Blob ObjectIDs
  - Fetch full Blob objects using `get_sui_objects`

#### 2.3 Update `StoreOp::RegisteredInBlobManager` in `resource.rs`
- **Current**: Contains `blob_id`, `deletable`, `object_id`, `operation`
- **New**: Contains `blob: Blob`, `operation` (similar to `StoreOp::RegisterNew`)
- **Changes**:
  - Remove `blob_id`, `deletable`, `object_id` fields
  - Add `blob: Blob` field (caller owns the blob)
  - Matches the pattern of non-managed blobs

#### 2.4 Update `certify_blob_in_blob_manager` in `transaction_builder.rs`
- **Current**: Takes `blob_id: BlobId` and `deletable: bool`
- **New**: Takes `blob: ArgumentOrOwnedObject` (the Blob object)
- **Changes**:
  - Change parameter to accept Blob object
  - Update Move call to pass blob object instead of blob_id + deletable

#### 2.5 Update `certify_blobs_in_blobmanager` in `client.rs`
- **Current**: Takes `&[(BlobId, bool, &ConfirmationCertificate)]`
- **New**: Takes `&[(&Blob, &ConfirmationCertificate)]` (similar to standard `certify_blobs`)
- **Changes**:
  - Extract Blob objects from `WalrusStoreBlob::WithCertificate`
  - Pass Blob objects to `certify_blob_in_blob_manager`

#### 2.6 Update `certify_and_complete_blobs` in `blobmanager_client.rs`
- **Current**: Extracts `blob_id`, `deletable` from `StoreOp::RegisteredInBlobManager`
- **New**: Extracts `blob` from `StoreOp::RegisteredInBlobManager`
- **Changes**:
  - Get Blob object from StoreOp
  - Pass Blob objects to `certify_blobs_in_blobmanager`

#### 2.7 Update `register_with_blobmanager` in `resource.rs`
- **Current**: Gets ObjectIDs from registration, stores in StoreOp
- **New**: Gets Blob objects from registration, stores in StoreOp
- **Changes**:
  - Extract Blob objects from `reserve_and_register_blobs_in_blobmanager` response
  - Create `StoreOp::RegisteredInBlobManager { blob, operation }`

#### 2.8 Update `get_all_blob_certificates` logic in `client.rs`
- **Current**: Constructs minimal Blob with ObjectID::ZERO for BlobManager blobs
- **New**: Uses the actual Blob object from StoreOp
- **Changes**:
  - Extract `blob` from `StoreOp::RegisteredInBlobManager`
  - Use it directly (no need to construct)

### 3. Data Flow

#### 3.1 Registration Flow
```
Before: register_blob → void → Extract ObjectID from Field → StoreOp { blob_id, deletable, object_id }
After:  register_blob → Blob → StoreOp { blob } (caller owns blob)
```

#### 3.2 Certification Flow
```
Before: certify_blob(blob_id, deletable) → Lookup in table → Certify in place
After:  certify_blob(blob) → Transfer ownership to BlobManager → Store in table → Certify
```

### 4. Benefits

1. **Simpler API**: Matches non-managed blob flow exactly
2. **No ObjectID extraction needed**: Blob object is directly available
3. **Clear ownership model**: Caller owns blob until certification
4. **Easier certificate fetching**: Can use the Blob object directly

### 5. Implementation Order

1. **Move Contract** (`blobmanager.move`, `blob_stash.move`)
   - Update `register_blob` to return Blob and only track object_id
   - Update `certify_blob` to take Blob and transfer ownership
   - Add helper functions for checking if blob is in table

2. **Rust Transaction Builder**
   - Update `register_blob_in_blob_manager` to return Argument
   - Update `certify_blob_in_blob_manager` to take Blob object

3. **Rust Client**
   - Update `reserve_and_register_blobs_in_blobmanager` to return Vec<Blob>
   - Update `certify_blobs_in_blobmanager` to take Blob objects

4. **Rust Types**
   - Update `StoreOp::RegisteredInBlobManager` to store Blob
   - Update all usage sites

5. **Testing**
   - Update E2E tests
   - Verify deduplication works correctly

### 6. Open Questions / Edge Cases

1. **Deduplication with caller-owned blobs**: 
   - If same blob_id + deletable is registered twice, both return Blob objects
   - First one certifies → stores in table
   - Second one tries to certify → how to handle?
     - Option A: Allow certification if caller owns it (even if tracked)
     - Option B: Abort with error
   - **Recommendation**: If tracked in `blob_id_to_objects` but not in `blobs_by_object_id`, allow certification (it's a new registration)

2. **Reusing existing blob from table**:
   - If blob exists in table and is certified → Abort (current behavior)
   - If blob exists in table and NOT certified → ? (shouldn't happen in new design)

3. **Cleanup on failed certification**:
   - If certification fails, blob remains owned by caller (good!)
   - Object_id tracking in `blob_id_to_objects` stays (may accumulate)
   - **Consider**: Periodic cleanup of orphaned object_ids

### 7. Migration Notes

- Existing blobs in BlobManager tables: Already stored, will continue to work
- New registrations: Will return Blob objects to caller
- Certification: Requires caller to provide Blob object

## Implementation Steps

### Step 1: Move Contract Changes

1. Update `register_blob`:
   - Return `Blob` instead of void
   - Only add object_id to `blob_id_to_objects`
   - Don't store blob in `blobs_by_object_id`

2. Update `certify_blob`:
   - Take `blob: Blob` parameter
   - Verify blob isn't already in table
   - Transfer ownership: store blob in table
   - Certify the blob

3. Add helper functions in `blob_stash.move`:
   - `add_blob_object_id_only` - tracking only
   - `is_blob_in_table` - check if managed
   - `verify_blob_not_in_table` - pre-certification check

### Step 2: Rust Transaction Builder

1. `register_blob_in_blob_manager`: Return Argument::Result
2. `certify_blob_in_blob_manager`: Take Blob object parameter

### Step 3: Rust Client

1. `reserve_and_register_blobs_in_blobmanager`: Return Vec<Blob>
2. `certify_blobs_in_blobmanager`: Take Vec<(&Blob, &ConfirmationCertificate)>

### Step 4: Rust Types & Usage

1. Update `StoreOp::RegisteredInBlobManager`: Store `blob: Blob`
2. Update all pattern matches and usage sites

### Step 5: Testing

1. Test basic registration → certification flow
2. Test deduplication edge cases
3. Verify error handling


# UnifiedStorage Implementation Checklist

## Pre-flight Checks
- [ ] Confirm BlobManager is not yet deployed (no migration needed)
- [ ] Review architecture plan at `/Users/heliu/.claude/plans/buzzing-baking-hedgehog.md`
- [ ] Review implementation guide at `docs/unified-storage-implementation.md`

## Phase 0: Critical Contract Updates (MUST DO FIRST)

### Add UnifiedStorage Events
- [ ] Add event definitions to `contracts/walrus/sources/system/events.move`
  - [ ] `UnifiedStorageCreated` struct
  - [ ] `UnifiedStorageExtended` struct
  - [ ] `emit_unified_storage_created` function
  - [ ] `emit_unified_storage_extended` function
- [ ] Update `contracts/walrus/sources/unified_storage.move`
  - [ ] Emit event in `new()` function
  - [ ] Emit event in `extend_storage()` function
- [ ] Build and test contracts
  - [ ] `sui move build` in walrus package
  - [ ] `sui move test` passes

## Phase 1: Contract Refactoring

### Move Funding to BlobManager
- [ ] Create `contracts/blob_manager/sources/coin_stash.move`
  - [ ] Move CoinStash struct from walrus package
  - [ ] Implement deposit/withdraw functions
- [ ] Update `contracts/blob_manager/sources/blob_manager.move`
  - [ ] Add CoinStash field
  - [ ] Add BlobManagerCap struct
  - [ ] Implement capability-based access control
  - [ ] Add permission constants
  - [ ] Implement `register_blob` with capability check
  - [ ] Implement `extend_storage` with capability check
  - [ ] Implement `create_delegated_cap`
- [ ] Remove funding concepts from walrus package
  - [ ] Remove CoinStash from unified_storage.move (if present)
  - [ ] Remove storage purchase policy from walrus package
- [ ] Build and test
  - [ ] `sui move build` in blob_manager package
  - [ ] `sui move test` passes

## Phase 2: Storage Node Updates

### Core Storage Refactoring
- [ ] Rename `blob_manager_info.rs` to `unified_storage_info.rs`
  - [ ] Update struct to `StoredUnifiedStorageInfo`
  - [ ] Remove `blob_manager_id` field
  - [ ] Update struct to `UnifiedStorageTable`
  - [ ] Remove secondary indices
- [ ] Create optional `blob_manager_index.rs`
  - [ ] Implement `BlobManagerIndex` struct
  - [ ] Add mapping functions
- [ ] Update `blob_info.rs`
  - [ ] Rename `ManagedBlobInfo` → `BlobV2Info`
  - [ ] Rename `RegisteredManagedBlobInfo` → `RegisteredBlobV2Info`
  - [ ] Rename `CertifiedManagedBlobInfo` → `CertifiedBlobV2Info`
  - [ ] Update `populate_epochs` to use `UnifiedStorageTable`
- [ ] Update `constants.rs`
  - [ ] Add `unified_storages_cf_name()`
  - [ ] Add `blob_manager_index_cf_name()` (optional)
  - [ ] Remove `blob_managers_cf_name()`

### Event Processing
- [ ] Update `events.rs` in walrus-sui
  - [ ] Add `UnifiedStorageCreated` struct
  - [ ] Add `UnifiedStorageExtended` struct
  - [ ] Add `StorageEvent` enum
  - [ ] Keep `BlobManagerEvent` separate
  - [ ] Rename ManagedBlob* events to BlobV2*
- [ ] Update `storage.rs`
  - [ ] Add `process_unified_storage_event()`
  - [ ] Update `process_blob_manager_event()` (optional indexing only)
  - [ ] Update blob event processing to use storage_id

### Build and Test
- [ ] `cargo build --features unified-storage`
- [ ] `cargo nextest run unified_storage`
- [ ] `cargo nextest run blob_v2`

## Phase 3: API Updates

### Update Endpoints
- [ ] Implement storage-based endpoints
  - [ ] `/storage/{storage_id}/blob/{blob_id}`
  - [ ] `/storage/{storage_id}/blobs`
- [ ] Add optional BlobManager compatibility
  - [ ] `/blob-manager/{id}/storage` (if index enabled)
- [ ] Update OpenAPI documentation

## Phase 4: Testing

### Unit Tests
- [ ] Test UnifiedStorage event emission
- [ ] Test BlobManager capability system
- [ ] Test storage node event processing
- [ ] Test BlobV2Info epoch population

### Integration Tests
- [ ] Test pure UnifiedStorage flow (no BlobManager)
- [ ] Test BlobManager-owned UnifiedStorage flow
- [ ] Test blob certification by third party
- [ ] Test invalid capability rejection

### End-to-End Tests
- [ ] Deploy contracts to testnet
- [ ] Create UnifiedStorage instances
- [ ] Register and certify BlobV2s
- [ ] Verify storage node tracking
- [ ] Test API endpoints

## Phase 5: Documentation

### Update Documentation
- [ ] Update architecture docs
- [ ] Update API documentation
- [ ] Update deployment guide
- [ ] Add examples for both UnifiedStorage patterns

## Deployment

### Testnet Deployment
- [ ] Deploy Walrus package with UnifiedStorage events
- [ ] Deploy BlobManager package
- [ ] Deploy updated storage nodes
- [ ] Monitor event processing
- [ ] Validate metrics

### Mainnet Deployment (Future)
- [ ] Complete testnet validation
- [ ] Security audit
- [ ] Performance testing
- [ ] Deploy contracts
- [ ] Rolling update of storage nodes

## Post-Deployment

### Monitoring
- [ ] UnifiedStorage creation rate normal
- [ ] BlobV2 registration working
- [ ] Event processing lag acceptable
- [ ] No error spikes in logs

### Validation
- [ ] All BlobV2s reference valid UnifiedStorage
- [ ] No orphaned entries
- [ ] Epoch boundaries respected
- [ ] APIs returning correct data

## Sign-off

- [ ] Code review completed
- [ ] Tests passing
- [ ] Documentation updated
- [ ] Deployment successful
- [ ] Monitoring healthy

---

**Remember**:
1. Phase 0 (UnifiedStorage events) is CRITICAL and must be done first
2. This is all new functionality - no migration needed
3. Storage nodes should only understand UnifiedStorage, not BlobManager
4. Capabilities are ONLY for BlobManager, not UnifiedStorage

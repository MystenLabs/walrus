# BlobManager Redesign Implementation Plan

## Executive Summary
Redesign BlobManager to use a single Storage object for all blobs, with dynamic fields for blob storage, while maintaining compatibility with existing Walrus core structures.

---

## Phase 1: Core Structure Redesign

### 1.1 New BlobManager Structure
```move
public struct BlobManager has key, store {
    id: UID,
    /// Single storage object managing lifetime for all blobs
    storage: Storage,
    /// Total storage capacity in bytes
    total_capacity: u64,
    /// Currently used storage in bytes
    used_space: u64,
    /// Number of blobs currently stored
    blob_count: u64,
    /// Current epoch for tracking
    current_epoch: u32,
    /// Owner capability ID for validation
    manager_cap_id: ID,
}

// Keep BlobManagerCap unchanged
public struct BlobManagerCap has key, store {
    id: UID,
    manager_id: ID,
}
```

### 1.2 Dynamic Field Structure for Blobs
```move
// Blobs stored as: blob_id (u256) -> ManagedBlob
public struct ManagedBlob has store {
    blob: Blob,  // Original Walrus Blob - unchanged
    added_epoch: u32,
    metadata: vector<u8>, // Optional metadata
}
```

### 1.3 Storage Accounting Structure
```move
public struct StorageAccount has store {
    /// Available capacity that can be allocated
    available_capacity: u64,
    /// Reserved but not yet used capacity
    reserved_capacity: u64,
    /// Minimum capacity to maintain
    min_capacity: u64,
    /// Auto-purchase configuration
    auto_purchase_enabled: bool,
    auto_purchase_threshold: u64, // % of capacity
    auto_purchase_amount: u64,
}
```

---

## Phase 2: Function Implementations

### 2.1 Constructor Functions
```move
// Create new BlobManager with initial storage
public fun new(
    initial_storage: Storage,
    ctx: &mut TxContext
): (BlobManager, BlobManagerCap)

// Create empty BlobManager (storage to be added later)
public fun new_empty(
    ctx: &mut TxContext
): (BlobManager, BlobManagerCap)
```

### 2.2 Storage Management Functions
```move
// Add storage capacity to the manager
public fun add_storage(
    self: &mut BlobManager,
    storage: Storage,
    system: &System,
    ctx: &mut TxContext
)

// Extend storage duration for all blobs
public fun extend_storage(
    self: &mut BlobManager,
    cap: &BlobManagerCap,
    additional_epochs: u32,
    payment: &mut Coin<FROST>,
    system: &mut System,
    ctx: &mut TxContext
)

// Auto-purchase storage if needed
fun auto_purchase_storage_if_needed(
    self: &mut BlobManager,
    required_size: u64,
    payment: &mut Coin<FROST>,
    system: &mut System,
    ctx: &mut TxContext
): bool
```

### 2.3 Blob Operations
```move
// Register and add new blob
public fun register_and_add_blob(
    self: &mut BlobManager,
    cap: &BlobManagerCap,
    blob_id: u256,
    root_hash: u256,
    size: u64,
    encoding_type: u8,
    deletable: bool,
    payment: &mut Coin<FROST>,
    system: &mut System,
    ctx: &mut TxContext
)

// Add existing blob (must be certified)
public fun add_existing_blob(
    self: &mut BlobManager,
    cap: &BlobManagerCap,
    blob: Blob,
    ctx: &mut TxContext
)

// Remove blob (returns it)
public fun remove_blob(
    self: &mut BlobManager,
    cap: &BlobManagerCap,
    blob_id: u256,
    ctx: &mut TxContext
): Blob

// Delete blob (destroys it)
public fun delete_blob(
    self: &mut BlobManager,
    cap: &BlobManagerCap,
    blob_id: u256,
    system: &System,
    ctx: &mut TxContext
)

// Borrow blob (immutable)
public fun borrow_blob(
    self: &BlobManager,
    blob_id: u256
): &Blob

// Check if blob exists
public fun has_blob(
    self: &BlobManager,
    blob_id: u256
): bool

// Get list of blob IDs (paginated)
public fun get_blob_ids(
    self: &BlobManager,
    offset: u64,
    limit: u64
): vector<u256>
```

### 2.4 Query Functions
```move
public fun capacity_info(self: &BlobManager): (u64, u64, u64) // (total, used, available)
public fun storage_epochs(self: &BlobManager): (u32, u32) // (start, end)
public fun blob_count(self: &BlobManager): u64
public fun needs_extension(self: &BlobManager, current_epoch: u32): bool
```

---

## Phase 3: Migration from Current Design

### 3.1 Compatibility Layer
```move
// Wrapper to handle old-style operations
public fun migrate_from_stash(
    old_storage_stash: StorageStash,
    old_blob_stash: SimpleBlobStash,
    system: &System,
    ctx: &mut TxContext
): BlobManager

// Convert individual blob with storage to managed blob
fun convert_blob_with_storage(
    blob: Blob,
    ctx: &mut TxContext
): (Blob, Storage)
```

### 3.2 Remove Dependencies
- Remove `StorageStash` module usage
- Remove `SimpleBlobStash` module usage
- Remove `TimerWheel` dependency
- Keep `BigStack` if needed for other purposes

---

## Phase 4: Event System

### 4.1 Event Definitions
```move
public struct BlobAdded has copy, drop {
    manager_id: ID,
    blob_id: u256,
    size: u64,
    encoding_type: u8,
}

public struct BlobRemoved has copy, drop {
    manager_id: ID,
    blob_id: u256,
}

public struct StorageExtended has copy, drop {
    manager_id: ID,
    new_end_epoch: u32,
    additional_epochs: u32,
}

public struct CapacityWarning has copy, drop {
    manager_id: ID,
    used_space: u64,
    total_capacity: u64,
    percentage_used: u8,
}
```

---

## Phase 5: Testing Strategy

### 5.1 Unit Tests
- Test each function independently
- Verify capacity calculations
- Test dynamic field operations
- Validate Storage object handling

### 5.2 Integration Tests
- Full lifecycle: create → add blobs → extend → remove
- Capacity limits and auto-purchase
- Migration from old structure
- Concurrent operations simulation

### 5.3 Edge Cases
- Maximum blob count limits
- Storage expiration boundaries
- Zero capacity scenarios
- Overflow prevention

---

## Implementation Steps

### Week 1: Core Structure
1. **Day 1-2**: Implement new BlobManager structure
   - [ ] Define struct with Storage member
   - [ ] Add capacity tracking fields
   - [ ] Setup dynamic field structure

2. **Day 3-4**: Basic operations
   - [ ] Constructor functions
   - [ ] Basic getters/setters
   - [ ] Capability validation

3. **Day 5**: Storage management
   - [ ] add_storage function
   - [ ] extend_storage function
   - [ ] Capacity calculations

### Week 2: Blob Operations
1. **Day 1-2**: Add blob functions
   - [ ] register_and_add_blob
   - [ ] add_existing_blob
   - [ ] Dynamic field storage

2. **Day 3-4**: Remove/delete operations
   - [ ] remove_blob with capacity recovery
   - [ ] delete_blob with proper cleanup
   - [ ] Batch operations

3. **Day 5**: Query functions
   - [ ] Implement all getters
   - [ ] Pagination support
   - [ ] Capacity info

### Week 3: Advanced Features
1. **Day 1-2**: Auto-purchase
   - [ ] Implement purchase logic
   - [ ] Threshold management
   - [ ] Payment handling

2. **Day 3-4**: Event system
   - [ ] Define all events
   - [ ] Emit at appropriate points
   - [ ] Event testing

3. **Day 5**: Migration support
   - [ ] Compatibility functions
   - [ ] Migration testing
   - [ ] Documentation

### Week 4: Testing & Polish
1. **Day 1-3**: Comprehensive testing
   - [ ] Unit tests
   - [ ] Integration tests
   - [ ] Edge cases

2. **Day 4-5**: Documentation & Review
   - [ ] Code documentation
   - [ ] Usage examples
   - [ ] Performance optimization

---

## Key Design Decisions

### 1. Storage Object Handling
- **Decision**: Keep single Storage object inside BlobManager
- **Rationale**: Simplifies lifetime management, reduces gas costs
- **Trade-off**: All blobs share same expiration

### 2. Dynamic Fields vs Vectors
- **Decision**: Use dynamic fields for blob storage
- **Rationale**: Scales better, no vector size limits
- **Trade-off**: Slightly more complex iteration

### 3. Capacity Model
- **Decision**: Track total_capacity and used_space separately
- **Rationale**: Allows immediate reuse of freed space
- **Trade-off**: Need careful accounting

### 4. Auto-purchase Feature
- **Decision**: Optional auto-purchase with configurable thresholds
- **Rationale**: Improves UX, prevents failures
- **Trade-off**: Requires payment handling logic

---

## Compatibility Notes

### Unchanged External Structures
- ✅ `Blob` struct remains unchanged
- ✅ `Storage` struct remains unchanged
- ✅ `System` interface unchanged
- ✅ All Walrus core types preserved

### Internal Changes (BlobManager module only)
- ❌ Remove StorageStash
- ❌ Remove SimpleBlobStash
- ❌ Remove TimerWheel dependency
- ✅ New simplified structure

### Migration Path
1. Deploy new BlobManager alongside old
2. Provide migration function for existing instances
3. Deprecate old implementation
4. No breaking changes to external interfaces

---

## Risk Assessment

### Low Risk
- Structure changes are internal to BlobManager
- Core Walrus types unchanged
- Can deploy alongside existing code

### Medium Risk
- Dynamic field scaling limits need verification
- Storage accounting must be precise
- Event system adds complexity

### Mitigation
- Extensive testing before deployment
- Gradual rollout with monitoring
- Keep old implementation as fallback

---

## Success Metrics
- ✅ 50% reduction in gas costs for extensions
- ✅ 70% simpler codebase (fewer lines)
- ✅ Zero compatibility breaks
- ✅ 100% test coverage
- ✅ Clear migration path

---

## Next Actions
1. Review plan with team
2. Set up development environment
3. Begin Phase 1 implementation
4. Create test framework
5. Document API changes
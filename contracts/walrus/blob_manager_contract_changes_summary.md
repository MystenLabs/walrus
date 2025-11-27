# BlobManager Contract Changes Summary

## 1. New Modules Created

### 1.1 `managed_blob.move` (system module)
**Purpose**: Defines the ManagedBlob type, a new blob variant owned by BlobManagers.

**Key Components**:
- **ManagedBlob struct**:
  - No direct storage ownership (managed by BlobManager)
  - No `end_epoch` field (validity determined by BlobManager's pool)
  - Includes `blob_manager_id` reference
  - Supports both deletable and permanent variants
  - `blob_type` field (Regular or Quilt enum)
  - `attributes: VecMap<String, String>` - Internal key-value attributes

- **Attribute System**:
  - Limits: max 100 attributes, max 1KB per key, max 1KB per value
  - `attributes()` - Read-only accessor
  - `set_attribute(key, value)` - Add/update with validation
  - `get_attribute(key)` - Get value by key
  - `remove_attribute(key)` - Remove by key (aborts if not found)
  - `remove_attribute_if_exists(key)` - Safe removal
  - `clear_attributes()` - Remove all attributes

- **Key Functions**:
  - `new()` - Creates managed blob with BlobManager ownership
  - `certify_with_certified_msg()` - Certification without ownership transfer
  - `delete()` - Removes deletable blobs and emits event

### 1.2 `blob_stash.move`
**Purpose**: Set of ManagedBlob objects within a BlobManager.

**Key Components**:
- **BlobStashByBlobId struct** (simplified single-table design):
  - Single `Table<u256, ManagedBlob>` keyed by blob_id
  - Tracks `total_unencoded_size`
  - Only one blob per blob_id (either permanent or deletable)

- **BlobStash enum**:
  - `BlobIdBased(BlobStashByBlobId)` - Current variant

- **ManagedBlobInfo struct**:
  - `object_id: ID` - The ManagedBlob's object ID
  - `is_certified: bool` - Certification status

- **Key Functions**:
  - `find_blob_in_stash(blob_id, deletable)` - Find blob and check certification
  - `get_mut_blob_in_stash(blob_id, deletable)` - Get mutable ref with deletable check
  - `get_mut_blob_in_stash_unchecked(blob_id)` - Get mutable ref (for attributes)
  - `add_blob_to_stash(managed_blob)` - Store new managed blob
  - `remove_blob_from_stash(blob_id, deletable)` - Delete and return blob
  - `has_blob_in_stash(blob_id)` - Check existence
  - `blob_count_in_stash()` / `total_blob_size_in_stash()` - Stats

### 1.3 `blob_storage.move`
**Purpose**: Unified storage accounting for BlobManager's storage pool.

**Key Components**:
- **UnifiedStorage struct**:
  - Accounting-based storage management
  - Tracks available/total capacity
  - Single `end_epoch` for entire pool

- **Key Functions**:
  - `allocate_storage()` - Reserve storage for blob
  - `release_storage()` - Return storage on deletion
  - `add_storage()` - Expand capacity
  - `extend_managed_storage()` - Extend validity period

### 1.4 `blob_manager.move`
**Purpose**: Main BlobManager implementation coordinating all components.

**Key Components**:
- **BlobManager struct**:
  ```move
  struct BlobManager {
      id: UID,
      storage: BlobStorage,
      blob_stash: BlobStash,
      coin_stash: CoinStash,
  }
  ```

- **BlobManagerCap struct**:
  ```move
  struct BlobManagerCap {
      id: UID,
      manager_id: ID,
      is_admin: bool,
      fund_manager: bool,
  }
  ```

- **Capability Management**:
  - `create_cap(is_admin, fund_manager)` - Returns new cap (PTB handles transfer)
  - `cap_manager_id()` / `cap_is_admin()` / `cap_fund_manager()` - Accessors
  - Permission rules:
    - Admin caps can create new caps
    - Only fund_manager caps can create new fund_manager caps
    - Any cap can write blobs

- **Core Operations**:
  - `register_blob()` - Register managed blob
  - `certify_blob()` - Certify without ownership transfer
  - `delete_blob()` - Remove deletable blobs

- **Blob Attribute Operations**:
  - `set_blob_attribute(cap, blob_id, key, value)` - Set attribute
  - `remove_blob_attribute(cap, blob_id, key)` - Remove attribute
  - `clear_blob_attributes(cap, blob_id)` - Clear all attributes

- **Community Funding**:
  - `deposit_wal_to_coin_stash()` - Add WAL tokens
  - `deposit_sui_to_coin_stash()` - Add SUI tokens
  - `buy_storage_from_stash()` - Purchase storage capacity
  - `extend_storage_from_stash()` - Extend storage duration
  - `withdraw_wal_from_coin_stash()` - Withdraw WAL (requires fund_manager)
  - `withdraw_sui_from_coin_stash()` - Withdraw SUI (requires fund_manager)

### 1.5 `coin_stash.move`
**Purpose**: Community funding mechanism for BlobManager operations.

**Key Components**:
- **CoinStashByBalance struct**:
  - Holds WAL and SUI balances
  - No contribution tracking (simplified)

- **Key Functions**:
  - `deposit_wal()` / `deposit_sui()` - Accept contributions
  - `withdraw_wal_for_storage()` - Use funds for storage
  - `withdraw_sui_for_gas()` - Use funds for gas
  - Balance query functions

---

## 2. Modified Existing Modules

### 2.1 `system/events.move`
**New Event Types Added**:
- `ManagedBlobRegistered` - Blob registered with BlobManager
- `ManagedBlobCertified` - Blob certified
- `ManagedBlobDeleted` - Blob deleted from BlobManager
- `BlobManagerCreated` - New BlobManager instantiated

### 2.2 `system.move`
**New Functions Added**:
- `register_managed_blob()` - System-level registration
- `certify_managed_blob()` - System-level certification
- Integration points for BlobManager operations

---

## 3. Extension Policy Module

### 3.1 `extension_policy.move`
**Purpose**: Controls when and how storage can be extended by anyone using the coin stash.

**Key Components**:
- **ExtensionPolicy enum**:
  ```move
  public enum ExtensionPolicy has store, copy, drop {
      Disabled,
      FundManagerOnly,
      Constrained {
          expiry_threshold_epochs: u32,
          max_extension_epochs: u32,
      },
  }
  ```

- **Policy Variants**:
  - `Disabled` - No one can extend storage (blocks everyone including fund managers)
  - `FundManagerOnly` - Only fund managers can extend (public extension blocked)
  - `Constrained` - Anyone can extend within constraints:
    - `expiry_threshold_epochs` - Extension only allowed when within N epochs of expiry
    - `max_extension_epochs` - Maximum epochs per extension call

- **Validation Functions**:
  - `validate_and_cap_extension()` - For public extension
    - Enforces time threshold (current_epoch >= end_epoch - expiry_threshold)
    - Caps to min(policy_max, system_max)
    - Aborts if Disabled or FundManagerOnly
  - `validate_and_cap_extension_fund_manager()` - For fund managers
    - Bypasses time/amount constraints
    - Still caps to system max epochs ahead
    - Only aborts if Disabled

- **Constructors**:
  - `disabled()` - Creates disabled policy
  - `fund_manager_only()` - Creates fund manager only policy
  - `constrained(expiry_threshold_epochs, max_extension_epochs)` - Creates constrained policy

### 3.2 BlobManager Extension Policy Integration
**Updated in `blob_manager.move`**:

- **New Field**:
  ```move
  struct BlobManager {
      ...
      extension_policy: ExtensionPolicy,  // Default: constrained(1, 10)
  }
  ```

- **Extension Functions**:
  - `extend_storage_from_stash()` - Public extension (follows policy)
  - `extend_storage_from_stash_fund_manager()` - Fund manager extension (bypasses constraints)
  - Both return actual epochs extended (may be less than requested due to caps)
  - Both return 0 if extension not possible

- **Policy Management** (require fund_manager permission):
  - `set_extension_policy_disabled()` - Block all extension
  - `set_extension_policy_fund_manager_only()` - Fund manager only mode
  - `set_extension_policy_constrained(threshold, max)` - Set constrained policy
  - `extension_policy()` - Query current policy

- **Policy Behavior**:
  | Policy | Public Extension | Fund Manager Extension |
  |--------|------------------|------------------------|
  | Disabled | ❌ Blocked | ❌ Blocked |
  | FundManagerOnly | ❌ Blocked | ✅ System cap only |
  | Constrained | ✅ Within threshold & max | ✅ System cap only |

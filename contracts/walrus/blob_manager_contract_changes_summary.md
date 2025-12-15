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
  - `attributes()` - Returns reference to VecMap
  - `attributes_count()` - Returns number of attributes
  - `has_attribute(key)` - Checks if key exists
  - `get_attribute(key)` - Get value by key (returns Option)
  - `set_attribute(key, value)` - Add/update with validation (public)
  - `remove_attribute(key)` - Remove by key (aborts if not found, public)
  - `remove_attribute_if_exists(key)` - Safe removal (public)
  - `clear_attributes()` - Remove all attributes (public)

- **Accessors**:
  - `object_id()` - Returns ManagedBlob's object ID
  - `registered_epoch()` - Returns registration epoch
  - `blob_id()` - Returns the blob ID
  - `size()` - Returns unencoded size
  - `encoding_type()` - Returns encoding type
  - `certified_epoch()` - Returns certification epoch (Option)
  - `blob_manager_id()` - Returns owning BlobManager ID
  - `is_deletable()` - Returns deletable flag
  - `blob_type()` - Returns BlobType enum (Regular or Quilt)
  - `encoded_size(n_shards)` - Calculates encoded size

- **Key Functions**:
  - `new()` - Creates managed blob with BlobManager ownership (package visibility)
  - `certify_with_certified_msg()` - Certification without ownership transfer (public)
  - `delete()` - Removes deletable blobs and emits event (package visibility)
  - `burn()` - Destroys managed blob without storage release (public)
  - `derive_blob_id(root_hash, encoding_type, size)` - Derives blob ID (public)

### 1.2 `blob_stash.move`
**Purpose**: Set of ManagedBlob objects within a BlobManager.

**Key Components**:
- **BlobStashByBlobId struct** (simplified single-table design):
  - Single `Table<u256, ManagedBlob>` keyed by blob_id
  - Tracks `total_unencoded_size`
  - Only one blob per blob_id (either permanent or deletable, not both)
  - Permanency conflict error if attempting to register with different deletable flag

- **BlobStash enum**:
  - `BlobIdBased(BlobStashByBlobId)` - Current variant

- **ManagedBlobInfo struct**:
  - `object_id: ID` - The ManagedBlob's object ID
  - `is_certified: bool` - Certification status

- **Dispatch Functions** (dispatch to variant):
  - `find_blob_in_stash(blob_id, deletable)` - Find blob and check certification, returns `Option<ManagedBlobInfo>`
  - `get_mut_blob_in_stash(blob_id, deletable)` - Get mutable ref with deletable check
  - `get_mut_blob_in_stash_unchecked(blob_id)` - Get mutable ref without deletable check (for attributes)
  - `add_blob_to_stash(managed_blob)` - Store new managed blob
  - `remove_blob_from_stash(blob_id, deletable)` - Delete and return blob
  - `has_blob_in_stash(blob_id)` - Check existence
  - `get_blob_object_id_from_stash(blob_id)` - Get object ID for blob_id
  - `blob_count_in_stash()` / `total_blob_size_in_stash()` - Stats

- **ManagedBlobInfo Accessors**:
  - `object_id()` - Returns the object ID
  - `is_certified()` - Returns certification status

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

### 1.4 `blob_manager.move` and `blob_manager/blob_manager_inner_v1.move`
**Purpose**: Main BlobManager implementation using a versioned design pattern for upgradability.

**Architecture**:
The BlobManager uses a two-layer design similar to `System` / `SystemStateInnerV1`:
- `blob_manager.move` - The stable interface layer with version tracking
- `blob_manager/blob_manager_inner_v1.move` - The implementation layer stored as a dynamic field

**Key Components**:
- **BlobManager struct** (interface - stable):
  ```move
  struct BlobManager {
      id: UID,
      version: u64,  // Currently 1
  }
  ```

- **BlobManagerInnerV1 struct** (implementation - can be upgraded):
  ```move
  struct BlobManagerInnerV1 {
      storage: BlobStorage,
      coin_stash: BlobManagerCoinStash,
      extension_policy: ExtensionPolicy,
      tip_policy: TipPolicy,
      storage_purchase_policy: StoragePurchasePolicy,
      caps_info: Table<ID, CapInfo>,
  }
  ```

- **BlobManagerCap struct** (stays in interface):
  ```move
  struct BlobManagerCap {
      id: UID,
      manager_id: ID,
      can_delegate: bool,
      can_withdraw_funds: bool,
  }
  ```

**Internal Accessors**:
  - `inner(self: &BlobManager)` - Get immutable reference to inner via dynamic field
  - `inner_mut(self: &mut BlobManager)` - Get mutable reference to inner via dynamic field
  - Both assert `self.version == VERSION` before accessing

- **Capability Management**:
  - `create_cap(can_delegate, can_withdraw_funds)` - Returns new cap (PTB handles transfer)
  - `cap_manager_id()` / `can_delegate()` / `can_withdraw_funds()` - Accessors
  - Permission rules:
    - Delegate caps can create new caps
    - Only can_withdraw_funds caps can create new can_withdraw_funds caps
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
  - `extend_storage_from_stash()` - Extend storage duration (public, follows policy)
  - `extend_storage_from_stash_fund_manager()` - Extend storage (can_withdraw_funds, bypasses constraints)
  - `withdraw_wal(amount)` - Withdraw WAL (requires can_withdraw_funds)
  - `withdraw_sui(amount)` - Withdraw SUI (requires can_withdraw_funds)

- **Query Functions**:
  - `manager_id()` - Returns BlobManager ID
  - `capacity_info()` - Returns (total, used, available) capacity
  - `storage_epochs()` - Returns (start, end) epochs
  - `blob_count()` - Returns number of blobs
  - `total_blob_size()` - Returns total unencoded size
  - `has_blob(blob_id)` - Checks if blob exists
  - `get_blob_object_id(blob_id)` - Returns object ID for blob_id
  - `get_blob_object_id_by_blob_id_and_deletable(blob_id, deletable)` - Returns object ID with deletable check
  - `coin_stash_balances()` - Returns (WAL, SUI) balances
  - `extension_policy()` - Returns current extension policy

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
**Purpose**: Controls when and how storage can be extended by community members using the coin stash.
Also configures tip amounts for community extenders.

**Key Components**:
- **ExtensionPolicy enum**:
  ```move
  public enum ExtensionPolicy has store, copy, drop {
      Disabled,
      Constrained {
          expiry_threshold_epochs: u32,
          max_extension_epochs: u32,
          tip_amount: u64,  // Tip in MIST for community extenders
      },
  }
  ```

- **Policy Variants**:
  - `Disabled` - No one can extend storage via community extension (admins can use `adjust_storage`)
  - `Constrained` - Anyone can extend within constraints:
    - `expiry_threshold_epochs` - Extension only allowed when within N epochs of expiry
    - `max_extension_epochs` - Maximum epochs per extension call
    - `tip_amount` - SUI tip (in MIST) rewarded to transaction sender

- **Validation Functions**:
  - `validate_and_compute_end_epoch()` - For public extension
    - Enforces time threshold (current_epoch >= end_epoch - expiry_threshold)
    - Caps to min(policy_max, system_max)
    - Aborts if Disabled

- **Accessors**:
  - `get_tip_amount()` - Returns tip amount (0 if Disabled)

- **Constructors**:
  - `disabled()` - Creates disabled policy
  - `constrained(expiry_threshold_epochs, max_extension_epochs, tip_amount)` - Creates constrained policy
  - `default_constrained()` - Creates default constrained policy (2 epoch threshold, 5 max extension, 1000 MIST tip)

### 3.2 BlobManager Extension Policy Integration
**Updated in `blob_manager.move`**:

- **Extension Functions**:
  - `extend_storage_from_stash()` - Community extension (follows policy, returns tip)
  - `adjust_storage(new_capacity, new_end_epoch)` - Admin-level storage adjustment (bypasses all policies)

- **Policy Management** (require can_withdraw_funds permission):
  - `set_extension_policy_disabled()` - Block community extension
  - `set_extension_policy_constrained(threshold, max, tip_amount)` - Set constrained policy with tip
  - `extension_policy()` - Query current policy

- **Admin Storage Adjustment** (requires can_withdraw_funds permission):
  - `adjust_storage(new_capacity, new_end_epoch)` - Direct storage control
    - Bypasses extension policy
    - Bypasses storage_purchase_policy
    - Increase-only (cannot decrease capacity or end_epoch)
    - Uses funds from coin stash

- **Policy Behavior**:
  | Policy | Community Extension | Admin adjust_storage |
  |--------|---------------------|----------------------|
  | Disabled | ❌ Blocked | ✅ Always works |
  | Constrained | ✅ Within threshold & max + tip | ✅ Always works |

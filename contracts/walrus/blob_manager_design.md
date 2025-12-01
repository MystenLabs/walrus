# BlobManager Contract Design

## What is BlobManager?

BlobManager is a shared object that manages blobs and storage resources on behalf of users. Instead of
users owning individual Blob objects and storage resources directly, a BlobManager holds a pool of
storage that all blobs can share. This enables use cases like community-funded storage, delegated
blob management, and simplified client interactions.

## How It Differs from Regular Blobs

In the regular Walrus flow, each user owns and manages their own Blob objects and storage resources.
The blob's validity period (`end_epoch`) is determined by its attached storage resource, and users
must manage each blob individually.

With BlobManager:
- The BlobManager owns all blobs (called ManagedBlobs) on behalf of users.
- All blobs share a single storage pool with one unified `end_epoch`.
- Users interact through capabilities (BlobManagerCap) rather than owning blobs directly, the
  capability objects can provide access control at fine granularity, e.g., write, admin, and fund
  managements.
- Anyone can contribute funds to the storage pool (community funding), and the extension policy can
  control who can extend the blobs with fund attached to the blob manager.
- Storage extensions affect all blobs at once.

## Core Components

### BlobManager Object

The BlobManager is a shared object containing four main parts:

1. **BlobStorage** - A unified storage pool that tracks total capacity, used capacity, and the
   storage validity period. All ManagedBlobs draw from this single pool. When you buy more storage
   or extend the validity period, it affects all blobs in the manager.

2. **BlobStash** - A table that stores all ManagedBlob objects, keyed by blob_id. Each blob_id can
   only have one entry (you cannot register the same blob as both deletable and permanent).

3. **CoinStash** - Holds WAL and SUI tokens for storage operations. Anyone can deposit funds here,
   but only authorized users can withdraw. These funds can be used to buy more storage capacity or
   extend the blobs.

4. **ExtensionPolicy** - Controls who can extend storage and under what conditions. This prevents
   abuse while allowing community-driven storage extensions.

### ManagedBlob

ManagedBlobs are similar to regular Blobs but with key differences:

- They have no `end_epoch` field.
- They store a reference to their owning BlobManager (`blob_manager_id`).
- They can hold key-value attributes (up to 100 entries, 1KB each) for metadata.
- They follow the same register → certify → (optionally delete) lifecycle as regular blobs.

The fields are: `blob_id`, `size`, `encoding_type`, `certified_epoch`, `deletable`, `blob_type` (Regular or Quilt), and `attributes`.

### BlobManagerCap (Capability)

Access control uses capability objects. Each capability has two permission flags:

- **is_admin** - Can create new capabilities of any type.
- **fund_manager** - Can withdraw funds and change extension policy.

Any capability holder can register, certify, and delete blobs. The initial capability created with the BlobManager is an admin + fund_manager cap.

## Blob Lifecycle

The lifecycle mirrors regular blobs:

1. **Register** - Call `register_blob()` with the blob_id, size, and whether it's deletable. This
   allocates space from the storage pool and creates a ManagedBlob entry.

2. **Certify** - After uploading slivers to storage nodes and getting confirmations, call
   `certify_blob()` with the certification message. This marks the blob as certified.

3. **Delete** (optional) - For deletable blobs, call `delete_blob()` to remove the blob and return its space to the storage pool.

Each operation emits corresponding events: `ManagedBlobRegistered`, `ManagedBlobCertified`, `ManagedBlobDeleted`.

## Storage Management

The storage pool is purchased upfront when creating the BlobManager. You specify initial capacity
and how many epochs ahead the storage should be valid.

To add more capacity later, use `buy_storage_from_stash()` which uses WAL tokens from the CoinStash.
To extend the validity period, use `extend_storage_from_stash()`.

When a blob is registered, its encoded size is deducted from available capacity. When deleted, the space is returned.

## Community Funding (CoinStash)

The CoinStash enables community funding of storage:

- **Anyone can deposit** WAL or SUI tokens using `deposit_wal_to_coin_stash()` or `deposit_sui_to_coin_stash()`.
- **Anyone can use funds** to extend the storage subject to the extension policy.
- **Only fund_managers can withdraw** coins.

This allows communities to pool resources for shared storage without giving everyone withdrawal access.

## Extension Policy

The extension policy controls public storage extensions. There are three modes:

1. **Disabled** - No one can extend storage, not even fund managers. Use this to lock down a manager.

2. **FundManagerOnly** - Only fund_manager capability holders can extend storage. Public extension calls are rejected.

3. **Constrained** - Anyone can extend storage, but with limits:
   - `expiry_threshold_epochs` - Extension only allowed when storage is within N epochs of expiring.
   - `max_extension_epochs` - Maximum epochs that can be extended per call.

## Query Functions

The main query functions are:

- `capacity_info()` - Returns (total, used, available) capacity.
- `storage_epochs()` - Returns (start, end) epochs.
- `blob_count()` / `total_blob_size()` - Statistics about stored blobs.
- `has_blob(blob_id)` - Check if a blob exists.
- `get_blob_object_id(blob_id)` - Get the ManagedBlob's object ID.
- `coin_stash_balances()` - Returns (WAL, SUI) balances.
- `extension_policy()` - Returns the current extension policy.

/// A high-level blob-management interface.
module blobmanager::blobmanager;

use blobmanager::simple_blob_stash::{Self, SimpleBlobStash};
use blobmanager::storage_stash::{Self, StorageStash};
use walrus::blob::Blob;
use walrus::storage_resource::Storage;
use walrus::system::System;

// === Error Codes ===

/// The provided BlobManagerCap does not match the BlobManager.
const EInvalidBlobManagerCap: u64 = 0;
/// The blob provided is not certified.
const EBlobNotCertified: u64 = 1;

// === Structs ===

/// The high-level blob-management interface.
public struct BlobManager has key, store {
    id: UID,
    storage_stash: StorageStash,
    blob_stash: SimpleBlobStash,
}

/// A capability which represents the authority to manage blobs.
///
/// Most operations on the `BlobManager` are gated via this capability.
public struct BlobManagerCap has key, store {
    id: UID,
    manager_id: ID,
}

// === Constructors ===

/// Creates and shares a new empty BlobManager.
///
/// A new `BlobManagerCap` is also returned.
public fun empty(
    n_shards: u16,
    current_epoch: u32,
    max_epochs_ahead: u32,
    ctx: &mut TxContext,
): BlobManagerCap {
    let manager_id = object::new(ctx);
    let manager_uid = manager_id.to_inner();

    let manager = BlobManager {
        id: manager_id,
        storage_stash: storage_stash::empty(max_epochs_ahead, current_epoch),
        blob_stash: simple_blob_stash::empty(
            n_shards,
            ctx,
        ),
    };

    let cap = BlobManagerCap {
        id: object::new(ctx),
        manager_id: manager_uid,
    };

    transfer::share_object(manager);
    cap
}

// === Capability operations ===

/// Duplicates the given `BlobManagerCap`.
public fun duplicate_cap(cap: &BlobManagerCap, ctx: &mut TxContext): BlobManagerCap {
    BlobManagerCap {
        id: object::new(ctx),
        manager_id: cap.manager_id,
    }
}

/// Checks that the given `BlobManagerCap` matches the `BlobManager`.
fun check_cap(self: &BlobManager, cap: &BlobManagerCap) {
    assert!(self.id.to_inner() == cap.manager_id, EInvalidBlobManagerCap);
}

// === Direct blob operations ===

/// Registers the blob, using the `Storage` in the `StorageStash`.
///
/// This is a convenience wrapper around the register_blob function in the `walrus::system` module.
/// Note: The caller must provide a payment (FROST coin) for the registration.
public fun register_blob<T>(
    self: &mut BlobManager,
    system: &mut System,
    storage: Storage,
    blob_id: u256,
    root_hash: u256,
    size: u64,
    encoding_type: u8,
    deletable: bool,
    payment: &mut T,
    ctx: &mut TxContext,
): Blob {
    // Use the system to register the blob with the provided storage
    system.register_blob(
        storage,
        blob_id,
        root_hash,
        size,
        encoding_type,
        deletable,
        payment,
        ctx,
    )
}

/// Adds a `Blob` to the BlobManager.
public fun add_blob(self: &mut BlobManager, cap: &BlobManagerCap, blob: Blob) {
    check_cap(self, cap);
    // Ensure the blob is certified.
    assert!(blob.certified_epoch().is_some(), EBlobNotCertified);
    self.blob_stash.add_blob(blob);
}

/// Removes a `Blob` from the BlobManager and returns it.
public fun remove_blob(self: &mut BlobManager, cap: &BlobManagerCap, blob_id: u256): Blob {
    check_cap(self, cap);
    self.blob_stash.remove_blob(blob_id)
}

/// Borrows a `Blob` from the BlobManager.
public fun borrow_blob(self: &BlobManager, cap: &BlobManagerCap, blob_id: u256): &Blob {
    check_cap(self, cap);
    self.blob_stash.borrow_blob(blob_id)
}

/// Mutably borrows a `Blob` from the BlobManager.
public fun borrow_blob_mut(self: &mut BlobManager, cap: &BlobManagerCap, blob_id: u256): &mut Blob {
    check_cap(self, cap);
    self.blob_stash.borrow_blob_mut(blob_id)
}

/// Deletes a `Blob` from the BlobManager.
///
/// If the blob is deletable, places the storage back into the StorageStash.
public fun delete_or_burn_blob(
    self: &mut BlobManager,
    cap: &BlobManagerCap,
    blob_id: u256,
    system: &System,
    ctx: &mut TxContext,
) {
    check_cap(self, cap);
    self.blob_stash.delete_or_burn_blob(system, blob_id, &mut self.storage_stash, ctx)
}

/// Extends the `Blob`, using the `Storage` in the `StorageStash`.
/// 
/// This function can only be called with a valid `BlobManagerCap`.  For the
/// extension function that allows anyone to extend, see the
/// `extend_blob_with_tip` function.
public fun extend_blob_permissioned(
    self: &mut BlobManager,
    cap: &BlobManagerCap,
    blob_id: u256,
    extend_epochs_ahead: u32,
    system: &System,
    ctx: &mut TxContext,
) {
    check_cap(self, cap);
    // TODO: check if the storage stash has enough storage to extend the blob,
    // if not check if we can buy more storage. Otherwise, do not abort but
    // emit an event with the blob id and the amount of storage missing.
    self.blob_stash.extend_blob(blob_id, extend_epochs_ahead, system, &mut self.storage_stash, ctx)
}



// === Direct storage operations ===

/// Adds storage to the BlobManager's StorageStash.
///
/// Note: Anyone can deposit storage, no capability is needed.
public fun deposit_storage(self: &mut BlobManager, storage: Storage, ctx: &mut TxContext) {
    self.storage_stash.deposit_storage(storage, ctx)
}

/// Removes storage from the BlobManager's StorageStash and returns it.
public fun withdraw_storage(
    self: &mut BlobManager,
    cap: &BlobManagerCap,
    start_epochs_ahead: u32,
    end_epochs_ahead: u32,
    size: u64,
    ctx: &mut TxContext,
): Storage {
    check_cap(self, cap);
    self.storage_stash.withdraw_storage(start_epochs_ahead, end_epochs_ahead, size, ctx)
}

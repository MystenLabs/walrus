/// A simplified version of the blob stash, that does not automate blob extension.
module blobmanager::simple_blob_stash;

use blobmanager::storage_stash::StorageStash;
use sui::table::{Self, Table};
use walrus::blob::Blob;
use walrus::system::System;

// === Error codes ===

/// The blob with the given ID does not exist
const EBlobNotFound: u64 = 0;
/// The blob with the given ID already exists
const EBlobAlreadyExists: u64 = 1;
/// The blob stash is not empty
const ENotEmpty: u64 = 2;

// === Structs ===

/// The `SimpleBlobStash` struct that manages a collection of blobs
public struct SimpleBlobStash has store {
    /// Number of shards in the system (needed for size calculations).
    n_shards: u16,
    /// Table containing all `Blob` objects indexed by their `BlobId`
    blob_table: Table<u256, Blob>,
}

// === Public functions ===

/// Creates an empty SimpleBlobStash with the given parameters.
public(package) fun empty(
    n_shards: u16,
    ctx: &mut TxContext,
): SimpleBlobStash {
    SimpleBlobStash {
        n_shards,
        blob_table: table::new(ctx),
    }
}

/// Adds a blob to the stash.
public(package) fun add_blob(self: &mut SimpleBlobStash, blob: Blob) {
    let blob_id = blob.blob_id();
    
    // Check if blob already exists
    assert!(!self.blob_table.contains(blob_id), EBlobAlreadyExists);
    
    // Add blob to the table
    self.blob_table.add(blob_id, blob);
}

/// Removes a blob from the stash by ID.
public(package) fun remove_blob(self: &mut SimpleBlobStash, blob_id: u256): Blob {
    // Check if blob exists
    assert!(self.blob_table.contains(blob_id), EBlobNotFound);
    
    // Remove and return the blob
    self.blob_table.remove(blob_id)
}

/// Borrows a blob from the stash by ID.
public(package) fun borrow_blob(self: &SimpleBlobStash, blob_id: u256): &Blob {
    assert!(self.blob_table.contains(blob_id), EBlobNotFound);
    &self.blob_table[blob_id]
}

/// Mutably borrows a blob from the stash by ID.
public(package) fun borrow_blob_mut(self: &mut SimpleBlobStash, blob_id: u256): &mut Blob {
    assert!(self.blob_table.contains(blob_id), EBlobNotFound);
    &mut self.blob_table[blob_id]
}

/// Extends the end epoch of a blob.
///
/// As a result, the new end epoch is `extend_epochs_ahead` epochs in the future.
public(package) fun extend_blob(
    self: &mut SimpleBlobStash,
    blob_id: u256,
    extend_epochs_ahead: u32,
    system: &System,
    storage_stash: &mut StorageStash,
    ctx: &mut TxContext,
) {
    assert!(self.blob_table.contains(blob_id), EBlobNotFound);
    let blob = &mut self.blob_table[blob_id];
    
    let current_epoch = system.epoch();
    let current_end_epoch = blob.end_epoch();
    let current_epochs_ahead = if (current_end_epoch > current_epoch) {
        current_end_epoch - current_epoch
    } else {
        // The blob has already expired.
        return
    };

    if (extend_epochs_ahead <= current_epochs_ahead) {
        // No extension needed.
        return
    };
    
    let blob_size = blob.storage().size();
    let extension_storage = storage_stash.withdraw_storage(
        current_epochs_ahead,
        extend_epochs_ahead,
        blob_size,
        ctx,
    );
    
    // Extend the blob with the new storage
    system.extend_blob_with_resource(blob, extension_storage);
}

/// Deletes a blob from the stash by ID.
///
/// If the blob is deletable, it deletes the blob and reclaims the storage resource,
/// depositing it into the provided storage stash.
///
/// If the blob is not deletable, it simply removes it from the stash without reclaiming storage.
public(package) fun delete_or_burn_blob(
    self: &mut SimpleBlobStash,
    system: &System,
    blob_id: u256,
    storage_stash: &mut StorageStash,
    ctx: &mut TxContext,
) {
    // Remove the blob from the stash
    let blob = self.remove_blob(blob_id);

    if (!blob.is_deletable()) {
        // If the blob is not deletable, burn it.
        blob.burn();
    } else {
        let storage = system.delete_blob(blob);
      storage_stash.deposit_storage(storage, ctx);
    }
}

/// Destroys the SimpleBlobStash if it's empty.
public(package) fun destroy_if_empty(self: SimpleBlobStash) {
    let SimpleBlobStash {
        n_shards: _,
        blob_table,
    } = self;
    
    // Check that the stash is empty
    assert!(blob_table.is_empty(), ENotEmpty);
    
    // Destroy the table
    blob_table.destroy_empty();
}

/// Checks if the stash contains a blob with the given ID.
public(package) fun contains(self: &SimpleBlobStash, blob_id: u256): bool {
    self.blob_table.contains(blob_id)
}

/// Returns the number of blobs in the stash.
public(package) fun size(self: &SimpleBlobStash): u64 {
    self.blob_table.length()
}

/// Returns whether the stash is empty.
public(package) fun is_empty(self: &SimpleBlobStash): bool {
    self.blob_table.is_empty()
}

// === Tests ===

#[test_only]
use walrus::blob;
#[test_only]
use walrus::system;
#[test_only]
use walrus::test_utils;
#[test_only]
use walrus::messages;
#[test_only]
use blobmanager::storage_stash;

#[test]
fun test_simple_blob_stash_basic_operations() {
    let ctx = &mut tx_context::dummy();
    let n_shards: u16 = 100;
    
    // Create a simple blob stash
    let mut stash = empty(n_shards, ctx);
    
    // Create a test system
    let mut system = system::new_for_testing(ctx);
    
    // Create storage resources
    let mut payment = test_utils::mint_frost(10_000_000_000, ctx);
    let storage1 = system.reserve_space(5_000_000, 5, &mut payment, ctx);
    let storage2 = system.reserve_space(5_000_000, 5, &mut payment, ctx);
    
    // Create test blobs
    let blob_id1 = blob::derive_blob_id(0x1111, 1, 1111);
    let blob1 = system.register_blob(
        storage1,
        blob_id1,
        0x1111,
        1111,
        1,
        false,
        &mut payment,
        ctx,
    );
    
    let blob_id2 = blob::derive_blob_id(0x2222, 1, 2222);
    let blob2 = system.register_blob(
        storage2,
        blob_id2,
        0x2222,
        2222,
        1,
        false,
        &mut payment,
        ctx,
    );
    
    // Test adding blobs
    assert!(stash.is_empty(), 0);
    stash.add_blob(blob1);
    stash.add_blob(blob2);
    assert!(stash.size() == 2, 1);
    
    // Verify blobs are in the table
    assert!(stash.contains(blob_id1), 2);
    assert!(stash.contains(blob_id2), 3);
    
    // Test borrowing a blob
    let borrowed = stash.borrow_blob(blob_id1);
    assert!(borrowed.blob_id() == blob_id1, 4);
    
    // Test removing a blob
    let removed_blob = stash.remove_blob(blob_id2);
    assert!(removed_blob.blob_id() == blob_id2, 5);
    assert!(!stash.contains(blob_id2), 6);
    assert!(stash.size() == 1, 7);
    
    // Remove remaining blob
    let removed_blob1 = stash.remove_blob(blob_id1);
    assert!(stash.is_empty(), 8);
    
    // Clean up
    removed_blob.burn();
    removed_blob1.burn();
    payment.burn_for_testing();
    
    // Destroy empty stash
    stash.destroy_if_empty();
    sui::test_utils::destroy(system);
}

#[test]
fun test_simple_blob_stash_delete_blob() {
    let ctx = &mut tx_context::dummy();
    let n_shards: u16 = 100;
    let current_epoch: u32 = 0;
    let max_epochs_ahead: u32 = 10;
    
    // Create a simple blob stash
    let mut stash = empty(n_shards, ctx);
    
    // Create a test system
    let mut system = system::new_for_testing(ctx);
    
    // Create storage stash
    let mut storage_stash = storage_stash::empty(max_epochs_ahead, current_epoch);
    
    // Create storage and payment
    let mut payment = test_utils::mint_frost(10_000_000_000, ctx);
    let storage = system.reserve_space(5_000_000, 5, &mut payment, ctx);
    
    // Create a deletable test blob
    let blob_id = blob::derive_blob_id(0x3333, 1, 3333);
    let blob = system.register_blob(
        storage,
        blob_id,
        0x3333,
        3333,
        1,
        true,
        &mut payment,
        ctx,
    );
    
    // Add blob to stash
    stash.add_blob(blob);
    assert!(stash.contains(blob_id), 0);
    
    // Delete the blob and reclaim storage
    stash.delete_or_burn_blob(&system, blob_id, &mut storage_stash, ctx);
    
    // Verify blob is no longer in the table
    assert!(!stash.contains(blob_id), 1);
    assert!(stash.is_empty(), 2);
    
    // Clean up
    payment.burn_for_testing();
    stash.destroy_if_empty();
    storage_stash.destroy_for_testing();
    sui::test_utils::destroy(system);
}

#[test]
fun test_simple_blob_stash_extend_blob() {
    let ctx = &mut tx_context::dummy();
    let n_shards: u16 = 100;
    let current_epoch: u32 = 0;
    let max_epochs_ahead: u32 = 20;
    
    // Create a simple blob stash
    let mut stash = empty(n_shards, ctx);
    
    // Create a test system
    let mut system = system::new_for_testing(ctx);
    
    // Create storage stash with initial storage
    let mut storage_stash = storage_stash::empty(max_epochs_ahead, current_epoch);
    let mut payment = test_utils::mint_frost(20_000_000_000, ctx);
    
    // Add some storage to the storage stash for extensions
    let extension_storage = system.reserve_space(10_000_000, 15, &mut payment, ctx);
    storage_stash.deposit_storage(extension_storage, ctx);
    
    // Create a blob that expires in 5 epochs
    let blob_storage = system.reserve_space(5_000_000, 5, &mut payment, ctx);
    let blob_id = blob::derive_blob_id(0x5555, 1, 5555);
    let mut blob = system.register_blob(
        blob_storage,
        blob_id,
        0x5555,
        5555,
        1,
        true,
        &mut payment,
        ctx,
    );

    let message = messages::certified_deletable_blob_message_for_testing(
        blob.blob_id(),
        blob.object_id(),
    );
    blob.certify_with_certified_msg_for_testing(current_epoch, message);
    
    // Add blob to stash
    stash.add_blob(blob);
    
    // Verify initial end epoch
    let initial_end_epoch = stash.borrow_blob(blob_id).end_epoch();
    assert!(initial_end_epoch == current_epoch + 5, 0);
    
    // Extend blob to expire at epoch 10
    stash.extend_blob(
        blob_id,
        10, // extend_epochs_ahead
        &system,
        &mut storage_stash,
        ctx,
    );
    
    // Verify the blob was extended
    let extended_end_epoch = stash.borrow_blob(blob_id).end_epoch();
    assert!(extended_end_epoch == current_epoch + 10, 1);
    
    // Test extending to a shorter period (should be no-op)
    stash.extend_blob(
        blob_id,
        8, // extend_epochs_ahead (less than current 10)
        &system,
        &mut storage_stash,
        ctx,
    );
    
    // Verify the blob end epoch hasn't changed
    let same_end_epoch = stash.borrow_blob(blob_id).end_epoch();
    assert!(same_end_epoch == current_epoch + 10, 2);
    
    // Clean up - remove the extended blob
    let extended_blob = stash.remove_blob(blob_id);
    extended_blob.burn();
    payment.burn_for_testing();
    
    // Destroy empty structures
    stash.destroy_if_empty();
    storage_stash.destroy_for_testing();
    sui::test_utils::destroy(system);
}

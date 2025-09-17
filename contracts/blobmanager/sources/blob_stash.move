/// The `BlobStash` is a collection of blobs that ensures easy addition, deletion,
/// and extension of `Blob` objects.
module blobmanager::blob_stash;

use blobmanager::big_stack::{Self, BigStack};
use blobmanager::storage_stash::StorageStash;
use blobmanager::timer_wheel::{Self, TimerWheel};
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
/// Invalid epoch for blob extension
const EInvalidEpoch: u64 = 3;
/// The blob has not yet expired
const EBlobNotExpired: u64 = 4;

// === Structs ===

/// The `BlobStash` struct that manages a collection of blobs
public struct BlobStash has store {
    /// Number of shards in the system (needed for size calculations).
    n_shards: u16,
    /// Table containing all `Blob` objects indexed by their `BlobId`
    blob_table: Table<u256, Blob>,
    /// TimerWheel mapping epochs to stacks of blob IDs expiring in those epochs
    expiry_wheel: TimerWheel<BigStack<u256>>,
    /// Maximum size of each slice in the BigStack
    max_slice_size: u64,
}

// === Public functions ===

/// Creates an empty BlobStash with the given current epoch.
public(package) fun empty(
    n_shards: u16,
    current_epoch: u32,
    max_epochs_ahead: u32,
    max_slice_size: u64,
    ctx: &mut TxContext,
): BlobStash {
    BlobStash {
        n_shards,
        blob_table: table::new(ctx),
        expiry_wheel: timer_wheel::empty(max_epochs_ahead, current_epoch),
        max_slice_size,
    }
}

/// Adds a blob to the stash.
///
/// The blob is added to the table, and its ID is pushed to the timer wheel stack
/// at the entry corresponding to its expiration epoch.
public(package) fun add_blob(self: &mut BlobStash, blob: Blob, ctx: &mut TxContext) {
    let blob_id = blob.blob_id();
    let expiration_epoch = blob.end_epoch();

    // Check if blob already exists
    assert!(!self.blob_table.contains(blob_id), EBlobAlreadyExists);

    // Add blob to the table
    self.blob_table.add(blob_id, blob);

    // Get or create the stack for the expiration epoch
    let current_epoch = self.expiry_wheel.current_epoch();

    if (expiration_epoch < current_epoch) {
        // The blob has already expired. We return with a no-op.
        return
    };

    // Get the stack for this epoch, or create it if it doesn't exist
    let stack_option = self.expiry_wheel.borrow_mut_at_epoch(expiration_epoch);
    if (stack_option.is_none()) {
        // Create a new stack for this epoch
        let new_stack = big_stack::empty(self.max_slice_size, ctx);
        stack_option.fill(new_stack);
    };

    // Push the blob ID onto the stack
    let stack = stack_option.borrow_mut();
    stack.push(blob_id);
}

/// Removes a blob from the stash by ID.
///
/// This only removes the blob from the table, not from the timer wheel
/// The timer wheel entries are cleaned up during advancement or extension.
public(package) fun remove_blob(self: &mut BlobStash, blob_id: u256): Blob {
    // Check if blob exists
    assert!(self.blob_table.contains(blob_id), EBlobNotFound);

    // Remove and return the blob
    let blob = self.blob_table.remove(blob_id);

    blob
}

/// Borrows a blob from the stash by ID.
public(package) fun borrow_blob(self: &BlobStash, blob_id: u256): &Blob {
    assert!(self.blob_table.contains(blob_id), EBlobNotFound);
    &self.blob_table[blob_id]
}

/// Mutably borrows a blob from the stash by ID.
public(package) fun borrow_blob_mut(self: &mut BlobStash, blob_id: u256): &mut Blob {
    assert!(self.blob_table.contains(blob_id), EBlobNotFound);
    &mut self.blob_table[blob_id]
}

/// Deletes a blob from the stash by ID, burns it to reclaim the storage resource,
/// and deposits the storage into the provided StorageStash.
///
/// This removes the blob from the table (not from the timer wheel),
/// burns the blob to reclaim its storage, and deposits the storage
/// into the storage stash for future use.
public(package) fun delete_blob(
    self: &mut BlobStash,
    system: &System,
    blob_id: u256,
    storage_stash: &mut StorageStash,
    ctx: &mut TxContext,
) {
    // Remove the blob from the stash
    let blob = self.remove_blob(blob_id);

    // Get the storage from the blob by deleting it
    // Note: The blob must be deletable for this to work
    let storage = system.delete_blob(blob);

    // Deposit the storage into the storage stash
    storage_stash.deposit_storage(storage, ctx);
}

/// Destroys the BlobStash if it's empty.
///
/// Drops all items that may remain in the timer wheel.
public(package) fun destroy_if_empty(self: BlobStash) {
    let BlobStash {
        n_shards: _,
        blob_table,
        expiry_wheel,
        max_slice_size: _,
    } = self;

    // Check that the stash is empty
    assert!(blob_table.is_empty(), ENotEmpty);

    // Destroy the table
    blob_table.destroy_empty();

    // Destroy the timer wheel by emptying all stacks if necessary.
    let remaining = timer_wheel::destroy_with_clear(expiry_wheel);
    remaining.destroy!(|mut stack| {
        while (!stack.is_empty()) {
            let _ = stack.pop();
        };

        stack.destroy_empty()
    });
}

/// Advances the timer wheel to the target epoch, destroying expired blobs.
///
/// Returns a vector of destroyed blobs that expired during the advancement.
public(package) fun advance_to_epoch(self: &mut BlobStash, target_epoch: u32) {
    let current_epoch = self.expiry_wheel.current_epoch();
    if (target_epoch <= current_epoch) {
        abort EInvalidEpoch
    };

    let epochs_to_advance = target_epoch - current_epoch;
    self.advance_by(epochs_to_advance)
}

/// Advances the timer wheel by a specified number of epochs, destroying expired blobs.
///
/// Returns a vector of destroyed blobs that expired during the advancement.
public(package) fun advance_by(self: &mut BlobStash, epochs: u32) {
    // Advance the timer wheel and get expired stacks
    let expired_stacks = self.expiry_wheel.advance_by_and_return(epochs);

    // Process each expired stack
    expired_stacks.do!(|mut stack| {
        // Pop all blob IDs from the expired stack
        while (!stack.is_empty()) {
            let blob_id = stack.pop();

            // Check if the blob still exists in the table (may have been deleted)
            if (self.blob_table.contains(blob_id)) {
                // Remove the blob from the table and burn it.
                let blob = self.blob_table.remove(blob_id);
                if (blob.end_epoch() > self.expiry_wheel.current_epoch()) {
                    // This should never happen, but we check just in case.
                    abort EBlobNotExpired
                };

                blob.burn();
            }
        };

        // Destroy the empty stack
        stack.destroy_empty();
    });
}

/// Extends all blobs that expire before the given number of epochs ahead.
///
/// The timer wheel is iterated (but not advanced) to get all entries until the given epoch.
/// These entries are taken from the table and, if they still exist (have not been deleted),
/// they are extended using the provided StorageStash. After extension, the blob ID is pushed
/// back to the new expiry epoch in the timer wheel.
public(package) fun extend_blobs_expiring_before(
    self: &mut BlobStash,
    system: &System,
    storage_stash: &mut StorageStash,
    extension_epochs_ahead: u32,
    expiring_before_epochs_ahead: u32,
    ctx: &mut TxContext,
) {
    assert!(extension_epochs_ahead > 0, EInvalidEpoch);
    assert!(expiring_before_epochs_ahead < extension_epochs_ahead, EInvalidEpoch);

    // Ensure that the bigstack for the new epoch exists.
    let new_stack_option = self.expiry_wheel.borrow_mut(extension_epochs_ahead);
    if (new_stack_option.is_none()) {
        let new_stack = big_stack::empty(self.max_slice_size, ctx);
        new_stack_option.fill(new_stack);
    };

    let current_epoch = self.expiry_wheel.current_epoch();

    // Process each epoch from current (0) up to epochs_ahead
    let mut epoch_offset = 0;
    while (epoch_offset <= expiring_before_epochs_ahead) {
        // Take the stack for this epoch if it exists
        let stack_option = self.expiry_wheel.borrow_mut(epoch_offset);
        let mut to_push_back = vector::empty<u256>();

        if (stack_option.is_some()) {
            let stack = stack_option.borrow_mut();

            // Process all blob IDs in this stack
            while (!stack.is_empty()) {
                let blob_id = stack.pop();

                // Check if the blob still exists in the table
                if (self.blob_table.contains(blob_id)) {
                    let blob = &mut self.blob_table[blob_id];
                    let current_end_epoch = blob.end_epoch();
                    let current_end_epochs_ahead = current_end_epoch - current_epoch;

                    // Get the required storage from the stash
                    let extension_storage = storage_stash.withdraw_storage(
                        current_end_epochs_ahead,
                        extension_epochs_ahead,
                        blob.storage().size(),
                        ctx,
                    );

                    // Extend the blob with the new storage
                    system.extend_blob_with_resource(blob, extension_storage);
                    to_push_back.push_back(blob_id);
                }
            };
        };

        if (!to_push_back.is_empty()) {
            let new_stack_option = self.expiry_wheel.borrow_mut(extension_epochs_ahead);
            // SAFETY: We ensured at the beginning of the function that the stack exists.
            let new_stack = new_stack_option.borrow_mut();
            while (!to_push_back.is_empty()) {
                new_stack.push(to_push_back.pop_back());
            };
        };

        epoch_offset = epoch_offset + 1;
    };
}

// === Tests ===

#[test_only]
use walrus::blob;
#[test_only]
use walrus::system;
#[test_only]
use walrus::test_utils;
#[test_only]
use blobmanager::storage_stash;
#[test_only]
use walrus::messages;

#[test]
fun test_blob_stash_basic_operations() {
    let ctx = &mut tx_context::dummy();
    let n_shards: u16 = 100;
    let current_epoch: u32 = 0;
    let max_epochs_ahead: u32 = 10;
    let max_slice_size: u64 = 100;

    // Create a blob stash
    let mut stash = empty(n_shards, current_epoch, max_epochs_ahead, max_slice_size, ctx);

    // Create a test system
    let mut system = system::new_for_testing(ctx);

    // Create storage resources
    let mut payment = test_utils::mint_frost(10_000_000_000, ctx);
    let storage1 = system.reserve_space(5_000_000, 5, &mut payment, ctx);
    let storage2 = system.reserve_space(5_000_000, 5, &mut payment, ctx);
    let storage3 = system.reserve_space(5_000_000, 5, &mut payment, ctx);

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

    let blob_id3 = blob::derive_blob_id(0x3333, 1, 3333);
    let blob3 = system.register_blob(
        storage3,
        blob_id3,
        0x3333,
        3333,
        1,
        false,
        &mut payment,
        ctx,
    );

    // Test adding blobs
    stash.add_blob(blob1, ctx);
    stash.add_blob(blob2, ctx);
    stash.add_blob(blob3, ctx);

    // Verify blobs are in the table
    assert!(stash.blob_table.contains(blob_id1), 0);
    assert!(stash.blob_table.contains(blob_id2), 1);
    assert!(stash.blob_table.contains(blob_id3), 2);

    // Test removing a blob
    let removed_blob = stash.remove_blob(blob_id2);
    assert!(removed_blob.blob_id() == blob_id2, 3);
    assert!(!stash.blob_table.contains(blob_id2), 4);

    // Remove remaining blobs
    let removed_blob1 = stash.remove_blob(blob_id1);
    let removed_blob3 = stash.remove_blob(blob_id3);

    // Clean up
    removed_blob.burn();
    removed_blob1.burn();
    removed_blob3.burn();
    payment.burn_for_testing();

    // Destroy empty stash
    stash.destroy_if_empty();
    sui::test_utils::destroy(system);
}

#[test]
fun test_delete_blob_with_storage_stash() {
    let ctx = &mut tx_context::dummy();
    let n_shards: u16 = 100;
    let current_epoch: u32 = 0;
    let max_epochs_ahead: u32 = 10;
    let max_slice_size: u64 = 100;

    // Create a blob stash
    let mut stash = empty(n_shards, current_epoch, max_epochs_ahead, max_slice_size, ctx);

    // Create a test system
    let mut system = system::new_for_testing(ctx);

    // Create storage stash
    let mut storage_stash = storage_stash::empty(max_epochs_ahead, current_epoch);

    // Create storage and payment
    let mut payment = test_utils::mint_frost(10_000_000_000, ctx);
    let storage = system.reserve_space(5_000_000, 5, &mut payment, ctx);

    // Create a test blob
    let blob_id = blob::derive_blob_id(0x4444, 1, 4444);
    let blob = system.register_blob(
        storage,
        blob_id,
        0x4444,
        4444,
        1,
        true,
        &mut payment,
        ctx,
    );

    // Add blob to stash
    stash.add_blob(blob, ctx);

    // Verify blob is in the table
    assert!(stash.blob_table.contains(blob_id), 0);

    // Delete the blob and reclaim storage
    stash.delete_blob(&system, blob_id, &mut storage_stash, ctx);

    // Verify blob is no longer in the table
    assert!(!stash.blob_table.contains(blob_id), 1);

    // Verify we can withdraw the reclaimed storage from the stash
    let withdrawn_storage = storage_stash.withdraw_storage(0, 5, 5_000_000, ctx);
    assert!(withdrawn_storage.size() == 5_000_000, 2);
    assert!(withdrawn_storage.start_epoch() == current_epoch, 3);
    assert!(withdrawn_storage.end_epoch() == current_epoch + 5, 4);
    withdrawn_storage.destroy();

    // Clean up
    payment.burn_for_testing();
    stash.destroy_if_empty();
    storage_stash.destroy_empty();
    sui::test_utils::destroy(system);
}

#[test]
fun test_extend_blobs_expiring_before() {
    let ctx = &mut tx_context::dummy();
    let n_shards: u16 = 100;
    let current_epoch: u32 = 0;
    let max_epochs_ahead: u32 = 20;
    let max_slice_size: u64 = 100;
    
    // Create a blob stash
    let mut stash = empty(n_shards, current_epoch, max_epochs_ahead, max_slice_size, ctx);
    
    // Create a test system
    let mut system = system::new_for_testing(ctx);
    
    // Create storage stash with initial storage
    let mut storage_stash = storage_stash::empty(max_epochs_ahead, current_epoch);
    let mut payment = test_utils::mint_frost(20_000_000_000, ctx);
    
    // Add some storage to the storage stash for extensions
    let extension_storage = system.reserve_space(10_000_000, 10, &mut payment, ctx);
    storage_stash.deposit_storage(extension_storage, ctx);
    
    // Create a blob that expires in 3 epochs (expiring soon)
    let blob_storage = system.reserve_space(5_000_000, 3, &mut payment, ctx);
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
    stash.add_blob(blob, ctx);
    
    // Verify initial end epoch
    let initial_end_epoch = stash.blob_table[blob_id].end_epoch();
    assert!(initial_end_epoch == current_epoch + 3, 0);
    
    // Extend blobs expiring before epoch 5 to expire at epoch 10
    stash.extend_blobs_expiring_before(
        &system,
        &mut storage_stash,
        10, // extension_epochs_ahead
        5,  // expiring_before_epochs_ahead
        ctx,
    );
    
    // Verify the blob was extended
    let extended_end_epoch = stash.blob_table[blob_id].end_epoch();
    assert!(extended_end_epoch == current_epoch + 10, 1);
    
    // Clean up - remove the extended blob
    let extended_blob = stash.remove_blob(blob_id);
    extended_blob.burn();
    payment.burn_for_testing();
    
    // Destroy empty structures
    stash.destroy_if_empty();
    storage_stash.destroy_for_testing();
    sui::test_utils::destroy(system);
}


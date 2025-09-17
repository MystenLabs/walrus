// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/// The `StorageStash` is a struct to manage `Storage` resources across epochs.
///
/// It uses the TimerWheel data structure to organize storage resources by epoch
/// and provides functionality to deposit, withdraw, and advance through epochs.
module blobmanager::storage_stash;

use blobmanager::timer_wheel::{Self, TimerWheel};
use walrus::storage_resource::Storage;

// === Error codes ===

/// The requested size exceeds the available storage for the epoch.
const EInsufficientStorage: u64 = 0;
/// Cannot withdraw from past epochs.
const EInvalidEpoch: u64 = 1;
/// There is still some storage left in the stash, cannot destroy.
const EStorageNotEmpty: u64 = 2;

/// The `StorageStash` manages `Storage` resources across multiple epochs.
///
/// It uses a TimerWheel data structure. Each epoch contains a consolidated
/// `Storage` object representing the total available storage for that epoch.
public struct StorageStash has store {
    /// The timer wheel that holds Storage objects for different epochs
    storage_wheel: TimerWheel<Storage>,
}

/// Creates a new `StorageStash` with the given maximum number of epochs
/// and current epoch.
public(package) fun empty(max_epochs: u32, current_epoch: u32): StorageStash {
    StorageStash {
        storage_wheel: timer_wheel::empty(max_epochs, current_epoch),
    }
}

/// Returns the current epoch of the storage stash.
public(package) fun current_epoch(self: &StorageStash): u32 {
    self.storage_wheel.current_epoch()
}

/// Returns the maximum number of epochs the stash can handle.
public(package) fun max_epochs(self: &StorageStash): u32 {
    self.storage_wheel.max_epochs_ahead()
}

/// Deposits a `Storage` resource into the stash. The storage is split epoch by epoch
/// for each epoch it spans, and the corresponding Storage object is merged to the
/// Storage object of that epoch. Thus, the stash becomes a "histogram" with a
/// Storage object per epoch, of different sizes.
public(package) fun deposit_storage(self: &mut StorageStash, storage: Storage, ctx: &mut TxContext) {
    let end_epoch = storage.end_epoch();
    let current_epoch = self.storage_wheel.current_epoch();

    // If the entire storage is in the past or current epoch, destroy it
    if (end_epoch <= current_epoch) {
        storage.destroy();
        return
    };

    let mut remaining_storage = storage;

    // Break storage into 1-epoch chunks
    while (remaining_storage.start_epoch() + 1 < remaining_storage.end_epoch()) {
        let chunk_end_epoch = remaining_storage.start_epoch() + 1;
        let excess = remaining_storage.split_by_epoch(chunk_end_epoch, ctx);
        let chunk = remaining_storage;
        remaining_storage = excess;

        // Check if this chunk is in the past
        if (chunk.end_epoch() <= current_epoch) {
            chunk.destroy();
        } else {
            // Store the chunk in the appropriate slot
            let existing_option_mut = self.storage_wheel.borrow_mut_at_epoch(chunk.start_epoch());
            if (existing_option_mut.is_some()) {
                existing_option_mut.borrow_mut().fuse(chunk);
            } else {
                existing_option_mut.fill(chunk);
            };
        };
    };

    // We are sure that at least the last chunk is not in the past, as we have
    // checked that the storage did not end in the past.
    let existing_option_mut = self
        .storage_wheel
        .borrow_mut_at_epoch(remaining_storage.start_epoch());
    if (existing_option_mut.is_some()) {
        existing_option_mut.borrow_mut().fuse(remaining_storage);
    } else {
        existing_option_mut.fill(remaining_storage);
    };
}

/// Withdraws storage for a number of epochs ahead and a certain size. The algorithm
/// iterates over each epoch ahead, splits a piece of storage of appropriate size,
/// and merges them all into one. Aborts if insufficient storage is available.
public(package) fun withdraw_storage(
    self: &mut StorageStash,
    start_epochs_ahead: u32,
    end_epochs_ahead: u32,
    size: u64,
    ctx: &mut TxContext,
): Storage {
    assert!(end_epochs_ahead > 0 &&
    end_epochs_ahead > start_epochs_ahead, EInvalidEpoch);

    // Get the storage piece from the first epoch (current epoch, which is 0 epochs ahead)
    let first_epoch_option_mut = self.storage_wheel.borrow_mut(start_epochs_ahead);
    assert!(first_epoch_option_mut.is_some(), EInsufficientStorage);

    let first_epoch_storage = first_epoch_option_mut.borrow_mut();
    let first_epoch_storage_size = first_epoch_storage.size();
    assert!(first_epoch_storage_size >= size, EInsufficientStorage);

    let mut accumulated_storage = first_epoch_storage.split_by_size(
        first_epoch_storage_size - size,
        ctx,
    );

    // Iterate through remaining epochs
    let mut epoch_offset = start_epochs_ahead + 1;
    while (epoch_offset < end_epochs_ahead) {
        let epoch_option_mut = self.storage_wheel.borrow_mut(epoch_offset);
        assert!(epoch_option_mut.is_some(), EInsufficientStorage);

        let epoch_storage = epoch_option_mut.borrow_mut();
        let epoch_storage_size = epoch_storage.size();
        assert!(epoch_storage_size >= size, EInsufficientStorage);

        let piece = epoch_storage.split_by_size(epoch_storage_size - size, ctx);
        accumulated_storage.fuse_periods(piece);

        epoch_offset = epoch_offset + 1;
    };

    accumulated_storage
}

/// Advances the timer wheel by a specified number of epochs, returning the
/// storage objects of the expired epochs instead of destroying them.
public(package) fun advance_by(self: &mut StorageStash, epochs: u32): vector<Storage> {
    self.storage_wheel.advance_by_and_return(epochs)
}

/// Advances the timer wheel to a specific epoch, returning the storage objects
/// of the expired epochs instead of destroying them.
public(package) fun advance_to_epoch(self: &mut StorageStash, target_epoch: u32): vector<Storage> {
    if (target_epoch > self.storage_wheel.current_epoch()) {
        self.storage_wheel.advance_to_epoch_and_return(target_epoch)
    } else {
        vector[]
    }
}

/// Checks if the stash has any storage available for a given epoch offset.
public(package) fun has_storage_for_epoch(self: &StorageStash, epochs_ahead: u32): bool {
    self.storage_wheel.contains(epochs_ahead)
}

/// Gets the available storage size for a given epoch offset, returns 0 if no storage exists.
public(package) fun get_storage_size_for_epoch(self: &StorageStash, epochs_ahead: u32): u64 {
    if (self.storage_wheel.contains(epochs_ahead)) {
        let storage_option = self.storage_wheel.borrow(epochs_ahead);
        storage_option.borrow().size()
    } else {
        0
    }
}

/// Returns the total storage size across all epochs.
public(package) fun total_storage_size(self: &StorageStash): u64 {
    let mut total_size = 0;
    let mut epoch_offset = 0;

    while (epoch_offset < self.storage_wheel.max_epochs_ahead()) {
        if (self.storage_wheel.contains(epoch_offset)) {
            let storage_option = self.storage_wheel.borrow(epoch_offset);
            total_size = total_size + storage_option.borrow().size();
        };
        epoch_offset = epoch_offset + 1;
    };

    total_size
}

public(package) fun destroy_empty(self: StorageStash) {
    let StorageStash { storage_wheel } = self;
    let returned = storage_wheel.destroy_with_clear();
    // Destroy any remaining storage objects, ensuring they have 0 storage resources left
    returned.destroy!(
        |storage| { assert!(storage.size() == 0, EStorageNotEmpty); storage.destroy() },
    );
}

#[test_only]
public(package) fun destroy_for_testing(self: StorageStash) {
    let StorageStash { storage_wheel } = self;
    let remaining_storage = timer_wheel::destroy_with_clear(storage_wheel);
    // Destroy any remaining storage objects
    remaining_storage.destroy!(|storage| storage.destroy());
}

#[test_only]
use walrus::storage_resource;

#[test]
fun test_storage_stash_basic_operations() {
    let mut ctx = tx_context::dummy();
    let mut stash = empty(10, 100);

    // Create a storage resource spanning epochs 100-103
    let storage = storage_resource::create_for_test(100, 103, 1000, &mut ctx);

    // Debug: check storage properties before deposit
    assert!(storage.start_epoch() == 100, 100);
    assert!(storage.end_epoch() == 103, 101);
    assert!(storage.size() == 1000, 102);

    // Deposit the storage
    stash.deposit_storage(storage, &mut ctx);

    // Check that storage exists for epochs 100, 101, 102, 103
    assert!(stash.has_storage_for_epoch(0), 0);
    assert!(stash.has_storage_for_epoch(1), 1);
    assert!(stash.has_storage_for_epoch(2), 2);
    assert!(!stash.has_storage_for_epoch(3), 3);

    // Check storage sizes
    assert!(stash.get_storage_size_for_epoch(0) == 1000, 4);
    assert!(stash.get_storage_size_for_epoch(1) == 1000, 5);
    assert!(stash.get_storage_size_for_epoch(2) == 1000, 6);

    // Check total storage size
    assert!(stash.total_storage_size() == 3000, 7);

    stash.destroy_for_testing();
}

#[test]
fun test_storage_stash_withdraw() {
    let mut ctx = tx_context::dummy();
    let mut stash = empty(10, 100);

    // Create and deposit storage
    let storage = storage_resource::create_for_test(100, 104, 1000, &mut ctx);
    stash.deposit_storage(storage, &mut ctx);

    // Withdraw storage for 2 epochs with size 500
    let withdrawn = stash.withdraw_storage(1, 3, 500, &mut ctx);

    // Check the withdrawn storage properties
    assert!(withdrawn.size() == 500, 0);
    assert!(withdrawn.start_epoch() == 101, 1);
    assert!(withdrawn.end_epoch() == 103, 2);

    // Check remaining storage
    assert!(stash.get_storage_size_for_epoch(0) == 1000, 3);
    assert!(stash.get_storage_size_for_epoch(1) == 500, 4);
    assert!(stash.get_storage_size_for_epoch(2) == 500, 5);
    assert!(stash.get_storage_size_for_epoch(3) == 1000, 6);
    assert!(stash.get_storage_size_for_epoch(4) == 0, 7);

    withdrawn.destroy();
    stash.destroy_for_testing();
}

#[test]
fun test_storage_stash_advance_epochs() {
    let mut ctx = tx_context::dummy();
    let mut stash = empty(10, 100);

    // Create and deposit storage
    let storage = storage_resource::create_for_test(100, 105, 1000, &mut ctx);
    stash.deposit_storage(storage, &mut ctx);

    // Advance by 2 epochs and get the expired storage objects
    let expired_storage = stash.advance_by(2);

    // Check that current epoch is now 102
    assert!(stash.current_epoch() == 102, 0);

    // Check that we got back the expired storage objects
    // The expired storage should contain storage from epochs 100 and 101
    assert!(expired_storage.length() == 2, 1);

    // Clean up expired storage
    expired_storage.do!(|storage| storage.destroy());

    stash.destroy_for_testing();
}

#[test]
fun test_storage_stash_advance_to_epoch() {
    let mut ctx = tx_context::dummy();
    let mut stash = empty(10, 100);

    // Create and deposit storage
    let storage = storage_resource::create_for_test(100, 105, 1000, &mut ctx);
    stash.deposit_storage(storage, &mut ctx);

    // Advance to epoch 103 and get the expired storage objects
    let expired_storage = stash.advance_to_epoch(103);

    // Check that current epoch is now 103
    assert!(stash.current_epoch() == 103, 0);

    // Check that we got back the expired storage objects
    // The expired storage should contain storage from epochs 100, 101, and 102
    assert!(expired_storage.length() == 3, 1);

    // Clean up expired storage
    expired_storage.do!(|storage| storage.destroy());

    stash.destroy_for_testing();
}

#[test]
#[expected_failure(abort_code = EInsufficientStorage)]
fun test_storage_stash_insufficient_storage() {
    let mut ctx = tx_context::dummy();
    let mut stash = empty(10, 100);

    // Create storage with size 500
    let storage = storage_resource::create_for_test(100, 102, 500, &mut ctx);
    stash.deposit_storage(storage, &mut ctx);

    // Try to withdraw 600 (more than available)
    let withdrawn = stash.withdraw_storage(0, 2, 600, &mut ctx);
    withdrawn.destroy();

    stash.destroy_for_testing();
}

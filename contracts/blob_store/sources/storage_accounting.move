// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

module blob_store::storage_accounting {

    use sui::balance::{Self, Balance};
    use std::vector::{Self};

    // Errors
    const ERROR_INDEX_OUT_OF_BOUNDS : u64 = 3;

    /// Holds information about a future epoch, namely how much
    /// storage needs to be reclaimed and the rewards to be distributed.
    struct FutureAccounting<phantom TAG, phantom WAL> has store {
        epoch: u64,
        storage_to_reclaim: u64,
        rewards_to_distribute: Balance<WAL>,
    }

    /// Constructor for FutureAccounting
    public fun new_future_accounting<TAG, WAL>(
        epoch: u64,
        storage_to_reclaim: u64,
        rewards_to_distribute: Balance<WAL>,
    ) : FutureAccounting<TAG, WAL> {
        FutureAccounting {
            epoch,
            storage_to_reclaim,
            rewards_to_distribute,
        }
    }

    /// Accessor for epoch, read-only
    public fun epoch<TAG, WAL>(accounting: &FutureAccounting<TAG, WAL>) : u64 {
        *&accounting.epoch
    }

    /// Accessor for storage_to_reclaim, mutable.
    public fun storage_to_reclaim<TAG, WAL>(
            accounting: &mut FutureAccounting<TAG, WAL>)
            : u64 {
        accounting.storage_to_reclaim
    }

    /// Increase storage to reclaim
    public fun increase_storage_to_reclaim<TAG, WAL>(
            accounting: &mut FutureAccounting<TAG, WAL>,
            amount: u64) {
        accounting.storage_to_reclaim = accounting.storage_to_reclaim + amount;
    }

    /// Accessor for rewards_to_distribute, mutable.
    public fun rewards_to_distribute<TAG, WAL>(
            accounting: &mut FutureAccounting<TAG, WAL>)
            : &mut Balance<WAL> {
        &mut accounting.rewards_to_distribute
    }

    /// Destructor for FutureAccounting, when empty.
    public fun delete_empty_future_accounting<TAG, WAL>(
        self: FutureAccounting<TAG, WAL>,
    ) {
        let FutureAccounting {
            epoch: _,
            storage_to_reclaim: _,
            rewards_to_distribute,
        } = self;

        balance::destroy_zero(rewards_to_distribute)
    }

    #[test_only]
    public fun burn_for_testing<TAG, WAL>(
        self: FutureAccounting<TAG, WAL>,
    ) {
        let FutureAccounting {
            epoch: _,
            storage_to_reclaim: _,
            rewards_to_distribute,
        } = self;

        balance::destroy_for_testing(rewards_to_distribute);
    }

    /// A ring buffer holding future accounts for a continuous range of epochs.
    struct FutureAccountingRingBuffer<phantom TAG, phantom WAL> has store {
        current_index : u64,
        length : u64,
        ring_buffer : vector<FutureAccounting<TAG, WAL>>,
    }

    /// Constructor for FutureAccountingRingBuffer
    public fun ring_new<TAG, WAL>(
        length: u64,
    ) : FutureAccountingRingBuffer<TAG, WAL> {

        let ring_buffer :  vector<FutureAccounting<TAG,WAL>> = vector::empty();
        let i = 0;
        while (i < length) {
            vector::push_back(&mut ring_buffer, FutureAccounting {
                epoch: i,
                storage_to_reclaim: 0,
                rewards_to_distribute: balance::zero(),
            });
            i = i + 1;
        };

        FutureAccountingRingBuffer {
            current_index: 0,
            length: length,
            ring_buffer: ring_buffer,
        }
    }

    /// Lookup an entry a number of epochs in the future.
    public fun ring_lookup_mut<TAG, WAL>(
        self: &mut FutureAccountingRingBuffer<TAG, WAL>,
        epochs_in_future: u64,
    ) : &mut FutureAccounting<TAG, WAL> {

        // Check for out-of-bounds access.
        assert!(epochs_in_future < self.length, ERROR_INDEX_OUT_OF_BOUNDS);

        let actual_index = (epochs_in_future + self.current_index) % self.length;
        vector::borrow_mut(&mut self.ring_buffer, actual_index)
    }

    public fun ring_pop_expand<TAG,WAL>(
        self: &mut FutureAccountingRingBuffer<TAG, WAL>)
        : FutureAccounting<TAG, WAL> {

        // Get current epoch
        let current_index = self.current_index;
        let current_epoch = vector::borrow_mut(&mut self.ring_buffer, current_index).epoch;

        // Expand the ring buffer
        vector::push_back(&mut self.ring_buffer, FutureAccounting {
                epoch: current_epoch + self.length,
                storage_to_reclaim: 0,
                rewards_to_distribute: balance::zero(),
            });

        // Now swap remove the current element and increment the current_index
        let accounting = vector::swap_remove(&mut self.ring_buffer, current_index);
        self.current_index = (current_index + 1) % self.length;

        accounting
    }
}

// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

module blob_store::system {

    use sui::object::{Self, UID};
    use sui::balance::{Self};
    use sui::coin::{Self, Coin};
    use sui::table::{Self, Table};
    use sui::tx_context::{TxContext};
    use std::option::{Self, Option};


    use blob_store::committee::{Self, Committee};
    use blob_store::storage_accounting::{Self, FutureAccounting, FutureAccountingRingBuffer};
    use blob_store::storage_resource::{Self, Storage};

    // Errors
    const ERROR_INCORRECT_COMMITTEE : u64 = 0;
    const ERROR_SYNC_EPOCH_CHANGE : u64 = 1;
    const ERROR_INVALID_PERIODS_AHEAD : u64 = 2;
    const ERROR_STORAGE_EXCEEDED : u64 = 3;


    #[allow(unused_const)]
    const EPOCH_STATUS_DONE : u8 = 0;
    #[allow(unused_const)]
    const EPOCH_STATUS_SYNC : u8 = 1;

    /// The maximum number of periods ahead we allow for storage reservations.
    /// TODO: the number here is a placeholder, and assumes an epoch is a week,
    /// and therefore 2 x 52 weeks = 2 years.
    const MAX_PERIODS_AHEAD : u64 = 104;

    #[allow(unused_field)]
    struct System<phantom TAG, phantom WAL:store> has key, store {

        id: UID,

        /// The current committee, with the current epoch.
        /// The option is always Some, but need it for swap.
        current_committee: Option<Committee<TAG>>,

        /// When we first enter the current epoch we SYNC,
        /// and then we are DONE after a cert from a quorum.
        epoch_status: u8,

        // Some accounting
        total_capacity_size : u64,
        used_capacity_size : u64,

        /// The price per unit size of storage.
        /// to support simple direct buy.
        price_per_unit_size: u64,

        /// Tables about the future and the past.
        past_committees: Table<u64, Committee<TAG>>,
        future_accounting: FutureAccountingRingBuffer<TAG,WAL>,
    }

    /// Get epoch. Uses the committee to get the epoch.
    public fun epoch<TAG, WAL:store>(
        self: &System<TAG, WAL>
    ) : u64 {
        committee::epoch(option::borrow(&self.current_committee))
    }

    /// Accessor for total capacity size.
    public fun total_capacity_size<TAG, WAL:store>(
        self: &System<TAG, WAL>
    ) : u64 {
        self.total_capacity_size
    }

    /// Accessor for used capacity size.
    public fun used_capacity_size<TAG, WAL:store>(
        self: &System<TAG, WAL>
    ) : u64 {
        self.used_capacity_size
    }

    /// A privileged constructor ensures we can build the type TAG and provides
    /// an initial system object, at epoch 0 with a given committee, and a given
    /// capacity and price.
    public fun new<TAG, WAL:store>(
        _witness: &TAG, // Ensures the caller can construct this type.
        first_committee: Committee<TAG>,
        capacity: u64,
        price: u64,
        ctx: &mut TxContext
    ) : System<TAG, WAL> {

        assert!(committee::epoch(&first_committee) == 0, ERROR_INCORRECT_COMMITTEE);

        System {
            id: object::new(ctx),
            current_committee: option::some(first_committee),
            epoch_status: EPOCH_STATUS_DONE,
            total_capacity_size: capacity,
            used_capacity_size: 0,
            price_per_unit_size: price,
            past_committees: table::new(ctx),
            future_accounting: storage_accounting::ring_new(MAX_PERIODS_AHEAD),
        }
    }

    /// An accessor for the current committee.
    public fun current_committee<TAG, WAL:store>(
        self: &System<TAG, WAL>
    ) : &Committee<TAG> {
        option::borrow(&self.current_committee)
    }

    /// Update epoch to next epoch, and also update the committee, price and capacity.
    public fun next_epoch<TAG, WAL:store>(
        self: &mut System<TAG, WAL>,
        new_committee: Committee<TAG>,
        new_capacity: u64,
        new_price: u64,
    ) : FutureAccounting<TAG, WAL> {

        // Must be in DONE state to move epochs. This is the way.
        assert!(self.epoch_status == EPOCH_STATUS_DONE, ERROR_SYNC_EPOCH_CHANGE);

        // Check new committee is valid, the existence of a committee for the next epoch
        // is proof that the time has come to move epochs.
        let old_epoch = epoch(self);
        let new_epoch = old_epoch + 1;
        assert!(committee::epoch(&new_committee) == new_epoch, ERROR_INCORRECT_COMMITTEE);
        let old_committee = option::swap(&mut self.current_committee, new_committee);

        // Add the old committee to the past_committees table.
        table::add(&mut self.past_committees, old_epoch, old_committee);

        // Update the system object.
        self.total_capacity_size = new_capacity;
        self.price_per_unit_size = new_price;
        self.epoch_status = EPOCH_STATUS_SYNC;

        let accounts_old_epoch = storage_accounting::ring_pop_expand(&mut self.future_accounting);
        assert!(storage_accounting::epoch(&accounts_old_epoch) == old_epoch,
            ERROR_SYNC_EPOCH_CHANGE);

        // Update storage based on the accounts data.
        self.used_capacity_size = self.used_capacity_size
            - storage_accounting::storage_to_reclaim(&mut accounts_old_epoch);

        accounts_old_epoch
    }

    /// Allow buying a storage reservation for a given period of epochs.
    public fun reserve_space<TAG, WAL:store>(
        self: &mut System<TAG, WAL>,
        storage_amount: u64,
        periods_ahead: u64,
        payment: Coin<WAL>,
        ctx: &mut TxContext,
        ) : (Storage<TAG>, Coin<WAL>) {

        // Check the period is within the allowed range.
        assert!(periods_ahead > 0, ERROR_INVALID_PERIODS_AHEAD);
        assert!(periods_ahead <= MAX_PERIODS_AHEAD, ERROR_INVALID_PERIODS_AHEAD);

        // Check capacity is available.
        assert!(storage_amount <= storage_resource::max_storage_amount(), ERROR_STORAGE_EXCEEDED);
        assert!(self.used_capacity_size + storage_amount <= self.total_capacity_size,
            ERROR_STORAGE_EXCEEDED);

        // Pay rewards for each future epoch into the future accounting.
        let period_payment_due = self.price_per_unit_size * storage_amount;
        let coin_balance = coin::balance_mut(&mut payment);

        let i = 0;
        while (i < periods_ahead) {
            let accounts = storage_accounting::ring_lookup_mut(&mut self.future_accounting, i);

            // Distribute rewards
            let rewards_balance = storage_accounting::rewards_to_distribute(accounts);
            // Note this will abort if the balance is not enough.
            let epoch_payment = balance::split(coin_balance, period_payment_due);
            balance::join(rewards_balance, epoch_payment);

            i = i + 1;
        };

        // Update the storage accounting.
        self.used_capacity_size = self.used_capacity_size + storage_amount;

        // Account the space to reclaim in the future.
        let final_account = storage_accounting::ring_lookup_mut(
            &mut self.future_accounting, periods_ahead - 1);
        storage_accounting::increase_storage_to_reclaim(final_account, storage_amount);

        let self_epoch = epoch(self);
        (storage_resource::create_storage(
            self_epoch,
            self_epoch + periods_ahead,
            storage_amount,
            ctx,
        ),
        payment)
    }

    #[test_only]
    public fun set_done_for_testing<TAG, WAL:store>(
        self: &mut System<TAG, WAL>
    ) {
        self.epoch_status = EPOCH_STATUS_DONE;
    }



}

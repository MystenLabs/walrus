// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[allow(unused_variable, unused_mut_parameter, unused_field)]
module walrus::system_state_inner;
use sui::coin::Coin;
use sui::sui::SUI;
use walrus::committee::{Self, Committee};
use walrus::storage_accounting::{Self, FutureAccountingRingBuffer};
use walrus::storage_node::StorageNodeCap;
use walrus::storage_resource::{Self, Storage};


/// The maximum number of periods ahead we allow for storage reservations.
/// TODO: the number here is a placeholder, and assumes an epoch is a week,
/// and therefore 2 x 52 weeks = 2 years.
const MAX_PERIODS_AHEAD: u64 = 104;

// Keep in sync with the same constant in `crates/walrus-sui/utils.rs`.
const BYTES_PER_UNIT_SIZE: u64 = 1_024;

// Errors
const ENotImplemented: u64 = 0;
const EStorageExceeded: u64 = 1;
const EInvalidPeriodsAhead: u64 = 2;

// Epoch status values
const EPOCH_STATUS_DONE: u8 = 0;
#[allow(unused_const)]
const EPOCH_STATUS_SYNC: u8 = 1;

/// The inner object that is not present in signatures and can be versioned.
#[allow(unused_field)]
public struct SystemStateInnerV1 has store {
    /// The current committee, with the current epoch.
    /// The option is always Some, but need it for swap.
    current_committee: Option<Committee>,
    /// When we first enter the current epoch we SYNC,
    /// and then we are DONE after a cert from a quorum.
    epoch_status: u8,
    // Some accounting
    total_capacity_size: u64,
    used_capacity_size: u64,
    /// The price per unit size of storage.
    price_per_unit_size: u64,
    /// The previous committee.
    previous_committee: Option<Committee>,
    /// Accounting ring buffer for future epochs.
    future_accounting: FutureAccountingRingBuffer,
}

public(package) fun epoch_sync_done(
    self: &mut SystemStateInnerV1,
    cap: &StorageNodeCap,
    epoch_number: u64,
) {
    abort ENotImplemented
}

public(package) fun shard_transfer_failed(
    self: &mut SystemStateInnerV1,
    cap: &StorageNodeCap,
    node_identity: vector<u8>,
    shard_ids: vector<u16>,
) {
    abort ENotImplemented
}

/// Allow buying a storage reservation for a given period of epochs.
public(package) fun reserve_space(
    self: &mut SystemStateInnerV1,
    storage_amount: u64,
    periods_ahead: u64,
    payment: &mut Coin<SUI>,
    ctx: &mut TxContext,
): Storage {
    // Check the period is within the allowed range.
    assert!(periods_ahead > 0, EInvalidPeriodsAhead);
    assert!(periods_ahead <= MAX_PERIODS_AHEAD, EInvalidPeriodsAhead);

    // Check capacity is available.
    assert!(
        self.used_capacity_size + storage_amount <= self.total_capacity_size,
        EStorageExceeded,
    );

    // Pay rewards for each future epoch into the future accounting.
    let storage_units = (storage_amount + BYTES_PER_UNIT_SIZE - 1) / BYTES_PER_UNIT_SIZE;
    let period_payment_due = self.price_per_unit_size * storage_units;
    let coin_balance = payment.balance_mut();

    let mut i = 0;
    while (i < periods_ahead) {
        let accounts = self.future_accounting.ring_lookup_mut(i);

        // Distribute rewards
        let rewards_balance = accounts.rewards_to_distribute();
        // Note this will abort if the balance is not enough.
        let epoch_payment = coin_balance.split(period_payment_due);
        rewards_balance.join(epoch_payment);

        i = i + 1;
    };

    // Update the storage accounting.
    self.used_capacity_size = self.used_capacity_size + storage_amount;

    // Account the space to reclaim in the future.
    let final_account = self.future_accounting.ring_lookup_mut(periods_ahead - 1);
    final_account.increase_storage_to_reclaim(storage_amount);

    let self_epoch = epoch(self);

    storage_resource::create_storage(
        self_epoch,
        self_epoch + periods_ahead,
        storage_amount,
        ctx,
    )
}

public(package) fun invalidate_blob_id(
    self: &mut SystemStateInnerV1,
    signature: vector<u8>,
    members: vector<u16>,
    message: vector<u8>,
) {
    abort ENotImplemented
}

public(package) fun certify_event_blob(
    self: &mut SystemStateInnerV1,
    cap: &StorageNodeCap,
    blob_id: u256,
    size: u64,
) {
    abort ENotImplemented
}

// === Accessors ===

/// Get epoch. Uses the committee to get the epoch.
public(package) fun epoch(self: &SystemStateInnerV1): u64 {
    self.current_committee.borrow().epoch()
}

/// Accessor for total capacity size.
public(package) fun total_capacity_size(self: &SystemStateInnerV1): u64 {
    self.total_capacity_size
}

/// Accessor for used capacity size.
public(package) fun used_capacity_size(self: &SystemStateInnerV1): u64 {
    self.used_capacity_size
}

/// An accessor for the current committee.
public(package) fun current_committee(self: &SystemStateInnerV1): &Committee {
    self.current_committee.borrow()
}

public(package) fun n_shards(self: &SystemStateInnerV1): u16 {
    current_committee(self).n_shards()
}

// == Testing ==

#[test_only]
public(package) fun new_for_testing(ctx: &mut TxContext): SystemStateInnerV1 {
    let committee = committee::committee_for_testing(0);
    SystemStateInnerV1 {
        current_committee: option::some(committee),
        epoch_status: EPOCH_STATUS_DONE,
        total_capacity_size: 1_000_000_000,
        used_capacity_size: 0,
        price_per_unit_size: 5,
        previous_committee: option::none(),
        future_accounting: storage_accounting::ring_new(MAX_PERIODS_AHEAD),
    }
}

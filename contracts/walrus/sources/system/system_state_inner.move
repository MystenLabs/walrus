// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[allow(unused_variable, unused_mut_parameter, unused_field)]
module walrus::system_state_inner;

use sui::{balance::Balance, coin::Coin, sui::SUI};
use walrus::{
    blob::{Self, Blob},
    blob_events::emit_invalid_blob_id,
    committee::{Self, Committee},
    storage_accounting::{Self, FutureAccountingRingBuffer},
    storage_node::StorageNodeCap,
    storage_resource::{Self, Storage}
};

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
const EInvalidIdEpoch: u64 = 3;
const EIncorrectCommittee: u64 = 4;
const EInvalidAccountingEpoch: u64 = 5;

/// The inner object that is not present in signatures and can be versioned.
#[allow(unused_field)]
public struct SystemStateInnerV1 has store {
    /// The current committee, with the current epoch.
    /// The option is always Some, but need it for swap.
    current_committee: Option<Committee>,
    // Some accounting
    total_capacity_size: u64,
    used_capacity_size: u64,
    /// The price per unit size of storage.
    storage_price_per_unit_size: u64,
    /// The write price per unit size.
    write_price_per_unit_size: u64,
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

/// Update epoch to next epoch, and update the committee, price and capacity.
///
/// Called by the epoch change function that connects `Staking` and `System`. Returns
/// the balance of the rewards from the previous epoch.
public(package) fun advance_epoch(
    self: &mut SystemStateInnerV1,
    new_committee: Committee,
    new_capacity: u64,
    new_storage_price: u64,
    new_write_price: u64,
): Balance<SUI> {
    // Check new committee is valid, the existence of a committee for the next epoch
    // is proof that the time has come to move epochs.
    let old_epoch = self.epoch();
    let new_epoch = old_epoch + 1;

    assert!(new_committee.epoch() == new_epoch, EIncorrectCommittee);
    let previous_committee = self.current_committee.swap(new_committee);
    let _ = self.previous_committee.swap_or_fill(previous_committee);

    // Update the system object.
    self.total_capacity_size = new_capacity;
    self.storage_price_per_unit_size = new_storage_price;
    self.write_price_per_unit_size = new_write_price;

    let accounts_old_epoch = self.future_accounting.ring_pop_expand();

    // Make sure that we have the correct epoch
    assert!(accounts_old_epoch.epoch() == old_epoch, EInvalidAccountingEpoch);

    // Update storage based on the accounts data.
    self.used_capacity_size = self.used_capacity_size - accounts_old_epoch.storage_to_reclaim();

    accounts_old_epoch.unwrap_balance()
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
    let storage_units = storage_units_from_size(storage_amount);
    let period_payment_due = self.storage_price_per_unit_size * storage_units;
    let coin_balance = payment.balance_mut();

    let mut i = 0;
    while (i < periods_ahead) {
        let accounts = self.future_accounting.ring_lookup_mut(i);

        // Distribute rewards
        let rewards_balance = accounts.rewards_balance();
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

/// Processes invalid blob id message. Checks the certificate in the current committee and ensures
/// that the epoch is correct before emitting an event.
public(package) fun invalidate_blob_id(
    self: &SystemStateInnerV1,
    signature: vector<u8>,
    members: vector<u16>,
    message: vector<u8>,
): u256 {
    let committee = self.current_committee.borrow();

    let certified_message = committee.verify_quorum_in_epoch(
        signature,
        members,
        message,
    );

    let invalid_blob_message = certified_message.invalid_blob_id_message();
    let blob_id = invalid_blob_message.invalid_blob_id();
    // Assert the epoch is correct.
    let epoch = invalid_blob_message.certified_invalid_epoch();
    assert!(epoch == self.epoch(), EInvalidIdEpoch);

    // Emit the event about a blob id being invalid here.
    emit_invalid_blob_id(
        epoch,
        blob_id,
    );
    blob_id
}

/// Registers a new blob in the system.
/// `size` is the size of the unencoded blob. The reserved space in `storage` must be at
/// least the size of the encoded blob.
public(package) fun register_blob(
    self: &mut SystemStateInnerV1,
    storage: Storage,
    blob_id: u256,
    root_hash: u256,
    size: u64,
    erasure_code_type: u8,
    write_payment_coin: &mut Coin<SUI>,
    ctx: &mut TxContext,
): Blob {
    let blob = blob::new(
        storage,
        blob_id,
        root_hash,
        size,
        erasure_code_type,
        self.epoch(),
        self.n_shards(),
        ctx,
    );
    let write_price = self.write_price(blob.encoded_size(self.n_shards()));
    let payment = write_payment_coin.split(write_price, ctx).into_balance();
    let accounts = self.future_accounting.ring_lookup_mut(0).rewards_balance().join(payment);
    blob
}

/// Certify that a blob will be available in the storage system until the end epoch of the
/// storage associated with it.
public(package) fun certify_blob(
    self: &SystemStateInnerV1,
    blob: &mut Blob,
    signature: vector<u8>,
    members: vector<u16>,
    message: vector<u8>,
) {
    let certified_msg = self
        .current_committee()
        .verify_quorum_in_epoch(
            signature,
            members,
            message,
        );
    let certified_blob_msg = certified_msg.certify_blob_message();
    blob.certify_with_certified_msg(self.epoch(), certified_blob_msg);
}

/// Extend the period of validity of a blob with a new storage resource.
/// The new storage resource must be the same size as the storage resource
/// used in the blob, and have a longer period of validity.
public(package) fun extend_blob_with_resource(
    self: &SystemStateInnerV1,
    blob: &mut Blob,
    extension: Storage,
) {
    blob.extend_with_resource(extension, self.epoch());
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

public(package) fun write_price(self: &SystemStateInnerV1, write_size: u64): u64 {
    let storage_units = storage_units_from_size(write_size);
    self.write_price_per_unit_size * storage_units
}

fun storage_units_from_size(size: u64): u64 {
    (size + BYTES_PER_UNIT_SIZE - 1) / BYTES_PER_UNIT_SIZE
}

// == Testing ==

#[test_only]
public(package) fun new_for_testing(ctx: &mut TxContext): SystemStateInnerV1 {
    let committee = committee::committee_for_testing(0);
    SystemStateInnerV1 {
        current_committee: option::some(committee),
        total_capacity_size: 1_000_000_000,
        used_capacity_size: 0,
        storage_price_per_unit_size: 5,
        write_price_per_unit_size: 1,
        previous_committee: option::none(),
        future_accounting: storage_accounting::ring_new(MAX_PERIODS_AHEAD),
    }
}

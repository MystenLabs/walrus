// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[allow(unused_variable, unused_mut_parameter, unused_field)]
module walrus::system_state_inner;

use sui::{balance::Balance, coin::Coin};
use wal::wal::WAL;
use walrus::{
    blob::{Self, Blob},
    bls_aggregate::{Self, BlsCommittee},
    epoch_parameters::EpochParams,
    events::emit_invalid_blob_id,
    storage_accounting::{Self, FutureAccountingRingBuffer},
    storage_node::StorageNodeCap,
    storage_resource::{Self, Storage}
};

/// The maximum number of periods ahead we allow for storage reservations.
/// TODO: the number here is a placeholder, and assumes an epoch is a week,
/// and therefore 2 x 52 weeks = 2 years.
const MAX_EPOCHS_AHEAD: u32 = 104;

// Keep in sync with the same constant in `crates/walrus-sui/utils.rs`.
const BYTES_PER_UNIT_SIZE: u64 = 1_024;

// Errors
const ENotImplemented: u64 = 0;
const EStorageExceeded: u64 = 1;
const EInvalidEpochsAhead: u64 = 2;
const EInvalidIdEpoch: u64 = 3;
const EIncorrectCommittee: u64 = 4;
const EInvalidAccountingEpoch: u64 = 5;

/// The inner object that is not present in signatures and can be versioned.
#[allow(unused_field)]
public struct SystemStateInnerV1 has key, store {
    id: UID,
    /// The current committee, with the current epoch.
    committee: BlsCommittee,
    // Some accounting
    total_capacity_size: u64,
    used_capacity_size: u64,
    /// The price per unit size of storage.
    storage_price_per_unit_size: u64,
    /// The write price per unit size.
    write_price_per_unit_size: u64,
    /// Accounting ring buffer for future epochs.
    future_accounting: FutureAccountingRingBuffer,
}

/// Creates an empty system state with a capacity of zero and an empty committee.
public(package) fun create_empty(ctx: &mut TxContext): SystemStateInnerV1 {
    let committee = bls_aggregate::new_bls_committee(0, vector[]);
    let future_accounting = storage_accounting::ring_new(MAX_EPOCHS_AHEAD);
    let id = object::new(ctx);
    SystemStateInnerV1 {
        id,
        committee,
        total_capacity_size: 0,
        used_capacity_size: 0,
        storage_price_per_unit_size: 0,
        write_price_per_unit_size: 0,
        future_accounting,
    }
}

/// Update epoch to next epoch, and update the committee, price and capacity.
///
/// Called by the epoch change function that connects `Staking` and `System`. Returns
/// the balance of the rewards from the previous epoch.
public(package) fun advance_epoch(
    self: &mut SystemStateInnerV1,
    new_committee: BlsCommittee,
    new_epoch_params: EpochParams,
): Balance<WAL> {
    // Check new committee is valid, the existence of a committee for the next epoch
    // is proof that the time has come to move epochs.
    let old_epoch = self.epoch();
    let new_epoch = old_epoch + 1;

    assert!(new_committee.epoch() == new_epoch, EIncorrectCommittee);
    self.committee = new_committee;

    // Update the system object.
    self.total_capacity_size = new_epoch_params.capacity().max(self.used_capacity_size);
    self.storage_price_per_unit_size = new_epoch_params.storage_price();
    self.write_price_per_unit_size = new_epoch_params.write_price();

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
    epochs_ahead: u32,
    payment: &mut Coin<WAL>,
    ctx: &mut TxContext,
): Storage {
    // Check the period is within the allowed range.
    assert!(epochs_ahead > 0, EInvalidEpochsAhead);
    assert!(epochs_ahead <= MAX_EPOCHS_AHEAD, EInvalidEpochsAhead);

    // Check capacity is available.
    assert!(self.used_capacity_size + storage_amount <= self.total_capacity_size, EStorageExceeded);

    // Pay rewards for each future epoch into the future accounting.
    self.process_storage_payments(storage_amount, 0, epochs_ahead, payment);

    // Update the storage accounting.
    self.used_capacity_size = self.used_capacity_size + storage_amount;

    // Account the space to reclaim in the future.
    let final_account = self.future_accounting.ring_lookup_mut(epochs_ahead - 1);
    final_account.increase_storage_to_reclaim(storage_amount);

    let self_epoch = epoch(self);

    storage_resource::create_storage(
        self_epoch,
        self_epoch + epochs_ahead,
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
    let certified_message = self
        .committee
        .verify_quorum_in_epoch(
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
    encoding_type: u8,
    deletable: bool,
    write_payment_coin: &mut Coin<WAL>,
    ctx: &mut TxContext,
): Blob {
    let blob = blob::new(
        storage,
        blob_id,
        root_hash,
        size,
        encoding_type,
        deletable,
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
    signers: vector<u16>,
    message: vector<u8>,
) {
    let certified_msg = self
        .committee()
        .verify_quorum_in_epoch(
            signature,
            signers,
            message,
        );
    let certified_blob_msg = certified_msg.certify_blob_message();
    blob.certify_with_certified_msg(self.epoch(), certified_blob_msg);
}

/// Deletes a deletable blob and returns the contained storage resource.
public(package) fun delete_blob(self: &SystemStateInnerV1, blob: Blob): Storage {
    blob.delete(self.epoch())
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

/// Extend the period of validity of a blob by extending its contained storage resource.
public(package) fun extend_blob(
    self: &mut SystemStateInnerV1,
    blob: &mut Blob,
    epochs_ahead: u32,
    payment: &mut Coin<WAL>,
) {
    // Check that the blob is certified and not expired.
    blob.assert_certified_not_expired(self.epoch());

    let start_offset = blob.storage().end_epoch() - self.epoch();
    let end_offset = start_offset + epochs_ahead;

    // Check the period is within the allowed range.
    assert!(epochs_ahead > 0, EInvalidEpochsAhead);
    assert!(end_offset <= MAX_EPOCHS_AHEAD, EInvalidEpochsAhead);

    // Pay rewards for each future epoch into the future accounting.
    let storage_size = blob.storage().storage_size();
    self.process_storage_payments(storage_size, start_offset, end_offset, payment);

    // Account the space to reclaim in the future.

    // First account for the space not being freed in the original end epoch.
    self
        .future_accounting
        .ring_lookup_mut(start_offset - 1)
        .decrease_storage_to_reclaim(storage_size);

    // Then account for the space being freed in the new end epoch.
    self
        .future_accounting
        .ring_lookup_mut(end_offset - 1)
        .increase_storage_to_reclaim(storage_size);

    blob.storage_mut().extend_end_epoch(epochs_ahead);

    blob.emit_certified(true);
}

fun process_storage_payments(
    self: &mut SystemStateInnerV1,
    storage_size: u64,
    start_offset: u32,
    end_offset: u32,
    payment: &mut Coin<WAL>,
) {
    let storage_units = storage_units_from_size(storage_size);
    let period_payment_due = self.storage_price_per_unit_size * storage_units;
    let coin_balance = payment.balance_mut();

    start_offset.range_do!(end_offset, |i| {
        let accounts = self.future_accounting.ring_lookup_mut(i);

        // Distribute rewards
        let rewards_balance = accounts.rewards_balance();
        // Note this will abort if the balance is not enough.
        let epoch_payment = coin_balance.split(period_payment_due);
        rewards_balance.join(epoch_payment);
    });
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
public(package) fun epoch(self: &SystemStateInnerV1): u32 {
    self.committee.epoch()
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
public(package) fun committee(self: &SystemStateInnerV1): &BlsCommittee {
    &self.committee
}

public(package) fun n_shards(self: &SystemStateInnerV1): u16 {
    self.committee.n_shards()
}

public(package) fun write_price(self: &SystemStateInnerV1, write_size: u64): u64 {
    let storage_units = storage_units_from_size(write_size);
    self.write_price_per_unit_size * storage_units
}

fun storage_units_from_size(size: u64): u64 {
    (size + BYTES_PER_UNIT_SIZE - 1) / BYTES_PER_UNIT_SIZE
}

// === Testing ===

#[test_only]
use walrus::{test_utils};

#[test_only]
public(package) fun new_for_testing(): SystemStateInnerV1 {
    let committee = test_utils::new_bls_committee_for_testing(0);
    let ctx = &mut tx_context::dummy();
    let id = object::new(ctx);
    SystemStateInnerV1 {
        id,
        committee,
        total_capacity_size: 1_000_000_000,
        used_capacity_size: 0,
        storage_price_per_unit_size: 5,
        write_price_per_unit_size: 1,
        future_accounting: storage_accounting::ring_new(MAX_EPOCHS_AHEAD),
    }
}

// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

#[allow(unused_variable, unused_mut_parameter, unused_field)]
module walrus::system_state_inner;

use sui::{balance::{Self, Balance}, coin::Coin, vec_map::{Self, VecMap}};
use wal::wal::WAL;
use walrus::{
    blob::{Self, Blob},
    bls_aggregate::{Self, BlsCommittee},
    encoding::encoded_blob_length,
    epoch_parameters::EpochParams,
    event_blob::{Self, EventBlobCertificationState, new_attestation},
    events,
    extended_field::{Self, ExtendedField},
    messages,
    storage_accounting::{Self, FutureAccountingRingBuffer},
    storage_node::StorageNodeCap,
    storage_resource::{Self, Storage}
};

/// An upper limit for the maximum number of epochs ahead for which a blob can be registered.
/// Needed to bound the size of the `future_accounting`.
const MAX_MAX_EPOCHS_AHEAD: u32 = 1000;

// Keep in sync with the same constant in `crates/walrus-sui/utils.rs`.
const BYTES_PER_UNIT_SIZE: u64 = 1_024 * 1_024; // 1 MiB

// Error codes
// Error types in `walrus-sui/types/move_errors.rs` are auto-generated from the Move error codes.
/// The system parameter for the maximum number of epochs ahead is invalid.
const EInvalidMaxEpochsAhead: u64 = 0;
/// The storage capacity of the system is exceeded.
const EStorageExceeded: u64 = 1;
/// The number of epochs in the future to reserve storage for exceeds the maximum.
const EInvalidEpochsAhead: u64 = 2;
/// Invalid epoch in the certificate.
const EInvalidIdEpoch: u64 = 3;
/// Trying to set an incorrect committee for the next epoch.
const EIncorrectCommittee: u64 = 4;
/// Incorrect epoch in the storage accounting.
const EInvalidAccountingEpoch: u64 = 5;
/// Incorrect event blob attestation.
const EIncorrectAttestation: u64 = 6;
/// Repeated attestation for an event blob.
const ERepeatedAttestation: u64 = 7;
/// The node is not a member of the committee.
const ENotCommitteeMember: u64 = 8;
/// Incorrect deny list sequence number.
const EIncorrectDenyListSequence: u64 = 9;
/// Deny list certificate contains the wrong node ID.
const EIncorrectDenyListNode: u64 = 10;
/// Trying to obtain a resource with an invalid size.
const EInvalidResourceSize: u64 = 11;
/// Trying to update the protocol version for an invalid start epoch.
const EInvalidStartEpoch: u64 = 12;
/// The node has already voted for slashing the target node.
const EDuplicateSlashingVote: u64 = 13;
/// Cannot vote to slash oneself.
const ECannotSlashSelf: u64 = 14;
/// Function is deprecated.
const EDeprecatedFunction: u64 = 15;

/// The inner object that is not present in signatures and can be versioned.
/// V1 is the original version without slashing support (versions 1-2).
#[allow(unused_field)]
public struct SystemStateInnerV1 has store {
    /// The current committee, with the current epoch.
    committee: BlsCommittee,
    /// Maximum capacity size for the current and future epochs.
    /// Changed by voting on the epoch parameters.
    total_capacity_size: u64,
    /// Contains the used capacity size for the current epoch.
    used_capacity_size: u64,
    /// The price per unit size of storage.
    storage_price_per_unit_size: u64,
    /// The write price per unit size.
    write_price_per_unit_size: u64,
    /// Accounting ring buffer for future epochs.
    future_accounting: FutureAccountingRingBuffer,
    /// Event blob certification state
    event_blob_certification_state: EventBlobCertificationState,
    /// Sizes of deny lists for storage nodes. Only current committee members
    /// can register their updates in this map. Hence, we don't expect it to bloat.
    ///
    /// Max number of stored entries is ~6500. If there's any concern about the
    /// performance of the map, it can be cleaned up as a side effect of the
    /// updates / registrations.
    deny_list_sizes: ExtendedField<VecMap<ID, u64>>,
}

/// V2 adds slashing support (version 3+).
#[allow(unused_field)]
public struct SystemStateInnerV2 has store {
    /// The current committee, with the current epoch.
    committee: BlsCommittee,
    /// Maximum capacity size for the current and future epochs.
    /// Changed by voting on the epoch parameters.
    total_capacity_size: u64,
    /// Contains the used capacity size for the current epoch.
    used_capacity_size: u64,
    /// The price per unit size of storage.
    storage_price_per_unit_size: u64,
    /// The write price per unit size.
    write_price_per_unit_size: u64,
    /// Accounting ring buffer for future epochs.
    future_accounting: FutureAccountingRingBuffer,
    /// Event blob certification state
    event_blob_certification_state: EventBlobCertificationState,
    /// Sizes of deny lists for storage nodes. Only current committee members
    /// can register their updates in this map. Hence, we don't expect it to bloat.
    ///
    /// Max number of stored entries is ~6500. If there's any concern about the
    /// performance of the map, it can be cleaned up as a side effect of the
    /// updates / registrations.
    deny_list_sizes: ExtendedField<VecMap<ID, u64>>,
    /// Slashing votes for the current epoch. Maps target node ID to a map of voter node ID to
    /// their voting weight (number of shards). Votes are reset when apply_slashing is called.
    slashing_votes: ExtendedField<VecMap<ID, VecMap<ID, u16>>>,
}

/// Creates an empty system state with a capacity of zero and an empty
/// committee.
public(package) fun create_empty(max_epochs_ahead: u32, ctx: &mut TxContext): SystemStateInnerV2 {
    let committee = bls_aggregate::new_bls_committee(0, vector[]);
    assert!(max_epochs_ahead <= MAX_MAX_EPOCHS_AHEAD, EInvalidMaxEpochsAhead);
    let future_accounting = storage_accounting::ring_new(max_epochs_ahead);
    let event_blob_certification_state = event_blob::create_with_empty_state();
    SystemStateInnerV2 {
        committee,
        total_capacity_size: 0,
        used_capacity_size: 0,
        storage_price_per_unit_size: 0,
        write_price_per_unit_size: 0,
        future_accounting,
        event_blob_certification_state,
        deny_list_sizes: extended_field::new(vec_map::empty(), ctx),
        slashing_votes: extended_field::new(vec_map::empty(), ctx),
    }
}

/// Migrates SystemStateInnerV1 to SystemStateInnerV2 by adding the slashing_votes field.
public(package) fun migrate_v1_to_v2(
    v1: SystemStateInnerV1,
    ctx: &mut TxContext,
): SystemStateInnerV2 {
    let SystemStateInnerV1 {
        committee,
        total_capacity_size,
        used_capacity_size,
        storage_price_per_unit_size,
        write_price_per_unit_size,
        future_accounting,
        event_blob_certification_state,
        deny_list_sizes,
    } = v1;

    SystemStateInnerV2 {
        committee,
        total_capacity_size,
        used_capacity_size,
        storage_price_per_unit_size,
        write_price_per_unit_size,
        future_accounting,
        event_blob_certification_state,
        deny_list_sizes,
        slashing_votes: extended_field::new(vec_map::empty(), ctx),
    }
}

/// Macro implementing the advance_epoch logic.
macro fun advance_epoch_impl(
    $committee: &mut BlsCommittee,
    $future_accounting: &mut FutureAccountingRingBuffer,
    $event_blob_certification_state: &mut EventBlobCertificationState,
    $used_capacity_size: &mut u64,
    $total_capacity_size: &mut u64,
    $deny_list_sizes: &ExtendedField<VecMap<ID, u64>>,
    $new_committee: BlsCommittee,
    $new_epoch_params: &EpochParams,
): VecMap<ID, Balance<WAL>> {
    let committee = $committee;
    let future_accounting = $future_accounting;
    let event_blob_certification_state = $event_blob_certification_state;
    let used_capacity_size = $used_capacity_size;
    let total_capacity_size = $total_capacity_size;
    let deny_list_sizes = $deny_list_sizes;
    let new_committee = $new_committee;
    let new_epoch_params = $new_epoch_params;

    // Check new committee is valid, the existence of a committee for the next
    // epoch is proof that the time has come to move epochs.
    let old_epoch = committee.epoch();
    let new_epoch = old_epoch + 1;
    let old_committee = *$committee;

    assert!(new_committee.epoch() == new_epoch, EIncorrectCommittee);

    // === Update the system object ===
    *$committee = $new_committee;

    let accounts_old_epoch = future_accounting.ring_pop_expand();

    // Make sure that we have the correct epoch
    assert!(accounts_old_epoch.epoch() == old_epoch, EInvalidAccountingEpoch);

    // Stop tracking all event blobs
    event_blob_certification_state.reset();

    // Update storage based on the accounts data.
    let old_epoch_used_capacity = accounts_old_epoch.used_capacity();

    // Update used capacity size to the new epoch without popping the ring buffer.
    *$used_capacity_size = future_accounting.ring_lookup_mut(0).used_capacity();

    // Update capacity. Prices are no longer updated here; they are applied immediately
    // when price votes are cast via set_storage_price_vote / set_write_price_vote.
    *$total_capacity_size = new_epoch_params.capacity().max(*$used_capacity_size);

    // === Rewards distribution ===

    let mut total_rewards = accounts_old_epoch.unwrap_balance();

    // to perform the calculation of rewards, we account for the deny list sizes
    // in comparison to the used capacity size, and the weights of the nodes in
    // the committee.
    //
    // specific reward for a node is calculated as:
    // reward = (weight * (used_capacity_size - deny_list_size)) / total_stored * total_rewards
    // where `total_stored` is the sum of all nodes' values.
    //
    // leftover rewards are added to the next epoch's accounting to avoid rounding errors.

    let deny_list_sizes = deny_list_sizes.borrow();
    let (node_ids, weights) = old_committee.to_vec_map().into_keys_values();
    let mut stored_vec = vector[];
    let mut total_stored = 0;

    node_ids.zip_do!(weights, |node_id, weight| {
        let deny_list_size = deny_list_sizes.try_get(&node_id).destroy_or!(0);
        // The deny list size cannot exceed the used capacity.
        let deny_list_size = deny_list_size.min(old_epoch_used_capacity);
        // The total encoded size of all blobs excluding the ones on the nodes deny list.
        let stored = old_epoch_used_capacity - deny_list_size;
        let stored_weighted = (weight as u128) * (stored as u128);

        total_stored = total_stored + stored_weighted;
        stored_vec.push_back(stored_weighted);
    });

    let total_stored = total_stored.max(1); // avoid division by zero
    let total_rewards_value = total_rewards.value() as u128;
    let reward_values = stored_vec.map!(|stored| {
        total_rewards.split((stored * total_rewards_value / total_stored) as u64)
    });

    // add the leftover rewards to the next epoch
    future_accounting.ring_lookup_mut(0).rewards_balance().join(total_rewards);
    vec_map::from_keys_values(node_ids, reward_values)
}

/// Update epoch to next epoch, and update the committee, price and capacity.
///
/// Called by the epoch change function that connects `Staking` and `System`.
/// Returns the mapping of node IDs from the old committee to the rewards they
/// received in the epoch.
///
/// Note: VecMap must contain values only for the nodes from the previous
/// committee, the `staking` part of the system relies on this assumption.
public(package) fun advance_epoch(
    self: &mut SystemStateInnerV1,
    new_committee: BlsCommittee,
    new_epoch_params: &EpochParams,
): VecMap<ID, Balance<WAL>> {
    advance_epoch_impl!(
        &mut self.committee,
        &mut self.future_accounting,
        &mut self.event_blob_certification_state,
        &mut self.used_capacity_size,
        &mut self.total_capacity_size,
        &self.deny_list_sizes,
        new_committee,
        new_epoch_params,
    )
}

public(package) fun advance_epoch_v2(
    self: &mut SystemStateInnerV2,
    new_committee: BlsCommittee,
    new_epoch_params: &EpochParams,
): VecMap<ID, Balance<WAL>> {
    advance_epoch_impl!(
        &mut self.committee,
        &mut self.future_accounting,
        &mut self.event_blob_certification_state,
        &mut self.used_capacity_size,
        &mut self.total_capacity_size,
        &self.deny_list_sizes,
        new_committee,
        new_epoch_params,
    )
}

/// Extracts the balance that will be burned for the current epoch. This function is used when
/// executing the epoch change.
public(package) fun extract_burn_balance_v2(self: &mut SystemStateInnerV2): Balance<WAL> {
    self.future_accounting.extract_burn_balance()
}

/// Macro implementing reserve_space logic.
macro fun reserve_space_impl(
    $future_accounting: &mut FutureAccountingRingBuffer,
    $used_capacity_size: &mut u64,
    $total_capacity_size: u64,
    $storage_price_per_unit_size: u64,
    $current_epoch: u32,
    $storage_amount: u64,
    $epochs_ahead: u32,
    $payment: &mut Coin<WAL>,
    $ctx: &mut TxContext,
): Storage {
    let future_accounting = $future_accounting;
    let used_capacity_size = $used_capacity_size;
    let total_capacity_size = $total_capacity_size;
    let storage_price_per_unit_size = $storage_price_per_unit_size;
    let current_epoch = $current_epoch;
    let storage_amount = $storage_amount;
    let epochs_ahead = $epochs_ahead;
    let payment = $payment;
    let ctx = $ctx;

    // Check the period is within the allowed range.
    assert!(epochs_ahead > 0, EInvalidEpochsAhead);
    assert!(epochs_ahead <= future_accounting.max_epochs_ahead(), EInvalidEpochsAhead);

    let start_epoch = current_epoch;
    let end_epoch = start_epoch + epochs_ahead;
    let start_offset = 0u32;
    let end_offset = epochs_ahead;

    // Pay rewards for each future epoch into the future accounting.
    process_storage_payments_impl!(
        future_accounting,
        storage_price_per_unit_size,
        storage_amount,
        start_offset,
        end_offset,
        payment,
    );

    // Reserve the space
    reserve_space_without_payment_impl!(
        future_accounting,
        used_capacity_size,
        total_capacity_size,
        current_epoch,
        storage_amount,
        start_offset,
        end_offset,
        true,
        ctx,
    )
}

/// Allow buying a storage reservation for a given period of epochs.
public(package) fun reserve_space(
    self: &mut SystemStateInnerV1,
    storage_amount: u64,
    epochs_ahead: u32,
    payment: &mut Coin<WAL>,
    ctx: &mut TxContext,
): Storage {
    reserve_space_impl!(
        &mut self.future_accounting,
        &mut self.used_capacity_size,
        self.total_capacity_size,
        self.storage_price_per_unit_size,
        self.committee.epoch(),
        storage_amount,
        epochs_ahead,
        payment,
        ctx,
    )
}

public(package) fun reserve_space_v2(
    self: &mut SystemStateInnerV2,
    storage_amount: u64,
    epochs_ahead: u32,
    payment: &mut Coin<WAL>,
    ctx: &mut TxContext,
): Storage {
    reserve_space_impl!(
        &mut self.future_accounting,
        &mut self.used_capacity_size,
        self.total_capacity_size,
        self.storage_price_per_unit_size,
        self.committee.epoch(),
        storage_amount,
        epochs_ahead,
        payment,
        ctx,
    )
}

/// Macro implementing reserve_space_for_epochs logic.
macro fun reserve_space_for_epochs_impl(
    $future_accounting: &mut FutureAccountingRingBuffer,
    $used_capacity_size: &mut u64,
    $total_capacity_size: u64,
    $storage_price_per_unit_size: u64,
    $current_epoch: u32,
    $storage_amount: u64,
    $start_epoch: u32,
    $end_epoch: u32,
    $payment: &mut Coin<WAL>,
    $ctx: &mut TxContext,
): Storage {
    let future_accounting = $future_accounting;
    let used_capacity_size = $used_capacity_size;
    let total_capacity_size = $total_capacity_size;
    let storage_price_per_unit_size = $storage_price_per_unit_size;
    let current_epoch = $current_epoch;
    let storage_amount = $storage_amount;
    let start_epoch = $start_epoch;
    let end_epoch = $end_epoch;
    let payment = $payment;
    let ctx = $ctx;

    // If the start epoch has already passed, reserve space starting at the current epoch.
    let start_epoch = start_epoch.max(current_epoch);
    let start_offset = start_epoch - current_epoch;

    // Check that the interval is non-empty.
    assert!(end_epoch > start_epoch, EInvalidEpochsAhead);

    let end_offset = end_epoch - current_epoch;

    // Check the period is within the allowed range.
    assert!(end_offset <= future_accounting.max_epochs_ahead(), EInvalidEpochsAhead);

    // Pay rewards for each future epoch into the future accounting.
    process_storage_payments_impl!(
        future_accounting,
        storage_price_per_unit_size,
        storage_amount,
        start_offset,
        end_offset,
        payment,
    );

    // Reserve the space
    reserve_space_without_payment_impl!(
        future_accounting,
        used_capacity_size,
        total_capacity_size,
        current_epoch,
        storage_amount,
        start_offset,
        end_offset,
        true,
        ctx,
    )
}

/// Allows buying a storage reservation for a given period of epochs.
///
/// Returns a storage resource for the period between `start_epoch` (inclusive) and
/// `end_epoch` (exclusive). If `start_epoch` has already passed, reserves space starting
/// from the current epoch.
public(package) fun reserve_space_for_epochs(
    self: &mut SystemStateInnerV1,
    storage_amount: u64,
    start_epoch: u32,
    end_epoch: u32,
    payment: &mut Coin<WAL>,
    ctx: &mut TxContext,
): Storage {
    reserve_space_for_epochs_impl!(
        &mut self.future_accounting,
        &mut self.used_capacity_size,
        self.total_capacity_size,
        self.storage_price_per_unit_size,
        self.committee.epoch(),
        storage_amount,
        start_epoch,
        end_epoch,
        payment,
        ctx,
    )
}

public(package) fun reserve_space_for_epochs_v2(
    self: &mut SystemStateInnerV2,
    storage_amount: u64,
    start_epoch: u32,
    end_epoch: u32,
    payment: &mut Coin<WAL>,
    ctx: &mut TxContext,
): Storage {
    reserve_space_for_epochs_impl!(
        &mut self.future_accounting,
        &mut self.used_capacity_size,
        self.total_capacity_size,
        self.storage_price_per_unit_size,
        self.committee.epoch(),
        storage_amount,
        start_epoch,
        end_epoch,
        payment,
        ctx,
    )
}

/// Macro implementing reserve_space_without_payment logic.
macro fun reserve_space_without_payment_impl(
    $future_accounting: &mut FutureAccountingRingBuffer,
    $used_capacity_size: &mut u64,
    $total_capacity_size: u64,
    $current_epoch: u32,
    $storage_amount: u64,
    $start_epoch_offset: u32,
    $end_epoch_offset: u32,
    $check_capacity: bool,
    $ctx: &mut TxContext,
): Storage {
    let future_accounting = $future_accounting;
    let used_capacity_size = $used_capacity_size;
    let total_capacity_size = $total_capacity_size;
    let current_epoch = $current_epoch;
    let storage_amount = $storage_amount;
    let start_epoch_offset = $start_epoch_offset;
    let end_epoch_offset = $end_epoch_offset;
    let check_capacity = $check_capacity;
    let ctx = $ctx;

    assert!(end_epoch_offset - start_epoch_offset > 0, EInvalidEpochsAhead);
    assert!(end_epoch_offset <= future_accounting.max_epochs_ahead(), EInvalidEpochsAhead);
    assert!(storage_amount > 0, EInvalidResourceSize);

    start_epoch_offset.range_do!(end_epoch_offset, |i| {
        let used_capacity = future_accounting
            .ring_lookup_mut(i)
            .increase_used_capacity(storage_amount);
        if (i == 0) {
            *used_capacity_size = used_capacity;
        };
        assert!(!check_capacity || used_capacity <= total_capacity_size, EStorageExceeded);
    });

    let start_epoch = current_epoch + start_epoch_offset;
    let end_epoch = current_epoch + end_epoch_offset;
    storage_resource::create_storage(start_epoch, end_epoch, storage_amount, ctx)
}

/// Allow obtaining a storage reservation for a given period of epochs without
/// payment. The epochs are provided as offsets from the current epoch.
fun reserve_space_without_payment(
    self: &mut SystemStateInnerV1,
    storage_amount: u64,
    start_epoch_offset: u32,
    end_epoch_offset: u32,
    check_capacity: bool,
    ctx: &mut TxContext,
): Storage {
    reserve_space_without_payment_impl!(
        &mut self.future_accounting,
        &mut self.used_capacity_size,
        self.total_capacity_size,
        self.committee.epoch(),
        storage_amount,
        start_epoch_offset,
        end_epoch_offset,
        check_capacity,
        ctx,
    )
}

fun reserve_space_without_payment_v2(
    self: &mut SystemStateInnerV2,
    storage_amount: u64,
    start_epoch_offset: u32,
    end_epoch_offset: u32,
    check_capacity: bool,
    ctx: &mut TxContext,
): Storage {
    reserve_space_without_payment_impl!(
        &mut self.future_accounting,
        &mut self.used_capacity_size,
        self.total_capacity_size,
        self.committee.epoch(),
        storage_amount,
        start_epoch_offset,
        end_epoch_offset,
        check_capacity,
        ctx,
    )
}

/// Macro implementing invalidate_blob_id logic.
macro fun invalidate_blob_id_impl(
    $committee: &BlsCommittee,
    $epoch: u32,
    $signature: vector<u8>,
    $members_bitmap: vector<u8>,
    $message: vector<u8>,
): u256 {
    let committee = $committee;
    let certified_message = committee.verify_one_correct_node_in_epoch(
        $signature,
        $members_bitmap,
        $message,
    );

    let epoch = certified_message.cert_epoch();
    let invalid_blob_message = certified_message.invalid_blob_id_message();
    let blob_id = invalid_blob_message.invalid_blob_id();
    assert!(epoch == $epoch, EInvalidIdEpoch);
    events::emit_invalid_blob_id(epoch, blob_id);
    blob_id
}

/// Processes invalid blob id message. Checks the certificate in the current
/// committee and ensures that the epoch is correct before emitting an event.
public(package) fun invalidate_blob_id(
    self: &SystemStateInnerV1,
    signature: vector<u8>,
    members_bitmap: vector<u8>,
    message: vector<u8>,
): u256 {
    invalidate_blob_id_impl!(
        &self.committee,
        self.committee.epoch(),
        signature,
        members_bitmap,
        message,
    )
}

public(package) fun invalidate_blob_id_v2(
    self: &SystemStateInnerV2,
    signature: vector<u8>,
    members_bitmap: vector<u8>,
    message: vector<u8>,
): u256 {
    invalidate_blob_id_impl!(
        &self.committee,
        self.committee.epoch(),
        signature,
        members_bitmap,
        message,
    )
}

/// Macro implementing register_blob logic.
macro fun register_blob_impl(
    $committee: &BlsCommittee,
    $future_accounting: &mut FutureAccountingRingBuffer,
    $write_price_per_unit_size: u64,
    $storage: Storage,
    $blob_id: u256,
    $root_hash: u256,
    $size: u64,
    $encoding_type: u8,
    $deletable: bool,
    $write_payment_coin: &mut Coin<WAL>,
    $ctx: &mut TxContext,
): Blob {
    let committee = $committee;
    let future_accounting = $future_accounting;
    let write_price_per_unit_size = $write_price_per_unit_size;
    let storage = $storage;
    let blob_id = $blob_id;
    let root_hash = $root_hash;
    let size = $size;
    let encoding_type = $encoding_type;
    let deletable = $deletable;
    let write_payment_coin = $write_payment_coin;
    let ctx = $ctx;

    let blob = blob::new(
        storage,
        blob_id,
        root_hash,
        size,
        encoding_type,
        deletable,
        committee.epoch(),
        committee.n_shards(),
        ctx,
    );
    let encoded_size = blob.encoded_size(committee.n_shards());
    let storage_units = storage_units_from_size!(encoded_size);
    let write_price = write_price_per_unit_size * storage_units;
    let payment = write_payment_coin.balance_mut().split(write_price);
    future_accounting.ring_lookup_mut(0).rewards_balance().join(payment);
    blob
}

/// Registers a new blob in the system.
/// - `size` is the size of the unencoded blob.
/// - The reserved space in `storage` must be at least the size of the encoded blob.
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
    register_blob_impl!(
        &self.committee,
        &mut self.future_accounting,
        self.write_price_per_unit_size,
        storage,
        blob_id,
        root_hash,
        size,
        encoding_type,
        deletable,
        write_payment_coin,
        ctx,
    )
}

public(package) fun register_blob_v2(
    self: &mut SystemStateInnerV2,
    storage: Storage,
    blob_id: u256,
    root_hash: u256,
    size: u64,
    encoding_type: u8,
    deletable: bool,
    write_payment_coin: &mut Coin<WAL>,
    ctx: &mut TxContext,
): Blob {
    register_blob_impl!(
        &self.committee,
        &mut self.future_accounting,
        self.write_price_per_unit_size,
        storage,
        blob_id,
        root_hash,
        size,
        encoding_type,
        deletable,
        write_payment_coin,
        ctx,
    )
}

/// Macro implementing certify_blob logic.
macro fun certify_blob_impl(
    $committee: &BlsCommittee,
    $blob: &mut Blob,
    $signature: vector<u8>,
    $signers_bitmap: vector<u8>,
    $message: vector<u8>,
) {
    let committee = $committee;
    let blob = $blob;
    let signature = $signature;
    let signers_bitmap = $signers_bitmap;
    let message = $message;

    let certified_msg = committee.verify_quorum_in_epoch(
        signature,
        signers_bitmap,
        message,
    );
    assert!(certified_msg.cert_epoch() == committee.epoch(), EInvalidIdEpoch);

    let certified_blob_msg = certified_msg.certify_blob_message();
    blob.certify_with_certified_msg(committee.epoch(), certified_blob_msg);
}

/// Certify that a blob will be available in the storage system until the end
/// epoch of the storage associated with it.
public(package) fun certify_blob(
    self: &SystemStateInnerV1,
    blob: &mut Blob,
    signature: vector<u8>,
    signers_bitmap: vector<u8>,
    message: vector<u8>,
) {
    certify_blob_impl!(&self.committee, blob, signature, signers_bitmap, message)
}

public(package) fun certify_blob_v2(
    self: &SystemStateInnerV2,
    blob: &mut Blob,
    signature: vector<u8>,
    signers_bitmap: vector<u8>,
    message: vector<u8>,
) {
    certify_blob_impl!(&self.committee, blob, signature, signers_bitmap, message)
}

/// Deletes a deletable blob and returns the contained storage resource.
public(package) fun delete_blob(self: &SystemStateInnerV1, blob: Blob): Storage {
    blob.delete(self.committee.epoch())
}

public(package) fun delete_blob_v2(self: &SystemStateInnerV2, blob: Blob): Storage {
    blob.delete(self.committee.epoch())
}

/// Extend the period of validity of a blob with a new storage resource.
/// The new storage resource must be the same size as the storage resource
/// used in the blob, and have a longer period of validity.
public(package) fun extend_blob_with_resource(
    self: &SystemStateInnerV1,
    blob: &mut Blob,
    extension: Storage,
) {
    blob.extend_with_resource(extension, self.committee.epoch());
}

public(package) fun extend_blob_with_resource_v2(
    self: &SystemStateInnerV2,
    blob: &mut Blob,
    extension: Storage,
) {
    blob.extend_with_resource(extension, self.committee.epoch());
}

/// Macro implementing extend_blob logic.
macro fun extend_blob_impl(
    $committee: &BlsCommittee,
    $future_accounting: &mut FutureAccountingRingBuffer,
    $total_capacity_size: u64,
    $storage_price_per_unit_size: u64,
    $blob: &mut Blob,
    $extended_epochs: u32,
    $payment: &mut Coin<WAL>,
) {
    let committee = $committee;
    let future_accounting = $future_accounting;
    let total_capacity_size = $total_capacity_size;
    let storage_price_per_unit_size = $storage_price_per_unit_size;
    let blob = $blob;
    let extended_epochs = $extended_epochs;
    let payment = $payment;

    // Check that the blob is certified and not expired.
    blob.assert_certified_not_expired(committee.epoch());

    let start_offset = blob.storage().end_epoch() - committee.epoch();
    let end_offset = start_offset + extended_epochs;

    // Check the period is within the allowed range.
    assert!(extended_epochs > 0, EInvalidEpochsAhead);
    assert!(end_offset <= future_accounting.max_epochs_ahead(), EInvalidEpochsAhead);

    // Pay rewards for each future epoch into the future accounting.
    let storage_size = blob.storage().size();
    process_storage_payments_impl!(
        future_accounting,
        storage_price_per_unit_size,
        storage_size,
        start_offset,
        end_offset,
        payment,
    );

    // Account the used space: increase the used capacity for each epoch in the
    // future. Iterates: [start, end)
    start_offset.range_do!(end_offset, |i| {
        let used_capacity = future_accounting
            .ring_lookup_mut(i)
            .increase_used_capacity(storage_size);

        assert!(used_capacity <= total_capacity_size, EStorageExceeded);
    });

    blob.storage_mut().extend_end_epoch(extended_epochs);

    blob.emit_certified(true);
}

/// Extend the period of validity of a blob by extending its contained storage
/// resource by `extended_epochs` epochs.
public(package) fun extend_blob(
    self: &mut SystemStateInnerV1,
    blob: &mut Blob,
    extended_epochs: u32,
    payment: &mut Coin<WAL>,
) {
    extend_blob_impl!(
        &self.committee,
        &mut self.future_accounting,
        self.total_capacity_size,
        self.storage_price_per_unit_size,
        blob,
        extended_epochs,
        payment,
    )
}

public(package) fun extend_blob_v2(
    self: &mut SystemStateInnerV2,
    blob: &mut Blob,
    extended_epochs: u32,
    payment: &mut Coin<WAL>,
) {
    extend_blob_impl!(
        &self.committee,
        &mut self.future_accounting,
        self.total_capacity_size,
        self.storage_price_per_unit_size,
        blob,
        extended_epochs,
        payment,
    )
}

/// Macro implementing process_storage_payments logic.
macro fun process_storage_payments_impl(
    $future_accounting: &mut FutureAccountingRingBuffer,
    $storage_price_per_unit_size: u64,
    $storage_size: u64,
    $start_offset: u32,
    $end_offset: u32,
    $payment: &mut Coin<WAL>,
) {
    let future_accounting = $future_accounting;
    let storage_price_per_unit_size = $storage_price_per_unit_size;
    let storage_size = $storage_size;
    let start_offset = $start_offset;
    let end_offset = $end_offset;
    let payment = $payment;

    let storage_units = storage_units_from_size!(storage_size);
    let period_payment_due = storage_price_per_unit_size * storage_units;
    let coin_balance = payment.balance_mut();

    start_offset.range_do!(end_offset, |i| {
        // Distribute rewards
        // Note this will abort if the balance is not enough.
        let epoch_payment = coin_balance.split(period_payment_due);
        future_accounting.ring_lookup_mut(i).rewards_balance().join(epoch_payment);
    });
}

fun process_storage_payments(
    self: &mut SystemStateInnerV1,
    storage_size: u64,
    start_offset: u32,
    end_offset: u32,
    payment: &mut Coin<WAL>,
) {
    process_storage_payments_impl!(
        &mut self.future_accounting,
        self.storage_price_per_unit_size,
        storage_size,
        start_offset,
        end_offset,
        payment,
    )
}

fun process_storage_payments_v2(
    self: &mut SystemStateInnerV2,
    storage_size: u64,
    start_offset: u32,
    end_offset: u32,
    payment: &mut Coin<WAL>,
) {
    process_storage_payments_impl!(
        &mut self.future_accounting,
        self.storage_price_per_unit_size,
        storage_size,
        start_offset,
        end_offset,
        payment,
    )
}

/// Macro implementing certify_event_blob logic.
macro fun certify_event_blob_impl(
    $committee: &BlsCommittee,
    $future_accounting: &mut FutureAccountingRingBuffer,
    $event_blob_certification_state: &mut EventBlobCertificationState,
    $used_capacity_size: &mut u64,
    $total_capacity_size: u64,
    $cap: &mut StorageNodeCap,
    $blob_id: u256,
    $root_hash: u256,
    $size: u64,
    $encoding_type: u8,
    $ending_checkpoint_sequence_num: u64,
    $epoch: u32,
    $ctx: &mut TxContext,
) {
    // Bind macro parameters to local variables for method calls
    let committee = $committee;
    let future_accounting = $future_accounting;
    let state = $event_blob_certification_state;
    let used_capacity_size = $used_capacity_size;
    let total_capacity_size = $total_capacity_size;
    let cap = $cap;
    let blob_id = $blob_id;
    let root_hash = $root_hash;
    let size = $size;
    let encoding_type = $encoding_type;
    let ending_checkpoint_sequence_num = $ending_checkpoint_sequence_num;
    let epoch = $epoch;
    let ctx = $ctx;

    assert!(committee.contains(&cap.node_id()), ENotCommitteeMember);
    assert!(epoch == committee.epoch(), EInvalidIdEpoch);

    cap.last_event_blob_attestation().do!(|attestation| {
        assert!(
            attestation.last_attested_event_blob_epoch() < committee.epoch() ||
                ending_checkpoint_sequence_num >
                    attestation.last_attested_event_blob_checkpoint_seq_num(),
            ERepeatedAttestation,
        );
        let latest_certified_ckpt_seq = state.get_latest_certified_checkpoint_sequence_number();

        if (latest_certified_ckpt_seq.is_some()) {
            let latest_certified_cp_seq_num = latest_certified_ckpt_seq.destroy_some();
            assert!(
                attestation.last_attested_event_blob_epoch() < committee.epoch() ||
                    attestation.last_attested_event_blob_checkpoint_seq_num()
                        <= latest_certified_cp_seq_num,
                EIncorrectAttestation,
            );
        } else {
            assert!(
                attestation.last_attested_event_blob_epoch() < committee.epoch(),
                EIncorrectAttestation,
            );
        }
    });

    let attestation = new_attestation(ending_checkpoint_sequence_num, epoch);
    cap.set_last_event_blob_attestation(attestation);

    let blob_certified = state.is_blob_already_certified(
        ending_checkpoint_sequence_num,
    );

    if (blob_certified) {
        return
    };

    state.start_tracking_blob(blob_id, ending_checkpoint_sequence_num);
    let weight = committee.get_member_weight(&cap.node_id());
    let agg_weight = state.update_aggregate_weight(
        blob_id,
        ending_checkpoint_sequence_num,
        weight,
    );
    let certified = committee.is_quorum(agg_weight);
    if (!certified) {
        return
    };

    let num_shards = committee.n_shards();
    let epochs_ahead = future_accounting.max_epochs_ahead();
    let storage = reserve_space_without_payment_impl!(
        future_accounting,
        used_capacity_size,
        total_capacity_size,
        committee.epoch(),
        encoded_blob_length(size, encoding_type, num_shards),
        0,
        epochs_ahead,
        false,
        ctx,
    );
    let mut blob = blob::new(
        storage,
        blob_id,
        root_hash,
        size,
        encoding_type,
        false,
        committee.epoch(),
        committee.n_shards(),
        ctx,
    );
    let certified_blob_msg = messages::certified_event_blob_message(blob_id);
    blob.certify_with_certified_msg(committee.epoch(), certified_blob_msg);
    state.update_latest_certified_event_blob(
        ending_checkpoint_sequence_num,
        blob_id,
    );
    state.reset();
    blob.burn();
}

public(package) fun certify_event_blob(
    self: &mut SystemStateInnerV1,
    cap: &mut StorageNodeCap,
    blob_id: u256,
    root_hash: u256,
    size: u64,
    encoding_type: u8,
    ending_checkpoint_sequence_num: u64,
    epoch: u32,
    ctx: &mut TxContext,
) {
    certify_event_blob_impl!(
        &self.committee,
        &mut self.future_accounting,
        &mut self.event_blob_certification_state,
        &mut self.used_capacity_size,
        self.total_capacity_size,
        cap,
        blob_id,
        root_hash,
        size,
        encoding_type,
        ending_checkpoint_sequence_num,
        epoch,
        ctx,
    )
}

public(package) fun certify_event_blob_v2(
    self: &mut SystemStateInnerV2,
    cap: &mut StorageNodeCap,
    blob_id: u256,
    root_hash: u256,
    size: u64,
    encoding_type: u8,
    ending_checkpoint_sequence_num: u64,
    epoch: u32,
    ctx: &mut TxContext,
) {
    certify_event_blob_impl!(
        &self.committee,
        &mut self.future_accounting,
        &mut self.event_blob_certification_state,
        &mut self.used_capacity_size,
        self.total_capacity_size,
        cap,
        blob_id,
        root_hash,
        size,
        encoding_type,
        ending_checkpoint_sequence_num,
        epoch,
        ctx,
    )
}

/// Macro implementing add_subsidy logic.
macro fun add_subsidy_impl(
    $future_accounting: &mut FutureAccountingRingBuffer,
    $subsidy: Coin<WAL>,
    $epochs_ahead: u32,
) {
    let future_accounting = $future_accounting;
    let subsidy = $subsidy;
    let epochs_ahead = $epochs_ahead;

    // Check the period is within the allowed range.
    assert!(epochs_ahead > 0, EInvalidEpochsAhead);
    assert!(epochs_ahead <= future_accounting.max_epochs_ahead(), EInvalidEpochsAhead);

    let mut subsidy_balance = subsidy.into_balance();
    let reward_per_epoch = subsidy_balance.value() / (epochs_ahead as u64);

    epochs_ahead.do!(|i| {
        future_accounting
            .ring_lookup_mut(i)
            .rewards_balance()
            .join(subsidy_balance.split(reward_per_epoch));
    });

    // Add leftover rewards to the first epoch's accounting.
    future_accounting.ring_lookup_mut(0).rewards_balance().join(subsidy_balance);
}

/// Adds rewards to the system for the specified number of epochs ahead.
/// The rewards are split equally across the future accounting ring buffer up to the
/// specified epoch.
public(package) fun add_subsidy(
    self: &mut SystemStateInnerV1,
    subsidy: Coin<WAL>,
    epochs_ahead: u32,
) {
    add_subsidy_impl!(&mut self.future_accounting, subsidy, epochs_ahead)
}

public(package) fun add_subsidy_v2(
    self: &mut SystemStateInnerV2,
    subsidy: Coin<WAL>,
    epochs_ahead: u32,
) {
    add_subsidy_impl!(&mut self.future_accounting, subsidy, epochs_ahead)
}

/// Macro implementing add_per_epoch_subsidies logic.
macro fun add_per_epoch_subsidies_impl(
    $future_accounting: &mut FutureAccountingRingBuffer,
    $subsidies: vector<Balance<WAL>>,
) {
    let future_accounting = $future_accounting;
    let subsidies = $subsidies;

    assert!(subsidies.length() <= future_accounting.max_epochs_ahead() as u64, EInvalidEpochsAhead);
    let mut epochs_in_future = 0;
    subsidies.do!(|per_epoch_subsidy| {
        future_accounting
            .ring_lookup_mut(epochs_in_future)
            .rewards_balance()
            .join(per_epoch_subsidy);
        epochs_in_future = epochs_in_future + 1;
    })
}

/// Adds rewards to the system for future epochs, where `subsidies[i]` is added to the rewards
/// of epoch `system.epoch() + i`.
public(package) fun add_per_epoch_subsidies(
    self: &mut SystemStateInnerV1,
    subsidies: vector<Balance<WAL>>,
) {
    add_per_epoch_subsidies_impl!(&mut self.future_accounting, subsidies)
}

public(package) fun add_per_epoch_subsidies_v2(
    self: &mut SystemStateInnerV2,
    subsidies: vector<Balance<WAL>>,
) {
    add_per_epoch_subsidies_impl!(&mut self.future_accounting, subsidies)
}

// === Accessors ===

/// Accessor for total capacity size.
public(package) fun total_capacity_size(self: &SystemStateInnerV1): u64 {
    self.total_capacity_size
}

public(package) fun total_capacity_size_v2(self: &SystemStateInnerV2): u64 {
    self.total_capacity_size
}

/// Accessor for used capacity size.
public(package) fun used_capacity_size(self: &SystemStateInnerV1): u64 {
    self.used_capacity_size
}

public(package) fun used_capacity_size_v2(self: &SystemStateInnerV2): u64 {
    self.used_capacity_size
}

/// An accessor for the current committee.
public(package) fun committee(self: &SystemStateInnerV1): &BlsCommittee {
    &self.committee
}

public(package) fun committee_v2(self: &SystemStateInnerV2): &BlsCommittee {
    &self.committee
}

/// Accessor for epoch.
public(package) fun epoch(self: &SystemStateInnerV1): u32 {
    self.committee.epoch()
}

public(package) fun epoch_v2(self: &SystemStateInnerV2): u32 {
    self.committee.epoch()
}

/// Accessor for n_shards.
public(package) fun n_shards(self: &SystemStateInnerV1): u16 {
    self.committee.n_shards()
}

public(package) fun n_shards_v2(self: &SystemStateInnerV2): u16 {
    self.committee.n_shards()
}

/// Read-only access to the accounting ring buffer.
public(package) fun future_accounting(self: &SystemStateInnerV1): &FutureAccountingRingBuffer {
    &self.future_accounting
}

public(package) fun future_accounting_v2(self: &SystemStateInnerV2): &FutureAccountingRingBuffer {
    &self.future_accounting
}

#[test_only]
public(package) fun committee_mut(self: &mut SystemStateInnerV1): &mut BlsCommittee {
    &mut self.committee
}

#[test_only]
public(package) fun committee_mut_v2(self: &mut SystemStateInnerV2): &mut BlsCommittee {
    &mut self.committee
}

// TODO: check if this function is used.
public(package) fun write_price(self: &SystemStateInnerV2, write_size: u64): u64 {
    let storage_units = storage_units_from_size!(write_size);
    self.write_price_per_unit_size * storage_units
}

/// Sets the storage price per unit size. Called when a price vote is cast and the quorum
/// price is recalculated.
public(package) fun set_storage_price(self: &mut SystemStateInnerV1, price: u64) {
    self.storage_price_per_unit_size = price;
}

public(package) fun set_storage_price_v2(self: &mut SystemStateInnerV2, price: u64) {
    self.storage_price_per_unit_size = price;
}

/// Sets the write price per unit size. Called when a price vote is cast and the quorum
/// price is recalculated.
public(package) fun set_write_price(self: &mut SystemStateInnerV1, price: u64) {
    self.write_price_per_unit_size = price;
}

public(package) fun set_write_price_v2(self: &mut SystemStateInnerV2, price: u64) {
    self.write_price_per_unit_size = price;
}

#[test_only]
/// Returns the raw storage price per unit size.
public(package) fun storage_price_per_unit_size(self: &SystemStateInnerV1): u64 {
    self.storage_price_per_unit_size
}

#[test_only]
public(package) fun storage_price_per_unit_size_v2(self: &SystemStateInnerV2): u64 {
    self.storage_price_per_unit_size
}

#[test_only]
/// Returns the raw write price per unit size.
public(package) fun write_price_per_unit_size(self: &SystemStateInnerV1): u64 {
    self.write_price_per_unit_size
}

#[test_only]
public(package) fun write_price_per_unit_size_v2(self: &SystemStateInnerV2): u64 {
    self.write_price_per_unit_size
}

#[test_only]
public(package) fun deny_list_sizes(self: &SystemStateInnerV2): &VecMap<ID, u64> {
    self.deny_list_sizes.borrow()
}

#[test_only]
public(package) fun deny_list_sizes_mut(self: &mut SystemStateInnerV2): &mut VecMap<ID, u64> {
    self.deny_list_sizes.borrow_mut()
}

#[test_only]
public(package) fun used_capacity_size_at_future_epoch(
    self: &SystemStateInnerV2,
    epochs_ahead: u32,
): u64 {
    self.future_accounting.ring_lookup(epochs_ahead).used_capacity()
}

macro fun storage_units_from_size($size: u64): u64 {
    let size = $size;
    size.divide_and_round_up(BYTES_PER_UNIT_SIZE)
}

// === Protocol Version ===

/// Macro implementing update_protocol_version logic.
macro fun update_protocol_version_impl(
    $committee: &BlsCommittee,
    $cap: &StorageNodeCap,
    $signature: vector<u8>,
    $members_bitmap: vector<u8>,
    $message: vector<u8>,
) {
    let committee = $committee;
    let cap = $cap;
    let signature = $signature;
    let members_bitmap = $members_bitmap;
    let message = $message;

    assert!(committee.contains(&cap.node_id()), ENotCommitteeMember);

    let certified_message = committee.verify_quorum_in_epoch(signature, members_bitmap, message);

    let epoch = certified_message.cert_epoch();
    let message = certified_message.protocol_version_message();
    let start_epoch = message.start_epoch();
    assert!(epoch == committee.epoch(), EInvalidIdEpoch);
    assert!(start_epoch >= committee.epoch(), EInvalidStartEpoch);

    events::emit_protocol_version(
        epoch,
        message.start_epoch(),
        message.protocol_version(),
    );
}

/// Check quorum of committee members and emit the protocol version event.
public(package) fun update_protocol_version(
    self: &SystemStateInnerV1,
    cap: &StorageNodeCap,
    signature: vector<u8>,
    members_bitmap: vector<u8>,
    message: vector<u8>,
) {
    update_protocol_version_impl!(&self.committee, cap, signature, members_bitmap, message)
}

public(package) fun update_protocol_version_v2(
    self: &SystemStateInnerV2,
    cap: &StorageNodeCap,
    signature: vector<u8>,
    members_bitmap: vector<u8>,
    message: vector<u8>,
) {
    update_protocol_version_impl!(&self.committee, cap, signature, members_bitmap, message)
}

// === DenyList ===

/// Macro implementing register_deny_list_update logic.
macro fun register_deny_list_update_impl(
    $committee: &BlsCommittee,
    $cap: &StorageNodeCap,
    $deny_list_root: u256,
    $deny_list_sequence: u64,
) {
    let committee = $committee;
    let cap = $cap;
    let deny_list_root = $deny_list_root;
    let deny_list_sequence = $deny_list_sequence;

    assert!(committee.contains(&cap.node_id()), ENotCommitteeMember);
    assert!(deny_list_sequence > cap.deny_list_sequence(), EIncorrectDenyListSequence);

    events::emit_register_deny_list_update(
        committee.epoch(),
        deny_list_root,
        deny_list_sequence,
        cap.node_id(),
    );
}

/// Announce a deny list update for a storage node.
public(package) fun register_deny_list_update(
    self: &SystemStateInnerV1,
    cap: &StorageNodeCap,
    deny_list_root: u256,
    deny_list_sequence: u64,
) {
    register_deny_list_update_impl!(&self.committee, cap, deny_list_root, deny_list_sequence)
}

public(package) fun register_deny_list_update_v2(
    self: &SystemStateInnerV2,
    cap: &StorageNodeCap,
    deny_list_root: u256,
    deny_list_sequence: u64,
) {
    register_deny_list_update_impl!(&self.committee, cap, deny_list_root, deny_list_sequence)
}

/// Macro implementing update_deny_list logic.
macro fun update_deny_list_impl(
    $committee: &BlsCommittee,
    $deny_list_sizes: &mut ExtendedField<VecMap<ID, u64>>,
    $cap: &mut StorageNodeCap,
    $signature: vector<u8>,
    $members_bitmap: vector<u8>,
    $message: vector<u8>,
) {
    let committee = $committee;
    let deny_list_sizes = $deny_list_sizes;
    let cap = $cap;
    let signature = $signature;
    let members_bitmap = $members_bitmap;
    let message = $message;

    assert!(committee.contains(&cap.node_id()), ENotCommitteeMember);

    let certified_message = committee.verify_quorum_in_epoch(signature, members_bitmap, message);

    let epoch = certified_message.cert_epoch();
    let message = certified_message.deny_list_update_message();
    let node_id = message.storage_node_id();
    let size = message.size();

    assert!(epoch == committee.epoch(), EInvalidIdEpoch);
    assert!(node_id == cap.node_id(), EIncorrectDenyListNode);
    assert!(cap.deny_list_sequence() < message.sequence_number(), EIncorrectDenyListSequence);

    let deny_list_root = message.root();
    let sequence_number = message.sequence_number();

    // update deny_list properties in the cap
    cap.set_deny_list_properties(deny_list_root, sequence_number, size);

    // then register the update in the system storage
    let sizes = deny_list_sizes.borrow_mut();
    if (sizes.contains(&node_id)) {
        *&mut sizes[&node_id] = message.size();
    } else {
        sizes.insert(node_id, message.size());
    };

    events::emit_deny_list_update(
        committee.epoch(),
        deny_list_root,
        sequence_number,
        cap.node_id(),
    );
}

/// Perform the update of the deny list; register updated root and sequence in
/// the `StorageNodeCap`.
public(package) fun update_deny_list(
    self: &mut SystemStateInnerV1,
    cap: &mut StorageNodeCap,
    signature: vector<u8>,
    members_bitmap: vector<u8>,
    message: vector<u8>,
) {
    update_deny_list_impl!(
        &self.committee,
        &mut self.deny_list_sizes,
        cap,
        signature,
        members_bitmap,
        message,
    )
}

public(package) fun update_deny_list_v2(
    self: &mut SystemStateInnerV2,
    cap: &mut StorageNodeCap,
    signature: vector<u8>,
    members_bitmap: vector<u8>,
    message: vector<u8>,
) {
    update_deny_list_impl!(
        &self.committee,
        &mut self.deny_list_sizes,
        cap,
        signature,
        members_bitmap,
        message,
    )
}

/// Macro implementing delete_deny_listed_blob logic.
macro fun delete_deny_listed_blob_impl(
    $committee: &BlsCommittee,
    $signature: vector<u8>,
    $members_bitmap: vector<u8>,
    $message: vector<u8>,
) {
    let committee = $committee;
    let signature = $signature;
    let members_bitmap = $members_bitmap;
    let message = $message;

    let certified_message = committee.verify_one_correct_node_in_epoch(
        signature,
        members_bitmap,
        message,
    );

    let epoch = certified_message.cert_epoch();
    let message = certified_message.deny_list_blob_deleted_message();

    assert!(epoch == committee.epoch(), EInvalidIdEpoch);

    events::emit_deny_listed_blob_deleted(epoch, message.blob_id());
}

/// Certify that a blob is on the deny list for at least one honest node. Emit
/// an event to mark it for deletion.
public(package) fun delete_deny_listed_blob(
    self: &SystemStateInnerV1,
    signature: vector<u8>,
    members_bitmap: vector<u8>,
    message: vector<u8>,
) {
    delete_deny_listed_blob_impl!(&self.committee, signature, members_bitmap, message)
}

public(package) fun delete_deny_listed_blob_v2(
    self: &SystemStateInnerV2,
    signature: vector<u8>,
    members_bitmap: vector<u8>,
    message: vector<u8>,
) {
    delete_deny_listed_blob_impl!(&self.committee, signature, members_bitmap, message)
}

// === Slashing ===

/// Votes to slash a target node's reward. The voter must be a current committee member.
/// The target must also be a current committee member. If the accumulated weight of votes
/// against a target exceeds 2f+1 shards, the target will not receive rewards in the
/// upcoming epoch change.
public(package) fun vote_to_slash(
    self: &mut SystemStateInnerV2,
    cap: &StorageNodeCap,
    target_node_id: ID,
) {
    let voter_node_id = cap.node_id();

    // Cannot slash yourself
    assert!(voter_node_id != target_node_id, ECannotSlashSelf);

    // Both voter and target must be in the current committee
    assert!(self.committee.contains(&voter_node_id), ENotCommitteeMember);
    assert!(self.committee.contains(&target_node_id), ENotCommitteeMember);

    // Get voter's weight (number of shards)
    let voter_weight = self.committee.get_member_weight(&voter_node_id);

    // Record the vote
    let slashing_votes = self.slashing_votes.borrow_mut();

    if (!slashing_votes.contains(&target_node_id)) {
        slashing_votes.insert(target_node_id, vec_map::empty());
    };

    let target_votes = &mut slashing_votes[&target_node_id];

    // Check for duplicate vote
    assert!(!target_votes.contains(&voter_node_id), EDuplicateSlashingVote);

    // Record the vote
    target_votes.insert(voter_node_id, voter_weight);
}

/// Calculates the total slashing weight for a target node.
fun calculate_slashing_weight(votes: &VecMap<ID, u16>): u16 {
    let mut total: u16 = 0;
    votes.length().do!(|i| {
        let (_, weight) = votes.get_entry_by_idx(i);
        total = total + *weight;
    });
    total
}

/// Returns the slashing threshold (2f+1) for the given number of shards.
fun is_slashing_threshold_reached(n_shards: u16, total_weight: u16): bool {
    // total_weight >= 2f + 1 = 2(n-1)/3 + 1
    // total_weight * 3 >= 2n + 1
    total_weight * 3 >= 2 * n_shards + 1
}

/// Returns the set of node IDs that have been slashed (received 2f+1 votes).
public(package) fun get_slashed_nodes(self: &SystemStateInnerV2): vector<ID> {
    let slashing_votes = self.slashing_votes.borrow();
    let mut slashed = vector[];

    slashing_votes.length().do!(|i| {
        let (target_id, votes) = slashing_votes.get_entry_by_idx(i);
        let total_weight = calculate_slashing_weight(votes);
        if (is_slashing_threshold_reached(self.committee.n_shards(), total_weight)) {
            slashed.push_back(*target_id);
        };
    });

    slashed
}

/// Returns the current slashing votes for a target node.
public(package) fun get_slashing_votes(self: &SystemStateInnerV2, target_node_id: ID): u16 {
    let slashing_votes = self.slashing_votes.borrow();
    if (!slashing_votes.contains(&target_node_id)) {
        return 0
    };
    calculate_slashing_weight(&slashing_votes[&target_node_id])
}

/// Applies slashing to the rewards map. Slashed nodes have their rewards burned.
/// Returns the modified rewards map with slashed nodes' rewards set to zero.
/// Must be called before `advance_epoch` in staking.
public(package) fun apply_slashing(
    self: &mut SystemStateInnerV2,
    rewards: VecMap<ID, Balance<WAL>>,
    treasury: &mut wal::wal::ProtectedTreasury,
    ctx: &mut TxContext,
): VecMap<ID, Balance<WAL>> {
    let slashed_nodes = self.get_slashed_nodes();

    // Clear slashing votes for the next epoch
    self.slashing_votes.swap(vec_map::empty());

    // If no nodes are slashed, return rewards unchanged
    if (slashed_nodes.is_empty()) {
        return rewards
    };

    // Process rewards: burn slashed nodes' rewards
    let (node_ids, rewards) = rewards.into_keys_values();
    let mut result = vec_map::empty();

    rewards.zip_do!(node_ids, |node_reward, node_id| {
        if (slashed_nodes.contains(&node_id)) {
            // Burn the slashed reward
            wal::wal::burn(treasury, node_reward.into_coin(ctx));
            // Add zero balance for this node
            result.insert(node_id, balance::zero());
        } else {
            result.insert(node_id, node_reward);
        };
    });

    result
}

// === Testing ===

#[test_only]
use walrus::test_utils;

#[test_only]
public(package) fun new_for_testing(): SystemStateInnerV2 {
    let committee = test_utils::new_bls_committee_for_testing(0);
    let ctx = &mut tx_context::dummy();
    SystemStateInnerV2 {
        committee,
        total_capacity_size: 1_000_000_000,
        used_capacity_size: 0,
        storage_price_per_unit_size: 5,
        write_price_per_unit_size: 1,
        future_accounting: storage_accounting::ring_new(104),
        event_blob_certification_state: event_blob::create_with_empty_state(),
        deny_list_sizes: extended_field::new(vec_map::empty(), ctx),
        slashing_votes: extended_field::new(vec_map::empty(), ctx),
    }
}

#[test_only]
public(package) fun new_for_testing_with_multiple_members(ctx: &mut TxContext): SystemStateInnerV2 {
    let committee = test_utils::new_bls_committee_with_multiple_members_for_testing(0, ctx);
    SystemStateInnerV2 {
        committee,
        total_capacity_size: 1_000_000_000,
        used_capacity_size: 0,
        storage_price_per_unit_size: 5,
        write_price_per_unit_size: 1,
        future_accounting: storage_accounting::ring_new(104),
        event_blob_certification_state: event_blob::create_with_empty_state(),
        deny_list_sizes: extended_field::new(vec_map::empty(), ctx),
        slashing_votes: extended_field::new(vec_map::empty(), ctx),
    }
}

#[test_only]
public(package) fun clear_slashing_votes_for_testing(self: &mut SystemStateInnerV2) {
    self.slashing_votes.swap(vec_map::empty());
}

#[test_only]
public(package) fun event_blob_certification_state(
    system: &SystemStateInnerV2,
): &EventBlobCertificationState {
    &system.event_blob_certification_state
}

#[test_only]
public(package) fun future_accounting_mut(
    self: &mut SystemStateInnerV1,
): &mut FutureAccountingRingBuffer {
    &mut self.future_accounting
}

#[test_only]
public(package) fun future_accounting_mut_v2(
    self: &mut SystemStateInnerV2,
): &mut FutureAccountingRingBuffer {
    &mut self.future_accounting
}

#[test_only]
public(package) fun destroy_for_testing(s: SystemStateInnerV2) {
    std::unit_test::destroy(s)
}

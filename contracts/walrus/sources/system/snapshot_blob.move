// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/// Module to certify blob info snapshot blobs.
///
/// A blob info snapshot is a deterministic serialization of the blob info tables that every
/// storage node produces at the epoch boundary. All honest nodes produce bit-identical bytes,
/// and therefore the same blob id, so certification is a per-epoch quorum vote on that blob id.
/// This follows the event blob certification pattern (`event_blob.move`) with one
/// simplification: there is exactly one snapshot per epoch, so attestations are keyed by epoch
/// instead of by checkpoint, and no per-capability attestation bookkeeping is needed — a node
/// may attest at most once per epoch, tracked in this state directly.
module walrus::snapshot_blob;

use sui::{vec_map::{Self, VecMap}, vec_set::{Self, VecSet}};

// Error codes
// Error types in `walrus-sui/types/move_errors.rs` are auto-generated from the Move error codes.
/// The epoch of the certified snapshot must be strictly increasing.
const EInvalidEpoch: u64 = 0;
/// The snapshot storage lifetime must be at most the system's `max_epochs_ahead` and at least
/// `MIN_SNAPSHOT_EPOCHS_AHEAD`, or `max_epochs_ahead` if that is smaller.
const EInvalidEpochsAhead: u64 = 1;

/// The smallest number of epochs ahead for which a certified snapshot blob may be stored.
///
/// A snapshot certified in epoch E with a lifetime of L epochs expires at epoch E + L. Storage
/// nodes check whether it certified at the start of epoch E + 1, after expiring the blobs whose
/// storage ends there, so a lifetime of one epoch would make every snapshot expire before that
/// check and never leave it available for recovery.
///
/// A system whose `max_epochs_ahead` is below this minimum cannot store any blob that long. It
/// is not made unmigratable for that: its lifetime is bounded by its maximum instead, and its
/// snapshots expire before the next boundary's check, as described above.
const MIN_SNAPSHOT_EPOCHS_AHEAD: u32 = 2;

/// A certified blob info snapshot.
public struct SnapshotBlob has copy, drop, store {
    /// The walrus epoch whose boundary state the snapshot captures.
    epoch: u32,
    /// Blob id of the certified snapshot blob.
    blob_id: u256,
    /// The epoch at which the storage of the snapshot blob ends (exclusive). Recorded here
    /// because the blob object is burned at certification, so the chain has no other record
    /// of how long the snapshot stays retrievable.
    end_epoch: u32,
}

/// State of blob info snapshot certification.
///
/// Lives in a dynamic field on the `System` object (see `system.move`) because the layout of
/// `SystemStateInnerV1` is frozen and cannot gain a field.
public struct SnapshotBlobCertificationState has store {
    /// The certified snapshot blobs, oldest first: after every certification, the newest
    /// entry, the previous one whatever its lifetime, and every older entry whose storage has
    /// not ended. Expired entries are dropped only when a snapshot is certified, so readers
    /// must compare `end_epoch` with the current epoch. Bounded by `max_epochs_ahead + 1`
    /// entries, since at most one snapshot is certified per epoch.
    certified: vector<SnapshotBlob>,
    /// Number of epochs ahead for which a certified snapshot blob is stored.
    epochs_ahead: u32,
    /// The epoch the attestations below belong to. Attestations of an older epoch are cleared
    /// lazily when the first attestation of a newer epoch arrives.
    tally_epoch: u32,
    /// Aggregate attested shard weight per snapshot blob id for `tally_epoch`.
    aggregate_weight_per_blob: VecMap<u256, u16>,
    /// Nodes that have attested a snapshot for `tally_epoch`.
    attested_nodes: VecSet<ID>,
}

// === Accessors for SnapshotBlob ===

/// Returns the epoch of the snapshot blob.
public(package) fun epoch(self: &SnapshotBlob): u32 {
    self.epoch
}

/// Returns the epoch at which the storage of the snapshot blob ends (exclusive).
public(package) fun end_epoch(self: &SnapshotBlob): u32 {
    self.end_epoch
}

/// Returns the blob id of the snapshot blob.
public(package) fun blob_id(self: &SnapshotBlob): u256 {
    self.blob_id
}

// === Accessors for SnapshotBlobCertificationState ===

/// Creates a certification state with no certified snapshot and no attestations.
///
/// The snapshot storage lifetime starts at the system's `max_epochs_ahead`, the longest a blob
/// can be stored: a recovering node needs the last certified snapshot to still be stored, so the
/// lifetime bounds the certification outage the network can recover from, and the cost of the
/// superseded snapshots this keeps is accepted for that. It can be lowered later through
/// `set_epochs_ahead`.
public(package) fun create_with_empty_state(max_epochs_ahead: u32): SnapshotBlobCertificationState {
    assert_valid_epochs_ahead(max_epochs_ahead, max_epochs_ahead);
    SnapshotBlobCertificationState {
        certified: vector[],
        epochs_ahead: max_epochs_ahead,
        tally_epoch: 0,
        aggregate_weight_per_blob: vec_map::empty(),
        attested_nodes: vec_set::empty(),
    }
}

/// Returns the number of epochs ahead for which a certified snapshot blob is stored.
public(package) fun epochs_ahead(self: &SnapshotBlobCertificationState): u32 {
    self.epochs_ahead
}

/// Sets the number of epochs ahead for which certified snapshot blobs are stored.
///
/// Applies to snapshots certified after the change; the storage of already certified snapshots
/// is unaffected.
public(package) fun set_epochs_ahead(
    self: &mut SnapshotBlobCertificationState,
    epochs_ahead: u32,
    max_epochs_ahead: u32,
) {
    assert_valid_epochs_ahead(epochs_ahead, max_epochs_ahead);
    self.epochs_ahead = epochs_ahead;
}

/// Aborts unless `min_epochs_ahead <= epochs_ahead <= max_epochs_ahead`, where the lower bound
/// is `MIN_SNAPSHOT_EPOCHS_AHEAD` or, on a system whose `max_epochs_ahead` is smaller, that
/// maximum: see the lower bound for why a shorter lifetime is useless, and the reservation is
/// bounded by the system's accounting horizon.
fun assert_valid_epochs_ahead(epochs_ahead: u32, max_epochs_ahead: u32) {
    let min_epochs_ahead = MIN_SNAPSHOT_EPOCHS_AHEAD.min(max_epochs_ahead);
    assert!(
        epochs_ahead >= min_epochs_ahead && epochs_ahead <= max_epochs_ahead,
        EInvalidEpochsAhead,
    );
}

#[test_only]
public fun destroy_for_testing(self: SnapshotBlobCertificationState) {
    let SnapshotBlobCertificationState {
        certified: _,
        epochs_ahead: _,
        tally_epoch: _,
        aggregate_weight_per_blob: _,
        attested_nodes: _,
    } = self;
}

/// Returns the certified snapshot blobs kept in the state (see `certified`), oldest first.
public(package) fun certified_history(
    self: &SnapshotBlobCertificationState,
): &vector<SnapshotBlob> {
    &self.certified
}

/// Returns the latest certified snapshot blob.
public(package) fun latest_certified(self: &SnapshotBlobCertificationState): Option<SnapshotBlob> {
    let length = self.certified.length();
    if (length == 0) {
        option::none()
    } else {
        option::some(self.certified[length - 1])
    }
}

/// Returns the certified snapshot blob of `epoch`, if it is among the ones kept in the state.
public(package) fun certified_for_epoch(
    self: &SnapshotBlobCertificationState,
    epoch: u32,
): Option<SnapshotBlob> {
    let mut index = 0;
    while (index < self.certified.length()) {
        if (self.certified[index].epoch == epoch) {
            return option::some(self.certified[index])
        };
        index = index + 1;
    };
    option::none()
}

/// Returns the epoch of the latest certified snapshot blob.
public(package) fun get_latest_certified_epoch(self: &SnapshotBlobCertificationState): Option<u32> {
    self.latest_certified().map!(|snapshot| snapshot.epoch())
}

/// Returns the blob id of the latest certified snapshot blob.
public(package) fun get_latest_certified_blob_id(
    self: &SnapshotBlobCertificationState,
): Option<u256> {
    self.latest_certified().map!(|snapshot| snapshot.blob_id())
}

/// Returns the number of snapshot blob ids being tracked for the current tally epoch.
public(package) fun get_num_tracked_blobs(self: &SnapshotBlobCertificationState): u64 {
    self.aggregate_weight_per_blob.length()
}

/// Returns true if a snapshot for `epoch` (or a later epoch) is already certified.
public(package) fun is_epoch_certified(self: &SnapshotBlobCertificationState, epoch: u32): bool {
    self.latest_certified().map!(|snapshot| snapshot.epoch() >= epoch).destroy_or!(false)
}

/// Returns true if `node_id` has already attested a snapshot for the current tally epoch.
public(package) fun has_attested(self: &SnapshotBlobCertificationState, node_id: &ID): bool {
    self.attested_nodes.contains(node_id)
}

/// Moves the tally to `epoch`, clearing all attestations of older epochs.
///
/// Called lazily on each attestation instead of during the epoch change, so that the epoch
/// change transaction does not need to touch this state.
public(package) fun advance_tally_epoch(self: &mut SnapshotBlobCertificationState, epoch: u32) {
    if (self.tally_epoch != epoch) {
        self.tally_epoch = epoch;
        self.aggregate_weight_per_blob = vec_map::empty();
        self.attested_nodes = vec_set::empty();
    }
}

/// Records that `node_id` attested a snapshot for the current tally epoch.
public(package) fun record_attestation(self: &mut SnapshotBlobCertificationState, node_id: ID) {
    self.attested_nodes.insert(node_id);
}

/// Adds `weight` to the aggregate weight of the snapshot with the given blob id and returns
/// the updated aggregate weight.
public(package) fun add_aggregate_weight(
    self: &mut SnapshotBlobCertificationState,
    blob_id: u256,
    weight: u16,
): u16 {
    if (!self.aggregate_weight_per_blob.contains(&blob_id)) {
        self.aggregate_weight_per_blob.insert(blob_id, 0);
    };
    let aggregate_weight = &mut self.aggregate_weight_per_blob[&blob_id];
    *aggregate_weight = *aggregate_weight + weight;
    *aggregate_weight
}

/// Records the snapshot with the given blob id as certified for `epoch`, with storage ending at
/// `end_epoch` (exclusive), drops the recorded snapshots whose storage has ended by `epoch`
/// except the previously certified one, and stops tracking the attestations that led to it.
///
/// `epoch` is the current epoch, since only attestations for the current epoch are accepted.
public(package) fun certify(
    self: &mut SnapshotBlobCertificationState,
    epoch: u32,
    blob_id: u256,
    end_epoch: u32,
) {
    self.get_latest_certified_epoch().do!(|latest_epoch| {
        assert!(epoch > latest_epoch, EInvalidEpoch);
    });
    let length = self.certified.length();
    let mut kept = vector[];
    length.do!(|index| {
        let snapshot = self.certified[index];
        // Expired snapshots are dropped, except the latest one: storage nodes reconcile it at
        // the next epoch boundary and a recovering node needs it as a fallback, whatever its
        // lifetime.
        if (snapshot.end_epoch > epoch || index + 1 == length) {
            kept.push_back(snapshot);
        };
    });
    kept.push_back(SnapshotBlob { epoch, blob_id, end_epoch });
    self.certified = kept;
    self.aggregate_weight_per_blob = vec_map::empty();
    self.attested_nodes = vec_set::empty();
}

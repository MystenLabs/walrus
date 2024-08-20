// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/// Module to emit events. Used to allow filtering all events in the
/// rust client (as work-around for the lack of composable event filters).
module walrus::events;

use sui::event;

// == Event definitions ==

/// Signals that a blob with meta-data has been registered.
public struct BlobRegistered has copy, drop {
    epoch: u64,
    blob_id: u256,
    size: u64,
    erasure_code_type: u8,
    end_epoch: u64,
    deletable: bool,
    // The object id of the related `Blob` object
    object_id: ID,
}

/// Signals that a blob is certified.
public struct BlobCertified has copy, drop {
    epoch: u64,
    blob_id: u256,
    end_epoch: u64,
    deletable: bool,
    // The object id of the related `Blob` object
    object_id: ID,
    // Marks if this is an extension for explorers, etc.
    is_extension: bool,
}

/// Signals that a blob has been deleted.
public struct BlobDeleted has copy, drop {
    epoch: u64,
    blob_id: u256,
    end_epoch: u64,
    // The object ID of the related `Blob` object.
    object_id: ID,
}

/// Signals that a BlobID is invalid.
public struct InvalidBlobID has copy, drop {
    epoch: u64, // The epoch in which the blob ID is first registered as invalid
    blob_id: u256,
}

/// Signals that the epoch change has started.
public struct EpochChangeStart has copy, drop {
    epoch: u64,
}

/// Signals that nodes with 2f+1 shards have finished the epoch change.
public struct EpochChangeDone has copy, drop {
    epoch: u64,
}

/// Signals that a specific node has received all shards for the new epoch.
public struct AllShardsReceived has copy, drop {
    epoch: u64,
    node_id: ID,
}

/// Signals that the committee for the next epoch has been selected.
public struct CommitteeSelected has copy, drop {
    epoch: u64,
}

/// Signals that the happy path shard transfer has failed for the given shards.
public struct ShardTransferFailed has copy, drop {
    epoch: u64,
    shards: vector<u16>,
}

// == Functions to emit the events from other modules ==

public(package) fun emit_blob_registered(
    epoch: u64,
    blob_id: u256,
    size: u64,
    erasure_code_type: u8,
    end_epoch: u64,
    deletable: bool,
    object_id: ID,
) {
    event::emit(BlobRegistered {
        epoch,
        blob_id,
        size,
        erasure_code_type,
        end_epoch,
        deletable,
        object_id,
    });
}

public(package) fun emit_blob_certified(
    epoch: u64,
    blob_id: u256,
    end_epoch: u64,
    deletable: bool,
    object_id: ID,
    is_extension: bool,
) {
    event::emit(BlobCertified { epoch, blob_id, end_epoch, deletable, object_id, is_extension });
}

public(package) fun emit_invalid_blob_id(epoch: u64, blob_id: u256) {
    event::emit(InvalidBlobID { epoch, blob_id });
}

public(package) fun emit_blob_deleted(epoch: u64, blob_id: u256, end_epoch: u64, object_id: ID) {
    event::emit(BlobDeleted { epoch, blob_id, end_epoch, object_id });
}

public(package) fun emit_epoch_change_start(epoch: u64) {
    event::emit(EpochChangeStart { epoch })
}

public(package) fun emit_epoch_change_done(epoch: u64) {
    event::emit(EpochChangeDone { epoch })
}

public(package) fun emit_all_shards_received(epoch: u64, node_id: ID) {
    event::emit(AllShardsReceived { epoch, node_id })
}

public(package) fun emit_committee_selected(epoch: u64) {
    event::emit(CommitteeSelected { epoch })
}

public(package) fun emit_shard_transfer_failed(epoch: u64, shards: vector<u16>) {
    event::emit(ShardTransferFailed { epoch, shards })
}

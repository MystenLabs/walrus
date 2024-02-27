// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

module blob_store::blob {

    use sui::object::{Self, UID};
    use sui::tx_context::TxContext;
    use sui::bcs;

    use blob_store::committee::{Self, CertifiedMessage};
    use blob_store::system::{Self, System};
    use blob_store::storage_resource::{Storage, start_epoch, end_epoch, storage_size};

    // Error codes
    const ERROR_INVALID_MSG_TYPE: u64 = 1;
    const ERROR_RESOURCE_BOUNDS: u64 = 2;
    const ERROR_RESOURCE_SIZE: u64 = 3;

    struct Blob<phantom TAG> has key, store {
        id: UID,
        stored_epoch: u64,
        blob_id: u256,
        size: u64,
        fe_type: u8,
        certified: bool,
        storage: Storage<TAG>,
    }

    // Accessor functions

    public fun stored_epoch<TAG>(b: &Blob<TAG>) : u64 {
        b.stored_epoch
    }

    public fun blob_id<TAG>(b: &Blob<TAG>) : u256 {
        b.blob_id
    }

    public fun size<TAG>(b: &Blob<TAG>) : u64 {
        b.size
    }

    public fun fe_type<TAG>(b: &Blob<TAG>) : u8 {
        b.fe_type
    }

    public fun storage<TAG>(b: &Blob<TAG>) : &Storage<TAG> {
        &b.storage
    }

    /// Register a new blob in the system.
    public fun register<TAG,WAL:store>(
        sys: &System<TAG,WAL>,
        storage: Storage<TAG>,
        blob_id: u256,
        size: u64,
        fe_type: u8,
        ctx: &mut TxContext,
        ) : Blob<TAG> {

        let id = object::new(ctx);
        let stored_epoch = system::epoch(sys);

        // Check resource bounds.
        assert!(stored_epoch >= start_epoch(&storage), ERROR_RESOURCE_BOUNDS);
        assert!(stored_epoch < end_epoch(&storage), ERROR_RESOURCE_BOUNDS);
        assert!(size <= storage_size(&storage), ERROR_RESOURCE_SIZE);

        // TODO: cryptographically verify that the Blob ID authenticates
        // both the size and fe_type.

        Blob {
            id,
            stored_epoch,
            blob_id,
            size,
            fe_type,
            certified: false,
            storage,
        }

    }

    // A certify blob message structure
    const BLOB_CERT_MSG_TYPE: u8 = 1;

    struct CertifiedBlobMessage<phantom TAG> has drop {
        epoch: u64,
        blob_id: u256,
    }

    /// Construct the certified blob message, note that constructing
    /// implies a certified message, that is already checked.
    public fun certify_blob_message<TAG>(
        message: CertifiedMessage<TAG>
        ) : CertifiedBlobMessage<TAG> {

        // Assert type is correct
        assert!(committee::intent_type(&message) == BLOB_CERT_MSG_TYPE,
            ERROR_INVALID_MSG_TYPE);

        // The certified blob message contain a blob_id : u256
        let epoch = committee::cert_epoch(&message);
        let message_body = committee::into_message(message);

        let bcs_body = bcs::new(message_body);
        let blob_id = bcs::peel_u256(&mut bcs_body);

        CertifiedBlobMessage {
            epoch,
            blob_id,
        }
    }

    const ERROR_WRONG_EPOCH: u64 = 4;
    const ERROR_ALREADY_CERTIFIED: u64 = 5;
    const ERROR_INVALID_BLOB_ID: u64 = 6;

    public fun certify<TAG, WAL:store>(
        sys: &System<TAG, WAL>,
        message: CertifiedBlobMessage<TAG>,
        blob: &mut Blob<TAG>,
    ){

        // Check that the blob is registered in the system
        assert!(blob_id(blob) == message.blob_id, ERROR_INVALID_BLOB_ID);

        // Check that the blob is not already certified
        assert!(!blob.certified, ERROR_ALREADY_CERTIFIED);

        // Check that the message is from the current epoch
        assert!(message.epoch == system::epoch(sys), ERROR_WRONG_EPOCH);

        // Check that the storage in the blob is still valid
        assert!(message.epoch < end_epoch(storage(blob)), ERROR_RESOURCE_BOUNDS);

        // Mark the blob as certified
        blob.certified = true;
    }

    public fun extend(){
        // TODO: implement at a later time
        assert!(false, 0);
    }


}
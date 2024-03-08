// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

module blob_store::blob {

    use sui::object::{Self, UID};
    use sui::tx_context::TxContext;
    use sui::bcs;
    use sui::event;

    use blob_store::committee::{Self, CertifiedMessage};
    use blob_store::system::{Self, System};
    use blob_store::storage_resource::{
        Storage,
        start_epoch,
        end_epoch,
        storage_size,
        fuse_periods,
        destroy};

    // A certify blob message structure
    const BLOB_CERT_MSG_TYPE: u8 = 1;

    // Error codes
    const ERROR_INVALID_MSG_TYPE: u64 = 1;
    const ERROR_RESOURCE_BOUNDS: u64 = 2;
    const ERROR_RESOURCE_SIZE: u64 = 3;
    const ERROR_WRONG_EPOCH: u64 = 4;
    const ERROR_ALREADY_CERTIFIED: u64 = 5;
    const ERROR_INVALID_BLOB_ID: u64 = 6;
    const ERROR_NOT_CERTIFIED : u64 = 7;

    // Event definitions

    // Signals a blob with meta-data is registered.
    struct BlobRegistered<phantom TAG> has copy, drop {
        epoch: u64,
        blob_id: u256,
        size: u64,
        erasure_code_type: u8,
        end_epoch: u64,
    }

    // Signals a blob is certified.
    struct BlobCertified<phantom TAG> has copy, drop {
        epoch: u64,
        blob_id: u256,
        end_epoch: u64,
    }


    // Object definitions

    /// The blob structure represents a blob that has been registered to with some storage,
    /// and then may eventually be certified as being available in the system.
    struct Blob<phantom TAG> has key, store {
        id: UID,
        stored_epoch: u64,
        blob_id: u256,
        size: u64,
        erasure_code_type: u8,
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

    public fun erasure_code_type<TAG>(b: &Blob<TAG>) : u8 {
        b.erasure_code_type
    }

    public fun certified<TAG>(b: &Blob<TAG>) : bool {
        b.certified
    }

    public fun storage<TAG>(b: &Blob<TAG>) : &Storage<TAG> {
        &b.storage
    }

    /// Register a new blob in the system.
    public fun register<TAG,WAL>(
        sys: &System<TAG,WAL>,
        storage: Storage<TAG>,
        blob_id: u256,
        size: u64,
        erasure_code_type: u8,
        ctx: &mut TxContext,
        ) : Blob<TAG> {

        let id = object::new(ctx);
        let stored_epoch = system::epoch(sys);

        // Check resource bounds.
        assert!(stored_epoch >= start_epoch(&storage), ERROR_RESOURCE_BOUNDS);
        assert!(stored_epoch < end_epoch(&storage), ERROR_RESOURCE_BOUNDS);
        assert!(size <= storage_size(&storage), ERROR_RESOURCE_SIZE);

        // TODO(#42): cryptographically verify that the Blob ID authenticates
        // both the size and fe_type.

        // Emit register event
        event::emit(BlobRegistered<TAG> {
            epoch: stored_epoch,
            blob_id,
            size,
            erasure_code_type,
            end_epoch: end_epoch(&storage),
        });

        Blob {
            id,
            stored_epoch,
            blob_id,
            size,
            //
            erasure_code_type,
            certified: false,
            storage,
        }

    }

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

        // On purpose we do not check that nothing is left in the message
        // to allow in the future for extensibility.

        CertifiedBlobMessage {
            epoch,
            blob_id,
        }
    }

    /// Certify that a blob will be available in the storage system until the end epoch of the
    /// storage associated with it.
    public fun certify<TAG, WAL>(
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

        // Emit certified event
        event::emit(BlobCertified<TAG> {
            epoch: message.epoch,
            blob_id: message.blob_id,
            end_epoch: end_epoch(storage(blob)),
        });
    }

    /// After the period of validity expires for the blob we can destroy the blob resource.
    public fun destroy_blob<TAG, WAL>(
        sys: &System<TAG, WAL>,
        blob: Blob<TAG>,
    ){

        let current_epoch = system::epoch(sys);
        assert!(current_epoch >= end_epoch(storage(&blob)), ERROR_RESOURCE_BOUNDS);

        // Destroy the blob
        let Blob {
            id,
            stored_epoch: _,
            blob_id: _,
            size: _,
            erasure_code_type: _,
            certified: _,
            storage,
        } = blob;

        object::delete(id);
        destroy(storage);
    }

    /// Extend the period of validity of a blob with a new storage resource.
    /// The new storage resource must be the same size as the storage resource
    /// used in the blob, and have a longer period of validity.
    public fun extend<TAG,WAL>(
        sys: &System<TAG, WAL>,
        blob: &mut Blob<TAG>,
        extension: Storage<TAG>){

        // We only extend certified blobs within their period of validity
        // with storage that extends this period. First we check for these
        // conditions.

        // Assert this is a certified blob
        assert!(blob.certified, ERROR_NOT_CERTIFIED);

        // Check the blob is within its availability period
        assert!(system::epoch(sys) < end_epoch(storage(blob)), ERROR_RESOURCE_BOUNDS);

        // Check that the extension is valid, and the end
        // period of the extension is after the current period.
        assert!(end_epoch(&extension) > end_epoch(storage(blob)), ERROR_RESOURCE_BOUNDS);

        // Note: if the amounts do not match there will be an error.
        fuse_periods(&mut blob.storage , extension);

    }

    // Testing Functions

    #[test_only]
    public fun drop_for_testing<TAG>(b: Blob<TAG>) {
        // deconstruct
        let Blob {
            id,
            stored_epoch: _,
            blob_id: _,
            size: _,
            erasure_code_type: _,
            certified: _,
            storage,
        } = b;

        object::delete(id);
        destroy(storage);
    }

    #[test_only]
    // Accessor for blob
    public fun message_blob_id<TAG>(m: &CertifiedBlobMessage<TAG>) : u256 {
        m.blob_id
    }


    #[test_only]
    public fun certified_blob_message_for_testing<TAG>(
        epoch: u64,
        blob_id: u256,
        ) : CertifiedBlobMessage<TAG> {
        CertifiedBlobMessage {
            epoch,
            blob_id,
        }
    }

}

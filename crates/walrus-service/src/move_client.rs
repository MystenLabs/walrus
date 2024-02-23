use std::time::Duration;

use crate::{config::Committee, crypto::Certificate, BlobId, Epoch};

/// A client for interacting with the Walrus Move smart contract.
pub trait MoveClient {
    /// The id of a storage resource.
    type StorageResourceId;

    /// Get the Walrus committee for a given epoch. If the epoch is not provided, the current epoch
    /// is used instead
    fn get_committee(&self, epoch: Option<Epoch>) -> Committee;

    /// Purchase blob storage with the specified parameters.
    ///
    /// # Arguments
    ///
    /// * `blob_id` - The ID of the blob.
    /// * `size` - The size of the blob in bytes.
    /// * `duration` - The duration of the storage lease.
    ///
    /// # Returns
    ///
    /// The ID of the storage resource.
    fn purchase_blob_storage(
        &self,
        blob_id: BlobId,
        size: usize,
        duration: Duration,
    ) -> Self::StorageResourceId;

    // Submit a blob certificate for a storage resource.
    ///
    /// # Arguments
    ///
    /// * `storage_resource_id` - The ID of the storage resource.
    /// * `certificate` - The blob certificate.
    /// * `epoch` - The epoch of the certificate.
    fn submit_blob_certificate(
        &self,
        storage_resource_id: Self::StorageResourceId,
        certificate: Certificate,
        epoch: Epoch,
    );
}

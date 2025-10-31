// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Client for managing blobs through BlobManager.

use std::{collections::HashMap, fmt::Debug};

use sui_types::base_types::ObjectID;
use walrus_core::EpochCount;
use walrus_sui::client::{BlobObjectMetadata, BlobPersistence, SuiContractClient};

use crate::{
    client::{
        WalrusNodeClient,
        WalrusStoreBlob,
        client_types::WalrusStoreBlobApi,
        resource::{RegisterBlobOp, StoreOp},
    },
    error::ClientResult,
};

/// A facade for interacting with Walrus BlobManager.
#[derive(Debug)]
pub struct BlobManagerClient<'a, T> {
    client: &'a mut WalrusNodeClient<T>,
    manager_id: ObjectID,
    manager_cap: ObjectID,
}

impl<'a, T> BlobManagerClient<'a, T> {
    /// Creates a new BlobManagerClient.
    pub fn new(
        client: &'a mut WalrusNodeClient<T>,
        manager_id: ObjectID,
        manager_cap: ObjectID,
    ) -> Self {
        Self {
            client,
            manager_id,
            manager_cap,
        }
    }

    /// Get the BlobManager object ID.
    pub fn manager_id(&self) -> ObjectID {
        self.manager_id
    }

    /// Get the BlobManagerCap object ID.
    pub fn manager_cap(&self) -> ObjectID {
        self.manager_cap
    }
}

impl BlobManagerClient<'_, SuiContractClient> {
    /// Reserve storage and register multiple blobs in the BlobManager.
    ///
    /// This function:
    /// 1. Reserves the total storage needed for all blobs
    /// 2. Splits the storage for each individual blob
    /// 3. Registers each blob with the BlobManager
    ///
    /// Returns the registered Blob objects. Check `blob.certified_epoch` to determine
    /// if a blob was newly registered (None) or already existed via deduplication (Some).
    pub async fn reserve_and_register_blobs(
        &mut self,
        epochs_ahead: EpochCount,
        blob_metadata_list: Vec<BlobObjectMetadata>,
        persistence: BlobPersistence,
    ) -> ClientResult<Vec<walrus_sui::types::move_structs::Blob>> {
        self.client
            .sui_client()
            .reserve_and_register_blobs_in_blobmanager(
                self.manager_id,
                self.manager_cap.into(),
                epochs_ahead,
                blob_metadata_list,
                persistence,
            )
            .await
            .map_err(|e| e.into())
    }

    /// Registers blobs with the BlobManager and returns WalrusStoreBlob results.
    ///
    /// This is similar to `ResourceManager::register_walrus_store_blobs()` but uses
    /// the BlobManager for registration instead of direct system registration.
    ///
    /// The BlobManager may return already-certified blobs (via deduplication), which
    /// will be wrapped in `WalrusStoreBlob::Completed`. Newly registered blobs will
    /// be wrapped in `WalrusStoreBlob::Registered`.
    pub async fn register_blobmanager_store_blobs<'a, T: Debug + Clone + Send + Sync>(
        &mut self,
        encoded_blobs_with_status: Vec<WalrusStoreBlob<'a, T>>,
        epochs_ahead: EpochCount,
        persistence: BlobPersistence,
    ) -> ClientResult<Vec<WalrusStoreBlob<'a, T>>> {
        // Extract metadata from blobs
        let blob_metadata_list: Vec<BlobObjectMetadata> = encoded_blobs_with_status
            .iter()
            .map(|blob| {
                blob.get_metadata()
                    .expect("metadata should be present")
                    .try_into()
                    .expect("metadata conversion should succeed")
            })
            .collect();

        // Register blobs in BlobManager
        let registered_blobs = self
            .reserve_and_register_blobs(epochs_ahead, blob_metadata_list.clone(), persistence)
            .await?;

        // Convert Blob objects to RegisterBlobOp based on their state
        let blob_ops: Vec<(walrus_sui::types::move_structs::Blob, RegisterBlobOp)> =
            registered_blobs
                .into_iter()
                .zip(blob_metadata_list.iter())
                .map(|(blob, metadata)| {
                    let register_op = if blob.certified_epoch.is_some() {
                        // Already certified (deduplication case)
                        RegisterBlobOp::ReuseRegistration {
                            encoded_length: metadata.encoded_size,
                        }
                    } else {
                        // Newly registered
                        RegisterBlobOp::RegisterFromScratch {
                            encoded_length: metadata.encoded_size,
                            epochs_ahead,
                        }
                    };
                    (blob, register_op)
                })
                .collect();

        // Build a map of blob_id -> Vec<(Blob, RegisterBlobOp)>
        let mut blob_id_map = HashMap::new();
        blob_ops.into_iter().for_each(|(blob, op)| {
            let blob_id = blob.blob_id;
            blob_id_map
                .entry(blob_id)
                .or_insert_with(Vec::new)
                .push((blob, op));
        });

        // Convert back to WalrusStoreBlob using the same pattern as register_or_reuse_resources
        Ok(encoded_blobs_with_status
            .into_iter()
            .map(|blob| {
                let blob_id = blob.get_blob_id().expect("blob ID should be present");

                let Some(entries) = blob_id_map.get_mut(&blob_id) else {
                    panic!("missing blob ID: {blob_id}");
                };

                if let Some((blob_obj, operation)) = entries.pop() {
                    if entries.is_empty() {
                        blob_id_map.remove(&blob_id);
                    }

                    blob.with_register_result(Ok(StoreOp::new(operation, blob_obj)))
                        .expect("should succeed on Ok result")
                } else {
                    panic!("missing blob ID: {blob_id}");
                }
            })
            .collect())
    }
}

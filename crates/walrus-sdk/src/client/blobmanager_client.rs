// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Client for managing blobs through BlobManager.

use std::fmt::Debug;

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
    client: &'a WalrusNodeClient<T>,
    manager_id: ObjectID,
    manager_cap: ObjectID,
}

impl<'a, T> BlobManagerClient<'a, T> {
    /// Creates a new BlobManagerClient.
    pub fn new(
        client: &'a WalrusNodeClient<T>,
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
    /// Returns the ObjectIDs of registered blobs (blobs stay in BlobManager's table).
    #[allow(dead_code)]
    pub async fn reserve_and_register_blobs(
        &self,
        epochs_ahead: EpochCount,
        blob_metadata_list: Vec<BlobObjectMetadata>,
        persistence: BlobPersistence,
    ) -> ClientResult<Vec<ObjectID>> {
        tracing::info!(
            "BlobManager reserve_and_register: manager_id={:?}, manager_cap={:?}, num_blobs={}",
            self.manager_id,
            self.manager_cap,
            blob_metadata_list.len()
        );

        let blob_infos: Vec<walrus_sui::types::move_structs::ManagedBlobInfo> = self
            .client
            .sui_client()
            .reserve_and_register_blobs_in_blobmanager(
                self.manager_id,
                self.manager_cap.into(),
                epochs_ahead,
                blob_metadata_list,
                persistence,
            )
            .await
            .map_err(crate::error::ClientError::from)?;

        // Extract ObjectIDs from ManagedBlobInfo
        Ok(blob_infos.into_iter().map(|info| info.object_id).collect())
    }

    /// Registers blobs with the BlobManager and returns WalrusStoreBlob results.
    ///
    /// DEPRECATED: This function is outdated and uses the old flow.
    /// Use the ResourceManager::register_walrus_store_blobs() with blob_manager_id/cap instead.
    #[allow(dead_code, deprecated, unused_variables, unreachable_code)]
    #[deprecated]
    pub async fn register_blobmanager_store_blobs<'a, T: Debug + Clone + Send + Sync>(
        &self,
        encoded_blobs_with_status: Vec<WalrusStoreBlob<'a, T>>,
        epochs_ahead: EpochCount,
        persistence: BlobPersistence,
    ) -> ClientResult<Vec<WalrusStoreBlob<'a, T>>> {
        // This function is deprecated and no longer works with the new ObjectID-based flow
        unimplemented!(
            "This function is deprecated. Use ResourceManager::register_walrus_store_blobs() with blob_manager_id/cap instead."
        );
    }

    /// Certifies and completes blobs that were registered with the BlobManager.
    ///
    /// Takes blobs with StoreOp::RegisteredInBlobManager, certifies them,
    /// and returns completed blobs with BlobStoreResult::ManagedByBlobManager.
    ///
    /// This is the main certification workflow for BlobManager-registered blobs.
    pub async fn certify_and_complete_blobs<'a, T: Debug + Clone + Send + Sync>(
        &self,
        blobs_to_certify: Vec<WalrusStoreBlob<'a, T>>,
    ) -> ClientResult<Vec<WalrusStoreBlob<'a, T>>> {
        use walrus_sui::client::ArgumentOrOwnedObject;

        use crate::client::responses::BlobStoreResult;

        if blobs_to_certify.is_empty() {
            return Ok(vec![]);
        }

        // Extract blob_ids, deletable flags, operations and certificates from blobs
        let mut blob_info: Vec<_> = Vec::new();
        let mut certs_with_blob_ids: Vec<_> = Vec::new();

        for blob in &blobs_to_certify {
            if let WalrusStoreBlob::WithCertificate(inner) = blob {
                if let StoreOp::RegisteredInBlobManager {
                    blob_id,
                    deletable,
                    operation,
                } = &inner.operation
                {
                    blob_info.push((
                        *blob_id,
                        *deletable,
                        operation.clone(),
                    ));
                    certs_with_blob_ids.push((*blob_id, *deletable, &inner.certificate));
                }
            }
        }

        if blob_info.is_empty() {
            return Ok(vec![]);
        }

        tracing::info!(
            "BlobManager certify_and_complete_blobs: manager_id={:?}, num_blobs={}",
            self.manager_id,
            blob_info.len()
        );

        // Certify all blobs in a single batch transaction
        self.client
            .sui_client()
            .certify_blobs_in_blobmanager(
                self.manager_id,
                ArgumentOrOwnedObject::Object(self.manager_cap),
                &certs_with_blob_ids,
            )
            .await?;

        tracing::info!(
            "Successfully certified {} blobs in BlobManager",
            certs_with_blob_ids.len()
        );

        // Get price computation and current epoch for cost/end_epoch calculation
        let price_computation = self.client.get_price_computation().await?;
        let current_epoch = self.client.get_committees().await?.epoch();

        // Complete the blobs by creating BlobStoreResult::ManagedByBlobManager
        let mut completed_blobs = Vec::new();
        for (blob, (blob_id, deletable, operation)) in
            blobs_to_certify.into_iter().zip(blob_info.iter())
        {
            // Calculate cost from the operation
            let cost = price_computation.operation_cost(operation);

            // Calculate end_epoch from epochs_ahead in the operation
            let end_epoch = match operation {
                RegisterBlobOp::RegisterFromScratch { epochs_ahead, .. } => {
                    current_epoch + *epochs_ahead
                }
                RegisterBlobOp::ReuseStorage { .. } | RegisterBlobOp::ReuseRegistration { .. } => {
                    // For reuse operations, we don't extend epochs, so end_epoch stays the same
                    // But we don't have the original blob's end_epoch here
                    // Use current_epoch as a fallback (this is conservative)
                    current_epoch
                }
                RegisterBlobOp::ReuseAndExtend {
                    epochs_extended, ..
                }
                | RegisterBlobOp::ReuseAndExtendNonCertified {
                    epochs_extended, ..
                } => {
                    // Extend from current epoch
                    current_epoch + *epochs_extended
                }
            };

            // Note: blob_object_id is not available without querying BlobManager
            // We use ObjectID::ZERO as a placeholder since it's not needed for certification
            // If needed later, we can query it via get_blob_object_ids(blob_id)
            let result = BlobStoreResult::ManagedByBlobManager {
                blob_id: *blob_id,
                blob_object_id: ObjectID::ZERO, // Not available without extra RPC call
                resource_operation: operation.clone(),
                cost,
                end_epoch,
            };

            let completed = blob.complete_with(result);
            completed_blobs.push(completed);
        }

        Ok(completed_blobs)
    }
}

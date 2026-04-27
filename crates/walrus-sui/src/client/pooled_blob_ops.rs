// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::types::move_structs::{PooledBlob, StoragePoolInnerV1};

/// Read-only view of a storage pool's current state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoragePoolStatus {
    /// The object ID of the storage pool.
    pub storage_pool_object_id: ObjectID,
    /// The start epoch of the pool.
    pub start_epoch: Epoch,
    /// The end epoch of the pool.
    pub end_epoch: Epoch,
    /// The total reserved encoded capacity in bytes.
    pub reserved_encoded_capacity_bytes: u64,
    /// The currently used encoded capacity in bytes.
    pub used_encoded_bytes: u64,
    /// The number of pooled blobs currently registered in the pool.
    pub blob_count: u64,
}

impl StoragePoolStatus {
    /// Returns the remaining encoded capacity in bytes.
    pub fn available_encoded_capacity_bytes(&self) -> u64 {
        self.reserved_encoded_capacity_bytes - self.used_encoded_bytes
    }
}

impl SuiContractClient {
    /// Creates a new storage pool and returns the created object ID.
    pub async fn create_storage_pool(
        &self,
        reserved_encoded_capacity_bytes: u64,
        epochs_ahead: EpochCount,
    ) -> SuiClientResult<ObjectID> {
        self.retry_on_wrong_version(|| async {
            self.inner
                .lock()
                .await
                .create_storage_pool(reserved_encoded_capacity_bytes, epochs_ahead)
                .await
        })
        .await
    }

    /// Registers pooled blobs in the specified storage pool and returns the created pooled blobs.
    pub async fn register_pooled_blobs(
        &self,
        storage_pool_object_id: ObjectID,
        blob_metadata_list: Vec<BlobObjectMetadata>,
        persistence: BlobPersistence,
    ) -> SuiClientResult<Vec<PooledBlob>> {
        self.retry_on_wrong_version(|| async {
            self.inner
                .lock()
                .await
                .register_pooled_blobs(
                    storage_pool_object_id,
                    blob_metadata_list.clone(),
                    persistence,
                )
                .await
        })
        .await
    }

    /// Certifies pooled blobs in the specified storage pool.
    pub async fn certify_pooled_blobs(
        &self,
        storage_pool_object_id: ObjectID,
        pooled_blobs_with_certificates: &[(&PooledBlob, ConfirmationCertificate)],
    ) -> SuiClientResult<()> {
        self.retry_on_wrong_version(|| async {
            self.inner
                .lock()
                .await
                .certify_pooled_blobs(storage_pool_object_id, pooled_blobs_with_certificates)
                .await
        })
        .await
    }

    /// Deletes a pooled blob from the specified storage pool.
    pub async fn delete_pooled_blob(
        &self,
        storage_pool_object_id: ObjectID,
        blob_id: BlobId,
    ) -> SuiClientResult<()> {
        self.retry_on_wrong_version(|| async {
            self.inner
                .lock()
                .await
                .delete_pooled_blob(storage_pool_object_id, blob_id)
                .await
        })
        .await
    }

    /// Extends the lifetime of a storage pool.
    pub async fn extend_storage_pool(
        &self,
        storage_pool_object_id: ObjectID,
        epochs_extended: EpochCount,
    ) -> SuiClientResult<()> {
        self.retry_on_wrong_version(|| async {
            self.inner
                .lock()
                .await
                .extend_storage_pool(storage_pool_object_id, epochs_extended)
                .await
        })
        .await
    }

    /// Increases the reserved encoded capacity of a storage pool.
    pub async fn increase_storage_pool_capacity(
        &self,
        storage_pool_object_id: ObjectID,
        additional_encoded_capacity_bytes: u64,
    ) -> SuiClientResult<()> {
        self.retry_on_wrong_version(|| async {
            self.inner
                .lock()
                .await
                .increase_storage_pool_capacity(
                    storage_pool_object_id,
                    additional_encoded_capacity_bytes,
                )
                .await
        })
        .await
    }

    /// Returns the current state of the storage pool.
    pub async fn storage_pool_status(
        &self,
        storage_pool_object_id: ObjectID,
    ) -> SuiClientResult<StoragePoolStatus> {
        let StoragePoolInnerV1 {
            storage,
            used_encoded_bytes,
            blob_count,
            blobs: _,
        } = self
            .read_client()
            .get_storage_pool_inner(storage_pool_object_id)
            .await?;
        Ok(StoragePoolStatus {
            storage_pool_object_id,
            start_epoch: storage.start_epoch,
            end_epoch: storage.end_epoch,
            reserved_encoded_capacity_bytes: storage.storage_size,
            used_encoded_bytes,
            blob_count,
        })
    }
}

impl SuiContractClientInner {
    /// Creates a new storage pool.
    pub async fn create_storage_pool(
        &mut self,
        reserved_encoded_capacity_bytes: u64,
        epochs_ahead: EpochCount,
    ) -> SuiClientResult<ObjectID> {
        let mut pt_builder = self.transaction_builder();
        pt_builder
            .create_storage_pool(reserved_encoded_capacity_bytes, epochs_ahead)
            .await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        let res = self
            .sign_and_send_transaction(transaction, "create_storage_pool")
            .await?;

        let storage_pool_ids = get_created_sui_object_ids_by_type(
            &res,
            &contracts::storage_pool::StoragePool
                .to_move_struct_tag_with_type_map(&self.read_client.type_origin_map(), &[])?,
        )?;
        ensure!(
            storage_pool_ids.len() == 1,
            "unexpected number of StoragePool objects created: {}",
            storage_pool_ids.len()
        );
        Ok(storage_pool_ids[0])
    }

    /// Registers pooled blobs in the specified storage pool.
    pub async fn register_pooled_blobs(
        &mut self,
        storage_pool_object_id: ObjectID,
        blob_metadata_list: Vec<BlobObjectMetadata>,
        persistence: BlobPersistence,
    ) -> SuiClientResult<Vec<PooledBlob>> {
        if blob_metadata_list.is_empty() {
            tracing::debug!("no pooled blobs to register");
            return Ok(vec![]);
        }

        let expected_num_blobs = blob_metadata_list.len();
        let mut pt_builder = self.transaction_builder();
        pt_builder
            .register_pooled_blobs(storage_pool_object_id, blob_metadata_list, persistence)
            .await?;

        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        let res = self
            .sign_and_send_transaction(transaction, "register_pooled_blobs")
            .await?;
        let pooled_blob_object_ids = get_created_sui_object_ids_by_type(
            &res,
            &contracts::storage_pool::PooledBlob
                .to_move_struct_tag_with_type_map(&self.read_client.type_origin_map(), &[])?,
        )?;

        ensure!(
            pooled_blob_object_ids.len() == expected_num_blobs,
            "unexpected number of pooled blob objects created: {} expected {}",
            pooled_blob_object_ids.len(),
            expected_num_blobs
        );

        self.retriable_sui_client()
            .get_sui_objects(&pooled_blob_object_ids)
            .await
    }

    /// Certifies pooled blobs in the specified storage pool.
    pub async fn certify_pooled_blobs(
        &mut self,
        storage_pool_object_id: ObjectID,
        pooled_blobs_with_certificates: &[(&PooledBlob, ConfirmationCertificate)],
    ) -> SuiClientResult<()> {
        if pooled_blobs_with_certificates.is_empty() {
            return Ok(());
        }

        let mut pt_builder = self.transaction_builder();
        pt_builder
            .certify_pooled_blobs(storage_pool_object_id, pooled_blobs_with_certificates)
            .await?;

        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        self.sign_and_send_transaction(transaction, "certify_pooled_blobs")
            .await?;

        Ok(())
    }

    /// Deletes a pooled blob from the specified storage pool.
    pub async fn delete_pooled_blob(
        &mut self,
        storage_pool_object_id: ObjectID,
        blob_id: BlobId,
    ) -> SuiClientResult<()> {
        let mut pt_builder = self.transaction_builder();
        pt_builder
            .delete_pooled_blob(storage_pool_object_id, blob_id)
            .await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        self.sign_and_send_transaction(transaction, "delete_pooled_blob")
            .await?;
        Ok(())
    }

    /// Extends the lifetime of the specified storage pool.
    pub async fn extend_storage_pool(
        &mut self,
        storage_pool_object_id: ObjectID,
        epochs_extended: EpochCount,
    ) -> SuiClientResult<()> {
        let StoragePoolInnerV1 { storage, .. } = self
            .read_client
            .get_storage_pool_inner(storage_pool_object_id)
            .await?;

        let mut pt_builder = self.transaction_builder();
        pt_builder
            .extend_storage_pool(
                storage_pool_object_id,
                epochs_extended,
                storage.storage_size,
            )
            .await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        self.sign_and_send_transaction(transaction, "extend_storage_pool")
            .await?;
        Ok(())
    }

    /// Increases the reserved encoded capacity of the specified storage pool.
    pub async fn increase_storage_pool_capacity(
        &mut self,
        storage_pool_object_id: ObjectID,
        additional_encoded_capacity_bytes: u64,
    ) -> SuiClientResult<()> {
        let storage_pool_inner = self
            .read_client
            .get_storage_pool_inner(storage_pool_object_id)
            .await?;
        let current_epoch = self.read_client.current_epoch().await?;
        ensure!(
            storage_pool_inner.storage.end_epoch > current_epoch,
            anyhow!("storage pool is not active").into()
        );
        let remaining_epochs = storage_pool_inner.storage.end_epoch - current_epoch;

        let mut pt_builder = self.transaction_builder();
        pt_builder
            .increase_storage_pool_capacity(
                storage_pool_object_id,
                additional_encoded_capacity_bytes,
                remaining_epochs,
            )
            .await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        self.sign_and_send_transaction(transaction, "increase_storage_pool_capacity")
            .await?;
        Ok(())
    }
}

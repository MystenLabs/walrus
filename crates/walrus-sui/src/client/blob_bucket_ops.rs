// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::types::{
    move_errors::BlobBucketError,
    move_structs::{PooledBlob, StoragePoolInnerV1},
};

/// Handle for a shared blob bucket and its capability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlobBucketHandle {
    /// The object ID of the shared blob bucket.
    pub bucket_object_id: ObjectID,
    /// The object ID of the corresponding bucket capability.
    pub cap_object_id: ObjectID,
    /// The object ID of the storage pool backing the bucket.
    pub storage_pool_id: ObjectID,
}

impl SuiContractClient {
    /// Creates a shared blob bucket and returns its object ID, cap ID, and backing storage pool ID.
    pub async fn create_blob_bucket(
        &self,
        blob_bucket_package_id: ObjectID,
        reserved_encoded_capacity_bytes: u64,
        epochs_ahead: EpochCount,
    ) -> SuiClientResult<BlobBucketHandle> {
        self.inner
            .lock()
            .await
            .create_blob_bucket(
                blob_bucket_package_id,
                reserved_encoded_capacity_bytes,
                epochs_ahead,
            )
            .await
    }

    /// Registers blobs in the specified blob bucket and returns the created pooled blob objects.
    pub async fn register_blobs_in_bucket(
        &self,
        blob_bucket_object_id: ObjectID,
        blob_bucket_cap_object_id: ObjectID,
        blob_metadata_list: Vec<BlobObjectMetadata>,
        persistence: BlobPersistence,
    ) -> SuiClientResult<Vec<PooledBlob>> {
        let blob_bucket_package_id = self
            .read_client()
            .blob_bucket_package_id(blob_bucket_object_id)
            .await?;
        match self
            .inner
            .lock()
            .await
            .register_blobs_in_bucket(
                blob_bucket_package_id,
                blob_bucket_object_id,
                blob_bucket_cap_object_id,
                blob_metadata_list.clone(),
                persistence,
            )
            .await
        {
            Err(
                error @ SuiClientError::TransactionExecutionError(MoveExecutionError::BlobBucket(
                    BlobBucketError::EWrongVersion(_),
                )),
            ) => {
                let refreshed_package_id = self
                    .read_client()
                    .blob_bucket_package_id(blob_bucket_object_id)
                    .await?;
                if refreshed_package_id != blob_bucket_package_id {
                    self.inner
                        .lock()
                        .await
                        .register_blobs_in_bucket(
                            refreshed_package_id,
                            blob_bucket_object_id,
                            blob_bucket_cap_object_id,
                            blob_metadata_list,
                            persistence,
                        )
                        .await
                } else {
                    Err(error)
                }
            }
            result => result,
        }
    }

    /// Certifies pooled blobs in the specified blob bucket.
    pub async fn certify_blobs_in_bucket(
        &self,
        blob_bucket_object_id: ObjectID,
        pooled_blobs_with_certificates: &[(&PooledBlob, ConfirmationCertificate)],
    ) -> SuiClientResult<()> {
        let blob_bucket_package_id = self
            .read_client()
            .blob_bucket_package_id(blob_bucket_object_id)
            .await?;
        match self
            .inner
            .lock()
            .await
            .certify_blobs_in_bucket(
                blob_bucket_package_id,
                blob_bucket_object_id,
                pooled_blobs_with_certificates,
            )
            .await
        {
            Err(
                error @ SuiClientError::TransactionExecutionError(MoveExecutionError::BlobBucket(
                    BlobBucketError::EWrongVersion(_),
                )),
            ) => {
                let refreshed_package_id = self
                    .read_client()
                    .blob_bucket_package_id(blob_bucket_object_id)
                    .await?;
                if refreshed_package_id != blob_bucket_package_id {
                    self.inner
                        .lock()
                        .await
                        .certify_blobs_in_bucket(
                            refreshed_package_id,
                            blob_bucket_object_id,
                            pooled_blobs_with_certificates,
                        )
                        .await
                } else {
                    Err(error)
                }
            }
            result => result,
        }
    }

    /// Deletes a deletable pooled blob from the specified bucket.
    pub async fn delete_blob_from_bucket(
        &self,
        blob_bucket_object_id: ObjectID,
        blob_bucket_cap_object_id: ObjectID,
        blob_id: BlobId,
    ) -> SuiClientResult<()> {
        let blob_bucket_package_id = self
            .read_client()
            .blob_bucket_package_id(blob_bucket_object_id)
            .await?;
        match self
            .inner
            .lock()
            .await
            .delete_blob_from_bucket(
                blob_bucket_package_id,
                blob_bucket_object_id,
                blob_bucket_cap_object_id,
                blob_id,
            )
            .await
        {
            Err(
                error @ SuiClientError::TransactionExecutionError(MoveExecutionError::BlobBucket(
                    BlobBucketError::EWrongVersion(_),
                )),
            ) => {
                let refreshed_package_id = self
                    .read_client()
                    .blob_bucket_package_id(blob_bucket_object_id)
                    .await?;
                if refreshed_package_id != blob_bucket_package_id {
                    self.inner
                        .lock()
                        .await
                        .delete_blob_from_bucket(
                            refreshed_package_id,
                            blob_bucket_object_id,
                            blob_bucket_cap_object_id,
                            blob_id,
                        )
                        .await
                } else {
                    Err(error)
                }
            }
            result => result,
        }
    }

    /// Extends the lifetime of the storage pool backing the specified blob bucket.
    pub async fn extend_blob_bucket_storage_pool(
        &self,
        blob_bucket_object_id: ObjectID,
        blob_bucket_cap_object_id: ObjectID,
        epochs_extended: EpochCount,
    ) -> SuiClientResult<()> {
        let blob_bucket_package_id = self
            .read_client()
            .blob_bucket_package_id(blob_bucket_object_id)
            .await?;
        match self
            .inner
            .lock()
            .await
            .extend_blob_bucket_storage_pool(
                blob_bucket_package_id,
                blob_bucket_object_id,
                blob_bucket_cap_object_id,
                epochs_extended,
            )
            .await
        {
            Err(
                error @ SuiClientError::TransactionExecutionError(MoveExecutionError::BlobBucket(
                    BlobBucketError::EWrongVersion(_),
                )),
            ) => {
                let refreshed_package_id = self
                    .read_client()
                    .blob_bucket_package_id(blob_bucket_object_id)
                    .await?;
                if refreshed_package_id != blob_bucket_package_id {
                    self.inner
                        .lock()
                        .await
                        .extend_blob_bucket_storage_pool(
                            refreshed_package_id,
                            blob_bucket_object_id,
                            blob_bucket_cap_object_id,
                            epochs_extended,
                        )
                        .await
                } else {
                    Err(error)
                }
            }
            result => result,
        }
    }

    /// Increases the reserved encoded capacity of the storage pool backing the specified bucket.
    pub async fn increase_blob_bucket_storage_pool_capacity(
        &self,
        blob_bucket_object_id: ObjectID,
        blob_bucket_cap_object_id: ObjectID,
        additional_encoded_capacity_bytes: u64,
    ) -> SuiClientResult<()> {
        let blob_bucket_package_id = self
            .read_client()
            .blob_bucket_package_id(blob_bucket_object_id)
            .await?;
        match self
            .inner
            .lock()
            .await
            .increase_blob_bucket_storage_pool_capacity(
                blob_bucket_package_id,
                blob_bucket_object_id,
                blob_bucket_cap_object_id,
                additional_encoded_capacity_bytes,
            )
            .await
        {
            Err(
                error @ SuiClientError::TransactionExecutionError(MoveExecutionError::BlobBucket(
                    BlobBucketError::EWrongVersion(_),
                )),
            ) => {
                let refreshed_package_id = self
                    .read_client()
                    .blob_bucket_package_id(blob_bucket_object_id)
                    .await?;
                if refreshed_package_id != blob_bucket_package_id {
                    self.inner
                        .lock()
                        .await
                        .increase_blob_bucket_storage_pool_capacity(
                            refreshed_package_id,
                            blob_bucket_object_id,
                            blob_bucket_cap_object_id,
                            additional_encoded_capacity_bytes,
                        )
                        .await
                } else {
                    Err(error)
                }
            }
            result => result,
        }
    }
}

impl SuiContractClientInner {
    /// Creates a new shared blob bucket.
    pub async fn create_blob_bucket(
        &mut self,
        blob_bucket_package_id: ObjectID,
        reserved_encoded_capacity_bytes: u64,
        epochs_ahead: EpochCount,
    ) -> SuiClientResult<BlobBucketHandle> {
        let mut pt_builder = self.transaction_builder();
        pt_builder
            .create_blob_bucket(
                blob_bucket_package_id,
                reserved_encoded_capacity_bytes,
                epochs_ahead,
            )
            .await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        let res = self
            .sign_and_send_transaction(transaction, "create_blob_bucket")
            .await?;

        let bucket_ids = get_created_sui_object_ids_by_type(
            &res,
            &contracts::blob_bucket::BlobBucket
                .to_move_struct_tag_with_package(blob_bucket_package_id, &[])?,
        )?;
        let cap_ids = get_created_sui_object_ids_by_type(
            &res,
            &contracts::blob_bucket::BlobBucketCap
                .to_move_struct_tag_with_package(blob_bucket_package_id, &[])?,
        )?;

        ensure!(
            bucket_ids.len() == 1,
            "unexpected number of BlobBucket objects created: {}",
            bucket_ids.len()
        );
        ensure!(
            cap_ids.len() == 1,
            "unexpected number of BlobBucketCap objects created: {}",
            cap_ids.len()
        );

        let storage_pool_id = self
            .read_client
            .get_blob_bucket_storage_pool_id(bucket_ids[0])
            .await?;
        Ok(BlobBucketHandle {
            bucket_object_id: bucket_ids[0],
            cap_object_id: cap_ids[0],
            storage_pool_id,
        })
    }

    /// Registers blobs in the specified blob bucket.
    pub async fn register_blobs_in_bucket(
        &mut self,
        blob_bucket_package_id: ObjectID,
        blob_bucket_object_id: ObjectID,
        blob_bucket_cap_object_id: ObjectID,
        blob_metadata_list: Vec<BlobObjectMetadata>,
        persistence: BlobPersistence,
    ) -> SuiClientResult<Vec<PooledBlob>> {
        if blob_metadata_list.is_empty() {
            tracing::debug!("no blobs to register in blob bucket");
            return Ok(vec![]);
        }

        let expected_num_blobs = blob_metadata_list.len();
        let mut pt_builder = self.transaction_builder();
        for blob_metadata in blob_metadata_list {
            pt_builder
                .register_blob_in_bucket(
                    blob_bucket_package_id,
                    blob_bucket_object_id,
                    blob_bucket_cap_object_id.into(),
                    blob_metadata,
                    persistence,
                )
                .await?;
        }

        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        let res = self
            .sign_and_send_transaction(transaction, "register_blobs_in_bucket")
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

    /// Certifies pooled blobs in the specified blob bucket.
    pub async fn certify_blobs_in_bucket(
        &mut self,
        blob_bucket_package_id: ObjectID,
        blob_bucket_object_id: ObjectID,
        pooled_blobs_with_certificates: &[(&PooledBlob, ConfirmationCertificate)],
    ) -> SuiClientResult<()> {
        if pooled_blobs_with_certificates.is_empty() {
            return Ok(());
        }

        let mut pt_builder = self.transaction_builder();
        for (pooled_blob, certificate) in pooled_blobs_with_certificates {
            pt_builder
                .certify_blob_in_bucket(
                    blob_bucket_package_id,
                    blob_bucket_object_id,
                    pooled_blob.blob_id,
                    certificate,
                )
                .await?;
        }

        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        let res = self
            .sign_and_send_transaction(transaction, "certify_blobs_in_bucket")
            .await?;

        if !res.errors.is_empty() {
            tracing::warn!(errors = ?res.errors, "failed to certify pooled blobs on Sui");
            return Err(anyhow!("could not certify pooled blobs: {:?}", res.errors).into());
        }

        Ok(())
    }

    /// Deletes a pooled blob from the specified bucket.
    pub async fn delete_blob_from_bucket(
        &mut self,
        blob_bucket_package_id: ObjectID,
        blob_bucket_object_id: ObjectID,
        blob_bucket_cap_object_id: ObjectID,
        blob_id: BlobId,
    ) -> SuiClientResult<()> {
        let mut pt_builder = self.transaction_builder();
        pt_builder
            .delete_blob_from_bucket(
                blob_bucket_package_id,
                blob_bucket_object_id,
                blob_bucket_cap_object_id.into(),
                blob_id,
            )
            .await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        self.sign_and_send_transaction(transaction, "delete_blob_from_bucket")
            .await?;
        Ok(())
    }

    /// Extends the storage pool backing the specified bucket.
    pub async fn extend_blob_bucket_storage_pool(
        &mut self,
        blob_bucket_package_id: ObjectID,
        blob_bucket_object_id: ObjectID,
        blob_bucket_cap_object_id: ObjectID,
        epochs_extended: EpochCount,
    ) -> SuiClientResult<()> {
        let StoragePoolInnerV1 {
            reserved_encoded_capacity_bytes,
            ..
        } = self
            .read_client
            .get_blob_bucket_storage_pool_inner(blob_bucket_object_id)
            .await?;
        let mut pt_builder = self.transaction_builder();
        pt_builder
            .extend_blob_bucket_storage_pool(
                blob_bucket_package_id,
                blob_bucket_object_id,
                blob_bucket_cap_object_id.into(),
                epochs_extended,
                reserved_encoded_capacity_bytes,
            )
            .await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        self.sign_and_send_transaction(transaction, "extend_blob_bucket_storage_pool")
            .await?;
        Ok(())
    }

    /// Increases the reserved encoded capacity of the storage pool backing the specified bucket.
    pub async fn increase_blob_bucket_storage_pool_capacity(
        &mut self,
        blob_bucket_package_id: ObjectID,
        blob_bucket_object_id: ObjectID,
        blob_bucket_cap_object_id: ObjectID,
        additional_encoded_capacity_bytes: u64,
    ) -> SuiClientResult<()> {
        let storage_pool_inner = self
            .read_client
            .get_blob_bucket_storage_pool_inner(blob_bucket_object_id)
            .await?;
        let current_epoch = self.read_client.current_epoch().await?;
        ensure!(
            storage_pool_inner.end_epoch > current_epoch,
            anyhow!("blob bucket storage pool is not active").into()
        );
        let remaining_epochs = storage_pool_inner.end_epoch - current_epoch;

        let mut pt_builder = self.transaction_builder();
        pt_builder
            .increase_blob_bucket_storage_pool_capacity(
                blob_bucket_package_id,
                blob_bucket_object_id,
                blob_bucket_cap_object_id.into(),
                additional_encoded_capacity_bytes,
                remaining_epochs,
            )
            .await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        self.sign_and_send_transaction(transaction, "increase_blob_bucket_storage_pool_capacity")
            .await?;
        Ok(())
    }
}

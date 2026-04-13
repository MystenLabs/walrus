// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use walrus_core::BlobId;

use super::*;
use crate::types::PooledBlob;

impl WalrusPtbBuilder {
    /// Adds a call to `system::create_storage_pool` and returns the created storage pool argument.
    pub async fn create_storage_pool(
        &mut self,
        reserved_encoded_capacity_bytes: u64,
        epochs_ahead: EpochCount,
    ) -> SuiClientResult<Argument> {
        let price = self
            .storage_price_for_encoded_length(reserved_encoded_capacity_bytes, epochs_ahead, false)
            .await?;
        self.fill_wal_balance(price).await?;

        let args = vec![
            self.system_arg(SharedObjectMutability::Mutable)?,
            self.pt_builder.pure(reserved_encoded_capacity_bytes)?,
            self.pt_builder.pure(epochs_ahead)?,
            self.wal_coin_arg()?,
        ];
        let result_arg = self.walrus_move_call(contracts::system::create_storage_pool, args)?;
        self.reduce_wal_balance(price)?;
        self.add_result_to_be_consumed(result_arg);
        Ok(result_arg)
    }

    /// Adds calls to `system::register_pooled_blob` for the provided metadata list.
    pub async fn register_pooled_blobs(
        &mut self,
        storage_pool_object_id: ObjectID,
        blob_metadata_list: Vec<BlobObjectMetadata>,
        persistence: BlobPersistence,
    ) -> SuiClientResult<()> {
        if blob_metadata_list.is_empty() {
            return Ok(());
        }

        let mut total_price = 0;
        for blob_metadata in &blob_metadata_list {
            total_price += self
                .write_price_for_encoded_length(blob_metadata.encoded_size, false)
                .await?;
        }
        self.fill_wal_balance(total_price).await?;

        let storage_pool_arg = self
            .argument_from_arg_or_obj(storage_pool_object_id.into())
            .await?;
        for blob_metadata in blob_metadata_list {
            let args = vec![
                self.system_arg(SharedObjectMutability::Mutable)?,
                storage_pool_arg,
                self.pt_builder.pure(blob_metadata.blob_id)?,
                self.pt_builder.pure(blob_metadata.root_hash.bytes())?,
                self.pt_builder.pure(blob_metadata.unencoded_size)?,
                self.pt_builder
                    .pure(u8::from(blob_metadata.encoding_type))?,
                self.pt_builder.pure(persistence.is_deletable())?,
                self.wal_coin_arg()?,
            ];
            self.walrus_move_call(contracts::system::register_pooled_blob, args)?;
        }
        self.reduce_wal_balance(total_price)?;
        Ok(())
    }

    /// Adds calls to `system::certify_pooled_blob` for the provided pooled blobs.
    pub async fn certify_pooled_blobs(
        &mut self,
        storage_pool_object_id: ObjectID,
        pooled_blobs_with_certificates: &[(&PooledBlob, ConfirmationCertificate)],
    ) -> SuiClientResult<()> {
        if pooled_blobs_with_certificates.is_empty() {
            return Ok(());
        }

        let storage_pool_arg = self
            .argument_from_arg_or_obj(storage_pool_object_id.into())
            .await?;
        for (pooled_blob, certificate) in pooled_blobs_with_certificates {
            let signers = self.signers_to_bitmap(&certificate.signers).await?;
            let args = vec![
                self.system_arg(SharedObjectMutability::Immutable)?,
                storage_pool_arg,
                self.pt_builder.pure(pooled_blob.blob_id)?,
                self.pt_builder.pure(certificate.signature.as_bytes())?,
                self.pt_builder.pure(&signers)?,
                self.pt_builder.pure(&certificate.serialized_message)?,
            ];
            self.walrus_move_call(contracts::system::certify_pooled_blob, args)?;
        }
        Ok(())
    }

    /// Adds a call to `system::delete_pooled_blob`.
    pub async fn delete_pooled_blob(
        &mut self,
        storage_pool_object_id: ObjectID,
        blob_id: BlobId,
    ) -> SuiClientResult<()> {
        let storage_pool_arg = self
            .argument_from_arg_or_obj(storage_pool_object_id.into())
            .await?;
        let args = vec![
            self.system_arg(SharedObjectMutability::Immutable)?,
            storage_pool_arg,
            self.pt_builder.pure(blob_id)?,
        ];
        self.walrus_move_call(contracts::system::delete_pooled_blob, args)?;
        Ok(())
    }

    /// Adds a call to `system::extend_storage_pool`.
    pub async fn extend_storage_pool(
        &mut self,
        storage_pool_object_id: ObjectID,
        epochs_extended: EpochCount,
        reserved_encoded_capacity_bytes: u64,
    ) -> SuiClientResult<()> {
        let price = self
            .storage_price_for_encoded_length(
                reserved_encoded_capacity_bytes,
                epochs_extended,
                false,
            )
            .await?;
        self.fill_wal_balance(price).await?;

        let storage_pool_arg = self
            .argument_from_arg_or_obj(storage_pool_object_id.into())
            .await?;
        let args = vec![
            self.system_arg(SharedObjectMutability::Mutable)?,
            storage_pool_arg,
            self.pt_builder.pure(epochs_extended)?,
            self.wal_coin_arg()?,
        ];
        self.walrus_move_call(contracts::system::extend_storage_pool, args)?;
        self.reduce_wal_balance(price)?;
        Ok(())
    }

    /// Adds a call to `system::increase_storage_pool_capacity`.
    pub async fn increase_storage_pool_capacity(
        &mut self,
        storage_pool_object_id: ObjectID,
        additional_encoded_capacity_bytes: u64,
        remaining_epochs: EpochCount,
    ) -> SuiClientResult<()> {
        let price = self
            .storage_price_for_encoded_length(
                additional_encoded_capacity_bytes,
                remaining_epochs,
                false,
            )
            .await?;
        self.fill_wal_balance(price).await?;

        let storage_pool_arg = self
            .argument_from_arg_or_obj(storage_pool_object_id.into())
            .await?;
        let args = vec![
            self.system_arg(SharedObjectMutability::Mutable)?,
            storage_pool_arg,
            self.pt_builder.pure(additional_encoded_capacity_bytes)?,
            self.wal_coin_arg()?,
        ];
        self.walrus_move_call(contracts::system::increase_storage_pool_capacity, args)?;
        self.reduce_wal_balance(price)?;
        Ok(())
    }
}

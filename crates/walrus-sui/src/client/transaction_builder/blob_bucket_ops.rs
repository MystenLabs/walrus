// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use walrus_core::BlobId;

use super::*;

impl WalrusPtbBuilder {
    /// Adds a call to `blob_bucket::new` and returns the created capability argument.
    pub async fn create_blob_bucket(
        &mut self,
        blob_bucket_package_id: ObjectID,
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
        let result_arg =
            self.move_call(blob_bucket_package_id, contracts::blob_bucket::new, args)?;
        self.reduce_wal_balance(price)?;
        self.add_result_to_be_consumed(result_arg);
        Ok(result_arg)
    }

    /// Adds a call to `blob_bucket::register_blob`.
    pub async fn register_blob_in_bucket(
        &mut self,
        blob_bucket_package_id: ObjectID,
        blob_bucket_object_id: ObjectID,
        blob_bucket_cap: ArgumentOrOwnedObject,
        blob_metadata: BlobObjectMetadata,
        persistence: BlobPersistence,
    ) -> SuiClientResult<()> {
        let price = self
            .write_price_for_encoded_length(blob_metadata.encoded_size, false)
            .await?;
        self.fill_wal_balance(price).await?;

        let args = vec![
            self.pt_builder.obj(
                self.read_client
                    .object_arg_for_shared_obj(
                        blob_bucket_object_id,
                        SharedObjectMutability::Mutable,
                    )
                    .await?,
            )?,
            self.argument_from_arg_or_obj(blob_bucket_cap).await?,
            self.system_arg(SharedObjectMutability::Mutable)?,
            self.pt_builder.pure(blob_metadata.blob_id)?,
            self.pt_builder.pure(blob_metadata.root_hash.bytes())?,
            self.pt_builder.pure(blob_metadata.unencoded_size)?,
            self.pt_builder
                .pure(u8::from(blob_metadata.encoding_type))?,
            self.pt_builder.pure(persistence.is_deletable())?,
            self.wal_coin_arg()?,
        ];
        self.move_call(
            blob_bucket_package_id,
            contracts::blob_bucket::register_blob,
            args,
        )?;
        self.reduce_wal_balance(price)?;
        Ok(())
    }

    /// Adds a call to `blob_bucket::certify_blob`.
    pub async fn certify_blob_in_bucket(
        &mut self,
        blob_bucket_package_id: ObjectID,
        blob_bucket_object_id: ObjectID,
        blob_id: BlobId,
        certificate: &ConfirmationCertificate,
    ) -> SuiClientResult<()> {
        let signers = self.signers_to_bitmap(&certificate.signers).await?;
        let args = vec![
            self.pt_builder.obj(
                self.read_client
                    .object_arg_for_shared_obj(
                        blob_bucket_object_id,
                        SharedObjectMutability::Mutable,
                    )
                    .await?,
            )?,
            self.system_arg(SharedObjectMutability::Immutable)?,
            self.pt_builder.pure(blob_id)?,
            self.pt_builder.pure(certificate.signature.as_bytes())?,
            self.pt_builder.pure(&signers)?,
            self.pt_builder.pure(&certificate.serialized_message)?,
        ];
        self.move_call(
            blob_bucket_package_id,
            contracts::blob_bucket::certify_blob,
            args,
        )?;
        Ok(())
    }

    /// Adds a call to `blob_bucket::delete_blob`.
    pub async fn delete_blob_from_bucket(
        &mut self,
        blob_bucket_package_id: ObjectID,
        blob_bucket_object_id: ObjectID,
        blob_bucket_cap: ArgumentOrOwnedObject,
        blob_id: BlobId,
    ) -> SuiClientResult<()> {
        let args = vec![
            self.pt_builder.obj(
                self.read_client
                    .object_arg_for_shared_obj(
                        blob_bucket_object_id,
                        SharedObjectMutability::Mutable,
                    )
                    .await?,
            )?,
            self.argument_from_arg_or_obj(blob_bucket_cap).await?,
            self.system_arg(SharedObjectMutability::Immutable)?,
            self.pt_builder.pure(blob_id)?,
        ];
        self.move_call(
            blob_bucket_package_id,
            contracts::blob_bucket::delete_blob,
            args,
        )?;
        Ok(())
    }

    /// Adds a call to `blob_bucket::extend_storage_pool`.
    pub async fn extend_blob_bucket_storage_pool(
        &mut self,
        blob_bucket_package_id: ObjectID,
        blob_bucket_object_id: ObjectID,
        blob_bucket_cap: ArgumentOrOwnedObject,
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

        let args = vec![
            self.pt_builder.obj(
                self.read_client
                    .object_arg_for_shared_obj(
                        blob_bucket_object_id,
                        SharedObjectMutability::Mutable,
                    )
                    .await?,
            )?,
            self.argument_from_arg_or_obj(blob_bucket_cap).await?,
            self.system_arg(SharedObjectMutability::Mutable)?,
            self.pt_builder.pure(epochs_extended)?,
            self.wal_coin_arg()?,
        ];
        self.move_call(
            blob_bucket_package_id,
            contracts::blob_bucket::extend_storage_pool,
            args,
        )?;
        self.reduce_wal_balance(price)?;
        Ok(())
    }

    /// Adds a call to `blob_bucket::increase_storage_pool_capacity`.
    pub async fn increase_blob_bucket_storage_pool_capacity(
        &mut self,
        blob_bucket_package_id: ObjectID,
        blob_bucket_object_id: ObjectID,
        blob_bucket_cap: ArgumentOrOwnedObject,
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

        let args = vec![
            self.pt_builder.obj(
                self.read_client
                    .object_arg_for_shared_obj(
                        blob_bucket_object_id,
                        SharedObjectMutability::Mutable,
                    )
                    .await?,
            )?,
            self.argument_from_arg_or_obj(blob_bucket_cap).await?,
            self.system_arg(SharedObjectMutability::Mutable)?,
            self.pt_builder.pure(additional_encoded_capacity_bytes)?,
            self.wal_coin_arg()?,
        ];
        self.move_call(
            blob_bucket_package_id,
            contracts::blob_bucket::increase_storage_pool_capacity,
            args,
        )?;
        self.reduce_wal_balance(price)?;
        Ok(())
    }
}

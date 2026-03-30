// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use super::*;

impl WalrusPtbBuilder {
    /// Adds a call to `reserve_space` to the `pt_builder` and returns the result [`Argument`].
    pub async fn reserve_space(
        &mut self,
        encoded_size: u64,
        epochs_ahead: EpochCount,
    ) -> SuiClientResult<Argument> {
        let price = self
            .storage_price_for_encoded_length(encoded_size, epochs_ahead, false)
            .await?;
        self.fill_wal_balance(price).await?;

        let reserve_arguments = vec![
            self.system_arg(SharedObjectMutability::Mutable)?,
            self.pt_builder.pure(encoded_size)?,
            self.pt_builder.pure(epochs_ahead)?,
            self.wal_coin_arg()?,
        ];
        let result_arg =
            self.walrus_move_call(contracts::system::reserve_space, reserve_arguments)?;
        self.reduce_wal_balance(price)?;
        self.add_result_to_be_consumed(result_arg);
        Ok(result_arg)
    }

    /// Adds a call to `reserve_space` to the `pt_builder` and returns the result [`Argument`].
    pub async fn reserve_space_with_credits(
        &mut self,
        encoded_size: u64,
        epochs_ahead: EpochCount,
    ) -> SuiClientResult<Argument> {
        let price = self
            .storage_price_for_encoded_length(encoded_size, epochs_ahead, true)
            .await?;
        self.fill_wal_balance(price).await?;

        let reserve_arguments = vec![
            self.credits_arg(SharedObjectMutability::Mutable)?,
            self.system_arg(SharedObjectMutability::Mutable)?,
            self.pt_builder.pure(encoded_size)?,
            self.pt_builder.pure(epochs_ahead)?,
            self.wal_coin_arg()?,
        ];
        let Some(credits_package_id) = self.read_client.credits_package_id() else {
            return Err(SuiClientError::CreditsNotEnabled);
        };
        let result_arg = self.move_call(
            credits_package_id,
            contracts::credits::reserve_space,
            reserve_arguments,
        )?;
        self.reduce_wal_balance(price)?;
        self.add_result_to_be_consumed(result_arg);
        Ok(result_arg)
    }

    /// Adds a call to `storage_resource::split_by_size` to the `pt_builder` and returns
    /// the result [`Argument`].
    ///
    /// The call modifies the input argument to cover `split_size` and a new object covering
    /// the initial size minus `split_size` is created.
    pub async fn split_storage_by_size(
        &mut self,
        storage_resource: ArgumentOrOwnedObject,
        split_size: u64,
    ) -> SuiClientResult<Argument> {
        let split_arguments = vec![
            self.argument_from_arg_or_obj(storage_resource).await?,
            self.pt_builder.pure(split_size)?,
        ];
        let result_arg =
            self.walrus_move_call(contracts::storage_resource::split_by_size, split_arguments)?;
        self.add_result_to_be_consumed(result_arg);
        Ok(result_arg)
    }

    /// Adds a call to `register_blob` to the `pt_builder` and returns the result [`Argument`].
    pub async fn register_blob(
        &mut self,
        storage_resource: ArgumentOrOwnedObject,
        blob_metadata: BlobObjectMetadata,
        persistence: BlobPersistence,
    ) -> SuiClientResult<Argument> {
        let price = self
            .write_price_for_encoded_length(blob_metadata.encoded_size, false)
            .await?;
        self.fill_wal_balance(price).await?;

        let storage_resource_arg = self.argument_from_arg_or_obj(storage_resource).await?;

        let register_arguments = vec![
            self.system_arg(SharedObjectMutability::Mutable)?,
            storage_resource_arg,
            self.pt_builder.pure(blob_metadata.blob_id)?,
            self.pt_builder.pure(blob_metadata.root_hash.bytes())?,
            self.pt_builder.pure(blob_metadata.unencoded_size)?,
            self.pt_builder
                .pure(u8::from(blob_metadata.encoding_type))?,
            self.pt_builder.pure(persistence.is_deletable())?,
            self.wal_coin_arg()?,
        ];
        let result_arg =
            self.walrus_move_call(contracts::system::register_blob, register_arguments)?;
        self.reduce_wal_balance(price)?;
        self.mark_arg_as_consumed(&storage_resource_arg);
        self.add_result_to_be_consumed(result_arg);
        Ok(result_arg)
    }

    /// Adds a call to `register_blob` to the `pt_builder` and returns the result [`Argument`].
    /// with credits.
    pub async fn register_blob_with_credits(
        &mut self,
        storage_resource: ArgumentOrOwnedObject,
        blob_metadata: BlobObjectMetadata,
        persistence: BlobPersistence,
    ) -> SuiClientResult<Argument> {
        let Some(credits_package_id) = self.read_client.credits_package_id() else {
            return Err(SuiClientError::CreditsNotEnabled);
        };
        let price = self
            .write_price_for_encoded_length(blob_metadata.encoded_size, true)
            .await?;
        self.fill_wal_balance(price).await?;

        let storage_resource_arg = self.argument_from_arg_or_obj(storage_resource).await?;

        let register_arguments = vec![
            self.credits_arg(SharedObjectMutability::Mutable)?,
            self.system_arg(SharedObjectMutability::Mutable)?,
            storage_resource_arg,
            self.pt_builder.pure(blob_metadata.blob_id)?,
            self.pt_builder.pure(blob_metadata.root_hash.bytes())?,
            self.pt_builder.pure(blob_metadata.unencoded_size)?,
            self.pt_builder
                .pure(u8::from(blob_metadata.encoding_type))?,
            self.pt_builder.pure(persistence.is_deletable())?,
            self.wal_coin_arg()?,
        ];
        let result_arg = self.move_call(
            credits_package_id,
            contracts::credits::register_blob,
            register_arguments,
        )?;

        self.reduce_wal_balance(price)?;
        self.mark_arg_as_consumed(&storage_resource_arg);
        self.add_result_to_be_consumed(result_arg);
        Ok(result_arg)
    }

    /// Adds a call to `certify_blob` to the `pt_builder`.
    #[tracing::instrument(skip(self, certificate), level = Level::DEBUG)]
    pub async fn certify_blob(
        &mut self,
        blob_object: ArgumentOrOwnedObject,
        certificate: &ConfirmationCertificate,
    ) -> SuiClientResult<()> {
        let signers = self.signers_to_bitmap(&certificate.signers).await?;

        let certify_args = vec![
            self.system_arg(SharedObjectMutability::Immutable)?,
            self.argument_from_arg_or_obj(blob_object).await?,
            self.pt_builder.pure(certificate.signature.as_bytes())?,
            self.pt_builder.pure(&signers)?,
            self.pt_builder.pure(&certificate.serialized_message)?,
        ];
        self.walrus_move_call(contracts::system::certify_blob, certify_args)?;
        Ok(())
    }

    // TODO(WAL-514): simplify and remove rpc call
    pub(super) async fn signers_to_bitmap(&self, signers: &[u16]) -> SuiClientResult<Vec<u8>> {
        let committee_size = self.system_object().await?.committee_size() as usize;
        let mut bitmap = vec![0; committee_size.div_ceil(8)];
        for signer in signers {
            let byte_index = signer / 8;
            let bit_index = signer % 8;
            bitmap[byte_index as usize] |= 1 << bit_index;
        }
        Ok(bitmap)
    }

    /// Adds a call to `certify_event_blob` to the `pt_builder`.
    pub async fn certify_event_blob(
        &mut self,
        blob_metadata: BlobObjectMetadata,
        storage_node_cap: ArgumentOrOwnedObject,
        ending_checkpoint_seq_num: u64,
        epoch: u32,
    ) -> SuiClientResult<()> {
        let arguments = vec![
            self.system_arg(SharedObjectMutability::Mutable)?,
            self.argument_from_arg_or_obj(storage_node_cap).await?,
            self.pt_builder.pure(blob_metadata.blob_id)?,
            self.pt_builder.pure(blob_metadata.root_hash.bytes())?,
            self.pt_builder.pure(blob_metadata.unencoded_size)?,
            self.pt_builder
                .pure(u8::from(blob_metadata.encoding_type))?,
            self.pt_builder.pure(ending_checkpoint_seq_num)?,
            self.pt_builder.pure(epoch)?,
        ];
        self.walrus_move_call(contracts::system::certify_event_blob, arguments)?;
        Ok(())
    }

    /// Adds a call to `delete_blob` to the `pt_builder` and returns the result [`Argument`].
    pub async fn delete_blob(
        &mut self,
        blob_object: ArgumentOrOwnedObject,
    ) -> SuiClientResult<Argument> {
        let blob_arg = self.argument_from_arg_or_obj(blob_object).await?;
        let delete_arguments = vec![self.system_arg(SharedObjectMutability::Mutable)?, blob_arg];
        let result_arg = self.walrus_move_call(contracts::system::delete_blob, delete_arguments)?;
        self.mark_arg_as_consumed(&blob_arg);
        self.add_result_to_be_consumed(result_arg);
        Ok(result_arg)
    }

    /// Adds a call to `burn` the blob to the `pt_builder`.
    pub async fn burn_blob(&mut self, blob_object: ArgumentOrOwnedObject) -> SuiClientResult<()> {
        let blob_arg = self.argument_from_arg_or_obj(blob_object).await?;
        self.walrus_move_call(contracts::blob::burn, vec![blob_arg])?;
        self.mark_arg_as_consumed(&blob_arg);
        Ok(())
    }

    /// Adds a call to create a new instance of Metadata and returns the result [`Argument`].
    pub fn new_metadata(&mut self) -> SuiClientResult<Argument> {
        let result_arg = self.walrus_move_call(contracts::metadata::new, vec![])?;
        self.add_result_to_be_consumed(result_arg);
        Ok(result_arg)
    }

    /// Adds a call to insert or update a key-value pair in a Metadata object.
    pub async fn insert_or_update_blob_attribute(
        &mut self,
        blob_attribute: ArgumentOrOwnedObject,
        key: String,
        value: String,
    ) -> SuiClientResult<()> {
        let metadata_arg = self.argument_from_arg_or_obj(blob_attribute).await?;
        let key_arg = self.pt_builder.pure(key)?;
        let value_arg = self.pt_builder.pure(value)?;
        self.walrus_move_call(
            contracts::metadata::insert_or_update,
            vec![metadata_arg, key_arg, value_arg],
        )?;
        Ok(())
    }

    /// Adds a call to add metadata to a blob.
    pub async fn add_blob_attribute(
        &mut self,
        blob_object: ArgumentOrOwnedObject,
        blob_attribute: BlobAttribute,
    ) -> SuiClientResult<()> {
        let metadata_arg = self.new_metadata()?;

        for (key, value) in blob_attribute.iter() {
            self.insert_or_update_blob_attribute(metadata_arg.into(), key.clone(), value.clone())
                .await?;
        }
        let blob_arg = self.argument_from_arg_or_obj(blob_object).await?;
        self.walrus_move_call(contracts::blob::add_metadata, vec![blob_arg, metadata_arg])?;
        self.mark_arg_as_consumed(&metadata_arg);
        Ok(())
    }

    /// Adds a call to remove metadata dynamic field from a blob and returns the
    /// result [`Argument`].
    ///
    /// Note the [`BlobAttribute`] corresponds to the `metadata::Metadata` in the contract.
    pub async fn remove_blob_attribute(
        &mut self,
        blob_object: ArgumentOrOwnedObject,
    ) -> SuiClientResult<Argument> {
        let blob_arg = self.argument_from_arg_or_obj(blob_object).await?;
        let result_arg = self.walrus_move_call(contracts::blob::take_metadata, vec![blob_arg])?;
        Ok(result_arg)
    }

    /// Adds calls to insert or update multiple metadata key-value pairs in a blob.
    pub async fn insert_or_update_blob_attribute_pairs<I, T>(
        &mut self,
        blob_object: ArgumentOrOwnedObject,
        pairs: I,
    ) -> SuiClientResult<()>
    where
        I: IntoIterator<Item = (T, T)>,
        T: Into<String>,
    {
        let pairs: Vec<_> = pairs.into_iter().collect();
        if pairs.is_empty() {
            return Ok(());
        }

        let blob_arg = self.argument_from_arg_or_obj(blob_object).await?;

        for (key, value) in pairs {
            let key_arg = self.pt_builder.pure(key.into())?;
            let value_arg = self.pt_builder.pure(value.into())?;
            self.walrus_move_call(
                contracts::blob::insert_or_update_metadata_pair,
                vec![blob_arg, key_arg, value_arg],
            )?;
        }
        Ok(())
    }

    /// Adds calls to remove multiple metadata key-value pairs from a blob.
    pub async fn remove_blob_attribute_pairs<I, K>(
        &mut self,
        blob_object: ArgumentOrOwnedObject,
        keys: I,
    ) -> SuiClientResult<()>
    where
        I: IntoIterator<Item = K>,
        K: AsRef<str>,
    {
        let blob_arg = self.argument_from_arg_or_obj(blob_object).await?;

        for key in keys {
            let key_arg = self.pt_builder.pure(key.as_ref().to_string())?;
            self.walrus_move_call(
                contracts::blob::remove_metadata_pair,
                vec![blob_arg, key_arg],
            )?;
        }
        Ok(())
    }

    /// Adds a call to extend an owned blob without credits.
    pub async fn extend_blob(
        &mut self,
        blob_object: ArgumentOrOwnedObject,
        epochs_extended: EpochCount,
        encoded_size: u64,
    ) -> SuiClientResult<()> {
        let price = self
            .storage_price_for_encoded_length(encoded_size, epochs_extended, false)
            .await?;

        self.fill_wal_balance(price).await?;

        let args = vec![
            self.system_arg(SharedObjectMutability::Mutable)?,
            self.argument_from_arg_or_obj(blob_object).await?,
            self.pt_builder.pure(epochs_extended)?,
            self.wal_coin_arg()?,
        ];
        self.walrus_move_call(contracts::system::extend_blob, args)?;
        self.reduce_wal_balance(price)?;
        Ok(())
    }

    /// Adds a call to extend an owned blob with credits.
    pub async fn extend_blob_with_credits(
        &mut self,
        blob_object: ArgumentOrOwnedObject,
        epochs_ahead: EpochCount,
        encoded_size: u64,
    ) -> SuiClientResult<()> {
        let Some(credits_package_id) = self.read_client.credits_package_id() else {
            return Err(SuiClientError::CreditsNotEnabled);
        };
        let price = self
            .storage_price_for_encoded_length(encoded_size, epochs_ahead, true)
            .await?;

        self.fill_wal_balance(price).await?;

        let args = vec![
            self.credits_arg(SharedObjectMutability::Mutable)?,
            self.system_arg(SharedObjectMutability::Mutable)?,
            self.argument_from_arg_or_obj(blob_object).await?,
            self.pt_builder.pure(epochs_ahead)?,
            self.wal_coin_arg()?,
        ];
        self.move_call(credits_package_id, contracts::credits::extend_blob, args)?;
        self.reduce_wal_balance(price)?;
        Ok(())
    }
}

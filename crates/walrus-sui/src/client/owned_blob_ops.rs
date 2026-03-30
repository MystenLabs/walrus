// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use super::*;

impl SuiContractClient {
    /// Purchases blob storage for the next `epochs_ahead` Walrus epochs and an encoded
    /// size of `encoded_size` and returns the created storage resource.
    pub async fn reserve_space(
        &self,
        encoded_size: u64,
        epochs_ahead: EpochCount,
    ) -> SuiClientResult<StorageResource> {
        self.retry_on_wrong_version(|| async {
            self.inner
                .lock()
                .await
                .reserve_space(encoded_size, epochs_ahead)
                .await
        })
        .await
    }

    /// Registers blobs with the specified [`BlobObjectMetadata`] and [`StorageResource`]s,
    /// and returns the created blob objects.
    pub async fn register_blobs(
        &self,
        blob_metadata_and_storage: Vec<(BlobObjectMetadata, StorageResource)>,
        persistence: BlobPersistence,
    ) -> SuiClientResult<Vec<Blob>> {
        self.retry_on_wrong_version(|| async {
            self.inner
                .lock()
                .await
                .register_blobs(blob_metadata_and_storage.clone(), persistence)
                .await
        })
        .await
    }

    /// Purchases blob storage for the next `epochs_ahead` Walrus epochs and uses the resulting
    /// storage resource to register a blob with the provided `blob_metadata`.
    ///
    /// This combines the [`reserve_space`][Self::reserve_space] and
    /// [`register_blobs`][Self::register_blobs] functions in one atomic transaction.
    pub async fn reserve_and_register_blobs(
        &self,
        epochs_ahead: EpochCount,
        blob_metadata_list: Vec<BlobObjectMetadata>,
        persistence: BlobPersistence,
    ) -> SuiClientResult<Vec<Blob>> {
        self.retry_on_wrong_version(|| async {
            self.inner
                .lock()
                .await
                .reserve_and_register_blobs(epochs_ahead, blob_metadata_list.clone(), persistence)
                .await
        })
        .await
    }

    /// Certifies the specified blob on Sui, given a certificate that confirms its storage and
    /// returns the certified blob.
    ///
    /// If the post store action is `share`, returns a mapping blob ID -> shared_blob_object_id.
    pub async fn certify_blobs(
        &self,
        blobs_with_certificates: &[(&BlobWithAttribute, ConfirmationCertificate)],
        post_store: PostStoreAction,
    ) -> SuiClientResult<HashMap<BlobId, ObjectID>> {
        self.retry_on_wrong_version(|| async {
            self.inner
                .lock()
                .await
                .certify_blobs(blobs_with_certificates, post_store)
                .await
        })
        .await
    }

    /// Certifies the specified event blob on Sui, with the given metadata and epoch.
    pub async fn certify_event_blob(
        &self,
        blob_metadata: BlobObjectMetadata,
        ending_checkpoint_seq_num: u64,
        epoch: u32,
        node_capability_object_id: ObjectID,
    ) -> SuiClientResult<()> {
        self.retry_on_wrong_version(|| async {
            self.inner
                .lock()
                .await
                .certify_event_blob(
                    blob_metadata.clone(),
                    ending_checkpoint_seq_num,
                    epoch,
                    node_capability_object_id,
                )
                .await
        })
        .await
    }

    /// Invalidates the specified blob id on Sui, given a certificate that confirms that it is
    /// invalid.
    pub async fn invalidate_blob_id(
        &self,
        certificate: &InvalidBlobCertificate,
    ) -> SuiClientResult<()> {
        self.retry_on_wrong_version(|| async {
            self.inner
                .lock()
                .await
                .invalidate_blob_id(certificate)
                .await
        })
        .await
    }

    /// Deletes the specified blob from the wallet's storage.
    pub async fn delete_blob(&self, blob_object_id: ObjectID) -> SuiClientResult<()> {
        self.retry_on_wrong_version(|| async {
            self.inner.lock().await.delete_blob(blob_object_id).await
        })
        .await
    }

    /// Burns the blob objects with the given object IDs.
    ///
    /// May use multiple PTBs in sequence to burn all the given object IDs.
    pub async fn burn_blobs(&self, blob_object_ids: &[ObjectID]) -> SuiClientResult<()> {
        self.retry_on_wrong_version(|| async {
            self.inner.lock().await.burn_blobs(blob_object_ids).await
        })
        .await
    }

    /// Extends the owned blob object by `epochs_extended` epochs.
    pub async fn extend_blob(
        &self,
        blob_obj_id: ObjectID,
        epochs_extended: EpochCount,
    ) -> SuiClientResult<()> {
        self.retry_on_wrong_version(|| async {
            self.inner
                .lock()
                .await
                .extend_blob(blob_obj_id, epochs_extended)
                .await
        })
        .await
    }

    /// Adds attribute to a blob object.
    ///
    /// If attribute does not exist, it is created with the given key-value pairs.
    /// If attribute already exists, an error is returned unless `force` is true.
    /// If `force` is true, the attribute is updated with the given key-value pairs.
    pub async fn add_blob_attribute(
        &mut self,
        blob_obj_id: ObjectID,
        blob_attribute: BlobAttribute,
        force: bool,
    ) -> SuiClientResult<()> {
        self.retry_on_wrong_version(|| async {
            let mut inner = self.inner.lock().await;
            match inner.add_blob_attribute(blob_obj_id, &blob_attribute).await {
                Ok(()) => Ok(()),
                Err(SuiClientError::TransactionExecutionError(MoveExecutionError::Blob(
                    BlobError::EDuplicateMetadata(_),
                ))) => {
                    if force {
                        inner
                            .insert_or_update_blob_attribute_pairs(
                                blob_obj_id,
                                blob_attribute.iter(),
                            )
                            .await
                    } else {
                        Err(SuiClientError::AttributeAlreadyExists)
                    }
                }
                Err(e) => Err(e),
            }
        })
        .await
    }

    /// Removes the attribute dynamic field from a blob object.
    ///
    /// If attribute does not exist, an error is returned.
    pub async fn remove_blob_attribute(&mut self, blob_obj_id: ObjectID) -> SuiClientResult<()> {
        self.retry_on_wrong_version(|| async {
            match self
                .inner
                .lock()
                .await
                .remove_blob_attribute(blob_obj_id)
                .await
            {
                Err(SuiClientError::TransactionExecutionError(MoveExecutionError::Blob(
                    BlobError::EMissingMetadata(_),
                ))) => Err(SuiClientError::AttributeDoesNotExist),
                result => result,
            }
        })
        .await
    }

    /// Inserts or updates key-value pairs in the blob's attribute.
    ///
    /// If the attribute does not exist and `force` is true, it will be created.
    /// If the attribute does not exist and `force` is false, an error is returned.
    /// If the attribute exists, the key-value pairs will be updated.
    pub async fn insert_or_update_blob_attribute_pairs<I, T>(
        &mut self,
        blob_obj_id: ObjectID,
        pairs: I,
        force: bool,
    ) -> SuiClientResult<()>
    where
        I: IntoIterator<Item = (T, T)>,
        T: Into<String>,
    {
        let mut inner = self.inner.lock().await;
        let pairs_clone: Vec<(String, String)> = pairs
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();

        let attribute_exists = inner
            .read_client
            .get_blob_attribute(&blob_obj_id)
            .await?
            .is_some();

        if !attribute_exists {
            if !force {
                return Err(SuiClientError::AttributeDoesNotExist);
            }
            inner
                .add_blob_attribute(blob_obj_id, &BlobAttribute::from(pairs_clone))
                .await
        } else {
            inner
                .insert_or_update_blob_attribute_pairs(blob_obj_id, pairs_clone)
                .await
        }
    }

    /// Removes key-value pairs from the blob's attribute.
    ///
    /// If any key does not exist, an error is returned.
    pub async fn remove_blob_attribute_pairs<I, T>(
        &mut self,
        blob_obj_id: ObjectID,
        keys: I,
    ) -> SuiClientResult<()>
    where
        I: IntoIterator<Item = T>,
        T: AsRef<str>,
    {
        match self
            .inner
            .lock()
            .await
            .remove_blob_attribute_pairs(blob_obj_id, keys)
            .await
        {
            Err(SuiClientError::TransactionExecutionError(MoveExecutionError::Blob(
                BlobError::EMissingMetadata(_),
            ))) => Err(SuiClientError::AttributeDoesNotExist),
            result => result,
        }
    }

    /// Certifies and extends the specified blob on Sui in a single transaction.
    ///
    /// Returns the shared blob object ID if the post store action is Share.
    /// See [`CertifyAndExtendBlobParams`] for the details of the parameters.
    #[tracing::instrument(level = Level::DEBUG, skip_all)]
    pub async fn certify_and_extend_blobs(
        &self,
        certify_and_extend_parameters: &[CertifyAndExtendBlobParams<'_>],
        post_store: PostStoreAction,
    ) -> SuiClientResult<Vec<CertifyAndExtendBlobResult>> {
        self.retry_on_wrong_version(|| async {
            self.inner
                .lock()
                .await
                .certify_and_extend_blobs(certify_and_extend_parameters, post_store)
                .await
        })
        .await
    }
}

impl SuiContractClientInner {
    /// Adds attribute to a blob object.
    pub async fn add_blob_attribute(
        &mut self,
        blob_obj_id: ObjectID,
        blob_attribute: &BlobAttribute,
    ) -> SuiClientResult<()> {
        let mut pt_builder = self.transaction_builder();
        pt_builder
            .add_blob_attribute(blob_obj_id.into(), blob_attribute.clone())
            .await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        self.sign_and_send_transaction(transaction, "add_blob_attribute")
            .await?;
        Ok(())
    }

    /// Removes the attribute dynamic field from a blob object.
    pub async fn remove_blob_attribute(&mut self, blob_obj_id: ObjectID) -> SuiClientResult<()> {
        let mut pt_builder = self.transaction_builder();
        pt_builder.remove_blob_attribute(blob_obj_id.into()).await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        self.sign_and_send_transaction(transaction, "remove_blob_attribute")
            .await?;
        Ok(())
    }

    /// Inserts or updates a key-value pair in the blob's attribute.
    pub async fn insert_or_update_blob_attribute_pairs<I, T>(
        &mut self,
        blob_obj_id: ObjectID,
        pairs: I,
    ) -> SuiClientResult<()>
    where
        I: IntoIterator<Item = (T, T)>,
        T: Into<String>,
    {
        let mut pt_builder = self.transaction_builder();
        pt_builder
            .insert_or_update_blob_attribute_pairs(blob_obj_id.into(), pairs)
            .await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        self.sign_and_send_transaction(transaction, "insert_or_update_blob_attribute_pairs")
            .await?;
        Ok(())
    }

    /// Removes key-value pairs from the blob's attribute.
    pub async fn remove_blob_attribute_pairs<I, T>(
        &mut self,
        blob_obj_id: ObjectID,
        keys: I,
    ) -> SuiClientResult<()>
    where
        I: IntoIterator<Item = T>,
        T: AsRef<str>,
    {
        let mut pt_builder = self.transaction_builder();
        pt_builder
            .remove_blob_attribute_pairs(blob_obj_id.into(), keys)
            .await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        self.sign_and_send_transaction(transaction, "remove_blob_attribute_pairs")
            .await?;
        Ok(())
    }

    /// Purchases blob storage for the next `epochs_ahead` Walrus epochs and an encoded
    /// size of `encoded_size` and returns the created storage resource.
    pub async fn reserve_space(
        &mut self,
        encoded_size: u64,
        epochs_ahead: EpochCount,
    ) -> SuiClientResult<StorageResource> {
        if self.read_client.credits_object_id().is_some() {
            match self
                .reserve_space_with_credits(encoded_size, epochs_ahead)
                .await
            {
                Ok(arg) => return Ok(arg),
                Err(SuiClientError::TransactionExecutionError(MoveExecutionError::System(
                    SystemError::EWrongVersion(_),
                ))) => {
                    tracing::warn!(
                        "Walrus package version mismatch in credits call,
                            falling back to direct contract call"
                    );
                }
                Err(e) => return Err(e),
            }
        }
        self.reserve_space_without_credits(encoded_size, epochs_ahead)
            .await
    }

    /// Purchases blob storage for the next `epochs_ahead` Walrus epochs and an encoded
    /// size of `encoded_size` and returns the created storage resource.
    async fn reserve_space_with_credits(
        &mut self,
        encoded_size: u64,
        epochs_ahead: EpochCount,
    ) -> SuiClientResult<StorageResource> {
        let mut pt_builder = self.transaction_builder();
        pt_builder
            .reserve_space_with_credits(encoded_size, epochs_ahead)
            .await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        let res = self
            .sign_and_send_transaction(transaction, "reserve_space_with_credits")
            .await?;
        let storage_id = get_created_sui_object_ids_by_type(
            &res,
            &contracts::storage_resource::Storage
                .to_move_struct_tag_with_type_map(&self.read_client.type_origin_map(), &[])?,
        )?;

        ensure!(
            storage_id.len() == 1,
            "unexpected number of storage resources created: {}",
            storage_id.len()
        );

        self.retriable_sui_client()
            .get_sui_object(storage_id[0])
            .await
    }

    /// Registers a blob with the specified [`BlobId`] using the provided [`StorageResource`],
    /// and returns the created blob object.
    async fn reserve_space_without_credits(
        &mut self,
        encoded_size: u64,
        epochs_ahead: EpochCount,
    ) -> SuiClientResult<StorageResource> {
        let mut pt_builder = self.transaction_builder();
        pt_builder.reserve_space(encoded_size, epochs_ahead).await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        let res = self
            .sign_and_send_transaction(transaction, "reserve_space_without_credits")
            .await?;
        let storage_id = get_created_sui_object_ids_by_type(
            &res,
            &contracts::storage_resource::Storage
                .to_move_struct_tag_with_type_map(&self.read_client.type_origin_map(), &[])?,
        )?;

        ensure!(
            storage_id.len() == 1,
            "unexpected number of storage resources created: {}",
            storage_id.len()
        );

        self.retriable_sui_client()
            .get_sui_object(storage_id[0])
            .await
    }

    /// Registers a blob with the specified [`BlobId`] using the provided [`StorageResource`],
    /// and returns the created blob object.
    ///
    /// `blob_size` is the size of the unencoded blob. The encoded size of the blob must be
    /// less than or equal to the size reserved in `storage`.
    #[tracing::instrument(level = Level::DEBUG, skip_all)]
    pub async fn register_blobs(
        &mut self,
        blob_metadata_and_storage: Vec<(BlobObjectMetadata, StorageResource)>,
        persistence: BlobPersistence,
    ) -> SuiClientResult<Vec<Blob>> {
        if blob_metadata_and_storage.is_empty() {
            tracing::debug!("no blobs to register");
            return Ok(vec![]);
        }

        let with_credits = self.read_client.credits_object_id().is_some();

        let expected_num_blobs = blob_metadata_and_storage.len();
        tracing::debug!(num_blobs = expected_num_blobs, "starting to register blobs");
        let mut pt_builder = self.transaction_builder();
        for (blob_metadata, storage) in blob_metadata_and_storage.into_iter() {
            if with_credits {
                pt_builder
                    .register_blob_with_credits(storage.id.into(), blob_metadata, persistence)
                    .await?;
            } else {
                pt_builder
                    .register_blob(storage.id.into(), blob_metadata, persistence)
                    .await?;
            };
        }
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        let res = self
            .sign_and_send_transaction(transaction, "register_blobs")
            .await?;
        let blob_obj_ids = get_created_sui_object_ids_by_type(
            &res,
            &contracts::blob::Blob
                .to_move_struct_tag_with_type_map(&self.read_client.type_origin_map(), &[])?,
        )?;
        ensure!(
            blob_obj_ids.len() == expected_num_blobs,
            "unexpected number of blob objects created: {} expected {} ",
            blob_obj_ids.len(),
            expected_num_blobs
        );

        self.retriable_sui_client()
            .get_sui_objects(&blob_obj_ids)
            .await
    }

    /// Purchases blob storage for the next `epochs_ahead` Walrus epochs and uses the resulting
    /// storage resource to register a blob with the provided `blob_metadata`.
    ///
    /// This combines the [`reserve_space`][Self::reserve_space] and
    /// [`register_blobs`][Self::register_blobs] functions in one atomic transaction.
    #[tracing::instrument(level = Level::DEBUG, skip_all)]
    pub async fn reserve_and_register_blobs(
        &mut self,
        epochs_ahead: EpochCount,
        blob_metadata_list: Vec<BlobObjectMetadata>,
        persistence: BlobPersistence,
    ) -> SuiClientResult<Vec<Blob>> {
        let with_credits = self.read_client.credits_object_id().is_some();
        if with_credits {
            match self
                .reserve_and_register_blobs_inner(
                    epochs_ahead,
                    blob_metadata_list.clone(),
                    persistence,
                    true,
                )
                .await
            {
                Ok(result) => return Ok(result),
                Err(SuiClientError::TransactionExecutionError(MoveExecutionError::System(
                    SystemError::EWrongVersion(_),
                ))) => {
                    tracing::warn!(
                        "Walrus package version mismatch in credits call, \
                            falling back to direct contract call"
                    );
                }
                Err(e) => return Err(e),
            }
        }
        self.reserve_and_register_blobs_inner(epochs_ahead, blob_metadata_list, persistence, false)
            .await
    }

    /// reserve and register blobs inner
    pub async fn reserve_and_register_blobs_inner(
        &mut self,
        epochs_ahead: EpochCount,
        blob_metadata_list: Vec<BlobObjectMetadata>,
        persistence: BlobPersistence,
        with_credits: bool,
    ) -> SuiClientResult<Vec<Blob>> {
        if blob_metadata_list.is_empty() {
            tracing::debug!("no blobs to register");
            return Ok(vec![]);
        }

        let expected_num_blobs = blob_metadata_list.len();
        tracing::debug!(
            num_blobs = expected_num_blobs,
            "starting to reserve and register blobs"
        );

        let mut pt_builder = self.transaction_builder();

        let mut main_storage_arg_size = blob_metadata_list
            .iter()
            .fold(0, |acc, metadata| acc + metadata.encoded_size);

        let main_storage_arg = if with_credits {
            pt_builder
                .reserve_space_with_credits(main_storage_arg_size, epochs_ahead)
                .await?
        } else {
            pt_builder
                .reserve_space(main_storage_arg_size, epochs_ahead)
                .await?
        };

        for blob_metadata in blob_metadata_list.into_iter() {
            let storage_arg = if main_storage_arg_size != blob_metadata.encoded_size {
                main_storage_arg_size -= blob_metadata.encoded_size;
                pt_builder
                    .split_storage_by_size(main_storage_arg.into(), main_storage_arg_size)
                    .await?
            } else {
                main_storage_arg
            };

            if with_credits {
                pt_builder
                    .register_blob_with_credits(storage_arg.into(), blob_metadata, persistence)
                    .await?
            } else {
                pt_builder
                    .register_blob(storage_arg.into(), blob_metadata, persistence)
                    .await?
            };
        }

        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        let res = self
            .sign_and_send_transaction(transaction, "reserve_and_register_blobs_inner")
            .await?;
        let blob_obj_ids = get_created_sui_object_ids_by_type(
            &res,
            &contracts::blob::Blob
                .to_move_struct_tag_with_type_map(&self.read_client.type_origin_map(), &[])?,
        )?;

        ensure!(
            blob_obj_ids.len() == expected_num_blobs,
            "unexpected number of blob objects created: {} expected {} ",
            blob_obj_ids.len(),
            expected_num_blobs
        );

        self.retriable_sui_client()
            .get_sui_objects(&blob_obj_ids)
            .await
    }

    /// Certifies the specified blob on Sui, given a certificate that confirms its storage and
    /// returns the certified blob.
    ///
    /// If the post store action is `share`, returns a mapping blob ID -> shared_blob_object_id.
    pub async fn certify_blobs(
        &mut self,
        blobs_with_certificates: &[(&BlobWithAttribute, ConfirmationCertificate)],
        post_store: PostStoreAction,
    ) -> SuiClientResult<HashMap<BlobId, ObjectID>> {
        let mut pt_builder = self.transaction_builder();
        for (i, (blob_with_attr, certificate)) in blobs_with_certificates.iter().enumerate() {
            let blob = &blob_with_attr.blob;
            tracing::debug!(
                blob_id = %blob.blob_id,
                count = format!("{}/{}", i + 1, blobs_with_certificates.len()),
                "certifying blob on Sui"
            );
            pt_builder.certify_blob(blob.id.into(), certificate).await?;

            if let Some(ref attribute) = blob_with_attr.attribute {
                pt_builder
                    .add_blob_attribute(blob.id.into(), attribute.clone())
                    .await?;
            }

            Self::apply_post_store_action(&mut pt_builder, blob.id, post_store).await?;
        }

        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        let res = self
            .sign_and_send_transaction(transaction, "certify_blobs")
            .await?;

        if !res.errors.is_empty() {
            tracing::warn!(errors = ?res.errors, "failed to certify blobs on Sui");
            return Err(anyhow!("could not certify blob: {:?}", res.errors).into());
        }

        if post_store != PostStoreAction::Share {
            return Ok(HashMap::new());
        }

        self.create_blob_id_to_shared_mapping(
            &res,
            blobs_with_certificates
                .iter()
                .map(|(blob_with_attr, _)| blob_with_attr.blob.blob_id)
                .collect::<Vec<_>>()
                .as_slice(),
        )
        .await
    }

    /// Certifies the specified event blob on Sui, with the given metadata and epoch.
    pub async fn certify_event_blob(
        &mut self,
        blob_metadata: BlobObjectMetadata,
        ending_checkpoint_seq_num: u64,
        epoch: u32,
        node_capability_object_id: ObjectID,
    ) -> SuiClientResult<()> {
        tracing::debug!(
            %node_capability_object_id,
            "calling certify_event_blob"
        );

        let mut pt_builder = self.transaction_builder();
        pt_builder
            .certify_event_blob(
                blob_metadata,
                node_capability_object_id.into(),
                ending_checkpoint_seq_num,
                epoch,
            )
            .await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        self.sign_and_send_transaction(transaction, "certify_event_blob")
            .await?;
        Ok(())
    }

    /// Invalidates the specified blob id on Sui, given a certificate that confirms that it is
    /// invalid.
    pub async fn invalidate_blob_id(
        &mut self,
        certificate: &InvalidBlobCertificate,
    ) -> SuiClientResult<()> {
        let mut pt_builder = self.transaction_builder();
        pt_builder.invalidate_blob_id(certificate).await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        self.sign_and_send_transaction(transaction, "invalidate_blob_id")
            .await?;
        Ok(())
    }

    /// Deletes the specified blob from the wallet's storage.
    pub async fn delete_blob(&mut self, blob_object_id: ObjectID) -> SuiClientResult<()> {
        let mut pt_builder = self.transaction_builder();
        pt_builder.delete_blob(blob_object_id.into()).await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        self.sign_and_send_transaction(transaction, "delete_blob")
            .await?;
        Ok(())
    }

    /// Burns the blob objects with the given object IDs.
    ///
    /// May use multiple PTBs in sequence to burn all the given object IDs.
    pub async fn burn_blobs(&mut self, blob_object_ids: &[ObjectID]) -> SuiClientResult<()> {
        tracing::debug!(n_blobs = blob_object_ids.len(), "burning blobs");

        for id_block in blob_object_ids.chunks(MAX_BURNS_PER_PTB) {
            let mut pt_builder = self.transaction_builder();
            for id in id_block {
                pt_builder.burn_blob(id.into()).await?;
            }
            let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
            self.sign_and_send_transaction(transaction, "burn_blobs")
                .await?;
        }

        Ok(())
    }

    /// Extends the owned blob object by `epochs_extended` epochs without credits.
    async fn extend_blob_without_credits(
        &mut self,
        blob_obj_id: ObjectID,
        epochs_extended: EpochCount,
    ) -> SuiClientResult<()> {
        let blob: Blob = self
            .read_client
            .retriable_sui_client()
            .get_sui_object(blob_obj_id)
            .await?;
        let mut pt_builder = self.transaction_builder();
        pt_builder
            .extend_blob(
                blob_obj_id.into(),
                epochs_extended,
                blob.storage.storage_size,
            )
            .await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        self.sign_and_send_transaction(transaction, "extend_blob_without_credits")
            .await?;
        Ok(())
    }

    /// Extends the owned blob object by `epochs_extended` epochs with credits.
    async fn extend_blob_with_credits(
        &mut self,
        blob_obj_id: ObjectID,
        epochs_extended: EpochCount,
    ) -> SuiClientResult<()> {
        let blob: Blob = self
            .read_client
            .retriable_sui_client()
            .get_sui_object(blob_obj_id)
            .await?;
        let mut pt_builder = self.transaction_builder();
        pt_builder
            .extend_blob_with_credits(
                blob_obj_id.into(),
                epochs_extended,
                blob.storage.storage_size,
            )
            .await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        self.sign_and_send_transaction(transaction, "extend_blob_with_credits")
            .await?;
        Ok(())
    }

    /// Extends the owned blob object by `epochs_extended` epochs.
    pub async fn extend_blob(
        &mut self,
        blob_obj_id: ObjectID,
        epochs_extended: EpochCount,
    ) -> SuiClientResult<()> {
        let with_credits = self.read_client.credits_package_id().is_some();
        if with_credits {
            match self
                .extend_blob_with_credits(blob_obj_id, epochs_extended)
                .await
            {
                Ok(_) => return Ok(()),
                Err(SuiClientError::TransactionExecutionError(MoveExecutionError::System(
                    SystemError::EWrongVersion(_),
                ))) => {
                    tracing::warn!(
                        "Walrus package version mismatch in credits call, \
                        call, falling back to direct contract call"
                    );
                }
                Err(e) => return Err(e),
            }
        }

        self.extend_blob_without_credits(blob_obj_id, epochs_extended)
            .await?;
        Ok(())
    }

    /// Certifies and extends the specified blob on Sui in a single transaction.
    /// Returns the shared blob object ID if the post store action is Share.
    pub async fn certify_and_extend_blobs(
        &mut self,
        certify_and_extend_parameters: &[CertifyAndExtendBlobParams<'_>],
        post_store: PostStoreAction,
    ) -> SuiClientResult<Vec<CertifyAndExtendBlobResult>> {
        let with_credits = self.read_client.credits_package_id().is_some();
        if with_credits {
            match self
                .certify_and_extend_blobs_inner(certify_and_extend_parameters, post_store, true)
                .await
            {
                Ok(result) => return Ok(result),
                Err(SuiClientError::TransactionExecutionError(
                    MoveExecutionError::Staking(StakingError::EWrongVersion(_))
                    | MoveExecutionError::System(SystemError::EWrongVersion(_)),
                )) => {
                    tracing::warn!(
                        "Walrus package version mismatch in credits call, \
                            falling back to direct contract call"
                    );
                }
                Err(e) => return Err(e),
            }
        }

        self.certify_and_extend_blobs_inner(certify_and_extend_parameters, post_store, false)
            .await
    }

    /// Common implementation for certifying and extending blobs with different extension strategies
    async fn certify_and_extend_blobs_inner<'b>(
        &mut self,
        certify_and_extend_parameters: &[CertifyAndExtendBlobParams<'b>],
        post_store: PostStoreAction,
        with_credits: bool,
    ) -> SuiClientResult<Vec<CertifyAndExtendBlobResult>> {
        if certify_and_extend_parameters.is_empty() {
            return Ok(vec![]);
        }

        let object_args = self
            .read_client
            .retriable_sui_client()
            .get_object_refs(
                &certify_and_extend_parameters
                    .iter()
                    .map(|blob_params| blob_params.blob.id)
                    .collect::<Vec<_>>(),
            )
            .await?
            .into_iter()
            .map(ObjectArg::ImmOrOwnedObject);

        let mut pt_builder = self.transaction_builder();

        for (blob_params, object_arg) in certify_and_extend_parameters
            .iter()
            .zip(object_args.into_iter())
        {
            let blob = blob_params.blob;

            if let Some(certificate) = blob_params.certificate.as_ref() {
                pt_builder
                    .certify_blob(object_arg.into(), certificate)
                    .await?;
            }

            pt_builder
                .insert_or_update_blob_attribute_pairs(object_arg.into(), blob_params.attribute)
                .await?;

            if let Some(epochs_extended) = blob_params.epochs_extended {
                if with_credits {
                    pt_builder
                        .extend_blob_with_credits(
                            object_arg.into(),
                            epochs_extended,
                            blob.storage.storage_size,
                        )
                        .await?;
                } else {
                    pt_builder
                        .extend_blob(
                            object_arg.into(),
                            epochs_extended,
                            blob.storage.storage_size,
                        )
                        .await?;
                }
            }

            SuiContractClientInner::apply_post_store_action(&mut pt_builder, blob.id, post_store)
                .await?;
        }

        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        let res = self
            .sign_and_send_transaction(transaction, "certify_and_extend_blobs")
            .await?;

        if !res.errors.is_empty() {
            tracing::warn!(errors = ?res.errors, "failed to certify/extend blobs on Sui");
            return Err(anyhow!("could not certify/extend blob: {:?}", res.errors).into());
        }

        let post_store_action_results = self
            .get_post_store_action_results(&res, certify_and_extend_parameters, &post_store)
            .await
            .map_err(|e| {
                tracing::warn!(error = ?e, "failed to get post store action results");
                anyhow!(
                    "Blobs have been stored, but could not get post store action results: {:?}",
                    e
                )
            })?;

        let results = certify_and_extend_parameters
            .iter()
            .zip(post_store_action_results.into_iter())
            .map(|(blob_params, r)| CertifyAndExtendBlobResult {
                blob_object_id: blob_params.blob.id,
                post_store_action_result: r,
            })
            .collect();

        Ok(results)
    }

    /// Helper function to create a mapping from blob IDs to shared blob object IDs.
    async fn get_post_store_action_results(
        &self,
        res: &SuiTransactionBlockResponse,
        certify_and_extend_parameters: &[CertifyAndExtendBlobParams<'_>],
        post_store: &PostStoreAction,
    ) -> SuiClientResult<Vec<PostStoreActionResult>> {
        if *post_store == PostStoreAction::Share {
            self.get_share_blob_result(certify_and_extend_parameters, res)
                .await
        } else {
            Ok(certify_and_extend_parameters
                .iter()
                .map(|_| PostStoreActionResult::new(post_store, None))
                .collect())
        }
    }
}

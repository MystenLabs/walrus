// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use indicatif::MultiProgress;
use sui_types::base_types::ObjectID;
use walrus_core::messages::BlobPersistenceType;
use walrus_sui::{
    client::{
        BlobBucketHandle,
        BlobBucketStoragePoolStatus,
        BlobObjectMetadata,
        BucketObjectState,
        BucketObjectVersionInput,
        PostStoreAction,
    },
    types::{ObjectHeaders, ObjectMetadata, ObjectTags, PooledBlob},
};

use super::{
    ActiveCommittees,
    ClientError,
    ClientErrorKind,
    EncodedBlob,
    PendingUploadContext,
    StoreArgs,
    SuiContractClient,
    UnencodedBlob,
    WalrusNodeClient,
    WalrusNodeClientCreatedInBackground,
    are_current_previous_different,
};
use crate::{error::ClientResult, node_client::StoreBlobsApi};

/// Headers, metadata, and tags for a new object version.
#[derive(Debug, Clone, Default)]
pub struct BucketObjectVersionAttrs {
    /// Standard object headers.
    pub headers: ObjectHeaders,
    /// Custom user metadata.
    pub metadata: ObjectMetadata,
    /// Object tags.
    pub tags: ObjectTags,
    /// Explicit content etag. Defaults to the blob ID string.
    pub content_etag: Option<String>,
    /// Explicit object etag. Defaults to a fresh random object ID string.
    pub object_etag: Option<String>,
}

/// Headers, metadata, and tags for an attribute-only update.
#[derive(Debug, Clone, Default)]
pub struct BucketObjectAttributeUpdate {
    /// Standard object headers.
    pub headers: ObjectHeaders,
    /// Custom user metadata.
    pub metadata: ObjectMetadata,
    /// Object tags.
    pub tags: ObjectTags,
    /// Explicit object etag. Defaults to a fresh random object ID string.
    pub object_etag: Option<String>,
}

impl WalrusNodeClient<SuiContractClient> {
    fn fresh_object_etag(&self) -> String {
        ObjectID::random().to_string()
    }

    fn object_blob_persistence_type(
        &self,
        pooled_blob_object_id: ObjectID,
        store_args: &StoreArgs,
    ) -> BlobPersistenceType {
        match store_args.persistence {
            walrus_sui::client::BlobPersistence::Permanent => BlobPersistenceType::Permanent,
            walrus_sui::client::BlobPersistence::Deletable => BlobPersistenceType::Deletable {
                object_id: pooled_blob_object_id.into(),
            },
        }
    }

    fn ensure_bucket_object_store_args(&self, store_args: &StoreArgs) -> ClientResult<()> {
        if store_args.post_store != PostStoreAction::Keep {
            return Err(ClientError::store_blob_internal(
                "bucket object store only supports PostStoreAction::Keep".to_string(),
            ));
        }
        Ok(())
    }

    fn encode_bucket_object_blob(
        &self,
        blob: Vec<u8>,
        store_args: &StoreArgs,
    ) -> ClientResult<EncodedBlob> {
        UnencodedBlob {
            unencoded_data: blob,
        }
        .encode(
            self.encoding_config.get_for_type(store_args.encoding_type),
            store_args.upload_relay_client.clone(),
        )
    }

    async fn ensure_bucket_storage_pool_ready_for_object(
        &self,
        blob_bucket: BlobBucketHandle,
        encoded_size: u64,
        committees: &ActiveCommittees,
        store_args: &StoreArgs,
    ) -> ClientResult<()> {
        let current_epoch = committees.write_committee().epoch;
        let target_end_epoch = current_epoch + store_args.epochs_ahead;

        let mut storage_pool_status = self
            .sui_client
            .blob_bucket_storage_pool_status(blob_bucket.bucket_object_id)
            .await?;
        self.ensure_bucket_storage_pool_active(storage_pool_status, current_epoch)?;

        if storage_pool_status.end_epoch < target_end_epoch {
            let epochs_extended = target_end_epoch - storage_pool_status.end_epoch;
            self.sui_client
                .extend_blob_bucket_storage_pool(
                    blob_bucket.bucket_object_id,
                    blob_bucket.cap_object_id,
                    epochs_extended,
                )
                .await?;
            storage_pool_status.end_epoch = target_end_epoch;
        }

        if storage_pool_status.available_encoded_capacity_bytes() < encoded_size {
            self.sui_client
                .increase_blob_bucket_storage_pool_capacity(
                    blob_bucket.bucket_object_id,
                    blob_bucket.cap_object_id,
                    encoded_size - storage_pool_status.available_encoded_capacity_bytes(),
                )
                .await?;
        }

        Ok(())
    }

    fn ensure_bucket_storage_pool_active(
        &self,
        storage_pool_status: BlobBucketStoragePoolStatus,
        current_epoch: walrus_core::Epoch,
    ) -> ClientResult<()> {
        if storage_pool_status.end_epoch <= current_epoch {
            return Err(ClientError::store_blob_internal(format!(
                "blob bucket storage pool {} is not active at epoch {}",
                storage_pool_status.storage_pool_id, current_epoch
            )));
        }
        Ok(())
    }

    async fn collect_bucket_object_certificate(
        &self,
        encoded_blob: &EncodedBlob,
        pooled_blob_object_id: ObjectID,
        store_args: &StoreArgs,
    ) -> ClientResult<walrus_core::messages::ConfirmationCertificate> {
        let persistence_type = self.object_blob_persistence_type(pooled_blob_object_id, store_args);
        let pending_context = PendingUploadContext::default();
        match &encoded_blob.data {
            super::BlobData::SliverPairs(sliver_pairs) => {
                let multi_pb = MultiProgress::new();
                self.upload_and_collect_certificate(
                    &encoded_blob.metadata,
                    sliver_pairs.clone(),
                    &persistence_type,
                    Some(&multi_pb),
                    store_args,
                    &pending_context,
                )
                .await
            }
            super::BlobData::BlobForUploadRelay(blob, upload_relay_client) => upload_relay_client
                .send_blob_data_and_get_certificate_with_relay(
                    &self.sui_client,
                    blob.as_ref(),
                    *encoded_blob.metadata.blob_id(),
                    store_args.encoding_type,
                    persistence_type,
                )
                .await
                .map_err(|error| ClientErrorKind::UploadRelayError(error).into()),
        }
    }

    async fn certify_pending_bucket_object_version(
        &self,
        blob_bucket: BlobBucketHandle,
        pending_state: &BucketObjectState,
        encoded_blob: &EncodedBlob,
        store_args: &StoreArgs,
    ) -> ClientResult<()> {
        let pending_version = pending_state.pending_version.as_ref().ok_or_else(|| {
            ClientError::store_blob_internal(
                "bucket object did not return a pending version after registration".to_string(),
            )
        })?;
        let pooled_blob_object_id = pending_version.pooled_blob_object_id.ok_or_else(|| {
            ClientError::store_blob_internal(
                "pending bucket-object version is missing pooled blob object ID".to_string(),
            )
        })?;
        let certificate = self
            .collect_bucket_object_certificate(encoded_blob, pooled_blob_object_id, store_args)
            .await?;
        let pooled_blob: PooledBlob = self
            .sui_client
            .retriable_sui_client()
            .get_sui_object(pooled_blob_object_id)
            .await?;
        self.sui_client
            .certify_blobs_in_bucket(blob_bucket.bucket_object_id, &[(&pooled_blob, certificate)])
            .await
            .map_err(|error| ClientError::from(ClientErrorKind::CertificationFailed(error)))?;
        Ok(())
    }

    async fn register_upload_and_finalize_bucket_object(
        &self,
        blob_bucket: BlobBucketHandle,
        encoded_blob: EncodedBlob,
        store_args: &StoreArgs,
        register: impl std::future::Future<Output = ClientResult<BucketObjectState>>,
    ) -> ClientResult<BucketObjectState> {
        self.ensure_bucket_object_store_args(store_args)?;

        let blob_metadata = BlobObjectMetadata::try_from(encoded_blob.metadata.as_ref())?;
        let committees = self.get_committees().await?;
        self.ensure_bucket_storage_pool_ready_for_object(
            blob_bucket,
            blob_metadata.encoded_size,
            &committees,
            store_args,
        )
        .await?;

        let pending_state = register.await?;
        if are_current_previous_different(
            committees.as_ref(),
            self.get_committees().await?.as_ref(),
        ) {
            return Err(ClientError::from(ClientErrorKind::CommitteeChangeNotified));
        }

        self.certify_pending_bucket_object_version(
            blob_bucket,
            &pending_state,
            &encoded_blob,
            store_args,
        )
        .await?;
        self.sui_client
            .finalize_bucket_object_if_certified(
                pending_state.bucket_object.id,
                blob_bucket.bucket_object_id,
            )
            .await
            .map_err(ClientError::from)
    }
}

/// Exact-key bucket-object APIs on top of the Sui client and Walrus upload path.
pub trait BucketObjectsApi: StoreBlobsApi + Sized {
    /// Creates a new bucket-object registry for the specified blob bucket.
    fn create_bucket_object_registry(
        &self,
        bucket_object_package_id: ObjectID,
        blob_bucket_object_id: ObjectID,
    ) -> impl std::future::Future<Output = ClientResult<ObjectID>> + Send {
        async move {
            let client = <Self as StoreBlobsApi>::client(self).await?;
            client
                .sui_client
                .create_bucket_object_registry(bucket_object_package_id, blob_bucket_object_id)
                .await
                .map_err(ClientError::from)
        }
    }

    /// Resolves a bucket object by key, if present.
    fn head_bucket_object_by_key(
        &self,
        registry_object_id: ObjectID,
        key: &str,
    ) -> impl std::future::Future<Output = ClientResult<Option<BucketObjectState>>> + Send {
        let key = key.to_owned();
        async move {
            let client = <Self as StoreBlobsApi>::client(self).await?;
            client
                .sui_client
                .read_client()
                .get_bucket_object_state_by_key(registry_object_id, &key)
                .await
                .map_err(ClientError::from)
        }
    }

    /// Returns the current state of the specified bucket object.
    fn head_bucket_object(
        &self,
        bucket_object_id: ObjectID,
    ) -> impl std::future::Future<Output = ClientResult<BucketObjectState>> + Send {
        async move {
            let client = <Self as StoreBlobsApi>::client(self).await?;
            client
                .sui_client
                .read_client()
                .get_bucket_object_state(bucket_object_id)
                .await
                .map_err(ClientError::from)
        }
    }

    /// Resolves a bucket object by key, creating it if absent.
    fn resolve_or_create_bucket_object(
        &self,
        registry_object_id: ObjectID,
        key: String,
    ) -> impl std::future::Future<Output = ClientResult<BucketObjectState>> + Send {
        async move {
            let client = <Self as StoreBlobsApi>::client(self).await?;
            client
                .sui_client
                .resolve_or_create_bucket_object(registry_object_id, key)
                .await
                .map_err(ClientError::from)
        }
    }

    /// Registers, uploads, certifies, and finalizes a new object version if the object is absent.
    fn put_bucket_object_if_absent(
        &self,
        registry_object_id: ObjectID,
        blob_bucket: BlobBucketHandle,
        key: String,
        blob: Vec<u8>,
        attrs: BucketObjectVersionAttrs,
        store_args: &StoreArgs,
    ) -> impl std::future::Future<Output = ClientResult<BucketObjectState>> + Send {
        let store_args = store_args.clone();
        async move {
            let client = <Self as StoreBlobsApi>::client(self).await?;
            let encoded_blob = client.encode_bucket_object_blob(blob, &store_args)?;
            let blob_metadata = BlobObjectMetadata::try_from(encoded_blob.metadata.as_ref())?;
            let bucket_object = client
                .sui_client
                .resolve_or_create_bucket_object(registry_object_id, key)
                .await?;
            let version_input = BucketObjectVersionInput {
                blob_metadata,
                persistence: store_args.persistence,
                headers: attrs.headers,
                metadata: attrs.metadata,
                tags: attrs.tags,
                content_etag: attrs
                    .content_etag
                    .unwrap_or_else(|| encoded_blob.metadata.blob_id().to_string()),
                object_etag: attrs
                    .object_etag
                    .unwrap_or_else(|| client.fresh_object_etag()),
            };
            client
                .register_upload_and_finalize_bucket_object(
                    blob_bucket,
                    encoded_blob,
                    &store_args,
                    async {
                        client
                            .sui_client
                            .put_bucket_object_if_absent_and_register(
                                bucket_object.bucket_object.id,
                                blob_bucket.bucket_object_id,
                                blob_bucket.cap_object_id,
                                version_input,
                            )
                            .await
                            .map_err(ClientError::from)
                    },
                )
                .await
        }
    }

    /// Registers, uploads, certifies, and finalizes a new object version if the etag matches.
    #[allow(clippy::too_many_arguments)]
    fn update_bucket_object_if_match(
        &self,
        registry_object_id: ObjectID,
        blob_bucket: BlobBucketHandle,
        key: String,
        expected_object_etag: String,
        blob: Vec<u8>,
        attrs: BucketObjectVersionAttrs,
        store_args: &StoreArgs,
    ) -> impl std::future::Future<Output = ClientResult<BucketObjectState>> + Send {
        let store_args = store_args.clone();
        async move {
            let client = <Self as StoreBlobsApi>::client(self).await?;
            let encoded_blob = client.encode_bucket_object_blob(blob, &store_args)?;
            let blob_metadata = BlobObjectMetadata::try_from(encoded_blob.metadata.as_ref())?;
            let bucket_object = client
                .sui_client
                .read_client()
                .get_bucket_object_state_by_key(registry_object_id, &key)
                .await?
                .ok_or_else(|| {
                    ClientError::store_blob_internal(format!(
                        "bucket object for key '{key}' does not exist"
                    ))
                })?;
            let version_input = BucketObjectVersionInput {
                blob_metadata,
                persistence: store_args.persistence,
                headers: attrs.headers,
                metadata: attrs.metadata,
                tags: attrs.tags,
                content_etag: attrs
                    .content_etag
                    .unwrap_or_else(|| encoded_blob.metadata.blob_id().to_string()),
                object_etag: attrs
                    .object_etag
                    .unwrap_or_else(|| client.fresh_object_etag()),
            };
            client
                .register_upload_and_finalize_bucket_object(
                    blob_bucket,
                    encoded_blob,
                    &store_args,
                    async {
                        client
                            .sui_client
                            .update_bucket_object_if_match_and_register(
                                bucket_object.bucket_object.id,
                                blob_bucket.bucket_object_id,
                                blob_bucket.cap_object_id,
                                expected_object_etag,
                                version_input,
                            )
                            .await
                            .map_err(ClientError::from)
                    },
                )
                .await
        }
    }

    /// Performs an attribute-only update on a live object version.
    fn update_bucket_object_attributes(
        &self,
        bucket_object_id: ObjectID,
        update: BucketObjectAttributeUpdate,
    ) -> impl std::future::Future<Output = ClientResult<BucketObjectState>> + Send {
        async move {
            let client = <Self as StoreBlobsApi>::client(self).await?;
            client
                .sui_client
                .update_bucket_object_attributes(
                    bucket_object_id,
                    update.headers,
                    update.metadata,
                    update.tags,
                    update
                        .object_etag
                        .unwrap_or_else(|| client.fresh_object_etag()),
                )
                .await
                .map_err(ClientError::from)
        }
    }

    /// Performs an attribute-only update when the current etag matches.
    fn update_bucket_object_attributes_if_match(
        &self,
        bucket_object_id: ObjectID,
        expected_object_etag: String,
        update: BucketObjectAttributeUpdate,
    ) -> impl std::future::Future<Output = ClientResult<BucketObjectState>> + Send {
        async move {
            let client = <Self as StoreBlobsApi>::client(self).await?;
            client
                .sui_client
                .update_bucket_object_attributes_if_match(
                    bucket_object_id,
                    expected_object_etag,
                    update.headers,
                    update.metadata,
                    update.tags,
                    update
                        .object_etag
                        .unwrap_or_else(|| client.fresh_object_etag()),
                )
                .await
                .map_err(ClientError::from)
        }
    }

    /// Deletes an object by creating a delete marker.
    fn delete_bucket_object(
        &self,
        bucket_object_id: ObjectID,
        object_etag: Option<String>,
    ) -> impl std::future::Future<Output = ClientResult<BucketObjectState>> + Send {
        async move {
            let client = <Self as StoreBlobsApi>::client(self).await?;
            client
                .sui_client
                .delete_bucket_object(
                    bucket_object_id,
                    object_etag.unwrap_or_else(|| client.fresh_object_etag()),
                )
                .await
                .map_err(ClientError::from)
        }
    }

    /// Deletes an object by creating a delete marker when the current etag matches.
    fn delete_bucket_object_if_match(
        &self,
        bucket_object_id: ObjectID,
        expected_object_etag: String,
        object_etag: Option<String>,
    ) -> impl std::future::Future<Output = ClientResult<BucketObjectState>> + Send {
        async move {
            let client = <Self as StoreBlobsApi>::client(self).await?;
            client
                .sui_client
                .delete_bucket_object_if_match(
                    bucket_object_id,
                    expected_object_etag,
                    object_etag.unwrap_or_else(|| client.fresh_object_etag()),
                )
                .await
                .map_err(ClientError::from)
        }
    }

    /// Copies a live object to a new key if the destination is absent.
    fn copy_bucket_object_if_absent(
        &self,
        registry_object_id: ObjectID,
        source_bucket_object_id: ObjectID,
        destination_key: String,
        object_etag: Option<String>,
    ) -> impl std::future::Future<Output = ClientResult<BucketObjectState>> + Send {
        async move {
            let client = <Self as StoreBlobsApi>::client(self).await?;
            client
                .sui_client
                .copy_bucket_object_if_absent(
                    registry_object_id,
                    source_bucket_object_id,
                    destination_key,
                    object_etag.unwrap_or_else(|| client.fresh_object_etag()),
                )
                .await
                .map_err(ClientError::from)
        }
    }

    /// Renames an object to a new key.
    fn rename_bucket_object(
        &self,
        registry_object_id: ObjectID,
        bucket_object_id: ObjectID,
        new_key: String,
    ) -> impl std::future::Future<Output = ClientResult<BucketObjectState>> + Send {
        async move {
            let client = <Self as StoreBlobsApi>::client(self).await?;
            client
                .sui_client
                .rename_bucket_object(registry_object_id, bucket_object_id, new_key)
                .await
                .map_err(ClientError::from)
        }
    }

    /// Renames an object to a new key when the current etag matches.
    fn rename_bucket_object_if_match(
        &self,
        registry_object_id: ObjectID,
        bucket_object_id: ObjectID,
        expected_object_etag: String,
        new_key: String,
    ) -> impl std::future::Future<Output = ClientResult<BucketObjectState>> + Send {
        async move {
            let client = <Self as StoreBlobsApi>::client(self).await?;
            client
                .sui_client
                .rename_bucket_object_if_match(
                    registry_object_id,
                    bucket_object_id,
                    expected_object_etag,
                    new_key,
                )
                .await
                .map_err(ClientError::from)
        }
    }
}

impl BucketObjectsApi for WalrusNodeClientCreatedInBackground<SuiContractClient> {}

impl BucketObjectsApi for WalrusNodeClient<SuiContractClient> {}

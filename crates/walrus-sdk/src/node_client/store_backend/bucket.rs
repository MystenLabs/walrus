// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Blob bucket store backend.

use walrus_sui::{
    client::{BlobBucketStoragePoolStatus, BlobObjectMetadata, PostStoreAction},
    types::PooledBlob,
};

use super::*;
use crate::node_client::client_types::{
    BucketBlobAwaitingUpload,
    BucketBlobPendingCertify,
    WalrusStoreBlob,
};

pub(in crate::node_client) struct BlobBucketStoreBackend<'a> {
    client: &'a WalrusNodeClient<SuiContractClient>,
    blob_bucket: BlobBucketHandle,
}

impl<'a> BlobBucketStoreBackend<'a> {
    pub(super) fn new(
        client: &'a WalrusNodeClient<SuiContractClient>,
        blob_bucket: BlobBucketHandle,
    ) -> Self {
        Self {
            client,
            blob_bucket,
        }
    }

    async fn reserve_and_store_encoded_blobs_inner(
        &self,
        encoded_blobs: Vec<WalrusStoreBlobUnfinished<EncodedBlob>>,
        store_args: &StoreArgs,
    ) -> ClientResult<Vec<WalrusStoreBlobFinished<BlobBucketStoreResult>>> {
        if encoded_blobs.is_empty() {
            return Ok(vec![]);
        }

        let store_args = self.bucket_store_args(store_args)?;

        tracing::info!(
            bucket_object_id = %self.blob_bucket.bucket_object_id,
            blob_count = encoded_blobs.len(),
            "writing blobs to blob bucket"
        );

        let (blobs_to_register, mut final_results) = self.prepare_register_blobs(encoded_blobs);
        if blobs_to_register.is_empty() {
            return Ok(final_results);
        }

        let committees = self.client.get_committees().await?;
        self.ensure_bucket_storage_pool_ready(&blobs_to_register, &committees, &store_args)
            .await?;

        let registered_blobs = self.register_blobs(blobs_to_register, &store_args).await?;
        let (blobs_awaiting_upload, completed_blobs) =
            client_types::partition_unfinished_finished(registered_blobs);
        final_results.extend(completed_blobs);

        if are_current_previous_different(
            committees.as_ref(),
            self.client.get_committees().await?.as_ref(),
        ) {
            tracing::warn!("committees have changed while registering bucket blobs");
            return Err(ClientError::from(ClientErrorKind::CommitteeChangeNotified));
        }

        let blobs_with_certificates = self
            .get_all_blob_certificates(blobs_awaiting_upload, &store_args)
            .await?;
        let (blobs_pending_certify, completed_blobs) =
            client_types::partition_unfinished_finished(blobs_with_certificates);
        final_results.extend(completed_blobs);

        final_results.extend(
            self.certify_blobs_in_bucket(blobs_pending_certify, &store_args)
                .await?,
        );

        Ok(final_results)
    }

    fn bucket_store_args(&self, store_args: &StoreArgs) -> ClientResult<StoreArgs> {
        if store_args.post_store != PostStoreAction::Keep {
            return Err(ClientError::store_blob_internal(
                "blob bucket store only supports PostStoreAction::Keep".to_string(),
            ));
        }

        let mut store_args = store_args.clone();
        if store_args.store_optimizations.optimistic_uploads_enabled() {
            tracing::debug!("disabling optimistic uploads for blob bucket store");
        }
        store_args.store_optimizations = store_args
            .store_optimizations
            .with_optimistic_uploads(false);
        Ok(store_args)
    }

    fn prepare_register_blobs(
        &self,
        encoded_blobs: Vec<WalrusStoreBlobUnfinished<EncodedBlob>>,
    ) -> (
        Vec<WalrusStoreBlobUnfinished<EncodedBlob>>,
        Vec<WalrusStoreBlobFinished<BlobBucketStoreResult>>,
    ) {
        let mut blobs_to_register = Vec::with_capacity(encoded_blobs.len());
        let mut final_results = Vec::new();

        for encoded_blob in encoded_blobs {
            let blob_id = encoded_blob.state.blob_id();
            if let Err(error) = self.client.check_blob_is_blocked(&blob_id) {
                final_results.push(WalrusStoreBlob {
                    common: encoded_blob.common,
                    state: BlobBucketStoreResult::Error {
                        blob_id: Some(blob_id),
                        failure_phase: "check_blob_is_blocked".to_string(),
                        error_msg: error.to_string(),
                    },
                });
            } else {
                blobs_to_register.push(encoded_blob);
            }
        }

        (blobs_to_register, final_results)
    }

    async fn ensure_bucket_storage_pool_ready(
        &self,
        encoded_blobs: &[WalrusStoreBlobUnfinished<EncodedBlob>],
        committees: &ActiveCommittees,
        store_args: &StoreArgs,
    ) -> ClientResult<()> {
        let required_encoded_capacity_bytes =
            encoded_blobs
                .iter()
                .try_fold(0_u64, |total, blob| -> ClientResult<u64> {
                    let encoded_size = blob.common.encoded_size().ok_or_else(|| {
                        ClientError::store_blob_internal(format!(
                            "cannot compute encoded size for blob {}",
                            blob.common.identifier
                        ))
                    })?;
                    total.checked_add(encoded_size).ok_or_else(|| {
                        ClientError::store_blob_internal(
                            "bucket store encoded capacity requirement overflowed".to_string(),
                        )
                    })
                })?;

        let current_epoch = committees.write_committee().epoch;
        let target_end_epoch = current_epoch + store_args.epochs_ahead;

        let mut storage_pool_status = self
            .client
            .sui_client
            .blob_bucket_storage_pool_status(self.blob_bucket.bucket_object_id)
            .await?;
        self.ensure_bucket_storage_pool_active(storage_pool_status, current_epoch)?;

        if storage_pool_status.end_epoch < target_end_epoch {
            let epochs_extended = target_end_epoch - storage_pool_status.end_epoch;
            self.client
                .sui_client
                .extend_blob_bucket_storage_pool(
                    self.blob_bucket.bucket_object_id,
                    self.blob_bucket.cap_object_id,
                    epochs_extended,
                )
                .await?;
            storage_pool_status.end_epoch = target_end_epoch;
        }

        let available_encoded_capacity_bytes =
            storage_pool_status.available_encoded_capacity_bytes();
        if available_encoded_capacity_bytes < required_encoded_capacity_bytes {
            let additional_encoded_capacity_bytes =
                required_encoded_capacity_bytes - available_encoded_capacity_bytes;
            self.client
                .sui_client
                .increase_blob_bucket_storage_pool_capacity(
                    self.blob_bucket.bucket_object_id,
                    self.blob_bucket.cap_object_id,
                    additional_encoded_capacity_bytes,
                )
                .await?;
        }

        Ok(())
    }

    fn ensure_bucket_storage_pool_active(
        &self,
        storage_pool_status: BlobBucketStoragePoolStatus,
        current_epoch: Epoch,
    ) -> ClientResult<()> {
        if storage_pool_status.end_epoch <= current_epoch {
            return Err(ClientError::store_blob_internal(format!(
                "blob bucket storage pool {} is not active at epoch {}",
                storage_pool_status.storage_pool_id, current_epoch
            )));
        }
        Ok(())
    }

    async fn register_blobs(
        &self,
        encoded_blobs: Vec<WalrusStoreBlobUnfinished<EncodedBlob>>,
        store_args: &StoreArgs,
    ) -> ClientResult<
        Vec<WalrusStoreBlobMaybeFinished<BucketBlobAwaitingUpload, BlobBucketStoreResult>>,
    > {
        let blob_metadata_list = encoded_blobs
            .iter()
            .map(|blob| {
                BlobObjectMetadata::try_from(blob.state.metadata.as_ref())
                    .map_err(ClientError::from)
            })
            .collect::<ClientResult<Vec<_>>>()?;

        let pooled_blobs = self
            .client
            .sui_client
            .register_blobs_in_bucket(
                self.blob_bucket.bucket_object_id,
                self.blob_bucket.cap_object_id,
                blob_metadata_list,
                store_args.persistence,
            )
            .await?;

        self.match_registered_pooled_blobs(encoded_blobs, pooled_blobs)
    }

    fn match_registered_pooled_blobs(
        &self,
        encoded_blobs: Vec<WalrusStoreBlobUnfinished<EncodedBlob>>,
        pooled_blobs: Vec<PooledBlob>,
    ) -> ClientResult<
        Vec<WalrusStoreBlobMaybeFinished<BucketBlobAwaitingUpload, BlobBucketStoreResult>>,
    > {
        let mut pooled_blob_map = HashMap::new();
        for pooled_blob in pooled_blobs {
            pooled_blob_map
                .entry(pooled_blob.blob_id)
                .or_insert_with(Vec::new)
                .push(pooled_blob);
        }

        Ok(encoded_blobs
            .into_iter()
            .map(|blob| {
                let blob_id = blob.state.blob_id();
                let Some(entries) = pooled_blob_map.get_mut(&blob_id) else {
                    return blob.fail_with(
                        ClientError::store_blob_internal(
                            "unable to match pooled blob returned from registration".to_string(),
                        ),
                        "register_blobs_in_bucket",
                    );
                };

                let pooled_blob = entries.pop().expect(
                    "we never insert an empty vec and remove vectors when we pop the final element",
                );
                if entries.is_empty() {
                    pooled_blob_map.remove(&blob_id);
                }

                blob.map_infallible(
                    |encoded_blob| BucketBlobAwaitingUpload {
                        encoded_blob,
                        pooled_blob: pooled_blob.clone(),
                    },
                    "with_bucket_register_result",
                )
                .into_maybe_finished()
            })
            .collect())
    }

    async fn get_all_blob_certificates(
        &self,
        blobs_to_be_certified: Vec<WalrusStoreBlobUnfinished<BucketBlobAwaitingUpload>>,
        store_args: &StoreArgs,
    ) -> ClientResult<
        Vec<WalrusStoreBlobMaybeFinished<BucketBlobPendingCertify, BlobBucketStoreResult>>,
    > {
        if blobs_to_be_certified.is_empty() {
            return Ok(vec![]);
        }

        let get_cert_timer = Instant::now();
        let multi_pb = Arc::new(MultiProgress::new());
        let pending_context = PendingUploadContext::default();
        let blobs: Vec<
            WalrusStoreBlobMaybeFinished<BucketBlobPendingCertify, BlobBucketStoreResult>,
        > = futures::future::try_join_all(blobs_to_be_certified.into_iter().map(
            |blob_to_be_certified| {
                let multi_pb = Arc::clone(&multi_pb);
                let pending_context = pending_context.clone();
                async move {
                    self.get_certificate(
                        blob_to_be_certified,
                        multi_pb.as_ref(),
                        store_args,
                        &pending_context,
                    )
                    .await
                }
            },
        ))
        .await?;

        if !walrus_utils::is_internal_run() {
            let certificate_count = blobs.iter().filter(|blob| !blob.is_finished()).count();
            tracing::info!(
                duration = ?get_cert_timer.elapsed(),
                "obtained {certificate_count} blob bucket certificate{}",
                if certificate_count == 1 { "" } else { "s" },
            );
        }

        Ok(blobs)
    }

    async fn get_certificate(
        &self,
        blob_to_be_certified: WalrusStoreBlobUnfinished<BucketBlobAwaitingUpload>,
        multi_pb: &MultiProgress,
        store_args: &StoreArgs,
        pending_context: &PendingUploadContext,
    ) -> ClientResult<WalrusStoreBlobMaybeFinished<BucketBlobPendingCertify, BlobBucketStoreResult>>
    {
        let BucketBlobAwaitingUpload {
            encoded_blob,
            pooled_blob,
        } = &blob_to_be_certified.state;

        tracing::debug!(
            delay = ?self.client.config.communication_config.registration_delay,
            "waiting to ensure that all storage nodes have seen the bucket registration"
        );
        tokio::time::sleep(self.client.config.communication_config.registration_delay).await;

        let certify_start_timer = Instant::now();
        let certificate_result: Result<_, ClientError> = match &encoded_blob.data {
            BlobData::SliverPairs(sliver_pairs) => {
                self.client
                    .upload_and_collect_certificate(
                        &encoded_blob.metadata,
                        sliver_pairs.clone(),
                        &pooled_blob.blob_persistence_type(),
                        Some(multi_pb),
                        store_args,
                        pending_context,
                    )
                    .await
            }
            BlobData::BlobForUploadRelay(blob, upload_relay_client) => upload_relay_client
                .send_blob_data_and_get_certificate_with_relay(
                    &self.client.sui_client,
                    blob.as_ref(),
                    pooled_blob.blob_id,
                    store_args.encoding_type,
                    pooled_blob.blob_persistence_type(),
                )
                .await
                .map_err(|error| ClientErrorKind::UploadRelayError(error).into()),
        };

        if !walrus_utils::is_internal_run() {
            tracing::debug!(
                blob_id = %pooled_blob.blob_id,
                duration = ?certify_start_timer.elapsed(),
                blob_size = pooled_blob.unencoded_size,
                "finished sending blob bucket data and collecting certificate"
            );
        }

        blob_to_be_certified.with_certificate_result(certificate_result)
    }

    async fn certify_blobs_in_bucket(
        &self,
        blobs_to_certify: Vec<WalrusStoreBlobUnfinished<BucketBlobPendingCertify>>,
        store_args: &StoreArgs,
    ) -> ClientResult<Vec<WalrusStoreBlobFinished<BlobBucketStoreResult>>> {
        if blobs_to_certify.is_empty() {
            return Ok(vec![]);
        }

        let start = Instant::now();
        let pooled_blobs_with_certificates = blobs_to_certify
            .iter()
            .map(|blob| {
                (
                    &blob.state.pooled_blob,
                    blob.state.certificate.as_ref().clone(),
                )
            })
            .collect::<Vec<_>>();

        let mut certified_pooled_blobs = self
            .client
            .sui_client
            .certify_blobs_in_bucket(
                self.blob_bucket.bucket_object_id,
                &pooled_blobs_with_certificates,
            )
            .await
            .map_err(|error| {
                tracing::warn!(%error, "failure occurred while certifying pooled blobs on Sui");
                ClientError::from(ClientErrorKind::CertificationFailed(error))
            })?
            .into_iter()
            .map(|pooled_blob| (pooled_blob.id, pooled_blob))
            .collect::<HashMap<_, _>>();

        let sui_cert_timer_duration = start.elapsed();
        tracing::info!(
            duration = ?sui_cert_timer_duration,
            "finished certifying pooled blobs on Sui",
        );
        store_args.maybe_observe_upload_certificate(sui_cert_timer_duration);

        let updated_blobs = blobs_to_certify
            .into_iter()
            .map(|blob| {
                let pooled_blob_id = blob.state.pooled_blob.id;
                let certified_pooled_blob = certified_pooled_blobs
                    .remove(&pooled_blob_id)
                    .ok_or_else(|| {
                        ClientError::store_blob_internal(format!(
                            "missing certified pooled blob for object {pooled_blob_id}"
                        ))
                    })?;
                Ok(blob.map_infallible(
                    |bucket_blob| {
                        bucket_blob.with_updated_pooled_blob(certified_pooled_blob.clone())
                    },
                    "with_updated_bucket_pooled_blob",
                ))
            })
            .collect::<ClientResult<Vec<_>>>()?;

        Ok(updated_blobs
            .into_iter()
            .map(|blob| {
                blob.map_infallible(
                    BucketBlobPendingCertify::with_certify_result,
                    "with_bucket_certify_result",
                )
            })
            .collect())
    }
}

impl StoreBackend for BlobBucketStoreBackend<'_> {
    type FinalResult = BlobBucketStoreResult;

    fn kind(&self) -> StoreBackendKind {
        StoreBackendKind::BlobBucket
    }

    fn reserve_and_store_encoded_blobs<'a>(
        &'a self,
        encoded_blobs: Vec<WalrusStoreBlobUnfinished<EncodedBlob>>,
        store_args: &'a StoreArgs,
    ) -> StoreBackendFuture<'a, Self::FinalResult> {
        Box::pin(async move {
            self.reserve_and_store_encoded_blobs_inner(encoded_blobs, store_args)
                .await
        })
    }
}

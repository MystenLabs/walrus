// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Orchestration for the owned blob store pipeline.

use super::*;

impl WalrusNodeClient<SuiContractClient> {
    /// Stores the blobs on Walrus, reserving space or extending registered blobs, if necessary.
    ///
    /// Returns a [`ClientErrorKind::CommitteeChangeNotified`] error if, during the registration or
    /// store operations, the client is notified that the committee has changed.
    #[tracing::instrument(level = Level::DEBUG, skip_all, fields(count = encoded_blobs.len()))]
    pub(super) async fn reserve_and_store_encoded_blobs(
        &self,
        encoded_blobs: Vec<WalrusStoreBlobUnfinished<EncodedBlob>>,
        store_args: &StoreArgs,
    ) -> ClientResult<Vec<WalrusStoreBlobFinished>> {
        let blobs_count = encoded_blobs.len();
        if blobs_count == 0 {
            tracing::debug!("no blobs provided");
            return Ok(vec![]);
        }

        let store_args = self.pending_upload_store_args(store_args);

        tracing::info!(
            "writing {blobs_count} blob{} to Walrus",
            if blobs_count == 1 { "" } else { "s" }
        );
        let status_start_timer = Instant::now();
        let committees = self.get_committees().await?;
        tracing::info!(duration = ?status_start_timer.elapsed(), "finished getting committees");

        let encoded_blobs_with_status = if store_args.store_optimizations.should_check_status() {
            // Retrieve the blob status, checking if the committee has changed in the meantime.
            // This operation can be safely interrupted as it does not require a wallet.
            let encoded_blobs_with_status = self
                .await_while_checking_notification(self.get_blob_statuses(encoded_blobs))
                .await?;

            let status_timer_duration = status_start_timer.elapsed();
            tracing::info!(
                duration = ?status_timer_duration,
                "retrieved blob statuses",
            );
            store_args.maybe_observe_checking_blob_status(status_timer_duration);

            encoded_blobs_with_status
        } else {
            tracing::info!(
                check_status = false,
                "skipping blob status checks; assuming BlobStatus::Nonexistent"
            );

            encoded_blobs
                .into_iter()
                .map(|encoded_blob| {
                    let blob_id = encoded_blob.state.blob_id();
                    if let Err(e) = self.check_blob_is_blocked(&blob_id) {
                        return Ok(encoded_blob
                            .into_maybe_finished::<BlobStoreResult>()
                            .fail_with(e, "check_blob_is_blocked"));
                    }

                    encoded_blob.into_maybe_finished::<BlobStoreResult>().map(
                        |blob| blob.with_status(Ok(BlobStatus::Nonexistent)),
                        "assume_blob_status_nonexistent",
                    )
                })
                .collect::<ClientResult<Vec<_>>>()?
        };

        debug_assert_eq!(
            encoded_blobs_with_status.len(),
            blobs_count,
            "the number of blob statuses and the number of blobs to store must be the same",
        );

        let pending_blobs = self.pending_upload_candidates(&encoded_blobs_with_status, &store_args);

        let store_op_timer = Instant::now();
        let (registered_blobs, pending_upload_result) = self
            .register_with_pending_uploads(
                encoded_blobs_with_status,
                &store_args,
                &committees,
                &pending_blobs,
            )
            .await?;
        debug_assert_eq!(
            registered_blobs.len(),
            blobs_count,
            "the number of registered blobs and the number of blobs to store must be the same",
        );

        let store_op_duration = store_op_timer.elapsed();
        tracing::info!(duration = ?store_op_duration, "finished registering blobs");
        tracing::debug!(?registered_blobs);
        store_args.maybe_observe_store_operation(store_op_duration);

        let pending_context = self.apply_pending_upload_outcome(pending_upload_result);

        let (mut final_result, blobs_awaiting_upload, mut blobs_pending_certify_and_extend) =
            Self::partition_registered_blobs(registered_blobs, blobs_count);
        let num_to_be_certified = blobs_awaiting_upload.len();

        // Check if the committee has changed while registering the blobs.
        if are_current_previous_different(
            committees.as_ref(),
            self.get_committees().await?.as_ref(),
        ) {
            tracing::warn!("committees have changed while registering blobs");
            return Err(ClientError::from(ClientErrorKind::CommitteeChangeNotified));
        }

        // Get blob certificates for to_be_certified blobs.
        let mut blobs_with_certificates = Vec::with_capacity(blobs_awaiting_upload.len());
        if !blobs_awaiting_upload.is_empty() {
            let get_certificates_timer = Instant::now();
            // Get the blob certificates, possibly storing slivers, while checking if the committee
            // has changed in the meantime.
            // This operation can be safely interrupted as it does not require a wallet.
            blobs_with_certificates = self
                .await_while_checking_notification(self.get_all_blob_certificates(
                    blobs_awaiting_upload,
                    &store_args,
                    &pending_context,
                ))
                .await?;

            debug_assert_eq!(blobs_with_certificates.len(), num_to_be_certified);
            let get_certificates_duration = get_certificates_timer.elapsed();

            tracing::debug!(
                duration = ?get_certificates_duration,
                "fetched certificates for {} blobs",
                blobs_with_certificates.len()
            );
            store_args.maybe_observe_get_certificates(get_certificates_duration);
        }

        // Move completed blobs to final_result and keep only non-completed ones.
        let (to_be_certified, completed_blobs) =
            client_types::partition_unfinished_finished(blobs_with_certificates);
        final_result.extend(completed_blobs);
        blobs_pending_certify_and_extend.extend(to_be_certified);

        // Certify and extend the blobs on Sui.
        final_result.extend(
            self.certify_and_extend_blobs(blobs_pending_certify_and_extend, &store_args)
                .await?,
        );

        Ok(final_result)
    }

    fn pending_upload_store_args(&self, store_args: &StoreArgs) -> StoreArgs {
        let mut store_args = store_args.clone();
        if self.config.communication_config.pending_uploads_enabled {
            store_args.store_optimizations.optimistic_uploads = true;
        }
        store_args
    }

    fn pending_upload_candidates(
        &self,
        encoded_blobs_with_status: &[WalrusStoreBlobMaybeFinished<BlobWithStatus>],
        store_args: &StoreArgs,
    ) -> Vec<(VerifiedBlobMetadataWithId, Arc<Vec<SliverPair>>)> {
        let pending_upload_max_blob_bytes = self.pending_upload_max_blob_bytes();
        let pending_uploads_enabled = self.config.communication_config.pending_uploads_enabled;

        let pending: Vec<_> = encoded_blobs_with_status
            .iter()
            .filter_map(|blob| {
                blob.pending_upload_payload(
                    pending_uploads_enabled,
                    store_args,
                    pending_upload_max_blob_bytes,
                )
            })
            .collect();

        tracing::debug!(
            pending_candidates = pending.len(),
            pending_enabled = pending_uploads_enabled,
            pending_uploads = store_args.store_optimizations.pending_uploads_enabled(),
            max_blob_bytes = pending_upload_max_blob_bytes,
            "computed pending upload candidates",
        );

        pending
    }

    fn start_pending_uploads<'a>(
        &'a self,
        pending_blobs: &'a [(VerifiedBlobMetadataWithId, Arc<Vec<SliverPair>>)],
        store_args: &StoreArgs,
    ) -> Option<PendingUploadHandle<'a>> {
        if pending_blobs.is_empty() {
            return None;
        }

        // We don't need to receive the results of the pending uploads,
        // so we can ignore the receiver. The progress bar is only updated during the upload
        // with immediate intent later.
        tracing::info!(
            pending_blobs = pending_blobs.len(),
            "starting pending upload task"
        );
        let (tx, mut rx) = tokio::sync::mpsc::channel(pending_blobs.len().max(1));
        tokio::spawn(async move { while rx.recv().await.is_some() {} });

        let stop_scheduling = CancellationToken::new();
        let cancel = CancellationToken::new();
        let future = Box::pin(self.distributed_upload_without_confirmation(
            pending_blobs,
            tx,
            UploadOptions {
                // Pending uploads are opportunistic and run concurrently with chain registration.
                // Detach the tail window so we can return as soon as quorum is reached, while still
                // allowing extra writes to complete in the background.
                tail_handling: TailHandling::Detached,
                target_nodes: None,
                upload_intent: UploadIntent::Pending,
                initial_completed_weight: None,
                stop_scheduling: Some(stop_scheduling.clone()),
                cancellation: Some(cancel.clone()),
                metrics: store_args.metrics.clone(),
            },
        ));

        Some(PendingUploadHandle {
            stop_scheduling,
            cancel,
            future,
        })
    }

    async fn register_with_pending_uploads(
        &self,
        encoded_blobs_with_status: Vec<WalrusStoreBlobMaybeFinished<BlobWithStatus>>,
        store_args: &StoreArgs,
        committees: &ActiveCommittees,
        pending_blobs: &[(VerifiedBlobMetadataWithId, Arc<Vec<SliverPair>>)],
    ) -> ClientResult<(
        Vec<WalrusStoreBlobMaybeFinished<OwnedRegisteredBlob>>,
        Option<RunOutput<Vec<BlobId>, StoreError>>,
    )> {
        let mut pending_upload_handle = self.start_pending_uploads(pending_blobs, store_args);

        // Register blobs if they are not registered, and get the store operations.
        let resource_manager = self.resource_manager(committees);
        let registration_fut = resource_manager.register_walrus_store_blobs(
            encoded_blobs_with_status,
            store_args.epochs_ahead,
            store_args.persistence,
            store_args.store_optimizations,
        );

        if let Some(ref mut pending) = pending_upload_handle {
            tokio::pin!(registration_fut);
            let mut pending_upload_result = None;
            let pending_grace = self.config.communication_config.pending_upload_grace;
            let registered_blobs = tokio::select! {
                reg = &mut registration_fut => {
                    // Stop scheduling new pending uploads once registration completes.
                    pending.stop_scheduling.cancel();
                    reg
                }
                pending_res = pending.future.as_mut() => {
                    // Pending uploads may finish first; record their output before awaiting
                    // registration.
                    pending_upload_result = Some(pending_res?);
                    registration_fut.await
                }
            }?;

            if pending_upload_result.is_none() {
                // Registration finished first; allow in-flight pending uploads to finish for the
                // grace period before hard-cancelling.
                pending.stop_scheduling.cancel();
                if pending_grace.is_zero() {
                    pending.cancel.cancel();
                    match pending.future.as_mut().await {
                        Ok(res) => pending_upload_result = Some(res),
                        Err(err) => {
                            tracing::debug!(?err, "pending upload task failed or cancelled")
                        }
                    }
                } else {
                    match tokio::time::timeout(pending_grace, pending.future.as_mut()).await {
                        Ok(res) => pending_upload_result = Some(res?),
                        Err(_) => {
                            pending.cancel.cancel();
                            match pending.future.as_mut().await {
                                Ok(res) => pending_upload_result = Some(res),
                                Err(err) => {
                                    tracing::debug!(?err, "pending upload failed or cancelled",)
                                }
                            }
                        }
                    }
                }
            }

            Ok((registered_blobs, pending_upload_result))
        } else {
            Ok((registration_fut.await?, None))
        }
    }

    fn apply_pending_upload_outcome(
        &self,
        mut pending_upload_result: Option<RunOutput<Vec<BlobId>, StoreError>>,
    ) -> PendingUploadContext {
        let mut context = PendingUploadContext::default();
        // Cancel any in-flight tail window from the pending (optimistic) uploader run. The pending
        // path is only used to seed initial success weight; we don't want it to compete with the
        // subsequent immediate upload.
        if let Some(ref mut pending_output) = pending_upload_result
            && let Some(handle) = pending_output.tail_handle.take()
        {
            handle.abort();
        }

        if let Some(pending_output) = pending_upload_result {
            let success_nodes = Self::successful_nodes_by_blob(&pending_output.results);
            if !success_nodes.is_empty() {
                context.initial_success_nodes = Some(success_nodes);
            }
        }

        context
    }

    fn partition_registered_blobs(
        registered_blobs: Vec<WalrusStoreBlobMaybeFinished<OwnedRegisteredBlob>>,
        blobs_count: usize,
    ) -> (
        Vec<WalrusStoreBlobFinished>,
        Vec<WalrusStoreBlobUnfinished<OwnedBlobAwaitingUpload>>,
        Vec<WalrusStoreBlobUnfinished<OwnedBlobPendingCertifyAndExtend>>,
    ) {
        // Classify the blobs into to_be_certified and to_be_extended, and move completed blobs to
        // final_result.
        let mut final_result: Vec<WalrusStoreBlobFinished> = Vec::with_capacity(blobs_count);
        let mut blobs_awaiting_upload = Vec::new();
        let mut blobs_pending_certify_and_extend = Vec::new();

        for registered_blob in registered_blobs {
            match registered_blob.try_finish() {
                Ok(blob) => final_result.push(blob),
                Err(blob) => match blob.map_either(|blob| blob.classify(), "classify") {
                    utils::Either::Left(blob_to_be_certified) => {
                        blobs_awaiting_upload.push(blob_to_be_certified)
                    }
                    utils::Either::Right(blob_to_be_extended) => {
                        blobs_pending_certify_and_extend.push(blob_to_be_extended)
                    }
                },
            }
        }
        let num_to_be_certified = blobs_awaiting_upload.len();
        debug_assert_eq!(
            num_to_be_certified + blobs_pending_certify_and_extend.len() + final_result.len(),
            blobs_count,
            "the sum of the number of blobs to certify, extend, and store must be the original \
            number of blobs"
        );

        (
            final_result,
            blobs_awaiting_upload,
            blobs_pending_certify_and_extend,
        )
    }

    /// Fetches the status of each blob.
    #[tracing::instrument(level = Level::DEBUG, skip_all)]
    async fn get_blob_statuses(
        &self,
        encoded_blobs: Vec<WalrusStoreBlobUnfinished<EncodedBlob>>,
    ) -> ClientResult<Vec<WalrusStoreBlobMaybeFinished<BlobWithStatus>>> {
        futures::future::try_join_all(encoded_blobs.into_iter().map(|encoded_blob| async move {
            let blob_id = encoded_blob.state.blob_id();
            if let Err(e) = self.check_blob_is_blocked(&blob_id) {
                return Ok(encoded_blob
                    .into_maybe_finished::<BlobStoreResult>()
                    .fail_with(e, "check_blob_is_blocked"));
            }
            let status_result = self
                .get_blob_status_with_retries(&blob_id, &self.sui_client)
                .await;
            encoded_blob
                .into_maybe_finished::<BlobStoreResult>()
                .map(|blob| blob.with_status(status_result), "get_blob_status")
        }))
        .await
    }

    /// Fetches the certificates for all the blobs, and returns a vector of
    /// WalrusStoreBlob::WithCertificate or WalrusStoreBlob::Error.
    #[tracing::instrument(level = Level::DEBUG, skip_all)]
    async fn get_all_blob_certificates(
        &self,
        blobs_to_be_certified: Vec<WalrusStoreBlobUnfinished<OwnedBlobAwaitingUpload>>,
        store_args: &StoreArgs,
        pending_context: &PendingUploadContext,
    ) -> ClientResult<Vec<WalrusStoreBlobMaybeFinished<OwnedBlobPendingCertifyAndExtend>>> {
        if blobs_to_be_certified.is_empty() {
            return Ok(vec![]);
        }

        let get_cert_timer = Instant::now();

        let multi_pb = Arc::new(MultiProgress::new());
        let blobs = futures::future::try_join_all(blobs_to_be_certified.into_iter().map(
            |blob_to_be_certified| {
                let multi_pb = Arc::clone(&multi_pb);
                async move {
                    self.get_certificate(
                        blob_to_be_certified,
                        multi_pb.as_ref(),
                        store_args,
                        pending_context,
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
                "obtained {certificate_count} blob certificate{}",
                if certificate_count == 1 { "" } else { "s" },
            );
        }

        Ok(blobs)
    }

    async fn get_certificate(
        &self,
        blob_to_be_certified: WalrusStoreBlobUnfinished<OwnedBlobAwaitingUpload>,
        multi_pb: &MultiProgress,
        store_args: &StoreArgs,
        pending_context: &PendingUploadContext,
    ) -> ClientResult<WalrusStoreBlobMaybeFinished<OwnedBlobPendingCertifyAndExtend>> {
        let committees = self.get_committees().await?;

        let OwnedBlobAwaitingUpload {
            encoded_blob,
            status: blob_status,
            blob_object,
            operation,
            ..
        } = &blob_to_be_certified.state;

        let certificate_result = match blob_status.initial_certified_epoch() {
            Some(certified_epoch) if !committees.is_change_in_progress() => {
                // If the blob is already certified on chain and there is no committee change in
                // progress, all nodes already have the slivers.
                self.get_certificate_standalone(
                    &blob_object.blob_id,
                    certified_epoch,
                    &blob_object.blob_persistence_type(),
                )
                .await
            }
            _ => {
                // If the blob is not certified, we need to store the slivers. Also, during
                // epoch change we may need to store the slivers again for an already certified
                // blob, as the current committee may not have synced them yet.

                if (operation.is_registration() || operation.is_reuse_storage())
                    && !blob_status.is_registered()
                {
                    tracing::debug!(
                        delay=?self.config.communication_config.registration_delay,
                        "waiting to ensure that all storage nodes have seen the registration"
                    );
                    tokio::time::sleep(self.config.communication_config.registration_delay).await;
                }

                let certify_start_timer = Instant::now();
                let result: Result<_, ClientError> = match &encoded_blob.data {
                    BlobData::SliverPairs(sliver_pairs) => {
                        self.upload_and_collect_certificate(
                            &encoded_blob.metadata,
                            sliver_pairs.clone(),
                            &blob_object.blob_persistence_type(),
                            Some(multi_pb),
                            store_args,
                            pending_context,
                        )
                        .await
                    }
                    BlobData::BlobForUploadRelay(blob, upload_relay_client) => upload_relay_client
                        .send_blob_data_and_get_certificate_with_relay(
                            &self.sui_client,
                            blob,
                            blob_object.blob_id,
                            store_args.encoding_type,
                            blob_object.blob_persistence_type(),
                        )
                        .await
                        .map_err(|error| ClientErrorKind::UploadRelayError(error).into()),
                };

                let blob_size = blob_object.size;
                if !walrus_utils::is_internal_run() {
                    tracing::debug!(
                        blob_id = %encoded_blob.blob_id(),
                        duration = ?certify_start_timer.elapsed(),
                        blob_size,
                        "finished sending blob data and collecting certificate"
                    );
                }
                result
            }
        };
        blob_to_be_certified.with_certificate_result(certificate_result)
    }

    async fn certify_and_extend_blobs(
        &self,
        blobs_to_certify_and_extend: Vec<
            WalrusStoreBlobUnfinished<OwnedBlobPendingCertifyAndExtend>,
        >,
        store_args: &StoreArgs,
    ) -> ClientResult<Vec<WalrusStoreBlobFinished>> {
        let blobs_count = blobs_to_certify_and_extend.len();
        if blobs_count == 0 {
            return Ok(vec![]);
        }

        let start = Instant::now();

        let certify_and_extend_parameters = blobs_to_certify_and_extend
            .iter()
            .map(|blob| blob.get_certify_and_extend_params())
            .collect::<Vec<_>>();

        let cert_and_extend_results = self
            .sui_client
            .certify_and_extend_blobs(&certify_and_extend_parameters, store_args.post_store)
            .await
            .map_err(|error| {
                tracing::warn!(
                    %error,
                    "failure occurred while certifying and extending blobs on Sui"
                );
                ClientError::from(ClientErrorKind::CertificationFailed(error))
            })?;

        let sui_cert_timer_duration = start.elapsed();
        tracing::info!(
            duration = ?sui_cert_timer_duration,
            "finished certifying and extending blobs on Sui",
        );
        store_args.maybe_observe_upload_certificate(sui_cert_timer_duration);

        // Build map from object ID to CertifyAndExtendBlobResult.
        let result_map: HashMap<ObjectID, CertifyAndExtendBlobResult> = cert_and_extend_results
            .into_iter()
            .map(|result| (result.blob_object_id, result))
            .collect();

        // Get price computation for completing blobs
        let price_computation = self.get_price_computation().await?;
        let results = blobs_to_certify_and_extend
            .into_iter()
            .map(|blob| {
                blob.map_infallible(
                    |blob| {
                        let certify_and_extend_result = result_map.get(&blob.blob_object.id);
                        blob.with_certify_and_extend_result(
                            certify_and_extend_result,
                            &price_computation,
                        )
                    },
                    "with_certify_and_extend_result",
                )
            })
            .collect();
        Ok(results)
    }
}

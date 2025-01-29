// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    cmp,
    collections::{hash_map::IntoValues, HashMap, VecDeque},
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex as SyncMutex, Weak},
    task::{ready, Context, Poll},
    vec::IntoIter,
};

use ::futures::{stream, FutureExt as _, StreamExt as _};
use futures::{future::BoxFuture, stream::FuturesUnordered, Stream as _, TryFutureExt as _};
use rand::{rngs::StdRng, seq::SliceRandom as _};
use tokio::{
    sync::watch,
    time::{self, error::Elapsed},
};
use tower::ServiceExt as _;
use tracing::Instrument as _;
use walrus_core::{
    bft,
    encoding::{
        self,
        EncodingAxis,
        GeneralRecoverySymbol,
        Primary,
        RecoverySymbol as RecoverySymbolData,
        Secondary,
        SliverData,
        SliverRecoveryOrVerificationError,
        SliverVerificationError,
    },
    inconsistency::{InconsistencyProof, SliverOrInconsistencyProof},
    merkle::MerkleProof,
    messages::{CertificateError, InvalidBlobCertificate, InvalidBlobIdAttestation},
    metadata::VerifiedBlobMetadataWithId,
    BlobId,
    Epoch,
    InconsistencyProof as InconsistencyProofEnum,
    RecoverySymbol,
    ShardIndex,
    Sliver,
    SliverIndex,
    SliverPairIndex,
    SliverType,
    SymbolId,
};
use walrus_sdk::client::RecoverySymbolsFilter;
use walrus_sui::types::Committee;
use walrus_utils::backoff::ExponentialBackoffState;

use super::{
    committee_service::NodeCommitteeServiceInner,
    node_service::{NodeService, NodeServiceError, Request, Response},
};
use crate::common::active_committees::CommitteeTracker;

pub(super) struct GetAndVerifyMetadata<'a, T> {
    blob_id: BlobId,
    epoch_certified: Epoch,
    backoff: ExponentialBackoffState,
    shared: &'a NodeCommitteeServiceInner<T>,
}

impl<'a, T> GetAndVerifyMetadata<'a, T>
where
    T: NodeService,
{
    pub fn new(
        blob_id: BlobId,
        epoch_certified: Epoch,
        shared: &'a NodeCommitteeServiceInner<T>,
    ) -> Self {
        Self {
            blob_id,
            epoch_certified,
            backoff: ExponentialBackoffState::new_infinite(
                shared.config.retry_interval_min,
                shared.config.retry_interval_max,
            ),
            shared,
        }
    }

    pub async fn run(mut self) -> VerifiedBlobMetadataWithId {
        let mut committee_listener = self.shared.subscribe_to_committee_changes();

        loop {
            let (n_members, weak_committee) = {
                let committee_tracker = committee_listener.borrow_and_update();
                let committee = committee_tracker
                    .committees()
                    .read_committee(self.epoch_certified)
                    .expect("epoch must not be in the future");

                (committee.n_members(), Arc::downgrade(committee))
            };

            // Check for the completed future or a notification that the committee has
            // changed. Only some changes to the committee will necessitate new requests.
            tokio::select! {
                maybe_metadata = self.run_once(&weak_committee, n_members) => {
                    if let Some(metadata) = maybe_metadata {
                        return metadata;
                    }
                    wait_before_next_attempts(&mut self.backoff, &self.shared.rng).await;
                }
                () = wait_for_read_committee_change(
                    self.epoch_certified,
                    &mut committee_listener,
                    &weak_committee,
                    are_storage_node_addresses_equivalent
                ) => {
                    tracing::debug!("read committee has changed, recreating requests");
                }
            };
        }
    }

    async fn run_once(
        &self,
        weak_committee: &Weak<Committee>,
        n_committee_members: usize,
    ) -> Option<VerifiedBlobMetadataWithId> {
        let n_requests = self.shared.config.max_concurrent_metadata_requests.get();

        let node_order = {
            let mut rng_guard = self
                .shared
                .rng
                .lock()
                .expect("thread must not panic with lock");
            rand::seq::index::sample(&mut *rng_guard, n_committee_members, n_committee_members)
        };

        let requests = node_order.into_iter().filter_map(|index| {
            let Some(committee) = weak_committee.upgrade() else {
                tracing::trace!("committee has been dropped, skipping node from committee");
                return None;
            };
            let node_public_key = &committee.members()[index].public_key;

            // Our own storage node cannot satisfy metadata requests.
            if self.shared.is_local(node_public_key) {
                return None;
            }

            let Some(client) = self.shared.get_node_service_by_id(node_public_key) else {
                tracing::trace!(
                    "unable to get the client, either creation failed or epoch is changing"
                );
                return None;
            };

            let request = async move {
                client
                    .oneshot(Request::GetVerifiedMetadata(self.blob_id))
                    .map_ok(Response::into_value)
                    .await
            };
            let request = time::timeout(self.shared.config.metadata_request_timeout, request)
                .map(log_and_discard_timeout_or_error)
                .instrument(tracing::info_span!(
                    "get_and_verify_metadata node", walrus.node.public_key = %node_public_key
                ));
            Some(request)
        });

        let requests = stream::iter(requests)
            .buffer_unordered(n_requests)
            .filter_map(std::future::ready);

        std::pin::pin!(requests).next().await
    }
}

// TODO(jsmith): Remove once the new code is enabled by default.
pub(super) struct LegacyRecoverSliver<'a, T> {
    metadata: Arc<VerifiedBlobMetadataWithId>,
    sliver_id: SliverPairIndex,
    sliver_type: SliverType,
    epoch_certified: Epoch,
    backoff: ExponentialBackoffState,
    shared: &'a NodeCommitteeServiceInner<T>,
}

impl<'a, T> LegacyRecoverSliver<'a, T>
where
    T: NodeService,
{
    pub fn new(
        metadata: Arc<VerifiedBlobMetadataWithId>,
        sliver_id: SliverPairIndex,
        sliver_type: SliverType,
        epoch_certified: Epoch,
        shared: &'a NodeCommitteeServiceInner<T>,
    ) -> Self {
        Self {
            metadata,
            sliver_id,
            sliver_type,
            epoch_certified,
            backoff: ExponentialBackoffState::new_infinite(
                shared.config.retry_interval_min,
                shared.config.retry_interval_max,
            ),
            shared,
        }
    }

    pub async fn run(mut self) -> Result<Sliver, InconsistencyProofEnum> {
        // Since recovery currently consumes the symbols, rather than copy the symbols in every
        // case to handle the rare cases when we fail to *decode* the sliver despite collecting the
        // required number of symbols, we instead retry the entire process with an increased amount.
        let mut additional_symbols = 0;
        loop {
            if let Some(result) = self
                .recover_with_additional_symbols(additional_symbols)
                .await
            {
                return result;
            }
            additional_symbols += 1;
        }
    }

    async fn recover_with_additional_symbols(
        &mut self,
        additional_symbols: usize,
    ) -> Option<Result<Sliver, InconsistencyProofEnum>> {
        let mut committee_listener = self.shared.subscribe_to_committee_changes();
        let mut collected_symbols: HashMap<ShardIndex, RecoverySymbol<MerkleProof>> =
            Default::default();

        loop {
            let weak_committee = {
                let committee_tracker = committee_listener.borrow_and_update();
                Arc::downgrade(
                    committee_tracker
                        .committees()
                        .read_committee(self.epoch_certified)
                        .expect("epoch must not be in the future"),
                )
            };

            let epoch_certified = self.epoch_certified;
            tokio::select! {
                result = self.collect_recovery_symbols(
                    &mut collected_symbols, additional_symbols, &weak_committee
                ) => {
                    match result {
                        Ok(n_symbols) => {
                            tracing::trace!(
                                %n_symbols,
                                "successfully collected the desired number of recovery symbols"
                            );
                            return self.decode_sliver(collected_symbols);
                        },
                        Err(n_symbols_remaining) => {
                            tracing::trace!(
                                %n_symbols_remaining,
                                "failed to collect sufficient recovery symbols"
                            );
                            wait_before_next_attempts(&mut self.backoff, &self.shared.rng).await;
                        }
                    }
                }
                () = wait_for_read_committee_change(
                    epoch_certified,
                    &mut committee_listener,
                    &weak_committee,
                    are_shard_addresses_equivalent
                ) => {
                    tracing::debug!(
                        "read committee has changed, recreating recovery symbol requests"
                    );
                }
            };
        }
    }

    fn total_symbols_required(&self, additional_symbols: usize) -> usize {
        let min_symbols_for_recovery = if self.sliver_type == SliverType::Primary {
            encoding::min_symbols_for_recovery::<Primary>
        } else {
            encoding::min_symbols_for_recovery::<Secondary>
        };
        usize::from(min_symbols_for_recovery(self.metadata.n_shards())) + additional_symbols
    }

    /// Request and store recovery symbols in `self.collected_symbols`.
    ///
    /// On success, returns the total number of collection symbols; on failure, the number of
    /// symbols still required.
    async fn collect_recovery_symbols(
        &mut self,
        collected_symbols: &mut HashMap<ShardIndex, RecoverySymbol<MerkleProof>>,
        additional_symbols: usize,
        weak_committee: &Weak<Committee>,
    ) -> Result<usize, usize> {
        let total_symbols_required = self.total_symbols_required(additional_symbols);
        let n_symbols_still_required = total_symbols_required - collected_symbols.len();
        debug_assert_ne!(n_symbols_still_required, 0);
        tracing::debug!(
            total_required = total_symbols_required,
            count_missing = n_symbols_still_required,
            "collecting recovery symbols"
        );

        let mut shard_order = {
            let mut rng_guard = self.shared.rng.lock().expect("mutex not poisoned");
            let mut shards =
                ShardIndex::range(0..self.metadata.n_shards().get()).collect::<Vec<_>>();
            shards.shuffle(&mut *rng_guard);
            shards.into_iter()
        };

        // Create an iterator around the remaining shards to filter and create the requests.
        // Drop the iterator between the times we need it, since filtering holds a reference to
        // the collected_symbols.
        let mut pending_requests = self
            .requests_iter(weak_committee, &mut shard_order, collected_symbols)
            .take(n_symbols_still_required)
            .collect::<FuturesUnordered<_>>();

        while let Some(response) = pending_requests.next().await {
            if let Some((shard_index, symbol)) = response {
                collected_symbols.insert(shard_index, symbol);
            } else {
                // Request failed and was logged, replenish the future.
                if let Some(future) = self
                    .requests_iter(weak_committee, &mut shard_order, collected_symbols)
                    .next()
                {
                    pending_requests.push(future);
                }
            }
        }

        debug_assert!(pending_requests.is_empty());
        let total_symbols_collected = collected_symbols.len();
        if total_symbols_collected == total_symbols_required {
            Ok(total_symbols_collected)
        } else {
            Err(total_symbols_required - total_symbols_collected)
        }
    }

    fn requests_iter<'iter>(
        &'a self,
        weak_committee: &'iter Weak<Committee>,
        shard_order: &'iter mut IntoIter<ShardIndex>,
        collected_symbols: &'iter HashMap<ShardIndex, RecoverySymbol<MerkleProof>>,
    ) -> impl Iterator<
        Item = impl Future<Output = Option<(ShardIndex, RecoverySymbol<MerkleProof>)>> + 'a,
    > + 'iter
    where
        'a: 'iter,
    {
        shard_order
            .filter_map(|shard_index| {
                if collected_symbols.contains_key(&shard_index) {
                    tracing::trace!("shard already collected, skipping");
                    return None;
                }

                let Some(committee) = weak_committee.upgrade() else {
                    tracing::trace!("committee has been dropped, skipping shard");
                    return None;
                };

                let index = committee
                    .member_index_for_shard(shard_index)
                    .expect("shard is present in the committee");
                let node_public_key = &committee.members()[index].public_key;

                let Some(client) = self.shared.get_node_service_by_id(node_public_key) else {
                    tracing::trace!(
                        "unable to get the client, either creation failed or epoch is changing"
                    );
                    return None;
                };

                let sliver_id = self.sliver_id;
                let sliver_pair_at_remote =
                    shard_index.to_pair_index(self.metadata.n_shards(), self.metadata.blob_id());

                let request = client
                    .oneshot(Request::GetVerifiedRecoverySymbol {
                        sliver_type: self.sliver_type,
                        metadata: self.metadata.clone(),
                        sliver_pair_at_remote,
                        intersecting_pair_index: sliver_id,
                    })
                    .map_ok(move |symbol| (shard_index, symbol.into_value()));
                let request = time::timeout(self.shared.config.sliver_request_timeout, request)
                    .map(log_and_discard_timeout_or_error)
                    .instrument(tracing::info_span!("get_recovery_symbol",
                        walrus.node.public_key = %node_public_key,
                        walrus.shard_index = %shard_index
                    ));
                tracing::trace!(walrus.shard_index = %shard_index, "created a request for shard");
                Some(request)
            })
            // Ensure that the resulting iterator will always return None when complete.
            .fuse()
    }

    #[tracing::instrument(skip_all)]
    fn decode_sliver(
        &mut self,
        collected_symbols: HashMap<ShardIndex, RecoverySymbol<MerkleProof>>,
    ) -> Option<Result<Sliver, InconsistencyProofEnum>> {
        let recovery_symbols = collected_symbols.into_values();

        if self.sliver_type == SliverType::Primary {
            let symbols = iterate_symbols(recovery_symbols);
            self.decode_sliver_by_axis::<Primary, _>(symbols)
        } else {
            let symbols = iterate_symbols(recovery_symbols);
            self.decode_sliver_by_axis::<Secondary, _>(symbols)
        }
    }

    fn decode_sliver_by_axis<A: EncodingAxis, I>(
        &self,
        recovery_symbols: I,
    ) -> Option<Result<Sliver, InconsistencyProofEnum>>
    where
        I: IntoIterator<Item = RecoverySymbolData<A, MerkleProof>>,
        SliverData<A>: Into<Sliver>,
        InconsistencyProof<A, MerkleProof>: Into<InconsistencyProofEnum>,
    {
        tracing::debug!("beginning to decode recovered sliver");
        let result = SliverData::<A>::recover_sliver_or_generate_inconsistency_proof(
            recovery_symbols,
            self.sliver_id
                .to_sliver_index::<A>(self.metadata.n_shards()),
            (*self.metadata).as_ref(),
            &self.shared.encoding_config,
        );
        tracing::debug!("completing decoding, parsing result");

        match result {
            Ok(SliverOrInconsistencyProof::Sliver(sliver)) => {
                tracing::debug!("successfully recovered sliver");
                Some(Ok(sliver.into()))
            }
            Ok(SliverOrInconsistencyProof::InconsistencyProof(proof)) => {
                tracing::debug!("resulted in an inconsistency proof");
                Some(Err(proof.into()))
            }
            Err(SliverRecoveryOrVerificationError::RecoveryError(err)) => match err {
                encoding::SliverRecoveryError::BlobSizeTooLarge(_) => {
                    panic!("blob size from verified metadata should not be too large")
                }
                encoding::SliverRecoveryError::DecodingFailure => {
                    tracing::debug!("unable to decode with collected symbols");
                    None
                }
            },
            Err(SliverRecoveryOrVerificationError::VerificationError(err)) => match err {
                SliverVerificationError::IndexTooLarge => {
                    panic!("checked above by pre-condition")
                }
                SliverVerificationError::SliverSizeMismatch
                | SliverVerificationError::SymbolSizeMismatch => panic!(
                    "should not occur since symbols were verified and sliver constructed here"
                ),
                SliverVerificationError::MerkleRootMismatch => {
                    panic!("should have been converted to an inconsistency proof")
                }
                SliverVerificationError::RecoveryFailed(_) => todo!("what generates this?"),
            },
        }
    }
}

pub(super) struct RecoverSliver<'a, T> {
    metadata: Arc<VerifiedBlobMetadataWithId>,
    target_index: SliverIndex,
    target_sliver_type: SliverType,
    epoch_certified: Epoch,
    backoff: ExponentialBackoffState,
    shared: &'a NodeCommitteeServiceInner<T>,
}

impl<'a, T> RecoverSliver<'a, T>
where
    T: NodeService,
{
    pub fn new(
        metadata: Arc<VerifiedBlobMetadataWithId>,
        sliver_id: SliverPairIndex,
        target_sliver_type: SliverType,
        epoch_certified: Epoch,
        shared: &'a NodeCommitteeServiceInner<T>,
    ) -> Self {
        Self {
            target_index: match target_sliver_type {
                SliverType::Primary => sliver_id.to_sliver_index::<Primary>(metadata.n_shards()),
                SliverType::Secondary => {
                    sliver_id.to_sliver_index::<Secondary>(metadata.n_shards())
                }
            },
            target_sliver_type,
            epoch_certified,
            backoff: ExponentialBackoffState::new_infinite(
                shared.config.retry_interval_min,
                shared.config.retry_interval_max,
            ),
            shared,
            metadata,
        }
    }

    pub async fn run(mut self) -> Result<Sliver, InconsistencyProofEnum> {
        tracing::trace!(
            sliver_type = %self.target_sliver_type,
            sliver_index = %self.target_index,
            "starting recovery for sliver"
        );

        // Since recovery currently consumes the symbols, rather than copy the symbols in every
        // case to handle the rare cases when we fail to *decode* the sliver despite collecting the
        // required number of symbols, we instead retry the entire process with an increased amount.
        let mut additional_symbols = 0;
        loop {
            if let Some(result) = self
                .recover_with_additional_symbols(additional_symbols)
                .await
            {
                return result;
            }
            additional_symbols += 1;
        }
    }

    #[tracing::instrument(skip(self))]
    async fn recover_with_additional_symbols(
        &mut self,
        additional_symbols: usize,
    ) -> Option<Result<Sliver, InconsistencyProofEnum>> {
        let mut committee_listener = self.shared.subscribe_to_committee_changes();
        let mut symbol_tracker = SymbolTracker::new(
            self.total_symbols_required(additional_symbols),
            self.target_index,
            self.target_sliver_type,
        );

        loop {
            let weak_committee = {
                let committee_tracker = committee_listener.borrow_and_update();
                Arc::downgrade(
                    committee_tracker
                        .committees()
                        .read_committee(self.epoch_certified)
                        .expect("epoch must not be in the future"),
                )
            };

            let epoch_certified = self.epoch_certified;
            let worker = CollectRecoverySymbols::new(
                self.metadata.clone(),
                &mut symbol_tracker,
                weak_committee.clone(),
                self.shared,
            );

            tokio::select! {
                result = worker.run() => {
                    match result {
                        Ok(n_symbols) => {
                            tracing::trace!(
                                %n_symbols,
                                "successfully collected the desired number of recovery symbols"
                            );
                            return self.decode_sliver(symbol_tracker);
                        },
                        Err(n_symbols_remaining) => {
                            tracing::trace!(
                                %n_symbols_remaining,
                                "failed to collect sufficient recovery symbols"
                            );
                            wait_before_next_attempts(&mut self.backoff, &self.shared.rng).await;
                        }
                    }
                }
                () = wait_for_read_committee_change(
                    epoch_certified,
                    &mut committee_listener,
                    &weak_committee,
                    |lhs, rhs| lhs == rhs
                ) => {
                    tracing::debug!(
                        "read committee has changed, recreating recovery symbol requests"
                    );
                }
            };
        }
    }

    fn total_symbols_required(&self, additional_symbols: usize) -> usize {
        let min_symbols_for_recovery = if self.target_sliver_type == SliverType::Primary {
            encoding::min_symbols_for_recovery::<Primary>
        } else {
            encoding::min_symbols_for_recovery::<Secondary>
        };
        usize::from(min_symbols_for_recovery(self.metadata.n_shards())) + additional_symbols
    }

    #[tracing::instrument(skip_all)]
    fn decode_sliver(
        &mut self,
        tracker: SymbolTracker,
    ) -> Option<Result<Sliver, InconsistencyProofEnum>> {
        if self.target_sliver_type == SliverType::Primary {
            self.decode_sliver_by_axis::<Primary, _>(tracker.into_symbols())
        } else {
            self.decode_sliver_by_axis::<Secondary, _>(tracker.into_symbols())
        }
    }

    fn decode_sliver_by_axis<A: EncodingAxis, I>(
        &self,
        recovery_symbols: I,
    ) -> Option<Result<Sliver, InconsistencyProofEnum>>
    where
        I: IntoIterator<Item = RecoverySymbolData<A, MerkleProof>>,
        SliverData<A>: Into<Sliver>,
        InconsistencyProof<A, MerkleProof>: Into<InconsistencyProofEnum>,
    {
        tracing::debug!("beginning to decode recovered sliver");
        let result = SliverData::<A>::recover_sliver_or_generate_inconsistency_proof(
            recovery_symbols,
            self.target_index,
            (*self.metadata).as_ref(),
            &self.shared.encoding_config,
        );
        tracing::debug!("completing decoding, parsing result");

        match result {
            Ok(SliverOrInconsistencyProof::Sliver(sliver)) => {
                tracing::debug!("successfully recovered sliver");
                Some(Ok(sliver.into()))
            }
            Ok(SliverOrInconsistencyProof::InconsistencyProof(proof)) => {
                tracing::debug!("resulted in an inconsistency proof");
                Some(Err(proof.into()))
            }
            Err(SliverRecoveryOrVerificationError::RecoveryError(err)) => match err {
                encoding::SliverRecoveryError::BlobSizeTooLarge(_) => {
                    panic!("blob size from verified metadata should not be too large")
                }
                encoding::SliverRecoveryError::DecodingFailure => {
                    tracing::debug!("unable to decode with collected symbols");
                    None
                }
            },
            Err(SliverRecoveryOrVerificationError::VerificationError(err)) => match err {
                SliverVerificationError::IndexTooLarge => {
                    panic!("checked above by pre-condition")
                }
                SliverVerificationError::SliverSizeMismatch
                | SliverVerificationError::SymbolSizeMismatch => panic!(
                    "should not occur since symbols were verified and sliver constructed here"
                ),
                SliverVerificationError::MerkleRootMismatch => {
                    panic!("should have been converted to an inconsistency proof")
                }
                SliverVerificationError::RecoveryFailed(_) => todo!("what generates this?"),
            },
        }
    }
}

struct CollectRecoverySymbols<'a, T> {
    tracker: &'a mut SymbolTracker,
    metadata: Arc<VerifiedBlobMetadataWithId>,
    committee: Weak<Committee>,
    shared: &'a NodeCommitteeServiceInner<T>,
    upcoming_nodes: RemainingShards,
    pending_requests: FuturesUnordered<BoxFuture<'a, (usize, Option<Vec<GeneralRecoverySymbol>>)>>,
}

impl<'a, T: NodeService> CollectRecoverySymbols<'a, T> {
    fn new(
        metadata: Arc<VerifiedBlobMetadataWithId>,
        tracker: &'a mut SymbolTracker,
        committee: Weak<Committee>,
        shared: &'a NodeCommitteeServiceInner<T>,
    ) -> Self {
        // Clear counts of in-progress collections.
        tracker.clear_in_progress();

        let upcoming_nodes = if let Some(committee) = committee.upgrade() {
            let mut rng_guard = shared.rng.lock().expect("mutex not poisoned");
            RemainingShards::new(&committee, &mut rng_guard)
        } else {
            RemainingShards::default()
        };

        Self {
            committee,
            metadata,
            pending_requests: Default::default(),
            shared,
            tracker,
            upcoming_nodes,
        }
    }

    async fn run(mut self) -> Result<usize, usize> {
        self.refill_pending_requests();

        while let Some((symbol_count, maybe_symbols)) = self.pending_requests.next().await {
            self.tracker.decrease_pending(symbol_count);

            if let Some(symbols) = maybe_symbols {
                self.tracker.extend_collected(symbols);
            }

            // The request submitted with some or all of the requested symbols, or it failed
            // completely. In both cases, we need to replenish the requests as the number requested
            // is potentially not equal to the number returned.
            self.refill_pending_requests();
        }

        debug_assert!(self.pending_requests.is_empty());

        if self.tracker.is_done() {
            Ok(self.tracker.collected_count())
        } else {
            Err(self.tracker.remaining_count())
        }
    }

    fn refill_pending_requests(&mut self) {
        let mut new_request_count = 0;
        let Some(committee) = self.committee.upgrade() else {
            tracing::trace!("committee has been dropped, skipping refill");
            return;
        };

        while let Some((node_index, shard_ids)) = self
            .upcoming_nodes
            .take_at_most(self.tracker.number_of_symbols_to_request(), &committee)
        {
            let _span_guard =
                tracing::trace_span!("refill_pending_requests", node_index = node_index).entered();

            let node_info = &committee.members()[node_index];
            tracing::trace!(
                ?shard_ids,
                "selected node and shards to request symbols from"
            );

            let Some(client) = self.shared.get_node_service_by_id(&node_info.public_key) else {
                tracing::trace!("unable to get the client: creation failed or epoch is changing");
                continue;
            };

            let symbols_to_request: Vec<_> = shard_ids
                .iter()
                .filter_map(|shard_id| {
                    let symbol_id = self.symbol_id_at_shard(*shard_id);

                    if self.tracker.is_collected(symbol_id) {
                        tracing::trace!(
                            %shard_id,
                            %symbol_id,
                            "skipping symbol from shard as it is already collected"
                        );
                        return None;
                    }
                    Some(symbol_id)
                })
                .collect();
            let symbols_count = symbols_to_request.len();

            if symbols_to_request.is_empty() {
                tracing::trace!("symbols in batch were all collected, skipping");
                continue;
            }

            let filter = if symbols_to_request.len() == node_info.shard_ids.len() {
                RecoverySymbolsFilter::for_sliver(self.target_index(), self.target_sliver_type())
            } else {
                RecoverySymbolsFilter::ids(symbols_to_request, self.target_sliver_type())
            };

            let request = Request::ListVerifiedRecoverySymbols {
                filter,
                metadata: self.metadata.clone(),
                target_index: self.target_index(),
                target_type: self.target_sliver_type(),
            };

            let request = time::timeout(
                self.shared.config.sliver_request_timeout,
                client.oneshot(request).map_ok(|symbol| symbol.into_value()),
            )
            .map(log_and_discard_timeout_or_error)
            .map(move |symbol| (symbols_count, symbol))
            .boxed();

            self.pending_requests.push(request);
            self.tracker.increase_pending(symbols_count);

            new_request_count += 1;
        }

        tracing::trace!(
            new_request_count,
            "completed refilling pending requests with additional futures"
        );
    }

    fn symbol_id_at_shard(&self, shard_id: ShardIndex) -> SymbolId {
        let n_shards = self.metadata.n_shards();
        let pair_at_shard = shard_id.to_pair_index(n_shards, self.blob_id());
        match self.target_sliver_type() {
            SliverType::Primary => SymbolId::new(
                self.target_index(),
                pair_at_shard.to_sliver_index::<Secondary>(n_shards),
            ),
            SliverType::Secondary => SymbolId::new(
                pair_at_shard.to_sliver_index::<Primary>(n_shards),
                self.target_index(),
            ),
        }
    }

    fn blob_id(&self) -> &BlobId {
        self.metadata.blob_id()
    }

    fn target_sliver_type(&self) -> SliverType {
        self.tracker.target_sliver_type
    }

    fn target_index(&self) -> SliverIndex {
        self.tracker.target_index
    }
}

/// Tracks the collection of recovery symbols.
#[derive(Debug, Clone)]
struct SymbolTracker {
    // Since all the symbols will be from a common axis, we do not need to store the full `SymbolId`
    // as the key, so use just the sliver index of the orthogonal axis. The enclosing struct handles
    // conversions to `SymbolId`.
    collected: HashMap<SliverIndex, GeneralRecoverySymbol>,
    symbols_in_progress_count: usize,
    symbols_still_required_count: usize,
    target_index: SliverIndex,
    target_sliver_type: SliverType,
}

impl SymbolTracker {
    /// Creates a new, empty instance of the symbol tracker to track the collection of
    /// the specified number of symbols.
    fn new(
        n_symbols_required: usize,
        target_index: SliverIndex,
        target_sliver_type: SliverType,
    ) -> Self {
        Self {
            symbols_still_required_count: n_symbols_required,
            symbols_in_progress_count: 0,
            target_sliver_type,
            target_index,
            collected: Default::default(),
        }
    }

    /// Returns the number of symbols that still need to be requested.
    ///
    /// This excludes the number of symbols that have been requested but are pending completion.
    fn number_of_symbols_to_request(&self) -> usize {
        self.symbols_still_required_count
            .saturating_sub(self.symbols_in_progress_count)
    }

    /// Returns true if the identified symbol has already been collected.
    fn is_collected(&self, symbol_id: SymbolId) -> bool {
        self.collected
            .contains_key(&self.symbol_id_to_key(symbol_id))
    }

    /// Increase the number of symbols in progress.
    fn increase_pending(&mut self, symbol_count: usize) {
        self.symbols_in_progress_count += symbol_count;
    }

    /// Decreases the number of symbols in progress.
    ///
    /// Must match a previous call to `increase_pending` so that the number of symbols in progress
    /// does not underflow.
    fn decrease_pending(&mut self, symbol_count: usize) {
        self.symbols_in_progress_count -= symbol_count;
    }

    /// The total number of symbols collected.
    fn collected_count(&self) -> usize {
        self.collected.len()
    }

    /// The total number of symbols collected.
    fn remaining_count(&self) -> usize {
        self.symbols_still_required_count
    }

    /// Returns true if the sufficient symbols have been collected, false otherwise.
    fn is_done(&self) -> bool {
        self.symbols_still_required_count == 0
    }

    /// Store the collected symbols and decrease the number of required symbols.
    fn extend_collected(&mut self, symbols: Vec<GeneralRecoverySymbol>) {
        for symbol in symbols.into_iter() {
            let key = self.symbol_id_to_key(symbol.id());
            // Only decrement the number of symbols required if an equivalent symbol wasnt present.
            if self.collected.insert(key, symbol).is_none() {
                // This holds the potential to underflow because we accept all valid symbols
                // returned by storage nodes, which may be more symbols than initially requested.
                // This can occur, for example, due to the remote node advancing an epoch and
                // responding to the request using their new shard assignment.
                self.symbols_still_required_count =
                    self.symbols_still_required_count.saturating_sub(1);
            }
        }
    }

    fn symbol_id_to_key(&self, symbol_id: SymbolId) -> SliverIndex {
        symbol_id.sliver_index(self.target_sliver_type.orthogonal())
    }

    fn clear_in_progress(&mut self) {
        self.symbols_in_progress_count = 0;
    }

    /// Convert the tracker into the collected symbols of the specified type.
    ///
    /// The stored symbols must be of the required typed.
    // TODO(jsmith): Remove once inconsistency proofs are updated to use the new recovery symbol.
    //
    // Inconsistency proofs have not yet been updated, so for now, simply use only recovery symbols
    // of a single type. This is ensured by the filter argument when requesting symbols as well as
    // in the client when receiving the results.
    fn into_symbols<A: EncodingAxis>(
        self,
    ) -> impl Iterator<Item = RecoverySymbolData<A, MerkleProof>>
    where
        RecoverySymbol<MerkleProof>: TryInto<RecoverySymbolData<A, MerkleProof>>,
    {
        self.collected.into_values().map(|symbol| {
            let Ok(symbol) = RecoverySymbol::from(symbol).try_into() else {
                panic!("symbols must be checked against filter in API call")
            };
            symbol
        })
    }
}

/// Track the remaining shards to be queried, grouped by storage node.
///
/// Since we request multiple symbols from each storage node, track the remaining set of storage
/// nodes from the committee, as well as their shards from which we have not yet requested symbols.
///
/// Storage nodes are returned in a random order, with the chance of a node appearing earlier being
/// proportional to the number of shards that it has.
#[derive(Debug, Clone, Default)]
struct RemainingShards {
    // Store the node indices as u16 instead of usize to save on space.
    upcoming_nodes: VecDeque<u16>,
    next_shard_index: usize,
}

impl RemainingShards {
    fn new(committee: &Committee, rng: &mut StdRng) -> Self {
        let n_members = u16::try_from(committee.n_members()).expect("at most 65k members");

        let node_indices: Vec<u16> = (0..n_members).collect();
        let upcoming_nodes = node_indices
            .choose_multiple_weighted(rng, committee.n_members(), |node_index| {
                let n_shards = committee.members()[usize::from(*node_index)]
                    .shard_ids
                    .len();
                u16::try_from(n_shards).expect("number of shards fits within u16")
            })
            .expect("u16 weights are valid")
            .copied()
            .collect();

        Self {
            upcoming_nodes,
            next_shard_index: 0,
        }
    }

    /// Returns the index of the next storage node to be queried, and at most `limit` shards from
    /// that storage node.
    fn take_at_most<'a>(
        &mut self,
        limit: usize,
        committee: &'a Committee,
    ) -> Option<(usize, &'a [ShardIndex])> {
        if limit == 0 {
            tracing::trace!("requested no shards, returning None");
            return None;
        }

        let next_node_index = self.upcoming_nodes.pop_front()?;
        let node_info = &committee.members()[usize::from(next_node_index)];
        tracing::trace!(limit, next_node_index, "taking shards from the next node");

        let range_start = self.next_shard_index;
        let range_end = std::cmp::min(range_start + limit, node_info.shard_ids.len());
        let shards = &node_info.shard_ids[range_start..range_end];

        if range_end != node_info.shard_ids.len() {
            // If the node has more shards, we can push it back onto the set of remaining nodes.
            tracing::trace!(
                n_remaining = node_info.shard_ids.len() - range_end,
                "the next node has shards remaining, replacing on queue"
            );
            self.upcoming_nodes.push_front(next_node_index);
            self.next_shard_index = range_end;
        } else {
            tracing::trace!("the next node has no shards remaining, moving on to next node");
            // Otherwise, reset the next_shard_index for the next node.
            self.next_shard_index = 0;
        }

        Some((usize::from(next_node_index), shards))
    }
}

pub(super) struct GetInvalidBlobCertificate<'a, T> {
    blob_id: BlobId,
    inconsistency_proof: &'a InconsistencyProofEnum,
    shared: &'a NodeCommitteeServiceInner<T>,
}

impl<'a, T> GetInvalidBlobCertificate<'a, T>
where
    T: NodeService,
{
    pub fn new(
        blob_id: BlobId,
        inconsistency_proof: &'a InconsistencyProofEnum,
        shared: &'a NodeCommitteeServiceInner<T>,
    ) -> Self {
        Self {
            blob_id,
            inconsistency_proof,
            shared,
        }
    }
    pub async fn run(mut self) -> InvalidBlobCertificate {
        let mut committee_listener = self.shared.subscribe_to_committee_changes();

        loop {
            let committee = committee_listener
                .borrow_and_update()
                .committees()
                .write_committee()
                .clone();

            tokio::select! {
                certificate = self.get_certificate_from_committee(committee.clone()) => {
                    return certificate;
                }
                () = wait_for_write_committee_change(
                    &mut committee_listener,
                    &committee,
                    // All signatures must be within the same epoch, most recent epoch.
                    |committee, other| committee.epoch == other.epoch
                ) => {
                    tracing::debug!(
                        "read committee has changed, restarting attempt at collecting signatures"
                    );
                }
            };
        }
    }

    async fn get_certificate_from_committee(
        &mut self,
        committee: Arc<Committee>,
    ) -> InvalidBlobCertificate {
        tracing::debug!(
            walrus.epoch = committee.epoch,
            "requesting certificate from the epoch's committee"
        );
        let mut collected_signatures = HashMap::new();
        let mut backoff = ExponentialBackoffState::new_infinite(
            self.shared.config.retry_interval_min,
            self.shared.config.retry_interval_max,
        );
        let mut node_order: Vec<_> = (0..committee.n_members()).collect();

        // Sort the nodes by their weight in the committee. This has the benefit of requiring the
        // least number of signatures for the certificate.
        node_order.sort_unstable_by_key(|&index| {
            cmp::Reverse(committee.members()[index].shard_ids.len())
        });

        loop {
            match PendingInvalidBlobAttestations::new(
                self.blob_id,
                self.inconsistency_proof,
                node_order.iter(),
                collected_signatures,
                committee.clone(),
                self.shared,
            )
            .await
            {
                Ok(fully_collected) => {
                    return Self::create_certificate(fully_collected);
                }
                Err(partially_collected) => {
                    collected_signatures = partially_collected;
                    wait_before_next_attempts(&mut backoff, &self.shared.rng).await;
                }
            }
        }
    }

    #[tracing::instrument(skip_all)]
    fn create_certificate(
        collected_signatures: HashMap<usize, InvalidBlobIdAttestation>,
    ) -> InvalidBlobCertificate {
        tracing::info!("extracting signers and messages");
        let (signer_indices, signed_messages): (Vec<_>, Vec<_>) = collected_signatures
            .into_iter()
            .map(|(index, message)| {
                let index = u16::try_from(index).expect("node indices are within u16");
                (index, message)
            })
            .unzip();

        tracing::info!(
            symbol_count = signed_messages.len(),
            "creating invalid blob certificate"
        );
        match InvalidBlobCertificate::from_signed_messages_and_indices(
            signed_messages,
            signer_indices,
        ) {
            Ok(certificate) => {
                tracing::info!("successfully created invalid blob certificate");
                certificate
            }
            Err(CertificateError::SignatureAggregation(err)) => {
                panic!("attestations must be verified beforehand: {:?}", err)
            }
            Err(CertificateError::MessageMismatch) => {
                panic!("messages must be verified against the same epoch and blob id")
            }
        }
    }
}

type RequestWeight = u16;
type NodeIndexInCommittee = usize;
type AttestationWithWeight = (
    NodeIndexInCommittee,
    RequestWeight,
    Option<InvalidBlobIdAttestation>,
);
type StoredFuture<'a> = BoxFuture<'a, AttestationWithWeight>;

#[pin_project::pin_project]
struct PendingInvalidBlobAttestations<'fut, 'iter, T> {
    /// The ID of the invalid blob.
    blob_id: BlobId,
    /// Proof of the blob's inconsistency.
    inconsistency_proof: &'fut InconsistencyProofEnum,
    /// The current committee from which attestations are requested.
    committee: Arc<Committee>,
    /// Shared state across futures.
    shared: &'fut NodeCommitteeServiceInner<T>,

    /// The remaining nodes over which to iterate.
    nodes: std::slice::Iter<'iter, usize>,
    /// The weight required before completion.
    required_weight: u16,
    /// The weight currently pending in requests.
    pending_weight: u16,
    /// Collected attestations.
    // INV: Only None once the future has completed.
    collected_signatures: Option<HashMap<usize, InvalidBlobIdAttestation>>,

    #[pin]
    pending_requests: FuturesUnordered<StoredFuture<'fut>>,
}

#[allow(clippy::needless_lifetimes)] // be consistent with other functions in this file
impl<'fut, 'iter, T> std::future::Future for PendingInvalidBlobAttestations<'fut, 'iter, T>
where
    T: NodeService + 'fut,
{
    type Output =
        Result<HashMap<usize, InvalidBlobIdAttestation>, HashMap<usize, InvalidBlobIdAttestation>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            let mut this = self.as_mut().project();
            assert!(
                this.collected_signatures.is_some(),
                "future must not be polled after completion"
            );

            let Some((index, weight, maybe_attestation)) =
                ready!(this.pending_requests.as_mut().poll_next(cx))
            else {
                // The pending requests stream ended. Since we fill it on creation and after each
                // failure, either (i) the user has polled a completed future, or (ii) the node
                // iterator has finished. Case (i) is handled above, so this must be case (ii).
                // This implies, however, that we could not reach our target.
                debug_assert_eq!(this.nodes.len(), 0);
                debug_assert!(*this.required_weight > 0);
                let progress = this
                    .collected_signatures
                    .take()
                    .expect("is Some until future complete");
                return Poll::Ready(Err(progress));
            };

            // Decrement the amount of weight pending.
            *this.pending_weight -= weight;

            if let Some(attestation) = maybe_attestation {
                // The request yielded an attestation from the storage node. We can store it and
                // reduce the amount of weight required until completion.
                this.collected_signatures
                    .as_mut()
                    .expect("is Some until future complete")
                    .insert(index, attestation);
                *this.required_weight = this.required_weight.saturating_sub(weight);

                if *this.required_weight == 0 {
                    let collected_signatures = this
                        .collected_signatures
                        .take()
                        .expect("not yet complete and so non-None");
                    return Poll::Ready(Ok(collected_signatures));
                }
            } else {
                // The request failed, we need to replenish weight such that we remain on-track to
                // collecting sufficient weight. We then continue to loop since any added futures
                // may already be ready, or none may have been added and we're done.
                self.as_mut().get_mut().refill_pending_requests();
            }
        }
    }
}

impl<'fut, 'iter, T> PendingInvalidBlobAttestations<'fut, 'iter, T>
where
    T: NodeService + 'fut,
{
    /// The remaining nodes over which to iterate.
    fn new(
        blob_id: BlobId,
        inconsistency_proof: &'fut InconsistencyProofEnum,
        nodes: std::slice::Iter<'iter, usize>,
        collected_signatures: HashMap<usize, InvalidBlobIdAttestation>,
        committee: Arc<Committee>,
        shared: &'fut NodeCommitteeServiceInner<T>,
    ) -> Self {
        let mut this = Self {
            blob_id,
            inconsistency_proof,
            shared,
            nodes,
            required_weight: bft::min_n_correct(committee.n_shards()).get(),
            committee,
            pending_weight: 0,
            pending_requests: Default::default(),
            collected_signatures: Some(collected_signatures),
        };
        this.refill_pending_requests();
        this
    }

    fn refill_pending_requests(&mut self) {
        while self.pending_weight < self.required_weight {
            let Some((weight, request)) = self.next_request() else {
                tracing::trace!("no more requests to dispatch");
                break;
            };
            self.pending_requests.push(request);
            self.pending_weight += weight;
        }
    }

    fn next_request(&mut self) -> Option<(RequestWeight, StoredFuture<'fut>)> {
        for &index in self.nodes.by_ref() {
            let committee_epoch = self.committee.epoch;
            let node_info = &self.committee.members()[index];
            let collected_signatures = self
                .collected_signatures
                .as_ref()
                .expect("cannot be called after future completes");

            if collected_signatures.contains_key(&index) {
                tracing::trace!("attestation already collected for node, skipping");
                continue;
            }

            let Some(client) = self.shared.get_node_service_by_id(&node_info.public_key) else {
                tracing::trace!(
                    "unable to get the client, either creation failed or epoch is changing"
                );
                continue;
            };

            let weight =
                u16::try_from(node_info.shard_ids.len()).expect("shard weight fits within u16");
            let request = client
                .oneshot(Request::SubmitProofForInvalidBlobAttestation {
                    blob_id: self.blob_id,
                    // TODO(jsmith): Accept the proof directly from the caller.
                    proof: self.inconsistency_proof.clone(),
                    epoch: committee_epoch,
                    public_key: node_info.public_key.clone(),
                })
                .map_ok(Response::into_value);

            let request = time::timeout(self.shared.config.invalidity_sync_timeout, request)
                .map(move |output| (index, weight, log_and_discard_timeout_or_error(output)))
                .instrument(tracing::info_span!(
                    "get_invalid_blob_certificate node",
                    walrus.node.public_key = %node_info.public_key
                ));

            tracing::trace!("created attestation request for node");
            return Some((weight, request.boxed()));
        }
        None
    }
}

fn iterate_symbols<A: EncodingAxis>(
    iterator: IntoValues<ShardIndex, RecoverySymbol<MerkleProof>>,
) -> impl Iterator<Item = RecoverySymbolData<A, MerkleProof>>
where
    RecoverySymbol<MerkleProof>: TryInto<RecoverySymbolData<A, MerkleProof>>,
    <RecoverySymbol<MerkleProof> as TryInto<RecoverySymbolData<A, MerkleProof>>>::Error:
        std::fmt::Debug,
{
    iterator.map(|symbol| symbol.try_into().expect("symbols are checked in API"))
}

/// Returns true if we would expect the client used to communicate with each storage node to remain
/// the same under each committee.
///
/// They are equivalent if the public keys, network public keys, network addresses are the same.
/// Their shard assignments are allowed to differ.
fn are_storage_node_addresses_equivalent(committee: &Committee, other: &Committee) -> bool {
    if committee.n_members() != other.n_members() {
        return false;
    }

    for member in committee.members() {
        let Some(other_member) = other.find(&member.node_id) else {
            return false;
        };
        if member.network_address != other_member.network_address
            || member.network_public_key != other_member.network_public_key
        {
            return false;
        }
    }
    true
}

/// Returns true if we would expect the client used to communicate with each shard to remain
/// the same under each committee.
fn are_shard_addresses_equivalent(committee: &Committee, other: &Committee) -> bool {
    if committee.n_members() != other.n_members() {
        return false;
    }

    for member in committee.members() {
        let Some(other_member) = other.find(&member.node_id) else {
            return false;
        };
        if member != other_member {
            return false;
        }
    }
    true
}

async fn wait_before_next_attempts(backoff: &mut ExponentialBackoffState, rng: &SyncMutex<StdRng>) {
    let delay = backoff
        .next_delay(&mut *rng.lock().expect("mutex is not poisoned"))
        .expect("infinite strategy");
    tracing::debug!(?delay, "sleeping before next attempts");
    tokio::time::sleep(delay).await;
}

async fn wait_for_read_committee_change<F>(
    epoch_certified: Epoch,
    listener: &mut watch::Receiver<CommitteeTracker>,
    current_committee: &Weak<Committee>,
    are_committees_equivalent: F,
) where
    F: Fn(&Committee, &Committee) -> bool,
{
    loop {
        listener.changed().await.expect("sender outlives futures");
        tracing::debug!("the active committees have changed during the request");

        let committee_tracker = listener.borrow_and_update();
        let new_read_committee = committee_tracker
            .committees()
            .read_committee(epoch_certified)
            .expect("exists since new committees handle all lower epochs");

        let Some(previous_committee) = current_committee.upgrade() else {
            tracing::debug!("the previous committee has been dropped and so is no longer valid");
            return;
        };

        if !are_committees_equivalent(new_read_committee, &previous_committee) {
            tracing::debug!(
                walrus.epoch = epoch_certified,
                "the read committee has changed for the request"
            );
            return;
        }
        tracing::trace!("the committees are equivalent, continuing to await changes");
    }
}

async fn wait_for_write_committee_change<F>(
    listener: &mut watch::Receiver<CommitteeTracker>,
    current_committee: &Arc<Committee>,
    are_committees_equivalent: F,
) where
    F: Fn(&Committee, &Committee) -> bool,
{
    loop {
        listener.changed().await.expect("sender outlives futures");
        tracing::debug!("the active committees have changed during the request");

        let committee_tracker = listener.borrow_and_update();
        let new_write_committee = committee_tracker.committees().write_committee();

        if !are_committees_equivalent(new_write_committee, current_committee) {
            tracing::debug!("the write committee has changed for the request");
            return;
        }
        tracing::trace!("the committees are equivalent, continuing to await changes");
    }
}

fn log_and_discard_timeout_or_error<T>(
    result: Result<Result<T, NodeServiceError>, Elapsed>,
) -> Option<T> {
    match result {
        Ok(Ok(value)) => {
            tracing::trace!("future completed successfully");
            return Some(value);
        }
        Ok(Err(error)) => tracing::debug!(%error),
        Err(error) => tracing::debug!(%error),
    }
    None
}

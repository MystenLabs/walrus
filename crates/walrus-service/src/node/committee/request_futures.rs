// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{
    cmp,
    collections::{HashMap, VecDeque},
    num::NonZero,
    pin::Pin,
    sync::{Arc, Mutex as SyncMutex, Weak},
    task::{Context, Poll, ready},
};

use ::futures::{FutureExt as _, StreamExt as _, stream};
use futures::{Stream as _, TryFutureExt as _, future::BoxFuture, stream::FuturesUnordered};
use rand::{rngs::StdRng, seq::SliceRandom as _};
use tokio::{
    sync::watch,
    time::{self, error::Elapsed},
};
use tower::ServiceExt as _;
use tracing::Instrument as _;
use walrus_core::{
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
};
use walrus_sdk::active_committees::CommitteeTracker;
use walrus_storage_node_client::RecoverySymbolsFilter;
use walrus_sui::types::Committee;
use walrus_utils::{backoff::ExponentialBackoffState, metrics::OwnedGaugeGuard};

use super::{
    committee_service::NodeCommitteeServiceInner,
    node_service::{NodeService, NodeServiceError, Request, Response},
};
use crate::node::metrics::CommitteeServiceMetricSet;

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
            let Some(client) = check_ready(client) else {
                tracing::trace!("skipping unready client");
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecoveryStateLabel {
    Init,
    CollectingSymbols,
    Backoff,
    /// The tail number of remaining requests, min 1, max 5.
    TailRequest(NonZero<u8>),
    BuildingSliver,
}

impl AsRef<str> for RecoveryStateLabel {
    fn as_ref(&self) -> &str {
        match self {
            Self::Init => "init",
            Self::CollectingSymbols => "collecting-symbols",
            Self::Backoff => "backoff",
            Self::TailRequest(_) => "tail-request",
            Self::BuildingSliver => "building-sliver",
        }
    }
}

/// Tracks aggregated statistics and the state of a RecoverSliver future.
struct RecoverSliverStats {
    /// The total number of backoffs performed during the recovery.
    total_backoffs: usize,
    /// The total number of failed requests observed during the recovery.
    total_failed_requests: usize,
    /// The current "recovery state" of the request, as identified by the `RecoveryStateLabel`.
    current_state: (RecoveryStateLabel, OwnedGaugeGuard),

    metrics: Arc<CommitteeServiceMetricSet>,
}

impl RecoverSliverStats {
    fn new(metrics: Arc<CommitteeServiceMetricSet>) -> Self {
        let current_state = (
            RecoveryStateLabel::Init,
            OwnedGaugeGuard::acquire(walrus_utils::with_label!(
                metrics.recovery_future_state,
                RecoveryStateLabel::Init,
                ""
            )),
        );

        Self {
            total_backoffs: 0,
            total_failed_requests: 0,
            current_state,
            metrics,
        }
    }

    fn record_state(&mut self, state: RecoveryStateLabel) {
        if self.current_state.0 == state {
            return;
        }

        let metric = if let RecoveryStateLabel::TailRequest(i) = state {
            walrus_utils::with_label!(self.metrics.recovery_future_state, state, &i.to_string())
        } else {
            walrus_utils::with_label!(self.metrics.recovery_future_state, state, "")
        };
        self.current_state = (state, OwnedGaugeGuard::acquire(metric));
    }
}

pub(super) struct RecoverSliver<'a, T> {
    metadata: Arc<VerifiedBlobMetadataWithId>,
    target_index: SliverIndex,
    target_sliver_type: SliverType,
    epoch_certified: Epoch,
    backoff: ExponentialBackoffState,
    shared: &'a NodeCommitteeServiceInner<T>,
    stats: RecoverSliverStats,
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
            stats: RecoverSliverStats::new(shared.metrics.clone()),
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

        // Total symbols required to decode the sliver.
        let total_symbols_required = self.total_symbols_required(additional_symbols);

        // Total symbols to request from other storage nodes initially.
        let total_symbols_to_request = std::cmp::min(
            total_symbols_required
                + self
                    .shared
                    .config
                    .experimental_sliver_recovery_additional_symbols,
            self.metadata.n_shards().get().into(),
        );

        // Track the collection of recovery symbols.
        let mut symbol_tracker = SymbolTracker::new(
            total_symbols_required,
            total_symbols_to_request,
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
                &mut self.stats,
            );

            tokio::select! {
                result = worker.run() => {
                    match result {
                        Ok(n_symbols) => {
                            tracing::trace!(
                                %n_symbols,
                                "successfully collected the desired number of recovery symbols"
                            );
                            self.stats.metrics.recovery_future_backoffs
                                .observe(self.stats.total_backoffs as f64);
                            self.stats.metrics.recovery_future_failed_requests
                                .observe(self.stats.total_failed_requests as f64);
                            self.stats.record_state(RecoveryStateLabel::BuildingSliver);

                            return self.decode_sliver(symbol_tracker).await;
                        },
                        Err(n_symbols_remaining) => {
                            tracing::trace!(
                                %n_symbols_remaining,
                                "failed to collect sufficient recovery symbols"
                            );
                            self.stats.record_state(RecoveryStateLabel::Backoff);
                            self.stats.metrics.recovery_future_backoff_total.inc();
                            self.stats.total_backoffs += 1;

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
    async fn decode_sliver(
        &mut self,
        tracker: SymbolTracker,
    ) -> Option<Result<Sliver, InconsistencyProofEnum>> {
        if self.target_sliver_type == SliverType::Primary {
            self.decode_sliver_by_axis::<Primary, _>(tracker.into_symbols())
                .await
        } else {
            self.decode_sliver_by_axis::<Secondary, _>(tracker.into_symbols())
                .await
        }
    }

    async fn decode_sliver_by_axis<A, I>(
        &self,
        recovery_symbols: I,
    ) -> Option<Result<Sliver, InconsistencyProofEnum>>
    where
        A: EncodingAxis + Send + 'static,
        I: IntoIterator<Item = RecoverySymbolData<A, MerkleProof>> + Send + 'static,
        SliverData<A>: Into<Sliver>,
        InconsistencyProof<A, MerkleProof>: Into<InconsistencyProofEnum>,
    {
        tracing::debug!("beginning to decode recovered sliver");
        let index = self.target_index;
        let metadata = self.metadata.clone();
        let encoding_config = self.shared.encoding_config.clone();
        let result = tokio::task::spawn_blocking(move || {
            SliverData::<A>::recover_sliver_or_generate_inconsistency_proof(
                recovery_symbols,
                index,
                metadata.metadata(),
                &encoding_config,
                false,
            )
        })
        .await
        .expect("sliver recovery must not panic");
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
    stats: &'a mut RecoverSliverStats,
}

impl<'a, T: NodeService> CollectRecoverySymbols<'a, T> {
    fn new(
        metadata: Arc<VerifiedBlobMetadataWithId>,
        tracker: &'a mut SymbolTracker,
        committee: Weak<Committee>,
        shared: &'a NodeCommitteeServiceInner<T>,
        stats: &'a mut RecoverSliverStats,
    ) -> Self {
        // Clear counts of in-progress collections.
        tracker.clear_in_progress();

        let upcoming_nodes = if let Some(committee) = committee.upgrade() {
            let mut rng_guard = shared.rng.lock().expect("mutex should not be poisoned");
            RemainingShards::new(&committee, &mut rng_guard)
        } else {
            // The committee has been dropped, so there are no nodes to query,
            // this will likely be followed by a refresh of the committee.
            RemainingShards::default()
        };

        Self {
            committee,
            metadata,
            pending_requests: Default::default(),
            shared,
            tracker,
            upcoming_nodes,
            stats,
        }
    }

    async fn run(mut self) -> Result<usize, usize> {
        self.refill_pending_requests();

        while let Some((symbol_count, maybe_symbols)) = self.pending_requests.next().await {
            self.tracker.decrease_pending(symbol_count);

            if let Some(symbols) = maybe_symbols {
                self.tracker.extend_collected(symbols);
            } else {
                // No symbols, which indicates a request failure.
                self.stats.total_failed_requests += 1;
            }

            // If we have collected enough symbols to decode the sliver, we can stop.
            if self.tracker.is_done() {
                break;
            }

            // The request submitted with some or all of the requested symbols, or it failed
            // completely. In both cases, we need to replenish the requests as the number requested
            // is potentially not equal to the number returned.
            self.refill_pending_requests();
        }

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
            let Some(client) = check_ready(client) else {
                tracing::trace!("skipping unready client");
                continue;
            };

            let filter = if symbols_to_request.len() == node_info.shard_ids.len() {
                RecoverySymbolsFilter::recovers(self.target_index(), self.target_sliver_type())
            } else {
                RecoverySymbolsFilter::ids(symbols_to_request).expect("symbols list is non-empty")
            };
            let filter = filter.require_proof_from_axis(self.target_sliver_type().orthogonal());

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

        match self.pending_requests.len() {
            0 => (),
            i @ 1..=5 => self.stats.record_state(RecoveryStateLabel::TailRequest(
                NonZero::new(i.try_into().expect("the number of requests is at most 5"))
                    .expect("zero is handled above"),
            )),
            _ => self
                .stats
                .record_state(RecoveryStateLabel::CollectingSymbols),
        }
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
    /// The number of symbols to request initially. This number is at least as large as
    /// `symbols_required_to_decode_count`, with additional symbols to request to account for
    /// potential errors in the symbols received.
    symbols_desired_to_request_count: usize,
    /// The number of symbols required to decode the target sliver.
    symbols_required_to_decode_count: usize,
    target_index: SliverIndex,
    target_sliver_type: SliverType,
}

impl SymbolTracker {
    /// Creates a new, empty instance of the symbol tracker to track the collection of
    /// the specified number of symbols.
    fn new(
        n_symbols_required: usize,
        total_symbols_to_request: usize,
        target_index: SliverIndex,
        target_sliver_type: SliverType,
    ) -> Self {
        Self {
            symbols_required_to_decode_count: n_symbols_required,
            symbols_desired_to_request_count: total_symbols_to_request,
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
        self.symbols_desired_to_request_count
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

    /// The total number of symbols remaining to be collected until
    /// `symbols_desired_to_request_count` is reached.
    fn remaining_count(&self) -> usize {
        self.symbols_desired_to_request_count
    }

    /// Returns true if the sufficient symbols have been collected, false otherwise.
    fn is_done(&self) -> bool {
        self.collected.len() >= self.symbols_required_to_decode_count
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
                self.symbols_desired_to_request_count =
                    self.symbols_desired_to_request_count.saturating_sub(1);
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
    /// For the node at the head of the queue, this is the starting index into their list of shards.
    shard_id_range_start: usize,
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
            shard_id_range_start: 0,
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

        let range_start = self.shard_id_range_start;
        let range_end = std::cmp::min(range_start + limit, node_info.shard_ids.len());
        let shards = &node_info.shard_ids[range_start..range_end];

        if range_end != node_info.shard_ids.len() {
            // If the node has more shards, we can push it back onto the set of remaining nodes.
            tracing::trace!(
                n_remaining = node_info.shard_ids.len() - range_end,
                "the next node has shards remaining, replacing on queue"
            );
            self.upcoming_nodes.push_front(next_node_index);
            self.shard_id_range_start = range_end;
        } else {
            tracing::trace!("the next node has no shards remaining, moving on to next node");
            // Otherwise, reset the next_shard_index for the next node.
            self.shard_id_range_start = 0;
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
            let Some(client) = check_ready(client) else {
                tracing::trace!("skipping unready client");
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

async fn wait_before_next_attempts(backoff: &mut ExponentialBackoffState, rng: &SyncMutex<StdRng>) {
    let delay = backoff
        .next_delay(&mut *rng.lock().expect("mutex should not be poisoned"))
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

fn check_ready<T: NodeService>(mut client: T) -> Option<T> {
    match client.ready().now_or_never() {
        None => {
            tracing::trace!("client is not ready, skipping the node");
            None
        }
        Some(Err(error)) => {
            tracing::warn!(
                ?error,
                "encountered an error while checking a remote node for readiness",
            );
            None
        }
        Some(Ok(_)) => Some(client),
    }
}

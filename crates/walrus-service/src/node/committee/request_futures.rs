// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    cmp::{self},
    collections::{hash_map::IntoValues, HashMap},
    future::Future,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
        Mutex as SyncMutex,
        Weak,
    },
    vec::IntoIter,
};

use ::futures::{stream, FutureExt as _, StreamExt as _};
use futures::{stream::FuturesUnordered, TryFutureExt as _};
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
    SliverPairIndex,
    SliverType,
};
use walrus_sdk::error::NodeError;
use walrus_sui::types::Committee;

use super::{
    committee_service::NodeCommitteeServiceInner,
    node_service::{NodeService, Request, Response},
};
use crate::{node::committee::active_committees::ActiveCommittees, utils::ExponentialBackoffState};

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
                let active_committees = committee_listener.borrow_and_update();
                let committee = active_committees
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

pub(super) struct RecoverSliver<'a, T> {
    metadata: &'a VerifiedBlobMetadataWithId,
    sliver_id: SliverPairIndex,
    sliver_type: SliverType,
    epoch_certified: Epoch,
    backoff: ExponentialBackoffState,
    shared: &'a NodeCommitteeServiceInner<T>,
}

impl<'a, T> RecoverSliver<'a, T>
where
    T: NodeService,
{
    pub fn new(
        metadata: &'a VerifiedBlobMetadataWithId,
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
        // case to handle the rare cases when we fail to recovery the sliver with the default number
        // of symbols, we instead retry the entire process with an increased symbol count.
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
                let active_committees = committee_listener.borrow_and_update();
                Arc::downgrade(
                    active_committees
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
                        metadata: self.metadata,
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
                Some(request)
            })
            // Ensure that the resulting iterator will always return None when complete.
            .fuse()
    }

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
        let result = SliverData::<A>::recover_sliver_or_generate_inconsistency_proof(
            recovery_symbols,
            self.sliver_id
                .to_sliver_index::<A>(self.metadata.n_shards()),
            self.metadata.as_ref(),
            &self.shared.encoding_config,
        );

        match result {
            Ok(SliverOrInconsistencyProof::Sliver(sliver)) => {
                tracing::debug!("successfully recovered sliver");
                Some(Ok(sliver.into()))
            }
            Ok(SliverOrInconsistencyProof::InconsistencyProof(proof)) => Some(Err(proof.into())),
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
        let mut required_weight = bft::min_n_correct(committee.n_shards()).get();

        let mut collected_signatures: Vec<(usize, InvalidBlobIdAttestation)> = vec![];
        // Use a vector of AtomicBool to track whether the signature has been collected for a node.
        // This allows us to both check and update this in a loop. Other non-lock based solutions
        // trigger false-positive issues with the borrow checker.
        let mut is_collected: Vec<AtomicBool> = vec![];
        is_collected.resize_with(committee.n_members(), Default::default);

        let mut backoff = ExponentialBackoffState::new_infinite(
            self.shared.config.retry_interval_min,
            self.shared.config.retry_interval_max,
        );

        // Sort the nodes by their weight in the committee. This has the benefit of requiring the
        // least number of signatures for the certificate.
        let mut node_order: Vec<_> = (0..committee.n_members()).collect();
        node_order.sort_unstable_by_key(|&index| {
            cmp::Reverse(committee.members()[index].shard_ids.len())
        });

        loop {
            let mut requests = node_order
                .iter()
                .filter_map(|&index| {
                    let committee_epoch = committee.epoch;
                    let node_info = &committee.members()[index];

                    // Ordering::Relaxed is sufficient since this code is single threaded.
                    if is_collected[index].load(Ordering::Relaxed) {
                        tracing::trace!("signature already collected, skipping");
                        return None;
                    }

                    let Some(client) = self.shared.get_node_service_by_id(&node_info.public_key)
                    else {
                        tracing::trace!(
                            "unable to get the client, either creation failed or epoch is changing"
                        );
                        return None;
                    };

                    let node_weight = u16::try_from(node_info.shard_ids.len())
                        .expect("shard weight fits within u16");

                    let request = client
                        .oneshot(Request::SubmitProofForInvalidBlobAttestation {
                            blob_id: self.blob_id,
                            proof: self.inconsistency_proof,
                            epoch: committee_epoch,
                            public_key: node_info.public_key.clone(),
                        })
                        .map_ok(Response::into_value);
                    let request =
                        time::timeout(self.shared.config.invalidity_sync_timeout, request)
                            .map(log_and_discard_timeout_or_error)
                            .map(move |output| (index, output, node_weight))
                            .instrument(tracing::info_span!(
                                "get_invalid_blob_certificate node",
                                walrus.node.public_key = %node_info.public_key
                            ));
                    Some((request, node_weight))
                })
                .fuse();

            let mut pending_requests = FuturesUnordered::default();
            let mut pending_weight: u16 = 0;

            'refill_pending_requests: loop {
                // Fill the pending requests with requests until there is more weight pending than
                // we require for a valid certificate.
                while pending_weight < required_weight {
                    let Some((request, weight)) = requests.next() else {
                        tracing::trace!("no more requests to dispatch");
                        break;
                    };
                    pending_requests.push(request);
                    pending_weight += weight;
                }

                while let Some((index, maybe_signature, weight)) = pending_requests.next().await {
                    pending_weight -= weight;

                    // Successful responses reduce the amount of weight required. Unsuccessful
                    // results require refilling the pending requests.
                    if let Some(signature) = maybe_signature {
                        required_weight = required_weight.saturating_sub(weight);
                        collected_signatures.push((index, signature));
                        is_collected[index].store(true, Ordering::Relaxed);
                    } else {
                        continue 'refill_pending_requests;
                    }
                }

                // Reaching here means we have exhausted the pending requests, which only occurs
                // if we've achieved the required weight or consumed all of the requests (some of
                // which may have failed). In either case, we can leave this inner loop.
                debug_assert!(
                    pending_requests.is_empty()
                        && (required_weight == 0 || requests.next().is_none())
                );
                break;
            }

            if required_weight == 0 {
                return Self::create_certificate(collected_signatures);
            } else {
                wait_before_next_attempts(&mut backoff, &self.shared.rng).await;
            }
        }
    }

    fn create_certificate(
        collected_signatures: Vec<(usize, InvalidBlobIdAttestation)>,
    ) -> InvalidBlobCertificate {
        let (signer_indices, signed_messages): (Vec<_>, Vec<_>) = collected_signatures
            .into_iter()
            .map(|(index, message)| {
                let index = u16::try_from(index).expect("node indices are within u16");
                (index, message)
            })
            .unzip();

        match InvalidBlobCertificate::from_signed_messages_and_indices(
            signed_messages,
            signer_indices,
        ) {
            Ok(certificate) => certificate,
            Err(CertificateError::SignatureAggregation(err)) => {
                panic!("attestations must be verified beforehand: {:?}", err)
            }
            Err(CertificateError::MessageMismatch) => {
                panic!("messages must be verified against the same epoch and blob id")
            }
        }
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
        let Some(other_member) = other.find(&member.public_key) else {
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
        let Some(other_member) = other.find(&member.public_key) else {
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
    listener: &mut watch::Receiver<ActiveCommittees>,
    current_committee: &Weak<Committee>,
    are_committees_equivalent: F,
) where
    F: Fn(&Committee, &Committee) -> bool,
{
    loop {
        listener.changed().await.expect("sender outlives futures");
        tracing::debug!("the active committees have changed during the request");

        let active_committees = listener.borrow_and_update();
        let new_read_committee = active_committees
            .read_committee(epoch_certified)
            .expect("exists since new committees handle all lower epochs");

        let Some(prior_committee) = current_committee.upgrade() else {
            tracing::debug!("the prior committee has been dropped and so is no longer valid");
            return;
        };

        if !are_committees_equivalent(new_read_committee, &prior_committee) {
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
    listener: &mut watch::Receiver<ActiveCommittees>,
    current_committee: &Arc<Committee>,
    are_committees_equivalent: F,
) where
    F: Fn(&Committee, &Committee) -> bool,
{
    loop {
        listener.changed().await.expect("sender outlives futures");
        tracing::debug!("the active committees have changed during the request");

        let active_committees = listener.borrow_and_update();
        let new_write_committee = active_committees.write_committee();

        if !are_committees_equivalent(new_write_committee, current_committee) {
            tracing::debug!("the write committee has changed for the request");
            return;
        }
        tracing::trace!("the committees are equivalent, continuing to await changes");
    }
}

fn log_and_discard_timeout_or_error<T>(result: Result<Result<T, NodeError>, Elapsed>) -> Option<T> {
    match result {
        Ok(Ok(value)) => return Some(value),
        Ok(Err(error)) => tracing::debug!(%error),
        Err(error) => tracing::debug!(%error),
    }
    None
}

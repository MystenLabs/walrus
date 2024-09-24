// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
#![allow(clippy::mutable_key_type)]

//! Committee lookup and management.

use std::{
    collections::HashMap,
    fmt::Debug,
    num::NonZeroU16,
    sync::{Arc, Mutex as SyncMutex},
};

use futures::TryFutureExt;
use rand::{rngs::StdRng, Rng, SeedableRng};
use tokio::sync::{watch, Mutex as TokioMutex};
use tower::ServiceExt as _;
use walrus_core::{
    encoding::EncodingConfig,
    keys::ProtocolKeyPair,
    merkle::MerkleProof,
    messages::InvalidBlobCertificate,
    metadata::VerifiedBlobMetadataWithId,
    BlobId,
    Epoch,
    InconsistencyProof as InconsistencyProofEnum,
    PublicKey,
    ShardIndex,
    Sliver,
    SliverPairIndex,
    SliverType,
};
use walrus_sui::types::Committee;

use super::{
    node_service::{NodeService, NodeServiceError, RemoteStorageNode, Request, Response},
    request_futures::{GetAndVerifyMetadata, GetInvalidBlobCertificate, RecoverSliver},
    CommitteeLookupService,
    CommitteeService,
    NodeServiceFactory,
};
use crate::{
    common::active_committees::{
        BeginCommitteeChangeError,
        CommitteeTracker,
        EndCommitteeChangeError,
    },
    node::{config::CommitteeServiceConfig, errors::SyncShardClientError},
};

/// Errors returned by [`NodeCommitteeService::begin_committee_change`].
#[derive(Debug, thiserror::Error)]
pub(crate) enum CommitteeTransitionError {
    /// The tracked committees and those returned by the [`CommitteeLookupService`] are out-of-sync.
    /// It is necessary to reset the tracked committee.
    #[error(transparent)]
    OutOfSync(#[from] BeginCommitteeChangeError),
    /// Failed to lookup the committees using the [`CommitteeLookupService`].
    #[error(transparent)]
    LookupError(anyhow::Error),
    /// Failed to create any service
    #[error(transparent)]
    AllServicesFailed(anyhow::Error),
}

pub(crate) struct NodeCommitteeServiceBuilder<T> {
    service_factory: Box<dyn NodeServiceFactory<Service = T>>,
    local_identity: Option<PublicKey>,
    rng: StdRng,
    config: CommitteeServiceConfig,
}

impl Default for NodeCommitteeServiceBuilder<RemoteStorageNode> {
    fn default() -> Self {
        Self {
            service_factory: Box::new(super::default_node_service_factory),
            local_identity: None,
            rng: StdRng::seed_from_u64(rand::thread_rng().gen()),
            config: CommitteeServiceConfig::default(),
        }
    }
}

impl<T> NodeCommitteeServiceBuilder<T>
where
    T: NodeService,
{
    #[cfg(test)]
    pub fn node_service_factory<F>(
        self,
        service_factory: F,
    ) -> NodeCommitteeServiceBuilder<F::Service>
    where
        F: NodeServiceFactory + 'static,
    {
        NodeCommitteeServiceBuilder {
            local_identity: self.local_identity,
            rng: self.rng,
            config: self.config,
            service_factory: Box::new(service_factory),
        }
    }

    pub fn local_identity(mut self, id: PublicKey) -> Self {
        self.local_identity = Some(id);
        self
    }

    pub fn config(mut self, config: CommitteeServiceConfig) -> Self {
        self.config = config;
        self
    }

    #[cfg(test)]
    pub fn randomness(mut self, rng: StdRng) -> Self {
        self.rng = rng;
        self
    }

    pub async fn build<S>(self, lookup_service: S) -> Result<NodeCommitteeService<T>, anyhow::Error>
    where
        S: CommitteeLookupService + std::fmt::Debug + 'static,
    {
        // TODO(jsmith): Allow setting the local service factory.
        let committee_tracker: CommitteeTracker =
            lookup_service.get_active_committees().await?.into();
        let encoding_config = Arc::new(EncodingConfig::new(
            committee_tracker
                .committees()
                .current_committee()
                .n_shards(),
        ));

        let inner = NodeCommitteeServiceInner::new(
            committee_tracker,
            self.service_factory,
            self.config,
            encoding_config,
            self.local_identity,
            self.rng,
        )
        .await?;

        Ok(NodeCommitteeService {
            inner,
            committee_lookup: Box::new(lookup_service),
        })
    }
}

/// Default committee service used for communicating between nodes.
///
/// Requests the current committee state using a [`CommitteeLookupService`].
pub(crate) struct NodeCommitteeService<T = RemoteStorageNode> {
    inner: NodeCommitteeServiceInner<T>,
    committee_lookup: Box<dyn super::CommitteeLookupService>,
}

impl NodeCommitteeService<RemoteStorageNode> {
    pub fn builder() -> NodeCommitteeServiceBuilder<RemoteStorageNode> {
        Default::default()
    }

    pub async fn new<S>(
        lookup_service: S,
        local_identity: PublicKey,
        config: CommitteeServiceConfig,
    ) -> Result<Self, anyhow::Error>
    where
        S: CommitteeLookupService + std::fmt::Debug + 'static,
    {
        Self::builder()
            .local_identity(local_identity)
            .config(config)
            .build(lookup_service)
            .await
    }
}

impl<T> NodeCommitteeService<T>
where
    T: NodeService,
{
    async fn sync_shard_as_of_epoch(
        &self,
        shard: ShardIndex,
        starting_blob_id: BlobId,
        sliver_count: u64,
        sliver_type: SliverType,
        shard_owner_epoch: Epoch,
        key_pair: &ProtocolKeyPair,
    ) -> Result<Vec<(BlobId, Sliver)>, SyncShardClientError> {
        let committee = self
            .inner
            .committee_tracker
            .borrow()
            .committees()
            .committee_for_epoch(shard_owner_epoch)
            .ok_or(SyncShardClientError::NoSyncClient)?
            .clone();

        let node_info = committee
            .member_index_for_shard(shard)
            .map(|index| &committee.members()[index])
            .expect("shard is valid for the committee");

        let service =
            if let Some(service) = self.inner.get_node_service_by_id(&node_info.public_key) {
                service
            } else {
                // TODO(jsmith): Cache this service to avoid rebuilding.
                tracing::trace!("service is unavailable for node, recreating it");
                let mut service_factory = self.inner.service_factory.lock().await;
                service_factory
                    .make_service(node_info, &self.inner.encoding_config)
                    .await
                    .map_err(|_| SyncShardClientError::NoSyncClient)?
            };

        let slivers = service
            .oneshot(Request::SyncShardAsOfEpoch {
                shard,
                starting_blob_id,
                sliver_count,
                sliver_type,
                shard_owner_epoch,
                key_pair: key_pair.clone(),
            })
            .map_ok(Response::into_value)
            .map_err(|error| match error {
                NodeServiceError::Node(error) => SyncShardClientError::RequestError(error),
                NodeServiceError::Other(other) => anyhow::anyhow!(other).into(),
            })
            .await?;

        Ok(slivers)
    }

    /// Reset the committees to the latest retrieved via the configured lookup service.
    #[allow(unused)]
    pub async fn reset_committees(&self) -> Result<(), anyhow::Error> {
        let latest = self.committee_lookup.get_active_committees().await?.into();
        self.reset_committees_to(latest).await;
        Ok(())
    }

    #[allow(unused)]
    async fn reset_committees_to(
        &self,
        committee_tracker: CommitteeTracker,
    ) -> Result<(), anyhow::Error> {
        let mut service_factory = self.inner.service_factory.lock().await;

        // Create services for the current committee.
        let mut new_services = create_services_from_committee(
            &mut service_factory,
            committee_tracker.committees().current_committee(),
            &self.inner.encoding_config,
        )
        .await?;

        // If we are transitioning, then the previous committee is still serving reads, so create
        // services for its members.
        if committee_tracker.is_change_in_progress() {
            if let Some(previous_committee) = committee_tracker.committees().previous_committee() {
                add_members_from_committee(
                    &mut new_services,
                    &mut service_factory,
                    previous_committee,
                    &self.inner.encoding_config,
                )
                .await?;
            }
        }

        let mut services = self
            .inner
            .services
            .lock()
            .expect("thread did not panic with mutex");
        self.inner.committee_tracker.send_replace(committee_tracker);
        services.extend(new_services);

        Ok(())
    }

    /// Begin the committee transition as part of the epoch change.
    #[allow(unused)]
    pub async fn begin_committee_change(&self) -> Result<(), CommitteeTransitionError> {
        let latest = self
            .committee_lookup
            .get_active_committees()
            .await
            .map_err(CommitteeTransitionError::LookupError)?;
        let current_committee: Committee = (**latest.current_committee()).clone();

        self.begin_committee_change_to(current_committee).await
    }

    #[allow(unused)]
    async fn begin_committee_change_to(
        &self,
        next_committee: Committee,
    ) -> Result<(), CommitteeTransitionError> {
        let mut service_factory = self.inner.service_factory.lock().await;

        // Begin by creating the needed services, placing them into a temporary map.
        // This allows us to keep the critical section where we modify the active committee and
        // service set that is being used, small.
        let mut new_services = create_services_from_committee(
            &mut service_factory,
            &next_committee,
            &self.inner.encoding_config,
        )
        .await
        .map_err(CommitteeTransitionError::AllServicesFailed)?;

        let mut services = self
            .inner
            .services
            .lock()
            .expect("thread did not panic with mutex");

        let mut result = Ok(());
        self.inner
            .committee_tracker
            .send_if_modified(|committee_tracker| {
                result = committee_tracker.begin_committee_change(next_committee);
                result.is_ok()
            });

        if result.is_ok() {
            services.extend(new_services);
        }
        result.map_err(CommitteeTransitionError::from)
    }

    /// Ends the current transition of the committee.
    ///
    /// If a transition was in progress with the specified epoch, then it is ended and true is
    /// returned. Otherwise, false is returned. It is therefore safe to call this method, even if
    /// a transition is not taking place.
    #[allow(unused)]
    pub fn end_committee_change(&self, epoch: Epoch) -> Result<(), EndCommitteeChangeError> {
        self.end_committee_transition_to(epoch)
    }

    #[allow(unused)]
    fn end_committee_transition_to(&self, epoch: Epoch) -> Result<(), EndCommitteeChangeError> {
        let mut services = self
            .inner
            .services
            .lock()
            .expect("thread did not panic with mutex");

        let mut maybe_result: Option<Result<_, EndCommitteeChangeError>> = None;
        self.inner
            .committee_tracker
            .send_if_modified(|committee_tracker| {
                let current = committee_tracker.committees().current_committee().clone();
                let result = committee_tracker
                    .end_committee_change(epoch)
                    .map(|outgoing| (outgoing.clone(), current));
                maybe_result = Some(result);

                maybe_result
                    .as_ref()
                    .map(|result| result.is_ok())
                    .expect("option value set above")
            });

        let (outgoing_committee, current_committee) =
            maybe_result.expect("option set in closure")?;

        // We already added services for the new committee members, which may have overlapped with
        // the old. Only remove those services corresponding to members in the old committee that
        // are not in the new.
        for outgoing_member in outgoing_committee.members() {
            if !current_committee.contains(&outgoing_member.public_key) {
                services.remove(&outgoing_member.public_key);
            }
        }

        Ok(())
    }
}

pub(super) struct NodeCommitteeServiceInner<T> {
    /// The set of active committees, which can be observed for changes.
    pub committee_tracker: watch::Sender<CommitteeTracker>,
    /// Services for members of the active read and write committees.
    pub services: SyncMutex<HashMap<PublicKey, T>>,
    /// Timeouts and other configuration for requests.
    pub config: CommitteeServiceConfig,
    /// System wide encoding parameters
    pub encoding_config: Arc<EncodingConfig>,
    /// Shared randomness.
    pub rng: SyncMutex<StdRng>,
    /// The identity of the local storage node within and across committees.
    local_identity: Option<PublicKey>,
    /// Function used to construct new services.
    service_factory: TokioMutex<Box<dyn NodeServiceFactory<Service = T>>>,
}

impl<T> NodeCommitteeServiceInner<T>
where
    T: NodeService,
{
    pub async fn new(
        committee_tracker: CommitteeTracker,
        mut service_factory: Box<dyn NodeServiceFactory<Service = T>>,
        config: CommitteeServiceConfig,
        encoding_config: Arc<EncodingConfig>,
        local_identity: Option<PublicKey>,
        rng: StdRng,
    ) -> Result<Self, anyhow::Error> {
        // TODO(jsmith): Accept committees with change in progress
        assert!(!committee_tracker.is_change_in_progress());
        let initial_committee = committee_tracker.committees().current_committee();

        let services = create_services_from_committee(
            &mut service_factory,
            initial_committee,
            &encoding_config,
        )
        .await?;

        let this = Self {
            committee_tracker: watch::Sender::new(committee_tracker),
            services: SyncMutex::new(services),
            service_factory: TokioMutex::new(service_factory),
            local_identity,
            config,
            rng: SyncMutex::new(rng),
            encoding_config,
        };

        Ok(this)
    }

    pub(super) fn is_local(&self, id: &PublicKey) -> bool {
        self.local_identity
            .as_ref()
            .map(|key| key == id)
            .unwrap_or(false)
    }

    pub(super) fn get_node_service_by_id(&self, id: &PublicKey) -> Option<T> {
        self.services
            .lock()
            .expect("thread did not panic with mutex")
            .get(id)
            .cloned()
    }

    pub(super) fn subscribe_to_committee_changes(&self) -> watch::Receiver<CommitteeTracker> {
        self.committee_tracker.subscribe()
    }
}

#[async_trait::async_trait]
impl<T> CommitteeService for NodeCommitteeService<T>
where
    T: NodeService,
{
    fn get_epoch(&self) -> Epoch {
        self.inner.committee_tracker.borrow().committees().epoch()
    }

    fn get_shard_count(&self) -> NonZeroU16 {
        self.inner.encoding_config.n_shards()
    }

    fn encoding_config(&self) -> &Arc<EncodingConfig> {
        &self.inner.encoding_config
    }

    fn committee(&self) -> Arc<Committee> {
        self.inner
            .committee_tracker
            .borrow()
            .committees()
            .current_committee()
            .clone()
    }

    #[tracing::instrument(name = "get_and_verify_metadata committee", skip_all)]
    async fn get_and_verify_metadata(
        &self,
        blob_id: BlobId,
        certified_epoch: Epoch,
    ) -> VerifiedBlobMetadataWithId {
        GetAndVerifyMetadata::new(blob_id, certified_epoch, &self.inner)
            .run()
            .await
    }

    #[tracing::instrument(name = "recover_sliver committee", skip_all)]
    async fn recover_sliver(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
        sliver_id: SliverPairIndex,
        sliver_type: SliverType,
        certified_epoch: Epoch,
    ) -> Result<Sliver, InconsistencyProofEnum<MerkleProof>> {
        RecoverSliver::new(
            metadata,
            sliver_id,
            sliver_type,
            certified_epoch,
            &self.inner,
        )
        .run()
        .await
    }

    #[tracing::instrument(name = "get_invalid_blob_certificate committee", skip_all)]
    async fn get_invalid_blob_certificate(
        &self,
        blob_id: BlobId,
        inconsistency_proof: &InconsistencyProofEnum,
    ) -> InvalidBlobCertificate {
        tracing::trace!("creating future to get invalid blob certificate");
        GetInvalidBlobCertificate::new(blob_id, inconsistency_proof, &self.inner)
            .run()
            .await
    }

    #[tracing::instrument(name = "sync_shard_before_epoch committee", skip_all)]
    async fn sync_shard_before_epoch(
        &self,
        shard: ShardIndex,
        starting_blob_id: BlobId,
        sliver_type: SliverType,
        sliver_count: u64,
        epoch: Epoch,
        key_pair: &ProtocolKeyPair,
    ) -> Result<Vec<(BlobId, Sliver)>, SyncShardClientError> {
        self.sync_shard_as_of_epoch(
            shard,
            starting_blob_id,
            sliver_count,
            sliver_type,
            epoch,
            key_pair,
        )
        .await
    }

    fn is_walrus_storage_node(&self, public_key: &PublicKey) -> bool {
        let committee_tracker = self.inner.committee_tracker.borrow();

        committee_tracker
            .committees()
            .current_committee()
            .contains(public_key)
            || committee_tracker
                .committees()
                .previous_committee()
                .map(|committee| committee.contains(public_key))
                .unwrap_or(false)
            || committee_tracker
                .committees()
                .next_committee()
                .map(|committee| committee.contains(public_key))
                .unwrap_or(false)
    }
}

impl<T> std::fmt::Debug for NodeCommitteeServiceInner<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeCommitteeServiceInner")
            .field("committee_tracker", &self.committee_tracker)
            .field("config", &self.config)
            .field("local_identity", &self.local_identity)
            .field(
                "encoding_config.n_shards",
                &self.encoding_config.n_shards().get(),
            )
            .finish_non_exhaustive()
    }
}

impl<T> std::fmt::Debug for NodeCommitteeService<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeCommitteeService")
            .field("inner", &self.inner)
            .field("committee_lookup", &self.committee_lookup)
            .finish_non_exhaustive()
    }
}

async fn create_services_from_committee<T: NodeService>(
    service_factory: &mut Box<dyn NodeServiceFactory<Service = T>>,
    committee: &Committee,
    encoding_config: &Arc<EncodingConfig>,
) -> Result<HashMap<PublicKey, T>, anyhow::Error> {
    let mut services = HashMap::default();
    add_members_from_committee(&mut services, service_factory, committee, encoding_config)
        .await
        .map(|_| services)
}

#[tracing::instrument(skip_all, fields(walrus.epoch = committee.epoch))]
async fn add_members_from_committee<T: NodeService>(
    services: &mut HashMap<PublicKey, T>,
    service_factory: &mut Box<dyn NodeServiceFactory<Service = T>>,
    committee: &Committee,
    encoding_config: &Arc<EncodingConfig>,
) -> Result<(), anyhow::Error> {
    let mut n_created = 0usize;

    for member in committee.members() {
        let public_key = &member.public_key;
        match service_factory.make_service(member, encoding_config).await {
            Ok(service) => {
                n_created += 1;

                if services.insert(public_key.clone(), service).is_some() {
                    tracing::debug!(
                        walrus.node.public_key = %public_key,
                        "replaced the service for a storage node"
                    );
                } else {
                    tracing::debug!(
                        walrus.node.public_key = %public_key,
                        "added a service for a storage node"
                    );
                }
            }
            Err(error) => {
                tracing::warn!(
                    walrus.node.public_key = %public_key, %error,
                    "failed to create service for committee member"
                );
            }
        }
    }

    walrus_core::ensure!(
        n_created != 0,
        "failed to create any service from the committee"
    );
    Ok(())
}

#[cfg(test)]
#[path = "test_committee_service.rs"]
mod tests;

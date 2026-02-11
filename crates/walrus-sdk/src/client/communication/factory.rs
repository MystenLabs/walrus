// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Create the vectors of node communications objects.

use std::{
    collections::{HashMap, HashSet, hash_map::Entry},
    sync::{Arc, Mutex},
};

use anyhow::anyhow;
use rand::{seq::SliceRandom, thread_rng};
use reqwest::Client as ReqwestClient;
use rustls::pki_types::CertificateDer;
use rustls_native_certs::CertificateResult;
use sui_types::base_types::ObjectID;
use tokio::sync::Semaphore;
use tracing::Level;
use walrus_core::{Epoch, NetworkPublicKey, encoding::EncodingConfig};
use walrus_storage_node_client::{ClientBuildError, StorageNodeClient, StorageNodeClientBuilder};
use walrus_sui::types::{Committee, NetworkAddress, StorageNode};
use walrus_utils::metrics::Registry;

use super::{NodeCommunication, NodeReadCommunication, NodeWriteCommunication};
use crate::{
    active_committees::ActiveCommittees,
    client::auto_tune::AutoTuneHandle,
    config::ClientCommunicationConfig,
    error::{ClientError, ClientErrorKind, ClientResult},
};

/// Factory to create objects amenable to communication with storage nodes.
#[derive(Clone, Debug)]
pub struct NodeCommunicationFactory {
    config: ClientCommunicationConfig,
    encoding_config: Arc<EncodingConfig>,
    client_cache: Arc<Mutex<HashMap<(NetworkAddress, NetworkPublicKey), StorageNodeClient>>>,
    native_certs: Vec<CertificateDer<'static>>,
    metrics_registry: Option<Registry>,
}

/// Factory to create the vectors of `NodeCommunication` objects.
impl NodeCommunicationFactory {
    /// Creates a new [`NodeCommunicationFactory`].
    #[tracing::instrument(name = "NodeCommunicationFactory::new", skip_all, level = Level::DEBUG)]
    pub fn new(
        config: ClientCommunicationConfig,
        encoding_config: Arc<EncodingConfig>,
        metrics_registry: Option<Registry>,
    ) -> ClientResult<Self> {
        let native_certs = if !config.disable_native_certs {
            tracing::debug!("loading native certs");
            let CertificateResult { certs, errors, .. } = rustls_native_certs::load_native_certs();
            tracing::debug!("finished loading native certs");
            if certs.is_empty() {
                return Err(ClientError::from(ClientErrorKind::FailedToLoadCerts(
                    errors,
                )));
            };
            if !errors.is_empty() {
                tracing::warn!(
                    "encountered {} errors when trying to load native certs",
                    errors.len(),
                );
                tracing::debug!(?errors, "errors encountered when loading native certs");
            }
            certs
        } else {
            vec![]
        };
        Ok(Self {
            config,
            encoding_config,
            client_cache: Default::default(),
            native_certs,
            metrics_registry,
        })
    }

    /// Returns a vector of [`NodeWriteCommunication`] objects representing nodes in random order.
    pub(crate) fn node_write_communications(
        &self,
        committees: &ActiveCommittees,
        sliver_write_limit: Arc<Semaphore>,
        auto_tune_handle: Option<AutoTuneHandle>,
    ) -> ClientResult<Vec<NodeWriteCommunication>> {
        self.remove_old_cached_clients(
            committees,
            &mut self
                .client_cache
                .lock()
                .expect("other threads should not panic"),
        );

        let write_committee = committees.write_committee();

        node_communications(write_committee, |index| {
            self.create_write_communication(
                write_committee,
                index,
                sliver_write_limit.clone(),
                auto_tune_handle.clone(),
            )
        })
    }

    /// Returns a vector of [`NodeReadCommunication`] objects representing nodes in random order.
    ///
    /// `certified_epoch` is the epoch where the blob to be read was initially certified.
    ///
    /// # Errors
    ///
    /// Returns a [`ClientError`] with [`ClientErrorKind::BehindCurrentEpoch`] if the certified
    /// epoch is greater than the current committee epoch.
    pub(crate) fn node_read_communications(
        &self,
        committees: &ActiveCommittees,
        certified_epoch: Epoch,
    ) -> ClientResult<Vec<NodeReadCommunication>> {
        self.remove_old_cached_clients(
            committees,
            &mut self
                .client_cache
                .lock()
                .expect("other threads should not panic"),
        );

        let read_committee = committees.read_committee(certified_epoch).ok_or_else(|| {
            ClientErrorKind::BehindCurrentEpoch {
                client_epoch: committees.epoch(),
                certified_epoch,
            }
        })?;

        node_communications(read_committee, |index| {
            self.create_read_communication(read_committee, index)
        })
    }

    /// Returns a vector of [`NodeReadCommunication`] objects, the weight of which is at least a
    /// quorum.
    pub(crate) fn node_read_communications_quorum(
        &self,
        committees: &ActiveCommittees,
        certified_epoch: Epoch,
    ) -> ClientResult<Vec<NodeReadCommunication>> {
        self.node_read_communications_threshold(committees, certified_epoch, |weight| {
            committees.is_quorum(weight)
        })
    }

    /// Returns a vector of [`NodeWriteCommunication`] objects, matching the specified node IDs.
    pub(crate) fn node_write_communications_by_id(
        &self,
        committees: &ActiveCommittees,
        sliver_write_limit: Arc<Semaphore>,
        auto_tune_handle: Option<AutoTuneHandle>,
        node_ids: impl IntoIterator<Item = ObjectID>,
    ) -> ClientResult<Vec<NodeWriteCommunication>> {
        self.remove_old_cached_clients(
            committees,
            &mut self
                .client_cache
                .lock()
                .expect("other threads should not panic"),
        );

        let write_committee = committees.write_committee();
        let node_ids: Vec<_> = node_ids.into_iter().collect();

        let comms = write_committee
            .members()
            .iter()
            .enumerate()
            .filter_map(|(idx, node)| {
                if node_ids.contains(&node.node_id) {
                    self.create_write_communication(
                        write_committee,
                        idx,
                        sliver_write_limit.clone(),
                        auto_tune_handle.clone(),
                    )
                    .transpose()
                } else {
                    None
                }
            })
            .collect::<Result<Vec<_>, ClientBuildError>>()
            .map_err(|error| {
                ClientError::store_blob_internal(format!(
                    "cannot communicate with one or more of the storage nodes: {error}",
                ))
            })?;

        Ok(comms)
    }

    /// Returns a vector of [`NodeWriteCommunication`] objects, matching the specified node indices.
    ///
    /// This is useful when retrying uploads to a subset of nodes without rebuilding communications
    /// for the entire committee.
    pub(crate) fn node_write_communications_by_index(
        &self,
        committees: &ActiveCommittees,
        sliver_write_limit: Arc<Semaphore>,
        auto_tune_handle: Option<AutoTuneHandle>,
        node_indices: impl IntoIterator<Item = usize>,
    ) -> ClientResult<Vec<NodeWriteCommunication>> {
        self.remove_old_cached_clients(
            committees,
            &mut self
                .client_cache
                .lock()
                .expect("other threads should not panic"),
        );

        let write_committee = committees.write_committee();
        let mut seen = HashSet::new();
        let mut comms = Vec::new();

        for index in node_indices {
            if !seen.insert(index) {
                continue;
            }

            if index >= write_committee.members().len() {
                tracing::warn!(
                    node = index,
                    committee_size = write_committee.members().len(),
                    "requested node index is out of range; likely stale committee info"
                );
                return Err(ClientError::store_blob_internal(format!(
                    "node index {} out of range for committee size {}",
                    index,
                    write_committee.members().len()
                )));
            }

            match self.create_write_communication(
                write_committee,
                index,
                sliver_write_limit.clone(),
                auto_tune_handle.clone(),
            ) {
                Ok(Some(comm)) => comms.push(comm),
                Ok(None) => {
                    tracing::warn!(
                        node = index,
                        "skipping storage node without shards when building write communications"
                    );
                    continue;
                }
                Err(error) => {
                    return Err(ClientError::store_blob_internal(format!(
                        "cannot communicate with one or more of the storage nodes: {error}",
                    )));
                }
            }
        }

        Ok(comms)
    }

    /// Builds a [`NodeCommunication`] object for the identified storage node within the
    /// committee.
    ///
    /// Returns `None` if the node has no shards.
    ///
    /// # Panics
    ///
    /// Panics if the index is out of range of the committee members.
    fn create_node_communication(
        &self,
        committee: &Committee,
        index: usize,
    ) -> Result<Option<NodeCommunication>, ClientBuildError> {
        let node = committee.members()[index].clone();
        let client = self.create_client(&node)?;

        Ok(NodeCommunication::new(
            index,
            committee.epoch,
            client,
            node,
            Arc::clone(&self.encoding_config),
            self.config.request_rate_config.clone(),
            self.config.sliver_status_check_threshold,
            self.config.confirmation_long_poll,
        ))
    }

    /// Builds a [`NodeReadCommunication`] object for the identified storage node within the
    /// committee.
    ///
    /// Returns `None` if the node has no shards.
    ///
    /// # Panics
    ///
    /// Panics if the index is out of range of the committee members.
    fn create_read_communication(
        &self,
        read_committee: &Committee,
        index: usize,
    ) -> Result<Option<NodeReadCommunication>, ClientBuildError> {
        self.create_node_communication(read_committee, index)
    }

    /// Builds a [`NodeWriteCommunication`] object for the given storage node.
    ///
    /// Returns `None` if the node has no shards.
    fn create_write_communication(
        &self,
        write_committee: &Committee,
        index: usize,
        sliver_write_limit: Arc<Semaphore>,
        auto_tune_handle: Option<AutoTuneHandle>,
    ) -> Result<Option<NodeWriteCommunication>, ClientBuildError> {
        let maybe_node_communication = self
            .create_node_communication(write_committee, index)?
            .map(|nc| nc.with_write_limits(sliver_write_limit, auto_tune_handle));
        Ok(maybe_node_communication)
    }

    /// Create a new [`StorageNodeClient`] for the given storage node.
    pub fn create_client(&self, node: &StorageNode) -> Result<StorageNodeClient, ClientBuildError> {
        let node_client_id = (
            node.network_address.clone(),
            node.network_public_key.clone(),
        );
        let mut cache = self
            .client_cache
            .lock()
            .expect("other threads should not panic");

        match cache.entry(node_client_id) {
            Entry::Occupied(occupied) => Ok(occupied.get().clone()),
            Entry::Vacant(vacant) => {
                let reqwest_builder = self.config.reqwest_config.apply(ReqwestClient::builder());
                let mut builder = StorageNodeClientBuilder::from_reqwest(reqwest_builder);
                if self.config.disable_proxy {
                    builder = builder.no_proxy();
                }
                if let Some(registry) = self.metrics_registry.as_ref() {
                    builder = builder.metric_registry(registry.clone());
                }

                let client = builder
                    .authenticate_with_public_key(node.network_public_key.clone())
                    .add_root_certificates(&self.native_certs)
                    .tls_built_in_root_certs(false)
                    .build(&node.network_address.0)?;
                Ok(vacant.insert(client).clone())
            }
        }
    }

    /// Clears the cache of all clients that are not in the previous, current, or next committee.
    #[allow(clippy::mutable_key_type)]
    fn remove_old_cached_clients(
        &self,
        committees: &ActiveCommittees,
        cache: &mut HashMap<(NetworkAddress, NetworkPublicKey), StorageNodeClient>,
    ) {
        #[allow(clippy::mutable_key_type)]
        let active_members = committees.unique_node_address_and_key();
        cache.retain(|(addr, key), _| active_members.contains(&(addr, key)));
    }

    /// Returns a vector of [`NodeReadCommunication`] objects the total weight of which fulfills the
    /// threshold function.
    ///
    /// The set and order of nodes included in the communication is randomized.
    ///
    /// # Errors
    ///
    /// Returns a [`ClientError`] with [`ClientErrorKind::Other`] if the threshold function is not
    /// fulfilled after considering all storage nodes. Returns a [`ClientError`] with
    /// [`ClientErrorKind::BehindCurrentEpoch`] if the certified epoch is greater than the current
    /// committee epoch.
    fn node_read_communications_threshold(
        &self,
        committees: &ActiveCommittees,
        certified_epoch: Epoch,
        threshold_fn: impl Fn(usize) -> bool,
    ) -> ClientResult<Vec<NodeReadCommunication>> {
        let read_committee = committees.read_committee(certified_epoch).ok_or_else(|| {
            ClientErrorKind::BehindCurrentEpoch {
                client_epoch: committees.epoch(),
                certified_epoch,
            }
        })?;

        let read_members = read_committee.members();

        let mut random_indices: Vec<_> = (0..read_members.len()).collect();
        random_indices.shuffle(&mut thread_rng());
        let mut random_indices = random_indices.into_iter();
        let mut weight = 0;
        let mut comms = vec![];

        loop {
            if threshold_fn(weight) {
                break Ok(comms);
            }
            let Some(index) = random_indices.next() else {
                break Err(ClientErrorKind::Other(
                    anyhow!("unable to create sufficient NodeCommunications").into(),
                )
                .into());
            };
            weight += read_members[index].shard_ids.len();

            // Since we are attempting this in a loop, we will retry until we have a threshold of
            // successfully constructed clients (no error and with shards).
            if let Ok(Some(comm)) = self.create_read_communication(read_committee, index) {
                comms.push(comm);
            }
        }
    }
}

/// Create a vector of node communication objects from the given committee and constructor.
fn node_communications<W>(
    committee: &Committee,
    constructor: impl Fn(usize) -> Result<Option<NodeCommunication<W>>, ClientBuildError>,
) -> ClientResult<Vec<NodeCommunication<W>>> {
    if committee.n_members() == 0 {
        return Err(ClientError::from(ClientErrorKind::EmptyCommittee));
    }

    let mut comms: Vec<_> = (0..committee.n_members())
        .map(|i| (i, constructor(i)))
        .collect();

    if comms.iter().all(|(_, result)| result.is_err()) {
        let Some((_, Err(sample_error))) = comms.pop() else {
            unreachable!("`all()` guarantees at least 1 result and all results are errors");
        };
        return Err(ClientError::from(ClientErrorKind::AllConnectionsFailed(
            sample_error,
        )));
    }

    let mut comms: Vec<_> = comms
        .into_iter()
        .filter_map(|(index, result)| match result {
            Ok(maybe_communication) => maybe_communication,
            Err(error) => {
                tracing::warn!(
                    node=index, %error, "unable to establish any connection to a storage node"
                );
                None
            }
        })
        .collect();
    comms.shuffle(&mut thread_rng());

    Ok(comms)
}

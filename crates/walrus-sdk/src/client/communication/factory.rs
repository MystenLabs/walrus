// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Create the vectors of node communications objects.

use std::{
    collections::{hash_map::Entry, HashMap},
    sync::{Arc, Mutex},
};

use anyhow::anyhow;
use rand::{seq::SliceRandom, thread_rng};
use reqwest::Client as ReqwestClient;
use rustls::pki_types::CertificateDer;
use rustls_native_certs::CertificateResult;
use tokio::sync::Semaphore;
use walrus_core::{encoding::EncodingConfig, Epoch, NetworkPublicKey};
use walrus_rest_client::{
    client::{Client as StorageNodeClient, ClientBuilder as StorageNodeClientBuilder},
    error::ClientBuildError,
};
use walrus_sui::types::{Committee, NetworkAddress, StorageNode};
use walrus_utils::metrics::Registry;

use super::{NodeCommunication, NodeReadCommunication, NodeWriteCommunication};
use crate::{
    active_committees::ActiveCommittees,
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
    pub fn new(
        config: ClientCommunicationConfig,
        encoding_config: Arc<EncodingConfig>,
        metrics_registry: Option<Registry>,
    ) -> ClientResult<Self> {
        let native_certs = if !config.disable_native_certs {
            let CertificateResult { certs, errors, .. } = rustls_native_certs::load_native_certs();
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
    pub(crate) fn node_write_communications<'a>(
        &'a self,
        committees: &'a ActiveCommittees,
        sliver_write_limit: Arc<Semaphore>,
    ) -> ClientResult<Vec<NodeWriteCommunication<'a>>> {
        self.remove_old_cached_clients(
            committees,
            &mut self
                .client_cache
                .lock()
                .expect("other threads should not panic"),
        );

        let write_committee = committees.write_committee();

        node_communications(write_committee, |index| {
            self.create_write_communication(write_committee, index, sliver_write_limit.clone())
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
    pub(crate) fn node_read_communications<'a>(
        &'a self,
        committees: &'a ActiveCommittees,
        certified_epoch: Epoch,
    ) -> ClientResult<Vec<NodeReadCommunication<'a>>> {
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
    pub(crate) fn node_read_communications_quorum<'a>(
        &'a self,
        committees: &'a ActiveCommittees,
        certified_epoch: Epoch,
    ) -> ClientResult<Vec<NodeReadCommunication<'a>>> {
        self.node_read_communications_threshold(committees, certified_epoch, |weight| {
            committees.is_quorum(weight)
        })
    }

    /// Builds a [`NodeCommunication`] object for the identified storage node within the
    /// committee.
    ///
    /// Returns `None` if the node has no shards.
    ///
    /// # Panics
    ///
    /// Panics if the index is out of range of the committee members.
    fn create_node_communication<'a>(
        &'a self,
        committee: &'a Committee,
        index: usize,
    ) -> Result<Option<NodeCommunication<'a>>, ClientBuildError> {
        let node = &committee.members()[index];
        let client = self.create_client(node)?;

        Ok(NodeCommunication::new(
            index,
            committee.epoch,
            client,
            node,
            &self.encoding_config,
            self.config.request_rate_config.clone(),
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
    fn create_read_communication<'a>(
        &'a self,
        read_committee: &'a Committee,
        index: usize,
    ) -> Result<Option<NodeReadCommunication<'a>>, ClientBuildError> {
        self.create_node_communication(read_committee, index)
    }

    /// Builds a [`NodeWriteCommunication`] object for the given storage node.
    ///
    /// Returns `None` if the node has no shards.
    fn create_write_communication<'a>(
        &'a self,
        write_committee: &'a Committee,
        index: usize,
        sliver_write_limit: Arc<Semaphore>,
    ) -> Result<Option<NodeWriteCommunication<'a>>, ClientBuildError> {
        let maybe_node_communication = self
            .create_node_communication(write_committee, index)?
            .map(|nc| nc.with_write_limits(sliver_write_limit));
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
    fn node_read_communications_threshold<'a>(
        &'a self,
        committees: &'a ActiveCommittees,
        certified_epoch: Epoch,
        threshold_fn: impl Fn(usize) -> bool,
    ) -> ClientResult<Vec<NodeReadCommunication<'a>>> {
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
fn node_communications<'a, W>(
    committee: &Committee,
    constructor: impl Fn(usize) -> Result<Option<NodeCommunication<'a, W>>, ClientBuildError>,
) -> ClientResult<Vec<NodeCommunication<'a, W>>> {
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

// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Create the vectors of node communications objects.

use std::{
    collections::{hash_map::Entry, HashMap},
    sync::{Arc, Mutex},
};

use anyhow::anyhow;
use rand::{seq::SliceRandom, thread_rng};
use reqwest::Client as ReqwestClient;
use tokio::sync::Semaphore;
use walrus_core::{encoding::EncodingConfig, Epoch, NetworkPublicKey};
use walrus_sdk::{
    client::{Client as StorageNodeClient, ClientBuilder as StorageNodeClientBuilder},
    error::ClientBuildError,
};
use walrus_sui::types::{Committee, NetworkAddress, StorageNode};

use super::{NodeCommunication, NodeReadCommunication, NodeWriteCommunication};
use crate::{
    client::{ClientCommunicationConfig, ClientErrorKind, ClientResult},
    common::active_committees::ActiveCommittees,
};

#[derive(Clone, Debug)]
pub(crate) struct NodeCommunicationFactory {
    config: ClientCommunicationConfig,
    committees: Arc<ActiveCommittees>,
    encoding_config: Arc<EncodingConfig>,
    client_cache: Arc<Mutex<HashMap<(NetworkAddress, NetworkPublicKey), StorageNodeClient>>>,
}

/// Factory to create the vectors of [`NodeCommunication`][super::NodeCommunication] objects.
impl NodeCommunicationFactory {
    pub(crate) fn new(
        config: ClientCommunicationConfig,
        committees: Arc<ActiveCommittees>,
        encoding_config: Arc<EncodingConfig>,
    ) -> Self {
        Self {
            config,
            committees,
            encoding_config,
            client_cache: Default::default(),
        }
    }

    /// Returns a vector of [`NodeWriteCommunication`] objects representing nodes in random order.
    pub(crate) fn node_write_communications(
        &self,
        sliver_write_limit: Arc<Semaphore>,
    ) -> ClientResult<Vec<NodeWriteCommunication>> {
        node_communications(self.committees.write_committee(), |index| {
            self.create_write_communication(index, sliver_write_limit.clone())
        })
    }

    /// Returns a vector of [`NodeReadCommunication`] objects representing nodes in random order.
    ///
    /// `certified_epoch` is the epoch where the blob to be read was initially certified.
    ///
    /// # Panics
    ///
    /// Panics if the `certified_epoch` provided is greater than the current committee epoch.
    pub(crate) fn node_read_communications(
        &self,
        certified_epoch: Epoch,
    ) -> ClientResult<Vec<NodeReadCommunication>> {
        node_communications(
            self.committees
                .read_committee(certified_epoch)
                .expect("the certified epoch must be less than the current known committee epoch"),
            |index| self.create_read_communication(index, certified_epoch),
        )
    }

    /// Returns a vector of [`NodeReadCommunication`] objects, the weight of which is at least a
    /// quorum.
    pub(crate) fn node_read_communications_quorum(
        &self,
        certified_epoch: Epoch,
    ) -> Vec<NodeReadCommunication> {
        self.node_read_communications_threshold(certified_epoch, |weight| {
            self.committees.is_quorum(weight)
        })
        .expect("the threshold is below the total number of shards")
    }

    // Private functions

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
        index: usize,
        committee: &'a Committee,
    ) -> Result<Option<NodeCommunication<'_>>, ClientBuildError> {
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
    /// Panics if the certified epoch is larger than the current known committee epoch.
    fn create_read_communication(
        &self,
        index: usize,
        certified_epoch: Epoch,
    ) -> Result<Option<NodeReadCommunication<'_>>, ClientBuildError> {
        let committee = self
            .committees
            .read_committee(certified_epoch)
            .expect("the certified epoch must be less than the current known committee epoch");
        self.create_node_communication(index, committee)
    }

    /// Builds a [`NodeWriteCommunication`] object for the given storage node.
    ///
    /// Returns `None` if the node has no shards.
    fn create_write_communication(
        &self,
        index: usize,
        sliver_write_limit: Arc<Semaphore>,
    ) -> Result<Option<NodeWriteCommunication<'_>>, ClientBuildError> {
        let maybe_node_communication = self
            .create_node_communication(index, self.committees.write_committee())?
            .map(|nc| nc.with_write_limits(sliver_write_limit));
        Ok(maybe_node_communication)
    }

    fn create_client(&self, node: &StorageNode) -> Result<StorageNodeClient, ClientBuildError> {
        let node_client_id = (
            node.network_address.clone(),
            node.network_public_key.clone(),
        );
        let mut cache = self
            .client_cache
            .lock()
            .expect("other threads should not panic");

        // Clear the cache; otherwise, as epochs advance, we keep around old clients.
        self.remove_old_cached_clients(&mut cache);

        match cache.entry(node_client_id) {
            Entry::Occupied(occupied) => Ok(occupied.get().clone()),
            Entry::Vacant(vacant) => {
                let reqwest_builder = self.config.reqwest_config.apply(ReqwestClient::builder());
                let client = StorageNodeClientBuilder::from_reqwest(reqwest_builder)
                    .authenticate_with_public_key(node.network_public_key.clone())
                    .build(&node.network_address.0)?;
                Ok(vacant.insert(client).clone())
            }
        }
    }

    /// Clears the cache of all clients that are not in the previous, current, or next committee.
    #[allow(clippy::mutable_key_type)]
    fn remove_old_cached_clients(
        &self,
        cache: &mut HashMap<(NetworkAddress, NetworkPublicKey), StorageNodeClient>,
    ) {
        #[allow(clippy::mutable_key_type)]
        let active_members = self.committees.unique_node_address_and_key();
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
    /// fulfilled after considering all storage nodes.
    ///
    /// # Panics
    ///
    /// Panics if the `certified_epoch` provided is greater than the current committee epoch.
    fn node_read_communications_threshold(
        &self,
        certified_epoch: Epoch,
        threshold_fn: impl Fn(usize) -> bool,
    ) -> ClientResult<Vec<NodeReadCommunication>> {
        let read_members = self
            .committees
            .read_committee(certified_epoch)
            .expect("the certified epoch must be less than the current known committee epoch")
            .members();

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
            if let Ok(Some(comm)) = self.create_read_communication(index, certified_epoch) {
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
    let mut comms: Vec<_> = (0..committee.n_members())
        .map(|i| (i, constructor(i)))
        .collect();

    if comms.iter().all(|(_, result)| result.is_err()) {
        let Some((_, Err(sample_error))) = comms.pop() else {
            unreachable!("`all()` guarantees at least 1 result and all results are errors");
        };
        return Err(ClientErrorKind::AllConnectionsFailed(sample_error).into());
    }

    let mut comms: Vec<_> = comms
        .into_iter()
        .filter_map(|(index, result)| match result {
            Ok(maybe_communication) => maybe_communication,
            Err(err) => {
                tracing::warn!(
                    node=index, %err, "unable to establish any connection to a storage node"
                );
                None
            }
        })
        .collect();
    comms.shuffle(&mut thread_rng());

    Ok(comms)
}

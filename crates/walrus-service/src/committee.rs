// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Committee lookup and management.

use std::num::NonZeroU16;

use anyhow::Context;
use async_trait::async_trait;
use walrus_core::{Epoch, PublicKey};
use walrus_sui::{client::ReadClient, types::Committee};

/// Factory used to create services for interacting with the committee on each epoch.
#[async_trait]
pub trait CommitteeServiceFactory: std::fmt::Debug + Send + Sync {
    /// Returns a new `Self::Service` for the current epoch.
    async fn new_for_epoch(&self) -> Result<Box<dyn CommitteeService>, anyhow::Error>;
}

/// A `CommitteeService` provides information on the current committee, as well as interactions
/// with committee members.
///
/// It is associated with a single storage epoch.
#[async_trait]
pub trait CommitteeService: std::fmt::Debug + Send + Sync {
    /// Returns the epoch associated with the committee.
    fn get_epoch(&self) -> Epoch;

    /// Returns the number of shards in the committee.
    fn get_shard_count(&self) -> NonZeroU16;

    /// Excludes a member from calls made to the committee.
    ///
    /// An excluded member will not be contacted when making calls against the committee. Returns
    /// false if the identified member does not exist in the committee.
    #[must_use]
    fn exclude_member(&mut self, identity: &PublicKey) -> bool;
}

/// Constructs [`NodeCommitteeService`]s by reading the current storage committee from the chain.
#[derive(Debug)]
pub struct SuiCommitteeServiceFactory<T> {
    read_client: T,
}

impl<T> SuiCommitteeServiceFactory<T>
where
    T: ReadClient,
{
    /// Creates a new service factory for the provided read client.
    pub fn new(read_client: T) -> Self {
        Self { read_client }
    }
}

#[async_trait]
impl<T> CommitteeServiceFactory for SuiCommitteeServiceFactory<T>
where
    T: ReadClient + std::fmt::Debug + Send + Sync,
{
    async fn new_for_epoch(&self) -> Result<Box<dyn CommitteeService>, anyhow::Error> {
        let committee = self
            .read_client
            .current_committee()
            .await
            .context("unable to create a committee service for the current epoch")?;

        Ok(Box::new(NodeCommitteeService { committee }))
    }
}

/// Provides details of the committee, as well as an API to perform requests
/// against committee members.
#[derive(Debug)]
pub struct NodeCommitteeService {
    committee: Committee,
}

impl CommitteeService for NodeCommitteeService {
    fn get_epoch(&self) -> Epoch {
        self.committee.epoch
    }

    fn get_shard_count(&self) -> NonZeroU16 {
        self.committee.n_shards()
    }

    fn exclude_member(&mut self, identity: &PublicKey) -> bool {
        // TODO(jsmith): Exclude from operations on nodes.
        self.committee
            .members()
            .iter()
            .any(|member| member.public_key == *identity)
    }
}

// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Committee lookup and management.

use std::num::NonZeroU16;

use async_trait::async_trait;
use walrus_core::{Epoch, PublicKey};

/// Factory used to create services for interacting with the committee on each epoch.
#[async_trait]
pub trait CommitteeServiceFactory: std::fmt::Debug + Send + Sync {
    /// Returns a new `Self::Service` for the specified epoch.
    ///
    /// If `epoch` is `None`, then a service should be created for the latest known epoch.
    async fn new_for_epoch(
        &self,
        epoch: Option<Epoch>,
    ) -> Result<Box<dyn CommitteeService>, anyhow::Error>;
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

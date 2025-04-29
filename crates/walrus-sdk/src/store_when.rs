// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Storage conditions.

/// Represents how the store operation should be carried out by the client.
#[derive(Debug, Clone, Copy)]
pub enum StoreWhen {
    /// Store the blob if not stored, but do not check the resources in the current wallet.
    ///
    /// With this command, the client does not check for usable registrations or
    /// storage space. This is useful when using the publisher, to avoid wasting multiple round
    /// trips to the fullnode.
    NotStoredIgnoreResources,
    /// Check the status of the blob before storing it, and store it only if it is not already.
    NotStored,
    /// Store the blob always, without checking the status.
    Always,
    /// Store the blob always, without checking the status, and ignore the resources in the wallet.
    AlwaysIgnoreResources,
}

impl StoreWhen {
    /// Returns `true` if the operation ignore the blob status.
    ///
    /// If `true`, the client should store the blob, even if the blob is already stored on Walrus
    /// for a sufficient number of epochs.
    pub fn is_ignore_status(&self) -> bool {
        matches!(self, Self::Always | Self::AlwaysIgnoreResources)
    }

    /// Returns `true` if the operation should ignore the resources in the wallet.
    pub fn is_ignore_resources(&self) -> bool {
        matches!(
            self,
            Self::NotStoredIgnoreResources | Self::AlwaysIgnoreResources
        )
    }

    /// Returns [`Self`] based on the value of the `force` and `ignore-resources` flags.
    pub fn from_flags(force: bool, ignore_resources: bool) -> Self {
        match (force, ignore_resources) {
            (true, true) => Self::AlwaysIgnoreResources,
            (true, false) => Self::Always,
            (false, true) => Self::NotStoredIgnoreResources,
            (false, false) => Self::NotStored,
        }
    }
}

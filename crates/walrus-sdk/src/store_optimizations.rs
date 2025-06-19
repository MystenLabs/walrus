// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Optimizations for storing blobs.

/// Represents how the store operation should be carried out by the client.
#[derive(Debug, Clone, Copy)]
pub struct StoreOptimizations {
    /// Check the status of the blob before storing it; store it only if it is not already stored as
    /// a permanent blob on Walrus for a sufficient number of epochs.
    pub check_status: bool,
    /// Reuse matching `Storage` resources and already registered `Blob` objects owned by the wallet
    /// if there are any.
    ///
    /// Specifically, check if there is an appropriate storage resource (with sufficient space and
    /// for a sufficient duration) that can be used to register the blob or if there is an already
    /// registered blob object that matches the blob ID and duration.
    pub reuse_resources: bool,
}

impl StoreOptimizations {
    /// Returns [`Self`] with all optimizations enabled.
    pub fn all() -> Self {
        Self {
            check_status: true,
            reuse_resources: true,
        }
    }

    /// Returns [`Self`] with all optimizations disabled.
    pub fn none() -> Self {
        Self {
            check_status: false,
            reuse_resources: false,
        }
    }

    /// Returns [`Self`] based on the value of the `force` and `ignore-resources` flags.
    pub fn from_force_and_ignore_resources_flags(force: bool, ignore_resources: bool) -> Self {
        Self {
            check_status: !force,
            reuse_resources: !ignore_resources,
        }
    }

    /// Sets the `check_status` flag.
    pub fn with_check_status(mut self, check_status: bool) -> Self {
        self.check_status = check_status;
        self
    }

    /// Sets the `reuse_resources` flag.
    pub fn with_reuse_resources(mut self, reuse_resources: bool) -> Self {
        self.reuse_resources = reuse_resources;
        self
    }

    /// Returns `true` if the operation should check the blob status.
    ///
    /// If `true`, the client does not store the blob if it is already stored on Walrus for a
    /// sufficient number of epochs.
    pub fn should_check_status(&self) -> bool {
        self.check_status
    }

    /// Returns `true` if the operation should check the resources in the wallet.
    pub fn should_check_existing_resources(&self) -> bool {
        self.reuse_resources
    }
}

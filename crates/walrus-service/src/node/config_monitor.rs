// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use tracing::{self, instrument};

use super::{
    committee::CommitteeService,
    config::StorageNodeConfig,
    contract_service::SystemContractService,
};

/// Monitors and syncs node configuration with on-chain parameters
pub struct ConfigMonitor {
    config: StorageNodeConfig,
    contract_service: Arc<dyn SystemContractService>,
    committee_service: Arc<dyn CommitteeService>,
    check_interval: Duration,
    enabled: AtomicBool,
}

impl ConfigMonitor {
    /// Creates a new enabled ConfigMonitor instance
    pub fn new(
        config: StorageNodeConfig,
        contract_service: Arc<dyn SystemContractService>,
        committee_service: Arc<dyn CommitteeService>,
        check_interval: Duration,
    ) -> Self {
        Self {
            config,
            contract_service,
            committee_service,
            check_interval,
            enabled: AtomicBool::new(true),
        }
    }

    /// Creates a disabled ConfigMonitor instance with the same parameters as new()
    pub fn disabled(
        config: StorageNodeConfig,
        contract_service: Arc<dyn SystemContractService>,
        committee_service: Arc<dyn CommitteeService>,
        check_interval: Duration,
    ) -> Self {
        Self {
            config,
            contract_service,
            committee_service,
            check_interval,
            enabled: AtomicBool::new(false),
        }
    }

    /// Runs the config monitoring loop until an error occurs
    pub async fn run(&self) -> anyhow::Result<()> {
        if !self.enabled.load(Ordering::Relaxed) {
            tracing::warn!("Config monitor is disabled, skipping background run");
            // If disabled, wait forever instead of returning
            std::future::pending::<()>().await;
            unreachable!();
        }

        loop {
            if let Err(e) = self.sync_node_params().await {
                tracing::error!("Failed to sync node params: {}", e);
                return Err(e);
            }
            if let Err(e) = self.sync_committee().await {
                tracing::error!("Failed to sync committee: {}", e);
                return Err(e);
            }
            tokio::time::sleep(self.check_interval).await;
        }
    }

    /// Syncs node parameters with on-chain values.
    ///
    /// This checks if the node parameters are in sync with the on-chain parameters.
    /// If not, it updates the node parameters on-chain.
    /// If the current node is not registered yet, it errors out.
    /// If the wallet is not present in the config, it does nothing.
    #[instrument(skip(self))]
    pub async fn sync_node_params(&self) -> anyhow::Result<()> {
        if !self.enabled.load(Ordering::Relaxed) {
            tracing::warn!("Config monitor is disabled, skipping sync");
            return Ok(());
        }

        self.contract_service.sync_node_params(&self.config).await
    }

    /// Refreshes the committee to the latest committee on chain.
    async fn sync_committee(&self) -> anyhow::Result<()> {
        if !self.enabled.load(Ordering::Relaxed) {
            tracing::warn!("Config monitor is disabled, skipping committee sync");
            return Ok(());
        }
        self.committee_service.async_committee_members().await?;

        Ok(())
    }
}

impl std::fmt::Debug for ConfigMonitor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConfigMonitor")
            .field("config", &self.config)
            .field("check_interval", &self.check_interval)
            // Skip contract_service since it's a trait object
            .finish_non_exhaustive()
    }
}

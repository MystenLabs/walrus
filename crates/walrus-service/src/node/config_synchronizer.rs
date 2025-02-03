// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{sync::Arc, time::Duration};

use tracing::{self, instrument};

use super::{
    committee::CommitteeService,
    config::StorageNodeConfig,
    contract_service::SystemContractService,
    StorageNodeError,
};

/// Monitors and syncs node configuration with on-chain parameters.
/// Syncs committee member information with on-chain committee information
pub struct ConfigSynchronizer {
    config: StorageNodeConfig,
    contract_service: Arc<dyn SystemContractService>,
    committee_service: Arc<dyn CommitteeService>,
    check_interval: Duration,
}

impl ConfigSynchronizer {
    /// Creates a new enabled ConfigSynchronizer instance.
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
        }
    }

    /// Runs the config synchronization loop
    /// Errors are ignored except for NodeNeedsReboot and RotationRequired
    pub async fn run(&self) -> Result<(), StorageNodeError> {
        loop {
            tokio::time::sleep(self.check_interval).await;

            if let Err(e) = self.sync_node_params().await {
                if matches!(
                    e,
                    StorageNodeError::NodeNeedsReboot
                        | StorageNodeError::ProtocolKeyPairRotationRequired
                ) {
                    tracing::info!("Going to reboot node due to {}", e);
                    return Err(e);
                }
                tracing::error!("Failed to sync node params: {}", e);
            }
            if let Err(e) = self.committee_service.sync_committee_members().await {
                tracing::error!("Failed to sync committee: {}", e);
            }
        }
    }

    /// Syncs node parameters with on-chain values.
    #[instrument(skip(self))]
    pub async fn sync_node_params(&self) -> Result<(), StorageNodeError> {
        self.contract_service.sync_node_params(&self.config).await
    }
}

impl std::fmt::Debug for ConfigSynchronizer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConfigSynchronizer")
            .field("check_interval", &self.check_interval)
            .field("current_config", &self.config)
            .finish_non_exhaustive()
    }
}

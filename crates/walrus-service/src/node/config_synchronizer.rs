// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{sync::Arc, time::Duration};

use sui_types::base_types::ObjectID;
use tracing::{self, instrument};

use super::{
    committee::CommitteeService,
    config::StorageNodeConfig,
    contract_service::SystemContractService,
    SyncNodeConfigError,
};

/// Monitors and syncs node configuration with on-chain parameters.
/// Syncs committee member information with on-chain committee information
pub struct ConfigSynchronizer {
    config: StorageNodeConfig,
    contract_service: Arc<dyn SystemContractService>,
    committee_service: Arc<dyn CommitteeService>,
    check_interval: Duration,
    node_capability_object_id: ObjectID,
}

impl ConfigSynchronizer {
    /// Creates a new enabled ConfigSynchronizer instance.
    pub fn new(
        config: StorageNodeConfig,
        contract_service: Arc<dyn SystemContractService>,
        committee_service: Arc<dyn CommitteeService>,
        check_interval: Duration,
        node_capability_object_id: ObjectID,
    ) -> Self {
        Self {
            config,
            contract_service,
            committee_service,
            check_interval,
            node_capability_object_id,
        }
    }

    /// Runs the config synchronization loop
    /// Errors are ignored except for NodeNeedsReboot and RotationRequired
    pub async fn run(&self) -> Result<(), SyncNodeConfigError> {
        loop {
            tokio::time::sleep(self.check_interval).await;

            if let Err(e) = self.sync_node_params().await {
                if matches!(
                    e,
                    SyncNodeConfigError::NodeNeedsReboot
                        | SyncNodeConfigError::ProtocolKeyPairRotationRequired
                ) {
                    tracing::info!("going to reboot node due to {}", e);
                    return Err(e);
                }
                tracing::error!("failed to sync node params: {}", e);
            }
            if let Err(e) = self.committee_service.sync_committee_members().await {
                tracing::error!("failed to sync committee: {}", e);
            }
        }
    }

    /// Syncs node parameters with on-chain values.
    #[instrument(skip(self))]
    pub async fn sync_node_params(&self) -> Result<(), SyncNodeConfigError> {
        // self.contract_service
        //     .add_quilt_task(self.node_capability_object_id, ObjectID::random())
        //     .await?;
        self.contract_service
            .sync_node_params(&self.config, self.node_capability_object_id)
            .await
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

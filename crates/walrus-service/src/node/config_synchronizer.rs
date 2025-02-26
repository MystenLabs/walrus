// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{path::PathBuf, sync::Arc, time::Duration};

use async_trait::async_trait;
use sui_types::base_types::ObjectID;
use tracing;

use super::{
    committee::CommitteeService,
    config::StorageNodeConfig,
    contract_service::SystemContractService,
    SyncNodeConfigError,
};
use crate::utils::load_from_yaml;

/// Trait for loading config from some source.
#[async_trait]
pub trait ConfigLoader: std::fmt::Debug + Sync + Send {
    /// Loads the storage node config.
    async fn load_storage_node_config(&self) -> anyhow::Result<StorageNodeConfig>;
}

/// Loads config from a file.
#[derive(Debug, Clone)]
pub struct StorageNodeConfigLoader {
    config_path: PathBuf,
}

impl StorageNodeConfigLoader {
    /// Construct a new config loader for the given path.
    pub fn new(config_path: PathBuf) -> Self {
        Self { config_path }
    }
}

#[async_trait]
impl ConfigLoader for StorageNodeConfigLoader {
    async fn load_storage_node_config(&self) -> anyhow::Result<StorageNodeConfig> {
        Ok(load_from_yaml(&self.config_path)?)
    }
}

/// Monitors and syncs node configuration with on-chain parameters.
/// Syncs committee member information with on-chain committee information
pub struct ConfigSynchronizer {
    contract_service: Arc<dyn SystemContractService>,
    committee_service: Arc<dyn CommitteeService>,
    check_interval: Duration,
    node_capability_object_id: ObjectID,
    config_loader: Option<Arc<dyn ConfigLoader>>,
}

impl ConfigSynchronizer {
    /// Creates a new enabled ConfigSynchronizer instance.
    pub fn new(
        contract_service: Arc<dyn SystemContractService>,
        committee_service: Arc<dyn CommitteeService>,
        check_interval: Duration,
        node_capability_object_id: ObjectID,
        config_loader: Option<Arc<dyn ConfigLoader>>,
    ) -> Self {
        Self {
            contract_service,
            committee_service,
            check_interval,
            node_capability_object_id,
            config_loader,
        }
    }

    /// Runs the config synchronization loop
    /// Errors are ignored except for NodeNeedsReboot and RotationRequired
    pub async fn run(&self) -> Result<(), SyncNodeConfigError> {
        loop {
            tokio::time::sleep(self.check_interval).await;

            tracing::debug!("config_synchronizer: syncing node params");
            if let Err(e) = self.sync_node_params().await {
                if matches!(
                    e,
                    SyncNodeConfigError::NodeNeedsReboot
                        | SyncNodeConfigError::ProtocolKeyPairRotationRequired
                ) {
                    tracing::warn!("going to reboot node due to {}", e);
                    return Err(e);
                }
                tracing::error!("failed to sync node params: {}", e);
            }
            if let Err(e) = self.committee_service.sync_committee_members().await {
                tracing::error!("failed to sync committee: {}", e);
            }
        }
    }

    /// Synchronously syncs the node parameters with the on-chain values.
    pub async fn sync_node_params(&self) -> Result<(), SyncNodeConfigError> {
        if let Some(config_loader) = &self.config_loader {
            let config = config_loader.load_storage_node_config().await?;
            self.contract_service
                .sync_node_params(&config, self.node_capability_object_id)
                .await
        } else {
            tracing::warn!("config loader is not set");
            Ok(())
        }
    }
}

impl std::fmt::Debug for ConfigSynchronizer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConfigSynchronizer")
            .field("check_interval", &self.check_interval)
            .finish_non_exhaustive()
    }
}

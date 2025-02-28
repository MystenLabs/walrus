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
        let mut config: StorageNodeConfig = load_from_yaml(&self.config_path)?;
        config.load_keys()?;
        Ok(config)
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

#[cfg(test)]
mod tests {
    use std::io::Write;

    use serde_yaml;
    use tempfile::TempDir;
    use walrus_core::keys::{NetworkKeyPair, ProtocolKeyPair};
    use walrus_sui::types::{move_structs::VotingParams, NetworkAddress};

    use super::*;
    use crate::node::config::{PathOrInPlace, StorageNodeConfig};

    #[tokio::test]
    async fn test_load_config_and_generate_update_params() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let config_path = temp_dir.path().join("config.yaml");
        let key_path = temp_dir.path().join("protocol_key.key");
        let network_key_path = temp_dir.path().join("network_key.key");

        create_protocol_key_file(&key_path)?;
        create_network_key_file(&network_key_path)?;

        let config = StorageNodeConfig {
            protocol_key_pair: PathOrInPlace::from_path(key_path),
            next_protocol_key_pair: None,
            name: "test-node".to_string(),
            storage_path: temp_dir.path().to_path_buf(),
            network_key_pair: PathOrInPlace::from_path(network_key_path), // Use key from file
            public_host: "localhost".to_string(),
            public_port: 9185,
            ..Default::default()
        };

        // Write config to file.
        let config_str = serde_yaml::to_string(&config)?;
        std::fs::write(&config_path, config_str)?;

        // Create a ConfigLoader to load the config from disk.
        let config_loader = StorageNodeConfigLoader::new(config_path);

        // Load the configuration.
        let loaded_config = config_loader.load_storage_node_config().await?;

        // Call generate_update_params() with the synced config.
        let update_params = loaded_config.generate_update_params(
            "old-name",
            "old-host:8080",
            loaded_config.network_key_pair().public(),
            &VotingParams {
                storage_price: 150,
                write_price: 2300,
                node_capacity: 251_000_000,
            },
        );

        // Verify expected updates are generated.
        assert_eq!(update_params.name, Some("test-node".to_string()));
        assert_eq!(
            update_params.network_address,
            Some(NetworkAddress("localhost:9185".to_string()))
        );
        assert_eq!(
            update_params.storage_price,
            Some(loaded_config.voting_params.storage_price)
        );
        assert_eq!(
            update_params.write_price,
            Some(loaded_config.voting_params.write_price)
        );
        assert_eq!(
            update_params.node_capacity,
            Some(loaded_config.voting_params.node_capacity)
        );

        Ok(())
    }

    fn create_protocol_key_file(path: &std::path::Path) -> anyhow::Result<()> {
        let mut file = std::fs::File::create(path)?;
        file.write_all(ProtocolKeyPair::generate().to_base64().as_bytes())?;
        Ok(())
    }

    fn create_network_key_file(path: &std::path::Path) -> anyhow::Result<()> {
        let mut file = std::fs::File::create(path)?;
        let network_key = NetworkKeyPair::generate();
        file.write_all(network_key.to_base64().as_bytes())?;
        Ok(())
    }
}

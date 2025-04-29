// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use sha2::{Digest, Sha256};
use sui_types::base_types::ObjectID;
use tokio::fs;
use tracing;
use walrus_utils::load_from_yaml;

use super::{
    SyncNodeConfigError,
    committee::CommitteeService,
    config::{StorageNodeConfig, TlsConfig},
    contract_service::SystemContractService,
};

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
/// Syncs committee member information with on-chain committee information.
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
        // Initialize TLS certificate hash.
        let mut cert_hash = None;
        if let Some(config_loader) = &self.config_loader {
            let config = config_loader.load_storage_node_config().await?;
            cert_hash = self.load_tls_cert_hash(&config.tls).await?;
        }

        loop {
            tokio::time::sleep(self.check_interval).await;

            if let Err(error) = self.committee_service.sync_committee_members().await {
                tracing::error!(%error, "failed to sync committee");
            }

            let Some(config_loader) = &self.config_loader else {
                continue;
            };

            let config = config_loader.load_storage_node_config().await?;

            tracing::debug!("config_synchronizer: syncing node params");
            if let Err(error) = self
                .contract_service
                .sync_node_params(&config, self.node_capability_object_id)
                .await
            {
                if matches!(
                    error,
                    SyncNodeConfigError::NodeNeedsReboot
                        | SyncNodeConfigError::ProtocolKeyPairRotationRequired
                ) {
                    tracing::warn!(%error, "going to reboot node");
                    return Err(error);
                }
                tracing::error!(%error, "failed to sync node params");
            }

            let new_cert_hash = self.load_tls_cert_hash(&config.tls).await?;
            if cert_hash != new_cert_hash {
                tracing::info!(
                    old_hash = ?cert_hash,
                    new_hash = ?new_cert_hash,
                    "TLS certificate has changed, node needs reboot"
                );
                return Err(SyncNodeConfigError::NodeNeedsReboot);
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
            Ok(())
        }
    }

    /// Loads and hashes the TLS certificate.
    ///
    /// Returns Ok(None) if TLS is disabled or certificate path is not configured.
    /// Returns Ok(Some(hash)) if certificate is successfully loaded and hashed.
    /// Returns Err for actual errors like file not found or relative paths.
    async fn load_tls_cert_hash(&self, tls_config: &TlsConfig) -> anyhow::Result<Option<Vec<u8>>> {
        // Skip if TLS is disabled or not configured.
        if tls_config.disable_tls {
            return Ok(None);
        }

        let Some(cert_path) = &tls_config.certificate_path else {
            return Ok(None);
        };

        // Return error if the path is relative.
        let cert_path = PathBuf::from(cert_path);
        if cert_path.is_relative() {
            return Err(anyhow!(
                "TLS certificate path must be absolute, got relative path: {}",
                cert_path.display()
            ));
        }

        // Read the certificate file.
        let cert_data = fs::read(&cert_path).await.context(format!(
            "failed to read certificate file at {}",
            cert_path.display()
        ))?;

        // Calculate hash.
        let mut hasher = Sha256::new();
        hasher.update(&cert_data);
        let current_hash = hasher.finalize().to_vec();

        Ok(Some(current_hash))
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
    use walrus_sui::types::{NetworkAddress, move_structs::VotingParams};

    use super::*;
    use crate::node::config::{PathOrInPlace, StorageNodeConfig, SyncedNodeConfigSet};

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

        // Create a SyncedNodeConfigSet with different values.
        let synced_config = SyncedNodeConfigSet {
            name: "old-name".to_string(),
            network_address: NetworkAddress("old-host:8080".to_string()),
            network_public_key: loaded_config.network_key_pair().public().clone(),
            public_key: loaded_config.protocol_key_pair().public().clone(),
            next_public_key: None,
            voting_params: VotingParams {
                storage_price: 150,
                write_price: 2300,
                node_capacity: 251_000_000,
            },
            metadata: Default::default(),
            commission_rate_data: Default::default(),
        };

        // Call generate_update_params() with the synced config.
        let update_params = loaded_config.generate_update_params(&synced_config);

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

    #[tokio::test]
    async fn test_tls_cert_hash_detection() -> anyhow::Result<()> {
        use std::{sync::Arc, time::Duration};

        use crate::node::{
            committee::MockCommitteeService,
            contract_service::MockSystemContractService,
        };

        // Set up test environment with temporary directory and certificate.
        let temp_dir = TempDir::new()?;
        let cert_path = temp_dir.path().join("cert.pem");
        let key_path = temp_dir.path().join("protocol_key.key");
        let network_key_path = temp_dir.path().join("network_key.key");
        let config_path = temp_dir.path().join("config.yaml");

        // Create initial certificate with sample content.
        let initial_cert_content = b"-----BEGIN CERTIFICATE-----\n\
            MIIDazCCAlOgAwIBAgIUJlq+zz4hBJ3ovpEeGFPUc4UdTw8wDQYJKoZIhvcNAQEL\n\
            -----END CERTIFICATE-----\n";
        std::fs::write(&cert_path, initial_cert_content)?;

        // Create key files for test configuration.
        create_protocol_key_file(&key_path)?;
        create_network_key_file(&network_key_path)?;

        // Create storage node config with TLS enabled.
        let config = StorageNodeConfig {
            protocol_key_pair: PathOrInPlace::from_path(key_path),
            next_protocol_key_pair: None,
            name: "test-node".to_string(),
            storage_path: temp_dir.path().to_path_buf(),
            network_key_pair: PathOrInPlace::from_path(network_key_path),
            public_host: "localhost".to_string(),
            public_port: 9185,
            tls: TlsConfig {
                disable_tls: false,
                certificate_path: Some(cert_path.clone()),
            },
            ..Default::default()
        };

        // Write config to file for testing.
        let config_str = serde_yaml::to_string(&config)?;
        std::fs::write(&config_path, config_str)?;

        // Initialize ConfigSynchronizer with mock services.
        let config_loader = Arc::new(StorageNodeConfigLoader::new(config_path.clone()));

        let synchronizer = ConfigSynchronizer::new(
            Arc::new(MockSystemContractService::new()),
            Arc::new(MockCommitteeService::new()),
            Duration::from_millis(100),
            ObjectID::random(),
            Some(config_loader.clone()),
        );

        // Get initial certificate hash.
        let loaded_config = config_loader.load_storage_node_config().await?;
        let cert_hash = synchronizer.load_tls_cert_hash(&loaded_config.tls).await?;
        assert!(cert_hash.is_some(), "cert_hash should be initialized");
        let original_hash = cert_hash.unwrap();

        // Verify hash remains the same when certificate is unchanged.
        let loaded_config = config_loader.load_storage_node_config().await?;
        let result = synchronizer.load_tls_cert_hash(&loaded_config.tls).await?;
        assert!(result.is_some(), "cert_hash should be updated");
        assert_eq!(
            result.unwrap(),
            original_hash,
            "cert_hash should not change on error"
        );

        // Modify the certificate file with different content.
        let modified_cert_content = b"-----BEGIN CERTIFICATE-----\n\
            DIFFERENTCERTIFICATECONTENTHERE1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ\n\
            -----END CERTIFICATE-----\n";
        std::fs::write(&cert_path, modified_cert_content)?;

        // Verify hash is different after certificate change.
        let loaded_config = config_loader.load_storage_node_config().await?;
        let result = synchronizer.load_tls_cert_hash(&loaded_config.tls).await?;
        assert!(result.is_some(), "cert_hash should be updated");
        assert_ne!(
            result.unwrap(),
            original_hash,
            "cert_hash should be updated"
        );

        Ok(())
    }
}

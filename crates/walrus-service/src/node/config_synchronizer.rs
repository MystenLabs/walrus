// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use prometheus::core::{AtomicU64, GenericGauge};
use sui_types::base_types::ObjectID;
use tokio::fs;
use tracing;
use walrus_utils::metrics::Registry;
use x509_cert::certificate::Certificate;

use super::{
    SyncNodeConfigError,
    committee::CommitteeService,
    config::{StorageNodeConfig, TlsConfig},
    contract_service::SystemContractService,
};
use crate::node::wal_price_monitor::WalPriceMonitor;

/// A warning is logged when the TLS certificate expires in less than this duration.
const CERT_EXPIRATION_WARNING_THRESHOLD: Duration = Duration::from_secs(60 * 60 * 24 * 14);

/// The node is rebooted when the TLS certificate expires in less than this multiple of the check
/// interval.
///
/// Setting this to a value above 1 guarantees that the node will be restarted before the
/// certificate expires.
const CERT_EXPIRATION_REBOOT_CHECK_INTERVAL_MULTIPLIER: u32 = 3;

type U64Gauge = GenericGauge<AtomicU64>;

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
        let mut config = StorageNodeConfig::load_config(&self.config_path)?.config;
        config.load_keys()?;
        Ok(config)
    }
}

walrus_utils::metrics::define_metric_set! {
    #[namespace = "walrus"]
    /// Metrics exported by the [`ConfigSynchronizer`].
    struct ConfigSynchronizerMetrics {
        #[help = "The last value for node capacity that was synced to the state on chain"]
        last_synced_node_capacity: U64Gauge[],
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
    wal_price_monitor: Option<Arc<WalPriceMonitor>>,
    metrics: ConfigSynchronizerMetrics,
}

impl ConfigSynchronizer {
    /// Creates a new enabled ConfigSynchronizer instance.
    pub fn new(
        contract_service: Arc<dyn SystemContractService>,
        committee_service: Arc<dyn CommitteeService>,
        check_interval: Duration,
        node_capability_object_id: ObjectID,
        config_loader: Option<Arc<dyn ConfigLoader>>,
        wal_price_monitor: Option<Arc<WalPriceMonitor>>,
        registry: &Registry,
    ) -> Self {
        Self {
            contract_service,
            committee_service,
            check_interval,
            node_capability_object_id,
            config_loader,
            wal_price_monitor,
            metrics: ConfigSynchronizerMetrics::new(registry),
        }
    }

    #[allow(clippy::result_large_err)]
    fn check_tls_cert(
        &self,
        loaded_cert: &Option<Certificate>,
        cert_on_disk: &Option<Certificate>,
    ) -> Result<(), SyncNodeConfigError> {
        match (&loaded_cert, &cert_on_disk) {
            (Some(loaded_cert), Some(cert_on_disk)) => {
                tracing::debug!(
                    ?loaded_cert,
                    ?cert_on_disk,
                    "checking TLS certificate replacement"
                );

                let loaded_cert_expiration = UNIX_EPOCH
                    + loaded_cert
                        .tbs_certificate
                        .validity
                        .not_after
                        .to_unix_duration();
                let cert_on_disk_expiration = UNIX_EPOCH
                    + cert_on_disk
                        .tbs_certificate
                        .validity
                        .not_after
                        .to_unix_duration();
                let now = SystemTime::now();

                // Check time until expiration of the new certificate.
                match cert_on_disk_expiration.duration_since(now) {
                    Ok(duration) if duration < CERT_EXPIRATION_WARNING_THRESHOLD => {
                        tracing::warn!(
                            time_until_expiration = %humantime::format_duration(duration),
                            "the TLS certificate will expire soon, please renew it"
                        );
                    }
                    Err(_) => {
                        tracing::error!(
                            expiration_time = %humantime::format_rfc3339(cert_on_disk_expiration),
                            "the TLS certificate is expired, please renew it"
                        );
                        // There is no point in restarting the node if also the new certificate is
                        // expired.
                        return Ok(());
                    }
                    Ok(_) => {}
                }

                let expiration_restart_threshold =
                    CERT_EXPIRATION_REBOOT_CHECK_INTERVAL_MULTIPLIER * self.check_interval;
                if loaded_cert_expiration
                    .duration_since(now)
                    .unwrap_or_default()
                    < expiration_restart_threshold
                    // Only restart the node if the new certificate is valid for a longer period.
                    && loaded_cert_expiration < cert_on_disk_expiration
                {
                    tracing::info!(
                        old_expiration_time = %humantime::format_rfc3339(loaded_cert_expiration),
                        new_expiration_time = %humantime::format_rfc3339(cert_on_disk_expiration),
                        expiration_restart_threshold = %humantime::format_duration(
                            expiration_restart_threshold
                        ),
                        "the currently loaded TLS certificate is expiring, restarting the node \
                        with the new certificate"
                    );
                    return Err(SyncNodeConfigError::NodeNeedsReboot);
                }

                // Check if the subject or extensions have changed.
                if loaded_cert.tbs_certificate.subject != cert_on_disk.tbs_certificate.subject
                    || loaded_cert.tbs_certificate.extensions
                        != cert_on_disk.tbs_certificate.extensions
                {
                    tracing::info!(
                        old_subject = %loaded_cert.tbs_certificate.subject,
                        new_subject = %cert_on_disk.tbs_certificate.subject,
                        "TLS certificate subject or extensions have changed, reboot required"
                    );
                    return Err(SyncNodeConfigError::NodeNeedsReboot);
                }

                if loaded_cert != cert_on_disk {
                    tracing::debug!(
                        "TLS certificate has been updated, but old certificate is still valid; \
                        skipping restart"
                    );
                }
            }
            (None, Some(_)) => {
                tracing::info!("TLS certificate has been added, reboot required");
                return Err(SyncNodeConfigError::NodeNeedsReboot);
            }
            (Some(_), None) => {
                tracing::info!("TLS certificate has been removed, reboot required");
                return Err(SyncNodeConfigError::NodeNeedsReboot);
            }
            (None, None) => {}
        }

        Ok(())
    }

    /// Runs the config synchronization loop.
    ///
    /// Errors are ignored except for [SyncNodeConfigError::NodeNeedsReboot] and
    /// [SyncNodeConfigError::ProtocolKeyPairRotationRequired].
    pub async fn run(&self) -> Result<(), SyncNodeConfigError> {
        // Initialize TLS certificate.
        let mut cert = None;
        if let Some(config_loader) = &self.config_loader {
            let config = config_loader.load_storage_node_config().await?;
            cert = self.load_tls_cert(&config.tls).await?;
        }

        loop {
            tokio::time::sleep(self.check_interval).await;

            if let Err(error) = self.committee_service.sync_committee_members().await {
                tracing::warn!(%error, "failed to sync committee");
            }

            let Some(config_loader) = &self.config_loader else {
                continue;
            };

            let config = config_loader.load_storage_node_config().await?;
            let wal_price = match self.wal_price_monitor.as_ref() {
                Some(monitor) => monitor.get_current_price().await,
                None => None,
            };

            tracing::debug!("config_synchronizer: syncing node params");
            if let Err(error) = self
                .contract_service
                .sync_node_params(&config, self.node_capability_object_id, wal_price)
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
                tracing::warn!(%error, "failed to sync node params");
            } else {
                self.metrics
                    .last_synced_node_capacity
                    .set(config.voting_params.node_capacity);
            }

            self.check_tls_cert(&cert, &self.load_tls_cert(&config.tls).await?)?;
        }
    }

    /// Synchronously syncs the node parameters with the on-chain values.
    pub async fn sync_node_params(&self) -> Result<(), SyncNodeConfigError> {
        if let Some(config_loader) = &self.config_loader {
            let config = config_loader.load_storage_node_config().await?;
            let wal_price = match self.wal_price_monitor.as_ref() {
                Some(monitor) => monitor.get_current_price().await,
                None => None,
            };
            self.contract_service
                .sync_node_params(&config, self.node_capability_object_id, wal_price)
                .await
        } else {
            Ok(())
        }
    }

    /// Loads and parses the TLS certificate.
    ///
    /// Returns `Ok(None)` if TLS is disabled or no certificate path is configured.
    /// Returns `Ok(Some(_))` if the certificate is loaded and parsed successfully.
    /// Returns `Err` for actual errors like file not found or relative paths.
    async fn load_tls_cert(&self, tls_config: &TlsConfig) -> anyhow::Result<Option<Certificate>> {
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
            "failed to read certificate file at '{}'",
            cert_path.display()
        ))?;

        // Parse the certificate.
        let cert = Certificate::load_pem_chain(&cert_data)?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("there must be at least one certificate present"))?;

        Ok(Some(cert))
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

    use rcgen::CertificateParams;
    use serde_yaml;
    use tempfile::TempDir;
    use walrus_core::keys::{NetworkKeyPair, ProtocolKeyPair};
    use walrus_sui::types::NetworkAddress;
    use walrus_test_utils::async_param_test;

    use super::*;
    use crate::node::{
        committee::MockCommitteeService,
        config::{PathOrInPlace, StorageNodeConfig, SyncedNodeConfigSet, VotingParamsConfig},
        contract_service::MockSystemContractService,
        server,
    };

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
            voting_params: VotingParamsConfig {
                storage_price: 150,
                write_price: 2300,
                node_capacity: 251_000_000,
                stable_pricing_config: None,
            },
            metadata: Default::default(),
            commission_rate_data: Default::default(),
        };

        // Call generate_update_params() with the synced config.
        let update_params = loaded_config.generate_update_params(&synced_config, None);

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

    fn create_network_key_file(path: &std::path::Path) -> anyhow::Result<NetworkKeyPair> {
        let mut file = std::fs::File::create(path)?;
        let network_key = NetworkKeyPair::generate();
        file.write_all(network_key.to_base64().as_bytes())?;
        Ok(network_key)
    }

    async_param_test! {
        test_tls_cert_detection -> anyhow::Result<()>: [
            same_subject_sufficient_validity_1: (
                Duration::from_secs(4),
                Duration::from_secs(4),
                "original-name",
                false
            ),
            same_subject_sufficient_validity_2: (
                Duration::from_secs(5),
                Duration::from_secs(6),
                "original-name",
                false
            ),
            different_subject_sufficient_validity: (
                Duration::from_secs(4),
                Duration::from_secs(4),
                "other-name",
                true
            ),
            same_subject_insufficient_validity_1: (
                Duration::from_secs(1),
                Duration::from_secs(4),
                "original-name",
                true
            ),
            same_subject_insufficient_validity_2: (
                Duration::from_secs(2),
                Duration::from_secs(1),
                "original-name",
                false
            ),
            already_expired: (
                Duration::from_secs(0),
                Duration::from_secs(0),
                "original-name",
                false
            ),
        ]
    }
    async fn test_tls_cert_detection(
        old_validity_duration: Duration,
        new_validity_duration: Duration,
        new_cert_subject: &str,
        expect_restart: bool,
    ) -> anyhow::Result<()> {
        walrus_test_utils::init_tracing();

        // Set up test environment with temporary directory and certificate.
        let temp_dir = TempDir::new()?;
        let cert_path = temp_dir.path().join("cert.pem");
        let key_path = temp_dir.path().join("protocol_key.key");
        let network_key_path = temp_dir.path().join("network_key.key");
        let config_path = temp_dir.path().join("config.yaml");

        // Create key files for test configuration.
        create_protocol_key_file(&key_path)?;
        let network_key_pair = create_network_key_file(&network_key_path)?;
        let network_key_pair_pkcs8 = server::to_pkcs8_key_pair(&network_key_pair);

        // Create initial certificate with sample content.
        let now = SystemTime::now();
        let mut initial_cert_params = CertificateParams::new(vec!["original-name".to_string()])?;
        initial_cert_params.not_after = (now + old_validity_duration).into();
        let initial_cert = initial_cert_params.self_signed(&network_key_pair_pkcs8)?;
        std::fs::write(&cert_path, initial_cert.pem())?;

        // Create storage node config with TLS enabled.
        let config = StorageNodeConfig {
            protocol_key_pair: PathOrInPlace::from_path(key_path),
            storage_path: temp_dir.path().to_path_buf(),
            network_key_pair: PathOrInPlace::from_path(network_key_path),
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
            Duration::from_secs(1),
            ObjectID::random(),
            Some(config_loader.clone()),
            None, // wal_price_monitor
            &Registry::new(prometheus::Registry::new()),
        );

        // Get initial certificate.
        let loaded_config = config_loader.load_storage_node_config().await?;
        let original_cert = synchronizer.load_tls_cert(&loaded_config.tls).await?;
        assert!(original_cert.is_some(), "certificate should be initialized");

        // Replace certificate with new one.
        let mut new_cert_params = CertificateParams::new(vec![new_cert_subject.to_string()])?;
        new_cert_params.not_after = (now + new_validity_duration).into();
        let new_cert = new_cert_params.self_signed(&network_key_pair_pkcs8)?;
        std::fs::write(&cert_path, new_cert.pem())?;

        // Check if certificate is detected as expired.
        let new_cert = synchronizer.load_tls_cert(&loaded_config.tls).await?;
        assert!(new_cert.is_some(), "certificate should be loaded");
        assert!(
            new_cert != original_cert,
            "new certificate should be different"
        );
        let check_result = synchronizer.check_tls_cert(&original_cert, &new_cert);
        println!("check_result: {check_result:?}");

        if expect_restart {
            assert!(
                matches!(check_result, Err(SyncNodeConfigError::NodeNeedsReboot)),
                "expected node to restart"
            );
        } else {
            assert!(matches!(check_result, Ok(())), "expected no restart");
        }

        Ok(())
    }
}

// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Walrus Client Configuration.
use std::{
    collections::HashMap,
    iter::once,
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
};

use anyhow::{Context, Result, anyhow, bail};
use indexmap::IndexSet;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use serde_yaml::Value;
use sui_types::base_types::ObjectID;
use tokio::sync::{Notify, mpsc};
use tracing::Level;
use walrus_sui::{
    client::{
        ReadClient,
        SuiClientError,
        SuiContractClient,
        SuiReadClient,
        contract_config::ContractConfig,
        retry_client::RetriableSuiClient,
    },
    config::WalletConfig,
    wallet::Wallet,
};
use walrus_utils::{
    backoff::ExponentialBackoffConfig,
    config::path_or_defaults_if_exist,
    is_internal_run,
};

use crate::client::{
    byte_range_read_client::ByteRangeReadClientConfig,
    quilt_client::QuiltClientConfig,
    refresh::{CommitteesRefresher, CommitteesRefresherHandle},
};

mod committees_refresh_config;
/// Communication configuration options.
pub mod communication_config;
mod reqwest_config;
mod sliver_write_extra_time;
mod upload_mode;

pub use self::{
    committees_refresh_config::CommitteesRefreshConfig,
    communication_config::{ClientCommunicationConfig, CommunicationLimits, UploadMode},
    reqwest_config::RequestRateConfig,
    sliver_write_extra_time::SliverWriteExtraTime,
    upload_mode::UploadMode as UploadPreset,
};

/// Returns the default paths for the Walrus configuration file.
pub fn default_configuration_paths() -> Vec<PathBuf> {
    const WALRUS_CONFIG_FILE_NAMES: [&str; 2] = ["client_config.yaml", "client_config.yml"];
    let mut directories = vec![PathBuf::from(".")];
    if let Ok(xdg_config_dir) = std::env::var("XDG_CONFIG_HOME") {
        directories.push(xdg_config_dir.into());
    }
    if let Some(home_dir) = home::home_dir() {
        directories.push(home_dir.join(".config").join("walrus"));
        directories.push(home_dir.join(".walrus"));
    }
    directories
        .into_iter()
        .cartesian_product(WALRUS_CONFIG_FILE_NAMES)
        .map(|(directory, file_name)| directory.join(file_name))
        .collect()
}

/// Loads the Walrus configuration from the given path and context.
///
/// If no path is provided, tries to load the configuration first from the local folder, and then
/// from the standard Walrus configuration directory. If the context is not provided, the default
/// context is used.
// NB: When making changes to the logic, make sure to update the argument docs in
// `crates/walrus-service/bin/client.rs`.
pub fn load_configuration(
    path: Option<impl AsRef<Path>>,
    context: Option<&str>,
) -> Result<ClientConfig> {
    let path = path_or_defaults_if_exist(path, &default_configuration_paths())
        .ok_or(anyhow!("could not find a valid Walrus configuration file"))?;
    let (config, context) = ClientConfig::load_from_multi_config(&path, context)?;
    if !is_internal_run() {
        tracing::info!(
            "using Walrus configuration from '{}' with {} context",
            path.display(),
            context.map_or("default".to_string(), |c| format!("'{c}'"))
        );
    }
    Ok(config)
}

/// Config for the client.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct ClientConfig {
    /// The Walrus contract config.
    #[serde(flatten)]
    pub contract_config: ContractConfig,
    /// The WAL exchange object ID.
    #[serde(default)]
    pub exchange_objects: Vec<ObjectID>,
    /// Path to the wallet configuration.
    #[serde(default)]
    pub wallet_config: Option<WalletConfig>,
    /// RPC URLs to use for reads.
    #[serde(default)]
    pub rpc_urls: Vec<String>,
    /// Configuration for the client's network communication.
    #[serde(default)]
    pub communication_config: ClientCommunicationConfig,
    /// The configuration of the committee refresh from chain.
    #[serde(default)]
    pub refresh_config: CommitteesRefreshConfig,
    /// The configuration of the QuiltClient.
    #[serde(default)]
    pub quilt_client_config: QuiltClientConfig,
    /// The configuration of the ByteRangeReadClient.
    #[serde(default)]
    pub byte_range_read_client_config: ByteRangeReadClientConfig,
}

impl ClientConfig {
    /// Creates a new client config from a contract config, using default values for the other
    /// fields.
    pub fn new_from_contract_config(contract_config: ContractConfig) -> Self {
        Self {
            contract_config,
            exchange_objects: Default::default(),
            wallet_config: Default::default(),
            rpc_urls: Default::default(),
            communication_config: Default::default(),
            refresh_config: Default::default(),
            quilt_client_config: Default::default(),
            byte_range_read_client_config: Default::default(),
        }
    }

    fn apply_upload_mode_preset(&mut self) {
        if let Some(mode) = self.communication_config.upload_mode {
            let preset: UploadPreset = mode.into();
            let communication_config = std::mem::take(&mut self.communication_config);
            self.communication_config = preset.apply_to(communication_config);
        }
    }

    /// Loads the Walrus client configuration from the given path along with a context. If the file
    /// is a multi-config file, the context argument can be used to override the default context.
    pub fn load_from_multi_config(
        path: impl AsRef<Path>,
        context: Option<&str>,
    ) -> anyhow::Result<(Self, Option<String>)> {
        let path = path.as_ref();
        let config = match load_multi_config_with_defaults(path) {
            Ok(config) => config,
            Err(e) => {
                bail!(
                    "unable to parse the client config file: [config_filename='{}', error='{}']\n\
                    see https://docs.wal.app/usage/setup.html#configuration for the correct format",
                    path.display(),
                    e
                )
            }
        };
        match config {
            MultiClientConfig::SingletonConfig(mut config) => {
                if let Some(context) = context {
                    bail!(
                        "cannot specify context when using a single-context configuration file \
                        [config_filename='{}', specified_context='{}']",
                        path.display(),
                        context,
                    )
                }
                config.apply_upload_mode_preset();
                Ok((config, None))
            }
            MultiClientConfig::MultiConfig {
                contexts,
                default_context,
            } => {
                let target_context = context.unwrap_or(&default_context);
                let mut config = contexts.get(target_context).cloned().ok_or_else(|| {
                    anyhow::anyhow!(
                        "context '{}' not found in multi-config file '{}'. available context(s): \
                        [{}]",
                        target_context,
                        path.display(),
                        contexts.keys().map(|x| format!("'{x}'")).join(", ")
                    )
                })?;
                config.apply_upload_mode_preset();
                Ok((config, Some(target_context.to_string())))
            }
        }
    }

    /// Creates a [`SuiReadClient`] based on the configuration.
    pub async fn new_read_client(
        &self,
        sui_client: RetriableSuiClient,
    ) -> Result<SuiReadClient, SuiClientError> {
        SuiReadClient::new(sui_client, &self.contract_config).await
    }

    /// Creates a [`SuiContractClient`] based on the configuration.
    pub async fn new_contract_client(
        &self,
        wallet: Wallet,
        gas_budget: Option<u64>,
    ) -> Result<SuiContractClient, SuiClientError> {
        let wallet_rpc_url = wallet.get_rpc_url().to_string();

        SuiContractClient::new(
            wallet,
            &combine_rpc_urls(wallet_rpc_url, &self.rpc_urls),
            &self.contract_config,
            self.backoff_config().clone(),
            gas_budget,
        )
        .await
    }

    /// Creates a [`SuiContractClient`] with a wallet configured in the client config.
    ///
    /// Returns an error if the client configuration does not contain a path to a valid Sui wallet
    /// configuration.
    pub async fn new_contract_client_with_wallet_in_config(
        &self,
        gas_budget: Option<u64>,
    ) -> anyhow::Result<SuiContractClient> {
        let wallet = WalletConfig::load_wallet(
            self.wallet_config.as_ref(),
            self.communication_config.sui_client_request_timeout,
        )
        .context("new_contract_client_with_wallet_in_config")?;
        Ok(self.new_contract_client(wallet, gas_budget).await?)
    }

    /// Returns a reference to the backoff configuration.
    pub fn backoff_config(&self) -> &ExponentialBackoffConfig {
        &self.communication_config.request_rate_config.backoff_config
    }

    /// Builds a new [`CommitteesRefresher`], spawns it on a separate task, and
    /// returns the [`CommitteesRefresherHandle`].
    #[tracing::instrument(skip_all, level = Level::DEBUG)]
    pub async fn build_refresher_and_run(
        &self,
        sui_client: impl ReadClient + 'static,
    ) -> Result<CommitteesRefresherHandle> {
        tracing::debug!("building a new committees refresher");
        let n_shards = if let Some(n_shards) = self.contract_config.n_shards {
            n_shards
        } else {
            sui_client
                .n_shards()
                .await
                .context("failed to determine n_shards before starting refresher")?
        };

        let notify = Arc::new(Notify::new());
        let (req_tx, req_rx) = mpsc::channel(self.refresh_config.refresher_channel_size);
        let handle = CommitteesRefresherHandle::new(notify.clone(), req_tx, n_shards);
        let config = self.refresh_config.clone();

        tokio::spawn(async move {
            if let Err(error) = async {
                tracing::debug!("building the committees refresher in a background task");
                let mut refresher =
                    CommitteesRefresher::new(config, sui_client, req_rx, notify).await?;
                tracing::debug!("running the committees refresher");
                refresher.run().await;
                Ok::<(), anyhow::Error>(())
            }
            .await
            {
                tracing::error!(?error, "error building or running the committees refresher");
            }
        });

        Ok(handle)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClientNetworkKind {
    Mainnet,
    Testnet,
    Default,
}

const MAINNET_CLIENT_CONFIG_YAML: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../setup/client_config_mainnet.yaml"
));
const TESTNET_CLIENT_CONFIG_YAML: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../setup/client_config_testnet.yaml"
));

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct ContractIds {
    system_object: ObjectID,
    staking_object: ObjectID,
}

#[derive(Debug)]
struct KnownNetworkIds {
    mainnet: Option<ContractIds>,
    testnet: Option<ContractIds>,
}

fn known_network_ids() -> &'static KnownNetworkIds {
    static KNOWN: OnceLock<KnownNetworkIds> = OnceLock::new();
    KNOWN.get_or_init(|| KnownNetworkIds {
        mainnet: load_contract_ids("mainnet", MAINNET_CLIENT_CONFIG_YAML),
        testnet: load_contract_ids("testnet", TESTNET_CLIENT_CONFIG_YAML),
    })
}

fn load_contract_ids(label: &'static str, yaml: &'static str) -> Option<ContractIds> {
    match serde_yaml::from_str::<ContractIds>(yaml) {
        Ok(ids) => Some(ids),
        Err(err) => {
            tracing::warn!(
                error = %err,
                "failed to parse contract ids from {label} client config"
            );
            None
        }
    }
}

fn detect_network_kind(config_value: &Value) -> ClientNetworkKind {
    let Some(config_ids) = extract_contract_ids(config_value) else {
        return ClientNetworkKind::Default;
    };
    let known = known_network_ids();
    if known.mainnet.as_ref() == Some(&config_ids) {
        return ClientNetworkKind::Mainnet;
    }
    if known.testnet.as_ref() == Some(&config_ids) {
        return ClientNetworkKind::Testnet;
    }
    ClientNetworkKind::Default
}

fn extract_contract_ids(config_value: &Value) -> Option<ContractIds> {
    let map = config_value.as_mapping()?;
    let system_object = object_id_from_value(map.get(Value::String("system_object".into()))?)?;
    let staking_object = object_id_from_value(map.get(Value::String("staking_object".into()))?)?;
    Some(ContractIds {
        system_object,
        staking_object,
    })
}

fn object_id_from_value(value: &Value) -> Option<ObjectID> {
    value
        .as_str()
        .and_then(|value| ObjectID::from_hex_literal(value).ok())
}

fn defaults_value_for(kind: ClientNetworkKind) -> Option<Value> {
    match kind {
        ClientNetworkKind::Mainnet => load_defaults_value("mainnet", MAINNET_CLIENT_CONFIG_YAML),
        ClientNetworkKind::Testnet => load_defaults_value("testnet", TESTNET_CLIENT_CONFIG_YAML),
        ClientNetworkKind::Default => None,
    }
}

fn load_defaults_value(label: &'static str, yaml: &'static str) -> Option<Value> {
    match serde_yaml::from_str::<Value>(yaml) {
        Ok(value) => Some(value),
        Err(err) => {
            tracing::warn!(
                error = %err,
                "failed to parse default values from {label} client config"
            );
            None
        }
    }
}

fn apply_network_defaults(mut raw_value: Value) -> Value {
    if let Value::Mapping(map) = &mut raw_value
        && let Some(Value::Mapping(contexts)) = map.get_mut(Value::String("contexts".to_string()))
    {
        for (_, context_value) in contexts.iter_mut() {
            apply_network_defaults_to_value(context_value);
        }
        return raw_value;
    }
    apply_network_defaults_to_value(&mut raw_value);
    raw_value
}

fn apply_network_defaults_to_value(value: &mut Value) {
    let kind = detect_network_kind(value);
    let Some(mut defaults_value) = defaults_value_for(kind) else {
        return;
    };
    let user_value = std::mem::replace(value, Value::Null);
    merge_yaml(&mut defaults_value, user_value);
    *value = defaults_value;
}

fn load_multi_config_with_defaults(path: &Path) -> anyhow::Result<MultiClientConfig> {
    let config_str = std::fs::read_to_string(path)
        .with_context(|| format!("unable to load config from {}", path.display()))?;
    let raw_value: Value = serde_yaml::from_str(&config_str)
        .with_context(|| format!("unable to parse config from {}", path.display()))?;
    let merged_value = apply_network_defaults(raw_value);
    serde_yaml::from_value(merged_value).with_context(|| {
        format!(
            "unable to deserialize merged config from {}",
            path.display()
        )
    })
}

fn merge_yaml(base: &mut Value, overlay: Value) {
    match (base, overlay) {
        (Value::Mapping(base_map), Value::Mapping(overlay_map)) => {
            for (key, value) in overlay_map {
                match base_map.get_mut(&key) {
                    Some(base_value) => merge_yaml(base_value, value),
                    None => {
                        base_map.insert(key, value);
                    }
                }
            }
        }
        (base_value, overlay_value) => {
            *base_value = overlay_value;
        }
    }
}

/// Combines the main RPC URL with additional RPC endpoints, ensuring uniqueness of each URL string.
pub fn combine_rpc_urls(rpc: impl AsRef<str>, additional_rpc_endpoints: &[String]) -> Vec<String> {
    once(rpc.as_ref().to_string())
        .chain(additional_rpc_endpoints.iter().cloned())
        .collect::<IndexSet<String>>()
        .into_iter()
        .collect::<Vec<_>>()
}

/// Multi config for the client.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
pub enum MultiClientConfig {
    /// A configuration with a single context.
    SingletonConfig(ClientConfig),
    /// A configuration with potentially multiple contexts.
    MultiConfig {
        /// The contexts for the configuration, the intent here is to enable configuration for
        /// multiple Walrus networks within one configuration.
        contexts: HashMap<String, ClientConfig>,
        /// The default context to use if none is specified.
        default_context: String,
    },
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU16;

    use indoc::indoc;
    use rand::{SeedableRng as _, rngs::StdRng};
    use tempfile::TempDir;
    use walrus_sui::{
        client::contract_config::ContractConfig,
        test_utils::system_setup::DEFAULT_MAX_EPOCHS_AHEAD,
    };
    use walrus_test_utils::{Result as TestResult, param_test};

    use super::*;

    /// Serializes a default config to the example file when tests are run.
    ///
    /// This test ensures that the `client_config_example.yaml` is kept in sync with the config
    /// struct in this file.
    #[test]
    fn check_and_update_example_client_config() -> TestResult {
        const EXAMPLE_CONFIG_PATH: &str = "client_config_example.yaml";

        let mut rng = StdRng::seed_from_u64(42);
        let contract_config = ContractConfig {
            n_shards: Some(NonZeroU16::new(1000).expect("1000 is non-zero")),
            max_epochs_ahead: Some(DEFAULT_MAX_EPOCHS_AHEAD),
            ..ContractConfig::new(
                ObjectID::random_from_rng(&mut rng),
                ObjectID::random_from_rng(&mut rng),
            )
        };
        let config = ClientConfig {
            contract_config,
            exchange_objects: vec![
                ObjectID::random_from_rng(&mut rng),
                ObjectID::random_from_rng(&mut rng),
            ],
            wallet_config: None,
            rpc_urls: vec!["https://fullnode.testnet.sui.io:443".into()],
            communication_config: Default::default(),
            refresh_config: Default::default(),
            quilt_client_config: Default::default(),
            byte_range_read_client_config: Default::default(),
        };

        walrus_test_utils::overwrite_file_and_fail_if_not_equal(
            EXAMPLE_CONFIG_PATH,
            serde_yaml::to_string(&config)?,
        )
        .expect("overwrite failed");

        Ok(())
    }

    param_test! {
        check_client_config -> TestResult: [
            testnet: ("../../setup/client_config_testnet.yaml", None, None),
            mainnet: ("../../setup/client_config_mainnet.yaml", None, None),
            multi_config: ("../../setup/client_config.yaml", None, Some("testnet")),
            multi_config_with_testnet_context: (
                "../../setup/client_config.yaml",
                Some("testnet"),
                Some("testnet"),
            ),
            multi_config_with_mainnet_context: (
                "../../setup/client_config.yaml",
                Some("mainnet"),
                Some("mainnet"),
            ),
        ]
    }
    /// This test ensures that the client configurations contained in our documentation are valid.
    fn check_client_config(
        path: &str,
        input_context: Option<&str>,
        expected_context: Option<&str>,
    ) -> TestResult {
        let (_config, context) = ClientConfig::load_from_multi_config(path, input_context)?;
        assert_eq!(context.as_deref(), expected_context);
        Ok(())
    }

    #[test]
    fn parses_minimal_config_file() -> TestResult {
        let yaml = indoc! {"
            system_object: 0xa2637d13d171b278eadfa8a3fbe8379b5e471e1f3739092e5243da17fc8090eb
            staking_object: 0xca7cf321e47a1fc9bfd032abc31b253f5063521fd5b4c431f2cdd3fee1b4ec00
        "};

        let _: ClientConfig = serde_yaml::from_str(yaml)?;

        Ok(())
    }

    #[test]
    fn parses_no_exchange_object_config_file() -> TestResult {
        let yaml = indoc! {"
            system_object: 0xa2637d13d171b278eadfa8a3fbe8379b5e471e1f3739092e5243da17fc8090eb
            staking_object: 0xca7cf321e47a1fc9bfd032abc31b253f5063521fd5b4c431f2cdd3fee1b4ec00
            exchange_objects: []
        "};

        let config: ClientConfig = serde_yaml::from_str(yaml)?;
        assert!(config.exchange_objects.is_empty());

        Ok(())
    }

    #[test]
    fn parses_old_config_file_containing_subsidies_object_without_error() -> TestResult {
        let yaml = indoc! {"
            system_object: 0xa2637d13d171b278eadfa8a3fbe8379b5e471e1f3739092e5243da17fc8090eb
            staking_object: 0xca7cf321e47a1fc9bfd032abc31b253f5063521fd5b4c431f2cdd3fee1b4ec00
            subsidies_object: 0xca7cf321e47a1fc9bfd032abc31b253f5063521fd5b4c431f2cdd3fee1b4ec00
            exchange_objects:
                - 0xa9b00f69d3b033e7b64acff2672b54fbb7c31361954251e235395dea8bd6dcac
        "};

        let _config: ClientConfig = serde_yaml::from_str(yaml)?;

        Ok(())
    }

    #[test]
    fn parses_single_exchange_object_config_file() -> TestResult {
        let yaml = indoc! {"
            system_object: 0xa2637d13d171b278eadfa8a3fbe8379b5e471e1f3739092e5243da17fc8090eb
            staking_object: 0xca7cf321e47a1fc9bfd032abc31b253f5063521fd5b4c431f2cdd3fee1b4ec00
            exchange_objects:
                - 0xa9b00f69d3b033e7b64acff2672b54fbb7c31361954251e235395dea8bd6dcac
        "};

        let config: ClientConfig = serde_yaml::from_str(yaml)?;
        assert_eq!(config.exchange_objects.len(), 1);

        Ok(())
    }

    #[test]
    fn parses_multiple_exchange_objects_config_file() -> TestResult {
        let yaml = indoc! {"
            system_object: 0xa2637d13d171b278eadfa8a3fbe8379b5e471e1f3739092e5243da17fc8090eb
            staking_object: 0xca7cf321e47a1fc9bfd032abc31b253f5063521fd5b4c431f2cdd3fee1b4ec00
            exchange_objects:
                - 0xa9b00f69d3b033e7b64acff2672b54fbb7c31361954251e235395dea8bd6dcac
                - 0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef
        "};

        let config: ClientConfig = serde_yaml::from_str(yaml)?;
        assert_eq!(config.exchange_objects.len(), 2);

        Ok(())
    }

    #[test]
    fn parses_partial_config_file() -> TestResult {
        let yaml = indoc! {"
            system_object: 0xa2637d13d171b278eadfa8a3fbe8379b5e471e1f3739092e5243da17fc8090eb
            staking_object: 0xca7cf321e47a1fc9bfd032abc31b253f5063521fd5b4c431f2cdd3fee1b4ec00
            exchange_objects:
                - 0xa9b00f69d3b033e7b64acff2672b54fbb7c31361954251e235395dea8bd6dcac
            wallet_config: path/to/wallet
            communication_config:
                max_concurrent_writes: 42
                max_data_in_flight: null
                reqwest_config:
                    total_timeout_millis: 30000
                    http2_keep_alive_while_idle: false
                request_rate_config:
                    max_node_connections: 10
                    backoff_config:
                        min_backoff_millis: 1000
                disable_proxy: false
                sliver_write_extra_time:
                    factor: 0.5
                max_total_blob_size: 1073741824
        "};

        let _: ClientConfig = serde_yaml::from_str(yaml)?;

        Ok(())
    }

    #[test]
    fn parses_singleton_config_specified_erroneous_context() -> TestResult {
        let dir = TempDir::new()?;
        let filename = dir.path().join("client_config.yaml");

        let yaml = indoc! {"
            system_object: 0xa2637d13d171b278eadfa8a3fbe8379b5e471e1f3739092e5243da17fc8090eb
            staking_object: 0xca7cf321e47a1fc9bfd032abc31b253f5063521fd5b4c431f2cdd3fee1b4ec00
            exchange_objects:
                - 0xa9b00f69d3b033e7b64acff2672b54fbb7c31361954251e235395dea8bd6dcac
        "};
        std::fs::write(filename.as_path(), yaml.as_bytes())?;

        let result = ClientConfig::load_from_multi_config(filename, Some("does-not-exist"));
        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn parses_multi_config_default_context_with_one() -> TestResult {
        let dir = TempDir::new()?;
        let filename = dir.path().join("client_config.yaml");

        // editorconfig-checker-disable
        let yaml = indoc! {"
            contexts:
                banana:
                    system_object: 0xa2637d13d171b278eadfa8a3fbe8379b5e471e1f3739092e5243da17fc8090eb
                    staking_object: 0xca7cf321e47a1fc9bfd032abc31b253f5063521fd5b4c431f2cdd3fee1b4ec00
                    exchange_objects:
                        - 0xa9b00f69d3b033e7b64acff2672b54fbb7c31361954251e235395dea8bd6dcac
            default_context: banana
        "};
        // editorconfig-checker-enable
        std::fs::write(filename.as_path(), yaml.as_bytes())?;

        let (config, context) = ClientConfig::load_from_multi_config(filename, None)?;
        assert_eq!(config.exchange_objects.len(), 1);
        assert_eq!(context.as_deref(), Some("banana"));

        Ok(())
    }

    #[test]
    fn parses_multi_config_default_context_with_two() -> TestResult {
        let dir = TempDir::new()?;
        let filename = dir.path().join("client_config.yaml");

        // editorconfig-checker-disable
        let yaml = indoc! {"
            contexts:
                banana:
                    system_object: 0xa2637d13d171b278eadfa8a3fbe8379b5e471e1f3739092e5243da17fc8090eb
                    staking_object: 0xca7cf321e47a1fc9bfd032abc31b253f5063521fd5b4c431f2cdd3fee1b4ec00
                    exchange_objects:
                        - 0xa9b00f69d3b033e7b64acff2672b54fbb7c31361954251e235395dea8bd6dcac
                apple:
                    system_object: 0xa2637d13d171b278eadfa8a3fbe8379b5e471e1f3739092e5243da17fc8090eb
                    staking_object: 0xca7cf321e47a1fc9bfd032abc31b253f5063521fd5b4c431f2cdd3fee1b4ec00
                    exchange_objects:
                        - 0xa9b00f69d3b033e7b64acff2672b54fbb7c31361954251e235395dea8bd6dcac
                        - 0xb9b00f69d3b033e7b64acff2672b54fbb7c31361954251e235395dea8bd6dcac
            default_context: apple
        "};
        // editorconfig-checker-enable
        std::fs::write(filename.as_path(), yaml.as_bytes())?;

        let (config, context) = ClientConfig::load_from_multi_config(filename, None)?;
        assert_eq!(config.exchange_objects.len(), 2);
        assert_eq!(context.as_deref(), Some("apple"));

        Ok(())
    }

    #[test]
    fn parses_multi_config_specified_context_with_two() -> TestResult {
        let dir = TempDir::new()?;
        let filename = dir.path().join("client_config.yaml");

        // editorconfig-checker-disable
        let yaml = indoc! {"
            contexts:
                banana:
                    system_object: 0xa2637d13d171b278eadfa8a3fbe8379b5e471e1f3739092e5243da17fc8090eb
                    staking_object: 0xca7cf321e47a1fc9bfd032abc31b253f5063521fd5b4c431f2cdd3fee1b4ec00
                    exchange_objects:
                        - 0xa9b00f69d3b033e7b64acff2672b54fbb7c31361954251e235395dea8bd6dcac
                apple:
                    system_object: 0xa2637d13d171b278eadfa8a3fbe8379b5e471e1f3739092e5243da17fc8090eb
                    staking_object: 0xca7cf321e47a1fc9bfd032abc31b253f5063521fd5b4c431f2cdd3fee1b4ec00
                    exchange_objects:
                        - 0xa9b00f69d3b033e7b64acff2672b54fbb7c31361954251e235395dea8bd6dcac
                        - 0xb9b00f69d3b033e7b64acff2672b54fbb7c31361954251e235395dea8bd6dcac
            default_context: apple
        "};
        // editorconfig-checker-enable
        std::fs::write(filename.as_path(), yaml.as_bytes())?;

        let (config, context) = ClientConfig::load_from_multi_config(filename, Some("banana"))?;
        assert_eq!(config.exchange_objects.len(), 1);
        assert_eq!(context.as_deref(), Some("banana"));

        Ok(())
    }

    #[test]
    fn parses_multi_config_specified_erroneous_context() -> TestResult {
        let dir = TempDir::new()?;
        let filename = dir.path().join("client_config.yaml");

        // editorconfig-checker-disable
        let yaml = indoc! {"
            contexts:
                banana:
                    system_object: 0xa2637d13d171b278eadfa8a3fbe8379b5e471e1f3739092e5243da17fc8090eb
                    staking_object: 0xca7cf321e47a1fc9bfd032abc31b253f5063521fd5b4c431f2cdd3fee1b4ec00
                    exchange_objects:
                        - 0xa9b00f69d3b033e7b64acff2672b54fbb7c31361954251e235395dea8bd6dcac
                apple:
                    system_object: 0xa2637d13d171b278eadfa8a3fbe8379b5e471e1f3739092e5243da17fc8090eb
                    staking_object: 0xca7cf321e47a1fc9bfd032abc31b253f5063521fd5b4c431f2cdd3fee1b4ec00
                    exchange_objects:
                        - 0xa9b00f69d3b033e7b64acff2672b54fbb7c31361954251e235395dea8bd6dcac
                        - 0xb9b00f69d3b033e7b64acff2672b54fbb7c31361954251e235395dea8bd6dcac
            default_context: apple
        "};
        // editorconfig-checker-enable
        std::fs::write(filename.as_path(), yaml.as_bytes())?;

        let result = ClientConfig::load_from_multi_config(filename, Some("grape"));
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn test_combine_rpc_urls() {
        let rpc = "http://localhost:1".to_string();
        let rpc_urls = vec![
            "http://localhost:2".to_string(),
            "http://localhost:2".to_string(),
            "http://localhost:3".to_string(),
            "http://localhost:1".to_string(),
            "http://localhost:3".to_string(),
        ];

        // Check that the duplicates are removed and the order is preserved.
        let combined = super::combine_rpc_urls(&rpc, &rpc_urls);
        assert_eq!(combined.len(), 3);
        assert_eq!(combined[0], "http://localhost:1");
        assert_eq!(combined[1], "http://localhost:2");
        assert_eq!(combined[2], "http://localhost:3");
    }
}

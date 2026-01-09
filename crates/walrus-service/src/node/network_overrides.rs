// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Network-specific defaults for the storage node config.

use std::sync::OnceLock;

use serde::Deserialize;
use serde_yaml::Value;
use sui_types::base_types::ObjectID;

use crate::node::config::StorageNodeConfig;

const MAINNET_CLIENT_CONFIG_YAML: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../setup/client_config_mainnet.yaml"
));
const TESTNET_CLIENT_CONFIG_YAML: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../setup/client_config_testnet.yaml"
));

/// Known network categories for per-network defaults.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetworkKind {
    /// Mainnet network kind.
    Mainnet,
    /// Testnet network kind.
    Testnet,
    /// Tests network kind.
    Tests,
    /// Simtest network kind.
    Simtest,
    /// The default network kind.
    Default,
}

/// Detects the network kind based on build flags and the provided config value.
pub(crate) fn detect_network_kind(config_value: &Value) -> NetworkKind {
    let Some(config_ids) = extract_contract_ids(config_value) else {
        return default_network_kind();
    };
    let known = known_network_ids();
    if known.mainnet.as_ref() == Some(&config_ids) {
        return NetworkKind::Mainnet;
    }
    if known.testnet.as_ref() == Some(&config_ids) {
        return NetworkKind::Testnet;
    }
    default_network_kind()
}

pub(crate) fn default_network_kind() -> NetworkKind {
    if cfg!(msim) {
        return NetworkKind::Simtest;
    }
    if cfg!(test) {
        return NetworkKind::Tests;
    }
    NetworkKind::Default
}

/// Returns per-network defaults for the storage node config.
pub fn defaults_for(kind: NetworkKind) -> StorageNodeConfig {
    match kind {
        NetworkKind::Mainnet => StorageNodeConfig::default_mainnet(),
        NetworkKind::Testnet => StorageNodeConfig::default_testnet(),
        NetworkKind::Tests => StorageNodeConfig::default_tests(),
        NetworkKind::Simtest => StorageNodeConfig::default_simtest(),
        NetworkKind::Default => StorageNodeConfig::default(),
    }
}

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

fn extract_contract_ids(config_value: &Value) -> Option<ContractIds> {
    let sui = config_value
        .as_mapping()?
        .get(Value::String("sui".to_string()))?;
    let sui_map = sui.as_mapping()?;
    let system_object = object_id_from_value(sui_map.get(Value::String("system_object".into()))?)?;
    let staking_object =
        object_id_from_value(sui_map.get(Value::String("staking_object".into()))?)?;
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use anyhow::Result;
    use rand::{SeedableRng as _, rngs::StdRng};

    use super::*;

    #[test]
    fn test_defaults_for_simtest_include_defaults() {
        let config = defaults_for(NetworkKind::Simtest);
        assert!(config.live_upload_deferral.enabled);
        assert_eq!(
            config.blob_recovery.monitor_interval,
            Duration::from_secs(5)
        );
        assert_eq!(
            config.shard_sync_config.shard_sync_retry_min_backoff,
            Duration::from_secs(1)
        );
    }

    #[test]
    fn test_defaults_for_tests_include_defaults() {
        let config = defaults_for(NetworkKind::Tests);
        assert!(config.live_upload_deferral.enabled);
        assert_eq!(
            config.blob_recovery.monitor_interval,
            Duration::from_secs(5)
        );
        assert!(config.consistency_check.enable_blob_info_invariants_check);
    }

    #[test]
    fn test_detects_network_kind_from_ids() -> Result<()> {
        let known = known_network_ids();
        let mainnet = known.mainnet.as_ref().expect("mainnet ids available");
        let testnet = known.testnet.as_ref().expect("testnet ids available");
        let mut rng = StdRng::seed_from_u64(42);

        let mainnet_yaml = format!(
            "sui:\n  system_object: {}\n  staking_object: {}\n",
            mainnet.system_object, mainnet.staking_object
        );
        let testnet_yaml = format!(
            "sui:\n  system_object: {}\n  staking_object: {}\n",
            testnet.system_object, testnet.staking_object
        );
        let unknown_yaml = format!(
            "sui:\n  system_object: {}\n  staking_object: {}\n",
            ObjectID::random_from_rng(&mut rng),
            ObjectID::random_from_rng(&mut rng)
        );

        let mainnet_value: Value = serde_yaml::from_str(&mainnet_yaml)?;
        let testnet_value: Value = serde_yaml::from_str(&testnet_yaml)?;
        let unknown_value: Value = serde_yaml::from_str(&unknown_yaml)?;

        assert_eq!(detect_network_kind(&mainnet_value), NetworkKind::Mainnet);
        assert_eq!(detect_network_kind(&testnet_value), NetworkKind::Testnet);
        assert_eq!(detect_network_kind(&unknown_value), NetworkKind::Tests);
        Ok(())
    }
}

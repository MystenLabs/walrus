// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Network overrides for the storage node config.

use std::sync::OnceLock;

use anyhow::{Context, Result, anyhow};
use serde::Deserialize;
use serde_yaml::{Mapping, Value};
use sui_types::base_types::ObjectID;

const MAINNET_CLIENT_CONFIG_YAML: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../setup/client_config_mainnet.yaml"
));
const TESTNET_CLIENT_CONFIG_YAML: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../setup/client_config_testnet.yaml"
));
const MAINNET_OVERRIDES_YAML: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../overrides/mainnet.yaml"
));
const TESTNET_OVERRIDES_YAML: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../overrides/testnet.yaml"
));
const TESTS_OVERRIDES_YAML: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../overrides/tests.yaml"
));
const SIMTEST_OVERRIDES_YAML: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../overrides/simtest.yaml"
));

/// Known network categories for per-network overrides.
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
pub fn detect_network_kind(config_value: &Value) -> NetworkKind {
    if cfg!(msim) {
        return NetworkKind::Simtest;
    }
    if cfg!(test) {
        return NetworkKind::Tests;
    }
    detect_network_kind_from_ids(config_value)
}

fn detect_network_kind_from_ids(config_value: &Value) -> NetworkKind {
    let Some(config_ids) = extract_contract_ids(config_value) else {
        return NetworkKind::Default;
    };
    let known = known_network_ids();
    if known.mainnet.as_ref() == Some(&config_ids) {
        return NetworkKind::Mainnet;
    }
    if known.testnet.as_ref() == Some(&config_ids) {
        return NetworkKind::Testnet;
    }
    NetworkKind::Default
}

/// Returns per-network overrides as a YAML value.
pub fn overrides_for(kind: NetworkKind) -> Result<Value> {
    let overrides_yaml = match kind {
        NetworkKind::Mainnet => MAINNET_OVERRIDES_YAML,
        NetworkKind::Testnet => TESTNET_OVERRIDES_YAML,
        NetworkKind::Tests => TESTS_OVERRIDES_YAML,
        NetworkKind::Simtest => SIMTEST_OVERRIDES_YAML,
        NetworkKind::Default => "",
    };
    let overrides = parse_overrides_yaml(overrides_yaml, kind)?;
    Ok(Value::Mapping(overrides))
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

fn parse_overrides_yaml(yaml: &str, kind: NetworkKind) -> Result<Mapping> {
    if yaml.trim().is_empty() {
        return Ok(Mapping::new());
    }
    let value: Value = serde_yaml::from_str(yaml)
        .with_context(|| format!("failed to parse overrides for {kind:?}"))?;
    match value {
        Value::Null => Ok(Mapping::new()),
        Value::Mapping(map) => Ok(map),
        _ => Err(anyhow!("overrides for {kind:?} must be a mapping or empty")),
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
    use std::path::Path;

    use rand::{SeedableRng as _, rngs::StdRng};

    use super::*;

    #[test]
    fn test_overrides_for_dir_accepts_dummy_overrides() -> Result<()> {
        let dir = tempfile::tempdir()?;
        std::fs::write(
            dir.path().join("tests.yaml"),
            "live_upload_deferral:\n  enabled: true\n  max_total_defer_millis: 50\n",
        )?;

        let overrides = overrides_for_dir(NetworkKind::Tests, dir.path())?;
        let Value::Mapping(map) = overrides else {
            panic!("expected overrides mapping for Tests");
        };
        let live_upload_deferral = map
            .get(Value::String("live_upload_deferral".to_string()))
            .expect("live_upload_deferral should exist")
            .as_mapping()
            .expect("live_upload_deferral should be a mapping");
        let max_total_defer_millis = live_upload_deferral
            .get(Value::String("max_total_defer_millis".to_string()))
            .and_then(Value::as_u64)
            .expect("max_total_defer_millis should be a u64");

        assert_eq!(max_total_defer_millis, 50);
        Ok(())
    }

    #[test]
    fn test_overrides_for_dir_missing_file_is_empty() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let overrides = overrides_for_dir(NetworkKind::Mainnet, dir.path())?;
        let Value::Mapping(map) = overrides else {
            panic!("expected overrides mapping for Mainnet");
        };
        assert!(map.is_empty(), "expected empty overrides for Mainnet");
        Ok(())
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

        assert_eq!(
            detect_network_kind_from_ids(&mainnet_value),
            NetworkKind::Mainnet
        );
        assert_eq!(
            detect_network_kind_from_ids(&testnet_value),
            NetworkKind::Testnet
        );
        assert_eq!(
            detect_network_kind_from_ids(&unknown_value),
            NetworkKind::Default
        );
        Ok(())
    }

    fn overrides_for_dir(kind: NetworkKind, overrides_dir: &Path) -> Result<Value> {
        let overrides_yaml = match kind {
            NetworkKind::Mainnet => overrides_dir.join("mainnet.yaml"),
            NetworkKind::Testnet => overrides_dir.join("testnet.yaml"),
            NetworkKind::Tests => overrides_dir.join("tests.yaml"),
            NetworkKind::Simtest => overrides_dir.join("simtest.yaml"),
            NetworkKind::Default => return Ok(Value::Mapping(Mapping::new())),
        };
        let overrides_yaml = if overrides_yaml.exists() {
            std::fs::read_to_string(&overrides_yaml)?
        } else {
            String::new()
        };
        let overrides = parse_overrides_yaml(&overrides_yaml, kind)?;
        Ok(Value::Mapping(overrides))
    }
}

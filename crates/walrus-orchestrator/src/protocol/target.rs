// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    fmt::{Debug, Display},
    num::NonZeroU16,
    path::PathBuf,
    time::Duration,
};

use serde::{Deserialize, Serialize};
use walrus_core::ShardIndex;
use walrus_service::testbed::{
    deploy_walrus_contract,
    even_shards_allocation,
    get_metrics_address,
    node_config_name_prefix,
};
use walrus_sui::utils::SuiNetwork;

use super::{ProtocolCommands, ProtocolMetrics, ProtocolParameters, CARGO_FLAGS, RUST_FLAGS};

#[derive(Clone, Serialize, Deserialize, Debug)]
enum ShardsAllocation {
    /// Evenly distribute the specified number of shards among the nodes.
    Even(NonZeroU16),
    /// Manually specify the shards for each node.
    Manual(Vec<Vec<ShardIndex>>),
}

impl Default for ShardsAllocation {
    fn default() -> Self {
        Self::Even(NonZeroU16::new(10).unwrap())
    }
}

impl ShardsAllocation {
    fn number_of_shards(&self) -> usize {
        match self {
            Self::Even(n) => n.get() as usize,
            Self::Manual(shards) => shards.iter().map(|s| s.len()).sum::<usize>(),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ProtocolNodeParameters {
    #[serde(default = "default_node_parameters::default_sui_network")]
    sui_network: SuiNetwork,
    #[serde(default = "ShardsAllocation::default")]
    shards_allocation: ShardsAllocation,
    #[serde(default = "default_node_parameters::default_contract_path")]
    contract_path: PathBuf,
    #[serde(default = "default_node_parameters::default_event_polling_interval")]
    event_polling_interval: Duration,
}

impl Default for ProtocolNodeParameters {
    fn default() -> Self {
        Self {
            sui_network: default_node_parameters::default_sui_network(),
            shards_allocation: ShardsAllocation::default(),
            contract_path: default_node_parameters::default_contract_path(),
            event_polling_interval: default_node_parameters::default_event_polling_interval(),
        }
    }
}

impl Debug for ProtocolNodeParameters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.shards_allocation.number_of_shards())
    }
}

impl Display for ProtocolNodeParameters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let n_shards = self.shards_allocation.number_of_shards();
        write!(f, "{:?} ({n_shards} shards)", self.sui_network)
    }
}

mod default_node_parameters {
    use std::{path::PathBuf, time::Duration};

    use walrus_service::config;
    use walrus_sui::utils::SuiNetwork;

    pub fn default_sui_network() -> SuiNetwork {
        SuiNetwork::Testnet
    }

    pub fn default_contract_path() -> PathBuf {
        PathBuf::from("./contracts/blob_store")
    }

    pub fn default_event_polling_interval() -> Duration {
        config::defaults::polling_interval()
    }
}

impl ProtocolParameters for ProtocolNodeParameters {}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ProtocolClientParameters {
    gas_budget: u64,
    // Percentage of writes in the workload.
    // Todo: This parameters will be used once we have a load generator (#128)
    load_type: u64,
}

impl Default for ProtocolClientParameters {
    fn default() -> Self {
        Self {
            gas_budget: 500_000_000,
            load_type: 100,
        }
    }
}

impl Display for ProtocolClientParameters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}% Writes", self.load_type)
    }
}

impl ProtocolParameters for ProtocolClientParameters {}

pub struct TargetProtocol;

impl ProtocolCommands for TargetProtocol {
    fn protocol_dependencies(&self) -> Vec<&'static str> {
        // Clang is required to compile rocksdb.
        vec!["sudo apt-get -y install clang"]
    }

    fn db_directories(&self) -> Vec<std::path::PathBuf> {
        // The service binary can delete its own storage directory before booting.
        vec![]
    }

    async fn genesis_command<'a, I>(
        &self,
        instances: I,
        parameters: &crate::benchmark::BenchmarkParameters,
    ) -> String
    where
        I: Iterator<Item = &'a crate::client::Instance>,
    {
        // Create an admin wallet locally to setup the Walrus smart contract.
        let ips = instances.map(|x| x.main_ip).collect::<Vec<_>>();
        let shards = match &parameters.node_parameters.shards_allocation {
            ShardsAllocation::Even(n) => {
                even_shards_allocation(*n, NonZeroU16::new(parameters.nodes as u16).unwrap())
            }
            ShardsAllocation::Manual(shards) => {
                if parameters.nodes
                    != parameters
                        .node_parameters
                        .shards_allocation
                        .number_of_shards()
                {
                    panic!("The number of nodes should match the shard of allocation.")
                }
                shards.clone()
            }
        };

        let sui_config = deploy_walrus_contract(
            parameters.settings.working_dir.as_path(),
            parameters.node_parameters.sui_network,
            parameters.node_parameters.contract_path.clone(),
            parameters.client_parameters.gas_budget,
            shards,
            ips.clone(),
            parameters.node_parameters.event_polling_interval,
        )
        .await
        .expect("Failed to create Walrus contract");

        // Generate a command to print the Sui config to all instances.
        let serialized_sui_config =
            serde_yaml::to_string(&sui_config).expect("Failed to serialize sui configs");
        let sui_config_path = parameters.settings.working_dir.join("sui_config.yaml");
        let upload_sui_config_command = format!(
            "echo -e '{serialized_sui_config}' > {}",
            sui_config_path.display()
        );

        // Generate a command to print all client and storage node configs on all instances.
        let generate_config_command = [
            &format!("{RUST_FLAGS} cargo run {CARGO_FLAGS} --bin walrus-node --"),
            "generate-dry-run-remote-configs",
            &format!(
                "--working-dir {}",
                parameters.settings.working_dir.display()
            ),
            &format!(
                "--sui-network {}",
                parameters.node_parameters.sui_network.r#type()
            ),
            &format!("--sui-config-path {}", sui_config_path.display()),
            &format!(
                "--ips {}",
                ips.iter()
                    .map(|x| x.to_string())
                    .collect::<Vec<_>>()
                    .join(" ")
            ),
        ]
        .join(" ");

        // Output a single command to run on all machines.
        [
            "source $HOME/.cargo/env",
            &upload_sui_config_command,
            &generate_config_command,
        ]
        .join(" && ")
    }

    fn node_command<I>(
        &self,
        instances: I,
        parameters: &crate::benchmark::BenchmarkParameters,
    ) -> Vec<(crate::client::Instance, String)>
    where
        I: IntoIterator<Item = crate::client::Instance>,
    {
        instances
            .into_iter()
            .enumerate()
            .map(|(i, instance)| {
                let working_dir = parameters.settings.working_dir.clone();
                let node_config_name = format!(
                    "{}.yaml",
                    node_config_name_prefix(
                        i as u16,
                        NonZeroU16::new(parameters.nodes as u16).unwrap()
                    )
                );
                let node_config_path = working_dir.join(node_config_name);

                let run_command = [
                    &format!("{RUST_FLAGS} cargo run {CARGO_FLAGS} --bin walrus-node --"),
                    "run",
                    &format!("--config-path {}", node_config_path.display()),
                    "--cleanup-storage",
                ]
                .join(" ");

                let command = [
                    "source $HOME/.cargo/env",
                    "export RUST_LOG=\"client=DEBUG,walrus=INFO\"",
                    &run_command,
                ]
                .join(" && ");
                (instance, command)
            })
            .collect()
    }

    fn client_command<I>(
        &self,
        _instances: I,
        _parameters: &crate::benchmark::BenchmarkParameters,
    ) -> Vec<(crate::client::Instance, String)>
    where
        I: IntoIterator<Item = crate::client::Instance>,
    {
        // Todo: Implement once we have a load generator (#128)
        vec![]
    }
}

impl ProtocolMetrics for TargetProtocol {
    const BENCHMARK_DURATION: &'static str = "benchmark_duration";
    const TOTAL_TRANSACTIONS: &'static str = "total_transactions";
    const LATENCY_BUCKETS: &'static str = "latency_buckets";
    const LATENCY_SUM: &'static str = "latency_sum";
    const LATENCY_SQUARED_SUM: &'static str = "latency_squared_sum";

    fn nodes_metrics_path<I>(&self, instances: I) -> Vec<(crate::client::Instance, String)>
    where
        I: IntoIterator<Item = crate::client::Instance>,
    {
        instances
            .into_iter()
            .enumerate()
            .map(|(i, instance)| {
                let metrics_address = get_metrics_address(instance.main_ip, i as u16);
                let metrics_path = format!("{metrics_address}/metrics",);
                (instance, metrics_path)
            })
            .collect()
    }

    fn clients_metrics_path<I>(&self, _instances: I) -> Vec<(crate::client::Instance, String)>
    where
        I: IntoIterator<Item = crate::client::Instance>,
    {
        // Todo: Implement once we have a load generator (#128)
        vec![]
    }
}

// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    fmt::{Debug, Display},
    num::NonZeroU16,
    ops::Deref,
    path::PathBuf,
    time::Duration,
};

use serde::{Deserialize, Serialize};
use walrus_core::ShardIndex;
use walrus_service::{
    config,
    testbed::{
        deploy_walrus_contract,
        even_shards_allocation,
        format_metrics_address,
        node_config_name_prefix,
        DeployTestbedContractParameters,
    },
};
use walrus_stress::StressParameters;
use walrus_sui::utils::SuiNetwork;

use super::{ProtocolCommands, ProtocolMetrics, ProtocolParameters, BINARY_PATH};
use crate::{benchmark::BenchmarkParameters, client::Instance};

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
    #[serde(default = "config::defaults::rest_api_port")]
    rest_api_port: u16,
    #[serde(default = "config::defaults::metrics_port")]
    metrics_port: u16,
    #[serde(default = "config::defaults::polling_interval")]
    event_polling_interval: Duration,
}

impl Default for ProtocolNodeParameters {
    fn default() -> Self {
        Self {
            sui_network: default_node_parameters::default_sui_network(),
            shards_allocation: ShardsAllocation::default(),
            contract_path: default_node_parameters::default_contract_path(),
            rest_api_port: config::defaults::rest_api_port(),
            metrics_port: config::defaults::metrics_port(),
            event_polling_interval: config::defaults::polling_interval(),
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
    use std::path::PathBuf;

    use walrus_sui::utils::SuiNetwork;

    pub fn default_sui_network() -> SuiNetwork {
        SuiNetwork::Testnet
    }

    pub fn default_contract_path() -> PathBuf {
        PathBuf::from("./contracts/blob_store")
    }
}

impl ProtocolParameters for ProtocolNodeParameters {}

#[derive(Default, Clone, Serialize, Deserialize, Debug)]
#[serde(transparent)]
pub struct ProtocolClientParameters(StressParameters);

impl Deref for ProtocolClientParameters {
    type Target = StressParameters;

    fn deref(&self) -> &Self::Target {
        &self.0
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

    async fn genesis_command<'a, I>(&self, instances: I, parameters: &BenchmarkParameters) -> String
    where
        I: Iterator<Item = &'a Instance>,
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

        let testbed_config = deploy_walrus_contract(DeployTestbedContractParameters {
            working_dir: parameters.settings.working_dir.as_path(),
            sui_network: parameters.node_parameters.sui_network,
            contract_path: parameters.node_parameters.contract_path.clone(),
            gas_budget: parameters.client_parameters.gas_budget,
            shards_information: shards,
            ips: ips.clone(),
            rest_api_port: parameters.node_parameters.rest_api_port,
            storage_capacity: 1_000_000_000_000,
            price_per_unit: 1,
        })
        .await
        .expect("Failed to create Walrus contract");

        // Generate a command to upload benchmark and testbed config to all instances.
        let serialized_testbed_config =
            serde_yaml::to_string(&testbed_config).expect("Failed to serialize sui configs");
        let testbed_config_path = parameters.settings.working_dir.join("testbed_config.yaml");
        let upload_testbed_config_command = format!(
            "echo -e '{serialized_testbed_config}' > {}",
            testbed_config_path.display()
        );

        let serialized_stress_parameters = serde_yaml::to_string(&parameters.client_parameters)
            .expect("Failed to serialize stress parameters");
        let stress_parameters_path = parameters
            .settings
            .working_dir
            .join("stress_parameters.yaml");
        let upload_stress_parameters_command = format!(
            "echo -e '{serialized_stress_parameters}' > {}",
            stress_parameters_path.display()
        );

        // Generate a command to print all client and storage node configs on all instances.
        let generate_config_command = [
            &format!("./{BINARY_PATH}/walrus-node"),
            "generate-dry-run-configs",
            &format!(
                "--working-dir {}",
                parameters.settings.working_dir.display()
            ),
            &format!("--testbed-config-path {}", testbed_config_path.display()),
            &format!("--metrics-port {}", parameters.node_parameters.metrics_port),
        ]
        .join(" ");

        // Output a single command to run on all machines.
        [
            "source $HOME/.cargo/env",
            &upload_testbed_config_command,
            &upload_stress_parameters_command,
            &generate_config_command,
        ]
        .join(" && ")
    }

    fn node_command<I>(
        &self,
        instances: I,
        parameters: &BenchmarkParameters,
    ) -> Vec<(Instance, String)>
    where
        I: IntoIterator<Item = Instance>,
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
                    &format!("./{BINARY_PATH}/walrus-node"),
                    "run",
                    &format!("--config-path {}", node_config_path.display()),
                    "--cleanup-storage",
                ]
                .join(" ");

                let command = [
                    "source $HOME/.cargo/env",
                    "export RUST_LOG=\"client=DEBUG,node=INFO\"",
                    &run_command,
                ]
                .join(" && ");
                (instance, command)
            })
            .collect()
    }

    fn client_command<I>(
        &self,
        instances: I,
        parameters: &BenchmarkParameters,
    ) -> Vec<(Instance, String)>
    where
        I: IntoIterator<Item = Instance>,
    {
        let clients: Vec<_> = instances.into_iter().collect();
        let load_per_client = (parameters.load / clients.len()).max(1);
        clients
            .into_iter()
            .map(|instance| {
                let working_dir = &parameters.settings.working_dir;
                let client_config_path = working_dir.clone().join("client_config.yaml");
                let stress_parameters_path = working_dir.clone().join("stress_parameters.yaml");
                let skip_pre_generation = if parameters.client_parameters.skip_pre_generation {
                    "--skip-pre-generation"
                } else {
                    ""
                };

                let run_command = [
                    &format!("./{BINARY_PATH}/walrus-stress"),
                    "-vv",
                    &format!("--load {}", load_per_client),
                    &format!("--config-path {}", client_config_path.display()),
                    &format!(
                        "--stress-parameters-path {}",
                        stress_parameters_path.display()
                    ),
                    &format!(
                        "--duration {}",
                        parameters.settings.benchmark_duration.as_secs()
                    ),
                    &format!(
                        "--sui-network {}",
                        parameters.node_parameters.sui_network.r#type()
                    ),
                    skip_pre_generation,
                ]
                .join(" ");

                let command = ["source $HOME/.cargo/env", &run_command].join(" && ");
                (instance, command)
            })
            .collect()
    }
}

impl ProtocolMetrics for TargetProtocol {
    const BENCHMARK_DURATION: &'static str = "benchmark_duration";
    const TOTAL_TRANSACTIONS: &'static str = "total_transactions";
    const LATENCY_BUCKETS: &'static str = "latency_buckets";
    const LATENCY_SUM: &'static str = "latency_sum";
    const LATENCY_SQUARED_SUM: &'static str = "latency_squared_sum";

    fn nodes_metrics_path<I>(
        &self,
        instances: I,
        parameters: &BenchmarkParameters,
    ) -> Vec<(Instance, String)>
    where
        I: IntoIterator<Item = Instance>,
    {
        instances
            .into_iter()
            .map(|instance| {
                let metrics_port = parameters.node_parameters.metrics_port;
                let metrics_address = format_metrics_address(instance.main_ip, metrics_port, None);
                let metrics_path = format!("{metrics_address}/metrics",);
                (instance, metrics_path)
            })
            .collect()
    }

    fn clients_metrics_path<I>(
        &self,
        instances: I,
        parameters: &BenchmarkParameters,
    ) -> Vec<(Instance, String)>
    where
        I: IntoIterator<Item = Instance>,
    {
        instances
            .into_iter()
            .map(|instance| {
                let instance_ip = instance.main_ip;
                let metrics_port = parameters.client_parameters.metrics_port;
                let metrics_path = format!("{instance_ip}:{metrics_port}/metrics");
                (instance, metrics_path)
            })
            .collect()
    }
}

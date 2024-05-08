// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{fmt::Display, num::NonZeroU16};

use serde::{Deserialize, Serialize};
use walrus_sui::utils::SuiNetwork;

use super::{ProtocolCommands, ProtocolMetrics, ProtocolParameters};

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ProtocolNodeParameters {
    committee_size: NonZeroU16,
    n_shards: NonZeroU16,
    sui_network: SuiNetwork,
}

impl Default for ProtocolNodeParameters {
    fn default() -> Self {
        Self {
            committee_size: NonZeroU16::new(10).unwrap(),
            n_shards: NonZeroU16::new(1000).unwrap(),
            sui_network: SuiNetwork::Devnet,
        }
    }
}

impl Display for ProtocolNodeParameters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:?}: {} Nodes, {} Shards",
            self.sui_network, self.committee_size, self.n_shards
        )
    }
}

impl ProtocolParameters for ProtocolNodeParameters {}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ProtocolClientParameters {
    gas_budget: u64,
    // Percentage of writes in the workload.
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
        vec![]
    }

    fn db_directories(&self) -> Vec<std::path::PathBuf> {
        // The service binary can delete its own storage directory before booting.
        vec![]
    }

    fn genesis_command<'a, I>(
        &self,
        _instances: I,
        _parameters: &crate::benchmark::BenchmarkParameters,
    ) -> String
    where
        I: Iterator<Item = &'a crate::client::Instance>,
    {
        todo!("Alberto: Implement once Walrus parameters are stable (#234)")

        // 1. Run the admin locally to setup the smart contract and derive all system information
        // 2. Print to file all the system information
        // 3. Upload the system information to all instance and generate the genesis on each instance
    }

    fn node_command<I>(
        &self,
        _instances: I,
        _parameters: &crate::benchmark::BenchmarkParameters,
    ) -> Vec<(crate::client::Instance, String)>
    where
        I: IntoIterator<Item = crate::client::Instance>,
    {
        todo!("Alberto: Implement once Walrus parameters are stable (#234)")
    }

    fn monitor_command<I>(&self, _instances: I) -> Vec<(crate::client::Instance, String)>
    where
        I: IntoIterator<Item = crate::client::Instance>,
    {
        todo!("Alberto: Implement once Walrus parameters are stable (#234)")
    }

    fn client_command<I>(
        &self,
        _instances: I,
        _parameters: &crate::benchmark::BenchmarkParameters,
    ) -> Vec<(crate::client::Instance, String)>
    where
        I: IntoIterator<Item = crate::client::Instance>,
    {
        todo!("Alberto: Implement once Walrus parameters are stable (#234)")
    }
}

impl ProtocolMetrics for TargetProtocol {
    const BENCHMARK_DURATION: &'static str = "benchmark_duration";
    const TOTAL_TRANSACTIONS: &'static str = "total_transactions";
    const LATENCY_BUCKETS: &'static str = "latency_buckets";
    const LATENCY_SUM: &'static str = "latency_sum";
    const LATENCY_SQUARED_SUM: &'static str = "latency_squared_sum";

    fn nodes_metrics_path<I>(&self, _instances: I) -> Vec<(crate::client::Instance, String)>
    where
        I: IntoIterator<Item = crate::client::Instance>,
    {
        todo!("Alberto: Implement once Walrus parameters are stable (#234)")
    }

    fn clients_metrics_path<I>(&self, _instances: I) -> Vec<(crate::client::Instance, String)>
    where
        I: IntoIterator<Item = crate::client::Instance>,
    {
        todo!("Alberto: Implement once Walrus parameters are stable (#234)")
    }
}

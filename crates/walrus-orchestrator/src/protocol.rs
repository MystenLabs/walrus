// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{
    fmt::{Debug, Display},
    path::{Path, PathBuf},
};

use eyre::Context;
use serde::{de::DeserializeOwned, Serialize};

use crate::{benchmark::BenchmarkParameters, client::Instance};

pub mod target;

pub const BINARY_PATH: &str = "target/release";

pub trait ProtocolParameters:
    Default + Clone + Serialize + DeserializeOwned + Debug + Display
{
    /// Load the configuration from a YAML file located at the provided path.
    fn load<P: AsRef<Path>>(path: P) -> Result<Self, eyre::Error> {
        let path = path.as_ref();
        let error_message = format!("Unable to load config from {}", path.display());
        let reader = std::fs::File::open(path).wrap_err(error_message)?;
        Ok(serde_yaml::from_reader(reader)?)
    }
}

/// The minimum interface that the protocol should implement to allow benchmarks from
/// the orchestrator.
pub trait ProtocolCommands {
    /// The list of dependencies to install (e.g., through apt-get).
    fn protocol_dependencies(&self) -> Vec<&'static str>;

    /// The directories of all databases (that should be erased before each run).
    fn db_directories(&self) -> Vec<PathBuf>;

    /// The command to generate the genesis and all configuration files. This command
    /// is run on each remote machine.
    async fn genesis_command<'a, I>(
        &self,
        instances: I,
        parameters: &BenchmarkParameters,
    ) -> String
    where
        I: Iterator<Item = &'a Instance>;

    /// The command to run a client. The function returns a vector of commands along with the
    /// associated instance on which to run the command.
    fn client_command<I>(
        &self,
        instances: I,
        parameters: &BenchmarkParameters,
    ) -> Vec<(Instance, String)>
    where
        I: IntoIterator<Item = Instance>;
}

/// The names of the minimum metrics exposed by the protocol that are required to
/// compute performance.
pub trait ProtocolMetrics {
    /// The name of the metric reporting the total duration of the benchmark (in seconds).
    const BENCHMARK_DURATION: &'static str;
    /// The name of the metric reporting the total number of finalized transactions.
    const TOTAL_TRANSACTIONS: &'static str;
    /// The name of the metric reporting the latency buckets.
    const LATENCY_BUCKETS: &'static str;
    /// The name of the metric reporting the sum of the end-to-end latency of all finalized
    /// transactions.
    const LATENCY_SUM: &'static str;
    /// The name of the metric reporting the square of the sum of the end-to-end latency of all
    /// finalized transactions.
    const LATENCY_SQUARED_SUM: &'static str;

    /// The network path where the clients expose prometheus metrics.
    fn clients_metrics_path<I>(
        &self,
        instances: I,
        parameters: &BenchmarkParameters,
    ) -> Vec<(Instance, String)>
    where
        I: IntoIterator<Item = Instance>;

    /// The command to retrieve the metrics from the clients.
    fn clients_metrics_command<I>(
        &self,
        instances: I,
        parameters: &BenchmarkParameters,
    ) -> Vec<(Instance, String)>
    where
        I: IntoIterator<Item = Instance>,
    {
        self.clients_metrics_path(instances, parameters)
            .into_iter()
            .map(|(instance, path)| (instance, format!("curl {path}")))
            .collect()
    }
}

#[cfg(test)]
pub mod test_protocol_metrics {
    use super::ProtocolMetrics;
    use crate::{benchmark::BenchmarkParameters, client::Instance};

    pub struct TestProtocolMetrics;

    impl ProtocolMetrics for TestProtocolMetrics {
        const BENCHMARK_DURATION: &'static str = "benchmark_duration";
        const TOTAL_TRANSACTIONS: &'static str = "latency_s_count";
        const LATENCY_BUCKETS: &'static str = "latency_s";
        const LATENCY_SUM: &'static str = "latency_s_sum";
        const LATENCY_SQUARED_SUM: &'static str = "latency_squared_s";

        fn clients_metrics_path<I>(
            &self,
            instances: I,
            _parameters: &BenchmarkParameters,
        ) -> Vec<(Instance, String)>
        where
            I: IntoIterator<Item = Instance>,
        {
            instances
                .into_iter()
                .enumerate()
                .map(|(i, instance)| (instance, format!("localhost:{}/metrics", 9000 + i as u16)))
                .collect()
        }
    }
}

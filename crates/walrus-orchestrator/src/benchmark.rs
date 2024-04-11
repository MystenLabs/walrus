// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    fmt::{Debug, Display},
    time::Duration,
};

use serde::{Deserialize, Serialize};

use crate::{faults::FaultsType, protocol::ProtocolParameters, ClientParameters, NodeParameters};

/// Shortcut avoiding to use the generic version of the benchmark parameters.
pub type BenchmarkParameters = BenchmarkParametersGeneric<NodeParameters, ClientParameters>;

/// The benchmark parameters for a run.
#[derive(Serialize, Deserialize, Clone)]
pub struct BenchmarkParametersGeneric<N, C> {
    /// The node's configuration parameters.
    pub node_parameters: N,
    /// The client's configuration parameters.
    pub client_parameters: C,
    /// The committee size.
    pub nodes: usize,
    /// The number of (crash-)faults.
    pub faults: FaultsType,
    /// The total load (tx/s) to submit to the system.
    pub load: usize,
    /// The duration of the benchmark.
    pub duration: Duration,
}

impl<N: Debug, C> Debug for BenchmarkParametersGeneric<N, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:?}-{:?}-{}-{}",
            self.node_parameters, self.faults, self.nodes, self.load
        )
    }
}

impl<N, C> Display for BenchmarkParametersGeneric<N, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} nodes ({}) - {} tx/s",
            self.nodes, self.faults, self.load
        )
    }
}

impl<N: ProtocolParameters, C: ProtocolParameters> BenchmarkParametersGeneric<N, C> {
    /// Make a new benchmark parameters.
    #[allow(dead_code)] // TODO(Alberto): Will be used to deploy nodes (#222")
    pub fn new_from_loads(
        node_parameters: N,
        client_parameters: C,
        nodes: usize,
        faults: FaultsType,
        loads: Vec<usize>,
        duration: Duration,
    ) -> Vec<Self> {
        loads
            .into_iter()
            .map(|load| Self {
                node_parameters: node_parameters.clone(),
                client_parameters: client_parameters.clone(),
                nodes,
                faults: faults.clone(),
                load,
                duration,
            })
            .collect()
    }

    #[cfg(test)]
    #[allow(dead_code)] // TODO(Alberto): Will be used to deploy nodes (#222")
    pub fn new_for_tests() -> Self {
        Self {
            node_parameters: N::default(),
            client_parameters: C::default(),
            nodes: 4,
            faults: FaultsType::default(),
            load: 500,
            duration: Duration::from_secs(60),
        }
    }
}

#[cfg(test)]
pub mod test {
    use std::{fmt::Display, str::FromStr};

    use serde::{Deserialize, Serialize};

    use super::ProtocolParameters;

    /// Mock benchmark type for unit tests.
    #[derive(
        Serialize, Deserialize, Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash, Default,
    )]
    pub struct TestNodeConfig;

    impl Display for TestNodeConfig {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "TestNodeConfig")
        }
    }

    impl FromStr for TestNodeConfig {
        type Err = ();

        fn from_str(_s: &str) -> Result<Self, Self::Err> {
            Ok(Self {})
        }
    }

    impl ProtocolParameters for TestNodeConfig {}
}

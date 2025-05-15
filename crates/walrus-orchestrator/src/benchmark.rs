// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::fmt::{Debug, Display};

use serde::{Deserialize, Serialize};

use crate::{ClientParameters, protocol::ProtocolParameters, settings::Settings};

/// Shortcut avoiding to use the generic version of the benchmark parameters.
pub type BenchmarkParameters = BenchmarkParametersGeneric<ClientParameters>;

/// The benchmark parameters for a run. These parameters are stored along with the performance data
/// and should be used to reproduce the results.
#[derive(Serialize, Deserialize, Clone)]
pub struct BenchmarkParametersGeneric<C> {
    /// The testbed settings.
    pub settings: Settings,
    /// The client's configuration parameters.
    pub client_parameters: C,
    /// The number of stress clients.
    pub clients: usize,
}

impl<C> Debug for BenchmarkParametersGeneric<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.clients)
    }
}

impl<C> Display for BenchmarkParametersGeneric<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} nodes", self.clients)
    }
}

impl<C: ProtocolParameters> BenchmarkParametersGeneric<C> {
    pub fn new(settings: Settings, client_parameters: C, clients: usize) -> Self {
        Self {
            settings,
            client_parameters,
            clients,
        }
    }

    #[cfg(test)]
    pub fn new_for_tests() -> Self {
        Self {
            settings: Settings::new_for_test(),
            client_parameters: C::default(),
            clients: 4,
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

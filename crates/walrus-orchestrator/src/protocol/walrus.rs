// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::fmt::Display;

use serde::{Deserialize, Serialize};

use super::{ProtocolCommands, ProtocolParameters};

#[derive(Default, Clone, Serialize, Deserialize, Debug)]
pub struct WalrusNodeParameters;

impl Display for WalrusNodeParameters {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!("Alberto: Implement once Walrus parameters are stable (#234)")
    }
}

impl ProtocolParameters for WalrusNodeParameters {}

#[derive(Default, Clone, Serialize, Deserialize, Debug)]
pub struct WalrusClientParameters;

impl Display for WalrusClientParameters {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!("Alberto: Implement once Walrus parameters are stable (#234)")
    }
}

impl ProtocolParameters for WalrusClientParameters {}

pub struct WalrusProtocol;

impl ProtocolCommands<WalrusNodeParameters, WalrusClientParameters> for WalrusProtocol {
    fn protocol_dependencies(&self) -> Vec<&'static str> {
        todo!("Alberto: Implement once Walrus parameters are stable (#234)")
    }

    fn db_directories(&self) -> Vec<std::path::PathBuf> {
        todo!("Alberto: Implement once Walrus parameters are stable (#234)")
    }

    fn cleanup_commands(&self) -> Vec<String> {
        todo!("Alberto: Implement once Walrus parameters are stable (#234)")
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

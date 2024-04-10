// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::fmt::Display;

use serde::{Deserialize, Serialize};

use super::{ProtocolCommands, ProtocolParameters};

#[derive(Default, Clone, Serialize, Deserialize, Debug)]
pub struct ProtocolNodeParameters;

impl Display for ProtocolNodeParameters {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!("Alberto: Implement once Walrus parameters are stable (#234)")
    }
}

impl ProtocolParameters for ProtocolNodeParameters {}

#[derive(Default, Clone, Serialize, Deserialize, Debug)]
pub struct ProtocolClientParameters;

impl Display for ProtocolClientParameters {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!("Alberto: Implement once Walrus parameters are stable (#234)")
    }
}

impl ProtocolParameters for ProtocolClientParameters {}

pub struct TargetProtocol;

impl ProtocolCommands<ProtocolNodeParameters, ProtocolClientParameters> for TargetProtocol {
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

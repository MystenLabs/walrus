// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Client for the Walrus service.

pub mod cli;
pub mod responses;

pub(crate) mod config;
pub use walrus_sdk::{
    config::{ClientCommunicationConfig, ClientConfig, default_configuration_paths},
    utils::string_prefix,
};

mod daemon;
pub use daemon::{ClientDaemon, PublisherQuery, WalrusWriteClient, auth::Claim};

mod refill;
pub use refill::{RefillHandles, Refiller};
mod multiplexer;

// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Client for the Walrus service.

pub mod cli;
pub mod responses;

pub(crate) mod config;
pub use walrus_sdk::{
    config::{default_configuration_paths, ClientCommunicationConfig, ClientConfig},
    utils::string_prefix,
};

mod daemon;
pub use daemon::{auth::Claim, ClientDaemon, PublisherQuery, WalrusWriteClient};

mod refill;
pub use refill::{RefillHandles, Refiller};
mod multiplexer;

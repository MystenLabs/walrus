// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Client for the Walrus service.

pub use walrus_sdk::{
    config::{ClientCommunicationConfig, ClientConfig, default_configuration_paths},
    utils::string_prefix,
};

pub mod cli;
pub mod responses;

pub(crate) mod config;

mod daemon;
pub use daemon::{ClientDaemon, PublisherQuery, WalrusWriteClient, auth::Claim};

mod multiplexer;

mod refill;
pub use refill::{RefillHandles, Refiller};

mod utils;

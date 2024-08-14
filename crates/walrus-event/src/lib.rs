// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use sui_types::base_types::ObjectID;

mod blob_writer;
mod checkpoint_processor;
mod event_blob;

/// Configuration for event processing.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EventConfig {
    /// The REST URL of the fullnode.
    pub rest_url: String,
    /// The URL of the checkpoint client to connect to.
    pub path: PathBuf,
    /// The path to the Sui genesis file.
    pub sui_genesis_path: PathBuf,
    /// ObjectID of the Walrus system package.
    pub system_pkg_id: ObjectID,
}

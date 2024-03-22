// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::HashMap,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Context;
use fastcrypto::traits::KeyPair as FastKeyPair;
use serde::{Deserialize, Serialize};
use walrus_core::{Epoch, KeyPair, PublicKey, ShardIndex};

/// Configuration of a Walrus storage node.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StorageNodeConfig {
    /// Directory in which to persist the database
    pub storage_path: PathBuf,
    /// Key pair used in Walrus protocol messages
    // TODO(jsmith): Handle use with files, as well proper key formatting, and remove default (#147)
    // TODO(jsmith): Add a CLI endpoint that can be used to generate a new private key file (#148)
    #[serde(default = "defaults::protocol_key_pair")]
    pub protocol_key_pair: Arc<KeyPair>,
    /// Socket address on which to the Prometheus server should export its metrics.
    #[serde(default = "defaults::metrics_address")]
    pub metrics_address: SocketAddr,
    /// Socket address on which the REST API listens.
    #[serde(default = "defaults::rest_api_address")]
    pub rest_api_address: SocketAddr,
}

impl StorageNodeConfig {
    /// Load the configuration from a YAML file located at the provided path.
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self, anyhow::Error> {
        let path = path.as_ref();
        tracing::trace!("Reading config from {}", path.display());

        let reader = std::fs::File::open(path)
            .with_context(|| format!("Unable to load config from {}", path.display()))?;

        Ok(serde_yaml::from_reader(reader)?)
    }
}

mod defaults {
    use std::net::Ipv4Addr;

    use super::*;

    const METRICS_PORT: u16 = 9184;
    const REST_API_PORT: u16 = 9185;

    pub fn metrics_address() -> SocketAddr {
        (Ipv4Addr::UNSPECIFIED, METRICS_PORT).into()
    }

    pub fn rest_api_address() -> SocketAddr {
        (Ipv4Addr::UNSPECIFIED, REST_API_PORT).into()
    }

    pub fn protocol_key_pair() -> Arc<KeyPair> {
        Arc::new(KeyPair::generate(&mut rand::thread_rng()))
    }
}

/// Represents a storage node identifier.
#[derive(Debug, Serialize, Deserialize)]
pub struct StorageNodeIdentifier {
    /// The public key of the storage node.
    pub public_key: PublicKey,
    /// The shards that the storage node is responsible for.
    pub shards: Vec<ShardIndex>,
}

/// Represents a committee of storage nodes, that is, the Walrus committee.
#[derive(Debug, Serialize, Deserialize)]
pub struct Committee {
    /// The members of the committee.
    pub members: Vec<StorageNodeIdentifier>,
    /// The epoch of the committee.
    pub epoch: Epoch,
}

impl Committee {
    /// Returns the total number of shards in the system.
    pub fn number_of_shards(&self) -> usize {
        self.members.iter().map(|node| node.shards.len()).sum()
    }

    /// Returns the number of shards required to reach a quorum (2f+1).
    pub fn quorum_threshold(&self) -> usize {
        self.number_of_shards() * 2 / 3 + 1
    }

    /// Returns the number of shards required to reach validity (f+1).
    pub fn validity_threshold(&self) -> usize {
        (self.number_of_shards() + 2) / 3
    }
}

/// Represents the global public parameters of the system.
pub struct GlobalPublicParameters {
    /// The network addresses of the shards.
    pub network_addresses: HashMap<ShardIndex, SocketAddr>,
    /// The metrics addresses of the shards.
    pub metrics_addresses: HashMap<ShardIndex, SocketAddr>,
}

/// Represents the private parameters of a shard.
pub struct ShardPrivateParameters {
    /// The storage directory of the shard.
    pub storage_directory: PathBuf,
}

/// Represents the private parameters of a storage node.
pub struct StorageNodePrivateParameters {
    /// The keypair of the storage node.
    pub keypair: KeyPair,
    /// The network address of the shard.
    pub network_address: SocketAddr,
    /// The metrics address of the shard.
    pub metrics_address: SocketAddr,
    /// The private parameters shards that the storage node is responsible for.
    pub shards: HashMap<ShardIndex, ShardPrivateParameters>,
}

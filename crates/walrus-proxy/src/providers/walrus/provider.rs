// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::HashMap,
    hash::{DefaultHasher, Hash, Hasher},
    path::PathBuf,
    sync::{Arc, RwLock},
    time::Duration,
};

use fastcrypto::traits::EncodeDecodeBase64;
use once_cell::sync::Lazy;
use prometheus::{CounterVec, HistogramOpts, HistogramVec, Opts};

use super::query::{get_walrus_nodes, NodeInfo};
use crate::{register_metric, Allower, NetworkPublicKey};

/// JSON_RPC_STATE is a counter vector to track number of successful/failed RPC calls.
///
/// # Panics
///
/// This will panic if the `CounterVec` cannot be registered to the metrics registry.
/// This usually indicates a programming error or conflicting metric name.
static JSON_RPC_STATE: Lazy<CounterVec> = Lazy::new(|| {
    register_metric!(CounterVec::new(
        Opts::new(
            "json_rpc_state",
            "Number of successful/failed requests made.",
        ),
        &["rpc_method", "status"]
    )
    .expect("Failed to create and register json_rpc_state CounterVec"))
});
/// JSON_RPC_DURATION is a histogram vector to record latencies of RPC calls.
///
/// # Panics
///
/// This will panic if the `HistogramVec` cannot be registered to the metrics registry.
/// This usually indicates a programming error or conflicting metric name.
static JSON_RPC_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    register_metric!(HistogramVec::new(
        HistogramOpts::new(
            "json_rpc_duration_seconds",
            "The json-rpc latencies in seconds.",
        )
        .buckets(vec![
            0.0008, 0.0016, 0.0032, 0.0064, 0.0128, 0.0256, 0.0512, 0.1024, 0.2048, 0.4096, 0.8192,
            1.0, 1.25, 1.5, 1.75, 2.0, 4.0, 8.0
        ]),
        &["rpc_method"]
    )
    .expect("Failed to create and register json_rpc_duration_seconds HistogramVec"))
});

/// AllowedPeers is a mapping of public key to AllowedPeer data
pub type AllowedPeers = Arc<RwLock<HashMap<u64, NodeInfo>>>;

/// WalrusNodeProvider queries the sui blockchain and keeps a record of known
/// nodes. Middleware and handlers use this node info to determine if we should
/// speak to this client
#[derive(Debug, Clone)]
pub struct WalrusNodeProvider {
    nodes: AllowedPeers,
    rpc_url: String,
    rpc_poll_interval: Duration,
    system_object_id: String,
    staking_object_id: String,
    /// path of the allow list file
    allowlist_path: Option<PathBuf>,
}

impl Allower<NetworkPublicKey> for WalrusNodeProvider {
    /// Checks if the given network public key is in the list of allowed Walrus nodes.
    ///
    /// # Panics
    ///
    /// This function will panic if acquiring a read lock on the node list fails.
    /// This could occur due to poisoned `RwLock`, which indicates a previous panic
    /// while the lock was held.
    fn allowed(&self, key: &NetworkPublicKey) -> bool {
        self.nodes
            .read()
            .expect("Failed to acquire read lock on known Walrus nodes")
            .contains_key(&stdlib_hash(key.as_ref()))
    }
}

impl WalrusNodeProvider {
    /// Create a new WalrusNodeProvider that will poll for nodes in the committee.
    pub fn new(
        rpc_url: &str,
        rpc_poll_interval: &Duration,
        system_object_id: &str,
        staking_object_id: &str,
        allowlist_path: Option<PathBuf>,
    ) -> Self {
        Self {
            nodes: Arc::new(RwLock::new(HashMap::new())),
            rpc_url: rpc_url.to_string(),
            rpc_poll_interval: rpc_poll_interval.to_owned(),
            system_object_id: system_object_id.to_string(),
            staking_object_id: staking_object_id.to_string(),
            allowlist_path,
        }
    }
    /// Poll the peer list at regular intervals to refresh the internal cache.
    pub fn poll_peer_list(&self) {
        tracing::info!("Started polling for peers using rpc: {}", self.rpc_url);

        let rpc_poll_interval = self.rpc_poll_interval;
        let cloned_self = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(rpc_poll_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                interval.tick().await;
                let timer =
                    walrus_utils::with_label!(JSON_RPC_DURATION, "update_peer_count").start_timer();
                cloned_self.update_walrus_nodes().await;
                timer.observe_duration();
            }
        });
    }
    /// Update the Walrus node list used by the provider.
    ///
    /// # Panics
    ///
    /// Panics if acquiring a write lock on the node map fails. This indicates a poisoned lock,
    /// which may result from a previous panic in a thread.
    async fn update_walrus_nodes(&self) {
        let committee = match get_walrus_nodes(
            &self.rpc_url,
            &self.system_object_id,
            &self.staking_object_id,
            &self.allowlist_path,
        )
        .await
        {
            Ok(node_infos) => {
                walrus_utils::with_label!(JSON_RPC_STATE, "update_peer_count", "success").inc();
                node_infos
            }
            Err(e) => {
                tracing::error!("unable to perform committee update; {e}");
                walrus_utils::with_label!(JSON_RPC_STATE, "update_peer_count", "failed").inc();
                return;
            }
        };

        for NodeInfo {
            name,
            network_address,
            network_public_key,
        } in &committee
        {
            tracing::info!(
                "loaded node:[{name}] network_address: [{network_address}] \
                network_public_key: [{network_public_key}]"
            );
        }
        if committee.is_empty() {
            tracing::error!("walrus committee is empty? refusing to attempt to update cache");
            return;
        }
        let mut nodes: HashMap<u64, NodeInfo> = HashMap::new();
        committee.into_iter().for_each(|node_info| {
            let pub_key = node_info.network_public_key.clone();
            let encoded_pub_key = pub_key.encode_base64();
            let cache_key = stdlib_hash(encoded_pub_key.clone().as_bytes());
            tracing::debug!("add {} {}", encoded_pub_key, cache_key);
            if nodes.insert(cache_key, node_info.clone()).is_some() {
                tracing::info!(
                    "node {} already in cache. Overwriting with new node info {:}",
                    encoded_pub_key,
                    node_info,
                );
            }
        });
        let mut allow = self.nodes.write().expect("Failed to acquire write lock on nodes map");
        allow.clear();
        allow.extend(nodes);
        tracing::info!(
            "{} walrus nodes managed to make it on the allow list",
            allow.len()
        );
    }
    /// Get the peer info for a given public key if it exists.
    ///
    /// # Panics
    ///
    /// Panics if acquiring a read lock on the node map fails. This indicates a poisoned lock.
    pub fn get(&self, key: &NetworkPublicKey) -> Option<NodeInfo> {
        let encoded_pub_key = key.encode_base64();
        let cache_key = stdlib_hash(encoded_pub_key.clone().as_bytes());
        tracing::debug!("look for {} {}", &encoded_pub_key, &cache_key);
        if let Some(v) = self
            .nodes
            .read()
            .expect("Failed to acquire read lock on nodes map")
            .get(&cache_key)
        {
            return Some(v.to_owned());
        }
        tracing::debug!("not found {} {}", &encoded_pub_key, &cache_key);
        None
    }
}

/// Use the stdlib hash to make stable, fixed length keys for our
/// node cache.
fn stdlib_hash(t: &[u8]) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

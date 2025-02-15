// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::HashMap,
    hash::{DefaultHasher, Hash, Hasher},
    sync::{Arc, RwLock},
    time::Duration,
};

use fastcrypto::{
    secp256r1::Secp256r1PublicKey,
    traits::{EncodeDecodeBase64, ToFromBytes},
};
use once_cell::sync::Lazy;
use prometheus::{CounterVec, HistogramOpts, HistogramVec, Opts};
use tracing::{debug, error, info};

use super::query::{get_walrus_nodes, NodeInfo};
use crate::{register_metric, Allower};

static JSON_RPC_STATE: Lazy<CounterVec> = Lazy::new(|| {
    register_metric!(CounterVec::new(
        Opts::new(
            "json_rpc_state",
            "Number of successful/failed requests made.",
        ),
        &["rpc_method", "status"]
    )
    .unwrap())
});
static JSON_RPC_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    register_metric!(HistogramVec::new(
        HistogramOpts::new(
            "json_rpc_duration_seconds",
            "The json-rpc latencies in seconds.",
        )
        .buckets(vec![
            0.0008, 0.0016, 0.0032, 0.0064, 0.0128, 0.0256, 0.0512, 0.1024, 0.2048, 0.4096, 0.8192,
            1.0, 1.25, 1.5, 1.75, 2.0, 4.0, 8.0
        ],),
        &["rpc_method"]
    )
    .unwrap())
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
}

impl Allower<Secp256r1PublicKey> for WalrusNodeProvider {
    fn allowed(&self, key: &Secp256r1PublicKey) -> bool {
        self.nodes
            .read()
            .unwrap()
            .contains_key(&stdlib_hash(key.as_ref()))
    }
}

impl WalrusNodeProvider {
    /// create a new walrus provider that will poll for nodes in committee
    pub fn new(
        rpc_url: &str,
        rpc_poll_interval: &Duration,
        system_object_id: &str,
        staking_object_id: &str,
    ) -> Self {
        Self {
            nodes: Arc::new(RwLock::new(HashMap::new())),
            rpc_url: rpc_url.to_string(),
            rpc_poll_interval: rpc_poll_interval.to_owned(),
            system_object_id: system_object_id.to_string(),
            staking_object_id: staking_object_id.to_string(),
        }
    }
    /// poll_peer_list will act as a refresh interval for our cache
    pub fn poll_peer_list(&self) {
        info!("Started polling for peers using rpc: {}", self.rpc_url);

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
    /// update the walrus node list that we will speak with
    async fn update_walrus_nodes(&self) {
        let committee = match get_walrus_nodes(
            &self.rpc_url,
            &self.system_object_id,
            &self.staking_object_id,
        )
        .await
        {
            Ok(v) => {
                walrus_utils::with_label!(JSON_RPC_STATE, "update_peer_count", "success").inc();
                v
            }
            Err(e) => {
                error!("unable to perform committee update; {e}");
                walrus_utils::with_label!(JSON_RPC_STATE, "update_peer_count", "failed").inc();
                return;
            }
        };

        for NodeInfo {
            name,
            network_address,
            ..
        } in &committee
        {
            info!("loaded node:[{name}] network_address: [{network_address}]");
        }
        if committee.is_empty() {
            error!("walrus committee is empty? refusing to attempt to update cache");
            return;
        }
        let nodes: HashMap<u64, NodeInfo> = committee
            .into_iter()
            .filter_map(|v| {
                let Ok(pub_key) = Secp256r1PublicKey::from_bytes(&v.network_public_key) else {
                    return None;
                };
                let encoded_pub_key = pub_key.encode_base64();
                let cache_key = stdlib_hash(encoded_pub_key.clone().as_bytes());
                debug!("add {} {}", encoded_pub_key, cache_key);
                Some((cache_key, v))
            })
            .collect();
        let mut allow = self.nodes.write().unwrap();
        allow.clear();
        allow.extend(nodes);
        info!(
            "{} walrus nodes managed to make it on the allow list",
            allow.len()
        );
    }
    /// get is used to retrieve peer info in our handlers
    pub fn get(&self, key: &Secp256r1PublicKey) -> Option<NodeInfo> {
        let encoded_pub_key = key.encode_base64();
        let cache_key = stdlib_hash(encoded_pub_key.clone().as_bytes());
        debug!("look for {} {}", &encoded_pub_key, &cache_key);
        if let Some(v) = self.nodes.read().unwrap().get(&cache_key) {
            return Some(v.to_owned());
        }
        debug!("not found {} {}", &encoded_pub_key, &cache_key);
        None
    }
}

/// use the stdlib hash to make stable, fixed length keys for our
/// node cache.
fn stdlib_hash(t: &[u8]) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

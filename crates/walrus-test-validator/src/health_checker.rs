// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Result;
use chrono::Utc;
use sui_types::base_types::ObjectID;
use tokio::{fs, io::AsyncWriteExt, sync::Mutex, task::JoinHandle, time};
use walrus_sdk::client::Client;
use walrus_sui::types::StorageNode;

pub struct HealthChecker {
    log_dir: PathBuf,
    node_probers: Arc<Mutex<HashMap<ObjectID, Arc<Mutex<NodeProber>>>>>,
    interval_secs: u64,
    health_check_handle: Option<JoinHandle<Result<()>>>,
}

impl HealthChecker {
    pub fn new(interval_secs: u64, log_dir: PathBuf) -> Self {
        Self {
            interval_secs,
            log_dir,
            node_probers: Arc::new(Mutex::new(
                HashMap::<ObjectID, Arc<Mutex<NodeProber>>>::new(),
            )),
            health_check_handle: None,
        }
    }

    pub async fn add_node(&mut self, node: StorageNode) -> Result<()> {
        let mut node_probers = self.node_probers.lock().await;
        let prober = NodeProber::new(node, &self.log_dir).await?;
        node_probers.insert(prober.node_id(), Arc::new(Mutex::new(prober)));
        Ok(())
    }

    #[allow(dead_code)]
    pub async fn remove_node(&mut self, node_id: &ObjectID) -> Result<()> {
        let mut node_probers = self.node_probers.lock().await;
        node_probers.remove(node_id);
        Ok(())
    }

    pub async fn start_health_check(&mut self) -> Result<()> {
        if let Some(handle) = self.health_check_handle.take() {
            handle.abort();
        }

        let node_probers = self.node_probers.clone();
        let interval = time::Duration::from_secs(self.interval_secs);

        let handle = tokio::spawn(async move {
            loop {
                let probers = node_probers.lock().await;
                let prober_refs: Vec<Arc<Mutex<NodeProber>>> = probers.values().cloned().collect();
                drop(probers);

                for prober in prober_refs {
                    let mut prober = prober.lock().await;
                    prober.check_health().await;
                }

                time::sleep(interval).await;
            }
        });

        self.health_check_handle = Some(handle);
        Ok(())
    }

    pub async fn run(&mut self) -> Result<()> {
        fs::create_dir_all(&self.log_dir).await?;
        self.start_health_check().await?;
        self.health_check_handle.take().unwrap().await?
    }
}

struct NodeProber {
    inner: StorageNode,
    log_file: fs::File,
    client: Client,
}

impl NodeProber {
    async fn new(node: StorageNode, log_dir: &Path) -> Result<Self> {
        let log_path = log_dir.join(format!("{}.log", node.node_id));
        let log_file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .await?;

        Ok(Self {
            client: Client::for_storage_node(&node.network_address.0, &node.network_public_key)
                .expect("Failed to create client"),
            inner: node,
            log_file,
        })
    }

    pub fn node_id(&self) -> ObjectID {
        self.inner.node_id
    }

    pub(crate) async fn check_health(&mut self) {
        let timestamp = Utc::now().to_rfc3339();

        let health_result = self
            .client
            .get_server_health_info(true)
            .await
            .map_err(|_| "Health check failed".to_string());

        let log_entry = match health_result {
            Ok(health_info) => format!("[{}] HEALTHY - {:?}\n", timestamp, health_info),
            Err(err) => format!("[{}] UNHEALTHY - {}\n", timestamp, err),
        };

        let _ = self.log_file.write_all(log_entry.as_bytes()).await;
    }
}

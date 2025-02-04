// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashMap, path::PathBuf, sync::Arc};

use anyhow::Result;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sui_types::base_types::ObjectID;
use tokio::{fs, io::AsyncWriteExt, sync::Mutex, task::JoinHandle, time};
use walrus_sdk::{api::ServiceHealthInfo, client::Client};
use walrus_sui::types::StorageNode;

const HEALTH_LOG_FILENAME: &str = "health.log";

#[derive(Debug, Serialize, Deserialize)]
pub struct HealthLogEntry {
    timestamp: String,
    node_id: ObjectID,
    health_info: Option<ServiceHealthInfo>,
    error: Option<String>,
}

impl HealthLogEntry {
    fn as_bytes(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_string(self)
            .map(|s| format!("{}\n", s))
            .map(String::into_bytes)?)
    }
}

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
        let prober = NodeProber::new(node).await?;
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
        let mut log_file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.log_dir.join(HEALTH_LOG_FILENAME))
            .await?;
        println!(
            "Health log file: {}",
            self.log_dir.join(HEALTH_LOG_FILENAME).display()
        );

        if let Some(handle) = self.health_check_handle.take() {
            handle.abort();
        }

        let node_probers = self.node_probers.clone();
        let start = time::Instant::now();
        let interval = time::Duration::from_secs(self.interval_secs);

        let handle = tokio::spawn(async move {
            loop {
                let probers = node_probers.lock().await;
                let prober_refs: Vec<Arc<Mutex<NodeProber>>> = probers.values().cloned().collect();
                drop(probers);

                for prober in prober_refs {
                    let mut prober = prober.lock().await;
                    let log_entry = prober.check_health().await;
                    if let Ok(bytes) = log_entry.as_bytes() {
                        let _ = log_file.write_all(&bytes).await;
                    }
                }

                time::sleep_until(start + interval).await;
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
    client: Client,
}

impl NodeProber {
    async fn new(node: StorageNode) -> Result<Self> {
        Ok(Self {
            client: Client::for_storage_node(&node.network_address.0, &node.network_public_key)
                .expect("Failed to create client"),
            inner: node,
        })
    }

    pub fn node_id(&self) -> ObjectID {
        self.inner.node_id
    }

    pub(crate) async fn check_health(&mut self) -> HealthLogEntry {
        let timestamp = Utc::now().to_rfc3339();

        let health_result = self
            .client
            .get_server_health_info(true)
            .await
            .map_err(|_| "Health check failed".to_string());

        if let Ok(health_info) = health_result {
            HealthLogEntry {
                timestamp,
                node_id: self.inner.node_id,
                health_info: Some(health_info),
                error: None,
            }
        } else {
            HealthLogEntry {
                timestamp,
                node_id: self.inner.node_id,
                health_info: None,
                error: Some(health_result.unwrap_err()),
            }
        }
    }
}

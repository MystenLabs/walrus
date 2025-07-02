// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Blocklist for blob IDs.
use std::{
    collections::HashSet,
    path::PathBuf,
    sync::{Arc, RwLock},
    time::Duration,
};

use anyhow::{Context, Result};
use prometheus::{IntGauge, register_int_gauge_with_registry};
use serde::{Deserialize, Serialize};
use serde_with::{DisplayFromStr, serde_as};
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use walrus_core::BlobId;
use walrus_utils::metrics::Registry;

/// Internal blocklist struct to deserialize from YAML.
#[serde_as]
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
struct BlocklistInner(#[serde_as(as = "Vec<DisplayFromStr>")] pub Vec<BlobId>);

/// A blocklist of blob IDs.
///
/// Supports checking if a blob ID is blocked and inserting/removing blob IDs.
#[derive(Debug, Default, Clone)]
pub struct Blocklist {
    blocked_blobs: Arc<RwLock<HashSet<BlobId>>>,
    deny_list_path: PathBuf,
    shutdown: CancellationToken,
    /// Metric to track the number of blocklisted blobs.
    blocklist_size_metric: Option<IntGauge>,
}

impl Blocklist {
    /// Reads a blocklist of blob IDs in YAML format from the provided path.
    ///
    /// If no path is provided, the returned blocklist is empty.
    ///
    /// Returns an error if the file is not found or parsing fails.
    pub fn new(path: &Option<PathBuf>) -> Result<Self> {
        Self::new_with_metrics(path, None)
    }

    /// Reads a blocklist of blob IDs in YAML format from the provided path with metrics support.
    ///
    /// If no path is provided, the returned blocklist is empty.
    /// If metrics_registry is provided, will expose a gauge metric for the number
    /// of blocklisted blobs.
    ///
    /// Returns an error if the file is not found or parsing fails.
    pub fn new_with_metrics(
        path: &Option<PathBuf>,
        metrics_registry: Option<&Registry>,
    ) -> Result<Self> {
        let blocklist_size_metric = metrics_registry.map(|registry| {
            register_int_gauge_with_registry!(
                "walrus_blocklist_size",
                "Number of blob IDs in the blocklist",
                registry,
            )
            .expect("this is a valid metrics registration")
        });

        let Some(path) = path else {
            let blocklist = Self {
                blocked_blobs: Arc::new(RwLock::new(HashSet::new())),
                deny_list_path: PathBuf::new(),
                shutdown: CancellationToken::new(),
                blocklist_size_metric,
            };

            if let Some(ref metric) = blocklist.blocklist_size_metric {
                metric.set(0);
            }

            return Ok(blocklist);
        };

        let blocklist = Self {
            blocked_blobs: Arc::new(RwLock::new(HashSet::new())),
            deny_list_path: path.clone(),
            shutdown: CancellationToken::new(),
            blocklist_size_metric,
        };

        blocklist.load()?;

        Ok(blocklist)
    }

    /// Starts a task to periodically refresh the blocklist.
    pub fn start_refresh_task(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(30));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        tracing::debug!("refreshing blocklist");
                        if let Err(error) = self.load() {
                            tracing::error!(?error, "failed to refresh blocklist");
                        }
                    }
                    _ = self.shutdown.cancelled() => {
                        tracing::info!("received shutdown signal");
                        break;
                    }
                }
            }
        });
    }

    /// Checks if a blob ID is blocked.
    #[inline]
    pub fn is_blocked(&self, blob_id: &BlobId) -> bool {
        let guard = self
            .blocked_blobs
            .read()
            .expect("mutex should not be poisoned");
        guard.contains(blob_id)
    }

    /// Adds a blob ID to the blocklist.
    ///
    /// Returns whether the ID was newly inserted.
    #[inline]
    pub fn insert(&mut self, blob_id: BlobId) -> Result<bool> {
        let mut guard: std::sync::RwLockWriteGuard<'_, HashSet<BlobId>> = self
            .blocked_blobs
            .write()
            .expect("mutex should not be poisoned");
        let was_inserted = guard.insert(blob_id);

        if let Some(ref metric) = self.blocklist_size_metric {
            metric.set(guard.len().try_into()?);
        }

        // Update yaml file to add this blob id
        let blobs = BlocklistInner(guard.iter().cloned().collect::<Vec<_>>());
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(&self.deny_list_path)?;
        serde_yaml::to_writer(&mut file, &blobs)?;
        Ok(was_inserted)
    }

    /// Removes a blob ID from the blocklist.
    ///
    /// Returns whether the ID was previously blocked.
    #[inline]
    pub fn remove(&mut self, blob_id: &BlobId) -> Result<bool> {
        let mut guard: std::sync::RwLockWriteGuard<'_, HashSet<BlobId>> = self
            .blocked_blobs
            .write()
            .expect("mutex should not be poisoned");
        let was_removed = guard.remove(blob_id);

        if let Some(ref metric) = self.blocklist_size_metric {
            metric.set(guard.len().try_into()?);
        }

        let blobs = BlocklistInner(guard.iter().cloned().collect::<Vec<_>>());

        if !self.deny_list_path.exists() {
            return Ok(false);
        };

        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&self.deny_list_path)?;
        serde_yaml::to_writer(&mut file, &blobs)?;
        Ok(was_removed)
    }

    /// Loads the blocklist from the file at the given path.
    fn load(&self) -> Result<()> {
        if !self.deny_list_path.exists() {
            return Ok(());
        }

        let content = std::fs::read_to_string(&self.deny_list_path).context(format!(
            "Unable to read blocklist file at {}",
            self.deny_list_path.display()
        ))?;

        if content.is_empty() {
            return Ok(());
        }

        let blocklist: BlocklistInner = serde_yaml::from_str(&content).context(format!(
            "Parsing blocklist at {} failed",
            self.deny_list_path.display()
        ))?;

        let mut guard = self
            .blocked_blobs
            .write()
            .expect("mutex should not be poisoned");
        let old_blobs = guard.iter().cloned().collect::<HashSet<_>>();
        let new_blobs = blocklist.0.iter().cloned().collect::<HashSet<_>>();
        let added_blobs = new_blobs.difference(&old_blobs).collect::<Vec<_>>();
        let removed_blobs = old_blobs.difference(&new_blobs).collect::<Vec<_>>();

        added_blobs.iter().for_each(|blob_id| {
            tracing::info!(%blob_id, "added blob to blocklist");
        });

        removed_blobs.iter().for_each(|blob_id| {
            tracing::info!(%blob_id, "removed blob from blocklist");
        });

        *guard = blocklist.0.into_iter().collect();

        if let Some(ref metric) = self.blocklist_size_metric {
            metric.set(guard.len().try_into()?);
        }

        Ok(())
    }
}

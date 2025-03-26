// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::HashSet,
    path::PathBuf,
    sync::{Arc, RwLock},
    time::Duration,
};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use walrus_core::BlobId;

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
}

impl Blocklist {
    /// Reads a blocklist of blob IDs in YAML format from the provided path.
    ///
    /// If no path is provided, the returned blocklist is empty.
    ///
    /// Returns an error if the file is not found or parsing fails.
    pub fn new(path: &Option<PathBuf>) -> Result<Self> {
        let Some(path) = path else {
            return Ok(Self::default());
        };

        let blocklist = Self {
            blocked_blobs: Arc::new(RwLock::new(HashSet::new())),
            deny_list_path: path.clone(),
            shutdown: CancellationToken::new(),
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
                        tracing::debug!("Refreshing blocklist");
                        if let Err(e) = self.load() {
                            tracing::error!("Failed to refresh deny list: {}", e);
                        }
                    }
                    _ = self.shutdown.cancelled() => {
                        tracing::info!("Received shutdown signal");
                        break;
                    }
                }
            }
        });
    }

    /// Checks if a blob ID is blocked.
    #[inline]
    pub fn is_blocked(&self, blob_id: &BlobId) -> bool {
        let guard = self.blocked_blobs.read().expect("mutex poisoned");
        guard.contains(blob_id)
    }

    /// Adds a blob ID to the blocklist.
    ///
    /// Returns whether the ID was newly inserted.
    #[inline]
    pub fn insert(&mut self, blob_id: BlobId) -> Result<bool> {
        let mut guard: std::sync::RwLockWriteGuard<'_, HashSet<BlobId>> =
            self.blocked_blobs.write().expect("mutex poisoned");
        guard.insert(blob_id);
        // Update yaml file to add this blob id
        let blobs = BlocklistInner(guard.iter().cloned().collect::<Vec<_>>());
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(&self.deny_list_path)?;
        serde_yaml::to_writer(&mut file, &blobs)?;
        Ok(true)
    }

    /// Removes a blob ID from the blocklist.
    ///
    /// Returns whether the ID was previously blocked.
    #[inline]
    pub fn remove(&mut self, blob_id: &BlobId) -> Result<bool> {
        let mut guard: std::sync::RwLockWriteGuard<'_, HashSet<BlobId>> =
            self.blocked_blobs.write().expect("mutex poisoned");
        guard.remove(blob_id);
        let blobs = BlocklistInner(guard.iter().cloned().collect::<Vec<_>>());

        if !self.deny_list_path.exists() {
            return Ok(false);
        };

        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&self.deny_list_path)?;
        serde_yaml::to_writer(&mut file, &blobs)?;
        Ok(true)
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

        let mut guard = self.blocked_blobs.write().expect("mutex poisoned");
        let old_blobs = guard.iter().cloned().collect::<HashSet<_>>();
        let new_blobs = blocklist.0.iter().cloned().collect::<HashSet<_>>();
        let added_blobs = new_blobs.difference(&old_blobs).collect::<Vec<_>>();
        let removed_blobs = old_blobs.difference(&new_blobs).collect::<Vec<_>>();

        added_blobs.iter().for_each(|blob_id| {
            tracing::info!("Added blob to deny list: {}", blob_id);
        });

        removed_blobs.iter().for_each(|blob_id| {
            tracing::info!("Removed blob from deny list: {}", blob_id);
        });

        *guard = blocklist.0.into_iter().collect();
        Ok(())
    }
}

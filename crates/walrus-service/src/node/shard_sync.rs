// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{HashMap, hash_map::Entry},
    sync::Arc,
};

use futures::{StreamExt, stream::FuturesUnordered};
#[cfg(msim)]
use sui_macros::{fail_point_arg, fail_point_if};
use tokio::{
    sync::{Mutex, Semaphore},
    time::Instant,
};
use walrus_core::{BlobId, Epoch, ShardIndex};
use walrus_storage_node_client::error::ServiceError;
use walrus_utils::backoff::{BackoffStrategy, ExponentialBackoff};

use super::{
    NodeStatus,
    StorageNodeInner,
    blob_retirement_notifier::ExecutionResultWithRetirementCheck,
    config::ShardSyncConfig,
    errors::SyncShardClientError,
    storage::{ShardStatus, ShardStorage, blob_info::BlobInfo},
};
use crate::node::{errors::ShardNotAssigned, storage::blob_info::CertifiedBlobInfoApi};

/// The result of syncing a shard.
enum SyncShardResult {
    /// The shard sync finished successfully.
    Success,
    /// The shard sync is not finished and should be retried after a backoff.
    /// The first bool indicates whether to directly recover the shard instead of using shard sync.
    /// The second bool indicates whether the shard sync made progress.
    RetryAfterBackoff {
        force_recovery: bool,
        shard_sync_made_progress: bool,
    },
    /// The shard sync contains errors and should be stopped.
    Failed,
}

/// Manages tasks for syncing shards during epoch change.
#[derive(Debug, Clone)]
pub struct ShardSyncHandler {
    node: Arc<StorageNodeInner>,
    shard_sync_in_progress: Arc<Mutex<HashMap<ShardIndex, tokio::task::JoinHandle<()>>>>,
    task_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    shard_sync_semaphore: Arc<Semaphore>,
    config: ShardSyncConfig,
}

impl ShardSyncHandler {
    pub fn new(node: Arc<StorageNodeInner>, config: ShardSyncConfig) -> Self {
        Self {
            node,
            shard_sync_in_progress: Arc::new(Mutex::new(HashMap::new())),
            task_handle: Arc::new(Mutex::new(None)),
            shard_sync_semaphore: Arc::new(Semaphore::new(config.shard_sync_concurrency)),
            config,
        }
    }

    /// Starts sync shards. If `recover_metadata` is true, syncs certified blob metadata before
    /// syncing shards.
    pub async fn start_sync_shards(
        &self,
        shards: Vec<ShardIndex>,
        recover_metadata: bool,
    ) -> Result<(), SyncShardClientError> {
        let mut task_handle = self.task_handle.lock().await;
        let sync_handler = self.clone();
        let new_task = tokio::spawn(async move {
            sync_handler
                .sync_shards_task(shards, recover_metadata)
                .await
        });

        // Abort any existing task before replacing it
        if let Some(old_task) = task_handle.replace(new_task) {
            old_task.abort();
        }

        Ok(())
    }

    async fn sync_shards_task(&self, shards: Vec<ShardIndex>, recover_metadata: bool) {
        if recover_metadata {
            let node_status = self
                .node
                .storage
                .node_status()
                .expect("failed to read node status from db");
            assert_eq!(node_status, NodeStatus::RecoverMetadata);

            if let Err(err) = self.sync_certified_blob_metadata().await {
                tracing::error!(?err, "failed to sync blob metadata; aborting shard sync");
                return;
            }
        }

        // Start sync for each shard
        for shard in shards {
            if let Err(err) = self.start_new_shard_sync(shard).await {
                tracing::error!(?err, %shard, "failed to start shard sync; skipping shard");
                continue;
            }
        }

        // Once we have started the shard sync task, the shard status has been persisted to
        // disk, so we can mark the node as active. Any restart from this point will re-start
        // the shard sync tasks only without syncing metadata again.
        let node_status = self
            .node
            .storage
            .node_status()
            .expect("failed to read node status from db");
        if node_status == NodeStatus::RecoverMetadata {
            self.node
                .set_node_status(NodeStatus::Active)
                .expect("failed to set node status to Active");
        }
    }

    /// Syncs the certified blob metadata before the current epoch.
    ///
    /// This function performs the following steps:
    /// 1. Retrieves all certified blob info from storage before the current epoch
    /// 2. Processes blobs concurrently up to max_concurrent_metadata_fetch limit
    /// 3. For each blob, syncs its metadata using sync_single_blob_metadata
    async fn sync_certified_blob_metadata(&self) -> Result<(), SyncShardClientError> {
        tracing::info!("start syncing blob metadata");
        let blob_infos = self
            .node
            .storage
            .certified_blob_info_iter_before_epoch(self.node.current_epoch());

        #[cfg(msim)]
        {
            inject_recovery_metadata_failure_before_fetch()?;
        }

        let mut futures = FuturesUnordered::new();
        let mut active_count = 0;

        #[cfg(msim)]
        let mut scan_count = 0; // Used to trigger fail point

        for blob_info in blob_infos {
            let (blob_id, blob_info) = blob_info?;
            let node_clone = self.node.clone();

            // TODO(WAL-478):
            //   - create a end point that can transfer multiple blob metadata at once.
            futures.push(Self::sync_single_blob_metadata(
                node_clone, blob_id, blob_info,
            ));
            active_count += 1;

            #[cfg(msim)]
            {
                scan_count += 1;
                inject_recovery_metadata_failure_during_fetch(scan_count)?;
            }

            // Wait for a task to complete if we've reached max concurrent limit
            while active_count >= self.config.max_concurrent_metadata_fetch {
                // Process one completed future
                if let Some(result) = futures.next().await {
                    result.map_err(|e| SyncShardClientError::Internal(e.into()))?;
                }
                active_count -= 1;
            }
        }

        // Wait for remaining tasks to complete
        while let Some(result) = futures.next().await {
            result.map_err(|e| SyncShardClientError::Internal(e.into()))?;
        }

        tracing::info!("finished syncing blob metadata");
        Ok(())
    }

    /// Syncs a single blob metadata.
    async fn sync_single_blob_metadata(
        node: Arc<StorageNodeInner>,
        blob_id: BlobId,
        blob_info: BlobInfo,
    ) -> Result<(), SyncShardClientError> {
        node.metrics
            .sync_blob_metadata_progress
            .set(i64::from(blob_id.first_two_bytes()));

        let result = node
            .blob_retirement_notifier
            .execute_with_retirement_check(&node, blob_id, || {
                node.get_or_recover_blob_metadata(
                    &blob_id,
                    blob_info
                        .initial_certified_epoch()
                        .expect("certified blob must have certified epoch set"),
                )
            })
            .await?;

        match result {
            ExecutionResultWithRetirementCheck::Executed(result) => {
                result?;
                node.metrics.sync_blob_metadata_count.inc();
            }
            ExecutionResultWithRetirementCheck::BlobRetired => {
                tracing::debug!(%blob_id, "blob retired; skipping sync");
                node.metrics.sync_blob_metadata_skipped.inc();
            }
        }

        Ok(())
    }

    /// Starts syncing a new shard. This method is used when a new shard is assigned to the node.
    async fn start_new_shard_sync(
        &self,
        shard_index: ShardIndex,
    ) -> Result<(), SyncShardClientError> {
        // restart_syncs() is called before event processor starts processing events. So, for any
        // resumed shard syncs, we should be able to observe them here, unless they have finished.
        if self
            .shard_sync_in_progress
            .lock()
            .await
            .contains_key(&shard_index)
        {
            tracing::info!(
                walrus.shard_index = %shard_index,
                "shard is already being synced; skipping starting new shard sync"
            );
            return Ok(());
        }

        // Get shard storage
        let shard_storage = self
            .node
            .storage
            .shard_storage(shard_index)
            .await
            .ok_or_else(|| {
                tracing::error!(
                    "{shard_index} is not assigned to this node; cannot start shard sync"
                );
                ShardNotAssigned(shard_index, self.node.current_epoch())
            })?;

        let shard_status = shard_storage.status()?;

        // Skip if shard is already active
        if shard_status == ShardStatus::Active {
            tracing::info!(
                walrus.shard_index = %shard_index,
                "shard has already been synced; skipping sync"
            );
            return Ok(());
        }

        // Update status and start sync. After this function returns, we can always restart
        // the sync upon node restart.
        shard_storage.record_start_shard_sync()?;
        self.start_shard_sync_impl(shard_storage.clone()).await;
        Ok(())
    }

    /// Restarts syncing shards that were previously syncing. This method is used when restarting
    /// the node.
    pub async fn restart_syncs(&self) -> Result<(), anyhow::Error> {
        let current_node_status = self.node.storage.node_status()?;
        if current_node_status == NodeStatus::RecoverMetadata {
            let shards_to_sync = self
                .node
                .storage
                .existing_shard_storages()
                .await
                .iter()
                .map(|s| s.id())
                .collect::<Vec<_>>();

            let sync_handler_clone = self.clone();
            self.task_handle
                .lock()
                .await
                .replace(tokio::spawn(async move {
                    sync_handler_clone
                        .sync_shards_task(shards_to_sync, true)
                        .await
                }));
        } else {
            for shard_storage in self.node.storage.existing_shard_storages().await {
                // Restart the syncing task for shards that were previously syncing (in ActiveSync
                // status).
                let shard_status = shard_storage.status()?;
                match shard_status {
                    ShardStatus::ActiveSync => {
                        self.start_shard_sync_impl(shard_storage.clone()).await;
                    }
                    ShardStatus::ActiveRecover => {
                        if self.config.restart_shard_sync_always_retry_transfer_first {
                            let shard_last_sync_status =
                                shard_storage.resume_active_shard_sync()?;
                            tracing::info!(
                                walrus.shard_index = %shard_storage.id(),
                                ?shard_last_sync_status,
                                "resuming shard sync from the last synced blob id"
                            );
                        }
                        self.start_shard_sync_impl(shard_storage.clone()).await;
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    }

    async fn start_shard_sync_impl(&self, shard_storage: Arc<ShardStorage>) {
        // This epoch must be the same as the epoch in the committee we refreshed when processing
        // epoch start event, or when the node starts up.
        let current_epoch = self.node.current_epoch();

        tracing::info!(
            walrus.shard_index = %shard_storage.id(),
            "syncing shard to the beginning of epoch {}",
            current_epoch
        );

        let mut shard_sync_in_progress = self.shard_sync_in_progress.lock().await;
        let Entry::Vacant(entry) = shard_sync_in_progress.entry(shard_storage.id()) else {
            // We have checked the shard_sync_in_progress map before starting the sync task. So,
            // this is an unexpected state.
            tracing::error!(
                shard_index=%shard_storage.id(),
                "shard is already being synced; skipping starting new sync task",
            );
            return;
        };

        let shard_sync_handler_clone = self.clone();
        let shard_sync_task = tokio::spawn(async move {
            let shard_index = shard_storage.id();
            let mut last_progress_time = Instant::now();

            let mut backoff = ExponentialBackoff::new_with_seed(
                shard_sync_handler_clone.config.shard_sync_retry_min_backoff,
                shard_sync_handler_clone.config.shard_sync_retry_max_backoff,
                None,
                u64::from(shard_index.0), // Seed the backoff with the shard index.
            );

            // Whether to directly recover the shard instead of using shard sync.
            let mut directly_recover_shard = false;
            let mut shard_sync_success = false;
            loop {
                tracing::info!(
                    shard_index=%shard_index,
                    ?directly_recover_shard,
                    "syncing shard to the beginning of epoch {}",
                    current_epoch
                );
                match shard_sync_handler_clone
                    .sync_shard_impl(shard_storage.clone(), current_epoch, directly_recover_shard)
                    .await
                {
                    SyncShardResult::Success => {
                        shard_sync_success = true;
                        break;
                    }
                    SyncShardResult::Failed => {
                        tracing::warn!(
                            shard_index=%shard_index,
                            "shard sync stopped due to errors; restart node to retry shard sync"
                        );
                        break;
                    }
                    SyncShardResult::RetryAfterBackoff {
                        force_recovery,
                        shard_sync_made_progress,
                    } => {
                        let backoff_duration = backoff.next_delay();
                        let Some(backoff_duration) = backoff_duration else {
                            tracing::warn!(
                                shard_index=%shard_index,
                                "maximum number of retries reached; stop shard sync; \
                                restart node to retry shard sync"
                            );
                            break;
                        };
                        tokio::time::sleep(backoff_duration).await;

                        if shard_sync_made_progress {
                            tracing::debug!(
                                shard_index=%shard_index,
                                "shard sync made progress"
                            );
                            last_progress_time = Instant::now();
                        }
                        if last_progress_time.elapsed()
                            > shard_sync_handler_clone
                                .config
                                .shard_sync_retry_switch_to_recovery_interval
                            || force_recovery
                        {
                            tracing::info!(
                                shard_index=%shard_index,
                                "shard sync failed; directly recovering shard"
                            );
                            directly_recover_shard = true;
                        }
                    }
                }
            }

            // Remove the task from the shard_sync_in_progress map upon completion.
            let epoch_sync_done = if shard_sync_success {
                let mut shard_sync_map =
                    shard_sync_handler_clone.shard_sync_in_progress.lock().await;
                shard_sync_map.remove(&shard_index);
                shard_sync_map.is_empty()
            } else {
                false
            };

            if epoch_sync_done {
                shard_sync_handler_clone
                    .node
                    .contract_service
                    .epoch_sync_done(
                        current_epoch,
                        shard_sync_handler_clone.node.node_capability(),
                    )
                    .await;
            }
        });
        entry.insert(shard_sync_task);
    }

    /// Syncs a shard using shard sync. If `directly_recover_shard` is true, the shard will be
    /// directly recovered instead of using shard sync.
    async fn sync_shard_impl(
        &self,
        shard_storage: Arc<ShardStorage>,
        current_epoch: Epoch,
        directly_recover_shard: bool,
    ) -> SyncShardResult {
        // The rate limit is enforced by the semaphore, without considering
        // the priority of the syncs.
        let Ok(_permit) = self.shard_sync_semaphore.acquire().await else {
            tracing::error!("failed to acquire shard sync semaphore.");
            return SyncShardResult::RetryAfterBackoff {
                force_recovery: false,
                shard_sync_made_progress: false,
            };
        };

        walrus_utils::with_label!(self.node.metrics.shard_sync_total, "start").inc();
        let shard_index = shard_storage.id();
        let (shard_sync_made_progress, sync_result) = shard_storage
            .start_sync_shard_before_epoch(
                current_epoch,
                self.node.clone(),
                &self.config,
                directly_recover_shard,
            )
            .await;
        match sync_result {
            Ok(_) => {
                walrus_utils::with_label!(self.node.metrics.shard_sync_total, "complete").inc();
                tracing::info!(
                    walrus.shard_index = %shard_index,
                    "successfully synced shard to before epoch {}",
                    current_epoch
                );
                SyncShardResult::Success
            }
            Err(error) => {
                walrus_utils::with_label!(self.node.metrics.shard_sync_total, "error").inc();
                tracing::error!(
                    ?error,
                    "failed to sync {shard_index} to before epoch {current_epoch}"
                );

                #[cfg(msim)]
                if check_no_retry_fail_point() {
                    return SyncShardResult::Success;
                }

                Self::handle_sync_error(
                    &error,
                    shard_index,
                    directly_recover_shard,
                    shard_sync_made_progress,
                )
            }
        }
    }

    /// Handles sync shard errors and determines whether/how to retry
    fn handle_sync_error(
        error: &SyncShardClientError,
        shard_index: ShardIndex,
        directly_recover_shard: bool,
        shard_sync_made_progress: bool,
    ) -> SyncShardResult {
        if let SyncShardClientError::RequestError(node_error) = error {
            // Handle epoch-related errors
            match node_error.service_error() {
                Some(ServiceError::InvalidEpoch {
                    request_epoch,
                    server_epoch,
                }) => {
                    if request_epoch > server_epoch {
                        tracing::info!(
                            request_epoch,
                            server_epoch,
                            shard_sync_made_progress,
                            "source storage node hasn't reached the epoch yet"
                        );
                        return SyncShardResult::RetryAfterBackoff {
                            force_recovery: false,
                            shard_sync_made_progress,
                        };
                    }
                }
                Some(ServiceError::RequestUnauthorized) => {
                    tracing::info!(
                        ?error,
                        shard_sync_made_progress,
                        "source storage node may not reach to the epoch where the \
                        destination storage node is in the committee; retry shard sync"
                    );
                    return SyncShardResult::RetryAfterBackoff {
                        force_recovery: false,
                        shard_sync_made_progress,
                    };
                }
                _ => {}
            }

            // Handle network errors. This means to capture all the networking related errors.
            // We want to retry shard sync instead of directly recovering the shard.
            if node_error.is_reqwest() {
                tracing::info!(
                    ?error,
                    shard_sync_made_progress,
                    "encounter reqwest error; retry shard sync"
                );
                return SyncShardResult::RetryAfterBackoff {
                    force_recovery: false,
                    shard_sync_made_progress,
                };
            }
        }

        if cfg!(msim)
            && error
                .to_string()
                .contains("fetch_sliver simulated sync failure, retryable: true")
        {
            return SyncShardResult::RetryAfterBackoff {
                force_recovery: false,
                shard_sync_made_progress,
            };
        }

        // Shard sync encountered non-retryable error. Try direct recovery if not already doing so
        if !directly_recover_shard {
            tracing::warn!(
                walrus.shard_index = %shard_index,
                ?error,
                shard_sync_made_progress,
                "shard sync failed; directly recovering shard next time"
            );
            SyncShardResult::RetryAfterBackoff {
                force_recovery: true,
                shard_sync_made_progress,
            }
        } else {
            tracing::warn!(
                walrus.shard_index = %shard_index,
                ?error,
                shard_sync_made_progress,
                "shard recovery also failed; stop shard sync"
            );
            SyncShardResult::Failed
        }
    }

    #[cfg(test)]
    pub async fn current_sync_task_count(&self) -> usize {
        self.shard_sync_in_progress
            .lock()
            .await
            .values()
            .filter(|task| !task.is_finished())
            .count()
    }

    #[cfg(all(msim, test, feature = "test-utils"))]
    pub async fn no_pending_recover_metadata(&self) -> bool {
        let task_handle = self.task_handle.lock().await;
        task_handle.is_none() || task_handle.as_ref().unwrap().is_finished()
    }

    #[cfg(all(msim, test, feature = "test-utils"))]
    pub async fn clear_shard_sync_tasks(&self) {
        self.shard_sync_in_progress.lock().await.clear();
    }
}

// Helper function for fail point testing
#[cfg(msim)]
fn check_no_retry_fail_point() -> bool {
    let mut no_retry = false;
    sui_macros::fail_point_if!("fail_point_shard_sync_no_retry", || { no_retry = true });
    no_retry
}

// Inject a failure point to simulate a sync failure.
#[cfg(msim)]
fn inject_recovery_metadata_failure_before_fetch() -> Result<(), SyncShardClientError> {
    let mut sync_blob_metadata_error = false;
    fail_point_if!(
        "fail_point_shard_sync_recovery_metadata_error_before_fetch",
        || {
            sync_blob_metadata_error = true;
        }
    );

    if sync_blob_metadata_error {
        return Err(SyncShardClientError::Internal(anyhow::anyhow!(
            "fail point triggered sync blob metadata error before fetching"
        )));
    }
    Ok(())
}

// Inject a failure point to simulate a sync failure.
#[cfg(msim)]
fn inject_recovery_metadata_failure_during_fetch(
    scan_count: u64,
) -> Result<(), SyncShardClientError> {
    let mut sync_blob_metadata_error = false;
    fail_point_arg!(
        "fail_point_shard_sync_recovery_metadata_error_during_fetch",
        |trigger_at: u64| {
            tracing::info!(
                trigger_index = ?trigger_at,
                blob_count = ?scan_count,
                fail_point = "fail_point_shard_sync_recovery_metadata_error_during_fetch",
            );
            if trigger_at == scan_count {
                sync_blob_metadata_error = true;
            }
        }
    );

    if sync_blob_metadata_error {
        return Err(SyncShardClientError::Internal(anyhow::anyhow!(
            "fail point triggered sync blob metadata error during fetching"
        )));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{StorageNodeHandle, TestCluster};

    async fn create_test_cluster(assignment: &[&[u16]]) -> TestCluster {
        TestCluster::<StorageNodeHandle>::builder()
            .with_shard_assignment(assignment)
            .build()
            .await
            .unwrap()
    }

    #[tokio::test(start_paused = false)]
    async fn test_restart_syncs() {
        let cluster = create_test_cluster(&[&[0, 1, 2]]).await;
        for i in [0, 2] {
            cluster.nodes[0]
                .storage_node
                .inner
                .storage
                .shard_storage(ShardIndex(i))
                .await
                .expect("Failed to get shard storage")
                .update_status_in_test(ShardStatus::ActiveSync)
                .expect("Failed to update shard status");
        }
        let shard_sync_handler = ShardSyncHandler::new(
            cluster.nodes[0].storage_node.inner.clone(),
            ShardSyncConfig::default(),
        );
        shard_sync_handler
            .restart_syncs()
            .await
            .expect("Failed to restart syncs");
        assert_eq!(shard_sync_handler.current_sync_task_count().await, 2);
        assert!(
            shard_sync_handler
                .shard_sync_in_progress
                .lock()
                .await
                .contains_key(&ShardIndex(0))
        );
        assert!(
            shard_sync_handler
                .shard_sync_in_progress
                .lock()
                .await
                .contains_key(&ShardIndex(2))
        );
    }

    #[tokio::test(start_paused = false)]
    async fn test_start_new_shard_sync() {
        let cluster = create_test_cluster(&[&[0]]).await;
        let shard_sync_handler = ShardSyncHandler::new(
            cluster.nodes[0].storage_node.inner.clone(),
            ShardSyncConfig::default(),
        );

        cluster.nodes[0]
            .storage_node
            .inner
            .storage
            .shard_storage(ShardIndex(0))
            .await
            .expect("Failed to get shard storage")
            .update_status_in_test(ShardStatus::None)
            .expect("Failed to update shard status");

        assert_eq!(shard_sync_handler.current_sync_task_count().await, 0);
        shard_sync_handler
            .start_new_shard_sync(ShardIndex(0))
            .await
            .expect("Failed to start new shard sync");
        assert_eq!(shard_sync_handler.current_sync_task_count().await, 1);

        assert!(matches!(
            shard_sync_handler.start_new_shard_sync(ShardIndex(1)).await,
            Err(SyncShardClientError::ShardNotAssigned(..))
        ));
    }
}

// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Async blob uploader for background uploads to additional storage nodes after quorum is reached.

use std::time::Instant;

use futures::{
    StreamExt,
    stream::FuturesUnordered,
};
use tokio::{
    sync::mpsc,
    task::JoinHandle,
    time::{Duration, timeout},
};
use tracing::{debug, info, warn};
use walrus_core::BlobId;

use crate::error::{ClientError, ClientResult};

/// A task for uploading blob data to additional nodes in the background.
pub struct BlobUploadTask {
    /// The blob identifier for logging.
    pub blob_id: BlobId,

    /// Remaining futures to execute for uploading to additional nodes.
    /// These are futures that will complete the upload to nodes that haven't confirmed yet.
    pub remaining_futures: Vec<std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send>>>,

    /// Maximum duration for the background upload.
    pub upload_duration: Duration,
}

/// Manages background blob upload tasks.
#[derive(Debug)]
pub struct AsyncBlobUploader {
    /// Channel to send new upload tasks.
    task_sender: mpsc::UnboundedSender<BlobUploadTask>,

    /// Handle to the background task.
    task_handle: Option<JoinHandle<()>>,
}

impl AsyncBlobUploader {
    /// Create a new async blob uploader.
    pub fn new(max_concurrent_tasks: usize) -> Self {
        let (task_sender, task_receiver) = mpsc::unbounded_channel();

        let task_handle = tokio::spawn(Self::run_background_loop(
            task_receiver,
            max_concurrent_tasks,
        ));

        Self {
            task_sender,
            task_handle: Some(task_handle),
        }
    }

    /// Submit a new upload task to the background worker.
    pub fn submit_task(&self, task: BlobUploadTask) -> ClientResult<()> {
        self.task_sender.send(task)
            .map_err(|e| ClientError::store_blob_internal(format!("Failed to submit background upload task: {}", e)))?;
        Ok(())
    }

    /// Shutdown the uploader gracefully.
    pub async fn shutdown(mut self) -> ClientResult<()> {
        // Close the channel to signal shutdown
        drop(self.task_sender);

        // Wait for background task to complete
        if let Some(handle) = self.task_handle.take() {
            let _ = handle.await;
        }

        Ok(())
    }

    /// Background loop that processes upload tasks.
    async fn run_background_loop(
        mut task_receiver: mpsc::UnboundedReceiver<BlobUploadTask>,
        max_concurrent_tasks: usize,
    ) {
        let mut running_tasks = FuturesUnordered::new();

        loop {
            tokio::select! {
                // Handle new tasks if we have capacity
                Some(task) = task_receiver.recv(), if running_tasks.len() < max_concurrent_tasks => {
                    let blob_id = task.blob_id;
                    let duration = task.upload_duration;

                    debug!(%blob_id, ?duration, "Starting background upload task");

                    // Execute the task with its timeout
                    running_tasks.push(async move {
                        let start = Instant::now();
                        let mut successful = 0;
                        let mut failed = 0;

                        let mut futures = FuturesUnordered::new();
                        for fut in task.remaining_futures {
                            futures.push(fut);
                        }

                        // Run with timeout
                        let _ = timeout(duration, async {
                            while let Some(result) = futures.next().await {
                                match result {
                                    Ok(()) => successful += 1,
                                    Err(_) => failed += 1,
                                }
                            }
                        }).await;

                        let elapsed = start.elapsed();
                        info!(
                            %blob_id,
                            successful_nodes = successful,
                            failed_nodes = failed,
                            duration_secs = elapsed.as_secs(),
                            "Background upload completed"
                        );
                    });
                }

                // Process completed tasks
                Some(()) = running_tasks.next() => {
                    // Task completed, metrics already logged
                }

                // Exit when channel is closed and no tasks remain
                else => {
                    if running_tasks.is_empty() {
                        debug!("Background uploader shutting down");
                        break;
                    }
                }
            }
        }

        // Wait for any remaining tasks to complete
        while let Some(()) = running_tasks.next().await {
            // Task completed
        }

        info!("Async blob uploader terminated");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_async_uploader_creation_and_shutdown() {
        let uploader = AsyncBlobUploader::new(10);
        assert!(uploader.shutdown().await.is_ok());
    }
}

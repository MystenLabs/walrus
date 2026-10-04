// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Walrus pool expansion precedes Strata's generic per-blob and epoch worker.

use strata::queue::{QueueWorker, WorkerConfig, WorkerProgress};
use tokio::sync::watch;

use super::*;
use crate::node::storage::blob_info::BlobInfoTable;

#[derive(Clone)]
pub(crate) struct StrataWorker {
    queue: StrataQueue,
    blob_info: BlobInfoTable,
    pub(super) worker: QueueWorker,
}

impl StrataWorker {
    pub(in crate::node::storage) fn new(
        queue: StrataQueue,
        blob_info: BlobInfoTable,
        worker: QueueWorker,
    ) -> Self {
        Self {
            queue,
            blob_info,
            worker,
        }
    }

    pub(crate) async fn process_batch(&self) -> Result<WorkerProgress, TypedStoreError> {
        self.blob_info
            .finish_strata_pool_extensions(&self.queue)
            .await?;
        self.worker.process_batch().await.map_err(queue_error)
    }

    pub(crate) async fn run(
        &self,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), TypedStoreError> {
        loop {
            if *shutdown.borrow() || shutdown.has_changed().is_err() {
                return Ok(());
            }
            if self.process_batch().await?.is_idle() {
                tokio::select! {
                    _ = tokio::time::sleep(WorkerConfig::default().poll_interval) => {},
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            return Ok(());
                        }
                    }
                }
            }
        }
    }
}

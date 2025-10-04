
// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! A component for orchestrating the distributed upload of multiple blobs to the Walrus network.
//!
//! The `DistributedUploader` is designed to handle the complexity of a multi-blob, multi-stage
//! upload process in an efficient and robust manner. It is the single source of truth for the
//! core upload logic, used by all parts of the client.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::Future;
use tokio::sync::Semaphore;
use walrus_core::metadata::VerifiedBlobMetadataWithId;
use walrus_core::{BlobId, encoding::SliverPair};

use crate::active_committees::ActiveCommittees;
use crate::client::communication::{NodeResult, NodeWriteCommunication};
use crate::utils::WeightedFutures;
use crate::error::ClientError;
use crate::config::SliverWriteExtraTime;

/// A trait for types that have a `BlobId`.
pub trait HasBlobId {
    fn blob_id(&self) -> &BlobId;
}

impl HasBlobId for BlobId {
    fn blob_id(&self) -> &BlobId {
        self
    }
}

impl<T> HasBlobId for (BlobId, T) {
    fn blob_id(&self) -> &BlobId {
        &self.0
    }
}

/// A work item for the uploader, representing a set of sliver pairs for a single blob
/// that need to be sent to a specific node.
#[derive(Debug)]
pub struct UploadWorkItem<'a> {
    pub metadata: &'a VerifiedBlobMetadataWithId,
    pub pairs: Vec<&'a SliverPair>,
}

impl<'a> UploadWorkItem<'a> {
    pub fn blob_id(&self) -> &BlobId {
        self.metadata.blob_id()
    }
}

/// Tracks the upload progress for a single blob.
#[derive(Debug, Clone, Default)]
pub struct BlobUploadProgress {
    /// The total weight of the nodes that have successfully stored the slivers for this blob.
    pub completed_weight: usize,
    /// A flag indicating whether the quorum has been reached for this blob.
    pub quorum_reached: bool,
}

// Default derived above

/// Events emitted by the `DistributedUploader` to report progress.
#[derive(Debug)]
pub enum UploaderEvent {
    /// A blob has reached the required quorum of storage nodes.
    BlobQuorumReached {
        blob_id: BlobId,
        elapsed: Duration,
    },
    /// A blob has been successfully uploaded to all nodes.
    BlobAllSliversUploaded { blob_id: BlobId },
}

/// The `DistributedUploader` component.
#[derive(Debug)]
pub struct DistributedUploader<'a> {
    /// The blobs to be uploaded.
    work_items: HashMap<usize, Vec<UploadWorkItem<'a>>>,
    /// The committees object.
    committees: Arc<ActiveCommittees>,
    /// A map to track the upload progress for each blob.
    progress: HashMap<BlobId, BlobUploadProgress>,
    /// The communication objects for the storage nodes.
    comms: Vec<NodeWriteCommunication<'a>>,
    /// The concurrency limiter for the uploads.
    sliver_write_limit: usize,
    /// The extra time to wait for tail-end writes.
    sliver_write_extra_time: SliverWriteExtraTime,
}

impl<'a> DistributedUploader<'a> {
    /// Creates a new `DistributedUploader`.
    pub fn new(
        blobs: &[(&'a VerifiedBlobMetadataWithId, &'a [SliverPair])],
        committees: Arc<ActiveCommittees>,
        comms: Vec<NodeWriteCommunication<'a>>,
        sliver_write_limit: usize,
        sliver_write_extra_time: SliverWriteExtraTime,
    ) -> Self {
        let mut work_items: HashMap<usize, Vec<UploadWorkItem>> = HashMap::new();
        let mut progress: HashMap<BlobId, BlobUploadProgress> = HashMap::new();

        for (metadata, pairs) in blobs {
            let blob_id = *metadata.blob_id();
            progress.entry(blob_id).or_default();

            let mut pairs_per_node: HashMap<usize, Vec<&'a SliverPair>> = HashMap::new();
            for pair in *pairs {
                let shard_index = pair.index().to_shard_index(committees.n_shards(), &blob_id);
                for (node_index, node) in committees.write_committee().members().iter().enumerate() {
                    if node.shard_ids.contains(&shard_index) {
                        pairs_per_node.entry(node_index).or_default().push(pair);
                    }
                }
            }

            for (node_index, pairs_for_node) in pairs_per_node {
                work_items
                    .entry(node_index)
                    .or_default()
                    .push(UploadWorkItem {
                        metadata,
                        pairs: pairs_for_node,
                    });
            }
        }

        Self {
            work_items,
            committees,
            progress,
            comms,
            sliver_write_limit,
            sliver_write_extra_time,
        }
    }

    /// Runs the distributed upload process.
    pub async fn run<F, Fut, R, T, E>(
        &mut self,
        upload_action: F,
        event_sender: tokio::sync::mpsc::Sender<UploaderEvent>,
    ) -> Result<Vec<NodeResult<R, E>>, ClientError>
    where
        F: Fn(NodeWriteCommunication<'a>, Vec<UploadWorkItem<'a>>) -> Fut,
        Fut: Future<Output = NodeResult<R, E>> + Send,
        R: AsRef<[T]> + Send + Sync + 'static,
        T: HasBlobId,
        E: Send + Sync + 'static,
    {
        let semaphore = Arc::new(Semaphore::new(self.sliver_write_limit));
        let futures = self.comms.drain(..).map(|n| {
            let work = self.work_items.remove(&n.node_index).unwrap_or_default();
            let fut = upload_action(n, work);
            let sem_clone = semaphore.clone();
            async move {
                let _permit = sem_clone
                    .acquire()
                    .await
                    .expect("semaphore acquire should not fail");
                fut.await
            }
        });

        let mut requests = WeightedFutures::new(futures);
        let start = std::time::Instant::now();

        let mut blobs_at_quorum = 0;

        while let Some(node_result) = requests.next(self.committees.n_shards().get().into()).await {
            if let Ok(successful_blobs) = &node_result.result {
                for successful_blob in successful_blobs.as_ref() {
                    let blob_id = successful_blob.blob_id();
                    let prog = self.progress.entry(*blob_id).or_default();
                    prog.completed_weight += node_result.weight;

                    if !prog.quorum_reached
                        && self
                            .committees
                            .write_committee()
                            .is_at_least_min_n_correct(prog.completed_weight)
                    {
                        prog.quorum_reached = true;
                        blobs_at_quorum += 1;
                        let _ = event_sender
                            .send(UploaderEvent::BlobQuorumReached {
                                blob_id: *blob_id,
                                elapsed: start.elapsed(),
                            })
                            .await;
                    }
                }
            }

            if blobs_at_quorum == self.progress.len() {
                break;
            }
        }

        let extra_time = self.sliver_write_extra_time.extra_time(start.elapsed());
        if extra_time > Duration::from_millis(0) {
            let _ = requests
                .execute_time(extra_time, self.committees.n_shards().get().into())
                .await;
        }

        Ok(requests.into_results())
    }
}

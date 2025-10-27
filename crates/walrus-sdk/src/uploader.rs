// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! A component for orchestrating the distributed upload of multiple blobs to the Walrus network.
//!
//! The `DistributedUploader` is designed to handle the complexity of a multi-blob, multi-stage
//! upload process in an efficient and robust manner. It is the single source of truth for the
//! core upload logic, used by all parts of the client.

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use futures::Future;
use tokio::task::JoinHandle;
use walrus_core::{BlobId, encoding::SliverPair, metadata::VerifiedBlobMetadataWithId};

use crate::{
    active_committees::ActiveCommittees,
    client::communication::{NodeResult, NodeWriteCommunication},
    config::SliverWriteExtraTime,
    error::ClientError,
    utils::WeightedFutures,
};

/// Controls how the extra tail window is handled once quorum is reached.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TailHandling {
    /// The uploader will block until the tail upload is complete.
    Blocking,
    /// The uploader will spawn a detached tail upload. Callers should either provide a collector to
    /// retain the join handles or expect the uploader to await them on a background task.
    Detached,
}

/// Outcome returned by the uploader run.
#[derive(Debug)]
pub struct RunOutput<R, E> {
    /// The results of the upload.
    pub results: Vec<NodeResult<R, E>>,
    /// The handle to the detached tail upload.
    pub tail_handle: Option<JoinHandle<()>>,
}

/// A work item for the uploader, representing a set of sliver pairs for a single blob
/// that need to be sent to a specific node.
#[derive(Debug, Clone)]
pub struct UploadWorkItem {
    /// The blob metadata.
    pub metadata: VerifiedBlobMetadataWithId,
    /// Shared storage for all sliver pairs of the blob.
    pub pairs: Arc<Vec<SliverPair>>,
    /// Indices of the sliver pairs destined for this node.
    pub pair_indices: Vec<usize>,
}

impl UploadWorkItem {
    /// The blob ID.
    pub fn blob_id(&self) -> &BlobId {
        self.metadata.blob_id()
    }
}

/// Tracks the upload progress for a single blob.
#[derive(Debug, Clone, Default)]
struct BlobUploadProgress {
    /// The total weight of the nodes that have successfully stored the slivers for this blob.
    pub completed_weight: usize,
    /// A flag indicating whether the quorum has been reached for this blob.
    pub quorum_reached: bool,
}

/// Events emitted by the `DistributedUploader` to report progress.
#[derive(Debug, Clone)]
pub enum UploaderEvent {
    /// Progress update for a blob.
    BlobProgress {
        /// The blob ID.
        blob_id: BlobId,
        /// The completed weight of the nodes that have successfully
        /// stored the slivers for this blob.
        completed_weight: usize,
        /// The required weight of the nodes that need to successfully
        /// store the slivers for this blob.
        required_weight: usize,
    },
    /// A blob has reached the required quorum of storage nodes.
    BlobQuorumReached {
        /// The blob ID.
        blob_id: BlobId,
        /// The elapsed time since the upload started.
        elapsed: Duration,
    },
}

/// The `DistributedUploader` component.
#[derive(Debug)]
pub struct DistributedUploader {
    /// Work scheduled per storage node, keyed by node index,
    /// in the order returned by the write committee.
    work_items: HashMap<usize, Vec<UploadWorkItem>>,
    /// The committees object.
    committees: Arc<ActiveCommittees>,
    /// A map to track the upload progress for each blob.
    progress: HashMap<BlobId, BlobUploadProgress>,
    /// Node write communications aligned with the committee used to derive `work_items`.
    comms: Vec<NodeWriteCommunication>,
    /// The extra time to wait for tail-end writes.
    sliver_write_extra_time: SliverWriteExtraTime,
}

impl DistributedUploader {
    /// Creates a new `DistributedUploader`. `comms` must be aligned with the write
    /// committee used to derive `work_items`.
    pub fn new(
        blobs: &[(VerifiedBlobMetadataWithId, Arc<Vec<SliverPair>>)],
        committees: Arc<ActiveCommittees>,
        comms: Vec<NodeWriteCommunication>,
        sliver_write_extra_time: SliverWriteExtraTime,
    ) -> Self {
        let mut work_items: HashMap<usize, Vec<UploadWorkItem>> = HashMap::new();
        let mut progress: HashMap<BlobId, BlobUploadProgress> = HashMap::new();

        for (metadata, pairs) in blobs {
            let blob_id = *metadata.blob_id();
            progress.entry(blob_id).or_default();

            let mut pairs_per_node: HashMap<usize, Vec<usize>> = HashMap::new();
            for (idx, pair) in pairs.iter().enumerate() {
                let shard_index = pair.index().to_shard_index(committees.n_shards(), &blob_id);
                for (node_index, node) in committees.write_committee().members().iter().enumerate()
                {
                    if node.shard_ids.contains(&shard_index) {
                        pairs_per_node.entry(node_index).or_default().push(idx);
                    }
                }
            }

            for (node_index, pairs_for_node) in pairs_per_node {
                work_items
                    .entry(node_index)
                    .or_default()
                    .push(UploadWorkItem {
                        metadata: metadata.clone(),
                        pairs: pairs.clone(),
                        pair_indices: pairs_for_node,
                    });
            }
        }

        Self {
            work_items,
            committees,
            progress,
            comms,
            sliver_write_extra_time,
        }
    }

    /// Runs the distributed upload process.
    pub async fn run_distributed_upload<F, Fut, R, E>(
        &mut self,
        upload_action: F,
        event_sender: tokio::sync::mpsc::Sender<UploaderEvent>,
        tail_handling: TailHandling,
    ) -> Result<RunOutput<R, E>, ClientError>
    where
        F: Fn(NodeWriteCommunication, Vec<UploadWorkItem>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = NodeResult<R, E>> + Send + 'static,
        R: AsRef<[BlobId]> + Send + Sync + 'static,
        E: Send + Sync + 'static,
    {
        let scheduled: Vec<_> = self
            .comms
            .drain(..)
            .map(|n| {
                let work = self.work_items.remove(&n.node_index).unwrap_or_default();
                (n, work)
            })
            .collect();

        let upload_action = Arc::new(upload_action);
        let futures = scheduled.into_iter().map({
            let upload_action = upload_action.clone();
            move |(n, work)| {
                let upload_action = upload_action.clone();
                async move { (*upload_action)(n, work).await }
            }
        });

        let mut requests = WeightedFutures::new(futures);
        let start = Instant::now();

        let mut blobs_at_quorum = 0;
        let mut results: Vec<NodeResult<R, E>> = Vec::new();

        while let Some(node_result) = requests.next(self.committees.n_shards().get().into()).await {
            if let Ok(successful_blobs) = &node_result.result {
                for &blob_id in successful_blobs.as_ref() {
                    let prog = self.progress.entry(blob_id).or_default();
                    prog.completed_weight += node_result.weight;

                    let required_weight = self.committees.min_n_correct();
                    if let Err(err) = event_sender
                        .send(UploaderEvent::BlobProgress {
                            blob_id,
                            completed_weight: prog.completed_weight,
                            required_weight,
                        })
                        .await
                    {
                        tracing::warn!(blob_id = %blob_id, ?err,
                            "failed to send blob progress event");
                    }

                    if !prog.quorum_reached
                        && self
                            .committees
                            .write_committee()
                            .is_at_least_min_n_correct(prog.completed_weight)
                    {
                        prog.quorum_reached = true;
                        blobs_at_quorum += 1;
                        tracing::debug!(blob_id = %blob_id, "sending blob quorum reached event");
                        if let Err(err) = event_sender
                            .send(UploaderEvent::BlobQuorumReached {
                                blob_id,
                                elapsed: start.elapsed(),
                            })
                            .await
                        {
                            tracing::warn!(blob_id = %blob_id, ?err,
                                "failed to send blob quorum reached event");
                        } else {
                            tracing::debug!(blob_id = %blob_id, "sent blob quorum reached event");
                        }
                    }
                }
            }

            results.push(node_result);
            if blobs_at_quorum == self.progress.len() {
                break;
            }
        }

        let extra_time = self.sliver_write_extra_time.extra_time(start.elapsed());
        let n_shards: usize = self.committees.n_shards().get().into();

        let tail_handle =
            if tail_handling == TailHandling::Detached && extra_time > Duration::from_millis(0) {
                tracing::debug!("uploader: spawning detached tail handle");
                Some(tokio::spawn(async move {
                    let mut requests = requests;
                    let reason = requests.execute_time(extra_time, n_shards).await;
                    tracing::debug!(
                        ?reason,
                        "uploader: detached tail handle completed with reason",
                    );
                    let results = requests.into_results();
                    tracing::debug!(
                        "uploader: detached tail handle results: {:?}",
                        results.len()
                    );
                }))
            } else {
                if !extra_time.is_zero() {
                    let reason = requests.execute_time(extra_time, n_shards).await;
                    tracing::debug!(
                        "uploader: detached tail handle completed with reason: {:?}",
                        reason
                    );
                }

                results.extend(requests.take_results());
                None
            };

        Ok(RunOutput {
            results,
            tail_handle,
        })
    }
}

// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Strata implementation of primary and secondary sliver storage.

use std::{path::Path, sync::Arc, time::Duration};

use strata::{
    BlobKey,
    ShardState,
    StrataLsn,
    StrataStore,
    StrataStoreConfig,
    StrataStoreMetrics,
    queue::{LockedBlobs, QueueWorker, WorkerConfig},
};
use strata_index::{StrataIndex, port::IndexDb};
use typed_store::{TypedStoreError, rocks::DBBatch};
use walrus_core::{BlobId, Epoch, ShardIndex, Sliver, SliverType};
use walrus_utils::metrics::Registry;

use super::{
    super::{
        blob_info::{BlobInfoApi, BlobInfoTable},
        strata_queue::{HaltOnIncompleteWrite, StrataQueue, StrataWorker, queue_error},
    },
    PrimarySliverData,
    SecondarySliverData,
};
use crate::utils;

// Time in seconds to wait for data to be fsynced when writing slivers into Strata.
const SYNC_WAIT_TIMEOUT: Duration = Duration::from_secs(30);

/// Strata store for slivers. Walrus control tables remain in RocksDB.
#[derive(Debug, Clone)]
pub(super) struct StrataSliverStore {
    store: Arc<StrataStore>,
    queue: StrataQueue,
    blob_info: Arc<BlobInfoTable>,
}

/// One Walrus shard within the Strata store, holding both primary and secondary slivers.
#[derive(Debug, Clone)]
pub(super) struct StrataShardSliverStore {
    node: StrataSliverStore,
    shard: ShardIndex,
    generation: u64,
}

impl StrataSliverStore {
    pub(super) fn open(
        path: &Path,
        metrics_registry: &Registry,
        database: Arc<dyn IndexDb>,
        queue: StrataQueue,
        blob_info: BlobInfoTable,
    ) -> anyhow::Result<Self> {
        let config = StrataStoreConfig::new(path, "slivers");
        let metrics = StrataStoreMetrics::new(
            metrics_registry.prometheus_registry(),
            format!("slivers:{}", path.display()),
        )?;
        Ok(Self {
            store: Arc::new(StrataStore::from_index(
                config.clone(),
                StrataIndex::from_db(database, config.index_cf_prefix())?,
                metrics,
            )?),
            queue,
            blob_info: Arc::new(blob_info),
        })
    }

    pub(super) fn worker(&self) -> StrataWorker {
        let worker = QueueWorker::new(
            self.queue.clone(),
            self.store.clone(),
            physical_keys,
            WorkerConfig::default(),
        )
        .expect("the queue and Strata share one database handle");
        StrataWorker::new(self.queue.clone(), self.blob_info.as_ref().clone(), worker)
    }

    /// Finish prerequisites outside the blob guard: the worker needs that same guard to apply
    /// them. After reacquiring, check again because registration can enqueue another lifetime.
    async fn lock_ready_blobs(&self, blob_ids: &[BlobId]) -> Result<LockedBlobs, TypedStoreError> {
        let keys: Vec<&[u8]> = blob_ids.iter().map(|id| id.0.as_slice()).collect();
        loop {
            let guard = self.queue.lock_blobs(&keys).await.map_err(queue_error)?;
            let mut pending = false;
            for key in &keys {
                pending |= !self
                    .queue
                    .pending_blob(&guard, key)
                    .map_err(queue_error)?
                    .commands()
                    .is_empty();
            }
            if !pending {
                return Ok(guard);
            }
            drop(guard);
            self.worker().process_batch().await?;
        }
    }

    pub(super) fn open_shard(
        &self,
        shard: ShardIndex,
    ) -> Result<StrataShardSliverStore, TypedStoreError> {
        let _guard =
            futures::executor::block_on(self.queue.lock_lifecycle()).map_err(queue_error)?;
        let mut completion = HaltOnIncompleteWrite::new(self.queue.clone());
        let generation = self
            .store
            .add_shard(u32::from(shard.0))
            .map_err(store_error)?
            .generation;
        completion.complete();
        Ok(StrataShardSliverStore {
            node: self.clone(),
            shard,
            generation,
        })
    }

    pub(super) fn shard_state(
        &self,
        shard: ShardIndex,
    ) -> Result<Option<ShardState>, TypedStoreError> {
        self.store
            .shard_info(u32::from(shard.0))
            .map(|info| info.map(|info| info.state))
            .map_err(store_error)
    }

    pub(super) fn contains_sliver_pairs_in_all(
        &self,
        blob_id: BlobId,
        shards: &[ShardIndex],
    ) -> Result<bool, TypedStoreError> {
        let shards = shards
            .iter()
            .map(|shard| u32::from(shard.0))
            .collect::<Vec<_>>();
        for sliver_type in [SliverType::Primary, SliverType::Secondary] {
            let key = sliver_key(&blob_id, sliver_type);
            if !self
                .store
                .contains_in_shards(&key, &shards)
                .map_err(store_error)?
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Waits until the Strata checkpoint makes `lsn` crash safe, not merely visible to reads.
    pub(super) async fn wait_durable_lsn(&self, lsn: StrataLsn) -> Result<(), TypedStoreError> {
        if lsn == 0 {
            return Ok(());
        }
        let mut receiver = self.store.subscribe_durability_progress();
        tokio::time::timeout(SYNC_WAIT_TIMEOUT, async move {
            loop {
                let progress = receiver.borrow_and_update().clone();
                if progress.published_lsn >= lsn {
                    return Ok(());
                }
                if let Some(reason) = progress.halt_reason {
                    return Err(TypedStoreError::TaskError(format!(
                        "Strata halted before sliver LSN {lsn} became durable: {reason}"
                    )));
                }
                receiver.changed().await.map_err(|_| {
                    TypedStoreError::TaskError("Strata durability notifier stopped".to_owned())
                })?;
            }
        })
        .await
        .map_err(|_| {
            TypedStoreError::TaskError(format!(
                "timed out waiting for Strata sliver LSN {lsn} to become durable"
            ))
        })?
    }
}

impl StrataShardSliverStore {
    /// Called while holding a blob or lifecycle guard, so retirement cannot race this check.
    fn check_generation(&self) -> Result<(), TypedStoreError> {
        let info = self
            .node
            .store
            .shard_info(u32::from(self.shard.0))
            .map_err(store_error)?;
        if !info.is_some_and(|info| info.is_active() && info.current_generation == self.generation)
        {
            return Err(TypedStoreError::TaskError(
                "Strata shard generation was retired".into(),
            ));
        }
        Ok(())
    }

    pub(super) fn drop_shard(&self) -> Result<(), TypedStoreError> {
        let _guard =
            futures::executor::block_on(self.node.queue.lock_lifecycle()).map_err(queue_error)?;
        self.check_generation()?;
        let mut completion = HaltOnIncompleteWrite::new(self.node.queue.clone());
        self.node
            .store
            .drop_shard(u32::from(self.shard.0))
            .map_err(store_error)?;
        // Strata drop returns when the generation fence is visible, not yet crash safe. It
        // does not return an LSN to wait on, so sync before removing RocksDB control tables.
        self.node.store.sync().map_err(store_error)?;
        completion.complete();
        Ok(())
    }

    pub(super) async fn put(
        &self,
        blob_id: BlobId,
        sliver: Sliver,
        epoch: Option<Epoch>,
    ) -> Result<bool, TypedStoreError> {
        self.write(vec![(blob_id, sliver)], epoch, None)
            .await
            .map(|count| count != 0)
    }

    pub(super) async fn put_many(
        &self,
        slivers: Vec<(BlobId, Sliver)>,
        epoch: Epoch,
        control_batch: DBBatch,
    ) -> Result<(), TypedStoreError> {
        self.write(slivers, Some(epoch), Some(control_batch))
            .await
            .map(|_| ())
    }

    async fn write(
        &self,
        slivers: Vec<(BlobId, Sliver)>,
        epoch: Option<Epoch>,
        control_batch: Option<DBBatch>,
    ) -> Result<usize, TypedStoreError> {
        let this = self.clone();
        // Detaching the awaiting request must not release guards around an in-flight write.
        utils::unwrap_or_resume_unwind(
            tokio::spawn(async move {
                let ids: Vec<_> = slivers.iter().map(|(id, _)| *id).collect();
                let _guard = this.node.lock_ready_blobs(&ids).await?;
                let prepare = this.clone();
                let payloads = utils::unwrap_or_resume_unwind(
                    tokio::task::spawn_blocking(move || {
                        prepare.check_generation()?;
                        let mut payloads = Vec::new();
                        for (blob_id, sliver) in slivers {
                            if let Some(epoch) = epoch {
                                // Persisted metadata is not evidence of a current reference.
                                // Re-read under the registration/delete lock at the final write.
                                if !prepare
                                    .node
                                    .blob_info
                                    .get(&blob_id)?
                                    .is_some_and(|info| info.is_registered(epoch))
                                {
                                    continue;
                                }
                                let registration = prepare
                                    .node
                                    .blob_info
                                    .strata_registration(&blob_id)?
                                    .ok_or_else(|| {
                                        TypedStoreError::TaskError(
                                            "registered Strata blob has no registration identity"
                                                .into(),
                                        )
                                    })?;
                                tracing::trace!(
                                    %blob_id,
                                    registration_event_index = registration.event_index,
                                    "admitting Strata put after registration lifecycle work"
                                );
                            }
                            payloads.push((
                                sliver_key(&blob_id, sliver.r#type()),
                                serialize_sliver(&sliver)?,
                            ));
                        }
                        Ok::<_, TypedStoreError>(payloads)
                    })
                    .await,
                )?;
                let count = payloads.len();
                let mut completion = HaltOnIncompleteWrite::new(this.node.queue.clone());
                let writer = this.clone();
                let lsn = utils::unwrap_or_resume_unwind(
                    tokio::task::spawn_blocking(move || {
                        if payloads.is_empty() {
                            return Ok(0);
                        }
                        let mut batch = writer.node.store.batch();
                        for (key, payload) in payloads {
                            batch.put(u32::from(writer.shard.0), key, Arc::<[u8]>::from(payload));
                        }
                        let result = batch.write().map_err(store_error)?;
                        Ok::<_, TypedStoreError>(result.last_lsn().unwrap_or(0))
                    })
                    .await,
                )?;
                this.node.wait_durable_lsn(lsn).await?;
                if let Some(batch) = control_batch {
                    utils::unwrap_or_resume_unwind(
                        tokio::task::spawn_blocking(move || batch.write()).await,
                    )?;
                }
                completion.complete();
                Ok(count)
            })
            .await,
        )
    }

    pub(super) fn get(
        &self,
        blob_id: &BlobId,
        sliver_type: SliverType,
    ) -> Result<Option<Sliver>, TypedStoreError> {
        let key = sliver_key(blob_id, sliver_type);
        self.node
            .store
            .get_from_shard(u32::from(self.shard.0), &key)
            .map_err(store_error)?
            .map(|payload| deserialize_sliver(sliver_type, &payload))
            .transpose()
    }

    pub(super) fn contains(
        &self,
        blob_id: &BlobId,
        sliver_type: SliverType,
    ) -> Result<bool, TypedStoreError> {
        let key = sliver_key(blob_id, sliver_type);
        self.node
            .store
            .contains_in_shards(&key, &[u32::from(self.shard.0)])
            .map_err(store_error)
    }

    pub(super) async fn delete_pair(&self, blob_id: BlobId) -> Result<(), TypedStoreError> {
        let this = self.clone();
        utils::unwrap_or_resume_unwind(
            tokio::spawn(async move {
                let _guard = this
                    .node
                    .queue
                    .lock_blobs(&[&blob_id.0])
                    .await
                    .map_err(queue_error)?;
                this.check_generation()?;
                let mut completion = HaltOnIncompleteWrite::new(this.node.queue.clone());
                let writer = this.clone();
                let lsn = utils::unwrap_or_resume_unwind(
                    tokio::task::spawn_blocking(move || {
                        let mut batch = writer.node.store.batch();
                        for sliver_type in [SliverType::Primary, SliverType::Secondary] {
                            batch.tombstone(
                                u32::from(writer.shard.0),
                                sliver_key(&blob_id, sliver_type),
                            );
                        }
                        let result = batch.write().map_err(store_error)?;
                        Ok::<_, TypedStoreError>(result.op_lsns().last().copied().unwrap_or(0))
                    })
                    .await,
                )?;
                this.node.wait_durable_lsn(lsn).await?;
                completion.complete();
                Ok(())
            })
            .await,
        )
    }
}

fn physical_keys(id: &[u8]) -> strata::queue::Result<Vec<BlobKey>> {
    let id: [u8; BlobId::LENGTH] = id.try_into().map_err(|_| {
        strata::queue::Error::InvalidPendingOperation(
            "invalid Walrus blob id in lifecycle queue".into(),
        )
    })?;
    Ok([SliverType::Primary, SliverType::Secondary]
        .into_iter()
        .map(|axis| sliver_key(&BlobId(id), axis))
        .collect())
}

fn sliver_key(blob_id: &BlobId, sliver_type: SliverType) -> BlobKey {
    let mut bytes = Vec::with_capacity(1 + BlobId::LENGTH);
    bytes.push(if sliver_type.is_primary() { 0 } else { 1 });
    bytes.extend_from_slice(&blob_id.0);
    BlobKey::new(bytes).expect("the fixed-size sliver key is valid")
}

fn serialize_sliver(sliver: &Sliver) -> Result<Vec<u8>, TypedStoreError> {
    match sliver {
        Sliver::Primary(primary) => bcs::to_bytes(&PrimarySliverData::from(primary.clone())),
        Sliver::Secondary(secondary) => {
            bcs::to_bytes(&SecondarySliverData::from(secondary.clone()))
        }
    }
    .map_err(|error| TypedStoreError::SerializationError(error.to_string()))
}

fn deserialize_sliver(sliver_type: SliverType, payload: &[u8]) -> Result<Sliver, TypedStoreError> {
    match sliver_type {
        SliverType::Primary => {
            bcs::from_bytes::<PrimarySliverData>(payload).map(|data| Sliver::Primary(data.into()))
        }
        SliverType::Secondary => bcs::from_bytes::<SecondarySliverData>(payload)
            .map(|data| Sliver::Secondary(data.into())),
    }
    .map_err(|error| TypedStoreError::SerializationError(error.to_string()))
}

fn store_error(error: strata::Error) -> TypedStoreError {
    TypedStoreError::TaskError(format!("Strata sliver store: {error}"))
}

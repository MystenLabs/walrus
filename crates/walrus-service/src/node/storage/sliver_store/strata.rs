// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Strata implementation of primary and secondary sliver storage.

use std::{path::Path, sync::Arc, time::Duration};

use strata::{BlobKey, ShardState, StrataLsn, StrataStore, StrataStoreConfig, StrataStoreMetrics};
use strata_index::{StrataIndex, port::IndexDb};
use typed_store::{Map, TypedStoreError, rocks::DBBatch};
use walrus_core::{BlobId, Epoch, ShardIndex, Sliver, SliverType};
use walrus_utils::metrics::Registry;

use super::{
    super::{
        blob_info::{BlobInfoApi, BlobInfoTable},
        strata_lifecycle::{
            DIRTY_BLOBS_CF,
            EpochProgress,
            HaltOnIncompleteWrite,
            LIFETIMES_CF,
            ReconcileBlob,
            StrataLifecycle,
            error,
        },
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
    pub(super) store: Arc<StrataStore>,
    lifecycle: Arc<StrataLifecycle>,
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
        lifecycle: Arc<StrataLifecycle>,
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
            lifecycle,
            blob_info: Arc::new(blob_info),
        })
    }

    /// The worker serializes epoch passes, but events and puts keep running. Only each batch's
    /// blob locks are held through submission, sync and acknowledgement. Shard/clock changes
    /// use the lifecycle lock; the reference-table scan never takes it exclusively.
    pub(super) async fn reconcile_epoch(
        &self,
        epoch: Epoch,
        delete_data: bool,
    ) -> anyhow::Result<()> {
        let this = self.clone();
        // Cancellation of the worker must not abandon a submitted write and its locks.
        let task = tokio::spawn(async move {
            this.lifecycle.check_running()?;
            // The sole worker awaits every pass. Refuse to replace unfinished work and skip
            // a pass that already completed; the checkpoint tells us which phase to resume.
            let progress = this.lifecycle.progress.get(&())?;
            match progress {
                Some(EpochProgress::Applying(pending) | EpochProgress::Advancing(pending)) => {
                    // The worker finishes interrupted work before starting a newer request.
                    anyhow::ensure!(
                        pending == epoch,
                        "cannot reconcile epoch {epoch} while epoch {pending} is unfinished"
                    );
                }
                Some(EpochProgress::Complete(done)) if done >= epoch => return Ok(()),
                _ => {}
            }
            let mut completion = HaltOnIncompleteWrite::new(this.lifecycle.clone());
            // Advancing means this pass's blob updates are already durable. After a crash,
            // resume the clock transition and cleanup below without repeating the scan.
            if progress != Some(EpochProgress::Advancing(epoch)) {
                let reader = this.clone();
                let scan = tokio::task::spawn_blocking(move || {
                    let mut batch = reader.lifecycle.progress.batch();
                    batch.insert_batch(
                        &reader.lifecycle.progress,
                        [((), EpochProgress::Applying(epoch))],
                    )?;
                    batch.write_with_sync(true)?;
                    reader.blob_info.reconciliation_candidates()
                });
                let snapshot = utils::unwrap_or_resume_unwind(scan.await)?;
                for group in snapshot.blobs.chunks(256) {
                    this.reconcile_blobs(epoch, group, delete_data).await?;
                }
                // Extension event 100 marked P dirty. Clear it only if no event 101 has
                // replaced that marker. Hold P's lock from the check through the synced delete;
                // otherwise event 101 could arrive between them and lose its pending work.
                for group in snapshot.pools.chunks(256) {
                    let pools: Vec<_> = group.iter().map(|(pool, _)| *pool).collect();
                    let _guard = this.lifecycle.lock_pools(&pools).await?;
                    let mut cleanup = HaltOnIncompleteWrite::new(this.lifecycle.clone());
                    let writer = this.clone();
                    let group = group.to_vec();
                    utils::unwrap_or_resume_unwind(
                        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
                            let mut batch = writer.lifecycle.pools.batch();
                            for (pool, event_index) in group {
                                if writer.lifecycle.pools.get(&pool)? == Some(event_index) {
                                    batch.delete_batch(&writer.lifecycle.pools, [pool])?;
                                }
                            }
                            batch.write_with_sync(true)?;
                            Ok(())
                        })
                        .await,
                    )?;
                    cleanup.complete();
                }
                let writer = this.clone();
                let finish = tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
                    let mut batch = writer.lifecycle.progress.batch();
                    batch.insert_batch(
                        &writer.lifecycle.progress,
                        [((), EpochProgress::Advancing(epoch))],
                    )?;
                    batch.write_with_sync(true)?;
                    Ok(())
                });
                utils::unwrap_or_resume_unwind(finish.await)?;
            }
            // This short clock transition, unlike the scan, excludes in-flight puts so a
            // put's lifetime check and submission cannot straddle the epoch advance.
            let _guard = this.lifecycle.lock_lifecycle().await?;
            let mut epoch_completion = HaltOnIncompleteWrite::new(this.lifecycle.clone());
            let writer = this.clone();
            let advance = tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
                if delete_data {
                    let mut batch = writer.store.batch();
                    batch.advance_epoch_to(u64::from(epoch));
                    batch.write()?;
                }
                writer.store.sync()?;
                let prefix = [b"walrus-epoch/".as_slice(), &epoch.to_be_bytes()].concat();
                let bindings = writer.store.index().submitted_batch_lsns();
                let mut ack = writer.lifecycle.db.write_batch();
                for row in bindings.safe_iter()? {
                    let (key, _) = row?;
                    if key.starts_with(&prefix) {
                        ack.delete(
                            bindings.cf_name(),
                            &strata_index::port::codec::encode_key(&key)?,
                        )?;
                    }
                }
                ack.put(
                    super::super::strata_lifecycle::EPOCH_CF,
                    &typed_store::rocks::be_fix_int_ser(&())?,
                    &bcs::to_bytes(&EpochProgress::Complete(epoch))?,
                )?;
                ack.write(true)?;
                Ok(())
            });
            utils::unwrap_or_resume_unwind(advance.await)?;
            epoch_completion.complete();
            completion.complete();
            Ok::<_, anyhow::Error>(())
        });
        utils::unwrap_or_resume_unwind(task.await)
    }

    /// Revalidate a snapshot batch under live blob locks. Keeping this separate also makes
    /// the snapshot/put race testable without timing assumptions.
    async fn reconcile_blobs(
        &self,
        epoch: Epoch,
        group: &[ReconcileBlob],
        delete_data: bool,
    ) -> anyhow::Result<()> {
        let keys: Vec<_> = group.iter().map(|blob| blob.id.0.as_slice()).collect();
        let _guard = self.lifecycle.lock_blobs(&keys).await?;
        let mut group_completion = HaltOnIncompleteWrite::new(self.lifecycle.clone());
        let writer = self.clone();
        let group = group.to_vec();
        utils::unwrap_or_resume_unwind(
            tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
                // Read live metadata while holding the same locks as registrations
                // and puts. The earlier snapshot supplies candidates, not permission
                // to delete. Sync these live inputs before any destructive write.
                let mut work = Vec::new();
                let mut inputs = writer.lifecycle.lifetimes.batch();
                for ReconcileBlob {
                    id,
                    end_epoch: snapshot_end,
                    dirty: expected,
                } in group
                {
                    let live = writer.lifecycle.blobs.get(&id)?;
                    let unchanged = live.map(|row| row.event_index)
                        == expected.map(|row| row.event_index)
                        && (delete_data || !live.is_some_and(|row| row.delete));
                    let registered = writer
                        .blob_info
                        .get(&id)?
                        .is_some_and(|info| info.is_registered(epoch));
                    let end = if unchanged {
                        snapshot_end
                    } else {
                        snapshot_end.max(writer.lifecycle.lifetimes.get(&id)?.unwrap_or_default())
                    }
                    .max(writer.blob_info.strata_pool_end_epoch(id)?);
                    let delete = delete_data && !registered && live.is_some_and(|row| row.delete);
                    if registered && end > epoch {
                        // Persist the lifetime hint before Strata. Otherwise a put after a crash
                        // could restore an old cached lifetime over a surviving extension, while
                        // the tracked LSN incorrectly tells recovery that extension is complete.
                        inputs.insert_batch(&writer.lifecycle.lifetimes, [(id, end)])?;
                    }
                    work.push((
                        id,
                        end,
                        unchanged,
                        delete,
                        registered,
                        live.map(|row| row.event_index),
                    ));
                }
                inputs.write_with_sync(true)?;
                let mut ack = writer.lifecycle.db.write_batch();
                let prefix = [b"walrus-epoch/".as_slice(), &epoch.to_be_bytes()].concat();
                let shards = writer
                    .store
                    .index()
                    .shards()
                    .safe_iter()?
                    .collect::<Result<Vec<_>, _>>()?;
                let mut submitted = false;
                for (id, end, unchanged, delete, registered, event_index) in &work {
                    let mut batch = writer.store.batch();
                    for axis in [SliverType::Primary, SliverType::Secondary] {
                        let key = sliver_key(id, axis);
                        if *delete {
                            for (shard, info) in &shards {
                                if info.is_active() {
                                    batch.tombstone(*shard, key.clone());
                                }
                            }
                        } else if *registered && u64::from(*end) > writer.store.current_epoch()? {
                            batch.set_blob_lifetime(key, u64::from(*end));
                        }
                    }
                    // Identity describes the actual operation, not its scan position.
                    // Keep bindings until the epoch is complete: a pool scan may be
                    // repeated after a crash even after individual dirty IDs were acked.
                    // A later registration/delete can occur while this pass is running. It
                    // must not reuse an earlier delete's binding after writing a new payload.
                    let lsn_key = [
                        prefix.as_slice(),
                        &bcs::to_bytes(&(id, end, delete, event_index))?,
                    ]
                    .concat();
                    if (*delete && shards.iter().any(|(_, info)| info.is_active()))
                        || (*registered && u64::from(*end) > writer.store.current_epoch()?)
                    {
                        if writer
                            .store
                            .index()
                            .submitted_batch_lsns()
                            .get(&lsn_key)?
                            .is_none()
                        {
                            batch.write_with_lsn(lsn_key)?;
                        }
                        submitted = true;
                    }
                    if *unchanged {
                        ack.delete(DIRTY_BLOBS_CF, &typed_store::rocks::be_fix_int_ser(id)?)?;
                    }
                    if *delete {
                        ack.delete(LIFETIMES_CF, &typed_store::rocks::be_fix_int_ser(id)?)?;
                    }
                }
                if submitted {
                    writer.store.sync()?;
                }
                // No marker may reappear after a later put is admitted on these keys.
                ack.write(true)?;
                Ok(())
            })
            .await,
        )?;
        group_completion.complete();
        Ok(())
    }

    pub(super) fn open_shard(
        &self,
        shard: ShardIndex,
    ) -> Result<StrataShardSliverStore, TypedStoreError> {
        let _guard = futures::executor::block_on(self.lifecycle.lock_lifecycle())?;
        let mut completion = HaltOnIncompleteWrite::new(self.lifecycle.clone());
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
        let _guard = futures::executor::block_on(self.node.lifecycle.lock_lifecycle())?;
        self.check_generation()?;
        let mut completion = HaltOnIncompleteWrite::new(self.node.lifecycle.clone());
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
                let _guard = this
                    .node
                    .lifecycle
                    .lock_blobs(&ids.iter().map(|id| id.0.as_slice()).collect::<Vec<_>>())
                    .await?;
                let mut completion = HaltOnIncompleteWrite::new(this.node.lifecycle.clone());
                let prepare = this.clone();
                let payloads = utils::unwrap_or_resume_unwind(
                    tokio::task::spawn_blocking(move || {
                        prepare.check_generation()?;
                        let mut payloads = Vec::new();
                        let mut admission = prepare.node.lifecycle.blobs.batch();
                        let mut changed = false;
                        for (blob_id, sliver) in slivers {
                            let mut lifetime = None;
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
                                // Reconciliation refreshes pool lifetimes before advancing
                                // Strata's epoch. Until then, the cached expiry remains valid.
                                let end_epoch = prepare
                                    .node
                                    .lifecycle
                                    .lifetimes
                                    .get(&blob_id)?
                                    .ok_or_else(|| error("registered blob has no lifetime"))?;
                                lifetime = Some(end_epoch);
                                if u64::from(end_epoch)
                                    <= prepare.node.store.current_epoch().map_err(store_error)?
                                {
                                    continue;
                                }
                                // A put cancels deletion, not lifetime reconciliation. Persist
                                // cancellation before releasing the lock so recovery cannot
                                // resurrect an old delete after this new payload.
                                if let Some(mut dirty) =
                                    prepare.node.lifecycle.blobs.get(&blob_id)?
                                    && dirty.delete
                                {
                                    dirty.delete = false;
                                    admission.insert_batch(
                                        &prepare.node.lifecycle.blobs,
                                        [(blob_id, dirty)],
                                    )?;
                                    changed = true;
                                }
                            }
                            payloads.push((
                                sliver_key(&blob_id, sliver.r#type()),
                                serialize_sliver(&sliver)?,
                                lifetime,
                            ));
                        }
                        if changed {
                            // Keep deletion cancellation durable before the put reaches Strata.
                            admission.write_with_sync(true)?;
                        }
                        Ok::<_, TypedStoreError>(payloads)
                    })
                    .await,
                )?;
                let count = payloads.len();
                let writer = this.clone();
                let lsn = utils::unwrap_or_resume_unwind(
                    tokio::task::spawn_blocking(move || {
                        if payloads.is_empty() {
                            return Ok(0);
                        }
                        let mut batch = writer.node.store.batch();
                        for (key, payload, end_epoch) in payloads {
                            if let Some(end_epoch) = end_epoch {
                                // Initialize the admitted registration in the same write as its
                                // payload, also protecting puts while Strata's epoch lags Walrus.
                                batch.set_blob_lifetime(key.clone(), u64::from(end_epoch));
                            }
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
                let _guard = this.node.lifecycle.lock_blobs(&[&blob_id.0]).await?;
                this.check_generation()?;
                let mut completion = HaltOnIncompleteWrite::new(this.node.lifecycle.clone());
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

#[cfg(test)]
mod tests;

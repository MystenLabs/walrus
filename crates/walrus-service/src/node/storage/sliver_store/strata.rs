// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Strata implementation of primary and secondary sliver storage.

use std::{
    path::Path,
    sync::{Arc, OnceLock},
    time::Duration,
};

use strata::{
    BlobKey, DEFAULT_GC_INITIAL_WORKER_COUNT, DEFAULT_GC_INTERVAL, DEFAULT_GC_IO_BYTES_PER_SEC,
    DEFAULT_GC_MIN_IO_BYTES_PER_SEC, DEFAULT_GC_SYNC_IMPACT_THRESHOLD,
    DEFAULT_GC_TUNING_WINDOW_CYCLES, DEFAULT_GC_WORKER_COUNT, DEFAULT_LSM_PARTITION_COUNT,
    DEFAULT_SEGMENT_MAX_BYTES, DEFAULT_SEGMENT_READER_CACHE_CAPACITY,
    DEFAULT_SHARD_DROP_GC_DRAIN_TIMEOUT, GcPlannerConfig, SealedSegmentIntegrityPolicy, ShardState,
    StrataLsn, StrataRecoveryPolicy, StrataStore, StrataStoreConfig, StrataStoreMetrics,
};
use tokio::sync::watch;
use typed_store::TypedStoreError;
use typed_store::rocks::{RocksDB, errors::typed_store_err_from_rocks_err};
use walrus_core::{BlobId, ShardIndex, Sliver, SliverType};
use walrus_utils::metrics::Registry;

use crate::utils;

use super::{DatabaseTableOptionsFactory, PrimarySliverData, SecondarySliverData, constants};

const DURABILITY_POLL_INTERVAL: Duration = Duration::from_millis(20);
const DURABILITY_WAIT_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Debug, Clone)]
pub(super) struct StrataSliverStore {
    store: Arc<StrataStore>,
    database: Arc<RocksDB>,
    table_options: DatabaseTableOptionsFactory,
    published: Arc<OnceLock<watch::Receiver<Result<StrataLsn, String>>>>,
}

#[derive(Debug, Clone)]
pub(super) struct StrataShardSliverStore {
    node: StrataSliverStore,
    shard: ShardIndex,
}

impl StrataSliverStore {
    pub(super) fn open(
        path: &Path,
        database: Arc<RocksDB>,
        table_options: DatabaseTableOptionsFactory,
        metrics_registry: &Registry,
    ) -> anyhow::Result<Self> {
        // Keep Strata under the configured storage directory so it uses the same mounted volume.
        // A RocksDB checkpoint still does not include this directory, so node-level checkpoints
        // are disabled while Strata is selected.
        let root_dir = path.join("strata-slivers");
        let config = StrataStoreConfig {
            root_dir,
            namespace: "slivers".to_owned(),
            segment_max_bytes: DEFAULT_SEGMENT_MAX_BYTES,
            write_queue_capacity: 1024,
            max_unsealed_segments: 8,
            segment_reader_cache_capacity: DEFAULT_SEGMENT_READER_CACHE_CAPACITY,
            lsm_partition_count: DEFAULT_LSM_PARTITION_COUNT,
            recovery_policy: StrataRecoveryPolicy::PointInTime,
            sealed_segment_integrity_policy: SealedSegmentIntegrityPolicy::MetadataOnly,
            gc_workers_enabled: true,
            gc_interval: DEFAULT_GC_INTERVAL,
            gc_worker_count: DEFAULT_GC_WORKER_COUNT,
            gc_initial_worker_count: DEFAULT_GC_INITIAL_WORKER_COUNT,
            gc_tuning_window_cycles: DEFAULT_GC_TUNING_WINDOW_CYCLES,
            gc_sync_impact_threshold: DEFAULT_GC_SYNC_IMPACT_THRESHOLD,
            gc_io_bytes_per_sec: DEFAULT_GC_IO_BYTES_PER_SEC,
            gc_min_io_bytes_per_sec: DEFAULT_GC_MIN_IO_BYTES_PER_SEC,
            gc_planner_config: GcPlannerConfig::default(),
            shard_drop_gc_drain_timeout: DEFAULT_SHARD_DROP_GC_DRAIN_TIMEOUT,
            starting_epoch: 0,
        };
        let metrics = StrataStoreMetrics::new(
            metrics_registry.prometheus_registry(),
            format!("slivers:{}", path.display()),
        )?;
        Ok(Self {
            store: Arc::new(StrataStore::open(config, metrics)?),
            database,
            table_options,
            published: Arc::new(OnceLock::new()),
        })
    }

    pub(super) fn open_shard(
        &self,
        shard: ShardIndex,
    ) -> Result<StrataShardSliverStore, TypedStoreError> {
        let primary = constants::primary_slivers_column_family_name(shard);
        if self.database.cf_handle(&primary).is_none() {
            self.database
                .create_cf(&primary, &self.table_options.shard())
                .map_err(typed_store_err_from_rocks_err)?;
        }
        self.store
            .add_shard(u32::from(shard.0))
            .map_err(store_error)?;
        // Empty RocksDB sliver CFs are only shard-creation completion markers. No sliver payload
        // is written to them in Strata mode.
        let secondary = constants::secondary_slivers_column_family_name(shard);
        if self.database.cf_handle(&secondary).is_none() {
            self.database
                .create_cf(&secondary, &self.table_options.shard())
                .map_err(typed_store_err_from_rocks_err)?;
        }
        Ok(StrataShardSliverStore {
            node: self.clone(),
            shard,
        })
    }

    pub(super) fn shard_was_dropped(&self, shard: ShardIndex) -> Result<bool, TypedStoreError> {
        let info = self
            .store
            .shard_info(u32::from(shard.0))
            .map_err(store_error)?
            .ok_or_else(|| {
                TypedStoreError::TaskError(format!(
                    "RocksDB has a completed shard marker for {shard}, but Strata has no shard"
                ))
            })?;
        Ok(info.state == ShardState::Dropped)
    }

    pub(super) fn contains_pairs_in_all(
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

    pub(super) async fn wait_published(&self, lsn: StrataLsn) -> Result<(), TypedStoreError> {
        if lsn == 0 {
            return Ok(());
        }
        let mut receiver = self
            .published
            .get_or_init(|| {
                let (sender, receiver) = watch::channel(Ok(0));
                let store = Arc::downgrade(&self.store);
                tokio::spawn(async move {
                    let mut interval = tokio::time::interval(DURABILITY_POLL_INTERVAL);
                    loop {
                        tokio::select! {
                            _ = sender.closed() => break,
                            _ = interval.tick() => {
                                let Some(store) = store.upgrade() else {
                                    break;
                                };
                                let result = utils::unwrap_or_resume_unwind(
                                    tokio::task::spawn_blocking(move || {
                                        store.published_lsn().map_err(store_error)
                                    }).await,
                                ).map_err(|error| error.to_string());
                                let failed = result.is_err();
                                let _ = sender.send_replace(result);
                                if failed {
                                    break;
                                }
                            }
                        }
                    }
                });
                receiver
            })
            .clone();
        tokio::time::timeout(DURABILITY_WAIT_TIMEOUT, async move {
            loop {
                let published = receiver.borrow().clone().map_err(|error| {
                    TypedStoreError::TaskError(format!("Strata durability publisher: {error}"))
                })?;
                if published >= lsn {
                    return Ok(());
                }
                receiver.changed().await.map_err(|_| {
                    TypedStoreError::TaskError("Strata durability publisher stopped".to_owned())
                })?;
            }
        })
        .await
        .map_err(|_| {
            TypedStoreError::TaskError(format!(
                "timed out waiting for Strata to publish sliver LSN {lsn}"
            ))
        })?
    }
}

impl StrataShardSliverStore {
    pub(super) fn drop_shard(&self) -> Result<(), TypedStoreError> {
        self.node
            .store
            .drop_shard(u32::from(self.shard.0))
            .map_err(store_error)?;
        // Unlike put/tombstone, the current Strata drop API does not return its LSN. Force one
        // sync before RocksDB's shard-completion marker is removed.
        self.node.store.sync().map_err(store_error)
    }

    pub(super) async fn put(&self, blob_id: BlobId, sliver: Sliver) -> Result<(), TypedStoreError> {
        let this = self.clone();
        let lsn = utils::unwrap_or_resume_unwind(
            tokio::task::spawn_blocking(move || this.put_visible(blob_id, &sliver)).await,
        )?;
        self.node.wait_published(lsn).await
    }

    fn put_visible(&self, blob_id: BlobId, sliver: &Sliver) -> Result<StrataLsn, TypedStoreError> {
        let key = sliver_key(&blob_id, sliver.r#type());
        let payload = serialize_sliver(sliver)?;
        self.node
            .store
            .put(u32::from(self.shard.0), &key, &payload)
            .map_err(store_error)
    }

    pub(super) async fn put_many(
        &self,
        slivers: Vec<(BlobId, Sliver)>,
    ) -> Result<(), TypedStoreError> {
        if slivers.is_empty() {
            return Ok(());
        }
        let this = self.clone();
        let lsn = utils::unwrap_or_resume_unwind(
            tokio::task::spawn_blocking(move || {
                let mut batch = this.node.store.batch();
                for (blob_id, sliver) in slivers {
                    let key = sliver_key(&blob_id, sliver.r#type());
                    let payload = serialize_sliver(&sliver)?;
                    batch.put(u32::from(this.shard.0), key, Arc::<[u8]>::from(payload));
                }
                let result = batch.write().map_err(store_error)?;
                Ok::<_, TypedStoreError>(result.op_lsns().last().copied().unwrap_or(0))
            })
            .await,
        )?;
        self.node.wait_published(lsn).await
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
        let lsn = utils::unwrap_or_resume_unwind(
            tokio::task::spawn_blocking(move || {
                let mut batch = this.node.store.batch();
                for sliver_type in [SliverType::Primary, SliverType::Secondary] {
                    batch.tombstone(u32::from(this.shard.0), sliver_key(&blob_id, sliver_type));
                }
                let result = batch.write().map_err(store_error)?;
                Ok::<_, TypedStoreError>(result.op_lsns().last().copied().unwrap_or(0))
            })
            .await,
        )?;
        self.node.wait_published(lsn).await
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

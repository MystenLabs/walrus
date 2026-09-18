// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Backend abstraction for primary and secondary sliver storage.

use std::{
    sync::{Arc, OnceLock},
    time::Instant,
};

use futures::{StreamExt, stream::FuturesUnordered};
use rocksdb::Transaction;
use serde::{Deserialize, Serialize};
use typed_store::{
    Map,
    TypedStoreError,
    rocks::{
        DBBatch,
        DBMap,
        ReadWriteOptions,
        RocksDB,
        SstIngestBuffer,
        SstIngestOptions,
        errors::typed_store_err_from_rocks_err,
    },
};
use walrus_core::{
    BlobId,
    ShardIndex,
    Sliver,
    SliverType,
    by_axis::ByAxis,
    encoding::{PrimarySliver, SecondarySliver},
};

use super::{
    DatabaseTableOptionsFactory,
    constants,
    metrics::{CommonDatabaseMetrics, Labels, OperationType},
};
use crate::utils;

/// Primary sliver data stored in the database.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PrimarySliverData {
    V1(PrimarySliver),
}

impl From<PrimarySliver> for PrimarySliverData {
    fn from(sliver: PrimarySliver) -> Self {
        Self::V1(sliver)
    }
}

impl From<PrimarySliverData> for PrimarySliver {
    fn from(data: PrimarySliverData) -> Self {
        match data {
            PrimarySliverData::V1(sliver) => sliver,
        }
    }
}

/// Secondary sliver data stored in the database.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SecondarySliverData {
    V1(SecondarySliver),
}

impl From<SecondarySliver> for SecondarySliverData {
    fn from(sliver: SecondarySliver) -> Self {
        Self::V1(sliver)
    }
}

impl From<SecondarySliverData> for SecondarySliver {
    fn from(data: SecondarySliverData) -> Self {
        match data {
            SecondarySliverData::V1(sliver) => sliver,
        }
    }
}

/// Node-wide sliver storage backend.
///
/// Cross-shard operations live on this type so a backend can answer them without Walrus issuing a
/// separate lookup for every shard. Per-shard operations are exposed through [`ShardSliverStore`].
#[derive(Debug, Clone)]
pub(crate) struct SliverStore {
    backend: Arc<SliverStoreBackend>,
}

#[derive(Debug)]
enum SliverStoreBackend {
    RocksDb(RocksDbSliverStore),
}

#[derive(Debug)]
struct RocksDbSliverStore {
    database: Arc<RocksDB>,
    table_options: DatabaseTableOptionsFactory,
}

impl SliverStore {
    pub(crate) fn new_rocksdb(
        database: Arc<RocksDB>,
        table_options: DatabaseTableOptionsFactory,
    ) -> Self {
        Self {
            backend: Arc::new(SliverStoreBackend::RocksDb(RocksDbSliverStore {
                database,
                table_options,
            })),
        }
    }

    /// Opens the backend storage for one Walrus shard.
    pub(super) fn open_shard(
        &self,
        shard: ShardIndex,
        metrics: CommonDatabaseMetrics,
    ) -> Result<ShardSliverStore, TypedStoreError> {
        match self.backend.as_ref() {
            SliverStoreBackend::RocksDb(store) => store.open_shard(shard, metrics),
        }
    }

    /// Returns whether both sliver types exist for `blob_id` in every required Walrus shard.
    ///
    /// The required shards are selected by the caller because committee and epoch policy do not
    /// belong in the storage backend. An empty set is complete by definition.
    pub(crate) async fn contains_sliver_pairs_in_all(
        &self,
        blob_id: BlobId,
        shards: Vec<(ShardIndex, ShardSliverStore)>,
    ) -> Result<bool, TypedStoreError> {
        match self.backend.as_ref() {
            SliverStoreBackend::RocksDb(_) => {
                if shards.len() > 1
                    && first_shard_failing_check(
                        blob_id,
                        &shards,
                        ShardSliverStore::may_have_sliver_pair,
                    )
                    .await?
                    .is_some()
                {
                    return Ok(false);
                }

                Ok(first_shard_failing_check(
                    blob_id,
                    &shards,
                    ShardSliverStore::is_sliver_pair_stored,
                )
                .await?
                .is_none())
            }
        }
    }

    /// Synchronous counterpart used when the caller already runs on a blocking executor.
    #[cfg(msim)]
    pub(crate) fn contains_sliver_pairs_in_all_sync(
        &self,
        blob_id: &BlobId,
        shards: &[(ShardIndex, ShardSliverStore)],
    ) -> Result<bool, TypedStoreError> {
        match self.backend.as_ref() {
            SliverStoreBackend::RocksDb(_) => {
                if shards.len() > 1 {
                    for (_, store) in shards {
                        if !store.may_have_sliver_pair(blob_id)? {
                            return Ok(false);
                        }
                    }
                }
                for (_, store) in shards {
                    if !store.is_sliver_pair_stored(blob_id)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
        }
    }
}

impl RocksDbSliverStore {
    fn open_shard(
        &self,
        shard: ShardIndex,
        metrics: CommonDatabaseMetrics,
    ) -> Result<ShardSliverStore, TypedStoreError> {
        let rw_options = ReadWriteOptions::default();

        macro_rules! reopen_cf {
            ($name:expr, $class:expr) => {{
                let name = $name;
                if self.database.cf_handle(&name).is_none() {
                    #[cfg(msim)]
                    sui_macros::fail_point!("create-cf-before");
                    self.database
                        .create_cf(&name, &self.table_options.shard())
                        .map_err(typed_store_err_from_rocks_err)?;
                }
                DBMap::reopen_with_class(
                    &self.database,
                    Some(&name),
                    Some($class),
                    &rw_options,
                    false,
                )?
            }};
        }

        // Sliver column families are deliberately opened after the shard control tables. Their
        // presence is the existing RocksDB completion marker for shard creation.
        let primary_slivers = reopen_cf!(
            constants::primary_slivers_column_family_name(shard),
            "primary_slivers"
        );
        let secondary_slivers = reopen_cf!(
            constants::secondary_slivers_column_family_name(shard),
            "secondary_slivers"
        );

        Ok(ShardSliverStore {
            metrics,
            backend: ShardSliverStoreBackend::RocksDb(RocksDbShardSliverStore {
                primary_slivers,
                secondary_slivers,
                sst_primary_buffer: Arc::new(OnceLock::new()),
                sst_secondary_buffer: Arc::new(OnceLock::new()),
            }),
        })
    }
}

/// Backend storage for a single Walrus shard.
#[derive(Debug, Clone)]
pub(crate) struct ShardSliverStore {
    metrics: CommonDatabaseMetrics,
    backend: ShardSliverStoreBackend,
}

#[derive(Debug, Clone)]
enum ShardSliverStoreBackend {
    RocksDb(RocksDbShardSliverStore),
}

#[derive(Debug, Clone)]
struct RocksDbShardSliverStore {
    primary_slivers: DBMap<BlobId, PrimarySliverData>,
    secondary_slivers: DBMap<BlobId, SecondarySliverData>,
    sst_primary_buffer: Arc<OnceLock<std::sync::Mutex<SstIngestBuffer<BlobId, PrimarySliverData>>>>,
    sst_secondary_buffer:
        Arc<OnceLock<std::sync::Mutex<SstIngestBuffer<BlobId, SecondarySliverData>>>>,
}

impl ShardSliverStore {
    /// Stores one sliver. This remains the normal foreground write API; bulk writes are only an
    /// implementation option for workflows such as shard sync.
    ///
    /// Successful completion is the durability boundary used by subsequent control-state writes.
    /// A backend that persists in the background must wait until the LSN corresponding to this
    /// write has been published durably before returning.
    pub(crate) async fn put(&self, blob_id: BlobId, sliver: Sliver) -> Result<(), TypedStoreError> {
        let start = Instant::now();
        let sliver_type = sliver.r#type();
        let response = match &self.backend {
            ShardSliverStoreBackend::RocksDb(store) => match sliver {
                Sliver::Primary(primary) => {
                    let table = store.primary_slivers.clone();
                    utils::unwrap_or_resume_unwind(
                        tokio::task::spawn_blocking(move || {
                            table.insert(&blob_id, &PrimarySliverData::from(primary))
                        })
                        .await,
                    )
                }
                Sliver::Secondary(secondary) => {
                    let table = store.secondary_slivers.clone();
                    utils::unwrap_or_resume_unwind(
                        tokio::task::spawn_blocking(move || {
                            table.insert(&blob_id, &SecondarySliverData::from(secondary))
                        })
                        .await,
                    )
                }
            },
        };
        self.metrics.observe_operation_duration(
            sliver_labels(
                sliver_type,
                OperationType::Insert,
                "INSERT (blob_id, sliver)",
            )
            .with_response(response.as_ref()),
            start.elapsed(),
        );
        response
    }

    pub(crate) fn get(
        &self,
        blob_id: &BlobId,
        sliver_type: SliverType,
    ) -> Result<Option<Sliver>, TypedStoreError> {
        let start = Instant::now();
        let query_summary = if sliver_type.is_primary() {
            "GET primary_sliver BY blob_id"
        } else {
            "GET secondary_sliver BY blob_id"
        };
        let response = match sliver_type {
            SliverType::Primary => self
                .get_primary(blob_id)
                .map(|sliver| sliver.map(Sliver::Primary)),
            SliverType::Secondary => self
                .get_secondary(blob_id)
                .map(|sliver| sliver.map(Sliver::Secondary)),
        };
        self.metrics.observe_operation_duration(
            sliver_labels(sliver_type, OperationType::Get, query_summary)
                .with_response(response.as_ref()),
            start.elapsed(),
        );
        response
    }

    fn get_primary(&self, blob_id: &BlobId) -> Result<Option<PrimarySliver>, TypedStoreError> {
        match &self.backend {
            ShardSliverStoreBackend::RocksDb(store) => store
                .primary_slivers
                .get(blob_id)
                .map(|sliver| sliver.map(Into::into)),
        }
    }

    fn get_secondary(&self, blob_id: &BlobId) -> Result<Option<SecondarySliver>, TypedStoreError> {
        match &self.backend {
            ShardSliverStoreBackend::RocksDb(store) => store
                .secondary_slivers
                .get(blob_id)
                .map(|sliver| sliver.map(Into::into)),
        }
    }

    pub(crate) fn may_have_sliver_pair(&self, blob_id: &BlobId) -> Result<bool, TypedStoreError> {
        Ok(self.may_have(blob_id, SliverType::Primary)?
            && self.may_have(blob_id, SliverType::Secondary)?)
    }

    pub(crate) fn may_have(
        &self,
        blob_id: &BlobId,
        sliver_type: SliverType,
    ) -> Result<bool, TypedStoreError> {
        let start = Instant::now();
        let response = match (&self.backend, sliver_type) {
            (ShardSliverStoreBackend::RocksDb(store), SliverType::Primary) => {
                store.primary_slivers.may_contain_key(blob_id)
            }
            (ShardSliverStoreBackend::RocksDb(store), SliverType::Secondary) => {
                store.secondary_slivers.may_contain_key(blob_id)
            }
        };
        self.metrics.observe_operation_duration(
            sliver_labels(
                sliver_type,
                OperationType::ContainsKey,
                "KEY_MAY_EXIST blob_id",
            )
            .with_response(response.as_ref()),
            start.elapsed(),
        );
        response
    }

    pub(crate) fn is_sliver_pair_stored(&self, blob_id: &BlobId) -> Result<bool, TypedStoreError> {
        Ok(self.contains(blob_id, SliverType::Primary)?
            && self.contains(blob_id, SliverType::Secondary)?)
    }

    pub(crate) fn contains(
        &self,
        blob_id: &BlobId,
        sliver_type: SliverType,
    ) -> Result<bool, TypedStoreError> {
        let start = Instant::now();
        let response = match (&self.backend, sliver_type) {
            (ShardSliverStoreBackend::RocksDb(store), SliverType::Primary) => {
                store.primary_slivers.contains_key(blob_id)
            }
            (ShardSliverStoreBackend::RocksDb(store), SliverType::Secondary) => {
                store.secondary_slivers.contains_key(blob_id)
            }
        };
        self.metrics.observe_operation_duration(
            sliver_labels(
                sliver_type,
                OperationType::ContainsKey,
                "CONTAINS_KEY blob_id",
            )
            .with_response(response.as_ref()),
            start.elapsed(),
        );
        response
    }

    pub(crate) fn get_many(
        &self,
        sliver_type: SliverType,
        blob_ids: &[BlobId],
    ) -> Result<Vec<(BlobId, Sliver)>, TypedStoreError> {
        let start = Instant::now();
        let response = match &self.backend {
            ShardSliverStoreBackend::RocksDb(store) => ByAxis::from(sliver_type)
                .map(
                    |_| store.primary_slivers.multi_get(blob_ids),
                    |_| store.secondary_slivers.multi_get(blob_ids),
                )
                .transpose(),
        };

        self.metrics.observe_operation_duration(
            sliver_labels(
                sliver_type,
                OperationType::MultiGet,
                "MULTI_GET sliver BY blob_id_list",
            )
            .with_response_as_unit(response.as_ref()),
            start.elapsed(),
        );

        let output = match response? {
            ByAxis::Primary(slivers) => blob_ids
                .iter()
                .zip(slivers)
                .filter_map(|(&blob_id, sliver)| {
                    let PrimarySliverData::V1(sliver) = sliver?;
                    Some((blob_id, Sliver::Primary(sliver)))
                })
                .collect(),
            ByAxis::Secondary(slivers) => blob_ids
                .iter()
                .zip(slivers)
                .filter_map(|(&blob_id, sliver)| {
                    let SecondarySliverData::V1(sliver) = sliver?;
                    Some((blob_id, Sliver::Secondary(sliver)))
                })
                .collect(),
        };
        Ok(output)
    }

    pub(crate) fn batch(&self, sliver_type: SliverType) -> DBBatch {
        match (&self.backend, sliver_type) {
            (ShardSliverStoreBackend::RocksDb(store), SliverType::Primary) => {
                store.primary_slivers.batch()
            }
            (ShardSliverStoreBackend::RocksDb(store), SliverType::Secondary) => {
                store.secondary_slivers.batch()
            }
        }
    }

    pub(crate) fn insert_in_batch(
        &self,
        batch: &mut DBBatch,
        blob_id: &BlobId,
        sliver: &Sliver,
    ) -> Result<(), TypedStoreError> {
        match (&self.backend, sliver) {
            (ShardSliverStoreBackend::RocksDb(store), Sliver::Primary(primary)) => batch
                .insert_batch(
                    &store.primary_slivers,
                    [(blob_id, &PrimarySliverData::from(primary.clone()))],
                )
                .map(|_| ()),
            (ShardSliverStoreBackend::RocksDb(store), Sliver::Secondary(secondary)) => batch
                .insert_batch(
                    &store.secondary_slivers,
                    [(blob_id, &SecondarySliverData::from(secondary.clone()))],
                )
                .map(|_| ()),
        }
    }

    pub(crate) fn push_sst(
        &self,
        blob_id: BlobId,
        sliver: &Sliver,
        options: SstIngestOptions,
    ) -> Result<(), TypedStoreError> {
        match (&self.backend, sliver) {
            (ShardSliverStoreBackend::RocksDb(store), Sliver::Primary(primary)) => {
                let buffer = store.sst_primary_buffer.get_or_init(|| {
                    std::sync::Mutex::new(
                        SstIngestBuffer::new(&store.primary_slivers, options)
                            .expect("SST buffer creation should succeed"),
                    )
                });
                buffer
                    .lock()
                    .expect("lock should succeed")
                    .push(blob_id, PrimarySliverData::from(primary.clone()))
            }
            (ShardSliverStoreBackend::RocksDb(store), Sliver::Secondary(secondary)) => {
                let buffer = store.sst_secondary_buffer.get_or_init(|| {
                    std::sync::Mutex::new(
                        SstIngestBuffer::new(&store.secondary_slivers, options)
                            .expect("SST buffer creation should succeed"),
                    )
                });
                buffer
                    .lock()
                    .expect("lock should succeed")
                    .push(blob_id, SecondarySliverData::from(secondary.clone()))
            }
        }
    }

    pub(crate) fn clear_sst_buffers(&self) -> Result<(), TypedStoreError> {
        match &self.backend {
            ShardSliverStoreBackend::RocksDb(store) => {
                if let Some(buffer) = store.sst_primary_buffer.get()
                    && let Ok(mut buffer) = buffer.lock()
                {
                    buffer.clear()?;
                }
                if let Some(buffer) = store.sst_secondary_buffer.get()
                    && let Ok(mut buffer) = buffer.lock()
                {
                    buffer.clear()?;
                }
                Ok(())
            }
        }
    }

    pub(crate) fn flush_sst(
        &self,
        sliver_type: SliverType,
        end_of_range: bool,
        sst_file_threshold: usize,
        compact_after_sync: bool,
    ) -> Result<bool, TypedStoreError> {
        match (&self.backend, sliver_type) {
            (ShardSliverStoreBackend::RocksDb(store), SliverType::Primary) => flush_sst_buffer(
                store.sst_primary_buffer.get(),
                &store.primary_slivers,
                end_of_range,
                sst_file_threshold,
                compact_after_sync,
            ),
            (ShardSliverStoreBackend::RocksDb(store), SliverType::Secondary) => flush_sst_buffer(
                store.sst_secondary_buffer.get(),
                &store.secondary_slivers,
                end_of_range,
                sst_file_threshold,
                compact_after_sync,
            ),
        }
    }

    pub(crate) fn delete_pair_in_batch(
        &self,
        batch: &mut DBBatch,
        blob_id: &BlobId,
    ) -> Result<(), TypedStoreError> {
        match &self.backend {
            ShardSliverStoreBackend::RocksDb(store) => {
                batch.delete_batch(&store.primary_slivers, std::iter::once(blob_id))?;
                batch.delete_batch(&store.secondary_slivers, std::iter::once(blob_id))?;
                Ok(())
            }
        }
    }

    pub(crate) fn delete_pair_in_transaction(
        &self,
        transaction: &Transaction<'_, rocksdb::OptimisticTransactionDB>,
        blob_id: &BlobId,
    ) -> anyhow::Result<()> {
        match &self.backend {
            ShardSliverStoreBackend::RocksDb(store) => {
                let column_families = [store.primary_slivers.cf()?, store.secondary_slivers.cf()?];
                for cf in column_families {
                    transaction.delete_cf(&cf, blob_id)?;
                }
                Ok(())
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn sliver_count(&self, sliver_type: SliverType) -> Result<usize, TypedStoreError> {
        match (&self.backend, sliver_type) {
            (ShardSliverStoreBackend::RocksDb(store), SliverType::Primary) => store
                .primary_slivers
                .safe_iter()?
                .try_fold(0, |count, entry| entry.map(|_| count + 1)),
            (ShardSliverStoreBackend::RocksDb(store), SliverType::Secondary) => store
                .secondary_slivers
                .safe_iter()?
                .try_fold(0, |count, entry| entry.map(|_| count + 1)),
        }
    }
}

fn sliver_labels(
    sliver_type: SliverType,
    operation_name: OperationType,
    query_summary: &'static str,
) -> Labels<'static> {
    Labels {
        collection_name: if sliver_type.is_primary() {
            "shard/primary-slivers"
        } else {
            "shard/secondary-slivers"
        },
        operation_name,
        query_summary,
        ..Labels::default()
    }
}

fn flush_sst_buffer<V>(
    buffer: Option<&std::sync::Mutex<SstIngestBuffer<BlobId, V>>>,
    table: &DBMap<BlobId, V>,
    end_of_range: bool,
    sst_file_threshold: usize,
    compact_after_sync: bool,
) -> Result<bool, TypedStoreError>
where
    V: Serialize,
{
    let Some(buffer) = buffer else {
        return Ok(false);
    };
    let mut buffer = buffer.lock().expect("lock should succeed");
    if buffer.size() < sst_file_threshold && !end_of_range {
        return Ok(false);
    }
    buffer.flush()?;
    if end_of_range && compact_after_sync {
        table.compact_range_to_bottom(&BlobId::ZERO, &BlobId::MAX)?;
    }
    Ok(true)
}

async fn first_shard_failing_check(
    blob_id: BlobId,
    shards: &[(ShardIndex, ShardSliverStore)],
    check: fn(&ShardSliverStore, &BlobId) -> Result<bool, TypedStoreError>,
) -> Result<Option<ShardIndex>, TypedStoreError> {
    #[cfg(msim)]
    {
        for (shard, store) in shards {
            if !check(store, &blob_id)? {
                return Ok(Some(*shard));
            }
        }
        return Ok(None);
    }

    #[cfg(not(msim))]
    {
        const MAX_CONCURRENT_SHARD_STORAGE_CHECKS: usize = 4;

        let make_check = |shard: ShardIndex, store: ShardSliverStore| async move {
            let passed = utils::unwrap_or_resume_unwind(
                tokio::task::spawn_blocking(move || check(&store, &blob_id)).await,
            )?;
            Ok::<_, TypedStoreError>((shard, passed))
        };

        let mut shard_iter = shards.iter();
        let mut checks = FuturesUnordered::new();
        for _ in 0..MAX_CONCURRENT_SHARD_STORAGE_CHECKS {
            let Some((shard, store)) = shard_iter.next() else {
                break;
            };
            checks.push(make_check(*shard, store.clone()));
        }

        let mut first_failed_shard = None;
        let mut first_error = None;
        while let Some(result) = checks.next().await {
            match result {
                Ok((shard, false)) if first_failed_shard.is_none() => {
                    first_failed_shard = Some(shard);
                }
                Ok(_) => {}
                Err(error) if first_error.is_none() => first_error = Some(error),
                Err(_) => {}
            }

            // Already-started blocking probes are drained, but no new work is queued after the
            // result is known.
            if first_failed_shard.is_none()
                && first_error.is_none()
                && let Some((shard, store)) = shard_iter.next()
            {
                checks.push(make_check(*shard, store.clone()));
            }
        }

        first_error.map_or(Ok(first_failed_shard), Err)
    }
}

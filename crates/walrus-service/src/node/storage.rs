// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use core::fmt::{self, Display};
use std::{
    collections::{HashMap, hash_map::Entry},
    fmt::Debug,
    ops::Bound::{Excluded, Included},
    path::Path,
    sync::Arc,
    time::Instant,
};

use futures::FutureExt as _;
use itertools::Itertools;
use rocksdb::Options;
use serde::{Deserialize, Serialize};
use sui_sdk::types::event::EventID;
use sui_types::base_types::ObjectID;
use tokio::sync::{OwnedRwLockWriteGuard, RwLock};
use typed_store::{
    Map,
    TypedStoreError,
    rocks::{self, DBBatch, DBMap, MetricConf, ReadWriteOptions, RocksDB},
};
use walrus_core::{
    BlobId,
    Epoch,
    ShardIndex,
    messages::{SyncShardRequest, SyncShardResponse},
    metadata::{BlobMetadata, VerifiedBlobMetadataWithId},
};
use walrus_sui::types::BlobEvent;
use walrus_utils::metrics::Registry;

use self::{
    blob_info::{
        BlobInfo,
        BlobInfoApi,
        BlobInfoIterator,
        BlobInfoTable,
        PerObjectBlobInfo,
        PerObjectBlobInfoIterator,
    },
    constants::{
        metadata_cf_name,
        node_status_cf_name,
        pending_recover_slivers_column_family_name,
        primary_slivers_column_family_name,
        secondary_slivers_column_family_name,
        shard_status_column_family_name,
        shard_sync_progress_column_family_name,
    },
    event_cursor_table::{EventCursorTable, EventIdWithProgress},
    metrics::{CommonDatabaseMetrics, Labels, OperationType},
};
use super::errors::{ShardNotAssigned, SyncShardServiceError};
use crate::utils;

pub(crate) mod blob_info;
pub(crate) mod constants;

mod database_config;
pub use database_config::DatabaseConfig;

mod event_cursor_table;
pub(super) use event_cursor_table::EventProgress;

mod event_sequencer;
mod metrics;
mod shard;

pub(crate) use shard::{
    PrimarySliverData,
    SecondarySliverData,
    ShardStatus,
    ShardStorage,
    pending_recover_slivers_column_family_options,
    primary_slivers_column_family_options,
    secondary_slivers_column_family_options,
    shard_status_column_family_options,
    shard_sync_progress_column_family_options,
};

pub(crate) fn metadata_options(db_config: &DatabaseConfig) -> Options {
    db_config.metadata().to_options()
}

pub(crate) fn node_status_options(db_config: &DatabaseConfig) -> Options {
    db_config.node_status().to_options()
}

/// The status of the node.
///
/// ```text
///    RecoveryCatchUpWithIncompleteHistory
///       ^                             |
///      /                              |
///     v                               v
/// Standby <--> RecoveryCatchUp --> RecoveryInProgress
///      \          /        ^       /
///       v        v          \     v
///      RecoverMetadata  --> Active
/// ```
// Important: this enum is committed to database. Do not modify the existing fields. Only add new
// fields at the end.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum NodeStatus {
    /// The node is up-to-date with events but not a committee member.
    Standby,
    /// The node is a committee member and up-to-date with events.
    Active,
    /// The node is in recovery mode and syncing metadata.
    RecoverMetadata,
    /// The node is in recovery mode and catching up with the chain.
    RecoveryCatchUp,
    /// The node is in recovery mode and recovering missing slivers. The included epoch is the
    /// epoch in which the node started recovering.
    RecoveryInProgress(Epoch),
    /// The node is in recovery mode and catching up with the chain, but the history is incomplete
    /// due to expired event blobs.
    RecoveryCatchUpWithIncompleteHistory {
        /// The first epoch for which all events are available and relevant. When processing events,
        /// we will discard all events issued before this epoch.
        first_complete_epoch: Epoch,
        /// The epoch at which the node started recovering. When processing events, we will include
        /// events for all blobs that expire after this epoch.
        epoch_at_start: Epoch,
    },
}

impl NodeStatus {
    /// Used to convert `NodeStatus` to `i64` for metrics.
    pub fn to_i64(&self) -> i64 {
        match self {
            NodeStatus::Standby => 0,
            NodeStatus::Active => 1,
            NodeStatus::RecoverMetadata => 2,
            NodeStatus::RecoveryCatchUp => 3,
            NodeStatus::RecoveryInProgress(_) => 4,
            NodeStatus::RecoveryCatchUpWithIncompleteHistory { .. } => 5,
        }
    }

    /// Returns `true` if the node is catching up.
    pub fn is_catching_up(&self) -> bool {
        matches!(
            self,
            NodeStatus::RecoveryCatchUp | NodeStatus::RecoveryCatchUpWithIncompleteHistory { .. }
        )
    }

    /// Returns `true` if the node is catching up with incomplete history.
    pub fn is_catching_up_with_incomplete_history(&self) -> bool {
        matches!(
            self,
            NodeStatus::RecoveryCatchUpWithIncompleteHistory { .. }
        )
    }
}

impl Display for NodeStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            NodeStatus::Standby => write!(f, "Standby"),
            NodeStatus::Active => write!(f, "Active"),
            NodeStatus::RecoverMetadata => write!(f, "RecoverMetadata"),
            NodeStatus::RecoveryCatchUp => write!(f, "RecoveryCatchUp"),
            NodeStatus::RecoveryInProgress(epoch) => write!(f, "RecoveryInProgress ({epoch})"),
            NodeStatus::RecoveryCatchUpWithIncompleteHistory {
                first_complete_epoch: first_epoch,
                epoch_at_start,
            } => write!(
                f,
                "RecoveryCatchUpWithIncompleteHistory \
                (first complete epoch: {first_epoch}, epoch at start: {epoch_at_start})"
            ),
        }
    }
}

/// Storage backing a [`StorageNode`][crate::node::StorageNode].
///
/// Enables storing blob metadata, which is shared across all shards. The method
/// [`shard_storage()`][Self::shard_storage] can be used to retrieve shard-specific storage.
#[derive(Debug, Clone)]
pub struct Storage {
    database: Arc<RocksDB>,
    node_status: DBMap<(), NodeStatus>,
    metadata: DBMap<BlobId, BlobMetadata>,
    blob_info: BlobInfoTable,
    event_cursor: EventCursorTable,
    shards: Arc<RwLock<HashMap<ShardIndex, Arc<ShardStorage>>>>,
    config: DatabaseConfig,
    metrics: Arc<CommonDatabaseMetrics>,
    metrics_registry: Registry,
}

/// An opaque lock object that can be required to later access the shards map.
pub(crate) struct StorageShardLock {
    // The shards that are currently present in the storage.
    existing_shards: Vec<ShardIndex>,
    // The guard to the shards map.
    shards_guard: OwnedRwLockWriteGuard<HashMap<ShardIndex, Arc<ShardStorage>>>,
}

impl StorageShardLock {
    /// Returns the shards that are currently present in the storage.
    pub fn existing_shards(&self) -> &[ShardIndex] {
        &self.existing_shards
    }
}

impl Storage {
    /// Opens the storage database located at the specified path, creating the database if absent.
    pub fn open(
        path: &Path,
        db_config: DatabaseConfig,
        metrics_config: MetricConf,
        registry: Registry,
    ) -> Result<Self, anyhow::Error> {
        let mut db_opts = Options::from(&db_config.global);
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);

        let existing_shards_ids = ShardStorage::existing_cf_shards_ids(path, &db_opts);
        tracing::info!(
            "open storage for existing shards IDs: {}",
            existing_shards_ids
                .iter()
                .map(ToString::to_string)
                .join(", ")
        );
        let mut shard_column_families = existing_shards_ids
            .iter()
            .copied()
            .flat_map(|id| {
                [
                    (
                        primary_slivers_column_family_name(id),
                        primary_slivers_column_family_options(&db_config),
                    ),
                    (
                        secondary_slivers_column_family_name(id),
                        secondary_slivers_column_family_options(&db_config),
                    ),
                    (
                        shard_status_column_family_name(id),
                        shard_status_column_family_options(&db_config),
                    ),
                    (
                        shard_sync_progress_column_family_name(id),
                        shard_sync_progress_column_family_options(&db_config),
                    ),
                    (
                        pending_recover_slivers_column_family_name(id),
                        pending_recover_slivers_column_family_options(&db_config),
                    ),
                ]
            })
            .collect::<Vec<_>>();

        let node_status_cf_name = node_status_cf_name();
        let node_status_options = node_status_options(&db_config);
        let metadata_options = metadata_options(&db_config);
        let metadata_cf_name = metadata_cf_name();
        let blob_info_column_families = BlobInfoTable::options(&db_config);
        let (event_cursor_cf_name, event_cursor_options) = EventCursorTable::options(&db_config);

        let expected_column_families: Vec<_> = shard_column_families
            .iter_mut()
            .map(|(name, opts)| (name.as_str(), std::mem::take(opts)))
            .chain([
                (node_status_cf_name, node_status_options),
                (metadata_cf_name, metadata_options),
                (event_cursor_cf_name, event_cursor_options),
            ])
            .chain(blob_info_column_families)
            .collect::<Vec<_>>();

        let database = rocks::open_cf_opts(
            path,
            Some(db_opts),
            metrics_config,
            &expected_column_families,
        )?;

        let node_status = DBMap::reopen(
            &database,
            Some(node_status_cf_name),
            &ReadWriteOptions::default(),
            false,
        )?;
        if node_status.get(&())?.is_none() {
            node_status.insert(&(), &NodeStatus::Standby)?;
        }

        let metadata = DBMap::reopen(
            &database,
            Some(metadata_cf_name),
            &ReadWriteOptions::default(),
            false,
        )?;

        let event_cursor = EventCursorTable::reopen(&database)?;
        let blob_info = BlobInfoTable::reopen(&database)?;
        let shards = Arc::new(RwLock::new(
            existing_shards_ids
                .into_iter()
                .map(|id| {
                    ShardStorage::create_or_reopen(id, &database, &db_config, None, &registry)
                        .map(|shard| (id, Arc::new(shard)))
                })
                .collect::<Result<_, _>>()?,
        ));

        Ok(Self {
            database,
            node_status,
            metadata,
            blob_info,
            event_cursor,
            shards,
            config: db_config,
            metrics: Arc::new(CommonDatabaseMetrics::new_with_id(
                &registry,
                "storage".to_owned(),
            )),
            metrics_registry: registry,
        })
    }

    /// Returns a reference to the database.
    pub(crate) fn get_db(&self) -> Arc<RocksDB> {
        self.database.clone()
    }

    pub(crate) fn node_status(&self) -> Result<NodeStatus, TypedStoreError> {
        self.node_status
            .get(&())
            .map(|value| value.expect("node status should always be set"))
    }

    pub(super) fn set_node_status(&self, status: NodeStatus) -> Result<(), TypedStoreError> {
        self.node_status.insert(&(), &status)
    }

    pub(crate) fn clear_blob_info_table(&self) -> Result<(), TypedStoreError> {
        self.blob_info.clear()
    }

    /// Returns lock write access to the shards map, and returns the underlying shard map.
    pub(crate) async fn lock_shards(&self) -> StorageShardLock {
        let shards_guard = self.shards.clone().write_owned().await;
        let existing_shards = shards_guard.keys().cloned().collect::<Vec<_>>();
        StorageShardLock {
            existing_shards,
            shards_guard,
        }
    }

    /// Creates the storage for the specified shards, if it does not exist yet.
    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn create_storage_for_shards(
        &self,
        new_shards: &[ShardIndex],
    ) -> Result<(), TypedStoreError> {
        let shard_map_lock = self.lock_shards().await;
        self.create_storage_for_shards_locked(shard_map_lock, new_shards)
            .await
    }

    pub(crate) async fn create_storage_for_shards_locked(
        &self,
        mut locked_map: StorageShardLock,
        new_shards: &[ShardIndex],
    ) -> Result<(), TypedStoreError> {
        let start = Instant::now();
        let labels = Labels {
            collection_name: "shards",
            operation_name: OperationType::Create,
            query_summary: "CREATE shards",
            ..Labels::default()
        };
        tracing::info!(count = new_shards.len(), "creating storage for shards");

        for &shard_index in new_shards {
            match locked_map.shards_guard.entry(shard_index) {
                Entry::Vacant(entry) => {
                    let shard_storage = ShardStorage::create_or_reopen(
                        shard_index,
                        &self.database,
                        &self.config,
                        Some(ShardStatus::None),
                        &self.metrics_registry,
                    )
                    .inspect_err(|error| {
                        self.metrics
                            .observe_operation_duration(labels.with_error(error), start.elapsed());
                    })?;

                    tracing::info!(
                        walrus.shard_index = %shard_index,
                        "successfully created storage for shard"
                    );
                    entry.insert(Arc::new(shard_storage));
                }
                Entry::Occupied(_) => (),
            }
        }

        self.metrics
            .observe_operation_duration(labels.with_response(Ok(&())), start.elapsed());
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    pub(crate) async fn remove_storage_for_shards(
        &self,
        removed: &[ShardIndex],
    ) -> Result<(), TypedStoreError> {
        let mut shard_map_lock = self.lock_shards().await;
        for shard_index in removed {
            tracing::info!(walrus.shard_index = %shard_index, "removing storage for shard");
            if let Some(shard_storage) = shard_map_lock.shards_guard.remove(shard_index) {
                // Do not hold the `shards` lock when deleting column families.
                shard_storage.delete_shard_storage()?;
            }
            tracing::info!(
                walrus.shard_index = %shard_index,
                "successfully removed storage for shard"
            );
        }
        Ok(())
    }

    /// Returns the indices of the shards managed by the storage.
    pub async fn existing_shards(&self) -> Vec<ShardIndex> {
        self.shards.read().await.keys().cloned().collect::<Vec<_>>()
    }

    /// Returns a vector of the shard storages managed by the storage.
    pub async fn existing_shard_storages(&self) -> Vec<Arc<ShardStorage>> {
        self.shards
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>()
    }

    /// Returns a handle over the storage for a single shard.
    pub async fn shard_storage(&self, shard: ShardIndex) -> Option<Arc<ShardStorage>> {
        self.shards.read().await.get(&shard).cloned()
    }

    /// Attempts to get the status of the stored shards.
    ///
    /// For each shard, the status is returned if it can be determined, otherwise, `None` is
    /// returned.
    pub async fn list_shard_status(&self) -> HashMap<ShardIndex, Option<ShardStatus>> {
        let shards = self.shards.read().await;

        let mut result = HashMap::new();
        for (shard, storage) in shards.iter() {
            result.insert(*shard, storage.status().await.ok());
        }
        result
    }

    /// Store the verified metadata without updating blob info. This is only
    /// used during storing metadata for event blobs which are stored without getting registered
    /// first.
    #[tracing::instrument(skip_all)]
    pub fn put_verified_metadata_without_blob_info(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
    ) -> Result<(), TypedStoreError> {
        self.metadata
            .insert(metadata.blob_id(), metadata.metadata())
    }

    /// Store the metadata without updating blob info. This is only used during storing metadata for
    /// event blobs which are stored without getting registered first.
    #[tracing::instrument(skip_all)]
    pub fn update_blob_info_with_metadata(&self, blob_id: &BlobId) -> Result<(), TypedStoreError> {
        let mut batch = self.metadata.batch();
        self.blob_info
            .set_metadata_stored(&mut batch, blob_id, true)?;
        batch.write()
    }

    /// Store the verified metadata.
    ///
    /// This must *not* be called for a blob that is not tracked in the blob-info table.
    #[tracing::instrument(skip_all)]
    pub async fn put_verified_metadata(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
    ) -> Result<(), TypedStoreError> {
        self.put_metadata(metadata.blob_id(), metadata.metadata())
            .await
    }

    // Important: This must *not* be called for a blob that is not tracked in the blob-info table.
    async fn put_metadata(
        &self,
        blob_id: &BlobId,
        metadata: &BlobMetadata,
    ) -> Result<(), TypedStoreError> {
        let start = Instant::now();
        let labels = Labels {
            collection_name: metadata_cf_name(),
            operation_name: OperationType::Insert,
            query_summary: "INSERT metadata BY blob_id, UPDATE blob_info",
            ..Default::default()
        };

        let mut batch = self.metadata.batch();
        batch.insert_batch(&self.metadata, [(blob_id, metadata)])?;
        self.blob_info
            .set_metadata_stored(&mut batch, blob_id, true)?;

        let response = tokio::task::spawn_blocking(move || batch.write())
            .map(utils::unwrap_or_resume_unwind)
            .await;

        self.metrics
            .observe_operation_duration(labels.with_response(response.as_ref()), start.elapsed());
        response
    }

    /// Returns the blob info for `blob_id`.
    #[tracing::instrument(skip_all)]
    pub(crate) fn get_blob_info(
        &self,
        blob_id: &BlobId,
    ) -> Result<Option<BlobInfo>, TypedStoreError> {
        self.blob_info.get(blob_id)
    }

    /// Returns the per-object blob info for `object_id`.
    pub(crate) fn get_per_object_info(
        &self,
        object_id: &ObjectID,
    ) -> Result<Option<PerObjectBlobInfo>, TypedStoreError> {
        self.blob_info.get_per_object_info(object_id)
    }

    /// Returns the current event cursor and the next event index.
    #[tracing::instrument(skip_all)]
    pub fn get_event_cursor_and_next_index(
        &self,
    ) -> Result<Option<EventIdWithProgress>, TypedStoreError> {
        self.event_cursor.get_event_cursor_and_next_index()
    }

    /// Updates the blob info for a blob based on the [`BlobEvent`].
    ///
    /// This must be called with monotonically increasing values of the `event_index`, and even
    /// across restarts the same `event_index` must be assigned to the an event. The function in
    /// turn ensures that the corresponding call is idempotent.
    #[tracing::instrument(skip_all)]
    pub fn update_blob_info(
        &self,
        event_index: u64,
        event: &BlobEvent,
    ) -> Result<(), TypedStoreError> {
        if let NodeStatus::RecoveryCatchUpWithIncompleteHistory { epoch_at_start, .. } =
            self.node_status()?
        {
            self.blob_info
                .update_blob_info_during_recovery_with_incomplete_history(
                    event_index,
                    event,
                    epoch_at_start,
                )
        } else {
            self.blob_info.update_blob_info(event_index, event)
        }
    }

    /// Repositions the event cursor to the specified event index.
    pub(crate) fn reposition_event_cursor(
        &self,
        event_index: u64,
        cursor: EventID,
    ) -> Result<(), TypedStoreError> {
        self.event_cursor
            .reposition_event_cursor(cursor, event_index)
    }

    /// Advances the event cursor to the most recent, sequential event observed.
    ///
    /// The `event_index` is a sequential index following the order in which cursors were observed
    /// from the chain starting with 0 for the very first event of the package.
    ///
    /// For calls to this function such as `(0, cursor0), (2, cursor2), (1, cursor1)`, the cursor
    /// will advance to `cursor0` after the first call since it is the first in next in the
    /// sequence; will remain at `cursor0` after the next call since `cursor2` is not the next in
    /// sequence; and will advance to cursor2 after the 3rd call, since `cursor1` fills the gap as
    /// identified by its sequence number.
    #[tracing::instrument(skip_all)]
    pub(crate) fn maybe_advance_event_cursor(
        &self,
        event_index: u64,
        cursor: &EventID,
    ) -> Result<EventProgress, TypedStoreError> {
        self.event_cursor
            .maybe_advance_event_cursor(event_index, cursor)
    }

    pub(crate) fn get_sequentially_processed_event_count(&self) -> Result<u64, TypedStoreError> {
        self.event_cursor.get_sequentially_processed_event_count()
    }

    /// Returns true if the metadata for the specified blob is stored.
    #[tracing::instrument(skip_all)]
    pub fn has_metadata(&self, blob_id: &BlobId) -> Result<bool, TypedStoreError> {
        Ok(self
            .get_blob_info(blob_id)?
            .as_ref()
            .map(BlobInfo::is_metadata_stored)
            .unwrap_or_default())
    }

    /// Gets the metadata for a given [`BlobId`] or None.
    #[tracing::instrument(skip_all)]
    pub fn get_metadata(
        &self,
        blob_id: &BlobId,
    ) -> Result<Option<VerifiedBlobMetadataWithId>, TypedStoreError> {
        let start = Instant::now();
        let labels = Labels {
            collection_name: metadata_cf_name(),
            operation_name: OperationType::Get,
            query_summary: "GET metadata BY blob_id",
            ..Labels::default()
        };

        let response = self.metadata.get(blob_id);

        self.metrics
            .observe_operation_duration(labels.with_response(response.as_ref()), start.elapsed());

        Ok(response?
            .map(|inner| VerifiedBlobMetadataWithId::new_verified_unchecked(*blob_id, inner)))
    }

    /// Deletes the metadata and slivers for the provided [`BlobId`] from the storage.
    ///
    /// This *does not* update the blob-info table in any way.
    #[tracing::instrument(skip_all)]
    pub async fn delete_blob_data(&self, blob_id: &BlobId) -> Result<(), TypedStoreError> {
        let mut batch = self.metadata.batch();
        self.delete_metadata(&mut batch, blob_id, false)?;
        self.delete_slivers(&mut batch, blob_id).await?;
        batch.write()?;
        Ok(())
    }

    /// Deletes the metadata for the provided [`BlobId`].
    fn delete_metadata(
        &self,
        batch: &mut DBBatch,
        blob_id: &BlobId,
        update_blob_info: bool,
    ) -> Result<(), TypedStoreError> {
        batch.delete_batch(&self.metadata, [blob_id])?;
        if update_blob_info {
            self.blob_info.set_metadata_stored(batch, blob_id, false)?;
        }
        Ok(())
    }

    /// Deletes the slivers on all shards for the provided [`BlobId`].
    async fn delete_slivers(
        &self,
        batch: &mut DBBatch,
        blob_id: &BlobId,
    ) -> Result<(), TypedStoreError> {
        for shard in self.existing_shard_storages().await {
            shard.delete_sliver_pair(batch, blob_id)?;
        }
        Ok(())
    }

    /// Returns true if the provided blob-id is stored at the specified shard.
    #[tracing::instrument(skip_all)]
    pub async fn is_stored_at_shard(
        &self,
        blob_id: &BlobId,
        shard: ShardIndex,
    ) -> anyhow::Result<bool> {
        Ok(self
            .shard_storage(shard)
            .await
            .ok_or(anyhow::anyhow!("shard {shard} does not exist"))?
            .is_sliver_pair_stored(blob_id)?)
    }

    /// Returns a list of identifiers of the shards that store their
    /// respective sliver for the specified blob.
    pub async fn shards_with_sliver_pairs(
        &self,
        blob_id: &BlobId,
    ) -> Result<Vec<ShardIndex>, TypedStoreError> {
        let shard_map = self.shards.read().await;
        let mut shards_with_sliver_pairs = Vec::with_capacity(shard_map.len());

        for shard in shard_map.values() {
            if shard.is_sliver_pair_stored(blob_id)? {
                shards_with_sliver_pairs.push(shard.id());
            }
        }

        Ok(shards_with_sliver_pairs)
    }

    /// Returns the shards currently present in the storage.
    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn shards_present(&self) -> Vec<ShardIndex> {
        self.shards.read().await.keys().copied().collect()
    }

    /// Handles a sync shard request. The validity of the request should be checked before calling
    /// this function.
    pub async fn handle_sync_shard_request(
        &self,
        request: &SyncShardRequest,
        current_epoch: Epoch,
    ) -> Result<SyncShardResponse, SyncShardServiceError> {
        #[cfg(msim)]
        {
            let mut return_error = false;
            sui_macros::fail_point_if!("fail_point_sync_shard_return_error", || return_error =
                true);
            if return_error {
                return Err(SyncShardServiceError::Internal(anyhow::anyhow!(
                    "sync shard request failed"
                )));
            }
        }

        let Some(shard) = self.shard_storage(request.shard_index()).await else {
            return Err(ShardNotAssigned(request.shard_index(), current_epoch).into());
        };

        let sliver_count = request.sliver_count();
        let mut fetched_blobs = Vec::with_capacity(sliver_count);
        let mut last_fetched_blob_id = None;
        while fetched_blobs.len() < sliver_count {
            let remaining_count = sliver_count - fetched_blobs.len();

            // Set starting point - either the initial request start or after last fetched blob
            let starting_blob_id_bound =
                last_fetched_blob_id.map_or(Included(request.starting_blob_id()), Excluded);

            // Scan certified slivers to fetch.
            let blobs_to_fetch = self
                .blob_info
                .certified_blob_info_iter_before_epoch(current_epoch, starting_blob_id_bound)
                .take(remaining_count)
                .map_ok(|(blob_id, _)| blob_id)
                .collect::<Result<Vec<_>, TypedStoreError>>()?;

            if blobs_to_fetch.is_empty() {
                // No more blobs to fetch.
                break;
            }

            // Update last fetched ID for next iteration
            last_fetched_blob_id = blobs_to_fetch.last().cloned();

            let mut slivers = shard.fetch_slivers(request.sliver_type(), &blobs_to_fetch)?;
            fetched_blobs.append(&mut slivers);
        }

        Ok(fetched_blobs.into())
    }

    /// Returns an iterator over the certified blob info before the specified epoch.
    pub(crate) fn certified_blob_info_iter_before_epoch(
        &self,
        epoch: Epoch,
    ) -> BlobInfoIterator<'_> {
        self.blob_info
            .certified_blob_info_iter_before_epoch(epoch, std::ops::Bound::Unbounded)
    }

    /// Returns an iterator over the certified per-object blob info before the specified epoch.
    pub(crate) fn certified_per_object_blob_info_iter_before_epoch(
        &self,
        epoch: Epoch,
    ) -> PerObjectBlobInfoIterator<'_> {
        self.blob_info
            .certified_per_object_blob_info_iter_before_epoch(epoch, std::ops::Bound::Unbounded)
    }

    /// Returns the current event cursor.
    pub(crate) fn get_event_cursor_progress(&self) -> Result<EventProgress, TypedStoreError> {
        self.event_cursor.get_event_cursor_progress()
    }

    /// Returns the latest event index that has been handled by the node.
    pub(crate) fn get_latest_handled_event_index(&self) -> Result<u64, TypedStoreError> {
        self.blob_info.get_latest_handled_event_index()
    }

    /// Clears the metadata in the storage for testing purposes.
    #[cfg(test)]
    pub fn clear_metadata_in_test(&self) -> Result<(), TypedStoreError> {
        tracing::info!("clear metadata in test");
        self.metadata.schedule_delete_all()?;
        self.metadata.flush()?;
        self.metadata
            .compact_range(&BlobId([0; 32]), &BlobId([255; 32]))?;
        Ok(())
    }

    /// Test utility to get the shards that are live on the node.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn existing_shards_live(&self) -> Vec<ShardIndex> {
        let shards = self.shards.read().await;
        let mut result = Vec::new();

        for shard_storage in shards.values() {
            if let Ok(status) = shard_storage.status().await
                && status.is_owned_by_node()
            {
                result.push(shard_storage.id());
            }
        }

        result
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::ops::Bound::{Excluded, Unbounded};

    use blob_info::{
        BlobCertificationStatus,
        BlobInfoMergeOperand,
        BlobInfoV1,
        BlobStatusChangeType,
        PermanentBlobInfoV1,
        ValidBlobInfoV1,
    };
    use constants::{
        pending_recover_slivers_column_family_name,
        primary_slivers_column_family_name,
        secondary_slivers_column_family_name,
        shard_status_column_family_name,
        shard_sync_progress_column_family_name,
    };
    use tempfile::TempDir;
    use tokio::runtime::Runtime;
    use walrus_core::{
        Sliver,
        SliverIndex,
        SliverType,
        SuiObjectId,
        encoding::{EncodingAxis, SliverData},
    };
    use walrus_sui::{
        test_utils::{EventForTesting, event_id_for_testing},
        types::{BlobCertified, BlobRegistered},
    };
    use walrus_test_utils::{Result as TestResult, WithTempDir, async_param_test};

    use super::*;
    use crate::test_utils::empty_storage_with_shards;

    type StorageSpec<'a> = &'a [(ShardIndex, Vec<(BlobId, WhichSlivers)>)];

    pub(crate) enum WhichSlivers {
        Primary,
        Secondary,
        Both,
    }

    pub(crate) const BLOB_ID: BlobId = BlobId([7; 32]);
    pub(crate) const SHARD_INDEX: ShardIndex = ShardIndex(3);
    pub(crate) const OTHER_SHARD_INDEX: ShardIndex = ShardIndex(9);

    /// Returns an empty storage, with the column families for [`SHARD_INDEX`] already created.
    pub(crate) async fn empty_storage() -> WithTempDir<Storage> {
        typed_store::metrics::DBMetrics::init(&prometheus::Registry::default());
        empty_storage_with_shards(&[SHARD_INDEX]).await
    }

    pub(crate) fn get_typed_sliver<E: EncodingAxis>(seed: u8) -> SliverData<E> {
        SliverData::new(
            vec![seed; usize::from(seed) * 512],
            16.try_into().unwrap(),
            SliverIndex(0),
        )
    }

    pub(crate) fn get_sliver(sliver_type: SliverType, seed: u8) -> Sliver {
        match sliver_type {
            SliverType::Primary => Sliver::Primary(get_typed_sliver(seed)),
            SliverType::Secondary => Sliver::Secondary(get_typed_sliver(seed)),
        }
    }

    pub(crate) async fn populated_storage(
        spec: StorageSpec<'_>,
    ) -> TestResult<WithTempDir<Storage>> {
        let mut storage = empty_storage().await;

        let mut seed = 10u8;
        for (shard, sliver_list) in spec {
            // TODO: call create storage once with the list of storages.
            storage
                .as_mut()
                .create_storage_for_shards(&[*shard])
                .await?;
            let shard_storage = storage
                .as_ref()
                .shard_storage(*shard)
                .await
                .expect("shard storage should be created");

            for (blob_id, which) in sliver_list.iter() {
                if matches!(*which, WhichSlivers::Primary | WhichSlivers::Both) {
                    shard_storage
                        .put_sliver(*blob_id, get_sliver(SliverType::Primary, seed))
                        .await?;
                    seed += 1;
                }
                if matches!(*which, WhichSlivers::Secondary | WhichSlivers::Both) {
                    shard_storage
                        .put_sliver(*blob_id, get_sliver(SliverType::Secondary, seed))
                        .await?;
                    seed += 1;
                }
            }
        }

        Ok(storage)
    }

    #[tokio::test]
    async fn can_write_then_read_metadata() -> TestResult {
        let storage = empty_storage().await;
        let storage = storage.as_ref();
        let metadata = walrus_core::test_utils::verified_blob_metadata();
        let blob_id = metadata.blob_id();
        let expected = VerifiedBlobMetadataWithId::new_verified_unchecked(
            *blob_id,
            metadata.metadata().clone(),
        );

        storage.update_blob_info(0, &BlobCertified::for_testing(*blob_id).into())?;

        storage
            .put_metadata(metadata.blob_id(), metadata.metadata())
            .await?;
        let retrieved = storage.get_metadata(blob_id)?;

        assert_eq!(retrieved, Some(expected));

        Ok(())
    }

    #[tokio::test]
    async fn stores_and_deletes_metadata() -> TestResult {
        let storage = empty_storage().await;
        let storage = storage.as_ref();
        let metadata = walrus_core::test_utils::verified_blob_metadata();
        let blob_id = metadata.blob_id();

        storage.update_blob_info(0, &BlobRegistered::for_testing(*blob_id).into())?;
        storage.update_blob_info(1, &BlobCertified::for_testing(*blob_id).into())?;

        storage
            .put_metadata(metadata.blob_id(), metadata.metadata())
            .await?;

        assert!(storage.has_metadata(blob_id)?);
        assert!(storage.get_metadata(blob_id)?.is_some());

        let mut batch = storage.metadata.batch();
        storage.delete_metadata(&mut batch, blob_id, true)?;
        batch.write()?;

        assert!(!storage.has_metadata(blob_id)?);
        assert!(storage.get_metadata(blob_id)?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn delete_on_empty_metadata_does_not_error() -> TestResult {
        let storage = empty_storage().await;
        let storage = storage.as_ref();

        let mut batch = storage.metadata.batch();
        storage
            .delete_metadata(&mut batch, &BLOB_ID, true)
            .expect("delete on empty metadata should not error");
        batch.write()?;
        Ok(())
    }

    async_param_test! {
        update_blob_info -> TestResult: [
            in_order: (false),
            skip_certify: (true),
        ]
    }
    async fn update_blob_info(skip_certify: bool) -> TestResult {
        let storage = empty_storage().await;
        let storage = storage.as_ref();
        let blob_id = BLOB_ID;

        let registered_epoch = Some(1);
        let registered_event = event_id_for_testing();
        println!("registered event: {registered_event:?}");
        let state0 = BlobInfo::new_for_testing(
            42,
            BlobCertificationStatus::Registered,
            registered_event,
            Some(1),
            None,
            None,
        );
        storage.blob_info.merge_blob_info(
            &blob_id,
            &BlobInfoMergeOperand::new_change_for_testing(
                BlobStatusChangeType::Register,
                false,
                1,
                42,
                registered_event,
            ),
        )?;
        assert_eq!(storage.get_blob_info(&blob_id)?, Some(state0));

        let certified_epoch = if skip_certify { None } else { Some(2) };
        let certified_event = event_id_for_testing();
        println!("certified event: {certified_event:?}");
        if !skip_certify {
            let mut state1 = BlobInfo::new_for_testing(
                42,
                BlobCertificationStatus::Certified,
                certified_event,
                registered_epoch,
                certified_epoch,
                None,
            );

            // Set correct registered event.
            let BlobInfo::V1(BlobInfoV1::Valid(ValidBlobInfoV1 {
                permanent_total: Some(PermanentBlobInfoV1 { event, .. }),
                ..
            })) = &mut state1
            else {
                panic!()
            };
            *event = registered_event;

            storage.blob_info.merge_blob_info(
                &blob_id,
                &BlobInfoMergeOperand::new_change_for_testing(
                    BlobStatusChangeType::Certify,
                    false,
                    2,
                    42,
                    certified_event,
                ),
            )?;
            assert_eq!(storage.get_blob_info(&blob_id)?, Some(state1));
        }

        let event = event_id_for_testing();
        let state2 = BlobInfo::new_for_testing(
            42,
            BlobCertificationStatus::Invalid,
            event,
            registered_epoch,
            certified_epoch,
            Some(3),
        );
        storage.blob_info.merge_blob_info(
            &blob_id,
            &BlobInfoMergeOperand::MarkInvalid {
                epoch: 3,
                status_event: event,
            },
        )?;
        assert_eq!(storage.get_blob_info(&blob_id)?, Some(state2));
        Ok(())
    }

    #[tokio::test]
    async fn update_blob_info_metadata_stored() -> TestResult {
        let storage = empty_storage().await;
        let storage = storage.as_ref();
        let blob_id = BLOB_ID;

        let event = event_id_for_testing();
        let state0 = BlobInfo::new_for_testing(
            42,
            BlobCertificationStatus::Registered,
            event,
            Some(1),
            None,
            None,
        );

        storage.blob_info.merge_blob_info(
            &blob_id,
            &BlobInfoMergeOperand::new_change_for_testing(
                BlobStatusChangeType::Register,
                false,
                1,
                42,
                event,
            ),
        )?;
        assert_eq!(storage.get_blob_info(&blob_id)?, Some(state0.clone()));

        let mut state1 = state0.clone();
        let BlobInfo::V1(BlobInfoV1::Valid(ValidBlobInfoV1 {
            is_metadata_stored, ..
        })) = &mut state1
        else {
            panic!()
        };
        *is_metadata_stored = true;

        storage
            .blob_info
            .merge_blob_info(&blob_id, &BlobInfoMergeOperand::MarkMetadataStored(true))?;
        assert_eq!(storage.get_blob_info(&blob_id)?, Some(state1));

        Ok(())
    }

    async_param_test! {
        maybe_advance_event_cursor_order -> TestResult: [
            in_order: (&[0, 1, 2], &[0, 1, 2]),
            out_of_order: (&[0, 3, 2, 1], &[0, 0, 0, 3]),
        ]
    }
    async fn maybe_advance_event_cursor_order(
        sequence_ids: &[u64],
        expected_sequence: &[u64],
    ) -> TestResult {
        let storage = empty_storage().await;
        let storage = storage.as_ref();

        let cursors: Vec<_> = sequence_ids
            .iter()
            .map(|&seq_id| (seq_id, event_id_for_testing()))
            .collect();
        let cursor_lookup: HashMap<_, _> = cursors.clone().into_iter().collect();

        for ((seq_id, cursor), expected_observed) in cursors.iter().zip(expected_sequence) {
            storage.maybe_advance_event_cursor(*seq_id, cursor)?;

            assert_eq!(
                storage
                    .get_event_cursor_and_next_index()?
                    .map(|e| e.event_id()),
                Some(cursor_lookup[expected_observed])
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn maybe_advance_event_cursor_missed_zero() -> TestResult {
        let storage = empty_storage().await;
        let storage = storage.as_ref();

        storage.maybe_advance_event_cursor(1, &event_id_for_testing())?;
        assert_eq!(storage.get_event_cursor_and_next_index()?, None);

        Ok(())
    }

    mod shards_with_sliver_pairs {
        use walrus_test_utils::async_param_test;

        use super::*;

        async_param_test! {
            returns_shard_if_it_stores_both -> TestResult: [
                both: (WhichSlivers::Both, true),
                only_primary: (WhichSlivers::Primary, false),
                only_secondary: (WhichSlivers::Secondary, false),
            ]
        }
        async fn returns_shard_if_it_stores_both(
            which: WhichSlivers,
            is_retrieved: bool,
        ) -> TestResult {
            let storage = populated_storage(&[(SHARD_INDEX, vec![(BLOB_ID, which)])]).await?;

            let result: Vec<_> = storage.as_ref().shards_with_sliver_pairs(&BLOB_ID).await?;

            if is_retrieved {
                assert_eq!(result, &[SHARD_INDEX]);
            } else {
                assert!(result.is_empty());
            }

            Ok(())
        }

        #[tokio::test]
        async fn identifies_all_shards_storing_sliver_pairs() -> TestResult {
            let storage = populated_storage(&[
                (SHARD_INDEX, vec![(BLOB_ID, WhichSlivers::Both)]),
                (OTHER_SHARD_INDEX, vec![(BLOB_ID, WhichSlivers::Both)]),
            ])
            .await?;

            let mut result: Vec<_> = storage.as_ref().shards_with_sliver_pairs(&BLOB_ID).await?;

            result.sort();

            assert_eq!(result, [SHARD_INDEX, OTHER_SHARD_INDEX]);

            Ok(())
        }

        #[tokio::test]
        async fn ignores_shards_without_both_sliver_pairs() -> TestResult {
            let storage = populated_storage(&[
                (SHARD_INDEX, vec![(BLOB_ID, WhichSlivers::Primary)]),
                (OTHER_SHARD_INDEX, vec![(BLOB_ID, WhichSlivers::Both)]),
            ])
            .await?;

            let result: Vec<_> = storage.as_ref().shards_with_sliver_pairs(&BLOB_ID).await?;

            assert_eq!(result, [OTHER_SHARD_INDEX]);

            Ok(())
        }
    }

    /// Open and populate the storage, optionally lock a shard, then close the storage.
    ///
    /// Runs in its own runtime to ensure that all tasked spawned by typed_store
    /// are dropped to free the storage lock.
    #[tokio::main(flavor = "current_thread")]
    async fn populate_storage_then_close(
        spec: StorageSpec,
        lock_shard: Option<ShardIndex>,
    ) -> TestResult<TempDir> {
        let storage = populated_storage(spec).await?;
        if let Some(shard) = lock_shard {
            storage
                .inner
                .shard_storage(shard)
                .await
                .expect("shard should be created")
                .lock_shard_for_epoch_change()
                .await
                .expect("shard should be sealed");
        }
        Ok(storage.temp_dir)
    }

    #[test]
    #[cfg_attr(msim, ignore)]
    fn can_reopen_storage_with_shards_and_access_data() -> TestResult {
        let directory = populate_storage_then_close(
            &[
                (SHARD_INDEX, vec![(BLOB_ID, WhichSlivers::Both)]),
                (OTHER_SHARD_INDEX, vec![(BLOB_ID, WhichSlivers::Both)]),
            ],
            None,
        )?;

        Runtime::new()?.block_on(async move {
            let storage = Storage::open(
                directory.path(),
                DatabaseConfig::default(),
                MetricConf::default(),
                Registry::default(),
            )?;

            for shard_id in [SHARD_INDEX, OTHER_SHARD_INDEX] {
                let Some(shard) = storage.shard_storage(shard_id).await else {
                    panic!("shard {shard_id} should exist");
                };

                for sliver_type in [SliverType::Primary, SliverType::Secondary] {
                    let _ = shard
                        .get_sliver(&BLOB_ID, sliver_type)
                        .expect("sliver lookup should not err")
                        .expect("sliver should be present");
                }
            }

            Result::<(), anyhow::Error>::Ok(())
        })?;

        Ok(())
    }

    // Tests that shard status can be restored upon restart.
    #[test]
    #[cfg_attr(msim, ignore)]
    fn can_reopen_storage_with_shards_status() -> TestResult {
        let directory = populate_storage_then_close(
            &[
                (SHARD_INDEX, vec![(BLOB_ID, WhichSlivers::Both)]),
                (OTHER_SHARD_INDEX, vec![(BLOB_ID, WhichSlivers::Both)]),
            ],
            Some(SHARD_INDEX), // Lock SHARD_INDEX
        )?;

        Runtime::new()?.block_on(async move {
            let storage = Storage::open(
                directory.path(),
                DatabaseConfig::default(),
                MetricConf::default(),
                Registry::default(),
            )?;

            // Check that the shard status is restored correctly.
            assert_eq!(
                storage
                    .shard_storage(SHARD_INDEX)
                    .await
                    .expect("shard should exist")
                    .status()
                    .await
                    .expect("status should be present"),
                ShardStatus::LockedToMove
            );

            assert_eq!(
                storage
                    .shard_storage(OTHER_SHARD_INDEX)
                    .await
                    .expect("shard should exist")
                    .status()
                    .await
                    .expect("status should be present"),
                ShardStatus::None
            );

            Result::<(), anyhow::Error>::Ok(())
        })?;

        Ok(())
    }

    #[tokio::test]
    async fn reopen_partially_created_sliver_column_family() -> TestResult {
        let test_shard_index = ShardIndex(123);
        let storage = empty_storage().await;

        let primary_cfs_name = primary_slivers_column_family_name(test_shard_index);
        let primary_cfs_options = primary_slivers_column_family_options(&DatabaseConfig::default());

        let secondary_cfs_name = secondary_slivers_column_family_name(test_shard_index);
        let secondary_cfs_options =
            secondary_slivers_column_family_options(&DatabaseConfig::default());

        let status_cfs_name = shard_status_column_family_name(test_shard_index);
        let status_cfs = shard_status_column_family_options(&DatabaseConfig::default());

        let sync_progress_cfs_name = shard_sync_progress_column_family_name(test_shard_index);
        let sync_progress_cfs =
            shard_sync_progress_column_family_options(&DatabaseConfig::default());

        let pending_recover_cfs_name = pending_recover_slivers_column_family_name(test_shard_index);
        let pending_recover_cfs =
            pending_recover_slivers_column_family_options(&DatabaseConfig::default());

        // Create all but secondary sliver column family. When restarting the storage, the
        // shard should not be detected as existing.
        storage
            .inner
            .database
            .create_cf(&primary_cfs_name, &primary_cfs_options)?;
        storage
            .inner
            .database
            .create_cf(&status_cfs_name, &status_cfs)?;
        storage
            .inner
            .database
            .create_cf(&sync_progress_cfs_name, &sync_progress_cfs)?;
        storage
            .inner
            .database
            .create_cf(&pending_recover_cfs_name, &pending_recover_cfs)?;
        assert!(
            !ShardStorage::existing_cf_shards_ids(storage.temp_dir.path(), &Options::default())
                .contains(&test_shard_index)
        );

        // Create the column family for the secondary sliver. When restarting the storage, the shard
        // should now be detected as existing.
        storage
            .inner
            .database
            .create_cf(&secondary_cfs_name, &secondary_cfs_options)?;
        assert!(
            ShardStorage::existing_cf_shards_ids(storage.temp_dir.path(), &Options::default())
                .contains(&test_shard_index)
        );

        Ok(())
    }

    fn check_cf_existence(db: Arc<RocksDB>, exists: bool) {
        for cf in [
            &primary_slivers_column_family_name(SHARD_INDEX),
            &secondary_slivers_column_family_name(SHARD_INDEX),
            &shard_status_column_family_name(SHARD_INDEX),
            &shard_sync_progress_column_family_name(SHARD_INDEX),
            &pending_recover_slivers_column_family_name(SHARD_INDEX),
        ] {
            if exists {
                assert!(db.cf_handle(cf).is_some());
            } else {
                assert!(db.cf_handle(cf).is_none());
            }
        }
    }

    #[test]
    #[cfg_attr(msim, ignore)]
    fn test_remove_shard_cf() -> TestResult {
        let directory = populate_storage_then_close(
            &[
                (SHARD_INDEX, vec![(BLOB_ID, WhichSlivers::Both)]),
                (OTHER_SHARD_INDEX, vec![(BLOB_ID, WhichSlivers::Both)]),
            ],
            None,
        )?;

        let path_clone = directory.path().to_path_buf();

        Runtime::new()?.block_on(async move {
            let storage = Storage::open(
                path_clone.as_path(),
                DatabaseConfig::default(),
                MetricConf::default(),
                Registry::default(),
            )?;

            // Check shard files exist.
            check_cf_existence(storage.database.clone(), true);

            storage.remove_storage_for_shards(&[SHARD_INDEX]).await?;

            // Check shard file does not exist.
            check_cf_existence(storage.database.clone(), false);

            Result::<(), anyhow::Error>::Ok(())
        })?;

        Runtime::new()?.block_on(async move {
            let storage = Storage::open(
                directory.path(),
                DatabaseConfig::default(),
                MetricConf::default(),
                Registry::default(),
            )?;

            // Reload storage and the shard should not exist.
            check_cf_existence(storage.database.clone(), false);

            // Remove it again should not encounter any error.
            storage.remove_storage_for_shards(&[SHARD_INDEX]).await?;

            Result::<(), anyhow::Error>::Ok(())
        })?;

        Ok(())
    }

    async_param_test! {
        handle_sync_shard_request_behave_expected -> TestResult: [
            scan_first: (SliverType::Primary, ShardIndex(3), 1, 1, &[1]),
            scan_all: (SliverType::Primary, ShardIndex(5), 1, 10, &[1, 2, 3, 8, 9, 10]),
            scan_tail: (SliverType::Primary, ShardIndex(3), 3, 10, &[3, 8, 9, 10]),
            scan_head: (SliverType::Primary, ShardIndex(5), 0, 2, &[1, 2]),
            scan_middle_single: (SliverType::Secondary, ShardIndex(5), 3, 1, &[3]),
            scan_middle_range: (SliverType::Secondary, ShardIndex(3), 2, 2, &[2, 3]),
            scan_end_over: (SliverType::Secondary, ShardIndex(5), 3, 20, &[3, 8, 9, 10]),
            scan_all_wide_range:
                (SliverType::Secondary, ShardIndex(3), 0, 100, &[1, 2, 3, 8, 9, 10]),
            scan_out_of_range: (SliverType::Secondary, ShardIndex(5), 11, 2, &[]),

            scan_containing_non_certified: (SliverType::Secondary, ShardIndex(3), 3, 2, &[3, 8]),
            scan_start_at_non_certified: (SliverType::Secondary, ShardIndex(3), 4, 2, &[8, 9]),
        ]
    }
    async fn handle_sync_shard_request_behave_expected(
        sliver_type: SliverType,
        shard_index: ShardIndex,
        start_blob_index: u8,
        count: u64,
        expected_blob_index_in_response: &[u8],
    ) -> TestResult {
        let mut storage = empty_storage().await;

        // All tests use the same setup:
        // - 2 shards: 3 and 5
        // - 10 blobs: blob 4 and 5 are expired, and blob 6 and 7 do not have slivers stored.
        // - 2 slivers per blob: primary and secondary

        // Create test data structure to track expected slivers
        let mut data: HashMap<ShardIndex, HashMap<BlobId, HashMap<SliverType, Sliver>>> =
            HashMap::new();
        let mut seed = 10u8;

        // Create test blob IDs
        let blob_ids: Vec<_> = (1..=10).map(|i| BlobId([i; 32])).collect();

        // Initialize storage with two shards
        let shards = [ShardIndex(3), ShardIndex(5)];
        storage.as_mut().create_storage_for_shards(&shards).await?;

        // Populate shards with slivers
        for shard in shards {
            let shard_storage = storage
                .as_ref()
                .shard_storage(shard)
                .await
                .expect("shard should exist");
            data.insert(shard, HashMap::new());

            for (index, blob_id) in blob_ids.iter().enumerate() {
                data.get_mut(&shard)
                    .unwrap()
                    .insert(*blob_id, HashMap::new());

                // Create and store both primary and secondary slivers
                for sliver_type in [SliverType::Primary, SliverType::Secondary] {
                    let sliver_data = get_sliver(sliver_type, seed);
                    seed += 1;

                    data.get_mut(&shard)
                        .unwrap()
                        .get_mut(blob_id)
                        .unwrap()
                        .insert(sliver_type, sliver_data.clone());

                    // Only store slivers for certain indices. This tests that
                    // handle_sync_shard_request should return the count of number of slivers
                    // corresponding to the request. If some blobs are certified, but the slivers
                    // are not stored, handle_sync_shard_request should continue getting following
                    // slivers until the count is reached.
                    if !(5..=6).contains(&index) {
                        shard_storage.put_sliver(*blob_id, sliver_data).await?;
                    }
                }
            }
        }

        // Register and certify blobs with appropriate epochs
        for (index, blob_id) in blob_ids.iter().enumerate() {
            let end_epoch = if !(3..=4).contains(&index) { 3 } else { 1 };

            // Register blob
            storage.as_mut().blob_info.merge_blob_info(
                blob_id,
                &BlobInfoMergeOperand::new_change_for_testing(
                    BlobStatusChangeType::Register,
                    false,
                    0,
                    end_epoch,
                    event_id_for_testing(),
                ),
            )?;

            // Certify blob
            storage.as_mut().blob_info.merge_blob_info(
                blob_id,
                &BlobInfoMergeOperand::new_change_for_testing(
                    BlobStatusChangeType::Certify,
                    false,
                    0,
                    end_epoch,
                    event_id_for_testing(),
                ),
            )?;
        }

        // Create and execute sync request
        let request = SyncShardRequest::new(
            shard_index,
            sliver_type,
            BlobId([start_blob_index; 32]),
            count,
            2,
        );
        let SyncShardResponse::V1(slivers) = storage
            .as_ref()
            .handle_sync_shard_request(&request, 2)
            .await?;

        // Verify response matches expected
        let expected_response = expected_blob_index_in_response
            .iter()
            .map(|blob_index| {
                (
                    BlobId([*blob_index; 32]),
                    data[&shard_index][&BlobId([*blob_index; 32])][&sliver_type].clone(),
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(slivers, expected_response);
        Ok(())
    }

    /// Tests that the storage returns correct error when trying to sync a shard that does not
    /// exist.
    #[tokio::test]
    async fn handle_sync_shard_request_shard_not_found() -> TestResult {
        let storage = empty_storage().await;

        let request =
            SyncShardRequest::new(ShardIndex(123), SliverType::Primary, BlobId([1; 32]), 1, 1);
        let response = storage
            .as_ref()
            .handle_sync_shard_request(&request, 0)
            .await
            .unwrap_err();

        assert!(matches!(
            response,
            SyncShardServiceError::ShardNotAssigned(..)
        ));

        Ok(())
    }

    fn registered_blob_info(epoch: Epoch) -> BlobInfo {
        BlobInfo::new_for_testing(
            100,
            BlobCertificationStatus::Registered,
            event_id_for_testing(),
            Some(epoch),
            None,
            None,
        )
    }

    fn certified_blob_info(epoch: Epoch) -> BlobInfo {
        BlobInfo::new_for_testing(
            100,
            BlobCertificationStatus::Certified,
            event_id_for_testing(),
            Some(epoch),
            Some(epoch),
            None,
        )
    }

    fn invalid_blob_info(epoch: Epoch) -> BlobInfo {
        BlobInfo::new_for_testing(
            100,
            BlobCertificationStatus::Invalid,
            event_id_for_testing(),
            Some(epoch),
            Some(epoch),
            Some(epoch),
        )
    }

    fn registered_per_object_blob_info(blob_id: BlobId, epoch: Epoch) -> PerObjectBlobInfo {
        PerObjectBlobInfo::new_for_testing(
            blob_id,
            epoch,
            None,
            100,
            true,
            event_id_for_testing(),
            false,
        )
    }

    fn certified_per_object_blob_info(
        blob_id: BlobId,
        certified_epoch: Epoch,
        end_epoch: Epoch,
        deleted: bool,
    ) -> PerObjectBlobInfo {
        PerObjectBlobInfo::new_for_testing(
            blob_id,
            1,
            Some(certified_epoch),
            end_epoch,
            true,
            event_id_for_testing(),
            deleted,
        )
    }

    fn all_certified_blob_ids(
        storage: &WithTempDir<Storage>,
        after_blob: Option<BlobId>,
        new_epoch: Epoch,
    ) -> Result<Vec<BlobId>, TypedStoreError> {
        storage
            .inner
            .blob_info
            .certified_blob_info_iter_before_epoch(
                new_epoch,
                after_blob.map_or(Unbounded, Excluded),
            )
            .map(|result| result.map(|(id, _info)| id))
            .collect::<Result<Vec<_>, _>>()
    }

    fn all_certified_blob_object_ids(
        storage: &WithTempDir<Storage>,
        new_epoch: Epoch,
    ) -> Result<Vec<ObjectID>, TypedStoreError> {
        storage
            .inner
            .blob_info
            .certified_per_object_blob_info_iter_before_epoch(new_epoch, Unbounded)
            .map(|result| result.map(|(id, _info)| id))
            .collect::<Result<Vec<_>, _>>()
    }

    #[tokio::test]
    async fn test_certified_blob_info_iter_before_epoch() -> TestResult {
        let storage = empty_storage().await;
        let blob_info = storage.inner.blob_info.clone();
        let new_epoch = 3;

        let blob_ids = [
            BlobId([0; 32]), // Not certified.
            BlobId([1; 32]), // Not exist.
            BlobId([2; 32]), // Certified within epoch 2
            BlobId([3; 32]), // Certified after epoch 2
            BlobId([4; 32]), // Invalid
            BlobId([5; 32]), // Certified within epoch 2
            BlobId([6; 32]), // Not exist.
        ];

        let blob_info_map = HashMap::from([
            (blob_ids[0], registered_blob_info(1)),
            (blob_ids[2], certified_blob_info(2)),
            (blob_ids[3], certified_blob_info(3)),
            (blob_ids[4], invalid_blob_info(2)),
            (blob_ids[5], certified_blob_info(2)),
        ]);

        let mut batch = blob_info.batch();
        blob_info.insert_batch(&mut batch, blob_info_map.iter())?;
        batch.write()?;

        assert_eq!(
            all_certified_blob_ids(&storage, None, new_epoch)?,
            vec![blob_ids[2], blob_ids[5]]
        );

        for blob_id in blob_ids.iter().take(2) {
            assert_eq!(
                all_certified_blob_ids(&storage, Some(*blob_id), new_epoch)?,
                vec![blob_ids[2], blob_ids[5]]
            );
        }
        for blob_id in blob_ids.iter().take(5).skip(2) {
            assert_eq!(
                all_certified_blob_ids(&storage, Some(*blob_id), new_epoch)?,
                vec![blob_ids[5]]
            );
        }
        for blob_id in blob_ids.iter().take(6).skip(5) {
            assert!(all_certified_blob_ids(&storage, Some(*blob_id), new_epoch)?.is_empty());
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_certified_per_object_blob_info_iter_before_epoch() -> TestResult {
        let storage = empty_storage().await;
        let blob_info = storage.inner.blob_info.clone();

        let blob_ids = [BlobId([0; 32]), BlobId([1; 32])];

        let object_ids = [
            SuiObjectId([0; 32]), // blob 0, not certified
            SuiObjectId([1; 32]), // blob 0, certified within epoch 2
            SuiObjectId([2; 32]), // blob 0, certified after epoch 2
            SuiObjectId([3; 32]), // blob 0, certified within epoch 2
            SuiObjectId([4; 32]), // blob 0, deleted
            SuiObjectId([5; 32]), // blob 1, certified within epoch 2
        ]
        .into_iter()
        .map(ObjectID::from)
        .collect::<Vec<_>>();

        let blob_info_map = HashMap::from([
            (
                object_ids[0],
                registered_per_object_blob_info(blob_ids[0], 1),
            ),
            (
                object_ids[1],
                certified_per_object_blob_info(blob_ids[0], 2, 100, false),
            ),
            (
                object_ids[2],
                certified_per_object_blob_info(blob_ids[0], 3, 100, false),
            ),
            (
                object_ids[3],
                certified_per_object_blob_info(blob_ids[0], 2, 4, false),
            ),
            (
                object_ids[4],
                certified_per_object_blob_info(blob_ids[0], 2, 100, true),
            ),
            (
                object_ids[5],
                certified_per_object_blob_info(blob_ids[1], 2, 4, false),
            ),
        ]);

        let mut batch = blob_info.batch();
        blob_info.insert_per_object_batch(&mut batch, blob_info_map.iter())?;
        batch.write()?;

        assert_eq!(
            all_certified_blob_object_ids(&storage, 3)?,
            vec![object_ids[1], object_ids[3], object_ids[5]]
        );

        assert_eq!(
            all_certified_blob_object_ids(&storage, 4)?,
            vec![object_ids[1], object_ids[2]]
        );

        Ok(())
    }

    // A sanity check to ensure that after certified_blob_info_iter_before_epoch is created,
    // any update to the blob info table will not affect the iterator.
    #[tokio::test]
    async fn test_certified_blob_info_iter_before_epoch_is_isolated() -> TestResult {
        let storage = empty_storage().await;
        let blob_info = storage.inner.blob_info.clone();

        let blob_ids = [
            BlobId([0; 32]),
            BlobId([1; 32]),
            BlobId([2; 32]),
            BlobId([3; 32]),
            BlobId([4; 32]),
        ];

        let blob_info_map = HashMap::from([
            (blob_ids[0], certified_blob_info(2)),
            (blob_ids[1], certified_blob_info(5)),
            (blob_ids[2], certified_blob_info(2)),
            (blob_ids[3], certified_blob_info(2)),
        ]);

        let mut batch = blob_info.batch();
        blob_info.insert_batch(&mut batch, blob_info_map.iter())?;
        batch.write()?;

        // Create the iterator, which should take the snapshot of the blob info table at the
        // creation time.
        let certified_blob_iter = storage
            .inner
            .blob_info
            .certified_blob_info_iter_before_epoch(3, Unbounded);

        // Update blob info table, and these updates should not be visible to the iterator.
        blob_info.insert(&blob_ids[4], &certified_blob_info(2))?;
        blob_info.remove(&blob_ids[0])?;

        // Check that the certified blob list matches the state of the blob info table at the
        // creation time.
        assert_eq!(
            certified_blob_iter
                .map(|result| result.map(|(id, _info)| id))
                .collect::<Result<Vec<_>, _>>()?,
            vec![blob_ids[0], blob_ids[2], blob_ids[3]]
        );

        Ok(())
    }
}

// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Walrus shard storage.

use core::fmt::{self, Display};
use std::{
    collections::{HashMap, HashSet},
    path::Path,
    sync::{Arc, OnceLock},
    time::Duration,
};

#[cfg(feature = "failure_injection")]
use anyhow::anyhow;
use fastcrypto::traits::KeyPair;
use futures::{stream::FuturesUnordered, StreamExt};
use regex::Regex;
use rocksdb::{Options, DB};
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;
use typed_store::{
    rocks::{
        be_fix_int_ser as to_rocks_db_key,
        errors::typed_store_err_from_rocks_err,
        DBBatch,
        DBMap,
        ReadWriteOptions,
        RocksDB,
    },
    Map,
    TypedStoreError,
};
use walrus_core::{
    encoding::{EncodingAxis, Primary, PrimarySliver, Secondary, SecondarySliver},
    BlobId,
    Epoch,
    ShardIndex,
    Sliver,
    SliverType,
};

use super::{blob_info::BlobInfo, BlobInfoIter, DatabaseConfig};
use crate::node::{blob_sync::recover_sliver, errors::SyncShardClientError, StorageNodeInner};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ShardStatus {
    /// Initial status of the shard when just created.
    None,

    /// The shard is active in this node serving reads and writes.
    Active,

    /// The shard is being synced to the last epoch.
    ActiveSync,

    /// The shard is being synced to the last epoch and recovering missing blobs using sliver
    /// recovery mechanism.
    ActiveRecover,

    /// The shard is locked for moving to another node. Shard does not accept any more writes in
    /// this status.
    LockedToMove,
}

impl Display for ShardStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl ShardStatus {
    pub fn is_owned_by_node(&self) -> bool {
        self != &ShardStatus::LockedToMove
    }

    /// Provides a string representation of the enum variant.
    pub fn as_str(&self) -> &'static str {
        match self {
            ShardStatus::None => "None",
            ShardStatus::Active => "Active",
            ShardStatus::ActiveSync => "ActiveSync",
            ShardStatus::ActiveRecover => "ActiveRecover",
            ShardStatus::LockedToMove => "LockedToMove",
        }
    }
}

// When syncing a shard, the task first requests the primary slivers following
// the order of the blob IDs, and then all the secondary slivers.
// This struct represents the current progress of syncing a shard.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct ShardSyncProgressV1 {
    last_synced_blob_id: BlobId,
    sliver_type: SliverType,
}

// Represents the progress of syncing a shard. It is used to resume syncing a shard
// if it was interrupted.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
enum ShardSyncProgress {
    V1(ShardSyncProgressV1),
}

impl ShardSyncProgress {
    fn new(last_synced_blob_id: BlobId, sliver_type: SliverType) -> Self {
        Self::V1(ShardSyncProgressV1 {
            last_synced_blob_id,
            sliver_type,
        })
    }
}

// Represents the last synced status of the shard after restart.
enum ShardLastSyncStatus {
    // The last synced blob ID for the primary slivers.
    // If the last_synced_blob_id is None, it means the shard has not synced any primary slivers.
    Primary { last_synced_blob_id: Option<BlobId> },
    // The last synced blob ID for the secondary slivers.
    Secondary { last_synced_blob_id: Option<BlobId> },
    // The shard is in recovery mode.
    Recovery,
}

#[derive(Debug, Clone)]
pub struct ShardStorage {
    id: ShardIndex,
    shard_status: DBMap<(), ShardStatus>,
    primary_slivers: DBMap<BlobId, PrimarySliver>,
    secondary_slivers: DBMap<BlobId, SecondarySliver>,
    shard_sync_progress: DBMap<(), ShardSyncProgress>,
    pending_recover_slivers: DBMap<(SliverType, BlobId), ()>,
}

/// Storage corresponding to a single shard.
impl ShardStorage {
    pub(crate) fn create_or_reopen(
        id: ShardIndex,
        database: &Arc<RocksDB>,
        db_config: &DatabaseConfig,
        initial_shard_status: Option<ShardStatus>,
    ) -> Result<Self, TypedStoreError> {
        let rw_options = ReadWriteOptions::default();

        // TODO: cleanup all the column faimily initiation as they are highly repetitive.
        let shard_cf_options = Self::slivers_column_family_options(id, db_config);
        for (_, (cf_name, options)) in shard_cf_options.iter() {
            if database.cf_handle(cf_name).is_none() {
                database
                    .create_cf(cf_name, options)
                    .map_err(typed_store_err_from_rocks_err)?;
            }
        }

        let shard_status_cf_name = shard_status_column_family_name(id);
        if database.cf_handle(&shard_status_cf_name).is_none() {
            let (_, options) = Self::shard_status_column_family_options(id, db_config);
            database
                .create_cf(&shard_status_cf_name, &options)
                .map_err(typed_store_err_from_rocks_err)?;
        }
        let shard_sync_progress_cf_name = shard_sync_progress_column_family_name(id);
        if database.cf_handle(&shard_sync_progress_cf_name).is_none() {
            let (_, options) = Self::shard_sync_progress_column_family_options(id, db_config);
            database
                .create_cf(&shard_sync_progress_cf_name, &options)
                .map_err(typed_store_err_from_rocks_err)?;
        }
        let pending_recover_slivers_cf_name = pending_recover_slivers_column_family_name(id);
        if database
            .cf_handle(&pending_recover_slivers_cf_name)
            .is_none()
        {
            let (_, options) = Self::pending_recover_slivers_column_family_options(id, db_config);
            database
                .create_cf(&pending_recover_slivers_cf_name, &options)
                .map_err(typed_store_err_from_rocks_err)?;
        }

        let primary_slivers = DBMap::reopen(
            database,
            Some(shard_cf_options[&SliverType::Primary].0.as_str()),
            &rw_options,
            false,
        )?;
        let secondary_slivers = DBMap::reopen(
            database,
            Some(shard_cf_options[&SliverType::Secondary].0.as_str()),
            &rw_options,
            false,
        )?;
        let shard_status =
            DBMap::reopen(database, Some(&shard_status_cf_name), &rw_options, false)?;
        let shard_sync_progress = DBMap::reopen(
            database,
            Some(&shard_sync_progress_cf_name),
            &rw_options,
            false,
        )?;
        let pending_recover_slivers = DBMap::reopen(
            database,
            Some(&pending_recover_slivers_cf_name),
            &rw_options,
            false,
        )?;

        if let Some(status) = initial_shard_status {
            shard_status.insert(&(), &status)?;
        }

        Ok(Self {
            id,
            shard_status,
            primary_slivers,
            secondary_slivers,
            shard_sync_progress,
            pending_recover_slivers,
        })
    }

    /// Stores the provided primary or secondary sliver for the given blob ID.
    #[tracing::instrument(skip_all, fields(walrus.shard_index = %self.id), err)]
    pub(crate) fn put_sliver(
        &self,
        blob_id: &BlobId,
        sliver: &Sliver,
    ) -> Result<(), TypedStoreError> {
        match sliver {
            Sliver::Primary(primary) => self.primary_slivers.insert(blob_id, primary),
            Sliver::Secondary(secondary) => self.secondary_slivers.insert(blob_id, secondary),
        }
    }

    pub(crate) fn id(&self) -> ShardIndex {
        self.id
    }

    /// Returns the sliver of the specified type that is stored for that Blob ID, if any.
    #[tracing::instrument(skip_all, fields(walrus.shard_index = %self.id), err)]
    pub(crate) fn get_sliver(
        &self,
        blob_id: &BlobId,
        sliver_type: SliverType,
    ) -> Result<Option<Sliver>, TypedStoreError> {
        match sliver_type {
            SliverType::Primary => self
                .get_primary_sliver(blob_id)
                .map(|s| s.map(Sliver::Primary)),
            SliverType::Secondary => self
                .get_secondary_sliver(blob_id)
                .map(|s| s.map(Sliver::Secondary)),
        }
    }

    /// Retrieves the stored primary sliver for the given blob ID.
    #[tracing::instrument(skip_all, fields(walrus.shard_index = %self.id), err)]
    pub(crate) fn get_primary_sliver(
        &self,
        blob_id: &BlobId,
    ) -> Result<Option<PrimarySliver>, TypedStoreError> {
        self.primary_slivers.get(blob_id)
    }

    /// Retrieves the stored secondary sliver for the given blob ID.
    #[tracing::instrument(skip_all, fields(walrus.shard_index = %self.id), err)]
    pub(crate) fn get_secondary_sliver(
        &self,
        blob_id: &BlobId,
    ) -> Result<Option<SecondarySliver>, TypedStoreError> {
        self.secondary_slivers.get(blob_id)
    }

    /// Returns true iff the sliver-pair for the given blob ID is stored by the shard.
    #[tracing::instrument(skip_all, fields(walrus.shard_index = %self.id), err)]
    pub(crate) fn is_sliver_pair_stored(&self, blob_id: &BlobId) -> Result<bool, TypedStoreError> {
        Ok(self.is_sliver_stored::<Primary>(blob_id)?
            && self.is_sliver_stored::<Secondary>(blob_id)?)
    }

    /// Deletes the sliver pair for the given [`BlobId`].
    #[tracing::instrument(skip_all, fields(walrus.shard_index = %self.id), err)]
    pub(crate) fn delete_sliver_pair(
        &self,
        batch: &mut DBBatch,
        blob_id: &BlobId,
    ) -> Result<(), TypedStoreError> {
        batch.delete_batch(&self.primary_slivers, std::iter::once(blob_id))?;
        batch.delete_batch(&self.secondary_slivers, std::iter::once(blob_id))?;
        Ok(())
    }

    #[tracing::instrument(skip_all, fields(walrus.shard_index = %self.id), err)]
    pub(crate) fn is_sliver_stored<A: EncodingAxis>(
        &self,
        blob_id: &BlobId,
    ) -> Result<bool, TypedStoreError> {
        self.is_sliver_type_stored(blob_id, A::sliver_type())
    }

    #[tracing::instrument(skip_all, fields(walrus.shard_index = %self.id), err)]
    pub(crate) fn is_sliver_type_stored(
        &self,
        blob_id: &BlobId,
        type_: SliverType,
    ) -> Result<bool, TypedStoreError> {
        match type_ {
            SliverType::Primary => self.primary_slivers.contains_key(blob_id),
            SliverType::Secondary => self.secondary_slivers.contains_key(blob_id),
        }
    }

    /// Returns the name and options for the column families for a shard's primary and secondary
    /// sliver with the specified index.
    pub(crate) fn slivers_column_family_options(
        id: ShardIndex,
        db_config: &DatabaseConfig,
    ) -> HashMap<SliverType, (String, Options)> {
        [
            (
                SliverType::Primary,
                (
                    primary_slivers_column_family_name(id),
                    db_config.shard().to_options(),
                ),
            ),
            (
                SliverType::Secondary,
                (
                    secondary_slivers_column_family_name(id),
                    db_config.shard().to_options(),
                ),
            ),
        ]
        .into()
    }

    pub(crate) fn shard_status_column_family_options(
        id: ShardIndex,
        db_config: &DatabaseConfig,
    ) -> (String, Options) {
        (
            shard_status_column_family_name(id),
            db_config.shard_status().to_options(),
        )
    }

    pub(crate) fn shard_sync_progress_column_family_options(
        id: ShardIndex,
        db_config: &DatabaseConfig,
    ) -> (String, Options) {
        (
            shard_sync_progress_column_family_name(id),
            db_config.shard_sync_progress().to_options(),
        )
    }

    pub(crate) fn pending_recover_slivers_column_family_options(
        id: ShardIndex,
        db_config: &DatabaseConfig,
    ) -> (String, Options) {
        (
            pending_recover_slivers_column_family_name(id),
            db_config.pending_recover_slivers().to_options(),
        )
    }

    /// Returns the ids of existing shards in the database at the provided path.
    pub(crate) fn existing_shards(path: &Path, options: &Options) -> HashSet<ShardIndex> {
        DB::list_cf(options, path)
            .unwrap_or_default()
            .into_iter()
            .filter_map(|cf_name| match id_from_column_family_name(&cf_name) {
                // To check existing shards, we only need to look at whether the secondary sliver
                // column was created or not, as it was created after the primary sliver column.
                Some((shard_index, SliverType::Secondary)) => Some(shard_index),
                Some((_, SliverType::Primary)) | None => None,
            })
            .collect()
    }

    pub(crate) fn status(&self) -> Result<ShardStatus, TypedStoreError> {
        self.shard_status
            .get(&())
            .map(|s| s.unwrap_or(ShardStatus::None))
    }

    /// Fetches the slivers with `sliver_type` for the provided blob IDs.
    pub(crate) fn fetch_slivers(
        &self,
        sliver_type: SliverType,
        slivers_to_fetch: &[BlobId],
    ) -> Result<Vec<(BlobId, Sliver)>, TypedStoreError> {
        fail::fail_point!("fail_point_sync_shard_return_empty", |_| { Ok(Vec::new()) });

        Ok(match sliver_type {
            SliverType::Primary => self
                .primary_slivers
                // TODO(#648): compare multi_get with scan for large value size.
                .multi_get(slivers_to_fetch)?
                .iter()
                .zip(slivers_to_fetch)
                .filter_map(|(sliver, blob_id)| {
                    sliver
                        .as_ref()
                        .map(|s| (*blob_id, Sliver::Primary(s.clone())))
                })
                .collect(),
            SliverType::Secondary => self
                .secondary_slivers
                .multi_get(slivers_to_fetch)?
                .iter()
                .zip(slivers_to_fetch)
                .filter_map(|(sliver, blob_id)| {
                    sliver
                        .as_ref()
                        .map(|s| (*blob_id, Sliver::Secondary(s.clone())))
                })
                .collect(),
        })
    }

    /// Syncs the shard to the current epoch from the previous shard owner.
    #[tracing::instrument(
        skip_all,
        fields(
            walrus.node = %node.protocol_key_pair.as_ref().public(),
            walrus.shard_index = %self.id
        ),
        err
    )]
    pub async fn start_sync_shard_before_epoch(
        &self,
        epoch: Epoch,
        node: Arc<StorageNodeInner>,
    ) -> Result<(), SyncShardClientError> {
        tracing::info!("Syncing shard to before epoch: {}", epoch);
        if self.status()? == ShardStatus::None {
            self.shard_status.insert(&(), &ShardStatus::ActiveSync)?
        }

        let shard_status = self.status()?;
        assert!(
            shard_status == ShardStatus::ActiveSync || shard_status == ShardStatus::ActiveRecover
        );

        match self.get_last_sync_status(&shard_status)? {
            ShardLastSyncStatus::Primary {
                last_synced_blob_id,
            } => {
                self.sync_shard_before_epoch_internal(
                    epoch,
                    node.clone(),
                    SliverType::Primary,
                    last_synced_blob_id,
                )
                .await?;
                self.sync_shard_before_epoch_internal(
                    epoch,
                    node.clone(),
                    SliverType::Secondary,
                    None,
                )
                .await?;
            }
            ShardLastSyncStatus::Secondary {
                last_synced_blob_id,
            } => {
                self.sync_shard_before_epoch_internal(
                    epoch,
                    node.clone(),
                    SliverType::Secondary,
                    last_synced_blob_id,
                )
                .await?;
            }
            ShardLastSyncStatus::Recovery => {}
        }

        self.recovery_any_missing_slivers(node).await?;

        let mut batch = self.shard_status.batch();
        batch.insert_batch(&self.shard_status, [((), ShardStatus::Active)])?;
        batch.delete_batch(&self.shard_sync_progress, [()])?;
        batch.write()?;

        Ok(())
    }

    /// Returns the last sync status for the shard.
    fn get_last_sync_status(
        &self,
        shard_status: &ShardStatus,
    ) -> Result<ShardLastSyncStatus, TypedStoreError> {
        let last_sync_status = if shard_status == &ShardStatus::ActiveRecover {
            // We are in recovery mode. Skip happy path syncing and directly jump to recover
            // missing blobs.
            ShardLastSyncStatus::Recovery
        } else {
            match self.shard_sync_progress.get(&())? {
                Some(ShardSyncProgress::V1(ShardSyncProgressV1 {
                    last_synced_blob_id,
                    sliver_type,
                })) => {
                    tracing::info!(
                        "Resuming shard sync from blob id: {}, sliver type: {}",
                        last_synced_blob_id,
                        sliver_type
                    );
                    match sliver_type {
                        SliverType::Primary => ShardLastSyncStatus::Primary {
                            last_synced_blob_id: Some(last_synced_blob_id),
                        },
                        SliverType::Secondary => ShardLastSyncStatus::Secondary {
                            last_synced_blob_id: Some(last_synced_blob_id),
                        },
                    }
                }
                None => ShardLastSyncStatus::Primary {
                    last_synced_blob_id: None,
                },
            }
        };
        Ok(last_sync_status)
    }

    #[tracing::instrument(
        skip_all,
        fields(
            walrus.sliver_type = %sliver_type
        ),
        err
    )]
    async fn sync_shard_before_epoch_internal(
        &self,
        epoch: Epoch,
        node: Arc<StorageNodeInner>,
        sliver_type: SliverType,
        mut last_synced_blob_id: Option<BlobId>,
    ) -> Result<(), SyncShardClientError> {
        // Helper to track the number of scanned blobs to test recovery. Not used in production.
        #[cfg(feature = "failure_injection")]
        let mut scan_count: u64 = 0;

        let mut blob_info_iter = node
            .storage
            .certified_blob_info_iter_before_epoch(epoch, last_synced_blob_id);

        let mut next_blob_info = blob_info_iter.next().transpose()?;
        while let Some((next_starting_blob_id, _)) = next_blob_info {
            tracing::debug!(
                "Syncing shard to before epoch: {}. Starting blob id: {}",
                epoch,
                next_starting_blob_id,
            );
            let mut batch = match sliver_type {
                SliverType::Primary => self.primary_slivers.batch(),
                SliverType::Secondary => self.secondary_slivers.batch(),
            };

            let fetched_slivers = node
                .committee_service
                .sync_shard_before_epoch(
                    self.id(),
                    next_starting_blob_id,
                    sliver_type,
                    10, // TODO(#705): make this configurable.
                    epoch,
                    &node.protocol_key_pair,
                )
                .await?;

            next_blob_info = self.batch_fetched_slivers_and_check_missing_blobs(
                epoch,
                &fetched_slivers,
                sliver_type,
                next_blob_info,
                &mut blob_info_iter,
                &mut batch,
            )?;

            #[cfg(feature = "failure_injection")]
            {
                scan_count += fetched_slivers.len() as u64;
                inject_failure(scan_count, sliver_type)?;
            }

            // Record sync progress.
            last_synced_blob_id = fetched_slivers.last().map(|(id, _)| *id);
            if let Some(last_synced_blob_id) = last_synced_blob_id {
                batch.insert_batch(
                    &self.shard_sync_progress,
                    [((), ShardSyncProgress::new(last_synced_blob_id, sliver_type))],
                )?;
            }
            batch.write()?;

            if last_synced_blob_id.is_none() {
                break;
            }
        }

        if next_blob_info.is_none() {
            return Ok(());
        }

        let mut batch = self.pending_recover_slivers.batch();
        while let Some((blob_id, _)) = next_blob_info {
            batch.insert_batch(
                &self.pending_recover_slivers,
                [((sliver_type, blob_id), ())],
            )?;
            next_blob_info = blob_info_iter.next().transpose()?;
        }
        batch.write()?;

        Ok(())
    }

    // Helper function to add fetched slivers to the db batch and check for missing blobs.
    // Advance blob_info_iter to the next blob that is greater than the last fetched blob id,
    // which is the next expected blob to fetch, and return the next expected blob.
    fn batch_fetched_slivers_and_check_missing_blobs(
        &self,
        epoch: Epoch,
        fetched_slivers: &[(BlobId, Sliver)],
        sliver_type: SliverType,
        mut next_blob_info: Option<(BlobId, BlobInfo)>,
        blob_info_iter: &mut BlobInfoIter,
        batch: &mut DBBatch,
    ) -> Result<Option<(BlobId, BlobInfo)>, SyncShardClientError> {
        for blob in fetched_slivers.iter() {
            tracing::debug!("Synced blob id: {} to before epoch: {}.", blob.0, epoch);
            //TODO(#705): verify sliver validity.
            //  - blob is certified
            //  - metadata is correct
            match &blob.1 {
                Sliver::Primary(primary) => {
                    assert_eq!(sliver_type, SliverType::Primary);
                    batch.insert_batch(&self.primary_slivers, [(blob.0, primary)])?;
                }
                Sliver::Secondary(secondary) => {
                    assert_eq!(sliver_type, SliverType::Secondary);
                    batch.insert_batch(&self.secondary_slivers, [(blob.0, secondary)])?;
                }
            }

            next_blob_info = self.check_and_record_missing_blobs(
                blob_info_iter,
                next_blob_info,
                blob.0,
                sliver_type,
                batch,
            )?;
        }
        Ok(next_blob_info)
    }

    /// Given a iterator over blob info table that is currently pointing to `next_blob_info`,
    /// and `fetched_blob_id` returned from the remote storage node, checks if there are any
    /// missing blobs between `next_blob_info` and `fetched_blob_id`.
    ///
    /// If there are missing blobs, add them in `db_batch` to be stored in
    /// `pending_recover_slivers` table.
    ///
    /// Returns the next blob info that is greater than `fetched_blob_id` or None if there are no.
    fn check_and_record_missing_blobs(
        &self,
        blob_info_iter: &mut BlobInfoIter,
        mut next_blob_info: Option<(BlobId, BlobInfo)>,
        fetched_blob_id: BlobId,
        sliver_type: SliverType,
        db_batch: &mut DBBatch,
    ) -> Result<Option<(BlobId, BlobInfo)>, TypedStoreError> {
        let Some((mut next_blob_id, _)) = next_blob_info else {
            // `next_blob_info is the last item read from `blob_info_iter`.
            // If it is None, there are no more blobs to check.
            debug_assert!(blob_info_iter.next().transpose()?.is_none());
            return Ok(None);
        };

        if next_blob_id == fetched_blob_id {
            return blob_info_iter.next().transpose();
        }

        while to_rocks_db_key(&next_blob_id) < to_rocks_db_key(&fetched_blob_id) {
            db_batch.insert_batch(
                &self.pending_recover_slivers,
                [((sliver_type, next_blob_id), ())],
            )?;

            next_blob_info = blob_info_iter.next().transpose()?;
            if let Some((blob_id, _)) = next_blob_info {
                next_blob_id = blob_id;
            } else {
                return Ok(None);
            }
        }

        if next_blob_id == fetched_blob_id {
            return blob_info_iter.next().transpose();
        }

        Ok(next_blob_info)
    }

    /// Recovers any missing blobs stored in `pending_recover_slivers` table.
    async fn recovery_any_missing_slivers(
        &self,
        node: Arc<StorageNodeInner>,
    ) -> Result<(), SyncShardClientError> {
        if self.pending_recover_slivers.is_empty() {
            return Ok(());
        }

        tracing::info!("Shard sync is done. Still has missing blobs. Shard enters recovery mode.",);
        self.shard_status.insert(&(), &ShardStatus::ActiveRecover)?;

        fail::fail_point!("fail_point_after_start_recovery", |_| {
            Err(SyncShardClientError::Internal(anyhow!(
                "sync shard simulated sync failure after start recovery"
            )))
        });

        loop {
            self.recover_missing_blobs(node.clone()).await?;

            if self.pending_recover_slivers.is_empty() {
                // TODO: in test, check that we have recovered all the certified blobs.
                break;
            }
            tracing::warn!("Recovering missing blobs still misses blobs. Retrying in 60 seconds.",);
            tokio::time::sleep(Duration::from_secs(60)).await;
        }

        Ok(())
    }

    /// Recovers missing blob slivers stored in `pending_recover_slivers` table, using
    /// `blob_sync_handler`.
    async fn recover_missing_blobs(
        &self,
        node: Arc<StorageNodeInner>,
    ) -> Result<(), TypedStoreError> {
        let semaphore = Semaphore::new(5); // TODO: make this configurable.
        let mut futures = FuturesUnordered::new();
        for recover_blob in self.pending_recover_slivers.safe_iter() {
            let ((sliver_type, blob_id), _) = recover_blob?;
            futures.push(self.recover_blob(blob_id, sliver_type, node.clone(), &semaphore));
        }

        while let Some(result) = futures.next().await {
            if let Err(err) = result {
                tracing::error!(error = ?err, "Error recovering missing blob sliver.");
            }
        }

        Ok(())
    }

    /// Recovers the missing blob sliver for the given blob ID.
    async fn recover_blob(
        &self,
        blob_id: BlobId,
        sliver_type: SliverType,
        node: Arc<StorageNodeInner>,
        semaphore: &Semaphore,
    ) -> Result<(), TypedStoreError> {
        let _guard = semaphore.acquire().await;
        tracing::info!(
            "Start recovering missing blob {} in shard {}",
            blob_id,
            self.id
        );

        let metadata = node.storage.get_metadata(&blob_id)?;
        if metadata.is_none() {
            tracing::warn!(
                "Blob {} is missing in the metadata table. For certified blob, Blob sync task should
                recover the metadata. Skip recovering it for now.",
                blob_id
            );
            return Ok(());
        }

        let sliver_id = self
            .id
            .to_pair_index(node.encoding_config.n_shards(), &blob_id);

        let result = match sliver_type {
            SliverType::Primary => {
                recover_sliver::<Primary>(
                    node.committee_service.as_ref(),
                    &metadata.unwrap(),
                    sliver_id,
                    &node.encoding_config,
                )
                .await
            }
            SliverType::Secondary => {
                recover_sliver::<Secondary>(
                    node.committee_service.as_ref(),
                    &metadata.unwrap(),
                    sliver_id,
                    &node.encoding_config,
                )
                .await
            }
        };

        match result {
            Ok(sliver) => self.put_sliver(&blob_id, &sliver)?,
            Err(inconsistency_proof) => {
                // TODO: once committee service supports multi-epoch. This needs to use the
                // committee from the latest epoch.
                let invalid_blob_certificate = node
                    .committee_service
                    .get_invalid_blob_certificate(
                        &blob_id,
                        &inconsistency_proof,
                        node.encoding_config.n_shards(),
                    )
                    .await;
                node.contract_service
                    .invalidate_blob_id(&invalid_blob_certificate)
                    .await
            }
        }

        let mut batch = self.pending_recover_slivers.batch();
        batch.delete_batch(
            &self.pending_recover_slivers,
            [
                (SliverType::Primary, blob_id),
                (SliverType::Secondary, blob_id),
            ],
        )?;
        batch.write()
    }

    #[cfg(test)]
    pub(crate) fn sliver_count(&self, sliver_type: SliverType) -> usize {
        match sliver_type {
            SliverType::Primary => self.primary_slivers.keys().count(),
            SliverType::Secondary => self.secondary_slivers.keys().count(),
        }
    }

    #[cfg(test)]
    pub(crate) fn lock_shard_for_epoch_change(&self) -> Result<(), TypedStoreError> {
        self.shard_status.insert(&(), &ShardStatus::LockedToMove)
    }

    #[cfg(test)]
    pub(crate) fn update_status_in_test(&self, status: ShardStatus) -> Result<(), TypedStoreError> {
        self.shard_status.insert(&(), &status)
    }

    #[cfg(test)]
    pub(crate) fn check_and_record_missing_blobs_in_test(
        &self,
        blob_info_iter: &mut BlobInfoIter,
        next_blob_info: Option<(BlobId, BlobInfo)>,
        fetched_blob_id: BlobId,
        sliver_type: SliverType,
    ) -> Result<Option<(BlobId, BlobInfo)>, TypedStoreError> {
        let mut db_batch = self.pending_recover_slivers.batch();
        let next_blob_info = self.check_and_record_missing_blobs(
            blob_info_iter,
            next_blob_info,
            fetched_blob_id,
            sliver_type,
            &mut db_batch,
        )?;
        db_batch.write()?;
        Ok(next_blob_info)
    }

    #[cfg(test)]
    pub(crate) fn all_pending_recover_slivers(
        &self,
    ) -> Result<Vec<(SliverType, BlobId)>, TypedStoreError> {
        self.pending_recover_slivers.keys().collect()
    }
}

fn id_from_column_family_name(name: &str) -> Option<(ShardIndex, SliverType)> {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"^shard-(\d+)/(primary|secondary)-slivers$").expect("valid static regex")
    })
    .captures(name)
    .and_then(|captures| {
        let Ok(id) = captures.get(1)?.as_str().parse() else {
            tracing::warn!(%name, "ignoring shard-like column family with an ID out of range");
            return None;
        };
        let sliver_type = match captures.get(2)?.as_str() {
            "primary" => SliverType::Primary,
            "secondary" => SliverType::Secondary,
            _ => panic!("Invalid sliver type in regex capture"),
        };
        Some((ShardIndex(id), sliver_type))
    })
}

#[inline]
fn base_column_family_name(id: ShardIndex) -> String {
    format!("shard-{}", id.0)
}

#[inline]
fn primary_slivers_column_family_name(id: ShardIndex) -> String {
    base_column_family_name(id) + "/primary-slivers"
}

#[inline]
fn secondary_slivers_column_family_name(id: ShardIndex) -> String {
    base_column_family_name(id) + "/secondary-slivers"
}

#[inline]
fn shard_status_column_family_name(id: ShardIndex) -> String {
    base_column_family_name(id) + "/status"
}

#[inline]
fn shard_sync_progress_column_family_name(id: ShardIndex) -> String {
    base_column_family_name(id) + "/sync_progress"
}

#[inline]
fn pending_recover_slivers_column_family_name(id: ShardIndex) -> String {
    base_column_family_name(id) + "/pending_recover_slivers"
}

#[cfg(feature = "failure_injection")]
fn inject_failure(scan_count: u64, sliver_type: SliverType) -> Result<(), anyhow::Error> {
    // Inject a failure point to simulate a sync failure.
    fail::fail_point!("fail_point_fetch_sliver", |arg| {
        if let Some(arg) = arg {
            let parts: Vec<&str> = arg.split(',').collect();
            let trigger_in_primary = parts[0].parse::<bool>().unwrap();
            let trigger_at = parts[1].parse::<u64>().unwrap();
            tracing::info!(
                fail_point = "fail_point_fetch_sliver",
                arg = ?arg,
                if_trigger_in_primary = ?trigger_in_primary,
                trigger_index = ?trigger_at,
                blob_count = ?scan_count
            );
            if ((trigger_in_primary && sliver_type == SliverType::Primary)
                || (!trigger_in_primary && sliver_type == SliverType::Secondary))
                && trigger_at <= scan_count
            {
                return Err(anyhow!("fetch_sliver simulated sync failure"));
            }
        }
        Ok(())
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use typed_store::{Map, TypedStoreError};
    use walrus_core::{
        encoding::{Primary, Secondary},
        BlobId,
        ShardIndex,
        Sliver,
        SliverType,
    };
    use walrus_sui::test_utils::event_id_for_testing;
    use walrus_test_utils::{async_param_test, param_test, Result as TestResult, WithTempDir};

    use super::id_from_column_family_name;
    use crate::{
        node::storage::{
            blob_info::{BlobCertificationStatus, BlobInfo},
            tests::{empty_storage, get_sliver, BLOB_ID, OTHER_SHARD_INDEX, SHARD_INDEX},
            Storage,
        },
        test_utils::empty_storage_with_shards,
    };

    async_param_test! {
        can_store_and_retrieve_sliver -> TestResult: [
            primary: (SliverType::Primary),
            secondary: (SliverType::Secondary),
        ]
    }
    async fn can_store_and_retrieve_sliver(sliver_type: SliverType) -> TestResult {
        let storage = empty_storage();
        let shard = storage.as_ref().shard_storage(SHARD_INDEX).unwrap();
        let sliver = get_sliver(sliver_type, 1);

        shard.put_sliver(&BLOB_ID, &sliver)?;
        let retrieved = shard.get_sliver(&BLOB_ID, sliver_type)?;

        assert_eq!(retrieved, Some(sliver));

        Ok(())
    }

    #[tokio::test]
    async fn stores_separate_primary_and_secondary_sliver() -> TestResult {
        let storage = empty_storage();
        let shard = storage.as_ref().shard_storage(SHARD_INDEX).unwrap();

        let primary = get_sliver(SliverType::Primary, 1);
        let secondary = get_sliver(SliverType::Secondary, 2);

        shard.put_sliver(&BLOB_ID, &primary)?;
        shard.put_sliver(&BLOB_ID, &secondary)?;

        let retrieved_primary = shard.get_sliver(&BLOB_ID, SliverType::Primary)?;
        let retrieved_secondary = shard.get_sliver(&BLOB_ID, SliverType::Secondary)?;

        assert_eq!(retrieved_primary, Some(primary), "invalid primary sliver");
        assert_eq!(
            retrieved_secondary,
            Some(secondary),
            "invalid secondary sliver"
        );

        Ok(())
    }

    #[tokio::test]
    async fn stores_and_deletes_slivers() -> TestResult {
        let storage = empty_storage();
        let shard = storage.as_ref().shard_storage(SHARD_INDEX).unwrap();

        let primary = get_sliver(SliverType::Primary, 1);
        let secondary = get_sliver(SliverType::Secondary, 2);

        shard.put_sliver(&BLOB_ID, &primary)?;
        shard.put_sliver(&BLOB_ID, &secondary)?;

        assert!(shard.is_sliver_pair_stored(&BLOB_ID)?);

        let mut batch = storage.inner.metadata.batch();
        shard.delete_sliver_pair(&mut batch, &BLOB_ID)?;
        batch.write()?;

        assert!(!shard.is_sliver_stored::<Primary>(&BLOB_ID)?);
        assert!(!shard.is_sliver_stored::<Secondary>(&BLOB_ID)?);

        Ok(())
    }

    #[tokio::test]
    async fn delete_on_empty_slivers_does_not_error() -> TestResult {
        let storage = empty_storage();
        let shard = storage.as_ref().shard_storage(SHARD_INDEX).unwrap();

        assert!(!shard.is_sliver_stored::<Primary>(&BLOB_ID)?);
        assert!(!shard.is_sliver_stored::<Secondary>(&BLOB_ID)?);

        let mut batch = storage.inner.metadata.batch();
        shard
            .delete_sliver_pair(&mut batch, &BLOB_ID)
            .expect("delete should not error");
        batch.write()?;
        Ok(())
    }

    async_param_test! {
        stores_and_retrieves_for_multiple_shards -> TestResult: [
            primary_primary: (SliverType::Primary, SliverType::Primary),
            secondary_secondary: (SliverType::Secondary, SliverType::Secondary),
            mixed: (SliverType::Primary, SliverType::Secondary),
        ]
    }
    async fn stores_and_retrieves_for_multiple_shards(
        type_first: SliverType,
        type_second: SliverType,
    ) -> TestResult {
        let storage = empty_storage_with_shards(&[SHARD_INDEX, OTHER_SHARD_INDEX]);

        let first_shard = storage.as_ref().shard_storage(SHARD_INDEX).unwrap();
        let first_sliver = get_sliver(type_first, 1);

        let second_shard = storage.as_ref().shard_storage(OTHER_SHARD_INDEX).unwrap();
        let second_sliver = get_sliver(type_second, 2);

        first_shard.put_sliver(&BLOB_ID, &first_sliver)?;
        second_shard.put_sliver(&BLOB_ID, &second_sliver)?;

        let first_retrieved = first_shard.get_sliver(&BLOB_ID, type_first)?;
        let second_retrieved = second_shard.get_sliver(&BLOB_ID, type_second)?;

        assert_eq!(
            first_retrieved,
            Some(first_sliver),
            "invalid sliver from first shard"
        );
        assert_eq!(
            second_retrieved,
            Some(second_sliver),
            "invalid sliver from second shard"
        );

        Ok(())
    }

    async_param_test! {
        indicates_when_sliver_pair_is_stored -> TestResult: [
            neither: (false, false),
            only_primary: (true, false),
            only_secondary: (false, true),
            both: (true, true),
        ]
    }
    async fn indicates_when_sliver_pair_is_stored(
        store_primary: bool,
        store_secondary: bool,
    ) -> TestResult {
        let is_pair_stored: bool = store_primary & store_secondary;

        let storage = empty_storage();
        let shard = storage.as_ref().shard_storage(SHARD_INDEX).unwrap();

        if store_primary {
            shard.put_sliver(&BLOB_ID, &get_sliver(SliverType::Primary, 3))?;
        }
        if store_secondary {
            shard.put_sliver(&BLOB_ID, &get_sliver(SliverType::Secondary, 4))?;
        }

        assert_eq!(shard.is_sliver_pair_stored(&BLOB_ID)?, is_pair_stored);

        Ok(())
    }

    param_test! {
        test_parse_column_family_name: [
            primary: ("shard-10/primary-slivers", Some((ShardIndex(10), SliverType::Primary))),
            secondary: (
                "shard-20/secondary-slivers",
                Some((ShardIndex(20), SliverType::Secondary))
            ),
            invalid_id: ("shard-a/primary-slivers", None),
            invalid_sliver_type: ("shard-20/random-slivers", None),
            invalid_sliver_name: ("shard-20/slivers", None),
        ]
    }
    fn test_parse_column_family_name(
        cf_name: &str,
        expected_output: Option<(ShardIndex, SliverType)>,
    ) {
        assert_eq!(id_from_column_family_name(cf_name), expected_output);
    }

    struct ShardStorageFetchSliversSetup {
        storage: WithTempDir<Storage>,
        blob_ids: [BlobId; 4],
        data: HashMap<BlobId, HashMap<SliverType, Sliver>>,
    }

    fn setup_storage() -> Result<ShardStorageFetchSliversSetup, TypedStoreError> {
        let storage = empty_storage();
        let shard = storage.as_ref().shard_storage(SHARD_INDEX).unwrap();

        let blob_ids = [
            BlobId([0; 32]),
            BlobId([1; 32]),
            BlobId([2; 32]),
            BlobId([3; 32]),
        ];

        // Only generates data for first and third blob IDs.
        let data = [
            (
                blob_ids[0],
                [
                    (SliverType::Primary, get_sliver(SliverType::Primary, 0)),
                    (SliverType::Secondary, get_sliver(SliverType::Secondary, 1)),
                ]
                .iter()
                .cloned()
                .collect::<HashMap<SliverType, Sliver>>(),
            ),
            (
                blob_ids[2],
                [
                    (SliverType::Primary, get_sliver(SliverType::Primary, 2)),
                    (SliverType::Secondary, get_sliver(SliverType::Secondary, 3)),
                ]
                .iter()
                .cloned()
                .collect::<HashMap<SliverType, Sliver>>(),
            ),
        ]
        .iter()
        .cloned()
        .collect::<HashMap<BlobId, HashMap<SliverType, Sliver>>>();

        // Pupulates the shard with the generated data.
        for blob_data in data.iter() {
            for (_sliver_type, sliver) in blob_data.1.iter() {
                shard.put_sliver(blob_data.0, sliver)?;
            }
        }

        Ok(ShardStorageFetchSliversSetup {
            storage,
            blob_ids,
            data,
        })
    }

    async_param_test! {
        test_shard_storage_fetch_single_sliver -> TestResult: [
            primary: (SliverType::Primary),
            secondary: (SliverType::Secondary),
        ]
    }
    async fn test_shard_storage_fetch_single_sliver(sliver_type: SliverType) -> TestResult {
        let ShardStorageFetchSliversSetup {
            storage,
            blob_ids,
            data,
        } = setup_storage()?;
        let shard = storage.as_ref().shard_storage(SHARD_INDEX).unwrap();
        assert_eq!(
            shard.fetch_slivers(sliver_type, &[blob_ids[0]])?,
            vec![(blob_ids[0], data[&blob_ids[0]][&sliver_type].clone())]
        );

        assert_eq!(
            shard.fetch_slivers(sliver_type, &[blob_ids[2]])?,
            vec![(blob_ids[2], data[&blob_ids[2]][&sliver_type].clone())]
        );

        Ok(())
    }

    async_param_test! {
        test_shard_storage_fetch_multiple_slivers -> TestResult: [
            primary: (SliverType::Primary),
            secondary: (SliverType::Secondary),
        ]
    }
    async fn test_shard_storage_fetch_multiple_slivers(sliver_type: SliverType) -> TestResult {
        let ShardStorageFetchSliversSetup {
            storage,
            blob_ids,
            data,
        } = setup_storage()?;
        let shard = storage.as_ref().shard_storage(SHARD_INDEX).unwrap();

        assert_eq!(
            shard.fetch_slivers(sliver_type, &[blob_ids[0], blob_ids[2]])?,
            vec![
                (blob_ids[0], data[&blob_ids[0]][&sliver_type].clone()),
                (blob_ids[2], data[&blob_ids[2]][&sliver_type].clone())
            ]
        );

        Ok(())
    }

    async_param_test! {
        test_shard_storage_fetch_non_existing_slivers -> TestResult: [
            primary: (SliverType::Primary),
            secondary: (SliverType::Secondary),
        ]
    }
    async fn test_shard_storage_fetch_non_existing_slivers(sliver_type: SliverType) -> TestResult {
        let ShardStorageFetchSliversSetup {
            storage, blob_ids, ..
        } = setup_storage()?;
        let shard = storage.as_ref().shard_storage(SHARD_INDEX).unwrap();

        assert!(shard.fetch_slivers(sliver_type, &[blob_ids[1]])?.is_empty());

        Ok(())
    }

    async_param_test! {
        test_shard_storage_fetch_mix_existing_non_existing_slivers -> TestResult: [
            primary: (SliverType::Primary),
            secondary: (SliverType::Secondary),
        ]
    }
    async fn test_shard_storage_fetch_mix_existing_non_existing_slivers(
        sliver_type: SliverType,
    ) -> TestResult {
        let ShardStorageFetchSliversSetup {
            storage,
            blob_ids,
            data,
        } = setup_storage()?;
        let shard = storage.as_ref().shard_storage(SHARD_INDEX).unwrap();

        assert_eq!(
            shard.fetch_slivers(sliver_type, &blob_ids)?,
            vec![
                (blob_ids[0], data[&blob_ids[0]][&sliver_type].clone()),
                (blob_ids[2], data[&blob_ids[2]][&sliver_type].clone())
            ]
        );

        Ok(())
    }

    // Tests for `check_and_record_missing_blobs` method.
    // We use 8 randomly generated blob IDs, and for the index = 4 and 7,
    // we remove the corresponding blob info from the client side.
    async_param_test! {
        test_check_and_record_missing_blobs -> TestResult: [
            missing_in_between: (0, 2, Some(3)),
            no_missing: (2, 2, Some(3)),
            next_certified_1: (2, 3, Some(5)),
            next_certified_2: (2, 4, Some(5)),
            no_next_1: (0, 6, None),
            no_next_2: (0, 7, None),
        ]
    }
    async fn test_check_and_record_missing_blobs(
        next_blob_info_index: usize,
        fetched_blob_id_index: usize,
        expected_next_blob_info_index: Option<usize>,
    ) -> TestResult {
        let storage = empty_storage();
        let blob_info = storage.inner.blob_info.clone();
        let new_epoch = 3;

        for _ in 0..8 {
            let blob_id = BlobId::random();
            blob_info.insert(
                &blob_id,
                &BlobInfo::new_for_testing(
                    10,
                    BlobCertificationStatus::Certified,
                    event_id_for_testing(),
                    Some(0),
                    Some(1),
                    None,
                ),
            )?;
        }

        let sorted_blob_ids = blob_info.keys().collect::<Result<Vec<_>, _>>()?;
        blob_info.remove(&sorted_blob_ids[4])?;
        blob_info.remove(&sorted_blob_ids[7])?;

        let shard = storage.as_ref().shard_storage(SHARD_INDEX).unwrap();
        let mut blob_info_iter = storage.inner.certified_blob_info_iter_before_epoch(
            new_epoch,
            Some(sorted_blob_ids[next_blob_info_index]),
        );
        let next_certified_blob_to_check = shard.check_and_record_missing_blobs_in_test(
            &mut blob_info_iter,
            Some((
                sorted_blob_ids[next_blob_info_index],
                blob_info
                    .get(&sorted_blob_ids[next_blob_info_index])
                    .unwrap()
                    .unwrap(),
            )),
            sorted_blob_ids[fetched_blob_id_index],
            SliverType::Primary,
        )?;

        assert_eq!(
            next_certified_blob_to_check.map(|(blob_id, _)| blob_id),
            expected_next_blob_info_index.map(|i| sorted_blob_ids[i])
        );

        let missing_blob_ids = shard
            .all_pending_recover_slivers()?
            .iter()
            .map(|(sliver_type, id)| {
                assert_eq!(sliver_type, &SliverType::Primary);
                *id
            })
            .collect::<Vec<_>>();
        let mut expected_missing_blobs = Vec::new();
        for (i, blob_id) in sorted_blob_ids
            .iter()
            .enumerate()
            .take(fetched_blob_id_index)
            .skip(next_blob_info_index)
        {
            if i != 4 && i != 7 {
                expected_missing_blobs.push(*blob_id);
            }
        }
        assert_eq!(missing_blob_ids, expected_missing_blobs);

        Ok(())
    }
}

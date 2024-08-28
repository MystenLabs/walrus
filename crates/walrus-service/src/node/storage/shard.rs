// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Walrus shard storage.

use core::fmt::{self, Display};
use std::{
    collections::{HashMap, HashSet},
    ops::Bound::{Excluded, Unbounded},
    path::Path,
    sync::{Arc, OnceLock},
};

#[cfg(feature = "failure_injection")]
use anyhow::anyhow;
use fastcrypto::traits::KeyPair;
use regex::Regex;
use rocksdb::{Options, DB};
use serde::{Deserialize, Serialize};
use typed_store::{
    rocks::{errors::typed_store_err_from_rocks_err, DBBatch, DBMap, ReadWriteOptions, RocksDB},
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

use super::{blob_info::BlobInfoApi, DatabaseConfig, Storage};
use crate::node::{errors::SyncShardClientError, StorageNodeInner};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ShardStatus {
    /// Initial status of the shard when just created.
    None,

    /// The shard is active in this node serving reads and writes.
    Active,

    /// The shard is being synced to the last epoch.
    ActiveSync,

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

#[derive(Debug, Clone)]
pub struct ShardStorage {
    id: ShardIndex,
    shard_status: DBMap<(), ShardStatus>,
    primary_slivers: DBMap<BlobId, PrimarySliver>,
    secondary_slivers: DBMap<BlobId, SecondarySliver>,
    shard_sync_progress: DBMap<(), ShardSyncProgress>,
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

        if let Some(status) = initial_shard_status {
            shard_status.insert(&(), &status)?;
        }

        Ok(Self {
            id,
            shard_status,
            primary_slivers,
            secondary_slivers,
            shard_sync_progress,
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

        assert_eq!(self.status()?, ShardStatus::ActiveSync);

        // Checks if the shard was previously syncing and resumes syncing from the last synced blob.
        let (mut last_synced_blob_id, skip_primary) = match self.shard_sync_progress.get(&())? {
            Some(ShardSyncProgress::V1(ShardSyncProgressV1 {
                last_synced_blob_id,
                sliver_type,
            })) => {
                tracing::info!(
                    "Resuming shard sync from blob id: {}, sliver type: {}",
                    last_synced_blob_id,
                    sliver_type
                );
                (
                    Some(last_synced_blob_id),
                    sliver_type == SliverType::Secondary,
                )
            }
            None => (None, false),
        };

        // TODO(#705): handle missing individual blobs.
        // TODO(#259): handle non-happy path.
        if !skip_primary {
            self.sync_shard_before_epoch_internal(
                epoch,
                node.clone(),
                SliverType::Primary,
                last_synced_blob_id,
            )
            .await?;
            last_synced_blob_id = None;
        }

        self.sync_shard_before_epoch_internal(
            epoch,
            node,
            SliverType::Secondary,
            last_synced_blob_id,
        )
        .await?;

        let mut batch = self.shard_status.batch();
        batch.insert_batch(&self.shard_status, [((), ShardStatus::Active)])?;
        batch.delete_batch(&self.shard_sync_progress, [()])?;
        batch.write()?;

        Ok(())
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
        let mut scan_count = 0;
        while let Some(next_starting_blob_id) =
            next_certified_blob_id_before_epoch(&node.storage, epoch, last_synced_blob_id)?
        {
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
            for blob in fetched_slivers.iter() {
                tracing::debug!("Synced blob id: {} to before epoch: {}.", blob.0, epoch,);
                //TODO(#705): Track missing blobs.
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

                // Inject a failure point to simulate a sync failure.
                #[cfg(feature = "failure_injection")]
                {
                    (|| -> Result<(), anyhow::Error> {
                        scan_count += 1;
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
                                    || (!trigger_in_primary
                                        && sliver_type == SliverType::Secondary))
                                    && trigger_at == scan_count
                                {
                                    return Err(anyhow!("fetch_sliver simulated sync failure"));
                                }
                            }
                            Ok(())
                        });
                        Ok(())
                    })()?;
                }
            }

            last_synced_blob_id = fetched_slivers.last().map(|(id, _)| *id);
            if let Some(last_synced_blob_id) = last_synced_blob_id {
                batch.insert_batch(
                    &self.shard_sync_progress,
                    [((), ShardSyncProgress::new(last_synced_blob_id, sliver_type))],
                )?;

                batch.write()?;
            } else {
                break;
            }
        }

        Ok(())
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
}

// Helper function to find the next certified blob ID after the provided blob ID.
fn next_certified_blob_id_before_epoch(
    storage: &Storage,
    certified_before_epoch: Epoch,
    after_blob: Option<BlobId>,
) -> Result<Option<BlobId>, TypedStoreError> {
    // The query starts from the blob after the provided blob ID in `after_blob`.
    let iter = storage.blob_info_iter(Some((
        if let Some(starting_blob_id) = after_blob {
            Excluded(starting_blob_id)
        } else {
            Unbounded
        },
        Unbounded,
    )));

    for result in iter {
        let (blob_id, blob_info) = result?;
        if blob_info.is_certified()
            && blob_info
                .status_changing_epoch(super::blob_info::BlobCertificationStatus::Certified)
                .unwrap()
                < certified_before_epoch
        {
            return Ok(Some(blob_id));
        }
    }

    Ok(None)
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
            shard,
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

    #[tokio::test]
    async fn test_next_certified_blob_id_before_epoch() -> TestResult {
        let storage = empty_storage();
        let blob_info = storage.inner.blob_info.clone();
        let new_epoch = 3;

        let blob_ids = [
            BlobId([1; 32]), // Not certified.
            BlobId([2; 32]), // Not exist.
            BlobId([3; 32]), // Certified within epoch 2
            BlobId([4; 32]), // Certified after epoch 2
            BlobId([5; 32]), // Invalid
            BlobId([6; 32]), // Certified within epoch 2
        ];

        blob_info.insert(
            &blob_ids[0],
            &BlobInfo::new_for_testing(
                10,
                BlobCertificationStatus::Registered,
                event_id_for_testing(),
                Some(0),
                None,
                None,
            ),
        )?;

        blob_info.insert(
            &blob_ids[2],
            &BlobInfo::new_for_testing(
                10,
                BlobCertificationStatus::Certified,
                event_id_for_testing(),
                Some(0),
                Some(1),
                None,
            ),
        )?;

        blob_info.insert(
            &blob_ids[3],
            &BlobInfo::new_for_testing(
                10,
                BlobCertificationStatus::Certified,
                event_id_for_testing(),
                Some(0),
                Some(3),
                None,
            ),
        )?;

        blob_info.insert(
            &blob_ids[4],
            &BlobInfo::new_for_testing(
                10,
                BlobCertificationStatus::Invalid,
                event_id_for_testing(),
                Some(0),
                Some(1),
                Some(1),
            ),
        )?;

        blob_info.insert(
            &blob_ids[5],
            &BlobInfo::new_for_testing(
                10,
                BlobCertificationStatus::Certified,
                event_id_for_testing(),
                Some(0),
                Some(2),
                None,
            ),
        )?;

        assert_eq!(
            shard::next_certified_blob_id_before_epoch(&storage.inner, new_epoch, None)?,
            Some(blob_ids[2])
        );
        assert_eq!(
            shard::next_certified_blob_id_before_epoch(
                &storage.inner,
                new_epoch,
                Some(blob_ids[1])
            )?,
            Some(blob_ids[2])
        );
        assert_eq!(
            shard::next_certified_blob_id_before_epoch(
                &storage.inner,
                new_epoch,
                Some(blob_ids[2])
            )?,
            Some(blob_ids[5])
        );
        assert_eq!(
            shard::next_certified_blob_id_before_epoch(
                &storage.inner,
                new_epoch,
                Some(blob_ids[5])
            )?,
            None,
        );

        Ok(())
    }
}

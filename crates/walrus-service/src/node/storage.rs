// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{hash_map::Entry, HashMap},
    fmt::Debug,
    ops::Bound::{Excluded, Unbounded},
    path::Path,
    sync::Arc,
};

use blob_info::merge_blob_info;
use rocksdb::Options;
use sui_sdk::types::event::EventID;
use typed_store::{
    rocks::{self, DBBatch, DBMap, MetricConf, ReadWriteOptions, RocksDB},
    Map,
    TypedStoreError,
};
use walrus_core::{
    messages::{SyncShardRequest, SyncShardResponse},
    metadata::{BlobMetadata, VerifiedBlobMetadataWithId},
    BlobId,
    Epoch,
    ShardIndex,
};
use walrus_sui::types::BlobEvent;

use self::{
    blob_info::{BlobInfo, BlobInfoApi, BlobInfoMergeOperand},
    event_cursor_table::EventCursorTable,
};
use super::errors::{ShardNotAssigned, SyncShardServiceError};

pub(crate) mod blob_info;

mod database_config;
pub use database_config::DatabaseConfig;

mod event_cursor_table;
pub(super) use event_cursor_table::EventProgress;

mod event_sequencer;

pub mod event_blob;
pub mod event_blob_writer;
mod shard;

pub(crate) use shard::{ShardStatus, ShardStorage};

/// Storage backing a [`StorageNode`][crate::node::StorageNode].
///
/// Enables storing blob metadata, which is shared across all shards. The method
/// [`shard_storage()`][Self::shard_storage] can be used to retrieve shard-specific storage.
#[derive(Debug, Clone)]
pub struct Storage {
    database: Arc<RocksDB>,
    metadata: DBMap<BlobId, BlobMetadata>,
    blob_info: DBMap<BlobId, BlobInfo>,
    event_cursor: EventCursorTable,
    shards: HashMap<ShardIndex, Arc<ShardStorage>>,
    config: DatabaseConfig,
}

/// A iterator over the blob info table.
pub(crate) struct BlobInfoIter<I: ?Sized>
where
    I: Iterator<Item = Result<(BlobId, BlobInfo), TypedStoreError>> + Send,
{
    iter: Box<I>,
    before_epoch: Epoch,
    only_certified: bool,
}

impl<I: ?Sized> BlobInfoIter<I>
where
    I: Iterator<Item = Result<(BlobId, BlobInfo), TypedStoreError>> + Send,
{
    pub fn new(iter: Box<I>, before_epoch: Epoch, only_certified: bool) -> Self {
        Self {
            iter,
            before_epoch,
            only_certified,
        }
    }
}

impl<I: ?Sized> Debug for BlobInfoIter<I>
where
    I: Iterator<Item = Result<(BlobId, BlobInfo), TypedStoreError>> + Send,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlobInfoIter")
            .field("before_epoch", &self.before_epoch)
            .field("only_certified", &self.only_certified)
            .finish()
    }
}

impl<I: ?Sized> Iterator for BlobInfoIter<I>
where
    I: Iterator<Item = Result<(BlobId, BlobInfo), TypedStoreError>> + Send,
{
    type Item = Result<(BlobId, BlobInfo), TypedStoreError>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut item = self.iter.next();
        if !self.only_certified {
            return item;
        }

        while let Some(Ok((_, ref blob_info))) = item {
            if matches!(
                blob_info.initial_certified_epoch(),
                Some(initial_certified_epoch) if initial_certified_epoch < self.before_epoch
            ) {
                return item;
            }
            item = self.iter.next();
        }
        item
    }
}

pub type BlobInfoIterator<'a> =
    BlobInfoIter<dyn Iterator<Item = Result<(BlobId, BlobInfo), TypedStoreError>> + Send + 'a>;

impl Storage {
    const METADATA_COLUMN_FAMILY_NAME: &'static str = "metadata";
    const BLOBINFO_COLUMN_FAMILY_NAME: &'static str = "blob_info";

    /// Opens the storage database located at the specified path, creating the database if absent.
    pub fn open(
        path: &Path,
        db_config: DatabaseConfig,
        metrics_config: MetricConf,
    ) -> Result<Self, anyhow::Error> {
        let mut db_opts = Options::from(&db_config.global);
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);

        let existing_shards_ids = ShardStorage::existing_shards(path, &db_opts);
        let mut shard_column_families = existing_shards_ids
            .iter()
            .copied()
            .flat_map(|id| {
                [
                    ShardStorage::primary_slivers_column_family_options(id, &db_config),
                    ShardStorage::secondary_slivers_column_family_options(id, &db_config),
                    ShardStorage::shard_status_column_family_options(id, &db_config),
                    ShardStorage::shard_sync_progress_column_family_options(id, &db_config),
                    ShardStorage::pending_recover_slivers_column_family_options(id, &db_config),
                ]
            })
            .collect::<Vec<_>>();

        let (metadata_cf_name, metadata_options) = Self::metadata_options(&db_config);
        let (blob_info_cf_name, blob_info_options) = Self::blob_info_options(&db_config);
        let (event_cursor_cf_name, event_cursor_options) = EventCursorTable::options(&db_config);

        let expected_column_families: Vec<_> = shard_column_families
            .iter_mut()
            .map(|(name, opts)| (name.as_str(), std::mem::take(opts)))
            .chain([
                (metadata_cf_name, metadata_options),
                (blob_info_cf_name, blob_info_options),
                (event_cursor_cf_name, event_cursor_options),
            ])
            .collect::<Vec<_>>();

        let database = rocks::open_cf_opts(
            path,
            Some(db_opts),
            metrics_config,
            &expected_column_families,
        )?;
        let metadata = DBMap::reopen(
            &database,
            Some(metadata_cf_name),
            &ReadWriteOptions::default(),
            false,
        )?;
        let event_cursor = EventCursorTable::reopen(&database)?;
        let blob_info = DBMap::reopen(
            &database,
            Some(blob_info_cf_name),
            &ReadWriteOptions::default(),
            false,
        )?;
        let shards = existing_shards_ids
            .into_iter()
            .map(|id| {
                ShardStorage::create_or_reopen(id, &database, &db_config, None)
                    .map(|shard| (id, Arc::new(shard)))
            })
            .collect::<Result<_, _>>()?;

        Ok(Self {
            database,
            metadata,
            blob_info,
            event_cursor,
            shards,
            config: db_config,
        })
    }

    /// Returns the storage for the specified shard, creating it if it does not exist.
    pub(crate) fn create_storage_for_shard(
        &mut self,
        shard: ShardIndex,
    ) -> Result<&ShardStorage, TypedStoreError> {
        match self.shards.entry(shard) {
            Entry::Occupied(entry) => Ok(entry.into_mut()),
            Entry::Vacant(entry) => {
                let shard_storage = Arc::new(ShardStorage::create_or_reopen(
                    shard,
                    &self.database,
                    &self.config,
                    Some(ShardStatus::Active),
                )?);
                Ok(entry.insert(shard_storage))
            }
        }
    }

    /// Returns the indices of the shards managed by the storage.
    pub fn shards(&self) -> impl ExactSizeIterator<Item = ShardIndex> + '_ {
        self.shards.keys().copied()
    }

    /// Returns an iterator over the shard storages managed by the storage.
    pub fn shard_storages(&self) -> impl ExactSizeIterator<Item = &Arc<ShardStorage>> {
        self.shards.values()
    }

    /// Returns a handle over the storage for a single shard.
    pub fn shard_storage(&self, shard: ShardIndex) -> Option<&Arc<ShardStorage>> {
        self.shards.get(&shard)
    }

    /// Store the verified metadata.
    #[tracing::instrument(skip_all)]
    pub fn put_verified_metadata(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
    ) -> Result<(), TypedStoreError> {
        self.put_metadata(metadata.blob_id(), metadata.metadata())
    }

    fn put_metadata(
        &self,
        blob_id: &BlobId,
        metadata: &BlobMetadata,
    ) -> Result<(), TypedStoreError> {
        self.metadata.insert(blob_id, metadata)?;
        self.merge_update_blob_info(blob_id, BlobInfoMergeOperand::MarkMetadataStored(true))
    }

    /// Get the blob info for `blob_id`
    #[tracing::instrument(skip_all)]
    pub fn get_blob_info(&self, blob_id: &BlobId) -> Result<Option<BlobInfo>, TypedStoreError> {
        self.blob_info.get(blob_id)
    }

    /// Returns an iterator over the blob info table.
    #[tracing::instrument(skip_all)]
    pub fn certified_blob_info_iter_before_epoch(
        &self,
        before_epoch: Epoch,
        last_synced_blob_id: Option<BlobId>,
    ) -> BlobInfoIterator {
        BlobInfoIter::new(
            Box::new(match last_synced_blob_id {
                Some(starting_blob_id) => self
                    .blob_info
                    .safe_range_iter((Excluded(starting_blob_id), Unbounded)),
                None => self.blob_info.safe_iter(),
            }),
            before_epoch,
            true,
        )
    }

    /// Get the event cursor for `event_type`
    #[tracing::instrument(skip_all)]
    pub fn get_event_cursor(&self) -> Result<Option<(u64, EventID)>, TypedStoreError> {
        self.event_cursor.get_event_cursor()
    }

    /// Update the blob info for a blob based on the `BlobEvent`
    #[tracing::instrument(skip_all)]
    pub fn update_blob_info(&self, event: &BlobEvent) -> Result<(), TypedStoreError> {
        self.merge_update_blob_info(&event.blob_id(), event.into())?;
        Ok(())
    }

    /// Update the blob info for `blob_id` using the specified merge `operation`.
    fn merge_update_blob_info(
        &self,
        blob_id: &BlobId,
        operation: BlobInfoMergeOperand,
    ) -> Result<(), TypedStoreError> {
        tracing::debug!(?operation, %blob_id, "updating blob info");

        let mut batch = self.blob_info.batch();
        batch.partial_merge_batch(&self.blob_info, [(blob_id, operation.to_bytes())])?;
        batch.write()
    }

    /// Advances the event cursor to the most recent, sequential event observed.
    ///
    /// The sequence-id is a sequence number of the order in which cursors were observed from the
    /// chain, and must start from 0 with the cursor following `get_event_cursor()`
    /// after the database is re-opened.
    ///
    /// For calls to this function such as `(0, cursor0), (2, cursor2), (1, cursor1)`, the cursor
    /// will advance to `cursor0` after the first call since it is the first in next in the
    /// sequence; will remain at `cursor0` after the next call since `cursor2` is not the next in
    /// sequence; and will advance to cursor2 after the 3rd call, since `cursor1` fills the gap as
    /// identified by its sequence number.
    #[tracing::instrument(skip_all)]
    pub(crate) fn maybe_advance_event_cursor(
        &self,
        event_index: usize,
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
        Ok(self
            .metadata
            .get(blob_id)?
            .map(|inner| VerifiedBlobMetadataWithId::new_verified_unchecked(*blob_id, inner)))
    }

    /// Deletes the provided [`BlobId`] from the storage.
    #[tracing::instrument(skip_all)]
    pub fn delete_blob(
        &self,
        blob_id: &BlobId,
        delete_blob_info: bool,
    ) -> Result<(), TypedStoreError> {
        let mut batch = self.metadata.batch();
        self.delete_metadata(&mut batch, blob_id, !delete_blob_info)?;
        self.delete_slivers(&mut batch, blob_id)?;
        if delete_blob_info {
            batch.delete_batch(&self.blob_info, [blob_id])?;
        }
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
            batch.partial_merge_batch(
                &self.blob_info,
                [(
                    blob_id,
                    BlobInfoMergeOperand::MarkMetadataStored(false).to_bytes(),
                )],
            )?;
        }
        Ok(())
    }

    /// Deletes the slivers on all shards for the provided [`BlobId`].
    fn delete_slivers(&self, batch: &mut DBBatch, blob_id: &BlobId) -> Result<(), TypedStoreError> {
        for shard in self.shards.values() {
            shard.delete_sliver_pair(batch, blob_id)?;
        }
        Ok(())
    }

    /// Returns true if the sliver pairs for the provided blob-id is stored at
    /// all of the storage's shards.
    #[tracing::instrument(skip_all)]
    pub fn is_stored_at_all_shards(&self, blob_id: &BlobId) -> Result<bool, TypedStoreError> {
        for shard in self.shards.values() {
            if !shard.status()?.is_owned_by_node() {
                continue;
            }

            if !shard.is_sliver_pair_stored(blob_id)? {
                return Ok(false);
            }
        }

        Ok(!self.shards.is_empty())
    }

    /// Returns true if the provided blob-id is invalid.
    #[tracing::instrument(skip_all)]
    pub fn is_invalid(&self, blob_id: &BlobId) -> Result<bool, TypedStoreError> {
        Ok(self
            .get_blob_info(blob_id)?
            .is_some_and(|blob_info| blob_info.is_invalid()))
    }

    /// Returns a list of identifiers of the shards that store their
    /// respective sliver for the specified blob.
    pub fn shards_with_sliver_pairs(
        &self,
        blob_id: &BlobId,
    ) -> Result<Vec<ShardIndex>, TypedStoreError> {
        let mut shards_with_sliver_pairs = Vec::with_capacity(self.shards.len());

        for shard in self.shards.values() {
            if shard.is_sliver_pair_stored(blob_id)? {
                shards_with_sliver_pairs.push(shard.id());
            }
        }

        Ok(shards_with_sliver_pairs)
    }

    fn metadata_options(db_config: &DatabaseConfig) -> (&'static str, Options) {
        (
            Self::METADATA_COLUMN_FAMILY_NAME,
            db_config.metadata.to_options(),
        )
    }

    fn blob_info_options(db_config: &DatabaseConfig) -> (&'static str, Options) {
        let mut options = db_config.blob_info.to_options();
        options.set_merge_operator("merge blob info", merge_blob_info, |_, _, _| None);
        (Self::BLOBINFO_COLUMN_FAMILY_NAME, options)
    }

    /// Returns the shards currently present in the storage.
    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn shards_present(&self) -> Vec<ShardIndex> {
        self.shards.keys().copied().collect()
    }

    /// Handles a sync shard request. The validity of the request should be checked before calling
    /// this function.
    pub fn handle_sync_shard_request(
        &self,
        request: &SyncShardRequest,
        current_epoch: Epoch,
    ) -> Result<SyncShardResponse, SyncShardServiceError> {
        let Some(shard) = self.shard_storage(request.shard_index()) else {
            return Err(ShardNotAssigned(request.shard_index(), current_epoch).into());
        };

        // Scan certified slivers to fetch.
        let blobs_to_fetch = self
            .blob_info
            .safe_iter_with_bounds(Some(request.starting_blob_id()), None)
            .filter_map(|blob_info| match blob_info {
                Ok((blob_id, blob_info)) => match blob_info.initial_certified_epoch() {
                    Some(epoch) if epoch < current_epoch => Some(Ok(blob_id)),
                    _ => None,
                },
                Err(e) => Some(Err(e)),
            })
            .take(request.sliver_count() as usize)
            .collect::<Result<Vec<_>, TypedStoreError>>()?;

        Ok(shard
            .fetch_slivers(request.sliver_type(), &blobs_to_fetch)?
            .into())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use blob_info::{
        BlobCertificationStatus,
        BlobInfoV1,
        BlobStatusChangeType,
        PermanentBlobInfoV1,
        ValidBlobInfoV1,
    };
    use prometheus::Registry;
    use tempfile::TempDir;
    use tokio::runtime::Runtime;
    use walrus_core::{
        encoding::{EncodingAxis, SliverData},
        Sliver,
        SliverIndex,
        SliverType,
    };
    use walrus_sui::{
        test_utils::{event_id_for_testing, EventForTesting},
        types::{BlobCertified, BlobRegistered},
    };
    use walrus_test_utils::{async_param_test, Result as TestResult, WithTempDir};

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
    pub(crate) fn empty_storage() -> WithTempDir<Storage> {
        typed_store::metrics::DBMetrics::init(&Registry::new());
        empty_storage_with_shards(&[SHARD_INDEX])
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

    pub(crate) fn populated_storage(spec: StorageSpec) -> TestResult<WithTempDir<Storage>> {
        let mut storage = empty_storage();

        let mut seed = 10u8;
        for (shard, sliver_list) in spec {
            storage.as_mut().create_storage_for_shard(*shard)?;
            let shard_storage = storage.as_ref().shard_storage(*shard).unwrap();

            for (blob_id, which) in sliver_list.iter() {
                if matches!(*which, WhichSlivers::Primary | WhichSlivers::Both) {
                    shard_storage.put_sliver(blob_id, &get_sliver(SliverType::Primary, seed))?;
                    seed += 1;
                }
                if matches!(*which, WhichSlivers::Secondary | WhichSlivers::Both) {
                    shard_storage.put_sliver(blob_id, &get_sliver(SliverType::Secondary, seed))?;
                    seed += 1;
                }
            }
        }

        Ok(storage)
    }

    #[tokio::test]
    async fn can_write_then_read_metadata() -> TestResult {
        let storage = empty_storage();
        let storage = storage.as_ref();
        let metadata = walrus_core::test_utils::verified_blob_metadata();
        let blob_id = metadata.blob_id();
        let expected = VerifiedBlobMetadataWithId::new_verified_unchecked(
            *blob_id,
            metadata.metadata().clone(),
        );

        storage.update_blob_info(&BlobCertified::for_testing(*blob_id).into())?;

        storage.put_metadata(metadata.blob_id(), metadata.metadata())?;
        let retrieved = storage.get_metadata(blob_id)?;

        assert_eq!(retrieved, Some(expected));

        Ok(())
    }

    #[tokio::test]
    async fn stores_and_deletes_metadata() -> TestResult {
        let storage = empty_storage();
        let storage = storage.as_ref();
        let metadata = walrus_core::test_utils::verified_blob_metadata();
        let blob_id = metadata.blob_id();

        storage.update_blob_info(&BlobRegistered::for_testing(*blob_id).into())?;
        storage.update_blob_info(&BlobCertified::for_testing(*blob_id).into())?;

        storage.put_metadata(metadata.blob_id(), metadata.metadata())?;

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
        let storage = empty_storage();
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
        let storage = empty_storage();
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
        storage.merge_update_blob_info(
            &blob_id,
            BlobInfoMergeOperand::new_change_for_testing(
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
                permanent_total: Some(PermanentBlobInfoV1 { ref mut event, .. }),
                ..
            })) = &mut state1
            else {
                panic!()
            };
            *event = registered_event;

            storage.merge_update_blob_info(
                &blob_id,
                BlobInfoMergeOperand::new_change_for_testing(
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
        storage.merge_update_blob_info(
            &blob_id,
            BlobInfoMergeOperand::MarkInvalid {
                epoch: 3,
                status_event: event,
            },
        )?;
        assert_eq!(storage.get_blob_info(&blob_id)?, Some(state2));
        Ok(())
    }

    #[tokio::test]
    async fn update_blob_info_metadata_stored() -> TestResult {
        let storage = empty_storage();
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

        storage.merge_update_blob_info(
            &blob_id,
            BlobInfoMergeOperand::new_change_for_testing(
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

        storage.merge_update_blob_info(&blob_id, BlobInfoMergeOperand::MarkMetadataStored(true))?;
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
        sequence_ids: &[usize],
        expected_sequence: &[usize],
    ) -> TestResult {
        let storage = empty_storage();
        let storage = storage.as_ref();

        let cursors: Vec<_> = sequence_ids
            .iter()
            .map(|&seq_id| (seq_id, event_id_for_testing()))
            .collect();
        let cursor_lookup: HashMap<_, _> = cursors.clone().into_iter().collect();

        for ((seq_id, cursor), expected_observed) in cursors.iter().zip(expected_sequence) {
            storage.maybe_advance_event_cursor(*seq_id, cursor)?;

            assert_eq!(
                storage.get_event_cursor()?.map(|(_, event_id)| event_id),
                Some(cursor_lookup[expected_observed])
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn maybe_advance_event_cursor_missed_zero() -> TestResult {
        let storage = empty_storage();
        let storage = storage.as_ref();

        storage.maybe_advance_event_cursor(1, &event_id_for_testing())?;
        assert_eq!(storage.get_event_cursor()?, None);

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
            let storage = populated_storage(&[(SHARD_INDEX, vec![(BLOB_ID, which)])])?;

            let result: Vec<_> = storage.as_ref().shards_with_sliver_pairs(&BLOB_ID)?;

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
            ])?;

            let mut result: Vec<_> = storage.as_ref().shards_with_sliver_pairs(&BLOB_ID)?;

            result.sort();

            assert_eq!(result, [SHARD_INDEX, OTHER_SHARD_INDEX]);

            Ok(())
        }

        #[tokio::test]
        async fn ignores_shards_without_both_sliver_pairs() -> TestResult {
            let storage = populated_storage(&[
                (SHARD_INDEX, vec![(BLOB_ID, WhichSlivers::Primary)]),
                (OTHER_SHARD_INDEX, vec![(BLOB_ID, WhichSlivers::Both)]),
            ])?;

            let result: Vec<_> = storage.as_ref().shards_with_sliver_pairs(&BLOB_ID)?;

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
        let storage = populated_storage(spec)?;
        if let Some(shard) = lock_shard {
            storage
                .inner
                .shard_storage(shard)
                .unwrap()
                .lock_shard_for_epoch_change()
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
            )?;

            for shard_id in [SHARD_INDEX, OTHER_SHARD_INDEX] {
                let Some(shard) = storage.shard_storage(shard_id) else {
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
            )?;

            // Check that the shard status is restored correctly.
            assert_eq!(
                storage
                    .shard_storage(SHARD_INDEX)
                    .expect("shard should exist")
                    .status()
                    .expect("status should be present"),
                ShardStatus::LockedToMove
            );

            assert_eq!(
                storage
                    .shard_storage(OTHER_SHARD_INDEX)
                    .expect("shard should exist")
                    .status()
                    .expect("status should be present"),
                ShardStatus::Active
            );

            Result::<(), anyhow::Error>::Ok(())
        })?;

        Ok(())
    }

    #[tokio::test]
    async fn reopen_partially_created_sliver_column_family() -> TestResult {
        let test_shard_index = ShardIndex(123);
        let storage = empty_storage();

        let primary_cfs = ShardStorage::primary_slivers_column_family_options(
            test_shard_index,
            &DatabaseConfig::default(),
        );

        let secondary_cfs = ShardStorage::secondary_slivers_column_family_options(
            test_shard_index,
            &DatabaseConfig::default(),
        );

        // Only create the column family for the primary sliver. When restarting the storage, the
        // shard should not be detected as existing.
        storage
            .inner
            .database
            .create_cf(&primary_cfs.0, &primary_cfs.1)
            .unwrap();
        assert!(
            !ShardStorage::existing_shards(storage.temp_dir.path(), &Options::default())
                .contains(&test_shard_index)
        );

        // Create the column family for the secondary sliver. When restarting the storage, the shard
        // should now be detected as existing.
        storage
            .inner
            .database
            .create_cf(&secondary_cfs.0, &secondary_cfs.1)
            .unwrap();
        assert!(
            ShardStorage::existing_shards(storage.temp_dir.path(), &Options::default())
                .contains(&test_shard_index)
        );

        Ok(())
    }

    async_param_test! {
        handle_sync_shard_request_behave_expected -> TestResult: [
            scan_first: (SliverType::Primary, ShardIndex(3), 1, 1, &[1]),
            scan_all: (SliverType::Primary, ShardIndex(5), 1, 5, &[1, 2, 3, 4, 5]),
            scan_tail: (SliverType::Primary, ShardIndex(3), 3, 5, &[3, 4, 5]),
            scan_head: (SliverType::Primary, ShardIndex(5), 0, 2, &[1, 2]),
            scan_middle_single: (SliverType::Secondary, ShardIndex(5), 3, 1, &[3]),
            scan_middle_range: (SliverType::Secondary, ShardIndex(3), 2, 2, &[2, 3]),
            scan_end_over: (SliverType::Secondary, ShardIndex(5), 3, 5, &[3, 4, 5]),
            scan_all_wide_range: (SliverType::Secondary, ShardIndex(3), 0, 100, &[1, 2, 3, 4, 5]),
            scan_out_of_range: (SliverType::Secondary, ShardIndex(5), 6, 2, &[]),
        ]
    }
    async fn handle_sync_shard_request_behave_expected(
        sliver_type: SliverType,
        shard_index: ShardIndex,
        start_blob_index: u8,
        count: u64,
        expected_blob_index_in_response: &[u8],
    ) -> TestResult {
        let mut storage = empty_storage();

        // All tests use the same setup:
        // - 2 shards: 3 and 5
        // - 5 blobs: 1, 2, 3, 4, 5
        // - 2 slivers per blob: primary and secondary

        let mut seed = 10u8;

        let blob_ids = [
            BlobId([1; 32]),
            BlobId([2; 32]),
            BlobId([3; 32]),
            BlobId([4; 32]),
            BlobId([5; 32]),
        ];

        let mut data: HashMap<ShardIndex, HashMap<BlobId, HashMap<SliverType, Sliver>>> =
            HashMap::new();
        for shard in [ShardIndex(3), ShardIndex(5)] {
            storage.as_mut().create_storage_for_shard(shard).unwrap();
            let shard_storage = storage.as_ref().shard_storage(shard).unwrap();
            data.insert(shard, HashMap::new());
            for blob in blob_ids.iter() {
                data.get_mut(&shard).unwrap().insert(*blob, HashMap::new());
                for sliver_type in [SliverType::Primary, SliverType::Secondary] {
                    let sliver_data = get_sliver(sliver_type, seed);
                    seed += 1;
                    data.get_mut(&shard)
                        .unwrap()
                        .get_mut(blob)
                        .unwrap()
                        .insert(sliver_type, sliver_data.clone());
                    shard_storage
                        .put_sliver(blob, &sliver_data)
                        .expect("Store should succeed");
                }
            }
        }

        for blob_id in blob_ids.iter() {
            storage
                .as_mut()
                .merge_update_blob_info(
                    blob_id,
                    BlobInfoMergeOperand::new_change_for_testing(
                        BlobStatusChangeType::Register,
                        false,
                        0,
                        2,
                        event_id_for_testing(),
                    ),
                )
                .expect("writing blob info should succeed");
            storage
                .as_mut()
                .merge_update_blob_info(
                    blob_id,
                    BlobInfoMergeOperand::new_change_for_testing(
                        BlobStatusChangeType::Certify,
                        false,
                        0,
                        2,
                        event_id_for_testing(),
                    ),
                )
                .expect("writing blob info should succeed");
        }

        let request = SyncShardRequest::new(
            shard_index,
            sliver_type,
            BlobId([start_blob_index; 32]),
            count,
            1,
        );
        let response = storage
            .as_ref()
            .handle_sync_shard_request(&request, 1)
            .unwrap();

        let SyncShardResponse::V1(slivers) = response;
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
        let storage = empty_storage();

        let request =
            SyncShardRequest::new(ShardIndex(123), SliverType::Primary, BlobId([1; 32]), 1, 1);
        let response = storage
            .as_ref()
            .handle_sync_shard_request(&request, 0)
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

    fn all_certified_blob_ids(
        storage: &WithTempDir<Storage>,
        after_blob: Option<BlobId>,
        new_epoch: Epoch,
    ) -> Result<Vec<BlobId>, TypedStoreError> {
        storage
            .inner
            .certified_blob_info_iter_before_epoch(new_epoch, after_blob)
            .map(|result| result.map(|(id, _info)| id))
            .collect::<Result<Vec<_>, _>>()
    }

    #[tokio::test]
    async fn test_certified_blob_info_iter_before_epoch() -> TestResult {
        let storage = empty_storage();
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
        batch.insert_batch(&blob_info, blob_info_map.iter())?;
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
}

// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Keeping track of the status of blob IDs and on-chain `Blob` objects.

mod blob_info_v1;

use std::{
    collections::HashSet,
    fmt::Debug,
    ops::Bound::{self, Unbounded},
    sync::{Arc, Mutex},
};

use anyhow::Context as _;
use enum_dispatch::enum_dispatch;
use rocksdb::{MergeOperands, Options, Transaction};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sui_types::{base_types::ObjectID, event::EventID};
use tokio::time::Instant;
use tracing::Level;
use typed_store::{
    Map,
    TypedStoreError,
    rocks::{DBBatch, DBMap, ReadWriteOptions, RocksDB},
};
use walrus_core::{BlobId, Epoch};
use walrus_storage_node_client::api::BlobStatus;
use walrus_sui::types::{BlobCertified, BlobDeleted, BlobEvent, BlobRegistered, InvalidBlobId};

#[cfg(test)]
pub(crate) use self::blob_info_v1::PermanentBlobInfoV1;
use self::per_object_blob_info::PerObjectBlobInfoMergeOperand;
pub(crate) use self::{
    blob_info_v1::{BlobInfoV1, ValidBlobInfoV1},
    per_object_blob_info::{PerObjectBlobInfo, PerObjectBlobInfoApi},
};
use super::{DatabaseTableOptionsFactory, constants};
use crate::{
    node::metrics::NodeMetricSet,
    utils::{self, process_items_in_batches},
};

pub type BlobInfoIterator<'a> = BlobInfoIter<
    BlobId,
    BlobInfo,
    dyn Iterator<Item = Result<(BlobId, BlobInfo), TypedStoreError>> + Send + 'a,
>;

pub type PerObjectBlobInfoIterator<'a> = BlobInfoIter<
    ObjectID,
    PerObjectBlobInfo,
    dyn Iterator<Item = Result<(ObjectID, PerObjectBlobInfo), TypedStoreError>> + Send + 'a,
>;

#[derive(Debug, Clone)]
pub(super) struct BlobInfoTable {
    aggregate_blob_info: DBMap<BlobId, BlobInfo>,
    per_object_blob_info: DBMap<ObjectID, PerObjectBlobInfo>,
    latest_handled_event_index: Arc<Mutex<DBMap<(), u64>>>,
}

/// Returns the options for the aggregate blob info column family.
pub(crate) fn blob_info_cf_options(db_table_opts_factory: &DatabaseTableOptionsFactory) -> Options {
    let mut options = db_table_opts_factory.blob_info();
    options.set_merge_operator("merge blob info", merge_mergeable::<BlobInfo>, |_, _, _| {
        None
    });
    options
}

/// Returns the options for the per object blob info column family.
pub(crate) fn per_object_blob_info_cf_options(
    db_table_opts_factory: &DatabaseTableOptionsFactory,
) -> Options {
    let mut options = db_table_opts_factory.per_object_blob_info();
    options.set_merge_operator(
        "merge per object blob info",
        merge_mergeable::<PerObjectBlobInfo>,
        |_, _, _| None,
    );
    options
}

impl BlobInfoTable {
    pub fn reopen(database: &Arc<RocksDB>) -> Result<Self, TypedStoreError> {
        let aggregate_blob_info = DBMap::reopen(
            database,
            Some(constants::aggregate_blob_info_cf_name()),
            &ReadWriteOptions::default(),
            false,
        )?;
        let per_object_blob_info = DBMap::reopen(
            database,
            Some(constants::per_object_blob_info_cf_name()),
            &ReadWriteOptions::default(),
            false,
        )?;
        let latest_handled_event_index = Arc::new(Mutex::new(DBMap::reopen(
            database,
            Some(constants::event_index_cf_name()),
            &ReadWriteOptions::default(),
            false,
        )?));

        Ok(Self {
            aggregate_blob_info,
            per_object_blob_info,
            latest_handled_event_index,
        })
    }

    pub fn clear(&self) -> Result<(), TypedStoreError> {
        self.aggregate_blob_info.schedule_delete_all()?;
        self.per_object_blob_info.schedule_delete_all()?;
        self.latest_handled_event_index
            .lock()
            .expect("mutex should not be poisoned")
            .schedule_delete_all()?;

        Ok(())
    }

    pub fn options(
        db_table_opts_factory: &DatabaseTableOptionsFactory,
    ) -> Vec<(&'static str, Options)> {
        vec![
            (
                constants::aggregate_blob_info_cf_name(),
                blob_info_cf_options(db_table_opts_factory),
            ),
            (
                constants::per_object_blob_info_cf_name(),
                per_object_blob_info_cf_options(db_table_opts_factory),
            ),
            (
                constants::event_index_cf_name(),
                // Doesn't make sense to have special options for the table containing a single
                // value.
                db_table_opts_factory.standard(),
            ),
        ]
    }

    /// Updates the blob info for a blob based on the [`BlobEvent`].
    ///
    /// Only updates the info if the provided `event_index` hasn't been processed yet.
    #[tracing::instrument(skip(self))]
    pub fn update_blob_info(
        &self,
        event_index: u64,
        event: &BlobEvent,
    ) -> Result<(), TypedStoreError> {
        let latest_handled_event_index = self
            .latest_handled_event_index
            .lock()
            .expect("mutex should not be poisoned");
        if Self::has_event_been_handled(latest_handled_event_index.get(&())?, event_index) {
            tracing::debug!("skip updating blob info for already handled event");
            return Ok(());
        }

        let operation = BlobInfoMergeOperand::from(event);
        tracing::debug!(?operation, "updating blob info");

        let mut batch = self.aggregate_blob_info.batch();

        batch.partial_merge_batch(
            &self.aggregate_blob_info,
            [(event.blob_id(), operation.to_bytes())],
        )?;
        self.update_per_object_blob_info(&mut batch, event)?;

        batch.insert_batch(&latest_handled_event_index, [(&(), event_index)])?;
        batch.write()
    }

    fn update_per_object_blob_info(
        &self,
        batch: &mut DBBatch,
        event: &BlobEvent,
    ) -> Result<(), TypedStoreError> {
        let (object_id, operand) = match event {
            BlobEvent::Registered(blob_registered) => (
                blob_registered.object_id,
                PerObjectBlobInfoMergeOperand::from(blob_registered),
            ),
            BlobEvent::Certified(blob_certified) => (
                blob_certified.object_id,
                PerObjectBlobInfoMergeOperand::from(blob_certified),
            ),
            BlobEvent::Deleted(BlobDeleted { object_id, .. }) => {
                batch.delete_batch(&self.per_object_blob_info, [(object_id)])?;
                return Ok(());
            }
            BlobEvent::InvalidBlobID(_) | BlobEvent::DenyListBlobDeleted(_) => {
                return Ok(());
            }
        };

        batch.partial_merge_batch(
            &self.per_object_blob_info,
            [(&object_id, operand.to_bytes())],
        )?;
        Ok(())
    }

    /// Updates the blob info for a blob based on the [`BlobEvent`] when the node is in recovery
    /// with incomplete history.
    ///
    /// Only updates the info if the provided `event_index` hasn't been processed yet.
    #[tracing::instrument(skip(self))]
    pub fn update_blob_info_during_recovery_with_incomplete_history(
        &self,
        event_index: u64,
        event: &BlobEvent,
        epoch_at_start: Epoch,
    ) -> Result<(), TypedStoreError> {
        tracing::debug!("updating blob info during recovery with incomplete history");
        let extension_event = match event {
            BlobEvent::Registered(BlobRegistered { end_epoch, .. })
            | BlobEvent::Certified(BlobCertified { end_epoch, .. })
            | BlobEvent::Deleted(BlobDeleted { end_epoch, .. })
                if end_epoch <= &epoch_at_start =>
            {
                tracing::debug!(
                    "skip updating blob info for event with end epoch before epoch at start"
                );
                return Ok(());
            }
            BlobEvent::Registered(_)
            // The registration event related to this certification must have the same end epoch, so
            // it must also be included in our incomplete event history. This means we have already
            // processed the registration event and can process the certification event normally.
            | BlobEvent::Certified(BlobCertified {
                is_extension: false,
                ..
            })
            | BlobEvent::Deleted(_)
            | BlobEvent::InvalidBlobID(_)
            | BlobEvent::DenyListBlobDeleted(_) => {
                tracing::debug!("performing standard blob-info update for event");
                return self.update_blob_info(event_index, event);
            }
            BlobEvent::Certified(event) => {
                // Extensions need special handling.
                event.clone()
            }
        };

        debug_assert!(
            extension_event.end_epoch > epoch_at_start,
            "checked end epoch in match above"
        );
        debug_assert!(
            extension_event.is_extension,
            "checked is_extension in match above"
        );

        if let Some(per_object_blob_info) =
            self.per_object_blob_info.get(&extension_event.object_id)?
        {
            assert!(per_object_blob_info.is_registered(epoch_at_start));
            tracing::debug!(
                ?per_object_blob_info,
                "perform standard blob-info update for extension event of tracked blob"
            );
            return self.update_blob_info(event_index, event);
        }

        let latest_handled_event_index = self
            .latest_handled_event_index
            .lock()
            .expect("mutex should not be poisoned");
        if Self::has_event_been_handled(latest_handled_event_index.get(&())?, event_index) {
            tracing::info!("skip updating blob info for already handled event");
            return Ok(());
        }

        tracing::info!(
            ?extension_event,
            "handling blob extension during recovery with incomplete history"
        );

        let mut batch = self.aggregate_blob_info.batch();
        let blob_id = extension_event.blob_id;
        let object_id = extension_event.object_id;
        let change_info = BlobStatusChangeInfo {
            blob_id,
            deletable: extension_event.deletable,
            epoch: extension_event.epoch,
            end_epoch: extension_event.end_epoch,
            status_event: extension_event.event_id,
        };
        let operations: Vec<_> = [
            BlobStatusChangeType::Register,
            BlobStatusChangeType::Certify,
        ]
        .into_iter()
        .map(|change_type| BlobInfoMergeOperand::ChangeStatus {
            change_type,
            change_info: change_info.clone(),
        })
        .collect();
        let aggregate_blob_operations = operations
            .iter()
            .map(|operation| (blob_id, operation.to_bytes()));
        let per_object_operations = operations.clone().into_iter().map(|operation| {
            (
                object_id,
                PerObjectBlobInfoMergeOperand::from_blob_info_merge_operand(operation)
                    .expect("we know this is a registered or certified event")
                    .to_bytes(),
            )
        });

        batch.partial_merge_batch(&self.aggregate_blob_info, aggregate_blob_operations)?;
        batch.partial_merge_batch(&self.per_object_blob_info, per_object_operations)?;
        batch.insert_batch(&latest_handled_event_index, [(&(), event_index)])?;
        batch.write()
    }

    fn has_event_been_handled(latest_handled_index: Option<u64>, event_index: u64) -> bool {
        latest_handled_index.is_some_and(|i| event_index <= i)
    }

    pub fn set_metadata_stored<'a>(
        &self,
        batch: &'a mut DBBatch,
        blob_id: &BlobId,
        metadata_stored: bool,
    ) -> Result<&'a mut DBBatch, TypedStoreError> {
        batch.partial_merge_batch(
            &self.aggregate_blob_info,
            [(
                blob_id,
                &BlobInfoMergeOperand::MarkMetadataStored(metadata_stored).to_bytes(),
            )],
        )
    }

    /// Returns an iterator over all entries in the aggregate blob info table within the given
    /// range.
    pub fn aggregate_blob_info_range_iter(
        &self,
        start_blob_id_bound: Bound<BlobId>,
        end_blob_id_bound: Bound<BlobId>,
    ) -> Result<impl Iterator<Item = Result<(BlobId, BlobInfo), TypedStoreError>>, TypedStoreError>
    {
        self.aggregate_blob_info
            .safe_range_iter((start_blob_id_bound, end_blob_id_bound))
    }

    /// Returns the column family handle for the aggregate blob info table.
    pub fn aggregate_cf(&self) -> Arc<rocksdb::BoundColumnFamily<'_>> {
        self.aggregate_blob_info
            .cf()
            .expect("we know that this CF exists")
    }

    pub fn get_for_update_in_transaction(
        &self,
        transaction: &Transaction<'_, rocksdb::OptimisticTransactionDB>,
        blob_id: &BlobId,
    ) -> Result<Option<BlobInfo>, rocksdb::Error> {
        // The value of the `exclusive` parameter does not matter for optimistic transactions.
        Ok(transaction
            .get_for_update_cf_opt(
                &self.aggregate_cf(),
                blob_id,
                false,
                &self.aggregate_blob_info.opts.readopts(),
            )?
            .and_then(|data| deserialize_from_db(&data)))
    }

    pub fn delete_in_transaction(
        &self,
        transaction: &Transaction<'_, rocksdb::OptimisticTransactionDB>,
        blob_id: &BlobId,
    ) -> Result<(), rocksdb::Error> {
        transaction.delete_cf(&self.aggregate_cf(), blob_id)
    }

    /// Returns an iterator over all blobs that were certified before the specified epoch in the
    /// blob info table starting with the `starting_blob_id` bound.
    #[tracing::instrument(skip_all)]
    pub fn certified_blob_info_iter_before_epoch(
        &self,
        before_epoch: Epoch,
        starting_blob_id_bound: Bound<BlobId>,
    ) -> BlobInfoIterator<'_> {
        BlobInfoIter::new(
            Box::new(
                self.aggregate_blob_info
                    .safe_range_iter((starting_blob_id_bound, Unbounded))
                    .expect("aggregate_blob_info cf must always exist in storage node"),
            ),
            before_epoch,
        )
    }

    /// Returns an iterator over all blob objects that were certified before the specified epoch in
    /// the per-object blob info table starting with the `starting_object_id` bound.
    #[tracing::instrument(skip_all)]
    pub fn certified_per_object_blob_info_iter_before_epoch(
        &self,
        before_epoch: Epoch,
        starting_object_id_bound: Bound<ObjectID>,
    ) -> PerObjectBlobInfoIterator<'_> {
        BlobInfoIter::new(
            Box::new(
                self.per_object_blob_info
                    .safe_range_iter((starting_object_id_bound, Unbounded))
                    .expect("per_object_blob_info cf must always exist in storage node"),
            ),
            before_epoch,
        )
    }

    /// Returns the blob info for `blob_id`.
    pub fn get(&self, blob_id: &BlobId) -> Result<Option<BlobInfo>, TypedStoreError> {
        self.aggregate_blob_info.get(blob_id)
    }

    /// Returns the per-object blob info for `object_id`.
    pub fn get_per_object_info(
        &self,
        object_id: &ObjectID,
    ) -> Result<Option<PerObjectBlobInfo>, TypedStoreError> {
        self.per_object_blob_info.get(object_id)
    }

    /// Returns the latest event index that has been handled by the node.
    pub(crate) fn get_latest_handled_event_index(&self) -> Result<u64, TypedStoreError> {
        Ok(self
            .latest_handled_event_index
            .lock()
            .expect("acquire latest_handled_event_index lock should not fail")
            .get(&())?
            .unwrap_or(0))
    }

    /// Processes blobs that have expired in the given epoch.
    ///
    /// This function iterates over the per-object blob info table, deleting any entries that have
    /// an end epoch equal to or less than the current epoch, and updating the aggregate blob info
    /// table in case of deletable blobs to reflect the new status of the blob objects.
    ///
    /// Processing is done in batches using `spawn_blocking` to avoid blocking the async runtime
    /// and make it possible to abort the task if the node is shutting down.
    #[tracing::instrument(skip_all, fields(walrus.epoch = %current_epoch))]
    pub(crate) async fn process_expired_blob_objects(
        &self,
        current_epoch: Epoch,
        node_metrics: &NodeMetricSet,
        batch_size: usize,
    ) -> anyhow::Result<()> {
        tracing::info!("starting to process expired blob objects");
        let start_time = Instant::now();

        let this = self.clone();
        let node_metrics = node_metrics.clone();

        let cleaned_up_objects_count = process_items_in_batches(move |last_processed_object_id| {
            this.process_expired_blob_objects_batch(
                last_processed_object_id,
                batch_size,
                current_epoch,
                &node_metrics,
            )
        })
        .await?;

        tracing::info!(
            cleaned_up_objects_count,
            duration = ?start_time.elapsed(),
            "finished processing expired blob objects",
        );

        Ok(())
    }

    /// Processes expired blob objects in batches.
    ///
    /// This is intended to be driven by [`utils::process_items_in_batches`].
    fn process_expired_blob_objects_batch(
        &self,
        last_processed_object_id: Option<ObjectID>,
        batch_size: usize,
        current_epoch: Epoch,
        node_metrics: &NodeMetricSet,
    ) -> anyhow::Result<utils::BatchProcessingResult<ObjectID>> {
        let mut modified_count = 0;
        let mut total_count = 0;
        let mut last_processed_object_id = last_processed_object_id;

        let start_bound = last_processed_object_id.map_or(Bound::Unbounded, Bound::Excluded);

        for result in self
            .per_object_blob_info
            .safe_range_iter((start_bound, Bound::Unbounded))?
            .take(batch_size)
        {
            total_count += 1;
            let (object_id, per_object_blob_info) = match result {
                Ok(values) => values,
                Err(error) => {
                    tracing::warn!(
                        ?error,
                        "error encountered while iterating over per-object blob info"
                    );
                    continue;
                }
            };
            last_processed_object_id = Some(object_id);

            if self.process_maybe_expired_blob_object(
                object_id,
                per_object_blob_info,
                current_epoch,
                node_metrics,
            )? {
                modified_count += 1;
            }
        }

        Ok(utils::BatchProcessingResult {
            total_count,
            modified_count,
            last_processed_item: last_processed_object_id,
        })
    }

    /// Cleans up a single expired blob object and updates the aggregate blob info if needed.
    fn process_maybe_expired_blob_object(
        &self,
        object_id: ObjectID,
        per_object_blob_info: PerObjectBlobInfo,
        current_epoch: Epoch,
        node_metrics: &NodeMetricSet,
    ) -> anyhow::Result<bool> {
        if per_object_blob_info.is_registered(current_epoch) {
            tracing::trace!(
                %object_id,
                ?per_object_blob_info,
                "skipping blob-info update for blob that is still active"
            );
            return Ok(false);
        }

        let blob_id = per_object_blob_info.blob_id();
        let was_certified = per_object_blob_info.initial_certified_epoch().is_some();
        let deletable = per_object_blob_info.is_deletable();
        let mut batch = self.per_object_blob_info.batch();
        // Clean up all expired objects.
        batch.delete_batch(&self.per_object_blob_info, [object_id])?;

        // Only update the aggregate blob info if the blob is not already deleted (in which case
        // it was already updated).
        if !per_object_blob_info.is_deleted() {
            tracing::debug!(
                %object_id,
                %blob_id,
                %was_certified,
                %deletable,
                "updating blob info for expired blob object"
            );
            let operand = if deletable {
                BlobInfoMergeOperand::DeletableExpired { was_certified }
            } else {
                BlobInfoMergeOperand::PermanentExpired { was_certified }
            };
            batch
                .partial_merge_batch(&self.aggregate_blob_info, [(blob_id, &operand.to_bytes())])?;
        } else {
            tracing::debug!(
                %object_id,
                %blob_id,
                "deleting per-object blob info for expired permanent blob"
            );
        }
        batch.write()?;

        // Record the number of deleted objects in a metric.
        node_metrics
            .garbage_collection_expired_blob_objects_deleted_total
            .inc();

        Ok(true)
    }

    /// Checks some internal invariants of the blob info table.
    ///
    /// The checks are not exhaustive yet.
    pub fn check_invariants(&self) -> Result<(), anyhow::Error> {
        let snapshot = self.aggregate_blob_info.rocksdb.snapshot();

        let mut per_object_table_blob_ids = HashSet::new();

        for result in self
            .per_object_blob_info
            .safe_iter_with_snapshot(&snapshot)
            .context("failed to create per-object blob info snapshot iterator")?
        {
            let Ok((object_id, PerObjectBlobInfo::V1(per_object_blob_info))) = result else {
                return Err(anyhow::anyhow!(
                    "error encountered while iterating over per-object blob info: {result:?}"
                ));
            };
            let blob_id = per_object_blob_info.blob_id();
            per_object_table_blob_ids.insert(blob_id);
            let Some(blob_info) = self
                .aggregate_blob_info
                .get_with_snapshot(&snapshot, &blob_id)?
            else {
                return Err(anyhow::anyhow!(
                    "blob info not found for blob ID {blob_id}, even though a corresponding \
                    per-object blob info entry exists (object ID: {object_id})"
                ));
            };
            let BlobInfo::V1(BlobInfoV1::Valid(ValidBlobInfoV1 {
                count_deletable_total,
                count_deletable_certified,
                latest_seen_deletable_registered_end_epoch,
                latest_seen_deletable_certified_end_epoch,
                ..
            })) = blob_info
            else {
                continue;
            };
            if per_object_blob_info.is_deletable() {
                let per_object_end_epoch = per_object_blob_info.end_epoch;
                anyhow::ensure!(
                    count_deletable_total > 0,
                    "count_deletable_total is 0 for blob ID {blob_id}, even though a deletable \
                    blob object exists (object ID: {object_id})"
                );
                anyhow::ensure!(
                    latest_seen_deletable_registered_end_epoch
                        .is_some_and(|e| e >= per_object_end_epoch),
                    "latest_seen_deletable_registered_end_epoch for blob ID {blob_id} is \
                    {latest_seen_deletable_registered_end_epoch:?}, which is inconsistent with the \
                    end epoch of a deletable blob object: {per_object_end_epoch} (object ID: \
                    {object_id})"
                );
                if per_object_blob_info.certified_epoch.is_some() {
                    anyhow::ensure!(
                        count_deletable_certified > 0,
                        "count_deletable_certified is 0 for blob ID {blob_id}, even though a \
                        deletable certified blob object exists (object ID: {object_id})"
                    );
                    anyhow::ensure!(
                        latest_seen_deletable_certified_end_epoch
                            .is_some_and(|e| e >= per_object_end_epoch),
                        "latest_seen_deletable_certified_end_epoch for blob ID {blob_id} is \
                        {latest_seen_deletable_certified_end_epoch:?}, which is inconsistent with \
                        the end epoch of a deletable certified blob object: {per_object_end_epoch} \
                        (object ID: {object_id})"
                    );
                }
            }
        }

        for result in self
            .aggregate_blob_info
            .safe_iter_with_snapshot(&snapshot)
            .context("failed to create aggregate blob info snapshot iterator")?
        {
            let Ok((blob_id, blob_info)) = result else {
                return Err(anyhow::anyhow!(
                    "error encountered while iterating over aggregate blob info: {result:?}"
                ));
            };
            let BlobInfo::V1(BlobInfoV1::Valid(blob_info)) = blob_info else {
                continue;
            };
            if !blob_info.has_no_objects() && !per_object_table_blob_ids.contains(&blob_id) {
                return Err(anyhow::anyhow!(
                    "per-object blob info not found for blob ID {blob_id}, even though a \
                    valid aggregate blob info entry referencing objects exists: {blob_info:?}"
                ));
            }
            blob_info.check_invariants().context(format!(
                "aggregate blob info invariants violated for blob ID {blob_id}"
            ))?;
        }
        Ok(())
    }
}

// TODO(#900): Rewrite other tests without relying on blob-info internals.
#[cfg(test)]
impl BlobInfoTable {
    pub fn batch(&self) -> DBBatch {
        self.aggregate_blob_info.batch()
    }

    pub fn merge_blob_info(
        &self,
        blob_id: &BlobId,
        operand: &BlobInfoMergeOperand,
    ) -> Result<(), TypedStoreError> {
        let mut batch = self.batch();
        batch.partial_merge_batch(&self.aggregate_blob_info, [(blob_id, operand.to_bytes())])?;
        batch.write()
    }

    pub fn insert(&self, blob_id: &BlobId, blob_info: &BlobInfo) -> Result<(), TypedStoreError> {
        self.aggregate_blob_info.insert(blob_id, blob_info)
    }

    pub fn remove(&self, blob_id: &BlobId) -> Result<(), TypedStoreError> {
        self.aggregate_blob_info.remove(blob_id)
    }

    pub fn keys(&self) -> Result<Vec<BlobId>, TypedStoreError> {
        self.aggregate_blob_info
            .safe_iter()
            .expect("aggregate_blob_info cf must always exist in storage node")
            .map(|r| r.map(|(k, _)| k))
            .collect()
    }

    pub fn insert_batch<'a>(
        &self,
        batch: &mut DBBatch,
        new_vals: impl IntoIterator<Item = (&'a BlobId, &'a BlobInfo)>,
    ) -> Result<(), TypedStoreError> {
        batch.insert_batch(&self.aggregate_blob_info, new_vals)?;
        Ok(())
    }

    pub fn insert_per_object_batch<'a>(
        &self,
        batch: &mut DBBatch,
        new_vals: impl IntoIterator<Item = (&'a ObjectID, &'a PerObjectBlobInfo)>,
    ) -> Result<(), TypedStoreError> {
        batch.insert_batch(&self.per_object_blob_info, new_vals)?;
        Ok(())
    }
}

/// An iterator over the blob info table.
pub(crate) struct BlobInfoIter<B, T: CertifiedBlobInfoApi, I: ?Sized>
where
    I: Iterator<Item = Result<(B, T), TypedStoreError>> + Send,
{
    iter: Box<I>,
    before_epoch: Epoch,
}

impl<B, T: CertifiedBlobInfoApi, I: ?Sized> BlobInfoIter<B, T, I>
where
    I: Iterator<Item = Result<(B, T), TypedStoreError>> + Send,
{
    pub fn new(iter: Box<I>, before_epoch: Epoch) -> Self {
        Self { iter, before_epoch }
    }
}

impl<B, T: CertifiedBlobInfoApi, I: ?Sized> Debug for BlobInfoIter<B, T, I>
where
    I: Iterator<Item = Result<(B, T), TypedStoreError>> + Send,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlobInfoIter")
            .field("before_epoch", &self.before_epoch)
            .finish()
    }
}

impl<B, T: CertifiedBlobInfoApi, I: ?Sized> Iterator for BlobInfoIter<B, T, I>
where
    I: Iterator<Item = Result<(B, T), TypedStoreError>> + Send,
{
    type Item = Result<(B, T), TypedStoreError>;

    fn next(&mut self) -> Option<Self::Item> {
        for item in self.iter.by_ref() {
            let Ok((_, blob_info)) = &item else {
                return Some(item);
            };

            // The iterator should return blobs that are certified before `before_epoch` and
            // are valid and remain certified at `before_epoch`.
            //
            // It is important to only return certified blobs certified before `before_epoch`
            // because we don't want to fetch blobs that are just certified at `before_epoch`.
            if matches!(
                blob_info.initial_certified_epoch(),
                Some(initial_certified_epoch) if initial_certified_epoch < self.before_epoch
            ) && blob_info.is_certified(self.before_epoch)
            {
                return Some(item);
            }
        }
        None
    }
}

pub(super) trait ToBytes: Serialize + Sized {
    /// Converts the value to a `Vec<u8>`.
    ///
    /// Uses BCS encoding (which is assumed to succeed) by default.
    fn to_bytes(&self) -> Vec<u8> {
        bcs::to_bytes(self).expect("value must be BCS-serializable")
    }
}
trait Mergeable: ToBytes + Debug + DeserializeOwned + Serialize + Sized {
    type MergeOperand: Debug + DeserializeOwned + ToBytes;
    type Key: Debug + DeserializeOwned + std::fmt::Display;

    /// Updates the existing blob info with the provided merge operand and returns the result.
    ///
    /// Returns the preexisting value if the merge fails. An error is logged in this case.
    #[must_use]
    fn merge_with(self, operand: Self::MergeOperand) -> Self;

    /// Creates a new object of `Self` applying the merge operand without preexisting value.
    ///
    /// Returns `None` if the merge fails. An error is logged in this case.
    #[must_use]
    fn merge_new(operand: Self::MergeOperand) -> Option<Self>;

    /// Updates the (optionally) existing blob info with the provided merge operand and returns the
    /// result.
    ///
    /// Returns the preexisting value if the merge fails. An error is logged in this case.
    #[must_use]
    fn merge(existing_val: Option<Self>, operand: Self::MergeOperand) -> Option<Self> {
        match existing_val {
            Some(existing_val) => Some(existing_val.merge_with(operand)),
            None => Self::merge_new(operand),
        }
    }
}

/// Trait defining methods for retrieving information about a certified blob.
#[enum_dispatch]
pub(crate) trait CertifiedBlobInfoApi {
    /// Returns true iff there exists at least one non-expired and certified deletable or permanent
    /// `Blob` object.
    fn is_certified(&self, current_epoch: Epoch) -> bool;

    /// Returns the epoch at which this blob was first certified.
    ///
    /// Returns `None` if it isn't certified.
    fn initial_certified_epoch(&self) -> Option<Epoch>;
}

/// Trait defining methods for retrieving information about a blob.
// NB: Before adding functions to this trait, think twice if you really need it as it needs to be
// implementable by future internal representations of the blob status as well.
#[enum_dispatch]
pub(crate) trait BlobInfoApi: CertifiedBlobInfoApi {
    /// Returns a boolean indicating whether the metadata of the blob is stored.
    fn is_metadata_stored(&self) -> bool;

    /// Returns true iff there exists at least one non-expired deletable or permanent `Blob` object.
    fn is_registered(&self, current_epoch: Epoch) -> bool;

    /// Returns true iff the data of the blob can be deleted at the given epoch. The default
    /// implementation simply checks if the blob is registered in that epoch.
    fn can_data_be_deleted(&self, current_epoch: Epoch) -> bool {
        !self.is_registered(current_epoch)
    }

    /// Returns true iff the *blob info* can be deleted at the given epoch. This is a stronger
    /// condition than whether the data can be deleted as it also checks that no deletable blob
    /// objects exist in the per-object blob info table.
    fn can_blob_info_be_deleted(&self, current_epoch: Epoch) -> bool;

    /// Returns the event through which this blob was marked invalid.
    ///
    /// Returns `None` if it isn't invalid.
    fn invalidation_event(&self) -> Option<EventID>;

    /// Converts the blob information to a `BlobStatus` object.
    fn to_blob_status(&self, current_epoch: Epoch) -> BlobStatus;
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Clone)]
pub(super) struct BlobStatusChangeInfo {
    pub(super) blob_id: BlobId,
    pub(super) deletable: bool,
    pub(super) epoch: Epoch,
    pub(super) end_epoch: Epoch,
    pub(super) status_event: EventID,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Clone, Copy)]
pub(super) enum BlobStatusChangeType {
    Register,
    Certify,
    // INV: Can only be applied to a certified blob.
    Extend,
    Delete { was_certified: bool },
}

trait ChangeTypeAndInfo {
    fn change_type(&self) -> BlobStatusChangeType;
    fn change_info(&self) -> BlobStatusChangeInfo;
}

impl ChangeTypeAndInfo for BlobRegistered {
    fn change_type(&self) -> BlobStatusChangeType {
        BlobStatusChangeType::Register
    }

    fn change_info(&self) -> BlobStatusChangeInfo {
        BlobStatusChangeInfo {
            blob_id: self.blob_id,
            deletable: self.deletable,
            epoch: self.epoch,
            end_epoch: self.end_epoch,
            status_event: self.event_id,
        }
    }
}

impl ChangeTypeAndInfo for BlobCertified {
    fn change_type(&self) -> BlobStatusChangeType {
        if self.is_extension {
            BlobStatusChangeType::Extend
        } else {
            BlobStatusChangeType::Certify
        }
    }

    fn change_info(&self) -> BlobStatusChangeInfo {
        BlobStatusChangeInfo {
            blob_id: self.blob_id,
            deletable: self.deletable,
            epoch: self.epoch,
            end_epoch: self.end_epoch,
            status_event: self.event_id,
        }
    }
}

impl ChangeTypeAndInfo for BlobDeleted {
    fn change_type(&self) -> BlobStatusChangeType {
        BlobStatusChangeType::Delete {
            was_certified: self.was_certified,
        }
    }

    fn change_info(&self) -> BlobStatusChangeInfo {
        BlobStatusChangeInfo {
            blob_id: self.blob_id,
            deletable: true,
            epoch: self.epoch,
            end_epoch: self.end_epoch,
            status_event: self.event_id,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Clone)]
pub(super) enum BlobInfoMergeOperand {
    MarkMetadataStored(bool),
    MarkInvalid {
        epoch: Epoch,
        status_event: EventID,
    },
    ChangeStatus {
        change_type: BlobStatusChangeType,
        change_info: BlobStatusChangeInfo,
    },
    // Adding a new variant should be fine as it does not affect the serialization of existing
    // variants.
    DeletableExpired {
        was_certified: bool,
    },
    PermanentExpired {
        was_certified: bool,
    },
}

impl ToBytes for BlobInfoMergeOperand {}

impl BlobInfoMergeOperand {
    #[cfg(test)]
    pub fn new_change_for_testing(
        change_type: BlobStatusChangeType,
        deletable: bool,
        epoch: Epoch,
        end_epoch: Epoch,
        status_event: EventID,
    ) -> Self {
        Self::ChangeStatus {
            change_type,
            change_info: BlobStatusChangeInfo {
                blob_id: walrus_core::test_utils::blob_id_from_u64(42),
                deletable,
                epoch,
                end_epoch,
                status_event,
            },
        }
    }
}

impl<T: ChangeTypeAndInfo> From<&T> for BlobInfoMergeOperand {
    fn from(value: &T) -> Self {
        Self::ChangeStatus {
            change_type: value.change_type(),
            change_info: value.change_info(),
        }
    }
}

impl From<&InvalidBlobId> for BlobInfoMergeOperand {
    fn from(value: &InvalidBlobId) -> Self {
        let InvalidBlobId {
            epoch,
            event_id,
            blob_id: _,
        } = value;
        Self::MarkInvalid {
            epoch: *epoch,
            status_event: *event_id,
        }
    }
}

impl From<&BlobEvent> for BlobInfoMergeOperand {
    fn from(value: &BlobEvent) -> Self {
        match value {
            BlobEvent::Registered(event) => event.into(),
            BlobEvent::Certified(event) => event.into(),
            BlobEvent::Deleted(event) => event.into(),
            BlobEvent::InvalidBlobID(event) => event.into(),
            BlobEvent::DenyListBlobDeleted(_) => {
                // TODO (WAL-424): Implement DenyListBlobDeleted event handling.
                // Note: It's fine to panic here with a todo!, because in order to trigger this
                // event, we need f+1 signatures and until the Rust integration is implemented no
                // such event should be emitted.
                todo!("DenyListBlobDeleted event handling is not yet implemented");
            }
        }
    }
}

/// Represents the status of a blob.
///
/// Currently only used for testing.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Copy)]
#[repr(u8)]
#[cfg(test)]
pub(super) enum BlobCertificationStatus {
    Registered,
    Certified,
    Invalid,
}

#[cfg(test)]
impl PartialOrd for BlobCertificationStatus {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
impl Ord for BlobCertificationStatus {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering::*;

        use BlobCertificationStatus::*;

        match (self, other) {
            (Registered, Certified) | (Registered, Invalid) | (Certified, Invalid) => Less,
            (left, right) if left == right => Equal,
            _ => Greater,
        }
    }
}

/// Internal representation of the aggregate blob information for use in the database etc. Use
/// [`walrus_storage_node_client::api::BlobStatus`] for anything public facing (e.g., communication
/// to the client).
#[enum_dispatch(CertifiedBlobInfoApi)]
#[enum_dispatch(BlobInfoApi)]
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Clone)]
pub(crate) enum BlobInfo {
    V1(BlobInfoV1),
}

impl BlobInfo {
    /// Creates a new (permanent) blob for testing purposes.
    #[cfg(test)]
    pub(super) fn new_for_testing(
        end_epoch: Epoch,
        status: BlobCertificationStatus,
        current_status_event: EventID,
        _registered_epoch: Option<Epoch>,
        certified_epoch: Option<Epoch>,
        invalidated_epoch: Option<Epoch>,
    ) -> Self {
        use blob_info_v1::PermanentBlobInfoV1;

        let blob_info = match status {
            BlobCertificationStatus::Invalid => BlobInfoV1::Invalid {
                epoch: invalidated_epoch
                    .expect("invalidated_epoch must be provided for Invalid status"),
                event: current_status_event,
            },

            BlobCertificationStatus::Registered | BlobCertificationStatus::Certified => {
                let permanent_total =
                    PermanentBlobInfoV1::new_first(end_epoch, current_status_event);
                let permanent_certified = matches!(status, BlobCertificationStatus::Certified)
                    .then(|| permanent_total.clone());
                ValidBlobInfoV1 {
                    permanent_total: Some(permanent_total),
                    permanent_certified,
                    initial_certified_epoch: certified_epoch,
                    ..Default::default()
                }
                .into()
            }
        };
        Self::V1(blob_info)
    }
}

impl ToBytes for BlobInfo {}

impl Mergeable for BlobInfo {
    type MergeOperand = BlobInfoMergeOperand;
    type Key = BlobId;

    fn merge_with(self, operand: Self::MergeOperand) -> Self {
        match self {
            Self::V1(value) => Self::V1(value.merge_with(operand)),
        }
    }

    fn merge_new(operand: Self::MergeOperand) -> Option<Self> {
        BlobInfoV1::merge_new(operand).map(Self::from)
    }
}

mod per_object_blob_info {
    use super::*;

    #[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Clone)]
    pub(crate) struct PerObjectBlobInfoMergeOperand {
        pub change_type: BlobStatusChangeType,
        pub change_info: BlobStatusChangeInfo,
    }

    impl ToBytes for PerObjectBlobInfoMergeOperand {}

    impl PerObjectBlobInfoMergeOperand {
        pub fn from_blob_info_merge_operand(
            blob_info_merge_operand: BlobInfoMergeOperand,
        ) -> Option<Self> {
            let BlobInfoMergeOperand::ChangeStatus {
                change_type,
                change_info,
            } = blob_info_merge_operand
            else {
                return None;
            };
            Some(Self {
                change_type,
                change_info,
            })
        }
    }

    impl<T: ChangeTypeAndInfo> From<&T> for PerObjectBlobInfoMergeOperand {
        fn from(value: &T) -> Self {
            Self {
                change_type: value.change_type(),
                change_info: value.change_info(),
            }
        }
    }

    /// Trait defining methods for retrieving information about a blob object.
    // NB: Before adding functions to this trait, think twice if you really need it as it needs to
    // be implementable by future internal representations of the per-object blob status as well.
    #[enum_dispatch]
    #[allow(dead_code)]
    pub(crate) trait PerObjectBlobInfoApi: CertifiedBlobInfoApi {
        /// Returns the blob ID associated with this object.
        fn blob_id(&self) -> BlobId;
        /// Returns true iff the object is deletable.
        fn is_deletable(&self) -> bool;
        /// Returns true iff the object is not expired and not deleted.
        fn is_registered(&self, current_epoch: Epoch) -> bool;
        /// Returns true iff the object is already deleted.
        fn is_deleted(&self) -> bool;
    }

    #[enum_dispatch(CertifiedBlobInfoApi)]
    #[enum_dispatch(PerObjectBlobInfoApi)]
    #[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Clone)]
    pub(crate) enum PerObjectBlobInfo {
        V1(PerObjectBlobInfoV1),
    }

    impl PerObjectBlobInfo {
        #[cfg(test)]
        pub(crate) fn new_for_testing(
            blob_id: BlobId,
            registered_epoch: Epoch,
            certified_epoch: Option<Epoch>,
            end_epoch: Epoch,
            deletable: bool,
            event: EventID,
            deleted: bool,
        ) -> Self {
            Self::V1(PerObjectBlobInfoV1 {
                blob_id,
                registered_epoch,
                certified_epoch,
                end_epoch,
                deletable,
                event,
                deleted,
            })
        }
    }

    impl ToBytes for PerObjectBlobInfo {}

    impl Mergeable for PerObjectBlobInfo {
        type MergeOperand = PerObjectBlobInfoMergeOperand;
        type Key = ObjectID;

        fn merge_with(self, operand: Self::MergeOperand) -> Self {
            match self {
                Self::V1(value) => Self::V1(value.merge_with(operand)),
            }
        }

        fn merge_new(operand: Self::MergeOperand) -> Option<Self> {
            PerObjectBlobInfoV1::merge_new(operand).map(Self::from)
        }
    }

    pub(crate) use super::blob_info_v1::PerObjectBlobInfoV1;
}

fn deserialize_from_db<'de, T>(data: &'de [u8]) -> Option<T>
where
    T: Deserialize<'de>,
{
    bcs::from_bytes(data)
        .inspect_err(|error| {
            tracing::error!(
                ?error,
                ?data,
                "failed to deserialize value stored in database"
            )
        })
        .ok()
}

#[tracing::instrument(
    level = Level::DEBUG,
    skip_all,
    fields(existing_val = existing_val.is_some(), key = tracing::field::Empty)
)]
fn merge_mergeable<T: Mergeable>(
    key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let mut current_val: Option<T> = existing_val.and_then(deserialize_from_db);
    let key_str = if cfg!(debug_assertions) {
        // In debug mode, we deserialize the key for more readable logging.
        bcs::from_bytes::<T::Key>(key)
            .expect("key must be valid")
            .to_string()
    } else {
        format!("{key:?}")
    };
    tracing::Span::current().record("key", &key_str);
    tracing::debug!(operands_count = operands.len(), "merging blob info");

    for operand_bytes in operands {
        let Some(operand) = deserialize_from_db::<T::MergeOperand>(operand_bytes) else {
            continue;
        };
        tracing::trace!(?current_val, ?operand, "applying operand");

        current_val = T::merge(current_val, operand);
    }
    tracing::debug!(final_val = ?current_val, "finished merging blob info");

    current_val.as_ref().map(|value| value.to_bytes())
}

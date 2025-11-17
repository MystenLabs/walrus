// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Keeping track of the status of blob IDs and on-chain `Blob` objects.

use std::{
    fmt::Debug,
    num::NonZeroU32,
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
use walrus_storage_node_client::api::{BlobStatus, DeletableCounts, ManagedBlobCounts};
use walrus_sui::types::{
    BlobCertified,
    BlobDeleted,
    BlobEvent,
    BlobRegistered,
    InvalidBlobId,
    ManagedBlobCertified,
    ManagedBlobDeleted,
    ManagedBlobRegistered,
};

use self::per_object_blob_info::PerObjectBlobInfoMergeOperand;
pub(crate) use self::per_object_blob_info::{PerObjectBlobInfo, PerObjectBlobInfoApi};
use super::{DatabaseTableOptionsFactory, constants};
use crate::node::metrics::NodeMetricSet;

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
        if let Some(object_id) = event.object_id() {
            let per_object_operation =
                PerObjectBlobInfoMergeOperand::from_blob_info_merge_operand(operation)
                    .expect("we know this is a registered, certified, or deleted event");
            batch.partial_merge_batch(
                &self.per_object_blob_info,
                [(object_id, per_object_operation.to_bytes())],
            )?;
        }

        batch.insert_batch(&latest_handled_event_index, [(&(), event_index)])?;
        batch.write()
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
            | BlobEvent::DenyListBlobDeleted(_)
            | BlobEvent::ManagedBlobRegistered(_)
            | BlobEvent::ManagedBlobCertified(_)
            | BlobEvent::ManagedBlobDeleted(_) => {
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

    /// Returns an iterator over all entries in the aggregate blob info table.
    pub fn aggregate_blob_info_iter(
        &self,
    ) -> Result<impl Iterator<Item = Result<(BlobId, BlobInfo), TypedStoreError>>, TypedStoreError>
    {
        self.aggregate_blob_info.safe_iter()
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
    #[tracing::instrument(skip_all, fields(walrus.epoch = %current_epoch))]
    pub(crate) async fn process_expired_blob_objects(
        &self,
        current_epoch: Epoch,
        node_metrics: &NodeMetricSet,
    ) -> anyhow::Result<()> {
        tracing::info!("starting to process expired blob objects");
        let mut cleaned_up_objects_count = 0;
        let start_time = Instant::now();

        for entry in self.per_object_blob_info.safe_iter()? {
            let (object_id, per_object_blob_info) = match entry {
                Ok(entry) => entry,
                Err(error) => {
                    tracing::warn!(
                        ?error,
                        "error encountered while iterating over per-object blob info"
                    );
                    break;
                }
            };

            if per_object_blob_info.is_registered(current_epoch) {
                tracing::trace!(
                    %object_id,
                    ?per_object_blob_info,
                    "skipping blob-info update for blob that is still active"
                );
                continue;
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
                batch.partial_merge_batch(
                    &self.aggregate_blob_info,
                    [(blob_id, &operand.to_bytes())],
                )?;
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
                .cleanup_expired_blob_objects_deleted_total
                .inc();
            cleaned_up_objects_count += 1;
        }

        tracing::info!(
            cleaned_up_objects_count,
            duration_seconds = %start_time.elapsed().as_secs_f64(),
            "finished processing expired blob objects",
        );

        Ok(())
    }

    /// Checks some internal invariants of the blob info table.
    ///
    /// The checks are not exhaustive yet.
    pub fn check_invariants(&self) -> Result<(), anyhow::Error> {
        for result in self.per_object_blob_info.safe_iter()? {
            let Ok((object_id, PerObjectBlobInfo::V1(per_object_blob_info))) = result else {
                return Err(anyhow::anyhow!(
                    "error encountered while iterating over per-object blob info: {result:?}"
                ));
            };
            let blob_id = per_object_blob_info.blob_id();
            let Some(blob_info) = self.aggregate_blob_info.get(&blob_id)? else {
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

        for result in self.aggregate_blob_info.safe_iter()? {
            let Ok((blob_id, blob_info)) = result else {
                return Err(anyhow::anyhow!(
                    "error encountered while iterating over aggregate blob info: {result:?}"
                ));
            };
            let BlobInfo::V1(BlobInfoV1::Valid(blob_info)) = blob_info else {
                continue;
            };
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

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Clone)]
pub(crate) struct BlobStatusChangeInfo {
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
    Delete {
        was_certified: bool,
    },
    /// Register a managed blob (owned by BlobManager).
    RegisterManaged {
        blob_manager_id: ObjectID,
    },
    CertifyManaged {
        blob_manager_id: ObjectID,
    },
    /// Delete a managed blob for a specific BlobManager.
    DeleteManaged {
        blob_manager_id: ObjectID,
        was_certified: bool,
    },
}

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

impl From<&BlobRegistered> for BlobInfoMergeOperand {
    fn from(value: &BlobRegistered) -> Self {
        let BlobRegistered {
            epoch,
            end_epoch,
            event_id,
            deletable,
            blob_id,
            ..
        } = value;
        Self::ChangeStatus {
            change_info: BlobStatusChangeInfo {
                deletable: *deletable,
                epoch: *epoch,
                end_epoch: *end_epoch,
                status_event: *event_id,
                blob_id: *blob_id,
            },
            change_type: BlobStatusChangeType::Register,
        }
    }
}

impl From<&BlobCertified> for BlobInfoMergeOperand {
    fn from(value: &BlobCertified) -> Self {
        let BlobCertified {
            epoch,
            end_epoch,
            event_id,
            deletable,
            is_extension,
            blob_id,
            ..
        } = value;
        let change_info = BlobStatusChangeInfo {
            deletable: *deletable,
            epoch: *epoch,
            end_epoch: *end_epoch,
            status_event: *event_id,
            blob_id: *blob_id,
        };
        let change_type = if *is_extension {
            BlobStatusChangeType::Extend
        } else {
            BlobStatusChangeType::Certify
        };
        Self::ChangeStatus {
            change_type,
            change_info,
        }
    }
}

impl From<&BlobDeleted> for BlobInfoMergeOperand {
    fn from(value: &BlobDeleted) -> Self {
        let BlobDeleted {
            epoch,
            end_epoch,
            was_certified,
            event_id,
            blob_id,
            ..
        } = value;
        Self::ChangeStatus {
            change_type: BlobStatusChangeType::Delete {
                was_certified: *was_certified,
            },
            change_info: BlobStatusChangeInfo {
                deletable: true,
                epoch: *epoch,
                end_epoch: *end_epoch,
                status_event: *event_id,
                blob_id: *blob_id,
            },
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

impl From<&ManagedBlobRegistered> for BlobInfoMergeOperand {
    fn from(value: &ManagedBlobRegistered) -> Self {
        let ManagedBlobRegistered {
            epoch,
            blob_manager_id,
            blob_id,
            end_epoch,
            deletable,
            event_id,
            ..
        } = value;
        tracing::debug!(
            "Converting ManagedBlobRegistered event: blob_id={:?}, blob_manager_id={:?}, \
             epoch={}, end_epoch={}, deletable={}",
            blob_id,
            blob_manager_id,
            epoch,
            end_epoch,
            deletable
        );
        Self::ChangeStatus {
            change_info: BlobStatusChangeInfo {
                deletable: *deletable,
                epoch: *epoch,
                end_epoch: *end_epoch,
                status_event: *event_id,
                blob_id: *blob_id,
            },
            change_type: BlobStatusChangeType::RegisterManaged {
                blob_manager_id: *blob_manager_id,
            },
        }
    }
}

impl From<&ManagedBlobCertified> for BlobInfoMergeOperand {
    fn from(value: &ManagedBlobCertified) -> Self {
        let ManagedBlobCertified {
            epoch,
            blob_manager_id,
            blob_id,
            deletable,
            event_id,
            ..
        } = value;
        Self::ChangeStatus {
            change_info: BlobStatusChangeInfo {
                deletable: *deletable,
                epoch: *epoch,
                end_epoch: 0, // Managed blobs don't have direct end_epoch.
                status_event: *event_id,
                blob_id: *blob_id,
            },
            change_type: BlobStatusChangeType::CertifyManaged {
                blob_manager_id: *blob_manager_id,
            },
        }
    }
}

impl From<&ManagedBlobDeleted> for BlobInfoMergeOperand {
    fn from(value: &ManagedBlobDeleted) -> Self {
        let ManagedBlobDeleted {
            epoch,
            blob_manager_id,
            blob_id,
            end_epoch,
            was_certified,
            event_id,
            ..
        } = value;
        Self::ChangeStatus {
            change_info: BlobStatusChangeInfo {
                deletable: true, // Deleted blobs must have been deletable.
                epoch: *epoch,
                end_epoch: *end_epoch,
                status_event: *event_id,
                blob_id: *blob_id,
            },
            change_type: BlobStatusChangeType::DeleteManaged {
                blob_manager_id: *blob_manager_id,
                was_certified: *was_certified,
            },
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
            BlobEvent::ManagedBlobRegistered(event) => event.into(),
            BlobEvent::ManagedBlobCertified(event) => event.into(),
            BlobEvent::ManagedBlobDeleted(event) => event.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum BlobInfoV1 {
    Invalid { epoch: Epoch, event: EventID },
    Valid(ValidBlobInfoV1),
}

impl ToBytes for BlobInfoV1 {}

// INV: count_deletable_total >= count_deletable_certified
// INV: permanent_total.is_none() => permanent_certified.is_none()
// INV: permanent_total.count >= permanent_certified.count
// INV: permanent_total.end_epoch >= permanent_certified.end_epoch
// INV: initial_certified_epoch.is_some()
//      <=> count_deletable_certified > 0 || permanent_certified.is_some()
// INV: latest_seen_deletable_registered_end_epoch >= latest_seen_deletable_certified_end_epoch
// See the `check_invariants` method for more details.
// Important: This struct MUST NOT be changed. Instead, if needed, a new `ValidBlobInfoV2` struct
// should be created.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub(crate) struct ValidBlobInfoV1 {
    pub is_metadata_stored: bool,
    pub count_deletable_total: u32,
    pub count_deletable_certified: u32,
    pub permanent_total: Option<PermanentBlobInfoV1>,
    pub permanent_certified: Option<PermanentBlobInfoV1>,
    pub initial_certified_epoch: Option<Epoch>,

    // Note: The following helper fields were used in the past to approximate the blob status for
    // deletable blobs. They are still used for the following reason: When deletable blobs expire,
    // we update the aggregate blob info in the background. So there is a delay between an epoch
    // change and the blob info being updated. During this delay, these fields are useful to
    // determine that a blob is already expired.
    /// The latest end epoch recorded for a registered deletable blob.
    pub latest_seen_deletable_registered_end_epoch: Option<Epoch>,
    /// The latest end epoch recorded for a certified deletable blob.
    pub latest_seen_deletable_certified_end_epoch: Option<Epoch>,
}

impl From<ValidBlobInfoV1> for BlobInfoV1 {
    fn from(value: ValidBlobInfoV1) -> Self {
        Self::Valid(value)
    }
}

impl ValidBlobInfoV1 {
    fn to_blob_status(&self, current_epoch: Epoch) -> BlobStatus {
        // The check of the `latest_seen_*_epoch` fields is there to reduce the cases where we
        // report the existence of deletable blobs that are already expired.
        let count_deletable_total = if self
            .latest_seen_deletable_registered_end_epoch
            .is_some_and(|e| e > current_epoch)
        {
            self.count_deletable_total
        } else {
            Default::default()
        };
        let count_deletable_certified = if self
            .latest_seen_deletable_certified_end_epoch
            .is_some_and(|e| e > current_epoch)
        {
            self.count_deletable_certified
        } else {
            Default::default()
        };
        let deletable_counts = DeletableCounts {
            count_deletable_total,
            count_deletable_certified,
        };

        let initial_certified_epoch = self.initial_certified_epoch;
        if let Some(PermanentBlobInfoV1 {
            end_epoch, event, ..
        }) = self.permanent_certified.as_ref()
            && *end_epoch > current_epoch
        {
            return BlobStatus::Permanent {
                end_epoch: *end_epoch,
                is_certified: true,
                status_event: *event,
                deletable_counts,
                initial_certified_epoch,
                managed_blob_counts: ManagedBlobCounts::default(),
            };
        }
        if let Some(PermanentBlobInfoV1 {
            end_epoch, event, ..
        }) = self.permanent_total.as_ref()
            && *end_epoch > current_epoch
        {
            return BlobStatus::Permanent {
                end_epoch: *end_epoch,
                is_certified: false,
                status_event: *event,
                deletable_counts,
                initial_certified_epoch,
                managed_blob_counts: ManagedBlobCounts::default(),
            };
        }

        if deletable_counts != Default::default() {
            BlobStatus::Deletable {
                initial_certified_epoch,
                deletable_counts,
                managed_blob_counts: ManagedBlobCounts::default(),
            }
        } else {
            BlobStatus::Nonexistent
        }
    }

    // The counts of deletable blobs are decreased when they expire. However, because this only
    // happens in the background, it is possible during a short time period at the beginning of an
    // epoch that all deletable blobs expired but the counts are not updated yet. For this case, we
    // use the `latest_seen_deletable_certified_end_epoch` field as an additional check.
    //
    // There is still a corner case where the blob with the
    // `latest_seen_deletable_certified_end_epoch` was deleted and all other blobs expired. In this
    // case, we would nevertheless return `true` during a short time period, before the counts are
    // updated in the background.
    fn is_certified(&self, current_epoch: Epoch) -> bool {
        let exists_certified_permanent_blob = self
            .permanent_certified
            .as_ref()
            .is_some_and(|p| p.end_epoch > current_epoch);
        let probably_exists_certified_deletable_blob = self.count_deletable_certified > 0
            && self
                .latest_seen_deletable_certified_end_epoch
                .is_some_and(|e| e > current_epoch);
        self.initial_certified_epoch
            .is_some_and(|epoch| epoch <= current_epoch)
            && (exists_certified_permanent_blob || probably_exists_certified_deletable_blob)
    }

    #[tracing::instrument]
    fn update_status(
        &mut self,
        change_type: BlobStatusChangeType,
        change_info: BlobStatusChangeInfo,
    ) {
        let was_certified = self.is_certified(change_info.epoch);
        if change_info.deletable {
            match change_type {
                BlobStatusChangeType::Register => {
                    self.count_deletable_total += 1;
                    self.maybe_increase_latest_deletable_registered_epoch(change_info.end_epoch);
                }
                BlobStatusChangeType::RegisterManaged { .. }
                | BlobStatusChangeType::CertifyManaged { .. }
                | BlobStatusChangeType::DeleteManaged { .. } => {
                    tracing::error!(
                        "attempt managed blob operation on V1 (regular blob): \
                         blob_id={:?}, change_type={:?}",
                        change_info.blob_id,
                        change_type,
                    );
                    return;
                }
                BlobStatusChangeType::Certify => {
                    if self.count_deletable_total <= self.count_deletable_certified {
                        tracing::error!(
                            "attempt to certify a deletable blob before corresponding register"
                        );
                        return;
                    }
                    self.count_deletable_certified += 1;
                    self.maybe_increase_latest_deletable_certified_epoch(change_info.end_epoch);
                }
                BlobStatusChangeType::Extend => {
                    self.maybe_increase_latest_deletable_registered_epoch(change_info.end_epoch);
                    self.maybe_increase_latest_deletable_certified_epoch(change_info.end_epoch);
                }
                BlobStatusChangeType::Delete { was_certified } => {
                    self.update_deletable_counters_and_end_epochs(was_certified);
                }
            }
        } else {
            match change_type {
                BlobStatusChangeType::Register => {
                    Self::register_permanent(&mut self.permanent_total, &change_info);
                }
                BlobStatusChangeType::RegisterManaged { .. }
                | BlobStatusChangeType::CertifyManaged { .. }
                | BlobStatusChangeType::DeleteManaged { .. } => {
                    tracing::error!(
                        "attempt managed blob operation on V1 (regular blob): \
                         blob_id={:?}, change_type={:?}",
                        change_info.blob_id,
                        change_type,
                    );
                    return;
                }
                BlobStatusChangeType::Certify => {
                    if !Self::certify_permanent(
                        &self.permanent_total,
                        &mut self.permanent_certified,
                        &change_info,
                    ) {
                        // Return early to prevent updating the `initial_certified_epoch` below.
                        return;
                    }
                }
                BlobStatusChangeType::Extend => {
                    Self::extend_permanent(&mut self.permanent_total, &change_info);
                    Self::extend_permanent(&mut self.permanent_certified, &change_info);
                }
                BlobStatusChangeType::Delete { .. } => {
                    tracing::error!("attempt to delete a permanent blob");
                    return;
                }
            }
        }

        // Update initial certified epoch for V1 operations only.
        match change_type {
            BlobStatusChangeType::Certify => {
                self.update_initial_certified_epoch(change_info.epoch, !was_certified);
            }
            BlobStatusChangeType::Delete { .. } => {
                self.maybe_unset_initial_certified_epoch();
            }
            // Explicit matches to make sure we cover all cases.
            BlobStatusChangeType::Register | BlobStatusChangeType::Extend => (),
            // Managed blob operations don't affect V1 initial_certified_epoch.
            BlobStatusChangeType::RegisterManaged { .. }
            | BlobStatusChangeType::CertifyManaged { .. }
            | BlobStatusChangeType::DeleteManaged { .. } => (),
        }
    }

    fn deletable_expired(&mut self, was_certified: bool) {
        self.update_deletable_counters_and_end_epochs(was_certified);
        self.maybe_unset_initial_certified_epoch();
    }

    fn update_deletable_counters_and_end_epochs(&mut self, was_certified: bool) {
        Self::decrement_deletable_counter_and_maybe_unset_latest_end_epoch(
            &mut self.count_deletable_total,
            &mut self.latest_seen_deletable_registered_end_epoch,
        );
        if was_certified {
            Self::decrement_deletable_counter_and_maybe_unset_latest_end_epoch(
                &mut self.count_deletable_certified,
                &mut self.latest_seen_deletable_certified_end_epoch,
            );
        }
    }

    fn update_initial_certified_epoch(&mut self, new_certified_epoch: Epoch, force: bool) {
        if force
            || self
                .initial_certified_epoch
                .is_none_or(|existing_epoch| existing_epoch > new_certified_epoch)
        {
            self.initial_certified_epoch = Some(new_certified_epoch);
        }
    }

    fn maybe_unset_initial_certified_epoch(&mut self) {
        if self.count_deletable_certified == 0 && self.permanent_certified.is_none() {
            self.initial_certified_epoch = None;
        }
    }

    fn maybe_increase_latest_deletable_registered_epoch(&mut self, epoch: Epoch) {
        self.latest_seen_deletable_registered_end_epoch = Some(
            epoch.max(
                self.latest_seen_deletable_registered_end_epoch
                    .unwrap_or_default(),
            ),
        )
    }

    fn maybe_increase_latest_deletable_certified_epoch(&mut self, epoch: Epoch) {
        self.latest_seen_deletable_certified_end_epoch = Some(
            epoch.max(
                self.latest_seen_deletable_certified_end_epoch
                    .unwrap_or_default(),
            ),
        )
    }

    /// Decrements a counter on blob deletion and unsets the corresponding latest seen end epoch in
    /// case the counter reaches 0.
    ///
    /// If the counter is already 0, an error is logged in release builds and the function panics in
    /// dev builds.
    fn decrement_deletable_counter_and_maybe_unset_latest_end_epoch(
        counter: &mut u32,
        latest_seen_end_epoch: &mut Option<Epoch>,
    ) {
        debug_assert!(*counter > 0);
        *counter = counter.checked_sub(1).unwrap_or_else(|| {
            tracing::error!("attempt to delete blob when count was already 0");
            0
        });
        if *counter == 0 {
            *latest_seen_end_epoch = None;
        }
    }

    /// Processes a register status change on the [`Option<PermanentBlobInfoV1>`] object
    /// representing all permanent blobs.
    pub(crate) fn register_permanent(
        permanent_total: &mut Option<PermanentBlobInfoV1>,
        change_info: &BlobStatusChangeInfo,
    ) {
        PermanentBlobInfoV1::update_optional(permanent_total, change_info)
    }

    /// Processes a certify status change on the [`PermanentBlobInfoV1`] objects representing all
    /// and the certified permanent blobs.
    ///
    /// Returns whether the update was successful.
    pub(crate) fn certify_permanent(
        permanent_total: &Option<PermanentBlobInfoV1>,
        permanent_certified: &mut Option<PermanentBlobInfoV1>,
        change_info: &BlobStatusChangeInfo,
    ) -> bool {
        let Some(permanent_total) = permanent_total else {
            tracing::error!("attempt to certify a permanent blob when none is tracked");
            return false;
        };

        let registered_end_epoch = permanent_total.end_epoch;
        let certified_end_epoch = change_info.end_epoch;
        if certified_end_epoch > registered_end_epoch {
            tracing::error!(
                registered_end_epoch,
                certified_end_epoch,
                "attempt to certify a permanent blob with later end epoch than any registered blob",
            );
            return false;
        }
        if permanent_total.count.get()
            <= permanent_certified
                .as_ref()
                .map(|p| p.count.get())
                .unwrap_or_default()
        {
            tracing::error!("attempt to certify a permanent blob before corresponding register");
            return false;
        }
        PermanentBlobInfoV1::update_optional(permanent_certified, change_info);
        true
    }

    /// Processes an extend status change on the [`PermanentBlobInfoV1`] object representing the
    /// certified permanent blobs.
    fn extend_permanent(
        permanent_info: &mut Option<PermanentBlobInfoV1>,
        change_info: &BlobStatusChangeInfo,
    ) {
        let Some(permanent_info) = permanent_info else {
            tracing::error!("attempt to extend a permanent blob when none is tracked");
            return;
        };

        permanent_info.update(change_info, false);
    }

    /// Processes a delete status change on the [`PermanentBlobInfoV1`] objects representing all and
    /// the certified permanent blobs.
    ///
    /// This is called when blobs expire at the end of an epoch.
    fn permanent_expired(&mut self, was_certified: bool) {
        Self::decrement_blob_info_inner(&mut self.permanent_total);
        if was_certified {
            Self::decrement_blob_info_inner(&mut self.permanent_certified);
        }
        self.maybe_unset_initial_certified_epoch();
    }

    fn decrement_blob_info_inner(blob_info_inner: &mut Option<PermanentBlobInfoV1>) {
        match blob_info_inner {
            None => tracing::error!("attempt to delete a permanent blob when none is tracked"),
            Some(PermanentBlobInfoV1 { count, .. }) => {
                if count.get() == 1 {
                    *blob_info_inner = None;
                } else {
                    *count = NonZeroU32::new(count.get() - 1)
                        .expect("we just checked that `count` is at least 2")
                }
            }
        }
    }

    /// Checks the invariants of the aggregate blob info, returning an error if any invariant is
    /// violated.
    fn check_invariants(&self) -> anyhow::Result<()> {
        let Self {
            count_deletable_total,
            count_deletable_certified,
            permanent_total,
            permanent_certified,
            initial_certified_epoch,
            latest_seen_deletable_registered_end_epoch,
            latest_seen_deletable_certified_end_epoch,
            ..
        } = self;

        anyhow::ensure!(count_deletable_total >= count_deletable_certified);
        match initial_certified_epoch {
            None => {
                anyhow::ensure!(*count_deletable_certified == 0 && permanent_certified.is_none())
            }
            Some(_) => {
                anyhow::ensure!(*count_deletable_certified > 0 || permanent_certified.is_some())
            }
        }

        match (permanent_total, permanent_certified) {
            (None, Some(_)) => {
                anyhow::bail!("permanent_total.is_none() => permanent_certified.is_none()")
            }
            (Some(total_inner), Some(certified_inner)) => {
                anyhow::ensure!(total_inner.end_epoch >= certified_inner.end_epoch);
                anyhow::ensure!(total_inner.count >= certified_inner.count);
            }
            _ => (),
        }

        match (
            latest_seen_deletable_registered_end_epoch,
            latest_seen_deletable_certified_end_epoch,
        ) {
            (None, Some(_)) => anyhow::bail!(
                "latest_seen_deletable_registered_end_epoch.is_none() => \
                latest_seen_deletable_certified_end_epoch.is_none()"
            ),
            (Some(registered), Some(certified)) => {
                anyhow::ensure!(registered >= certified);
            }
            _ => (),
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct PermanentBlobInfoV1 {
    /// The total number of `Blob` objects for that blob ID with the given status.
    pub count: NonZeroU32,
    /// The latest expiration epoch among these objects.
    pub end_epoch: Epoch,
    /// The ID of the first blob event that led to the status with the given `end_epoch`.
    pub event: EventID,
}

impl PermanentBlobInfoV1 {
    /// Creates a new `PermanentBlobInfoV1` object for the first blob with the given `end_epoch` and
    /// `event`.
    fn new_first(end_epoch: Epoch, event: EventID) -> Self {
        Self {
            count: NonZeroU32::new(1).expect("1 is non-zero"),
            end_epoch,
            event,
        }
    }

    /// Updates `self` with the `change_info`, increasing the count if `increase_count == true`.
    ///
    /// # Panics
    ///
    /// Panics if the change info has `deletable == true`.
    fn update(&mut self, change_info: &BlobStatusChangeInfo, increase_count: bool) {
        assert!(!change_info.deletable);

        if increase_count {
            self.count = self.count.saturating_add(1)
        };
        if change_info.end_epoch > self.end_epoch {
            *self = PermanentBlobInfoV1 {
                count: self.count,
                end_epoch: change_info.end_epoch,
                event: change_info.status_event,
            };
        }
    }

    /// Updates `existing_info` with the change info or creates a new `Self` if the input is `None`.
    ///
    /// # Panics
    ///
    /// Panics if the change info has `deletable == true`.
    fn update_optional(existing_info: &mut Option<Self>, change_info: &BlobStatusChangeInfo) {
        let BlobStatusChangeInfo {
            epoch: _,
            end_epoch: new_end_epoch,
            status_event: new_status_event,
            deletable,
            blob_id: _,
        } = change_info;
        assert!(!deletable);

        match existing_info {
            None => {
                *existing_info = Some(PermanentBlobInfoV1::new_first(
                    *new_end_epoch,
                    *new_status_event,
                ))
            }
            Some(permanent_blob_info) => permanent_blob_info.update(change_info, true),
        }
    }

    #[cfg(test)]
    fn new_fixed_for_testing(count: u32, end_epoch: Epoch, event_seq: u64) -> Self {
        Self {
            count: NonZeroU32::new(count).expect("count must be non-zero"),
            end_epoch,
            event: walrus_sui::test_utils::fixed_event_id_for_testing(event_seq),
        }
    }

    #[cfg(test)]
    fn new_for_testing(count: u32, end_epoch: Epoch) -> Self {
        Self {
            count: NonZeroU32::new(count).expect("count must be non-zero"),
            end_epoch,
            event: walrus_sui::test_utils::event_id_for_testing(),
        }
    }
}

impl CertifiedBlobInfoApi for BlobInfoV1 {
    fn is_certified(&self, current_epoch: Epoch) -> bool {
        if let Self::Valid(valid_blob_info) = self {
            valid_blob_info.is_certified(current_epoch)
        } else {
            false
        }
    }

    fn initial_certified_epoch(&self) -> Option<Epoch> {
        if let Self::Valid(ValidBlobInfoV1 {
            initial_certified_epoch,
            ..
        }) = self
        {
            *initial_certified_epoch
        } else {
            None
        }
    }
}

impl BlobInfoApi for BlobInfoV1 {
    fn is_metadata_stored(&self) -> bool {
        matches!(
            self,
            Self::Valid(ValidBlobInfoV1 {
                is_metadata_stored: true,
                ..
            })
        )
    }

    // Note: See the `is_certified` method for an explanation of the use of the
    // `latest_seen_deletable_registered_end_epoch` field.
    fn is_registered(&self, current_epoch: Epoch) -> bool {
        let Self::Valid(ValidBlobInfoV1 {
            count_deletable_total,
            permanent_total,
            latest_seen_deletable_registered_end_epoch,
            ..
        }) = self
        else {
            return false;
        };

        let exists_registered_permanent_blob = permanent_total
            .as_ref()
            .is_some_and(|p| p.end_epoch > current_epoch);
        let probably_exists_registered_deletable_blob = *count_deletable_total > 0
            && latest_seen_deletable_registered_end_epoch.is_some_and(|e| e > current_epoch);

        exists_registered_permanent_blob || probably_exists_registered_deletable_blob
    }

    fn can_blob_info_be_deleted(&self, current_epoch: Epoch) -> bool {
        match self {
            Self::Invalid { .. } => {
                // We don't know whether there are any deletable blob objects for this blob ID.
                false
            }
            Self::Valid(ValidBlobInfoV1 {
                count_deletable_total,
                permanent_total,
                ..
            }) => {
                *count_deletable_total == 0
                    && permanent_total.is_none()
                    && self.can_data_be_deleted(current_epoch)
            }
        }
    }

    fn invalidation_event(&self) -> Option<EventID> {
        if let Self::Invalid { event, .. } = self {
            Some(*event)
        } else {
            None
        }
    }

    fn to_blob_status(&self, current_epoch: Epoch) -> BlobStatus {
        match self {
            BlobInfoV1::Invalid { event, .. } => BlobStatus::Invalid { event: *event },
            BlobInfoV1::Valid(valid_blob_info) => valid_blob_info.to_blob_status(current_epoch),
        }
    }
}

impl Mergeable for BlobInfoV1 {
    type MergeOperand = BlobInfoMergeOperand;
    type Key = BlobId;

    fn merge_with(mut self, operand: Self::MergeOperand) -> Self {
        match (&mut self, operand) {
            // If the blob is already marked as invalid, do not update the status.
            (Self::Invalid { .. }, _) => (),
            (
                _,
                BlobInfoMergeOperand::MarkInvalid {
                    epoch,
                    status_event,
                },
            ) => {
                return Self::Invalid {
                    epoch,
                    event: status_event,
                };
            }
            (
                Self::Valid(ValidBlobInfoV1 {
                    is_metadata_stored, ..
                }),
                BlobInfoMergeOperand::MarkMetadataStored(new_is_metadata_stored),
            ) => {
                *is_metadata_stored = new_is_metadata_stored;
            }
            (
                Self::Valid(valid_blob_info),
                BlobInfoMergeOperand::ChangeStatus {
                    change_type,
                    change_info,
                },
            ) => valid_blob_info.update_status(change_type, change_info),
            (
                Self::Valid(valid_blob_info),
                BlobInfoMergeOperand::DeletableExpired { was_certified },
            ) => valid_blob_info.deletable_expired(was_certified),
            (
                Self::Valid(valid_blob_info),
                BlobInfoMergeOperand::PermanentExpired { was_certified },
            ) => valid_blob_info.permanent_expired(was_certified),
        }
        self
    }

    fn merge_new(operand: Self::MergeOperand) -> Option<Self> {
        match operand {
            BlobInfoMergeOperand::ChangeStatus {
                change_type: BlobStatusChangeType::Register,
                change_info:
                    BlobStatusChangeInfo {
                        deletable,
                        epoch: _,
                        end_epoch,
                        status_event,
                        blob_id: _,
                    },
            } => Some(
                if deletable {
                    ValidBlobInfoV1 {
                        count_deletable_total: 1,
                        latest_seen_deletable_registered_end_epoch: Some(end_epoch),
                        ..Default::default()
                    }
                } else {
                    ValidBlobInfoV1 {
                        permanent_total: Some(PermanentBlobInfoV1::new_first(
                            end_epoch,
                            status_event,
                        )),
                        ..Default::default()
                    }
                }
                .into(),
            ),
            BlobInfoMergeOperand::MarkInvalid {
                epoch,
                status_event,
            } => Some(BlobInfoV1::Invalid {
                epoch,
                event: status_event,
            }),
            BlobInfoMergeOperand::ChangeStatus { .. }
            | BlobInfoMergeOperand::MarkMetadataStored(_)
            | BlobInfoMergeOperand::DeletableExpired { .. }
            | BlobInfoMergeOperand::PermanentExpired { .. } => {
                tracing::error!(
                    ?operand,
                    "encountered an unexpected update for an untracked blob ID"
                );
                debug_assert!(false);
                None
            }
        }
    }
}

/// BlobInfoV2 extends V1 with support for managed blobs.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum BlobInfoV2 {
    Invalid { epoch: Epoch, event: EventID },
    Valid(ValidBlobInfoV2),
}

impl ToBytes for BlobInfoV2 {}

impl Mergeable for BlobInfoV2 {
    type MergeOperand = BlobInfoMergeOperand;
    type Key = BlobId;

    fn merge_with(mut self, operand: Self::MergeOperand) -> Self {
        match (&mut self, operand) {
            // If the blob is already marked as invalid, do not update the status.
            (Self::Invalid { .. }, _) => (),
            (
                _,
                BlobInfoMergeOperand::MarkInvalid {
                    epoch,
                    status_event,
                },
            ) => {
                return Self::Invalid {
                    epoch,
                    event: status_event,
                };
            }
            (
                Self::Valid(ValidBlobInfoV2 {
                    regular_blob_info,
                    managed_blob_info,
                }),
                BlobInfoMergeOperand::MarkMetadataStored(new_is_metadata_stored),
            ) => {
                // Mark metadata stored for both regular and managed blobs if they exist.
                if let Some(info) = regular_blob_info {
                    info.is_metadata_stored = new_is_metadata_stored;
                }
                if let Some(info) = managed_blob_info {
                    info.is_metadata_stored = new_is_metadata_stored;
                }
            }
            (
                Self::Valid(valid_blob_info),
                BlobInfoMergeOperand::ChangeStatus {
                    change_type,
                    change_info,
                },
            ) => {
                valid_blob_info.update_status(change_type, change_info);
            }
            (
                Self::Valid(ValidBlobInfoV2 {
                    regular_blob_info, ..
                }),
                BlobInfoMergeOperand::DeletableExpired { was_certified },
            ) => {
                // TODO(heliu): Revisit this.
                // Apply the same logic as V1 for expired deletable blobs
                if let Some(info) = regular_blob_info {
                    if was_certified {
                        info.count_deletable_certified =
                            info.count_deletable_certified.saturating_sub(1);
                    }
                    info.count_deletable_total = info.count_deletable_total.saturating_sub(1);
                }
            }
            (
                Self::Valid(ValidBlobInfoV2 {
                    regular_blob_info, ..
                }),
                BlobInfoMergeOperand::PermanentExpired { was_certified },
            ) => {
                // Apply the same logic as V1 for expired permanent blobs
                if let Some(info) = regular_blob_info {
                    if was_certified {
                        info.permanent_certified = None;
                    }
                    info.permanent_total = None;
                }
            }
        }
        self
    }

    fn merge_new(operand: Self::MergeOperand) -> Option<Self> {
        // V2 can only be created from RegisterManaged.
        match operand {
            BlobInfoMergeOperand::ChangeStatus {
                change_type: BlobStatusChangeType::RegisterManaged { blob_manager_id },
                change_info,
            } => Some(Self::Valid(ValidBlobInfoV2::new(
                blob_manager_id,
                change_info.deletable,
            ))),
            _ => None,
        }
    }
}

impl CertifiedBlobInfoApi for BlobInfoV2 {
    fn is_certified(&self, current_epoch: Epoch) -> bool {
        if let Self::Valid(ValidBlobInfoV2 {
            regular_blob_info,
            managed_blob_info,
        }) = self
        {
            regular_blob_info
                .as_ref()
                .is_some_and(|info| info.is_certified(current_epoch))
                || managed_blob_info
                    .as_ref()
                    .is_some_and(|info| info.is_certified(current_epoch))
        } else {
            false
        }
    }

    fn initial_certified_epoch(&self) -> Option<Epoch> {
        if let Self::Valid(valid_blob_info) = self {
            // Return the earliest initial_certified_epoch from regular or managed blobs.
            let regular_epoch = valid_blob_info
                .regular_blob_info
                .as_ref()
                .and_then(|info| info.initial_certified_epoch);
            let managed_epoch = valid_blob_info
                .managed_blob_info
                .as_ref()
                .and_then(|info| info.initial_certified_epoch);

            match (regular_epoch, managed_epoch) {
                (Some(regular), Some(managed)) => Some(regular.min(managed)),
                (Some(epoch), None) | (None, Some(epoch)) => Some(epoch),
                (None, None) => None,
            }
        } else {
            None
        }
    }
}

impl BlobInfoApi for BlobInfoV2 {
    fn is_metadata_stored(&self) -> bool {
        match self {
            Self::Invalid { .. } => false,
            Self::Valid(ValidBlobInfoV2 {
                regular_blob_info,
                managed_blob_info,
            }) => {
                // Check both regular and managed blob metadata storage.
                regular_blob_info
                    .as_ref()
                    .map(|info| info.is_metadata_stored)
                    .unwrap_or(false)
                    || managed_blob_info
                        .as_ref()
                        .map(|info| info.is_metadata_stored)
                        .unwrap_or(false)
            }
        }
    }

    fn is_registered(&self, current_epoch: Epoch) -> bool {
        match self {
            Self::Invalid { .. } => false,
            Self::Valid(ValidBlobInfoV2 {
                regular_blob_info,
                managed_blob_info,
            }) => {
                // Check both regular and managed blob registration.
                regular_blob_info
                    .as_ref()
                    .map(|info| {
                        let v1_blob_info = BlobInfoV1::Valid(info.clone());
                        v1_blob_info.is_registered(current_epoch)
                    })
                    .unwrap_or(false)
                    || managed_blob_info
                        .as_ref()
                        .map(|info| info.is_registered(current_epoch))
                        .unwrap_or(false)
            }
        }
    }

    fn to_blob_status(&self, current_epoch: Epoch) -> BlobStatus {
        match self {
            Self::Invalid { .. } => BlobStatus::Nonexistent,
            Self::Valid(valid_blob_info) => valid_blob_info.to_blob_status(current_epoch),
        }
    }

    // TODO(heliu): Implement this.
    fn can_blob_info_be_deleted(&self, current_epoch: Epoch) -> bool {
        match self {
            Self::Invalid { .. } => {
                // We don't know whether there are any deletable blob objects for this blob ID.
                false
            }
            Self::Valid(ValidBlobInfoV2 {
                regular_blob_info,
                managed_blob_info,
            }) => {
                // TODO: Check if managed blobs can be deleted.
                // For now, if there are any managed blobs, consider it not deletable.
                if let Some(managed_info) = managed_blob_info {
                    if !managed_info.registered.is_empty() {
                        return false;
                    }
                }
                // Otherwise, use V1 logic for regular blobs if it exists.
                if let Some(regular_info) = regular_blob_info {
                    return regular_info.count_deletable_total == 0
                        && regular_info
                            .permanent_total
                            .as_ref()
                            .is_some_and(|p| p.end_epoch <= current_epoch);
                }
                // If both are None, it can be deleted.
                true
            }
        }
    }

    fn invalidation_event(&self) -> Option<EventID> {
        match self {
            Self::Invalid { event, .. } => Some(*event),
            Self::Valid(_) => None,
        }
    }
}

/// Tracks managed blobs by storing which BlobManagers have registered/certified them.
/// This allows checking if any BlobManager still has this blob valid (not expired).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub(crate) struct ManagedBlobInfo {
    /// Whether metadata is stored for this managed blob.
    pub is_metadata_stored: bool,
    /// The epoch when this managed blob was first certified.
    pub initial_certified_epoch: Option<Epoch>,
    /// BlobManagers that have registered this blob (deletable or permanent).
    pub registered: std::collections::HashSet<ObjectID>,
    pub registered_deletable_counts: u32,
    /// BlobManagers that have certified this blob (deletable or permanent).
    pub certified: std::collections::HashSet<ObjectID>,
    pub certified_deletable_counts: u32,
}

impl ManagedBlobInfo {
    fn new(blob_manager_id: ObjectID, deletable: bool) -> Self {
        Self {
            is_metadata_stored: false,
            initial_certified_epoch: None,
            registered: std::collections::HashSet::from([blob_manager_id]),
            registered_deletable_counts: if deletable { 1 } else { 0 },
            certified: std::collections::HashSet::new(),
            // certified_deletable_counts should be 0 on registration,
            // only increment on certification.
            certified_deletable_counts: 0,
        }
    }

    /// Updates the managed blob info based on status change.
    fn update_status(
        &mut self,
        change_type: BlobStatusChangeType,
        change_info: &BlobStatusChangeInfo,
    ) {
        match change_type {
            BlobStatusChangeType::RegisterManaged { blob_manager_id } => {
                tracing::debug!(
                    "Registering managed blob: blob_id={:?}, blob_manager_id={:?}, \
                     deletable={}, registered_count={}",
                    change_info.blob_id,
                    blob_manager_id,
                    change_info.deletable,
                    self.registered.len() + 1
                );
                self.registered.insert(blob_manager_id);
                // Track deletable count.
                if change_info.deletable {
                    self.registered_deletable_counts += 1;
                }
            }
            BlobStatusChangeType::CertifyManaged { blob_manager_id } => {
                // Certify the managed blob for this BlobManager.
                if !self.registered.contains(&blob_manager_id) {
                    tracing::error!(
                        "attempted to certify a managed blob without having it registered:
                        blob_id={:?}, blob_manager_id={:?}",
                        change_info.blob_id,
                        blob_manager_id
                    );
                    return;
                }
                self.certified.insert(blob_manager_id);
                // Track deletable count.
                if change_info.deletable {
                    self.certified_deletable_counts += 1;
                }
                // Update initial certified epoch.
                if self.initial_certified_epoch.is_none() {
                    self.initial_certified_epoch = Some(change_info.epoch);
                }
            }
            BlobStatusChangeType::DeleteManaged {
                blob_manager_id,
                was_certified,
            } => {
                // Remove from registered set.
                if !self.registered.remove(&blob_manager_id) {
                    tracing::error!(
                        "attempted to delete a managed blob that wasn't registered:
                        blob_id={:?}, blob_manager_id={:?}",
                        change_info.blob_id,
                        blob_manager_id
                    );
                    return;
                }

                // Track deletable count.
                if change_info.deletable {
                    self.registered_deletable_counts =
                        self.registered_deletable_counts.saturating_sub(1);
                }

                // If certified, also remove from certified set.
                if was_certified {
                    if !self.certified.remove(&blob_manager_id) {
                        tracing::error!(
                            "attempted to delete a certified managed blob that wasn't in \
                            certified set: blob_id={:?}, blob_manager_id={:?}",
                            change_info.blob_id,
                            blob_manager_id
                        );
                        return;
                    }
                    if change_info.deletable {
                        self.certified_deletable_counts =
                            self.certified_deletable_counts.saturating_sub(1);
                    }
                }
            }
            BlobStatusChangeType::Register
            | BlobStatusChangeType::Certify
            | BlobStatusChangeType::Delete { .. }
            | BlobStatusChangeType::Extend => {
                // Regular blob operations shouldn't apply to managed blobs.
                tracing::warn!(
                    "{:?} operation applied to managed blob info for blob {}",
                    change_type,
                    change_info.blob_id
                );
            }
        }
    }

    fn is_certified(&self, current_epoch: Epoch) -> bool {
        !self.certified.is_empty()
            && self
                .initial_certified_epoch
                .is_some_and(|epoch| epoch <= current_epoch)
    }

    /// Returns true if any BlobManager has registered this blob.
    /// TODO: Check if any BlobManager in registered is still valid at current_epoch.
    fn is_registered(&self, current_epoch: Epoch) -> bool {
        !self.registered.is_empty()
            && self
                .initial_certified_epoch
                .is_some_and(|epoch| epoch <= current_epoch)
    }
}

/// ValidBlobInfoV2 with separate tracking for regular blobs (V1) and managed blobs.
/// This isolates the two types completely.
/// At least one of regular_blob_info or managed_blob_info must be Some.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ValidBlobInfoV2 {
    /// Information about regular blobs (V1-style with all V1 fields).
    pub regular_blob_info: Option<ValidBlobInfoV1>,
    /// Information about managed blobs (owned by BlobManagers).
    pub managed_blob_info: Option<ManagedBlobInfo>,
}

impl Default for ValidBlobInfoV2 {
    fn default() -> Self {
        Self {
            regular_blob_info: None,
            managed_blob_info: None,
        }
    }
}

impl From<ValidBlobInfoV1> for ValidBlobInfoV2 {
    fn from(v1: ValidBlobInfoV1) -> Self {
        Self {
            regular_blob_info: Some(v1),
            managed_blob_info: None,
        }
    }
}

impl ValidBlobInfoV2 {
    /// Creates a new ValidBlobInfoV2 for a managed blob.
    fn new(blob_manager_id: ObjectID, deletable: bool) -> Self {
        Self {
            regular_blob_info: None,
            managed_blob_info: Some(ManagedBlobInfo::new(blob_manager_id, deletable)),
        }
    }

    /// Check if this ValidBlobInfoV2 is effectively empty and can be removed.
    fn is_empty(&self) -> bool {
        // Check if regular_blob_info is empty or None
        let regular_empty = self.regular_blob_info.as_ref().map_or(true, |info| {
            info.count_deletable_total == 0
                && info.count_deletable_certified == 0
                && info.permanent_total.is_none()
                && info.permanent_certified.is_none()
        });

        // Check if managed_blob_info is empty or None
        let managed_empty = self.managed_blob_info.as_ref().map_or(true, |info| {
            info.registered.is_empty() && info.certified.is_empty()
        });

        regular_empty && managed_empty
    }

    /// Updates the status based on a change.
    /// Handles both regular blob operations and managed blob operations.
    fn update_status(
        &mut self,
        change_type: BlobStatusChangeType,
        change_info: BlobStatusChangeInfo,
    ) {
        match change_type {
            BlobStatusChangeType::RegisterManaged { .. } => {
                let managed_info = self
                    .managed_blob_info
                    .get_or_insert_with(ManagedBlobInfo::default);
                managed_info.update_status(change_type, &change_info);
            }
            BlobStatusChangeType::CertifyManaged { blob_manager_id }
            | BlobStatusChangeType::DeleteManaged {
                blob_manager_id, ..
            } => {
                // Managed blob operations update managed blob info.
                // ManagedBlobInfo must exist for these operations.
                let Some(ref mut managed_info) = self.managed_blob_info else {
                    tracing::error!(
                        "attempted to update status of a managed blob without having it registered:
                        blob_id={:?}",
                        change_info.blob_id,
                    );
                    return;
                };
                managed_info.update_status(change_type, &change_info);

                // If DeleteManaged removed all references:
                if matches!(change_type, BlobStatusChangeType::DeleteManaged { .. }) {
                    // Unset initial_certified_epoch when all certified references are removed.
                    if managed_info.certified.is_empty() {
                        managed_info.initial_certified_epoch = None;
                    }

                    // If all managed references are gone, remove managed_blob_info.
                    if managed_info.registered.is_empty() && managed_info.certified.is_empty() {
                        self.managed_blob_info = None;
                    }
                }
            }
            BlobStatusChangeType::Register => {
                let regular_info = self.regular_blob_info.get_or_insert_with(Default::default);
                regular_info.update_status(change_type, change_info);
            }
            BlobStatusChangeType::Certify
            | BlobStatusChangeType::Extend
            | BlobStatusChangeType::Delete { .. } => {
                // Regular blob operations update regular blob info using V1 logic.
                // Create ValidBlobInfoV1 if it doesn't exist.
                let regular_info = self.regular_blob_info.get_or_insert_with(Default::default);
                regular_info.update_status(change_type, change_info.clone());

                // If Delete operation resulted in empty regular blob info, set it to None.
                if matches!(change_type, BlobStatusChangeType::Delete { .. }) {
                    if let Some(info) = &self.regular_blob_info {
                        if info.count_deletable_total == 0
                            && info.count_deletable_certified == 0
                            && info.permanent_total.is_none()
                            && info.permanent_certified.is_none()
                        {
                            self.regular_blob_info = None;
                        }
                    }
                }
            }
        }
    }

    /// Calculates ManagedBlobCounts for V2 managed blobs.
    /// Returns None if there are no managed blobs.
    fn managed_blob_counts(&self, _current_epoch: Epoch) -> Option<ManagedBlobCounts> {
        let managed_info = self.managed_blob_info.as_ref()?;

        if managed_info.certified.is_empty() && managed_info.registered.is_empty() {
            return None;
        }

        // TODO: Query BlobManager objects to check which managed blobs are still
        // valid at current_epoch. For now, we count all registered/certified
        // managed blobs.
        Some(ManagedBlobCounts {
            count_registered_total: managed_info.registered.len() as u32,
            count_certified_total: managed_info.certified.len() as u32,
            count_registered_deletable: managed_info.registered_deletable_counts,
            count_certified_deletable: managed_info.certified_deletable_counts,
        })
    }

    /// Adds managed blob counts to the status with new ordering logic.
    /// Ordering: Nonexistent < Deletable < Managed < Permanent < Invalid
    /// Logic:
    /// - If Permanent: add managed counts to Permanent
    /// - Else if managed counts exist: return Managed with deletable counts
    /// - Else: return Deletable (or original status)
    fn add_managed_counts_to_status(
        status: BlobStatus,
        managed_counts: ManagedBlobCounts,
    ) -> BlobStatus {
        match status {
            // If there is Permanent, add the managed counts.
            BlobStatus::Permanent {
                end_epoch,
                is_certified,
                status_event,
                deletable_counts,
                initial_certified_epoch,
                ..
            } => BlobStatus::Permanent {
                end_epoch,
                is_certified,
                status_event,
                deletable_counts,
                initial_certified_epoch,
                managed_blob_counts: managed_counts,
            },
            // If no Permanent but managed counts exist, use Managed variant.
            BlobStatus::Deletable {
                initial_certified_epoch,
                deletable_counts,
                ..
            } if managed_counts != ManagedBlobCounts::default() => BlobStatus::Managed {
                initial_certified_epoch,
                unmanaged_deletable_counts: deletable_counts,
                managed_blob_counts: managed_counts,
            },
            BlobStatus::Nonexistent if managed_counts != ManagedBlobCounts::default() => {
                BlobStatus::Managed {
                    initial_certified_epoch: None, // TODO: Set from managed blob info.
                    unmanaged_deletable_counts: DeletableCounts::default(),
                    managed_blob_counts: managed_counts,
                }
            }
            // Otherwise, use the Deletable variant (or original status).
            BlobStatus::Deletable {
                initial_certified_epoch,
                deletable_counts,
                ..
            } => BlobStatus::Deletable {
                initial_certified_epoch,
                deletable_counts,
                managed_blob_counts: ManagedBlobCounts::default(),
            },
            BlobStatus::Invalid { event } => BlobStatus::Invalid { event },
            BlobStatus::Nonexistent => BlobStatus::Nonexistent,
            BlobStatus::Managed { .. } => {
                // This shouldn't happen as V1 never returns Managed.
                // But if it does, just pass through with updated counts.
                BlobStatus::Managed {
                    initial_certified_epoch: None,
                    unmanaged_deletable_counts: DeletableCounts::default(),
                    managed_blob_counts: managed_counts,
                }
            }
        }
    }

    /// Converts to BlobStatus, checking both regular blobs and managed blobs.
    /// For managed blobs, we need to check if any BlobManager in the certified set is still valid.
    pub(crate) fn to_blob_status(&self, current_epoch: Epoch) -> BlobStatus {
        // Get V1 status (regular blob status) if it exists.
        let v1_status = self
            .regular_blob_info
            .as_ref()
            .map(|info| info.to_blob_status(current_epoch))
            .unwrap_or(BlobStatus::Nonexistent);

        // Get managed blob counts.
        let managed_counts = self.managed_blob_counts(current_epoch).unwrap_or_default();

        // Add managed_blob_counts to the V1 status.
        Self::add_managed_counts_to_status(v1_status, managed_counts)
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
    V2(BlobInfoV2),
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
        match (self, &operand) {
            // V1 receiving RegisterManaged or CertifyManaged → transition to V2.
            (
                Self::V1(BlobInfoV1::Valid(v1)),
                BlobInfoMergeOperand::ChangeStatus {
                    change_type: ct @ BlobStatusChangeType::RegisterManaged { .. },
                    change_info,
                },
            ) => {
                // Convert V1 to V2, preserving all V1 data.
                let mut v2 = ValidBlobInfoV2::from(v1);
                // Apply the managed blob operation.
                v2.update_status(*ct, change_info.clone());
                Self::V2(BlobInfoV2::Valid(v2))
            }
            (Self::V2(v2), _) => Self::V2(v2.merge_with(operand)),
            // V1 receiving V1 operation → stay V1.
            (Self::V1(v1), _) => Self::V1(v1.merge_with(operand)),
        }
    }

    fn merge_new(operand: Self::MergeOperand) -> Option<Self> {
        if let BlobInfoMergeOperand::ChangeStatus {
            change_type: BlobStatusChangeType::RegisterManaged { .. },
            ..
        } = &operand
        {
            return BlobInfoV2::merge_new(operand).map(Self::from);
        }

        // Otherwise, create V1 entry.
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
        V2(PerObjectBlobInfoV2),
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
                Self::V2(value) => Self::V2(value.merge_with(operand)),
            }
        }

        fn merge_new(operand: Self::MergeOperand) -> Option<Self> {
            // Try V2 first (RegisterManaged), then fall back to V1 (Register).
            PerObjectBlobInfoV2::merge_new(operand.clone())
                .map(Self::from)
                .or_else(|| PerObjectBlobInfoV1::merge_new(operand).map(Self::from))
        }
    }

    #[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Clone)]
    pub(crate) struct PerObjectBlobInfoV1 {
        /// The blob ID.
        pub blob_id: BlobId,
        /// The epoch in which the blob has been registered.
        pub registered_epoch: Epoch,
        /// The epoch in which the blob was first certified, `None` if the blob is uncertified.
        pub certified_epoch: Option<Epoch>,
        /// The epoch in which the blob expires.
        pub end_epoch: Epoch,
        /// Whether the blob is deletable.
        pub deletable: bool,
        /// The ID of the last blob event related to this object.
        pub event: EventID,
        /// Whether the blob has been deleted.
        pub deleted: bool,
    }

    impl CertifiedBlobInfoApi for PerObjectBlobInfoV1 {
        fn is_certified(&self, current_epoch: Epoch) -> bool {
            self.is_registered(current_epoch)
                && self
                    .certified_epoch
                    .is_some_and(|epoch| epoch <= current_epoch)
        }

        fn initial_certified_epoch(&self) -> Option<Epoch> {
            self.certified_epoch
        }
    }

    impl PerObjectBlobInfoApi for PerObjectBlobInfoV1 {
        fn blob_id(&self) -> BlobId {
            self.blob_id
        }

        fn is_deletable(&self) -> bool {
            self.deletable
        }

        fn is_registered(&self, current_epoch: Epoch) -> bool {
            self.end_epoch > current_epoch && !self.deleted
        }

        fn is_deleted(&self) -> bool {
            self.deleted
        }
    }

    impl ToBytes for PerObjectBlobInfoV1 {}

    impl Mergeable for PerObjectBlobInfoV1 {
        type MergeOperand = PerObjectBlobInfoMergeOperand;
        type Key = ObjectID;

        fn merge_with(
            mut self,
            PerObjectBlobInfoMergeOperand {
                change_type,
                change_info,
            }: PerObjectBlobInfoMergeOperand,
        ) -> Self {
            assert_eq!(
                self.blob_id, change_info.blob_id,
                "blob ID mismatch in merge operand"
            );
            assert_eq!(
                self.deletable, change_info.deletable,
                "deletable mismatch in merge operand"
            );
            assert!(
                !self.deleted,
                "attempt to update an already deleted blob {}",
                self.blob_id
            );
            self.event = change_info.status_event;
            match change_type {
                // We ensure that the blob info is only updated a single time for each event. So if
                // we see a duplicated registered or certified event for the some object, this is a
                // serious bug somewhere.
                BlobStatusChangeType::Register => {
                    panic!(
                        "cannot register an already registered blob {}",
                        self.blob_id
                    );
                }
                BlobStatusChangeType::RegisterManaged { .. }
                | BlobStatusChangeType::CertifyManaged { .. }
                | BlobStatusChangeType::DeleteManaged { .. } => {
                    panic!(
                        "cannot apply managed blob operations to V1 (regular blob) {}",
                        self.blob_id
                    );
                }
                BlobStatusChangeType::Certify => {
                    assert!(
                        self.certified_epoch.is_none(),
                        "cannot certify an already certified blob {}",
                        self.blob_id
                    );
                    self.certified_epoch = Some(change_info.epoch);
                }
                BlobStatusChangeType::Extend => {
                    assert!(
                        self.certified_epoch.is_some(),
                        "cannot extend an uncertified blob {}",
                        self.blob_id
                    );
                    self.end_epoch = change_info.end_epoch;
                }
                BlobStatusChangeType::Delete { was_certified } => {
                    assert_eq!(self.certified_epoch.is_some(), was_certified);
                    self.deleted = true;
                }
            }
            self
        }

        fn merge_new(operand: Self::MergeOperand) -> Option<Self> {
            let PerObjectBlobInfoMergeOperand {
                change_type: BlobStatusChangeType::Register,
                change_info:
                    BlobStatusChangeInfo {
                        blob_id,
                        deletable,
                        epoch,
                        end_epoch,
                        status_event,
                    },
            } = operand
            else {
                tracing::error!(
                    ?operand,
                    "encountered an update other than 'register' for an untracked blob object"
                );
                debug_assert!(false);
                return None;
            };
            Some(Self {
                blob_id,
                registered_epoch: epoch,
                certified_epoch: None,
                end_epoch,
                deletable,
                event: status_event,
                deleted: false,
            })
        }
    }

    /// Per-object blob info for managed blobs (V2).
    ///
    /// Managed blobs are owned by a BlobManager and don't have a direct end_epoch.
    /// Instead, their validity is determined by the BlobManager's storage end_epoch.
    #[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Clone)]
    pub(crate) struct PerObjectBlobInfoV2 {
        /// The blob ID.
        pub blob_id: BlobId,
        /// The epoch in which the blob has been registered.
        pub registered_epoch: Epoch,
        /// The epoch in which the blob was first certified, `None` if the blob is uncertified.
        pub certified_epoch: Option<Epoch>,
        /// The ID of the BlobManager that manages this blob.
        pub blob_manager_id: ObjectID,
        /// Whether the blob is deletable.
        pub deletable: bool,
        /// The end epoch of the storage for this managed blob.
        pub end_epoch: Epoch,
        /// The ID of the last blob event related to this object.
        pub event: EventID,
        /// Whether the blob has been deleted.
        pub deleted: bool,
    }

    impl CertifiedBlobInfoApi for PerObjectBlobInfoV2 {
        fn is_certified(&self, current_epoch: Epoch) -> bool {
            self.is_registered(current_epoch)
                && self
                    .certified_epoch
                    .is_some_and(|epoch| epoch <= current_epoch)
        }

        fn initial_certified_epoch(&self) -> Option<Epoch> {
            self.certified_epoch
        }
    }

    impl PerObjectBlobInfoApi for PerObjectBlobInfoV2 {
        fn blob_id(&self) -> BlobId {
            self.blob_id
        }

        fn is_deletable(&self) -> bool {
            self.deletable
        }

        fn is_registered(&self, _current_epoch: Epoch) -> bool {
            // For managed blobs, validity is determined by the BlobManager's end_epoch.
            // For now, we consider them registered if not deleted.
            // TODO: Query BlobManager's end_epoch to determine validity.
            !self.deleted
        }

        fn is_deleted(&self) -> bool {
            self.deleted
        }
    }

    impl ToBytes for PerObjectBlobInfoV2 {}

    impl Mergeable for PerObjectBlobInfoV2 {
        type MergeOperand = PerObjectBlobInfoMergeOperand;
        type Key = ObjectID;

        fn merge_with(
            mut self,
            PerObjectBlobInfoMergeOperand {
                change_type,
                change_info,
            }: PerObjectBlobInfoMergeOperand,
        ) -> Self {
            assert_eq!(
                self.blob_id, change_info.blob_id,
                "blob ID mismatch in merge operand"
            );
            assert_eq!(
                self.deletable, change_info.deletable,
                "deletable mismatch in merge operand"
            );
            assert!(
                !self.deleted,
                "attempt to update an already deleted blob {}",
                self.blob_id
            );
            self.event = change_info.status_event;
            match change_type {
                BlobStatusChangeType::Register => {
                    panic!(
                        "cannot apply Register to V2 (managed blob) {}",
                        self.blob_id
                    );
                }
                BlobStatusChangeType::RegisterManaged { .. } => {
                    panic!(
                        "cannot register an already registered managed blob {}",
                        self.blob_id
                    );
                }
                BlobStatusChangeType::CertifyManaged { .. } => {
                    assert!(
                        self.certified_epoch.is_none(),
                        "cannot certify an already certified managed blob {}",
                        self.blob_id
                    );
                    self.certified_epoch = Some(change_info.epoch);
                }
                BlobStatusChangeType::Certify => {
                    // Regular Certify shouldn't be applied to managed blobs.
                    panic!(
                        "cannot apply Certify to V2 (managed blob) {} - use CertifyManaged",
                        self.blob_id
                    );
                }
                BlobStatusChangeType::Extend => {
                    // Managed blobs don't support extend operations.
                    // Their lifetime is managed by the BlobManager.
                    panic!(
                        "cannot extend a managed blob {} - use BlobManager instead",
                        self.blob_id
                    );
                }
                BlobStatusChangeType::Delete { was_certified } => {
                    assert_eq!(self.certified_epoch.is_some(), was_certified);
                    self.deleted = true;
                }
                BlobStatusChangeType::DeleteManaged { was_certified, .. } => {
                    assert_eq!(self.certified_epoch.is_some(), was_certified);
                    self.deleted = true;
                }
            }
            self
        }

        fn merge_new(operand: Self::MergeOperand) -> Option<Self> {
            let PerObjectBlobInfoMergeOperand {
                change_type: BlobStatusChangeType::RegisterManaged { blob_manager_id },
                change_info:
                    BlobStatusChangeInfo {
                        blob_id,
                        deletable,
                        epoch,
                        end_epoch,
                        status_event,
                    },
            } = operand
            else {
                return None;
            };
            Some(Self {
                blob_id,
                registered_epoch: epoch,
                certified_epoch: None,
                blob_manager_id,
                deletable,
                end_epoch,
                event: status_event,
                deleted: false,
            })
        }
    }
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

#[cfg(test)]
mod tests {
    use walrus_sui::test_utils::{event_id_for_testing, fixed_event_id_for_testing};
    use walrus_test_utils::param_test;

    use super::*;

    fn check_invariants(blob_info: &BlobInfoV1) {
        if let BlobInfoV1::Valid(valid_blob_info) = blob_info {
            valid_blob_info
                .check_invariants()
                .expect("aggregate blob info invariants violated")
        }
    }

    param_test! {
        test_merge_new_expected_failure_cases: [
            #[should_panic] metadata_true: (BlobInfoMergeOperand::MarkMetadataStored(true)),
            #[should_panic] metadata_false: (BlobInfoMergeOperand::MarkMetadataStored(false)),
            #[should_panic] certify_permanent: (BlobInfoMergeOperand::new_change_for_testing(
                BlobStatusChangeType::Certify,false, 42, 314, event_id_for_testing()
            )),
            #[should_panic] certify_deletable: (BlobInfoMergeOperand::new_change_for_testing(
                BlobStatusChangeType::Certify, true, 42, 314, event_id_for_testing()
            )),
            #[should_panic] extend: (BlobInfoMergeOperand::new_change_for_testing(
                BlobStatusChangeType::Extend, false, 42, 314, event_id_for_testing()
            )),
            #[should_panic] delete_deletable: (BlobInfoMergeOperand::new_change_for_testing(
                BlobStatusChangeType::Delete { was_certified: true },
                false,
                42,
                314,
                event_id_for_testing(),
            )),
            #[should_panic] delete_permanent: (BlobInfoMergeOperand::new_change_for_testing(
                BlobStatusChangeType::Delete { was_certified: false },
                false,
                42,
                314,
                event_id_for_testing(),
            )),
        ]
    }
    fn test_merge_new_expected_failure_cases(operand: BlobInfoMergeOperand) {
        let _ = BlobInfoV1::merge_new(operand);
    }

    param_test! {
        test_merge_new_expected_success_cases_invariants: [
            register_permanent: (BlobInfoMergeOperand::new_change_for_testing(
                BlobStatusChangeType::Register, false, 42, 314, event_id_for_testing()
            )),
            register_deletable: (BlobInfoMergeOperand::new_change_for_testing(
                BlobStatusChangeType::Register, true, 42, 314, event_id_for_testing()
            )),
            invalidate: (BlobInfoMergeOperand::MarkInvalid {
                epoch: 0,
                status_event: event_id_for_testing()
            }),
        ]
    }
    fn test_merge_new_expected_success_cases_invariants(operand: BlobInfoMergeOperand) {
        let blob_info = BlobInfoV1::merge_new(operand).expect("should be some");
        check_invariants(&blob_info);
    }

    param_test! {
        test_invalid_status_is_not_changed: [
            invalidate: (BlobInfoMergeOperand::MarkInvalid {
                epoch: 0,
                status_event: event_id_for_testing()
            }),
            metadata_true: (BlobInfoMergeOperand::MarkMetadataStored(true)),
            metadata_false: (BlobInfoMergeOperand::MarkMetadataStored(false)),
            register_permanent: (BlobInfoMergeOperand::new_change_for_testing(
                BlobStatusChangeType::Register, false, 42, 314, event_id_for_testing()
            )),
            register_deletable: (BlobInfoMergeOperand::new_change_for_testing(
                BlobStatusChangeType::Register, true, 42, 314, event_id_for_testing()
            )),
            certify_permanent: (BlobInfoMergeOperand::new_change_for_testing(
                BlobStatusChangeType::Certify, false, 42, 314, event_id_for_testing()
            )),
            certify_deletable: (BlobInfoMergeOperand::new_change_for_testing(
                BlobStatusChangeType::Certify, true, 42, 314, event_id_for_testing()
            )),
            extend: (BlobInfoMergeOperand::new_change_for_testing(
                BlobStatusChangeType::Extend, false, 42, 314, event_id_for_testing()
            )),
            delete_true: (BlobInfoMergeOperand::new_change_for_testing(
                BlobStatusChangeType::Delete { was_certified: true },
                false,
                42,
                314,
                event_id_for_testing(),
            )),
            delete_false: (BlobInfoMergeOperand::new_change_for_testing(
                BlobStatusChangeType::Delete { was_certified: false },
                false,
                42,
                314,
                event_id_for_testing(),
            )),
        ]
    }
    fn test_invalid_status_is_not_changed(operand: BlobInfoMergeOperand) {
        let blob_info = BlobInfoV1::Invalid {
            epoch: 42,
            event: event_id_for_testing(),
        };
        assert_eq!(blob_info, blob_info.clone().merge_with(operand));
    }

    param_test! {
        test_mark_metadata_stored_keeps_everything_else_unchanged: [
            default: (Default::default()),
            deletable: (ValidBlobInfoV1{count_deletable_total: 2, ..Default::default()}),
            deletable_certified: (ValidBlobInfoV1{
                count_deletable_total: 2,
                count_deletable_certified: 1,
                initial_certified_epoch: Some(0),
                ..Default::default()
            }),
            permanent: (ValidBlobInfoV1{
                permanent_total: Some(PermanentBlobInfoV1::new_fixed_for_testing(2, 3, 0)),
                ..Default::default()
            }),
            permanent_certified: (ValidBlobInfoV1{
                permanent_total: Some(PermanentBlobInfoV1::new_fixed_for_testing(2, 3, 0)),
                permanent_certified: Some(PermanentBlobInfoV1::new_fixed_for_testing(1, 2, 0)),
                initial_certified_epoch: Some(1),
                ..Default::default()
            }),
        ]
    }
    fn test_mark_metadata_stored_keeps_everything_else_unchanged(
        preexisting_info: ValidBlobInfoV1,
    ) {
        preexisting_info
            .check_invariants()
            .expect("preexisting blob info invariants violated");
        let expected_updated_info = ValidBlobInfoV1 {
            is_metadata_stored: true,
            ..preexisting_info.clone()
        };
        expected_updated_info
            .check_invariants()
            .expect("expected updated blob info invariants violated");

        let updated_info = BlobInfoV1::Valid(preexisting_info)
            .merge_with(BlobInfoMergeOperand::MarkMetadataStored(true));

        assert_eq!(updated_info, expected_updated_info.into());
    }

    param_test! {
        test_mark_invalid_marks_everything_invalid: [
            default: (Default::default()),
            deletable: (ValidBlobInfoV1{count_deletable_total: 2, ..Default::default()}),
            deletable_certified: (ValidBlobInfoV1{
                count_deletable_total: 2,
                count_deletable_certified: 1,
                initial_certified_epoch: Some(0),
                ..Default::default()
            }),
            permanent: (ValidBlobInfoV1{
                permanent_total: Some(PermanentBlobInfoV1::new_fixed_for_testing(2, 3, 0)),
                ..Default::default()
            }),
            permanent_certified: (ValidBlobInfoV1{
                permanent_total: Some(PermanentBlobInfoV1::new_fixed_for_testing(2, 3, 0)),
                permanent_certified: Some(PermanentBlobInfoV1::new_fixed_for_testing(1, 2, 0)),
                initial_certified_epoch: Some(1),
                ..Default::default()
            }),
        ]
    }
    fn test_mark_invalid_marks_everything_invalid(preexisting_info: ValidBlobInfoV1) {
        let preexisting_info = preexisting_info.into();
        check_invariants(&preexisting_info);
        let event = event_id_for_testing();
        let updated_info = preexisting_info.merge_with(BlobInfoMergeOperand::MarkInvalid {
            epoch: 2,
            status_event: event,
        });
        assert_eq!(BlobInfoV1::Invalid { epoch: 2, event }, updated_info);
    }

    param_test! {
        test_merge_preexisting_expected_successes: [
            register_first_deletable: (
                Default::default(),
                BlobInfoMergeOperand::new_change_for_testing(
                    BlobStatusChangeType::Register, true, 1, 2, event_id_for_testing()
                ),
                ValidBlobInfoV1{
                    count_deletable_total: 1,
                    latest_seen_deletable_registered_end_epoch: Some(2),
                    ..Default::default()
                },
            ),
            register_additional_deletable1: (
                ValidBlobInfoV1{
                    count_deletable_total: 3,
                    latest_seen_deletable_registered_end_epoch: Some(2),
                    ..Default::default()
                },
                BlobInfoMergeOperand::new_change_for_testing(
                    BlobStatusChangeType::Register, true, 1, 5, event_id_for_testing()
                ),
                ValidBlobInfoV1{
                    count_deletable_total: 4,
                    latest_seen_deletable_registered_end_epoch: Some(5),
                    ..Default::default()
                },
            ),
            register_additional_deletable2: (
                ValidBlobInfoV1{
                    count_deletable_total: 3,
                    latest_seen_deletable_registered_end_epoch: Some(4),
                    ..Default::default()
                },
                BlobInfoMergeOperand::new_change_for_testing(
                    BlobStatusChangeType::Register, true, 1, 3, event_id_for_testing()
                ),
                ValidBlobInfoV1{
                    count_deletable_total: 4,
                    latest_seen_deletable_registered_end_epoch: Some(4),
                    ..Default::default()
                },
            ),
            certify_first_deletable: (
                ValidBlobInfoV1{
                    count_deletable_total: 3,
                    latest_seen_deletable_registered_end_epoch: Some(4),
                    ..Default::default()
                },
                BlobInfoMergeOperand::new_change_for_testing(
                    BlobStatusChangeType::Certify, true, 1, 4, event_id_for_testing()
                ),
                ValidBlobInfoV1{
                    count_deletable_total: 3,
                    count_deletable_certified: 1,
                    initial_certified_epoch: Some(1),
                    latest_seen_deletable_registered_end_epoch: Some(4),
                    latest_seen_deletable_certified_end_epoch: Some(4),
                    ..Default::default()
                },
            ),
            certify_additional_deletable1: (
                ValidBlobInfoV1{
                    count_deletable_total: 3,
                    count_deletable_certified: 1,
                    initial_certified_epoch: Some(0),
                    latest_seen_deletable_registered_end_epoch: Some(4),
                    latest_seen_deletable_certified_end_epoch: Some(4),
                    ..Default::default()
                },
                BlobInfoMergeOperand::new_change_for_testing(
                    BlobStatusChangeType::Certify, true, 1, 2, event_id_for_testing()
                ),
                ValidBlobInfoV1{
                    count_deletable_total: 3,
                    count_deletable_certified: 2,
                    initial_certified_epoch: Some(0),
                    latest_seen_deletable_registered_end_epoch: Some(4),
                    latest_seen_deletable_certified_end_epoch: Some(4),
                    ..Default::default()
                },
            ),
            certify_additional_deletable2: (
                ValidBlobInfoV1{
                    count_deletable_total: 3,
                    count_deletable_certified: 1,
                    initial_certified_epoch: Some(1),
                    latest_seen_deletable_registered_end_epoch: Some(5),
                    latest_seen_deletable_certified_end_epoch: Some(4),
                    ..Default::default()
                },
                BlobInfoMergeOperand::new_change_for_testing(
                    BlobStatusChangeType::Certify, true, 0, 5, event_id_for_testing()
                ),
                ValidBlobInfoV1{
                    count_deletable_total: 3,
                    count_deletable_certified: 2,
                    initial_certified_epoch: Some(0),
                    latest_seen_deletable_registered_end_epoch: Some(5),
                    latest_seen_deletable_certified_end_epoch: Some(5),
                    ..Default::default()
                },
            ),
            register_first_permanent: (
                ValidBlobInfoV1{
                    ..Default::default()
                },
                BlobInfoMergeOperand::new_change_for_testing(
                    BlobStatusChangeType::Register, false, 1, 2, fixed_event_id_for_testing(0)
                ),
                ValidBlobInfoV1{
                    permanent_total: Some(PermanentBlobInfoV1::new_fixed_for_testing(1, 2, 0)),
                    ..Default::default()
                },
            ),
            extend_deletable: (
                ValidBlobInfoV1{
                    count_deletable_total: 3,
                    count_deletable_certified: 1,
                    initial_certified_epoch: Some(0),
                    latest_seen_deletable_registered_end_epoch: Some(4),
                    latest_seen_deletable_certified_end_epoch: Some(4),
                    ..Default::default()
                },
                BlobInfoMergeOperand::new_change_for_testing(
                    BlobStatusChangeType::Extend, true, 3, 42, event_id_for_testing()
                ),
                ValidBlobInfoV1{
                    count_deletable_total: 3,
                    count_deletable_certified: 1,
                    initial_certified_epoch: Some(0),
                    latest_seen_deletable_registered_end_epoch: Some(42),
                    latest_seen_deletable_certified_end_epoch: Some(42),
                    ..Default::default()
                },
            ),
            extend_permanent: (
                ValidBlobInfoV1{
                    initial_certified_epoch: Some(0),
                    permanent_total: Some(PermanentBlobInfoV1::new_fixed_for_testing(2, 4, 0)),
                    permanent_certified: Some(PermanentBlobInfoV1::new_fixed_for_testing(1, 4, 1)),
                    ..Default::default()
                },
                BlobInfoMergeOperand::new_change_for_testing(
                    BlobStatusChangeType::Extend, false, 3, 42, fixed_event_id_for_testing(2)
                ),
                ValidBlobInfoV1{
                    initial_certified_epoch: Some(0),
                    permanent_total: Some(PermanentBlobInfoV1::new_fixed_for_testing(2, 42, 2)),
                    permanent_certified: Some(PermanentBlobInfoV1::new_fixed_for_testing(1, 42, 2)),
                    ..Default::default()
                },
            ),
            certify_outdated_deletable: (
                ValidBlobInfoV1{
                    count_deletable_total: 3,
                    count_deletable_certified: 1,
                    initial_certified_epoch: Some(1),
                    latest_seen_deletable_registered_end_epoch: Some(8),
                    latest_seen_deletable_certified_end_epoch: Some(4),
                    ..Default::default()
                },
                BlobInfoMergeOperand::new_change_for_testing(
                    BlobStatusChangeType::Certify, true, 4, 6, event_id_for_testing()
                ),
                ValidBlobInfoV1{
                    count_deletable_total: 3,
                    count_deletable_certified: 2,
                    initial_certified_epoch: Some(4),
                    latest_seen_deletable_registered_end_epoch: Some(8),
                    latest_seen_deletable_certified_end_epoch: Some(6),
                    ..Default::default()
                },
            ),
            certify_outdated_permanent: (
                ValidBlobInfoV1{
                    initial_certified_epoch: Some(2),
                    permanent_total: Some(PermanentBlobInfoV1::new_fixed_for_testing(2, 42, 0)),
                    permanent_certified: Some(PermanentBlobInfoV1::new_for_testing(1, 5)),
                    ..Default::default()
                },
                BlobInfoMergeOperand::new_change_for_testing(
                    BlobStatusChangeType::Certify, false, 7, 42, fixed_event_id_for_testing(1)
                ),
                ValidBlobInfoV1{
                    initial_certified_epoch: Some(7),
                    permanent_total: Some(PermanentBlobInfoV1::new_fixed_for_testing(2, 42, 0)),
                    permanent_certified: Some(PermanentBlobInfoV1::new_fixed_for_testing(2, 42, 1)),
                    ..Default::default()
                },
            ),
            register_additional_permanent: (
                ValidBlobInfoV1{
                    permanent_total: Some(PermanentBlobInfoV1::new_fixed_for_testing(1, 2, 0)),
                    ..Default::default()
                },
                BlobInfoMergeOperand::new_change_for_testing(
                    BlobStatusChangeType::Register, false, 2, 3, fixed_event_id_for_testing(1)
                ),
                ValidBlobInfoV1{
                    permanent_total: Some(PermanentBlobInfoV1::new_fixed_for_testing(2, 3, 1)),
                    ..Default::default()
                },
            ),
            expire_permanent_blob: (
                ValidBlobInfoV1{
                    permanent_total: Some(PermanentBlobInfoV1::new_fixed_for_testing(3, 5, 0)),
                    permanent_certified: Some(PermanentBlobInfoV1::new_fixed_for_testing(2, 5, 1)),
                    initial_certified_epoch: Some(1),
                    ..Default::default()
                },
                BlobInfoMergeOperand::PermanentExpired {
                    was_certified: true,
                },
                ValidBlobInfoV1{
                    permanent_total: Some(PermanentBlobInfoV1::new_fixed_for_testing(2, 5, 0)),
                    permanent_certified: Some(PermanentBlobInfoV1::new_fixed_for_testing(1, 5, 1)),
                    initial_certified_epoch: Some(1),
                    ..Default::default()
                },
            ),
            expire_last_permanent_blob: (
                ValidBlobInfoV1{
                    permanent_total: Some(PermanentBlobInfoV1::new_fixed_for_testing(2, 5, 0)),
                    permanent_certified: Some(PermanentBlobInfoV1::new_fixed_for_testing(1, 5, 1)),
                    initial_certified_epoch: Some(1),
                    ..Default::default()
                },
                BlobInfoMergeOperand::PermanentExpired {
                    was_certified: true,
                },
                ValidBlobInfoV1{
                    permanent_total: Some(PermanentBlobInfoV1::new_fixed_for_testing(1, 5, 0)),
                    permanent_certified: None,
                    initial_certified_epoch: None,
                    ..Default::default()
                },
            ),
            delete_deletable_blob: (
                ValidBlobInfoV1{
                    count_deletable_total: 3,
                    count_deletable_certified: 2,
                    initial_certified_epoch: Some(1),
                    latest_seen_deletable_registered_end_epoch: Some(5),
                    latest_seen_deletable_certified_end_epoch: Some(4),
                    ..Default::default()
                },
                BlobInfoMergeOperand::new_change_for_testing(
                    BlobStatusChangeType::Delete { was_certified: true },
                    true,
                    1,
                    6,
                    event_id_for_testing(),
                ),
                ValidBlobInfoV1{
                    count_deletable_total: 2,
                    count_deletable_certified: 1,
                    initial_certified_epoch: Some(1),
                    latest_seen_deletable_registered_end_epoch: Some(5),
                    latest_seen_deletable_certified_end_epoch: Some(4),
                    ..Default::default()
                },
            ),
            expire_deletable_blob: (
                ValidBlobInfoV1{
                    count_deletable_total: 3,
                    count_deletable_certified: 2,
                    initial_certified_epoch: Some(1),
                    latest_seen_deletable_registered_end_epoch: Some(5),
                    latest_seen_deletable_certified_end_epoch: Some(4),
                    ..Default::default()
                },
                BlobInfoMergeOperand::DeletableExpired {
                    was_certified: true,
                },
                ValidBlobInfoV1{
                    count_deletable_total: 2,
                    count_deletable_certified: 1,
                    initial_certified_epoch: Some(1),
                    latest_seen_deletable_registered_end_epoch: Some(5),
                    latest_seen_deletable_certified_end_epoch: Some(4),
                    ..Default::default()
                },
            ),
            delete_last_deletable_blob: (
                ValidBlobInfoV1{
                    count_deletable_total: 2,
                    count_deletable_certified: 1,
                    initial_certified_epoch: Some(1),
                    latest_seen_deletable_registered_end_epoch: Some(5),
                    latest_seen_deletable_certified_end_epoch: Some(4),
                    ..Default::default()
                },
                BlobInfoMergeOperand::new_change_for_testing(
                    BlobStatusChangeType::Delete { was_certified: true },
                    true,
                    1,
                    4,
                    event_id_for_testing(),
                ),
                ValidBlobInfoV1{
                    count_deletable_total: 1,
                    count_deletable_certified: 0,
                    initial_certified_epoch: None,
                    latest_seen_deletable_registered_end_epoch: Some(5),
                    latest_seen_deletable_certified_end_epoch: None,
                    ..Default::default()
                },
            ),
            expire_last_deletable_blob: (
                ValidBlobInfoV1{
                    count_deletable_total: 2,
                    count_deletable_certified: 1,
                    initial_certified_epoch: Some(1),
                    latest_seen_deletable_registered_end_epoch: Some(5),
                    latest_seen_deletable_certified_end_epoch: Some(4),
                    ..Default::default()
                },
                BlobInfoMergeOperand::DeletableExpired {
                    was_certified: true,
                },
                ValidBlobInfoV1{
                    count_deletable_total: 1,
                    count_deletable_certified: 0,
                    initial_certified_epoch: None,
                    latest_seen_deletable_registered_end_epoch: Some(5),
                    latest_seen_deletable_certified_end_epoch: None,
                    ..Default::default()
                },
            ),
            expire_uncertified_permanent_blob: (
                ValidBlobInfoV1{
                    permanent_total: Some(PermanentBlobInfoV1::new_fixed_for_testing(3, 5, 0)),
                    ..Default::default()
                },
                BlobInfoMergeOperand::PermanentExpired {
                    was_certified: false,
                },
                ValidBlobInfoV1{
                    permanent_total: Some(PermanentBlobInfoV1::new_fixed_for_testing(2, 5, 0)),
                    ..Default::default()
                },
            ),
            expire_last_uncertified_permanent_blob: (
                ValidBlobInfoV1{
                    permanent_total: Some(PermanentBlobInfoV1::new_fixed_for_testing(1, 5, 0)),
                    ..Default::default()
                },
                BlobInfoMergeOperand::PermanentExpired {
                    was_certified: false,
                },
                ValidBlobInfoV1{
                    permanent_total: None,
                    ..Default::default()
                },
            ),
            delete_uncertified_deletable_blob: (
                ValidBlobInfoV1{
                    count_deletable_total: 3,
                    latest_seen_deletable_registered_end_epoch: Some(5),
                    ..Default::default()
                },
                BlobInfoMergeOperand::new_change_for_testing(
                    BlobStatusChangeType::Delete { was_certified: false },
                    true,
                    1,
                    6,
                    event_id_for_testing(),
                ),
                ValidBlobInfoV1{
                    count_deletable_total: 2,
                    latest_seen_deletable_registered_end_epoch: Some(5),
                    ..Default::default()
                },
            ),
            delete_last_uncertified_deletable_blob: (
                ValidBlobInfoV1{
                    count_deletable_total: 1,
                    latest_seen_deletable_registered_end_epoch: Some(5),
                    ..Default::default()
                },
                BlobInfoMergeOperand::new_change_for_testing(
                    BlobStatusChangeType::Delete { was_certified: false },
                    true,
                    2,
                    6,
                    event_id_for_testing(),
                ),
                ValidBlobInfoV1{
                    count_deletable_total: 0,
                    latest_seen_deletable_registered_end_epoch: None,
                    ..Default::default()
                },
            ),
        ]
    }
    fn test_merge_preexisting_expected_successes(
        preexisting_info: ValidBlobInfoV1,
        operand: BlobInfoMergeOperand,
        expected_info: ValidBlobInfoV1,
    ) {
        preexisting_info
            .check_invariants()
            .expect("preexisting blob info invariants violated");
        expected_info
            .check_invariants()
            .expect("expected blob info invariants violated");

        let updated_info = BlobInfoV1::Valid(preexisting_info).merge_with(operand);

        assert_eq!(updated_info, expected_info.into());
    }

    param_test! {
        test_merge_preexisting_expected_failures: [
            certify_permanent_without_register: (
                Default::default(),
                BlobInfoMergeOperand::new_change_for_testing(
                    BlobStatusChangeType::Certify, false, 42, 314, event_id_for_testing()
                ),
            ),
            extend_permanent_without_certify: (
                Default::default(),
                BlobInfoMergeOperand::new_change_for_testing(
                    BlobStatusChangeType::Extend, false, 42, 314, event_id_for_testing()
                ),
            ),
            certify_deletable_without_register: (
                Default::default(),
                BlobInfoMergeOperand::new_change_for_testing(
                    BlobStatusChangeType::Certify, true, 42, 314, event_id_for_testing()
                ),
            ),
        ]
    }
    fn test_merge_preexisting_expected_failures(
        preexisting_info: ValidBlobInfoV1,
        operand: BlobInfoMergeOperand,
    ) {
        preexisting_info
            .check_invariants()
            .expect("preexisting blob info invariants violated");
        let preexisting_info = BlobInfoV1::Valid(preexisting_info);
        let blob_info = preexisting_info.clone().merge_with(operand);
        assert_eq!(preexisting_info, blob_info);
    }

    param_test! {
        test_blob_status_is_inexistent_for_expired_blobs: [
            expired_permanent_registered_0: (
                ValidBlobInfoV1 {
                    permanent_total: Some(PermanentBlobInfoV1::new_fixed_for_testing(1, 2, 0)),
                    ..Default::default()
                },
                1,
                2,
            ),
            expired_permanent_registered_1: (
                ValidBlobInfoV1 {
                    permanent_total: Some(PermanentBlobInfoV1::new_fixed_for_testing(2, 3, 0)),
                    ..Default::default()
                },
                2,
                4,
            ),
            expired_permanent_certified: (
                ValidBlobInfoV1 {
                    permanent_total: Some(PermanentBlobInfoV1::new_fixed_for_testing(2, 2, 0)),
                    permanent_certified: Some(PermanentBlobInfoV1::new_fixed_for_testing(1, 2, 0)),
                    ..Default::default()
                },
                1,
                2,
            ),
            expired_deletable_registered: (
                ValidBlobInfoV1 {
                    count_deletable_total: 1,
                    latest_seen_deletable_registered_end_epoch: Some(2),
                    ..Default::default()
                },
                1,
                2,
            ),
            expired_deletable_certified: (
                ValidBlobInfoV1 {
                    count_deletable_total: 1,
                    latest_seen_deletable_registered_end_epoch: Some(2),
                    count_deletable_certified: 1,
                    latest_seen_deletable_certified_end_epoch: Some(2),
                    ..Default::default()
                },
                1,
                2,
            ),
        ]
    }
    fn test_blob_status_is_inexistent_for_expired_blobs(
        blob_info: ValidBlobInfoV1,
        epoch_not_expired: Epoch,
        epoch_expired: Epoch,
    ) {
        assert_ne!(
            BlobInfoV1::Valid(blob_info.clone()).to_blob_status(epoch_not_expired),
            BlobStatus::Nonexistent,
        );
        assert_eq!(
            BlobInfoV1::Valid(blob_info).to_blob_status(epoch_expired),
            BlobStatus::Nonexistent,
        );
    }

    // ============================================================================
    // V2 Merge Operation Tests
    // ============================================================================

    fn object_id_for_testing(n: u8) -> ObjectID {
        ObjectID::from_bytes([n; 32]).unwrap()
    }

    fn blob_id_for_testing() -> BlobId {
        use walrus_core::EncodingType;
        // Create a dummy BlobId for testing.
        BlobId::from_metadata([0u8; 32].into(), EncodingType::RS2, 0)
    }

    param_test! {
        test_v2_merge_new_with_register_managed: [
            register_managed_deletable: (
                BlobInfoMergeOperand::ChangeStatus {
                    change_type: BlobStatusChangeType::RegisterManaged {
                        blob_manager_id: object_id_for_testing(1),
                    },
                    change_info: BlobStatusChangeInfo {
                        epoch: 1,
                        end_epoch: 10,
                        deletable: true,
                        status_event: event_id_for_testing(),
                        blob_id: blob_id_for_testing(),
                    },
                },
                BlobInfoV2::Valid(ValidBlobInfoV2 {
                    regular_blob_info: None,
                    managed_blob_info: Some(ManagedBlobInfo {
                        is_metadata_stored: false,
                        initial_certified_epoch: None,
                        registered: [object_id_for_testing(1)].into_iter().collect(),
                        registered_deletable_counts: 1,
                        certified: Default::default(),
                        certified_deletable_counts: 0,
                    }),
                }),
            ),
            register_managed_permanent: (
                BlobInfoMergeOperand::ChangeStatus {
                    change_type: BlobStatusChangeType::RegisterManaged {
                        blob_manager_id: object_id_for_testing(2),
                    },
                    change_info: BlobStatusChangeInfo {
                        epoch: 1,
                        end_epoch: 10,
                        deletable: false,
                        status_event: event_id_for_testing(),
                        blob_id: blob_id_for_testing(),
                    },
                },
                BlobInfoV2::Valid(ValidBlobInfoV2 {
                    regular_blob_info: None,
                    managed_blob_info: Some(ManagedBlobInfo {
                        is_metadata_stored: false,
                        initial_certified_epoch: None,
                        registered: [object_id_for_testing(2)].into_iter().collect(),
                        registered_deletable_counts: 0,
                        certified: Default::default(),
                        certified_deletable_counts: 0,
                    }),
                }),
            ),
        ]
    }
    fn test_v2_merge_new_with_register_managed(
        operand: BlobInfoMergeOperand,
        expected: BlobInfoV2,
    ) {
        let result = BlobInfoV2::merge_new(operand).expect("should create V2 from RegisterManaged");
        assert_eq!(result, expected);
    }

    param_test! {
        test_v2_mark_metadata_stored: [
            with_regular_blob: (
                BlobInfoV2::Valid(ValidBlobInfoV2 {
                    regular_blob_info: Some(ValidBlobInfoV1::default()),
                    managed_blob_info: None,
                }),
                BlobInfoV2::Valid(ValidBlobInfoV2 {
                    regular_blob_info: Some(ValidBlobInfoV1 {
                        is_metadata_stored: true,
                        ..Default::default()
                    }),
                    managed_blob_info: None,
                }),
            ),
            with_managed_blob: (
                BlobInfoV2::Valid(ValidBlobInfoV2 {
                    regular_blob_info: None,
                    managed_blob_info: Some(ManagedBlobInfo::default()),
                }),
                BlobInfoV2::Valid(ValidBlobInfoV2 {
                    regular_blob_info: None,
                    managed_blob_info: Some(ManagedBlobInfo {
                        is_metadata_stored: true,
                        ..Default::default()
                    }),
                }),
            ),
            with_both: (
                BlobInfoV2::Valid(ValidBlobInfoV2 {
                    regular_blob_info: Some(ValidBlobInfoV1::default()),
                    managed_blob_info: Some(ManagedBlobInfo::default()),
                }),
                BlobInfoV2::Valid(ValidBlobInfoV2 {
                    regular_blob_info: Some(ValidBlobInfoV1 {
                        is_metadata_stored: true,
                        ..Default::default()
                    }),
                    managed_blob_info: Some(ManagedBlobInfo {
                        is_metadata_stored: true,
                        ..Default::default()
                    }),
                }),
            ),
        ]
    }
    fn test_v2_mark_metadata_stored(preexisting: BlobInfoV2, expected: BlobInfoV2) {
        let result = preexisting.merge_with(BlobInfoMergeOperand::MarkMetadataStored(true));
        assert_eq!(result, expected);
    }

    param_test! {
        test_v2_register_managed_blob: [
            register_managed_on_existing_v1: (
                BlobInfoV2::Valid(ValidBlobInfoV2 {
                    regular_blob_info: Some(ValidBlobInfoV1 {
                        count_deletable_total: 1,
                        latest_seen_deletable_registered_end_epoch: Some(10),
                        ..Default::default()
                    }),
                    managed_blob_info: None,
                }),
                BlobInfoMergeOperand::ChangeStatus {
                    change_type: BlobStatusChangeType::RegisterManaged {
                        blob_manager_id: object_id_for_testing(1),
                    },
                    change_info: BlobStatusChangeInfo {
                        epoch: 1,
                        end_epoch: 10,
                        deletable: true,
                        status_event: event_id_for_testing(),
                        blob_id: blob_id_for_testing(),
                    },
                },
                BlobInfoV2::Valid(ValidBlobInfoV2 {
                    regular_blob_info: Some(ValidBlobInfoV1 {
                        count_deletable_total: 1,
                        latest_seen_deletable_registered_end_epoch: Some(10),
                        ..Default::default()
                    }),
                    managed_blob_info: Some(ManagedBlobInfo {
                        is_metadata_stored: false,
                        initial_certified_epoch: None,
                        registered: [object_id_for_testing(1)].into_iter().collect(),
                        registered_deletable_counts: 1,
                        certified: Default::default(),
                        certified_deletable_counts: 0,
                    }),
                }),
            ),
            register_managed_on_existing_v2_managed: (
                BlobInfoV2::Valid(ValidBlobInfoV2 {
                    regular_blob_info: None,
                    managed_blob_info: Some(ManagedBlobInfo {
                        is_metadata_stored: false,
                        initial_certified_epoch: None,
                        registered: [object_id_for_testing(1)].into_iter().collect(),
                        registered_deletable_counts: 1,
                        certified: Default::default(),
                        certified_deletable_counts: 0,
                    }),
                }),
                BlobInfoMergeOperand::ChangeStatus {
                    change_type: BlobStatusChangeType::RegisterManaged {
                        blob_manager_id: object_id_for_testing(2),
                    },
                    change_info: BlobStatusChangeInfo {
                        epoch: 1,
                        end_epoch: 10,
                        deletable: false,
                        status_event: event_id_for_testing(),
                        blob_id: blob_id_for_testing(),
                    },
                },
                BlobInfoV2::Valid(ValidBlobInfoV2 {
                    regular_blob_info: None,
                    managed_blob_info: Some(ManagedBlobInfo {
                        is_metadata_stored: false,
                        initial_certified_epoch: None,
                        registered: [object_id_for_testing(1), object_id_for_testing(2)]
                            .into_iter()
                            .collect(),
                        registered_deletable_counts: 1,
                        certified: Default::default(),
                        certified_deletable_counts: 0,
                    }),
                }),
            ),
            register_managed_on_existing_v2_both: (
                BlobInfoV2::Valid(ValidBlobInfoV2 {
                    regular_blob_info: Some(ValidBlobInfoV1 {
                        count_deletable_total: 2,
                        count_deletable_certified: 1,
                        latest_seen_deletable_registered_end_epoch: Some(10),
                        latest_seen_deletable_certified_end_epoch: Some(10),
                        ..Default::default()
                    }),
                    managed_blob_info: Some(ManagedBlobInfo {
                        is_metadata_stored: false,
                        initial_certified_epoch: None,
                        registered: [object_id_for_testing(1)].into_iter().collect(),
                        registered_deletable_counts: 1,
                        certified: Default::default(),
                        certified_deletable_counts: 0,
                    }),
                }),
                BlobInfoMergeOperand::ChangeStatus {
                    change_type: BlobStatusChangeType::RegisterManaged {
                        blob_manager_id: object_id_for_testing(3),
                    },
                    change_info: BlobStatusChangeInfo {
                        epoch: 1,
                        end_epoch: 10,
                        deletable: true,
                        status_event: event_id_for_testing(),
                        blob_id: blob_id_for_testing(),
                    },
                },
                BlobInfoV2::Valid(ValidBlobInfoV2 {
                    regular_blob_info: Some(ValidBlobInfoV1 {
                        count_deletable_total: 2,
                        count_deletable_certified: 1,
                        latest_seen_deletable_registered_end_epoch: Some(10),
                        latest_seen_deletable_certified_end_epoch: Some(10),
                        ..Default::default()
                    }),
                    managed_blob_info: Some(ManagedBlobInfo {
                        is_metadata_stored: false,
                        initial_certified_epoch: None,
                        registered: [object_id_for_testing(1), object_id_for_testing(3)]
                            .into_iter()
                            .collect(),
                        registered_deletable_counts: 2,
                        certified: Default::default(),
                        certified_deletable_counts: 0,
                    }),
                }),
            ),
        ]
    }
    fn test_v2_register_managed_blob(
        preexisting: BlobInfoV2,
        operand: BlobInfoMergeOperand,
        expected: BlobInfoV2,
    ) {
        let result = preexisting.merge_with(operand);
        assert_eq!(result, expected);
    }

    param_test! {
        test_v2_certify_managed_blob: [
            certify_after_register: (
                BlobInfoV2::Valid(ValidBlobInfoV2 {
                    regular_blob_info: None,
                    managed_blob_info: Some(ManagedBlobInfo {
                        is_metadata_stored: false,
                        initial_certified_epoch: None,
                        registered: [object_id_for_testing(1)].into_iter().collect(),
                        registered_deletable_counts: 1,
                        certified: Default::default(),
                        certified_deletable_counts: 0,
                    }),
                }),
                object_id_for_testing(1),
                1,
                true,
                BlobInfoV2::Valid(ValidBlobInfoV2 {
                    regular_blob_info: None,
                    managed_blob_info: Some(ManagedBlobInfo {
                        is_metadata_stored: false,
                        initial_certified_epoch: Some(1),
                        registered: [object_id_for_testing(1)].into_iter().collect(),
                        registered_deletable_counts: 1,
                        certified: [object_id_for_testing(1)].into_iter().collect(),
                        certified_deletable_counts: 1,
                    }),
                }),
            ),
            certify_second_manager: (
                BlobInfoV2::Valid(ValidBlobInfoV2 {
                    regular_blob_info: None,
                    managed_blob_info: Some(ManagedBlobInfo {
                        is_metadata_stored: false,
                        initial_certified_epoch: Some(1),
                        registered: [object_id_for_testing(1), object_id_for_testing(2)]
                            .into_iter()
                            .collect(),
                        registered_deletable_counts: 1,
                        certified: [object_id_for_testing(1)].into_iter().collect(),
                        certified_deletable_counts: 1,
                    }),
                }),
                object_id_for_testing(2),
                2,
                false,
                BlobInfoV2::Valid(ValidBlobInfoV2 {
                    regular_blob_info: None,
                    managed_blob_info: Some(ManagedBlobInfo {
                        is_metadata_stored: false,
                        initial_certified_epoch: Some(1),
                        registered: [object_id_for_testing(1), object_id_for_testing(2)]
                            .into_iter()
                            .collect(),
                        registered_deletable_counts: 1,
                        certified: [object_id_for_testing(1), object_id_for_testing(2)]
                            .into_iter()
                            .collect(),
                        certified_deletable_counts: 1,
                    }),
                }),
            ),
        ]
    }
    fn test_v2_certify_managed_blob(
        preexisting: BlobInfoV2,
        blob_manager_id: ObjectID,
        epoch: Epoch,
        deletable: bool,
        expected: BlobInfoV2,
    ) {
        let operand = BlobInfoMergeOperand::ChangeStatus {
            change_type: BlobStatusChangeType::CertifyManaged { blob_manager_id },
            change_info: BlobStatusChangeInfo {
                epoch,
                end_epoch: 10,
                deletable,
                status_event: event_id_for_testing(),
                blob_id: blob_id_for_testing(),
            },
        };
        let result = preexisting.merge_with(operand);
        assert_eq!(result, expected);
    }

    param_test! {
        test_v2_regular_and_managed_coexist: [
            register_regular_then_managed: (
                BlobInfoV2::Valid(ValidBlobInfoV2::default()),
                object_id_for_testing(1),
                BlobInfoV2::Valid(ValidBlobInfoV2 {
                    regular_blob_info: Some(ValidBlobInfoV1 {
                        count_deletable_total: 1,
                        latest_seen_deletable_registered_end_epoch: Some(10),
                        ..Default::default()
                    }),
                    managed_blob_info: Some(ManagedBlobInfo {
                        is_metadata_stored: false,
                        initial_certified_epoch: None,
                        registered: [object_id_for_testing(1)].into_iter().collect(),
                        registered_deletable_counts: 1,
                        certified: Default::default(),
                        certified_deletable_counts: 0,
                    }),
                }),
            ),
            register_managed_then_regular: (
                BlobInfoV2::Valid(ValidBlobInfoV2::default()),
                object_id_for_testing(1),
                BlobInfoV2::Valid(ValidBlobInfoV2 {
                    regular_blob_info: Some(ValidBlobInfoV1 {
                        count_deletable_total: 1,
                        latest_seen_deletable_registered_end_epoch: Some(10),
                        ..Default::default()
                    }),
                    managed_blob_info: Some(ManagedBlobInfo {
                        is_metadata_stored: false,
                        initial_certified_epoch: None,
                        registered: [object_id_for_testing(1)].into_iter().collect(),
                        registered_deletable_counts: 1,
                        certified: Default::default(),
                        certified_deletable_counts: 0,
                    }),
                }),
            ),
        ]
    }
    fn test_v2_regular_and_managed_coexist(
        mut preexisting: BlobInfoV2,
        blob_manager_id: ObjectID,
        expected: BlobInfoV2,
    ) {
        // First add managed blob.
        let register_managed = BlobInfoMergeOperand::ChangeStatus {
            change_type: BlobStatusChangeType::RegisterManaged { blob_manager_id },
            change_info: BlobStatusChangeInfo {
                epoch: 1,
                end_epoch: 10,
                deletable: true,
                status_event: event_id_for_testing(),
                blob_id: blob_id_for_testing(),
            },
        };
        preexisting = preexisting.merge_with(register_managed);

        // Then add regular blob.
        let register_regular = BlobInfoMergeOperand::ChangeStatus {
            change_type: BlobStatusChangeType::Register,
            change_info: BlobStatusChangeInfo {
                epoch: 1,
                end_epoch: 10,
                deletable: true,
                status_event: event_id_for_testing(),
                blob_id: blob_id_for_testing(),
            },
        };
        let result = preexisting.merge_with(register_regular);
        assert_eq!(result, expected);
    }

    param_test! {
        test_v2_mark_invalid: [
            empty_v2: (
                BlobInfoV2::Valid(ValidBlobInfoV2::default()),
                2,
            ),
            with_regular: (
                BlobInfoV2::Valid(ValidBlobInfoV2 {
                    regular_blob_info: Some(ValidBlobInfoV1 {
                        count_deletable_total: 1,
                        ..Default::default()
                    }),
                    managed_blob_info: None,
                }),
                3,
            ),
            with_managed: (
                BlobInfoV2::Valid(ValidBlobInfoV2 {
                    regular_blob_info: None,
                    managed_blob_info: Some(ManagedBlobInfo {
                        registered: [object_id_for_testing(1)].into_iter().collect(),
                        ..Default::default()
                    }),
                }),
                4,
            ),
            with_both: (
                BlobInfoV2::Valid(ValidBlobInfoV2 {
                    regular_blob_info: Some(ValidBlobInfoV1 {
                        count_deletable_total: 1,
                        ..Default::default()
                    }),
                    managed_blob_info: Some(ManagedBlobInfo {
                        registered: [object_id_for_testing(1)].into_iter().collect(),
                        ..Default::default()
                    }),
                }),
                5,
            ),
        ]
    }
    fn test_v2_mark_invalid(preexisting: BlobInfoV2, epoch: Epoch) {
        let event = event_id_for_testing();
        let result = preexisting.merge_with(BlobInfoMergeOperand::MarkInvalid {
            epoch,
            status_event: event,
        });
        assert_eq!(result, BlobInfoV2::Invalid { epoch, event });
    }

    param_test! {
        test_v2_invalid_status_not_changed: [
            mark_metadata_stored: (BlobInfoMergeOperand::MarkMetadataStored(true)),
            register_regular: (BlobInfoMergeOperand::ChangeStatus {
                change_type: BlobStatusChangeType::Register,
                change_info: BlobStatusChangeInfo {
                    epoch: 1,
                    end_epoch: 10,
                    deletable: true,
                    status_event: event_id_for_testing(),
                    blob_id: blob_id_for_testing(),
                },
            }),
            register_managed: (BlobInfoMergeOperand::ChangeStatus {
                change_type: BlobStatusChangeType::RegisterManaged {
                    blob_manager_id: object_id_for_testing(1),
                },
                change_info: BlobStatusChangeInfo {
                    epoch: 1,
                    end_epoch: 10,
                    deletable: true,
                    status_event: event_id_for_testing(),
                    blob_id: blob_id_for_testing(),
                },
            }),
            certify_managed: (BlobInfoMergeOperand::ChangeStatus {
                change_type: BlobStatusChangeType::CertifyManaged {
                    blob_manager_id: object_id_for_testing(1),
                },
                change_info: BlobStatusChangeInfo {
                    epoch: 1,
                    end_epoch: 10,
                    deletable: true,
                    status_event: event_id_for_testing(),
                    blob_id: blob_id_for_testing(),
                },
            }),
        ]
    }
    fn test_v2_invalid_status_not_changed(operand: BlobInfoMergeOperand) {
        let invalid = BlobInfoV2::Invalid {
            epoch: 42,
            event: event_id_for_testing(),
        };
        let result = invalid.clone().merge_with(operand);
        assert_eq!(result, invalid);
    }

    param_test! {
        test_v2_delete_managed: [
            delete_uncertified_managed: (
                BlobInfoV2::Valid(ValidBlobInfoV2 {
                    regular_blob_info: None,
                    managed_blob_info: Some(ManagedBlobInfo {
                        is_metadata_stored: false,
                        initial_certified_epoch: None,
                        registered: [object_id_for_testing(1)].into_iter().collect(),
                        registered_deletable_counts: 1,
                        certified: Default::default(),
                        certified_deletable_counts: 0,
                    }),
                }),
                BlobInfoMergeOperand::ChangeStatus {
                    change_type: BlobStatusChangeType::DeleteManaged {
                        blob_manager_id: object_id_for_testing(1),
                        was_certified: false,
                    },
                    change_info: BlobStatusChangeInfo {
                        epoch: 1,
                        end_epoch: 10,
                        deletable: true,
                        status_event: event_id_for_testing(),
                        blob_id: blob_id_for_testing(),
                    },
                },
                // Both regular and managed are None/empty - entry can be removed.
                BlobInfoV2::Valid(ValidBlobInfoV2 {
                    regular_blob_info: None,
                    managed_blob_info: None,
                }),
            ),
            delete_certified_managed: (
                BlobInfoV2::Valid(ValidBlobInfoV2 {
                    regular_blob_info: None,
                    managed_blob_info: Some(ManagedBlobInfo {
                        is_metadata_stored: false,
                        initial_certified_epoch: Some(5),
                        registered: [object_id_for_testing(1)].into_iter().collect(),
                        registered_deletable_counts: 1,
                        certified: [object_id_for_testing(1)].into_iter().collect(),
                        certified_deletable_counts: 1,
                    }),
                }),
                BlobInfoMergeOperand::ChangeStatus {
                    change_type: BlobStatusChangeType::DeleteManaged {
                        blob_manager_id: object_id_for_testing(1),
                        was_certified: true,
                    },
                    change_info: BlobStatusChangeInfo {
                        epoch: 1,
                        end_epoch: 10,
                        deletable: true,
                        status_event: event_id_for_testing(),
                        blob_id: blob_id_for_testing(),
                    },
                },
                BlobInfoV2::Valid(ValidBlobInfoV2 {
                    regular_blob_info: None,
                    managed_blob_info: None,  // Removed since all references are gone
                }),
            ),
            delete_one_of_multiple_managed: (
                BlobInfoV2::Valid(ValidBlobInfoV2 {
                    regular_blob_info: None,
                    managed_blob_info: Some(ManagedBlobInfo {
                        is_metadata_stored: false,
                        initial_certified_epoch: Some(3),
                        registered: [object_id_for_testing(1), object_id_for_testing(2)]
                            .into_iter().collect(),
                        registered_deletable_counts: 1,
                        certified: [object_id_for_testing(1)].into_iter().collect(),
                        certified_deletable_counts: 1,
                    }),
                }),
                BlobInfoMergeOperand::ChangeStatus {
                    change_type: BlobStatusChangeType::DeleteManaged {
                        blob_manager_id: object_id_for_testing(1),
                        was_certified: true,
                    },
                    change_info: BlobStatusChangeInfo {
                        epoch: 1,
                        end_epoch: 10,
                        deletable: true,
                        status_event: event_id_for_testing(),
                        blob_id: blob_id_for_testing(),
                    },
                },
                BlobInfoV2::Valid(ValidBlobInfoV2 {
                    regular_blob_info: None,
                    managed_blob_info: Some(ManagedBlobInfo {
                        is_metadata_stored: false,
                        initial_certified_epoch: None,  // Unset since no certified refs remain
                        registered: [object_id_for_testing(2)].into_iter().collect(),
                        registered_deletable_counts: 0,
                        certified: Default::default(),
                        certified_deletable_counts: 0,
                    }),
                }),
            ),
            delete_managed_keep_regular: (
                BlobInfoV2::Valid(ValidBlobInfoV2 {
                    regular_blob_info: Some(ValidBlobInfoV1 {
                        count_deletable_total: 2,
                        count_deletable_certified: 1,
                        latest_seen_deletable_registered_end_epoch: Some(10),
                        latest_seen_deletable_certified_end_epoch: Some(10),
                        ..Default::default()
                    }),
                    managed_blob_info: Some(ManagedBlobInfo {
                        is_metadata_stored: false,
                        initial_certified_epoch: None,
                        registered: [object_id_for_testing(1)].into_iter().collect(),
                        registered_deletable_counts: 1,
                        certified: Default::default(),
                        certified_deletable_counts: 0,
                    }),
                }),
                BlobInfoMergeOperand::ChangeStatus {
                    change_type: BlobStatusChangeType::DeleteManaged {
                        blob_manager_id: object_id_for_testing(1),
                        was_certified: false,
                    },
                    change_info: BlobStatusChangeInfo {
                        epoch: 1,
                        end_epoch: 10,
                        deletable: true,
                        status_event: event_id_for_testing(),
                        blob_id: blob_id_for_testing(),
                    },
                },
                BlobInfoV2::Valid(ValidBlobInfoV2 {
                    regular_blob_info: Some(ValidBlobInfoV1 {
                        count_deletable_total: 2,
                        count_deletable_certified: 1,
                        latest_seen_deletable_registered_end_epoch: Some(10),
                        latest_seen_deletable_certified_end_epoch: Some(10),
                        ..Default::default()
                    }),
                    managed_blob_info: None,
                }),
            ),
            delete_permanent_managed: (
                BlobInfoV2::Valid(ValidBlobInfoV2 {
                    regular_blob_info: None,
                    managed_blob_info: Some(ManagedBlobInfo {
                        is_metadata_stored: false,
                        initial_certified_epoch: None,
                        registered: [object_id_for_testing(1), object_id_for_testing(2)]
                            .into_iter().collect(),
                        registered_deletable_counts: 0,
                        certified: Default::default(),
                        certified_deletable_counts: 0,
                    }),
                }),
                BlobInfoMergeOperand::ChangeStatus {
                    change_type: BlobStatusChangeType::DeleteManaged {
                        blob_manager_id: object_id_for_testing(2),
                        was_certified: false,
                    },
                    change_info: BlobStatusChangeInfo {
                        epoch: 1,
                        end_epoch: 10,
                        deletable: false,
                        status_event: event_id_for_testing(),
                        blob_id: blob_id_for_testing(),
                    },
                },
                BlobInfoV2::Valid(ValidBlobInfoV2 {
                    regular_blob_info: None,
                    managed_blob_info: Some(ManagedBlobInfo {
                        is_metadata_stored: false,
                        initial_certified_epoch: None,
                        registered: [object_id_for_testing(1)].into_iter().collect(),
                        registered_deletable_counts: 0,
                        certified: Default::default(),
                        certified_deletable_counts: 0,
                    }),
                }),
            ),
            delete_certified_managed_with_mixed_deletable: (
                BlobInfoV2::Valid(ValidBlobInfoV2 {
                    regular_blob_info: None,
                    managed_blob_info: Some(ManagedBlobInfo {
                        is_metadata_stored: false,
                        initial_certified_epoch: Some(3),
                        registered: [object_id_for_testing(1), object_id_for_testing(2), object_id_for_testing(3)]
                            .into_iter().collect(),
                        registered_deletable_counts: 2,
                        certified: [object_id_for_testing(1), object_id_for_testing(2)]
                            .into_iter().collect(),
                        certified_deletable_counts: 1,
                    }),
                }),
                BlobInfoMergeOperand::ChangeStatus {
                    change_type: BlobStatusChangeType::DeleteManaged {
                        blob_manager_id: object_id_for_testing(1),
                        was_certified: true,
                    },
                    change_info: BlobStatusChangeInfo {
                        epoch: 5,
                        end_epoch: 15,
                        deletable: true,
                        status_event: event_id_for_testing(),
                        blob_id: blob_id_for_testing(),
                    },
                },
                BlobInfoV2::Valid(ValidBlobInfoV2 {
                    regular_blob_info: None,
                    managed_blob_info: Some(ManagedBlobInfo {
                        is_metadata_stored: false,
                        initial_certified_epoch: Some(3),
                        registered: [object_id_for_testing(2), object_id_for_testing(3)]
                            .into_iter().collect(),
                        registered_deletable_counts: 1,
                        certified: [object_id_for_testing(2)].into_iter().collect(),
                        certified_deletable_counts: 0,
                    }),
                }),
            ),
            delete_managed_both_v1_and_v2_certified: (
                BlobInfoV2::Valid(ValidBlobInfoV2 {
                    regular_blob_info: Some(ValidBlobInfoV1 {
                        count_deletable_total: 1,
                        count_deletable_certified: 1,
                        latest_seen_deletable_registered_end_epoch: Some(20),
                        latest_seen_deletable_certified_end_epoch: Some(20),
                        initial_certified_epoch: Some(2),
                        ..Default::default()
                    }),
                    managed_blob_info: Some(ManagedBlobInfo {
                        is_metadata_stored: false,
                        initial_certified_epoch: Some(4),
                        registered: [object_id_for_testing(1)].into_iter().collect(),
                        registered_deletable_counts: 1,
                        certified: [object_id_for_testing(1)].into_iter().collect(),
                        certified_deletable_counts: 1,
                    }),
                }),
                BlobInfoMergeOperand::ChangeStatus {
                    change_type: BlobStatusChangeType::DeleteManaged {
                        blob_manager_id: object_id_for_testing(1),
                        was_certified: true,
                    },
                    change_info: BlobStatusChangeInfo {
                        epoch: 10,
                        end_epoch: 25,
                        deletable: true,
                        status_event: event_id_for_testing(),
                        blob_id: blob_id_for_testing(),
                    },
                },
                BlobInfoV2::Valid(ValidBlobInfoV2 {
                    regular_blob_info: Some(ValidBlobInfoV1 {
                        count_deletable_total: 1,
                        count_deletable_certified: 1,
                        latest_seen_deletable_registered_end_epoch: Some(20),
                        latest_seen_deletable_certified_end_epoch: Some(20),
                        initial_certified_epoch: Some(2),
                        ..Default::default()
                    }),
                    managed_blob_info: None,
                }),
            ),
        ]
    }
    fn test_v2_delete_managed(
        preexisting: BlobInfoV2,
        operand: BlobInfoMergeOperand,
        expected: BlobInfoV2,
    ) {
        let result = preexisting.merge_with(operand);
        assert_eq!(result, expected);
    }
}

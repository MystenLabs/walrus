// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Keeping track of the status of blob IDs and on-chain `Blob` objects.

use std::{
    fmt::Debug,
    num::NonZeroU32,
    ops::Bound::{self, Unbounded},
    sync::{Arc, Mutex},
};

use enum_dispatch::enum_dispatch;
use rocksdb::{MergeOperands, Options};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sui_types::{base_types::ObjectID, event::EventID};
use tracing::Level;
use typed_store::{
    Map,
    TypedStoreError,
    rocks::{DBBatch, DBMap, ReadWriteOptions, RocksDB},
};
use walrus_core::{BlobId, Epoch};
use walrus_storage_node_client::api::{BlobStatus, DeletableCounts};
use walrus_sui::types::{BlobCertified, BlobDeleted, BlobEvent, BlobRegistered, InvalidBlobId};

use self::per_object_blob_info::PerObjectBlobInfoMergeOperand;
pub(crate) use self::per_object_blob_info::{PerObjectBlobInfo, PerObjectBlobInfoApi};
use super::{DatabaseConfig, constants, database_config::DatabaseTableOptions};

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
pub(crate) fn blob_info_cf_options(db_config: &DatabaseConfig) -> Options {
    let mut options = db_config.blob_info().to_options();
    options.set_merge_operator("merge blob info", merge_mergeable::<BlobInfo>, |_, _, _| {
        None
    });
    options
}

/// Returns the options for the per object blob info column family.
pub(crate) fn per_object_blob_info_cf_options(db_config: &DatabaseConfig) -> Options {
    let mut options = db_config.per_object_blob_info().to_options();
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

    pub fn options(db_config: &DatabaseConfig) -> Vec<(&'static str, Options)> {
        vec![
            (
                constants::aggregate_blob_info_cf_name(),
                blob_info_cf_options(db_config),
            ),
            (
                constants::per_object_blob_info_cf_name(),
                per_object_blob_info_cf_options(db_config),
            ),
            (
                constants::event_index_cf_name(),
                // Doesn't make sense to have special options for the table containing a single
                // value.
                DatabaseTableOptions::default().to_options(),
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

    /// Returns an iterator over all blobs that were certified before the specified epoch in the
    /// blob info table starting with the `starting_blob_id` bound.
    #[tracing::instrument(skip_all)]
    pub fn certified_blob_info_iter_before_epoch(
        &self,
        before_epoch: Epoch,
        starting_blob_id_bound: Bound<BlobId>,
    ) -> BlobInfoIterator {
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
    ) -> PerObjectBlobInfoIterator {
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
pub(super) trait Mergeable: ToBytes + Debug + DeserializeOwned + Serialize + Sized {
    type MergeOperand: Debug + DeserializeOwned + ToBytes;

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
}

impl ToBytes for BlobInfoMergeOperand {}

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
// INV: latest_seen_deletable_registered_epoch >= latest_seen_deletable_certified_epoch
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub(crate) struct ValidBlobInfoV1 {
    pub is_metadata_stored: bool,
    pub count_deletable_total: u32,
    pub count_deletable_certified: u32,
    pub permanent_total: Option<PermanentBlobInfoV1>,
    pub permanent_certified: Option<PermanentBlobInfoV1>,
    pub initial_certified_epoch: Option<Epoch>,

    // TODO: The following are helper fields that are needed as long as we don't properly clean up
    // deletable blobs. (WAL-473)
    pub latest_seen_deletable_registered_epoch: Option<Epoch>,
    pub latest_seen_deletable_certified_epoch: Option<Epoch>,
}

impl From<ValidBlobInfoV1> for BlobInfoV1 {
    fn from(value: ValidBlobInfoV1) -> Self {
        Self::Valid(value)
    }
}

impl ValidBlobInfoV1 {
    fn to_blob_status(&self, current_epoch: Epoch) -> BlobStatus {
        // TODO: The following should be adjusted/simplified when we have proper cleanup (WAL-473).
        let count_deletable_total = if self
            .latest_seen_deletable_registered_epoch
            .is_some_and(|e| e > current_epoch)
        {
            self.count_deletable_total
        } else {
            Default::default()
        };
        let count_deletable_certified = if self
            .latest_seen_deletable_certified_epoch
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
        {
            if *end_epoch > current_epoch {
                return BlobStatus::Permanent {
                    end_epoch: *end_epoch,
                    is_certified: true,
                    status_event: *event,
                    deletable_counts,
                    initial_certified_epoch,
                };
            }
        }
        if let Some(PermanentBlobInfoV1 {
            end_epoch, event, ..
        }) = self.permanent_total.as_ref()
        {
            if *end_epoch > current_epoch {
                return BlobStatus::Permanent {
                    end_epoch: *end_epoch,
                    is_certified: false,
                    status_event: *event,
                    deletable_counts,
                    initial_certified_epoch,
                };
            }
        }

        if deletable_counts != Default::default() {
            BlobStatus::Deletable {
                initial_certified_epoch,
                deletable_counts,
            }
        } else {
            BlobStatus::Nonexistent
        }
    }

    // TODO: This is currently just an approximation: It is possible that this returns true even
    // though there is no existing certified blob because the blob with the latest expiration epoch
    // was deleted. This should be adjusted/simplified when we have proper cleanup (WAL-473).
    fn is_certified(&self, current_epoch: Epoch) -> bool {
        let exists_certified_permanent_blob = self
            .permanent_certified
            .as_ref()
            .is_some_and(|p| p.end_epoch > current_epoch);
        let maybe_exists_certified_deletable_blob = self.count_deletable_certified > 0
            && self
                .latest_seen_deletable_certified_epoch
                .is_some_and(|l| l > current_epoch);
        self.initial_certified_epoch
            .is_some_and(|epoch| epoch <= current_epoch)
            && (exists_certified_permanent_blob || maybe_exists_certified_deletable_blob)
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
                    Self::decrement_deletable_counter_on_deletion(&mut self.count_deletable_total);
                    if was_certified {
                        Self::decrement_deletable_counter_on_deletion(
                            &mut self.count_deletable_certified,
                        );
                    }
                }
            }
        } else {
            match change_type {
                BlobStatusChangeType::Register => {
                    Self::register_permanent(&mut self.permanent_total, &change_info);
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
                BlobStatusChangeType::Delete { was_certified } => {
                    Self::delete_permanent(
                        &mut self.permanent_total,
                        &mut self.permanent_certified,
                        was_certified,
                    );
                }
            }
        }

        // Update initial certified epoch.
        match change_type {
            BlobStatusChangeType::Certify => {
                self.update_initial_certified_epoch(change_info.epoch, !was_certified);
            }
            BlobStatusChangeType::Delete { .. } => {
                self.maybe_unset_initial_certified_epoch();
            }
            // Explicit matches to make sure we cover all cases.
            BlobStatusChangeType::Register | BlobStatusChangeType::Extend => (),
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
        self.latest_seen_deletable_registered_epoch = Some(
            epoch.max(
                self.latest_seen_deletable_registered_epoch
                    .unwrap_or_default(),
            ),
        )
    }

    fn maybe_increase_latest_deletable_certified_epoch(&mut self, epoch: Epoch) {
        self.latest_seen_deletable_certified_epoch = Some(
            epoch.max(
                self.latest_seen_deletable_certified_epoch
                    .unwrap_or_default(),
            ),
        )
    }

    /// Decrements a counter on blob deletion.
    ///
    /// If the counter is 0, an error is logged in release builds and the function panics in dev
    /// builds.
    fn decrement_deletable_counter_on_deletion(counter: &mut u32) {
        debug_assert!(*counter > 0);
        *counter = counter.checked_sub(1).unwrap_or_else(|| {
            tracing::error!("attempt to delete blob when count was already 0");
            0
        });
    }

    /// Processes a register status change on the [`Option<PermanentBlobInfoV1>`] object
    /// representing all permanent blobs.
    fn register_permanent(
        permanent_total: &mut Option<PermanentBlobInfoV1>,
        change_info: &BlobStatusChangeInfo,
    ) {
        PermanentBlobInfoV1::update_optional(permanent_total, change_info)
    }

    /// Processes a certify status change on the [`PermanentBlobInfoV1`] objects representing all
    /// and the certified permanent blobs.
    ///
    /// Returns whether the update was successful.
    fn certify_permanent(
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
    fn delete_permanent(
        permanent_total: &mut Option<PermanentBlobInfoV1>,
        permanent_certified: &mut Option<PermanentBlobInfoV1>,
        was_certified: bool,
    ) {
        Self::decrement_blob_info_inner(permanent_total);
        if was_certified {
            Self::decrement_blob_info_inner(permanent_certified);
        }
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

    #[cfg(test)]
    fn check_invariants(&self) {
        let Self {
            is_metadata_stored: _,
            count_deletable_total,
            count_deletable_certified,
            permanent_total,
            permanent_certified,
            initial_certified_epoch,
            latest_seen_deletable_registered_epoch,
            latest_seen_deletable_certified_epoch,
            ..
        } = self;

        assert!(count_deletable_total >= count_deletable_certified);
        match initial_certified_epoch {
            None => assert!(*count_deletable_certified == 0 && permanent_certified.is_none()),
            Some(_) => assert!(*count_deletable_certified > 0 || permanent_certified.is_some()),
        }

        match (permanent_total, permanent_certified) {
            (None, Some(_)) => panic!("permanent_total.is_none() => permanent_certified.is_none()"),
            (Some(total_inner), Some(certified_inner)) => {
                assert!(total_inner.end_epoch >= certified_inner.end_epoch);
                assert!(total_inner.count >= certified_inner.count);
            }
            _ => (),
        }

        match (
            latest_seen_deletable_registered_epoch,
            latest_seen_deletable_certified_epoch,
        ) {
            (None, Some(_)) => panic!(
                "latest_seen_deletable_registered_epoch.is_none() => \
                latest_seen_deletable_certified_epoch.is_none()"
            ),
            (Some(registered), Some(certified)) => {
                assert!(registered >= certified);
            }
            _ => (),
        }
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

    // TODO: This is currently just an approximation: It is possible that this returns true even
    // though there is no existing registered blob because the blob with the latest expiration epoch
    // was deleted. This should be adjusted/simplified when we have proper cleanup (WAL-473).
    fn is_registered(&self, current_epoch: Epoch) -> bool {
        let Self::Valid(ValidBlobInfoV1 {
            count_deletable_total,
            permanent_total,
            latest_seen_deletable_registered_epoch,
            ..
        }) = self
        else {
            return false;
        };

        let exists_registered_permanent_blob = permanent_total
            .as_ref()
            .is_some_and(|p| p.end_epoch > current_epoch);
        let maybe_exists_registered_deletable_blob = *count_deletable_total > 0
            && latest_seen_deletable_registered_epoch.is_some_and(|l| l > current_epoch);

        exists_registered_permanent_blob || maybe_exists_registered_deletable_blob
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
                        latest_seen_deletable_registered_epoch: Some(end_epoch),
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
            BlobInfoMergeOperand::MarkMetadataStored(_) => Some(
                ValidBlobInfoV1 {
                    is_metadata_stored: true,
                    ..Default::default()
                }
                .into(),
            ),
            BlobInfoMergeOperand::ChangeStatus { .. } => {
                tracing::error!(
                    ?operand,
                    "encountered an unexpected update for an untracked blob ID"
                );
                None
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
        let blob_info = match status {
            BlobCertificationStatus::Invalid => BlobInfoV1::Invalid {
                epoch: invalidated_epoch.unwrap(),
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

        fn merge_with(self, operand: Self::MergeOperand) -> Self {
            match self {
                Self::V1(value) => Self::V1(value.merge_with(operand)),
            }
        }

        fn merge_new(operand: Self::MergeOperand) -> Option<Self> {
            PerObjectBlobInfoV1::merge_new(operand).map(Self::from)
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
    }

    impl ToBytes for PerObjectBlobInfoV1 {}

    impl Mergeable for PerObjectBlobInfoV1 {
        type MergeOperand = PerObjectBlobInfoMergeOperand;

        fn merge_with(
            mut self,
            PerObjectBlobInfoMergeOperand {
                change_type,
                change_info,
            }: PerObjectBlobInfoMergeOperand,
        ) -> Self {
            assert_eq!(self.blob_id, change_info.blob_id);
            assert_eq!(self.deletable, change_info.deletable);
            assert!(!self.deleted);
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
    skip(existing_val, operands),
    fields(existing_val = existing_val.is_some())
)]
pub(crate) fn merge_mergeable<T: Mergeable>(
    key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let mut current_val: Option<T> = existing_val.and_then(deserialize_from_db);

    for operand_bytes in operands {
        let Some(operand) = deserialize_from_db::<T::MergeOperand>(operand_bytes) else {
            continue;
        };
        tracing::debug!(?current_val, ?operand, "updating blob info");

        current_val = T::merge(current_val, operand);
    }

    current_val.as_ref().map(|value| value.to_bytes())
}

#[cfg(test)]
mod tests {
    use walrus_sui::test_utils::{event_id_for_testing, fixed_event_id_for_testing};
    use walrus_test_utils::param_test;

    use super::*;

    fn check_invariants(blob_info: &BlobInfoV1) {
        if let BlobInfoV1::Valid(valid_blob_info) = blob_info {
            valid_blob_info.check_invariants()
        }
    }

    param_test! {
        test_merge_new_expected_failure_cases: [
            certify_permanent: (BlobInfoMergeOperand::new_change_for_testing(
                BlobStatusChangeType::Certify,false, 42, 314, event_id_for_testing()
            )),
            certify_deletable: (BlobInfoMergeOperand::new_change_for_testing(
                BlobStatusChangeType::Certify, true, 42, 314, event_id_for_testing()
            )),
            extend: (BlobInfoMergeOperand::new_change_for_testing(
                BlobStatusChangeType::Extend, false, 42, 314, event_id_for_testing()
            )),
            delete_deletable: (BlobInfoMergeOperand::new_change_for_testing(
                BlobStatusChangeType::Delete { was_certified: true },
                false,
                42,
                314,
                event_id_for_testing(),
            )),
            delete_permanent: (BlobInfoMergeOperand::new_change_for_testing(
                BlobStatusChangeType::Delete { was_certified: false },
                false,
                42,
                314,
                event_id_for_testing(),
            )),
        ]
    }
    fn test_merge_new_expected_failure_cases(operand: BlobInfoMergeOperand) {
        assert!(BlobInfoV1::merge_new(operand).is_none());
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
            metadata_true: (BlobInfoMergeOperand::MarkMetadataStored(true)),
            metadata_false: (BlobInfoMergeOperand::MarkMetadataStored(false)),
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
        preexisting_info.check_invariants();
        let expected_updated_info = ValidBlobInfoV1 {
            is_metadata_stored: true,
            ..preexisting_info.clone()
        };
        expected_updated_info.check_invariants();

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
                    latest_seen_deletable_registered_epoch: Some(2),
                    ..Default::default()
                },
            ),
            register_additional_deletable1: (
                ValidBlobInfoV1{
                    count_deletable_total: 3,
                    latest_seen_deletable_registered_epoch: Some(2),
                    ..Default::default()
                },
                BlobInfoMergeOperand::new_change_for_testing(
                    BlobStatusChangeType::Register, true, 1, 5, event_id_for_testing()
                ),
                ValidBlobInfoV1{
                    count_deletable_total: 4,
                    latest_seen_deletable_registered_epoch: Some(5),
                    ..Default::default()
                },
            ),
            register_additional_deletable2: (
                ValidBlobInfoV1{
                    count_deletable_total: 3,
                    latest_seen_deletable_registered_epoch: Some(4),
                    ..Default::default()
                },
                BlobInfoMergeOperand::new_change_for_testing(
                    BlobStatusChangeType::Register, true, 1, 3, event_id_for_testing()
                ),
                ValidBlobInfoV1{
                    count_deletable_total: 4,
                    latest_seen_deletable_registered_epoch: Some(4),
                    ..Default::default()
                },
            ),
            certify_first_deletable: (
                ValidBlobInfoV1{
                    count_deletable_total: 3,
                    latest_seen_deletable_registered_epoch: Some(4),
                    ..Default::default()
                },
                BlobInfoMergeOperand::new_change_for_testing(
                    BlobStatusChangeType::Certify, true, 1, 4, event_id_for_testing()
                ),
                ValidBlobInfoV1{
                    count_deletable_total: 3,
                    count_deletable_certified: 1,
                    initial_certified_epoch: Some(1),
                    latest_seen_deletable_registered_epoch: Some(4),
                    latest_seen_deletable_certified_epoch: Some(4),
                    ..Default::default()
                },
            ),
            certify_additional_deletable1: (
                ValidBlobInfoV1{
                    count_deletable_total: 3,
                    count_deletable_certified: 1,
                    initial_certified_epoch: Some(0),
                    latest_seen_deletable_registered_epoch: Some(4),
                    latest_seen_deletable_certified_epoch: Some(4),
                    ..Default::default()
                },
                BlobInfoMergeOperand::new_change_for_testing(
                    BlobStatusChangeType::Certify, true, 1, 2, event_id_for_testing()
                ),
                ValidBlobInfoV1{
                    count_deletable_total: 3,
                    count_deletable_certified: 2,
                    initial_certified_epoch: Some(0),
                    latest_seen_deletable_registered_epoch: Some(4),
                    latest_seen_deletable_certified_epoch: Some(4),
                    ..Default::default()
                },
            ),
            certify_additional_deletable2: (
                ValidBlobInfoV1{
                    count_deletable_total: 3,
                    count_deletable_certified: 1,
                    initial_certified_epoch: Some(1),
                    latest_seen_deletable_registered_epoch: Some(5),
                    latest_seen_deletable_certified_epoch: Some(4),
                    ..Default::default()
                },
                BlobInfoMergeOperand::new_change_for_testing(
                    BlobStatusChangeType::Certify, true, 0, 5, event_id_for_testing()
                ),
                ValidBlobInfoV1{
                    count_deletable_total: 3,
                    count_deletable_certified: 2,
                    initial_certified_epoch: Some(0),
                    latest_seen_deletable_registered_epoch: Some(5),
                    latest_seen_deletable_certified_epoch: Some(5),
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
                    latest_seen_deletable_registered_epoch: Some(4),
                    latest_seen_deletable_certified_epoch: Some(4),
                    ..Default::default()
                },
                BlobInfoMergeOperand::new_change_for_testing(
                    BlobStatusChangeType::Extend, true, 3, 42, event_id_for_testing()
                ),
                ValidBlobInfoV1{
                    count_deletable_total: 3,
                    count_deletable_certified: 1,
                    initial_certified_epoch: Some(0),
                    latest_seen_deletable_registered_epoch: Some(42),
                    latest_seen_deletable_certified_epoch: Some(42),
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
                    latest_seen_deletable_registered_epoch: Some(8),
                    latest_seen_deletable_certified_epoch: Some(4),
                    ..Default::default()
                },
                BlobInfoMergeOperand::new_change_for_testing(
                    BlobStatusChangeType::Certify, true, 4, 6, event_id_for_testing()
                ),
                ValidBlobInfoV1{
                    count_deletable_total: 3,
                    count_deletable_certified: 2,
                    initial_certified_epoch: Some(4),
                    latest_seen_deletable_registered_epoch: Some(8),
                    latest_seen_deletable_certified_epoch: Some(6),
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
            delete_permanent_blob: (
                ValidBlobInfoV1{
                    permanent_total: Some(PermanentBlobInfoV1::new_fixed_for_testing(3, 5, 0)),
                    permanent_certified: Some(PermanentBlobInfoV1::new_fixed_for_testing(2, 5, 1)),
                    initial_certified_epoch: Some(1),
                    ..Default::default()
                },
                BlobInfoMergeOperand::new_change_for_testing(
                    BlobStatusChangeType::Delete { was_certified: true },
                    false,
                    2,
                    6,
                    fixed_event_id_for_testing(2),
                ),
                ValidBlobInfoV1{
                    permanent_total: Some(PermanentBlobInfoV1::new_fixed_for_testing(2, 5, 0)),
                    permanent_certified: Some(PermanentBlobInfoV1::new_fixed_for_testing(1, 5, 1)),
                    initial_certified_epoch: Some(1),
                    ..Default::default()
                },
            ),
            delete_last_permanent_blob: (
                ValidBlobInfoV1{
                    permanent_total: Some(PermanentBlobInfoV1::new_fixed_for_testing(2, 5, 0)),
                    permanent_certified: Some(PermanentBlobInfoV1::new_fixed_for_testing(1, 5, 1)),
                    initial_certified_epoch: Some(1),
                    ..Default::default()
                },
                BlobInfoMergeOperand::new_change_for_testing(
                    BlobStatusChangeType::Delete { was_certified: true },
                    false,
                    1,
                    6,
                    fixed_event_id_for_testing(2),
                ),
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
                    latest_seen_deletable_registered_epoch: Some(5),
                    latest_seen_deletable_certified_epoch: Some(4),
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
                    latest_seen_deletable_registered_epoch: Some(5),
                    latest_seen_deletable_certified_epoch: Some(4),
                    ..Default::default()
                },
            ),
            delete_last_deletable_blob: (
                ValidBlobInfoV1{
                    count_deletable_total: 2,
                    count_deletable_certified: 1,
                    initial_certified_epoch: Some(1),
                    latest_seen_deletable_registered_epoch: Some(5),
                    latest_seen_deletable_certified_epoch: Some(4),
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
                    latest_seen_deletable_registered_epoch: Some(5),
                    latest_seen_deletable_certified_epoch: Some(4),
                    ..Default::default()
                },
            ),
            delete_uncertified_permanent_blob: (
                ValidBlobInfoV1{
                    permanent_total: Some(PermanentBlobInfoV1::new_fixed_for_testing(3, 5, 0)),
                    ..Default::default()
                },
                BlobInfoMergeOperand::new_change_for_testing(
                    BlobStatusChangeType::Delete { was_certified: false },
                    false,
                    1,
                    6,
                    fixed_event_id_for_testing(2),
                ),
                ValidBlobInfoV1{
                    permanent_total: Some(PermanentBlobInfoV1::new_fixed_for_testing(2, 5, 0)),
                    ..Default::default()
                },
            ),
            delete_last_uncertified_permanent_blob: (
                ValidBlobInfoV1{
                    permanent_total: Some(PermanentBlobInfoV1::new_fixed_for_testing(1, 5, 0)),
                    ..Default::default()
                },
                BlobInfoMergeOperand::new_change_for_testing(
                    BlobStatusChangeType::Delete { was_certified: false },
                    false,
                    2,
                    5,
                    fixed_event_id_for_testing(2),
                ),
                ValidBlobInfoV1{
                    permanent_total: None,
                    ..Default::default()
                },
            ),
            delete_uncertified_deletable_blob: (
                ValidBlobInfoV1{
                    count_deletable_total: 3,
                    latest_seen_deletable_registered_epoch: Some(5),
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
                    latest_seen_deletable_registered_epoch: Some(5),
                    ..Default::default()
                },
            ),
            delete_last_uncertified_deletable_blob: (
                ValidBlobInfoV1{
                    count_deletable_total: 1,
                    latest_seen_deletable_registered_epoch: Some(5),
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
                    latest_seen_deletable_registered_epoch: Some(5),
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
        preexisting_info.check_invariants();
        expected_info.check_invariants();

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
        preexisting_info.check_invariants();
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
                    latest_seen_deletable_registered_epoch: Some(2),
                    ..Default::default()
                },
                1,
                2,
            ),
            expired_deletable_certified: (
                ValidBlobInfoV1 {
                    count_deletable_total: 1,
                    latest_seen_deletable_registered_epoch: Some(2),
                    count_deletable_certified: 1,
                    latest_seen_deletable_certified_epoch: Some(2),
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
        assert!(!matches!(
            BlobInfoV1::Valid(blob_info.clone()).to_blob_status(epoch_not_expired),
            BlobStatus::Nonexistent,
        ));
        assert!(matches!(
            BlobInfoV1::Valid(blob_info).to_blob_status(epoch_expired),
            BlobStatus::Nonexistent,
        ));
    }
}

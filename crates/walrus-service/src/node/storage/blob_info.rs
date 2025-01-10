// Copyright (c) Mysten Labs, Inc.
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
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sui_types::{base_types::ObjectID, event::EventID};
use tracing::Level;
use typed_store::{
    rocks::{DBBatch, DBMap, ReadWriteOptions, RocksDB},
    Map,
    TypedStoreError,
};
use walrus_core::{BlobId, Epoch};
use walrus_sdk::api::{BlobStatus, DeletableCounts};
use walrus_sui::types::{BlobCertified, BlobDeleted, BlobEvent, BlobRegistered, InvalidBlobId};

use self::per_object_blob_info::PerObjectBlobInfoMergeOperand;
pub(crate) use self::per_object_blob_info::{PerObjectBlobInfo, PerObjectBlobInfoApi};
use super::{database_config::DatabaseTableOptions, DatabaseConfig};

#[derive(Debug, Clone)]
pub(super) struct BlobInfoTable {
    aggregate_blob_info: DBMap<BlobId, BlobInfo>,
    per_object_blob_info: DBMap<ObjectID, PerObjectBlobInfo>,
    latest_handled_event_index: Arc<Mutex<DBMap<(), u64>>>,
}

impl BlobInfoTable {
    const AGGREGATE_BLOB_INFO_COLUMN_FAMILY_NAME: &'static str = "aggregate_blob_info";
    const PER_OBJECT_BLOB_INFO_COLUMN_FAMILY_NAME: &'static str = "per_object_blob_info";
    const EVENT_INDEX_COLUMN_FAMILY_NAME: &'static str = "latest_handled_event_index";

    pub fn reopen(database: &Arc<RocksDB>) -> Result<Self, TypedStoreError> {
        let aggregate_blob_info = DBMap::reopen(
            database,
            Some(Self::AGGREGATE_BLOB_INFO_COLUMN_FAMILY_NAME),
            &ReadWriteOptions::default(),
            false,
        )?;
        let per_object_blob_info = DBMap::reopen(
            database,
            Some(Self::PER_OBJECT_BLOB_INFO_COLUMN_FAMILY_NAME),
            &ReadWriteOptions::default(),
            false,
        )?;
        let latest_handled_event_index = Arc::new(Mutex::new(DBMap::reopen(
            database,
            Some(Self::EVENT_INDEX_COLUMN_FAMILY_NAME),
            &ReadWriteOptions::default(),
            false,
        )?));

        Ok(Self {
            aggregate_blob_info,
            per_object_blob_info,
            latest_handled_event_index,
        })
    }

    pub fn options(db_config: &DatabaseConfig) -> Vec<(&'static str, Options)> {
        let mut blob_info_options = db_config.blob_info.to_options();
        blob_info_options.set_merge_operator(
            "merge blob info",
            merge_mergeable::<BlobInfo>,
            |_, _, _| None,
        );

        let per_object_blob_info_options = {
            let mut options = db_config.per_object_blob_info.to_options();
            options.set_merge_operator(
                "merge per object blob info",
                merge_mergeable::<PerObjectBlobInfo>,
                |_, _, _| None,
            );
            options
        };

        vec![
            (
                Self::AGGREGATE_BLOB_INFO_COLUMN_FAMILY_NAME,
                blob_info_options,
            ),
            (
                Self::PER_OBJECT_BLOB_INFO_COLUMN_FAMILY_NAME,
                per_object_blob_info_options,
            ),
            (
                Self::EVENT_INDEX_COLUMN_FAMILY_NAME,
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
        event_index: usize,
        event: &BlobEvent,
    ) -> Result<(), TypedStoreError> {
        let event_index = event_index.try_into().expect("assume 64-bit architecture");
        let latest_handled_event_index = self.latest_handled_event_index.lock().unwrap();
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

    fn has_event_been_handled(latest_handled_index: Option<u64>, event_index: u64) -> bool {
        latest_handled_index.map_or(false, |i| event_index <= i)
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
                    .safe_range_iter((starting_blob_id_bound, Unbounded)),
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
}

// TODO(mlegner): Rewrite other tests without relying on blob-info internals. (#900)
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
        self.aggregate_blob_info.keys().collect()
    }

    pub fn insert_batch<'a>(
        &self,
        batch: &mut DBBatch,
        new_vals: impl IntoIterator<Item = (&'a BlobId, &'a BlobInfo)>,
    ) -> Result<(), TypedStoreError> {
        batch.insert_batch(&self.aggregate_blob_info, new_vals)?;
        Ok(())
    }
}

/// An iterator over the blob info table.
pub(crate) struct BlobInfoIter<I: ?Sized>
where
    I: Iterator<Item = Result<(BlobId, BlobInfo), TypedStoreError>> + Send,
{
    iter: Box<I>,
    before_epoch: Epoch,
}

impl<I: ?Sized> BlobInfoIter<I>
where
    I: Iterator<Item = Result<(BlobId, BlobInfo), TypedStoreError>> + Send,
{
    pub fn new(iter: Box<I>, before_epoch: Epoch) -> Self {
        Self { iter, before_epoch }
    }
}

impl<I: ?Sized> Debug for BlobInfoIter<I>
where
    I: Iterator<Item = Result<(BlobId, BlobInfo), TypedStoreError>> + Send,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlobInfoIter")
            .field("before_epoch", &self.before_epoch)
            .finish()
    }
}

impl<I: ?Sized> Iterator for BlobInfoIter<I>
where
    I: Iterator<Item = Result<(BlobId, BlobInfo), TypedStoreError>> + Send,
{
    type Item = Result<(BlobId, BlobInfo), TypedStoreError>;

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

pub type BlobInfoIterator<'a> =
    BlobInfoIter<dyn Iterator<Item = Result<(BlobId, BlobInfo), TypedStoreError>> + Send + 'a>;

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

/// Trait defining methods for retrieving information about a blob.
// NB: Before adding functions to this trait, think twice if you really need it as it needs to be
// implementable by future internal representations of the blob status as well.
#[enum_dispatch]
pub(crate) trait BlobInfoApi {
    /// Returns a boolean indicating whether the metadata of the blob is stored.
    fn is_metadata_stored(&self) -> bool;
    /// Returns true iff there exists at least one non-expired deletable or permanent `Blob` object.
    fn is_registered(&self, current_epoch: Epoch) -> bool;
    /// Returns true iff there exists at least one non-expired and certified deletable or permanent
    /// `Blob` object.
    fn is_certified(&self, current_epoch: Epoch) -> bool;

    /// Returns the epoch at which this blob was first certified.
    ///
    /// Returns `None` if it isn't certified.
    fn initial_certified_epoch(&self) -> Option<Epoch>;
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
                // TODO: WAL-424
                // NOTE: Zero chance of triggering this event until Rust
                // implementation supports deny-listing.
                todo!("DenyListBlobDeleted")
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
        let deletable_counts = DeletableCounts {
            count_deletable_total: self
                .latest_seen_deletable_registered_epoch
                .is_some_and(|e| e > current_epoch)
                .then_some(self.count_deletable_total)
                .unwrap_or_default(),
            count_deletable_certified: self
                .latest_seen_deletable_certified_epoch
                .is_some_and(|e| e > current_epoch)
                .then_some(self.count_deletable_certified)
                .unwrap_or_default(),
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

    #[tracing::instrument]
    fn update_status(
        &mut self,
        change_type: BlobStatusChangeType,
        change_info: BlobStatusChangeInfo,
    ) {
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
                self.update_initial_certified_epoch(change_info.epoch);
            }
            BlobStatusChangeType::Delete { .. } => {
                self.maybe_unset_initial_certified_epoch();
            }
            // Explicit matches to make sure we cover all cases.
            BlobStatusChangeType::Register | BlobStatusChangeType::Extend => (),
        }
    }

    fn update_initial_certified_epoch(&mut self, new_certified_epoch: Epoch) {
        if self
            .initial_certified_epoch
            .map_or(true, |existing_epoch| existing_epoch > new_certified_epoch)
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
                *existing_info = Some(PermanentBlobInfoV1 {
                    count: NonZeroU32::new(1).unwrap(),
                    end_epoch: *new_end_epoch,
                    event: *new_status_event,
                })
            }
            Some(permanent_blob_info) => permanent_blob_info.update(change_info, true),
        }
    }

    #[cfg(test)]
    fn new_fixed_for_testing(count: u32, end_epoch: Epoch) -> Self {
        Self {
            count: NonZeroU32::new(count).unwrap(),
            end_epoch,
            event: walrus_sui::test_utils::fixed_event_id_for_testing(),
        }
    }

    #[cfg(test)]
    fn new_for_testing(count: u32, end_epoch: Epoch) -> Self {
        Self {
            count: NonZeroU32::new(count).unwrap(),
            end_epoch,
            event: walrus_sui::test_utils::event_id_for_testing(),
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
            permanent_certified,
            latest_seen_deletable_certified_epoch,
            ..
        }) = self
        else {
            return false;
        };

        let exists_registered_permanent_blob = permanent_total
            .as_ref()
            .is_some_and(|p| p.end_epoch > current_epoch)

            // TODO(mlegner): This is a temporary workaround due to a previous bug (#1163) with the
            // blob-status tracking. This is no longer needed after a full redeployment.
            || permanent_certified
                .as_ref()
                .is_some_and(|p| p.end_epoch > current_epoch);
        let maybe_exists_registered_deletable_blob = *count_deletable_total > 0
            && latest_seen_deletable_registered_epoch.is_some_and(|l| l > current_epoch)

            // TODO(mlegner): This is a temporary workaround due to a previous bug (#1163) with the
            // blob-status tracking. This is no longer needed after a full redeployment.
            || latest_seen_deletable_certified_epoch.is_some_and(|l| l > current_epoch);

        exists_registered_permanent_blob || maybe_exists_registered_deletable_blob
    }

    // TODO: This is currently just an approximation: It is possible that this returns true even
    // though there is no existing certified blob because the blob with the latest expiration epoch
    // was deleted. This should be adjusted/simplified when we have proper cleanup (WAL-473).
    fn is_certified(&self, current_epoch: Epoch) -> bool {
        let Self::Valid(ValidBlobInfoV1 {
            count_deletable_certified,
            permanent_certified,
            latest_seen_deletable_certified_epoch,
            ..
        }) = self
        else {
            return false;
        };

        let exists_certified_permanent_blob = permanent_certified
            .as_ref()
            .is_some_and(|p| p.end_epoch > current_epoch);
        let maybe_exists_certified_deletable_blob = *count_deletable_certified > 0
            && latest_seen_deletable_certified_epoch.is_some_and(|l| l > current_epoch);
        exists_certified_permanent_blob || maybe_exists_certified_deletable_blob
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
                }
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
        let BlobInfoMergeOperand::ChangeStatus {
            change_type: BlobStatusChangeType::Register,
            change_info:
                BlobStatusChangeInfo {
                    deletable,
                    epoch: _,
                    end_epoch,
                    status_event,
                    blob_id: _,
                },
        } = operand
        else {
            tracing::error!(
                ?operand,
                "encountered an update other than 'register' for an untracked blob ID"
            );
            return None;
        };

        Some(
            if deletable {
                ValidBlobInfoV1 {
                    count_deletable_total: 1,
                    latest_seen_deletable_registered_epoch: Some(end_epoch),
                    ..Default::default()
                }
            } else {
                ValidBlobInfoV1 {
                    permanent_total: Some(PermanentBlobInfoV1 {
                        count: NonZeroU32::new(1).unwrap(),
                        end_epoch,
                        event: status_event,
                    }),
                    ..Default::default()
                }
            }
            .into(),
        )
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
/// [`walrus_sdk::api::BlobStatus`] for anything public facing (e.g., communication to the client).
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
                let permanent_total = PermanentBlobInfoV1 {
                    count: NonZeroU32::new(1).unwrap(),
                    end_epoch,
                    event: current_status_event,
                };
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
    pub(crate) trait PerObjectBlobInfoApi {
        /// Returns the blob ID associated with this object.
        fn blob_id(&self) -> BlobId;
        /// Returns true iff the object is deletable.
        fn is_deletable(&self) -> bool;

        /// Returns true iff the object is not expired and not deleted.
        fn is_registered(&self, current_epoch: Epoch) -> bool;
        /// Returns true iff the object is certified and not expired and not deleted.
        fn is_certified(&self, current_epoch: Epoch) -> bool;

        /// Returns the epoch at which this blob was first certified.
        ///
        /// Returns `None` if it was never certified.
        fn certified_epoch(&self) -> Option<Epoch>;
    }

    #[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Clone)]
    #[enum_dispatch(PerObjectBlobInfoApi)]
    pub(crate) enum PerObjectBlobInfo {
        V1(PerObjectBlobInfoV1),
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

        fn is_certified(&self, current_epoch: Epoch) -> bool {
            self.is_registered(current_epoch) && self.certified_epoch.is_some()
        }

        fn certified_epoch(&self) -> Option<Epoch> {
            self.certified_epoch
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
                    panic!("cannot register an already registered blob");
                }
                BlobStatusChangeType::Certify => {
                    assert!(
                        self.certified_epoch.is_none(),
                        "cannot certify an already certified blob"
                    );
                    self.certified_epoch = Some(change_info.epoch);
                }
                BlobStatusChangeType::Extend => {
                    assert!(
                        self.certified_epoch.is_some(),
                        "cannot extend an uncertified blob"
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
pub(super) fn merge_mergeable<T: Mergeable>(
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
            invalidate: (BlobInfoMergeOperand::MarkInvalid {
                epoch: 0,
                status_event: event_id_for_testing()
            }),
            metadata_true: (BlobInfoMergeOperand::MarkMetadataStored(true)),
            metadata_false: (BlobInfoMergeOperand::MarkMetadataStored(false)),
            certify_deletable_false: (BlobInfoMergeOperand::new_change_for_testing(
                BlobStatusChangeType::Certify,false, 42, 314, event_id_for_testing()
            )),
            certify_deletable_true: (BlobInfoMergeOperand::new_change_for_testing(
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
    fn test_merge_new_expected_failure_cases(operand: BlobInfoMergeOperand) {
        assert!(BlobInfoV1::merge_new(operand).is_none());
    }

    param_test! {
        test_merge_new_expected_success_cases_invariants: [
            register_deletable_false: (BlobInfoMergeOperand::new_change_for_testing(
                BlobStatusChangeType::Register, false, 42, 314, event_id_for_testing()
            )),
            register_deletable_true: (BlobInfoMergeOperand::new_change_for_testing(
                BlobStatusChangeType::Register, true, 42, 314, event_id_for_testing()
            )),
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
            register_deletable_false: (BlobInfoMergeOperand::new_change_for_testing(
                BlobStatusChangeType::Register, false, 42, 314, event_id_for_testing()
            )),
            register_deletable_true: (BlobInfoMergeOperand::new_change_for_testing(
                BlobStatusChangeType::Register, true, 42, 314, event_id_for_testing()
            )),
            certify_deletable_false: (BlobInfoMergeOperand::new_change_for_testing(
                BlobStatusChangeType::Certify, false, 42, 314, event_id_for_testing()
            )),
            certify_deletable_true: (BlobInfoMergeOperand::new_change_for_testing(
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
                permanent_total: Some(PermanentBlobInfoV1::new_fixed_for_testing(2, 3)),
                ..Default::default()
            }),
            permanent_certified: (ValidBlobInfoV1{
                permanent_total: Some(PermanentBlobInfoV1::new_fixed_for_testing(2, 3)),
                permanent_certified: Some(PermanentBlobInfoV1::new_fixed_for_testing(1,  2)),
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
                permanent_total: Some(PermanentBlobInfoV1::new_fixed_for_testing(2,  3)),
                ..Default::default()
            }),
            permanent_certified: (ValidBlobInfoV1{
                permanent_total: Some(PermanentBlobInfoV1::new_fixed_for_testing(2, 3)),
                permanent_certified: Some(PermanentBlobInfoV1::new_fixed_for_testing(1, 2)),
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
                    BlobStatusChangeType::Register, false, 1, 2, fixed_event_id_for_testing()
                ),
                ValidBlobInfoV1{
                    permanent_total: Some(PermanentBlobInfoV1::new_fixed_for_testing(1, 2)),
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
                    permanent_total: Some(PermanentBlobInfoV1::new_for_testing(2, 4)),
                    permanent_certified: Some(PermanentBlobInfoV1::new_for_testing(1, 4)),
                    ..Default::default()
                },
                BlobInfoMergeOperand::new_change_for_testing(
                    BlobStatusChangeType::Extend, false, 3, 42, fixed_event_id_for_testing()
                ),
                ValidBlobInfoV1{
                    initial_certified_epoch: Some(0),
                    permanent_total: Some(PermanentBlobInfoV1::new_fixed_for_testing(2, 42)),
                    permanent_certified: Some(PermanentBlobInfoV1::new_fixed_for_testing(1, 42)),
                    ..Default::default()
                },
            ),
            // TODO(mlegner): Add some more cases for permanent blobs and deletions (#1006).
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
                    permanent_total: Some(PermanentBlobInfoV1::new_fixed_for_testing(1, 2)),
                    ..Default::default()
                },
                1,
                2,
            ),
            expired_permanent_registered_1: (
                ValidBlobInfoV1 {
                    permanent_total: Some(PermanentBlobInfoV1::new_fixed_for_testing(2, 3)),
                    ..Default::default()
                },
                2,
                4,
            ),
            expired_permanent_certified: (
                ValidBlobInfoV1 {
                    permanent_total: Some(PermanentBlobInfoV1::new_fixed_for_testing(2, 2)),
                    permanent_certified: Some(PermanentBlobInfoV1::new_fixed_for_testing(1, 2)),
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

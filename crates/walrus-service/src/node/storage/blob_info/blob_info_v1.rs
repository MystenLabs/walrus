// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! V1 blob info types and merge logic.

use std::num::NonZeroU32;

use serde::{Deserialize, Serialize};
use sui_types::event::EventID;
use walrus_core::{BlobId, Epoch};
use walrus_storage_node_client::api::{BlobStatus, DeletableCounts};

use super::{
    BlobInfoApi,
    BlobInfoMergeOperand,
    BlobStatusChangeInfo,
    BlobStatusChangeType,
    CertifiedBlobInfoApi,
    Mergeable,
    ToBytes,
};

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
            };
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

    pub(crate) fn has_no_objects(&self) -> bool {
        matches!(
            self,
            Self {
                is_metadata_stored: _,
                count_deletable_total: 0,
                count_deletable_certified: 0,
                permanent_total: None,
                permanent_certified: None,
                initial_certified_epoch: None,
                latest_seen_deletable_registered_end_epoch: None,
                latest_seen_deletable_certified_end_epoch: None,
            }
        )
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
                    self.update_deletable_counters_and_end_epochs(was_certified);
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
                BlobStatusChangeType::Delete { .. } => {
                    tracing::error!("attempt to delete a permanent blob");
                    return;
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
    pub(crate) fn check_invariants(&self) -> anyhow::Result<()> {
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
    pub(crate) fn new_first(end_epoch: Epoch, event: EventID) -> Self {
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
    pub(crate) fn update(&mut self, change_info: &BlobStatusChangeInfo, increase_count: bool) {
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
    pub(crate) fn update_optional(
        existing_info: &mut Option<Self>,
        change_info: &BlobStatusChangeInfo,
    ) {
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
            BlobInfoMergeOperand::MarkMetadataStored(is_metadata_stored) => {
                tracing::info!(
                    is_metadata_stored,
                    "marking metadata stored for an untracked blob ID; blob info will be removed \
                    during the next garbage collection"
                );
                Some(
                    ValidBlobInfoV1 {
                        is_metadata_stored,
                        ..Default::default()
                    }
                    .into(),
                )
            }
            BlobInfoMergeOperand::ChangeStatus { .. }
            | BlobInfoMergeOperand::DeletableExpired { .. }
            | BlobInfoMergeOperand::PermanentExpired { .. } => {
                tracing::error!(
                    ?operand,
                    "encountered an unexpected update for an untracked blob ID"
                );
                debug_assert!(
                    false,
                    "encountered an unexpected update for an untracked blob ID: {operand:?}",
                );
                None
            }
        }
    }
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
}

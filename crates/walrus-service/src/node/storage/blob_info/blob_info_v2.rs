// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! V2 blob info types and merge logic.
//!
//! Initially a copy of V1; pool support and structural changes will be added separately.

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
    blob_info_v1::{BlobInfoV1, PermanentBlobInfoV1, ValidBlobInfoV1},
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum BlobInfoV2 {
    Invalid { epoch: Epoch, event: EventID },
    Valid(ValidBlobInfoV2),
}

/// Converts a V1 valid blob info to V2, keeping all fields identical.
impl From<ValidBlobInfoV1> for ValidBlobInfoV2 {
    fn from(v1: ValidBlobInfoV1) -> Self {
        Self {
            is_metadata_stored: v1.is_metadata_stored,
            count_deletable_total: v1.count_deletable_total,
            count_deletable_certified: v1.count_deletable_certified,
            permanent_total: v1.permanent_total,
            permanent_certified: v1.permanent_certified,
            initial_certified_epoch: v1.initial_certified_epoch,
            latest_seen_deletable_registered_end_epoch: v1
                .latest_seen_deletable_registered_end_epoch,
            latest_seen_deletable_certified_end_epoch: v1.latest_seen_deletable_certified_end_epoch,
        }
    }
}

/// Converts a V1 blob info to V2.
impl From<BlobInfoV1> for BlobInfoV2 {
    fn from(v1: BlobInfoV1) -> Self {
        match v1 {
            BlobInfoV1::Invalid { epoch, event } => BlobInfoV2::Invalid { epoch, event },
            BlobInfoV1::Valid(v) => BlobInfoV2::Valid(v.into()),
        }
    }
}

impl ToBytes for BlobInfoV2 {}

// INV: count_deletable_total >= count_deletable_certified
// INV: permanent_total.is_none() => permanent_certified.is_none()
// INV: permanent_total.count >= permanent_certified.count
// INV: permanent_total.end_epoch >= permanent_certified.end_epoch
// INV: initial_certified_epoch.is_some()
//      <=> count_deletable_certified > 0 || permanent_certified.is_some()
// INV: latest_seen_deletable_registered_end_epoch >= latest_seen_deletable_certified_end_epoch
// See the `check_invariants` method for more details.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub(crate) struct ValidBlobInfoV2 {
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

impl From<ValidBlobInfoV2> for BlobInfoV2 {
    fn from(value: ValidBlobInfoV2) -> Self {
        Self::Valid(value)
    }
}

impl ValidBlobInfoV2 {
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

    #[allow(dead_code)]
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
    #[allow(dead_code)]
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

impl CertifiedBlobInfoApi for BlobInfoV2 {
    fn is_certified(&self, current_epoch: Epoch) -> bool {
        if let Self::Valid(valid_blob_info) = self {
            valid_blob_info.is_certified(current_epoch)
        } else {
            false
        }
    }

    fn initial_certified_epoch(&self) -> Option<Epoch> {
        if let Self::Valid(ValidBlobInfoV2 {
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

impl BlobInfoApi for BlobInfoV2 {
    fn is_metadata_stored(&self) -> bool {
        matches!(
            self,
            Self::Valid(ValidBlobInfoV2 {
                is_metadata_stored: true,
                ..
            })
        )
    }

    // Note: See the `is_certified` method for an explanation of the use of the
    // `latest_seen_deletable_registered_end_epoch` field.
    fn is_registered(&self, current_epoch: Epoch) -> bool {
        let Self::Valid(ValidBlobInfoV2 {
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
            Self::Valid(ValidBlobInfoV2 {
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
            BlobInfoV2::Invalid { event, .. } => BlobStatus::Invalid { event: *event },
            BlobInfoV2::Valid(valid_blob_info) => valid_blob_info.to_blob_status(current_epoch),
        }
    }
}

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
                    ValidBlobInfoV2 {
                        count_deletable_total: 1,
                        latest_seen_deletable_registered_end_epoch: Some(end_epoch),
                        ..Default::default()
                    }
                } else {
                    ValidBlobInfoV2 {
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
            } => Some(BlobInfoV2::Invalid {
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
                    ValidBlobInfoV2 {
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

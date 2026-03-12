// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! V2 blob info types and merge logic, supporting both regular blobs and storage pool blobs.

use serde::{Deserialize, Serialize};
use sui_types::{base_types::ObjectID, event::EventID};
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
    per_object_blob_info::{PerObjectBlobInfoApi, PerObjectBlobInfoMergeOperand},
};

/// V2 aggregate blob info that supports both regular blobs and storage pool blobs.
///
/// Regular blob fields are almost identical to V1, except that we no longer track
/// highest seen end epochs for deletable blobs. Highest seen end epoch is an optimization to give
/// better hint about whether a deletable blob may be alive or not. However, it may not give
/// accurate answer given ongoing GC. So we removed it from V2 and rely on GC to keep blob
/// epoch status up to date.
///
/// Storage pool blobs are tracked via flat counters, similar to deletable blobs.
//
// INV: same invariants as V1 for regular fields, plus:
// INV: count_pooled_refs_total >= count_pooled_refs_certified
// INV: initial_certified_epoch considers both regular AND pool certified counts
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub(crate) struct ValidBlobInfoV2 {
    // Common fields for both regular and storage pool blobs.
    pub is_metadata_stored: bool,
    pub initial_certified_epoch: Option<Epoch>,

    // Regular blob fields (same as V1).
    pub count_deletable_total: u32,
    pub count_deletable_certified: u32,
    pub permanent_total: Option<PermanentBlobInfoV1>,
    pub permanent_certified: Option<PermanentBlobInfoV1>,

    // Storage pool references counters.
    pub count_pooled_refs_total: u32,
    pub count_pooled_refs_certified: u32,
}

// Conversion from V1 to V2. This is needed because even when storage pool is supported, we still
// create V1 blob info if the blob is a regular blob. Only after seen a pooled blob event, we then
// upgrade to V2. This makes all the storage nodes to have consistent behavior despite of when
// they upgrade their nodes.
impl From<ValidBlobInfoV1> for ValidBlobInfoV2 {
    fn from(v1: ValidBlobInfoV1) -> Self {
        Self {
            is_metadata_stored: v1.is_metadata_stored,
            count_deletable_total: v1.count_deletable_total,
            count_deletable_certified: v1.count_deletable_certified,
            permanent_total: v1.permanent_total,
            permanent_certified: v1.permanent_certified,
            initial_certified_epoch: v1.initial_certified_epoch,
            count_pooled_refs_total: 0,
            count_pooled_refs_certified: 0,
        }
    }
}

impl ValidBlobInfoV2 {
    fn to_blob_status(&self, current_epoch: Epoch) -> BlobStatus {
        let initial_certified_epoch = self.initial_certified_epoch;

        // Deletable counts include both regular and pool refs.
        let deletable_counts = DeletableCounts {
            count_deletable_total: self
                .count_deletable_total
                .saturating_add(self.count_pooled_refs_total),
            count_deletable_certified: self
                .count_deletable_certified
                .saturating_add(self.count_pooled_refs_certified),
        };

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

    // The counts of deletable and pooled blobs are decreased when they expire. However, because
    // this only happens in the background, it is possible during a short time period at the
    // beginning of an epoch that all deletable and pooled blobs expired but the counts are not
    // updated yet.
    //
    // Given that GC per object blob info table is expected to be very fast (in seconds), this
    // window is expected to be very short.
    fn is_certified(&self, current_epoch: Epoch) -> bool {
        let exists_certified_permanent_blob = self
            .permanent_certified
            .as_ref()
            .is_some_and(|p| p.end_epoch > current_epoch);

        // Note that at the beginning of the epoch before GC runs, newly expired deletable blob or
        // pooled blob's certified counter may still be non-zero, but the blob is already expired.
        // This will make the blob appears to be certified until GC finishes.
        self.initial_certified_epoch
            .is_some_and(|epoch| epoch <= current_epoch)
            && (exists_certified_permanent_blob
                || self.count_deletable_certified > 0
                || self.count_pooled_refs_certified > 0)
    }

    pub(crate) fn has_no_objects(&self) -> bool {
        self.count_deletable_total == 0
            && self.count_deletable_certified == 0
            && self.permanent_total.is_none()
            && self.permanent_certified.is_none()
            && self.initial_certified_epoch.is_none()
            && self.count_pooled_refs_total == 0
    }

    /// Handles regular blob status changes (same logic as V1).
    #[tracing::instrument]
    fn update_status(
        &mut self,
        change_type: BlobStatusChangeType,
        change_info: BlobStatusChangeInfo,
    ) {
        let was_certified = self.is_certified(change_info.epoch);
        // This should be the same as V1, except that we no longer track end epochs for deletable
        // blobs.
        if change_info.deletable {
            match change_type {
                BlobStatusChangeType::Register => {
                    self.count_deletable_total += 1;
                }
                BlobStatusChangeType::Certify => {
                    if self.count_deletable_total <= self.count_deletable_certified {
                        tracing::error!(
                            "attempt to certify a deletable blob before corresponding register"
                        );
                        return;
                    }
                    self.count_deletable_certified += 1;
                }
                BlobStatusChangeType::Extend => {
                    // No-op for V2 deletable blobs (no end epoch tracking).
                }
                BlobStatusChangeType::Delete { was_certified } => {
                    self.update_deletable_counters(was_certified);
                }
            }
        } else {
            // These should be the same as V1.
            match change_type {
                BlobStatusChangeType::Register => {
                    ValidBlobInfoV1::register_permanent(&mut self.permanent_total, &change_info);
                }
                BlobStatusChangeType::Certify => {
                    if !ValidBlobInfoV1::certify_permanent(
                        &self.permanent_total,
                        &mut self.permanent_certified,
                        &change_info,
                    ) {
                        // Return early to prevent updating the `initial_certified_epoch` below.
                        return;
                    }
                }
                BlobStatusChangeType::Extend => {
                    ValidBlobInfoV1::extend_permanent(&mut self.permanent_total, &change_info);
                    ValidBlobInfoV1::extend_permanent(&mut self.permanent_certified, &change_info);
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
        self.update_deletable_counters(was_certified);
        self.maybe_unset_initial_certified_epoch();
    }

    fn update_deletable_counters(&mut self, was_certified: bool) {
        self.count_deletable_total = self.count_deletable_total.saturating_sub(1);
        if was_certified {
            self.count_deletable_certified = self.count_deletable_certified.saturating_sub(1);
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
        if self.count_pooled_refs_certified == 0
            && self.count_deletable_certified == 0
            && self.permanent_certified.is_none()
        {
            self.initial_certified_epoch = None;
        }
    }

    fn permanent_expired(&mut self, was_certified: bool) {
        ValidBlobInfoV1::decrement_blob_info_inner(&mut self.permanent_total);
        if was_certified {
            ValidBlobInfoV1::decrement_blob_info_inner(&mut self.permanent_certified);
        }
        self.maybe_unset_initial_certified_epoch();
    }

    /// Checks the invariants of this V2 blob info.
    pub(crate) fn check_invariants(&self) -> anyhow::Result<()> {
        let has_regular_certified =
            self.count_deletable_certified > 0 || self.permanent_certified.is_some();
        let has_pool_certified = self.count_pooled_refs_certified > 0;

        anyhow::ensure!(self.count_deletable_total >= self.count_deletable_certified);
        match self.initial_certified_epoch {
            None => {
                anyhow::ensure!(!has_regular_certified && !has_pool_certified)
            }
            Some(_) => {
                anyhow::ensure!(has_regular_certified || has_pool_certified)
            }
        }

        match (&self.permanent_total, &self.permanent_certified) {
            (None, Some(_)) => {
                anyhow::bail!("permanent_total.is_none() => permanent_certified.is_none()")
            }
            (Some(total_inner), Some(certified_inner)) => {
                anyhow::ensure!(total_inner.end_epoch >= certified_inner.end_epoch);
                anyhow::ensure!(total_inner.count >= certified_inner.count);
            }
            _ => (),
        }

        anyhow::ensure!(
            self.count_pooled_refs_total >= self.count_pooled_refs_certified,
            "pool ref count_pooled_refs_total < count_pooled_refs_certified"
        );

        Ok(())
    }

    // --- Storage pool operations ---

    /// Registers a blob in a storage pool.
    fn pool_register(&mut self) {
        self.count_pooled_refs_total += 1;
    }

    /// Certifies a blob in a storage pool.
    fn pool_certify(&mut self, epoch: Epoch) {
        let was_certified = self.is_certified(epoch);

        if self.count_pooled_refs_total <= self.count_pooled_refs_certified {
            tracing::error!("attempt to certify a pool blob before corresponding register");
            return;
        }
        self.count_pooled_refs_certified += 1;
        self.update_initial_certified_epoch(epoch, !was_certified);
    }

    /// Deletes a blob from a storage pool.
    fn pool_delete(&mut self, was_certified: bool) {
        if self.count_pooled_refs_total == 0 {
            tracing::error!("attempt to delete a pool blob that is not registered");
            return;
        }
        self.count_pooled_refs_total = self.count_pooled_refs_total.saturating_sub(1);
        if was_certified {
            self.count_pooled_refs_certified = self.count_pooled_refs_certified.saturating_sub(1);
        }
        self.maybe_unset_initial_certified_epoch();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum BlobInfoV2 {
    Invalid { epoch: Epoch, event: EventID },
    Valid(ValidBlobInfoV2),
}

impl ToBytes for BlobInfoV2 {}

impl From<BlobInfoV1> for BlobInfoV2 {
    fn from(v1: BlobInfoV1) -> Self {
        match v1 {
            BlobInfoV1::Invalid { epoch, event } => BlobInfoV2::Invalid { epoch, event },
            BlobInfoV1::Valid(v) => BlobInfoV2::Valid(v.into()),
        }
    }
}

impl CertifiedBlobInfoApi for BlobInfoV2 {
    fn is_certified(&self, current_epoch: Epoch) -> bool {
        match self {
            Self::Valid(v) => v.is_certified(current_epoch),
            Self::Invalid { .. } => false,
        }
    }

    fn initial_certified_epoch(&self) -> Option<Epoch> {
        match self {
            Self::Valid(v) => v.initial_certified_epoch,
            Self::Invalid { .. } => None,
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

    fn is_registered(&self, _current_epoch: Epoch) -> bool {
        let Self::Valid(v) = self else {
            return false;
        };

        let exists_registered_permanent_blob = v
            .permanent_total
            .as_ref()
            .is_some_and(|p| p.end_epoch > _current_epoch);

        // Note that at the beginning of the epoch before GC runs, newly expired deletable blob or
        // pooled blob's registered counter may still be non-zero, but the blob is already expired.
        // This will make the blob appears to be registered until GC finishes.
        exists_registered_permanent_blob
            || v.count_deletable_total > 0
            || v.count_pooled_refs_total > 0
    }

    fn can_blob_info_be_deleted(&self, current_epoch: Epoch) -> bool {
        match self {
            Self::Invalid { .. } => false,
            Self::Valid(v) => {
                v.count_deletable_total == 0
                    && v.permanent_total.is_none()
                    && v.count_pooled_refs_total == 0
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
            // Regular blob status changes.
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
            // Storage pool operations.
            (
                Self::Valid(valid_blob_info),
                BlobInfoMergeOperand::PooledBlobChangeStatus {
                    change_type,
                    change_info,
                    ..
                },
            ) => match change_type {
                BlobStatusChangeType::Register => {
                    valid_blob_info.pool_register();
                }
                BlobStatusChangeType::Certify => {
                    valid_blob_info.pool_certify(change_info.epoch);
                }
                BlobStatusChangeType::Extend => {
                    // Extensions don't change ref counts for storage pool.
                    // The pool's end_epoch is tracked separately.
                }
                BlobStatusChangeType::Delete { was_certified } => {
                    valid_blob_info.pool_delete(was_certified);
                }
            },
            (
                Self::Valid(valid_blob_info),
                BlobInfoMergeOperand::PoolExpired { was_certified, .. },
            ) => valid_blob_info.pool_delete(was_certified),
        }
        self
    }

    fn merge_new(operand: Self::MergeOperand) -> Option<Self> {
        match operand {
            BlobInfoMergeOperand::PooledBlobChangeStatus {
                change_type: BlobStatusChangeType::Register,
                ..
            } => {
                let mut v2 = ValidBlobInfoV2::default();
                v2.pool_register();
                Some(BlobInfoV2::Valid(v2))
            }
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
                Some(BlobInfoV2::Valid(ValidBlobInfoV2 {
                    is_metadata_stored,
                    ..Default::default()
                }))
            }
            BlobInfoMergeOperand::ChangeStatus {
                change_type: BlobStatusChangeType::Register,
                change_info:
                    BlobStatusChangeInfo {
                        deletable,
                        end_epoch,
                        status_event,
                        ..
                    },
            } => Some(BlobInfoV2::Valid(if deletable {
                ValidBlobInfoV2 {
                    count_deletable_total: 1,
                    ..Default::default()
                }
            } else {
                ValidBlobInfoV2 {
                    permanent_total: Some(PermanentBlobInfoV1::new_first(end_epoch, status_event)),
                    ..Default::default()
                }
            })),
            _ => {
                tracing::error!(
                    ?operand,
                    "encountered an unexpected update for an untracked blob ID (V2)"
                );
                debug_assert!(
                    false,
                    "encountered an unexpected update for an untracked blob ID (V2): {operand:?}",
                );
                None
            }
        }
    }
}

// =============================================================================
// Per-object blob info V2
// =============================================================================

/// How the end epoch of a per-object blob is determined.
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Clone)]
pub(crate) enum EndEpochInfo {
    /// Regular blob with an individual end epoch.
    Individual(Epoch),
    /// Pooled blob whose lifetime is determined by the storage pool's end epoch.
    StoragePool(ObjectID),
}

/// V2 per-object blob info that supports both regular blobs and storage pool blobs.
///
/// Unlike V1, the end epoch and storage pool ID are combined into a single `EndEpochInfo`
/// enum: regular blobs have `EndEpochInfo::Individual(end_epoch)`, while pool blobs have
/// `EndEpochInfo::StoragePool(pool_id)`.
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Clone)]
pub(crate) struct PerObjectBlobInfoV2 {
    /// The blob ID.
    pub blob_id: BlobId,
    /// The epoch in which the blob has been registered.
    pub registered_epoch: Epoch,
    /// The epoch in which the blob was first certified, `None` if the blob is uncertified.
    pub certified_epoch: Option<Epoch>,
    /// How the blob's end epoch is determined.
    pub end_epoch_info: EndEpochInfo,
    /// Whether the blob is deletable.
    pub deletable: bool,
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

    fn is_registered(&self, current_epoch: Epoch) -> bool {
        if self.deleted {
            return false;
        }
        match self.end_epoch_info {
            EndEpochInfo::Individual(end_epoch) => end_epoch > current_epoch,
            // Pool blob liveness depends on the pool, not the blob.
            EndEpochInfo::StoragePool(_) => true,
        }
    }

    fn is_deleted(&self) -> bool {
        self.deleted
    }

    fn storage_pool_id(&self) -> Option<ObjectID> {
        match &self.end_epoch_info {
            EndEpochInfo::StoragePool(id) => Some(*id),
            EndEpochInfo::Individual(_) => None,
        }
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
                self.end_epoch_info = EndEpochInfo::Individual(change_info.end_epoch);
            }
            BlobStatusChangeType::Delete { was_certified } => {
                assert_eq!(self.certified_epoch.is_some(), was_certified);
                self.deleted = true;
            }
        }
        self
    }

    fn merge_new(operand: Self::MergeOperand) -> Option<Self> {
        // V2 entries are created via direct insert, not merge_new.
        tracing::error!(
            ?operand,
            "PerObjectBlobInfoV2::merge_new should not be called; V2 entries are created via \
            direct insert"
        );
        debug_assert!(
            false,
            "PerObjectBlobInfoV2::merge_new should not be called: {operand:?}"
        );
        None
    }
}

#[cfg(test)]
mod tests {
    use sui_types::base_types::ObjectID;
    use walrus_sui::test_utils::{event_id_for_testing, fixed_event_id_for_testing};
    use walrus_test_utils::param_test;

    use super::{EndEpochInfo, PerObjectBlobInfoV2, *};
    use crate::node::storage::blob_info::{
        BlobInfo,
        PooledBlobChangeInfo,
        per_object_blob_info::PerObjectBlobInfoMergeOperand,
    };

    fn pool_id() -> ObjectID {
        walrus_sui::test_utils::object_id_for_testing()
    }

    /// Shorthand: build a pool register/certify/delete operand.
    fn pool_op(change_type: BlobStatusChangeType, epoch: Epoch) -> BlobInfoMergeOperand {
        BlobInfoMergeOperand::PooledBlobChangeStatus {
            change_type,
            change_info: PooledBlobChangeInfo {
                epoch,
                storage_pool_id: pool_id(),
            },
        }
    }

    fn check_v2_invariants(info: &BlobInfoV2) {
        if let BlobInfoV2::Valid(v) = info {
            v.check_invariants()
                .expect("V2 blob info invariants violated")
        }
    }

    // --- V2 merge_new success cases ---

    param_test! {
        test_v2_merge_new_expected_success_cases_invariants: [
            register_permanent: (BlobInfoMergeOperand::new_change_for_testing(
                BlobStatusChangeType::Register, false, 42, 314, event_id_for_testing()
            )),
            register_deletable: (BlobInfoMergeOperand::new_change_for_testing(
                BlobStatusChangeType::Register, true, 42, 314, event_id_for_testing()
            )),
            invalidate: (BlobInfoMergeOperand::MarkInvalid {
                epoch: 0, status_event: event_id_for_testing()
            }),
            metadata_true: (BlobInfoMergeOperand::MarkMetadataStored(true)),
            metadata_false: (BlobInfoMergeOperand::MarkMetadataStored(false)),
            pool_register: (pool_op(BlobStatusChangeType::Register, 1)),
        ]
    }
    fn test_v2_merge_new_expected_success_cases_invariants(operand: BlobInfoMergeOperand) {
        let blob_info = BlobInfoV2::merge_new(operand).expect("should be some");
        check_v2_invariants(&blob_info);
    }

    // --- V2 mark metadata stored keeps everything else unchanged ---

    param_test! {
        test_v2_mark_metadata_stored_keeps_everything_else_unchanged: [
            default: (ValidBlobInfoV2::default()),
            deletable: (ValidBlobInfoV2 {
                count_deletable_total: 2, ..Default::default()
            }),
            deletable_certified: (ValidBlobInfoV2 {
                count_deletable_total: 2, count_deletable_certified: 1,
                initial_certified_epoch: Some(0), ..Default::default()
            }),
            permanent_certified: (ValidBlobInfoV2 {
                permanent_total: Some(PermanentBlobInfoV1::new_fixed_for_testing(2, 3, 0)),
                permanent_certified: Some(PermanentBlobInfoV1::new_fixed_for_testing(1, 2, 0)),
                initial_certified_epoch: Some(1), ..Default::default()
            }),
            pool_refs: (ValidBlobInfoV2 {
                count_pooled_refs_total: 3, count_pooled_refs_certified: 1,
                initial_certified_epoch: Some(2), ..Default::default()
            }),
            mixed_regular_and_pool: (ValidBlobInfoV2 {
                count_deletable_total: 1, count_deletable_certified: 1,
                count_pooled_refs_total: 2, count_pooled_refs_certified: 1,
                initial_certified_epoch: Some(1), ..Default::default()
            }),
        ]
    }
    fn test_v2_mark_metadata_stored_keeps_everything_else_unchanged(
        preexisting_info: ValidBlobInfoV2,
    ) {
        preexisting_info
            .check_invariants()
            .expect("preexisting invariants violated");
        let expected = ValidBlobInfoV2 {
            is_metadata_stored: true,
            ..preexisting_info.clone()
        };
        expected
            .check_invariants()
            .expect("expected invariants violated");

        let updated = BlobInfoV2::Valid(preexisting_info)
            .merge_with(BlobInfoMergeOperand::MarkMetadataStored(true));
        assert_eq!(updated, BlobInfoV2::Valid(expected));
    }

    // --- V2 mark invalid marks everything invalid ---

    param_test! {
        test_v2_mark_invalid_marks_everything_invalid: [
            default: (ValidBlobInfoV2::default()),
            deletable: (ValidBlobInfoV2 {
                count_deletable_total: 2, ..Default::default()
            }),
            deletable_certified: (ValidBlobInfoV2 {
                count_deletable_total: 2, count_deletable_certified: 1,
                initial_certified_epoch: Some(0), ..Default::default()
            }),
            permanent_certified: (ValidBlobInfoV2 {
                permanent_total: Some(PermanentBlobInfoV1::new_fixed_for_testing(2, 3, 0)),
                permanent_certified: Some(PermanentBlobInfoV1::new_fixed_for_testing(1, 2, 0)),
                initial_certified_epoch: Some(1), ..Default::default()
            }),
            pool_refs: (ValidBlobInfoV2 {
                count_pooled_refs_total: 3, count_pooled_refs_certified: 1,
                initial_certified_epoch: Some(2), ..Default::default()
            }),
            mixed_regular_and_pool: (ValidBlobInfoV2 {
                count_deletable_total: 1, count_deletable_certified: 1,
                count_pooled_refs_total: 2, count_pooled_refs_certified: 1,
                initial_certified_epoch: Some(1), ..Default::default()
            }),
        ]
    }
    fn test_v2_mark_invalid_marks_everything_invalid(preexisting_info: ValidBlobInfoV2) {
        preexisting_info
            .check_invariants()
            .expect("preexisting invariants violated");
        let event = event_id_for_testing();
        let updated =
            BlobInfoV2::Valid(preexisting_info).merge_with(BlobInfoMergeOperand::MarkInvalid {
                epoch: 2,
                status_event: event,
            });
        assert_eq!(BlobInfoV2::Invalid { epoch: 2, event }, updated);
    }

    // --- V2 merge preexisting expected successes ---
    // Covers regular ops (deletable, permanent, expire) and pool ops on ValidBlobInfoV2.
    // V2 does not track latest_seen_deletable_*_end_epoch, so those cases are omitted.

    param_test! {
        test_v2_merge_preexisting_expected_successes: [
            // --- regular deletable ---
            register_first_deletable: (
                ValidBlobInfoV2::default(),
                BlobInfoMergeOperand::new_change_for_testing(
                    BlobStatusChangeType::Register, true, 1, 2, event_id_for_testing()
                ),
                ValidBlobInfoV2 { count_deletable_total: 1, ..Default::default() },
            ),
            register_additional_deletable: (
                ValidBlobInfoV2 { count_deletable_total: 3, ..Default::default() },
                BlobInfoMergeOperand::new_change_for_testing(
                    BlobStatusChangeType::Register, true, 1, 5, event_id_for_testing()
                ),
                ValidBlobInfoV2 { count_deletable_total: 4, ..Default::default() },
            ),
            certify_first_deletable: (
                ValidBlobInfoV2 { count_deletable_total: 3, ..Default::default() },
                BlobInfoMergeOperand::new_change_for_testing(
                    BlobStatusChangeType::Certify, true, 1, 4, event_id_for_testing()
                ),
                ValidBlobInfoV2 {
                    count_deletable_total: 3, count_deletable_certified: 1,
                    initial_certified_epoch: Some(1), ..Default::default()
                },
            ),
            certify_additional_deletable_keeps_earlier_epoch: (
                ValidBlobInfoV2 {
                    count_deletable_total: 3, count_deletable_certified: 1,
                    initial_certified_epoch: Some(0), ..Default::default()
                },
                BlobInfoMergeOperand::new_change_for_testing(
                    BlobStatusChangeType::Certify, true, 1, 2, event_id_for_testing()
                ),
                ValidBlobInfoV2 {
                    count_deletable_total: 3, count_deletable_certified: 2,
                    initial_certified_epoch: Some(0), ..Default::default()
                },
            ),
            delete_deletable_blob: (
                ValidBlobInfoV2 {
                    count_deletable_total: 3, count_deletable_certified: 2,
                    initial_certified_epoch: Some(1), ..Default::default()
                },
                BlobInfoMergeOperand::new_change_for_testing(
                    BlobStatusChangeType::Delete { was_certified: true }, true, 1, 6,
                    event_id_for_testing(),
                ),
                ValidBlobInfoV2 {
                    count_deletable_total: 2, count_deletable_certified: 1,
                    initial_certified_epoch: Some(1), ..Default::default()
                },
            ),
            delete_last_certified_deletable_clears_epoch: (
                ValidBlobInfoV2 {
                    count_deletable_total: 2, count_deletable_certified: 1,
                    initial_certified_epoch: Some(1), ..Default::default()
                },
                BlobInfoMergeOperand::new_change_for_testing(
                    BlobStatusChangeType::Delete { was_certified: true }, true, 1, 4,
                    event_id_for_testing(),
                ),
                ValidBlobInfoV2 {
                    count_deletable_total: 1, count_deletable_certified: 0,
                    initial_certified_epoch: None, ..Default::default()
                },
            ),
            expire_deletable_blob: (
                ValidBlobInfoV2 {
                    count_deletable_total: 3, count_deletable_certified: 2,
                    initial_certified_epoch: Some(1), ..Default::default()
                },
                BlobInfoMergeOperand::DeletableExpired { was_certified: true },
                ValidBlobInfoV2 {
                    count_deletable_total: 2, count_deletable_certified: 1,
                    initial_certified_epoch: Some(1), ..Default::default()
                },
            ),
            expire_last_certified_deletable_clears_epoch: (
                ValidBlobInfoV2 {
                    count_deletable_total: 2, count_deletable_certified: 1,
                    initial_certified_epoch: Some(1), ..Default::default()
                },
                BlobInfoMergeOperand::DeletableExpired { was_certified: true },
                ValidBlobInfoV2 {
                    count_deletable_total: 1, count_deletable_certified: 0,
                    initial_certified_epoch: None, ..Default::default()
                },
            ),
            // --- regular permanent ---
            register_first_permanent: (
                ValidBlobInfoV2::default(),
                BlobInfoMergeOperand::new_change_for_testing(
                    BlobStatusChangeType::Register, false, 1, 2,
                    fixed_event_id_for_testing(0)
                ),
                ValidBlobInfoV2 {
                    permanent_total: Some(PermanentBlobInfoV1::new_fixed_for_testing(1, 2, 0)),
                    ..Default::default()
                },
            ),
            register_additional_permanent: (
                ValidBlobInfoV2 {
                    permanent_total: Some(PermanentBlobInfoV1::new_fixed_for_testing(1, 2, 0)),
                    ..Default::default()
                },
                BlobInfoMergeOperand::new_change_for_testing(
                    BlobStatusChangeType::Register, false, 2, 3,
                    fixed_event_id_for_testing(1)
                ),
                ValidBlobInfoV2 {
                    permanent_total: Some(PermanentBlobInfoV1::new_fixed_for_testing(2, 3, 1)),
                    ..Default::default()
                },
            ),
            extend_permanent: (
                ValidBlobInfoV2 {
                    initial_certified_epoch: Some(0),
                    permanent_total: Some(PermanentBlobInfoV1::new_fixed_for_testing(2, 4, 0)),
                    permanent_certified: Some(PermanentBlobInfoV1::new_fixed_for_testing(1, 4, 1)),
                    ..Default::default()
                },
                BlobInfoMergeOperand::new_change_for_testing(
                    BlobStatusChangeType::Extend, false, 3, 42,
                    fixed_event_id_for_testing(2)
                ),
                ValidBlobInfoV2 {
                    initial_certified_epoch: Some(0),
                    permanent_total: Some(PermanentBlobInfoV1::new_fixed_for_testing(2, 42, 2)),
                    permanent_certified: Some(PermanentBlobInfoV1::new_fixed_for_testing(1, 42, 2)),
                    ..Default::default()
                },
            ),
            expire_permanent_blob: (
                ValidBlobInfoV2 {
                    permanent_total: Some(PermanentBlobInfoV1::new_fixed_for_testing(3, 5, 0)),
                    permanent_certified: Some(PermanentBlobInfoV1::new_fixed_for_testing(2, 5, 1)),
                    initial_certified_epoch: Some(1), ..Default::default()
                },
                BlobInfoMergeOperand::PermanentExpired { was_certified: true },
                ValidBlobInfoV2 {
                    permanent_total: Some(PermanentBlobInfoV1::new_fixed_for_testing(2, 5, 0)),
                    permanent_certified: Some(PermanentBlobInfoV1::new_fixed_for_testing(1, 5, 1)),
                    initial_certified_epoch: Some(1), ..Default::default()
                },
            ),
            expire_last_certified_permanent_clears_epoch: (
                ValidBlobInfoV2 {
                    permanent_total: Some(PermanentBlobInfoV1::new_fixed_for_testing(2, 5, 0)),
                    permanent_certified: Some(PermanentBlobInfoV1::new_fixed_for_testing(1, 5, 1)),
                    initial_certified_epoch: Some(1), ..Default::default()
                },
                BlobInfoMergeOperand::PermanentExpired { was_certified: true },
                ValidBlobInfoV2 {
                    permanent_total: Some(PermanentBlobInfoV1::new_fixed_for_testing(1, 5, 0)),
                    permanent_certified: None,
                    initial_certified_epoch: None, ..Default::default()
                },
            ),
            // --- pool operations ---
            pool_register: (
                ValidBlobInfoV2::default(),
                pool_op(BlobStatusChangeType::Register, 1),
                ValidBlobInfoV2 { count_pooled_refs_total: 1, ..Default::default() },
            ),
            pool_certify: (
                ValidBlobInfoV2 { count_pooled_refs_total: 1, ..Default::default() },
                pool_op(BlobStatusChangeType::Certify, 3),
                ValidBlobInfoV2 {
                    count_pooled_refs_total: 1, count_pooled_refs_certified: 1,
                    initial_certified_epoch: Some(3), ..Default::default()
                },
            ),
            pool_delete_certified: (
                ValidBlobInfoV2 {
                    count_pooled_refs_total: 2, count_pooled_refs_certified: 2,
                    initial_certified_epoch: Some(1), ..Default::default()
                },
                pool_op(BlobStatusChangeType::Delete { was_certified: true }, 5),
                ValidBlobInfoV2 {
                    count_pooled_refs_total: 1, count_pooled_refs_certified: 1,
                    initial_certified_epoch: Some(1), ..Default::default()
                },
            ),
            pool_delete_last_certified_clears_epoch: (
                ValidBlobInfoV2 {
                    count_pooled_refs_total: 2, count_pooled_refs_certified: 1,
                    initial_certified_epoch: Some(1), ..Default::default()
                },
                pool_op(BlobStatusChangeType::Delete { was_certified: true }, 5),
                ValidBlobInfoV2 {
                    count_pooled_refs_total: 1, count_pooled_refs_certified: 0,
                    initial_certified_epoch: None, ..Default::default()
                },
            ),
            pool_expired: (
                ValidBlobInfoV2 {
                    count_pooled_refs_total: 2, count_pooled_refs_certified: 1,
                    initial_certified_epoch: Some(1), ..Default::default()
                },
                BlobInfoMergeOperand::PoolExpired {
                    storage_pool_id: pool_id(), was_certified: true,
                },
                ValidBlobInfoV2 {
                    count_pooled_refs_total: 1, count_pooled_refs_certified: 0,
                    initial_certified_epoch: None, ..Default::default()
                },
            ),
            // --- mixed: pool cert keeps epoch alive after regular deletion ---
            mixed_regular_delete_pool_cert_keeps_epoch: (
                ValidBlobInfoV2 {
                    count_deletable_total: 1, count_deletable_certified: 1,
                    count_pooled_refs_total: 1, count_pooled_refs_certified: 1,
                    initial_certified_epoch: Some(3), ..Default::default()
                },
                BlobInfoMergeOperand::DeletableExpired { was_certified: true },
                ValidBlobInfoV2 {
                    count_deletable_total: 0, count_deletable_certified: 0,
                    count_pooled_refs_total: 1, count_pooled_refs_certified: 1,
                    initial_certified_epoch: Some(3), ..Default::default()
                },
            ),
        ]
    }
    fn test_v2_merge_preexisting_expected_successes(
        preexisting: ValidBlobInfoV2,
        operand: BlobInfoMergeOperand,
        expected: ValidBlobInfoV2,
    ) {
        preexisting
            .check_invariants()
            .expect("preexisting invariants violated");
        expected
            .check_invariants()
            .expect("expected invariants violated");
        let updated = BlobInfoV2::Valid(preexisting).merge_with(operand);
        assert_eq!(updated, BlobInfoV2::Valid(expected));
    }

    // --- V1->V2 conversion preserves fields and zeroes pool counters ---
    #[test]
    fn v1_to_v2_conversion() {
        let v1 = ValidBlobInfoV1 {
            is_metadata_stored: true,
            count_deletable_total: 3,
            count_deletable_certified: 1,
            permanent_total: Some(PermanentBlobInfoV1::new_fixed_for_testing(2, 10, 0)),
            permanent_certified: Some(PermanentBlobInfoV1::new_fixed_for_testing(1, 10, 1)),
            initial_certified_epoch: Some(5),
            latest_seen_deletable_registered_end_epoch: Some(8),
            latest_seen_deletable_certified_end_epoch: Some(7),
        };
        let v2: ValidBlobInfoV2 = v1.clone().into();
        assert_eq!(v2.count_deletable_total, v1.count_deletable_total);
        assert_eq!(v2.permanent_total, v1.permanent_total);
        assert_eq!(v2.initial_certified_epoch, v1.initial_certified_epoch);
        assert_eq!(
            (v2.count_pooled_refs_total, v2.count_pooled_refs_certified),
            (0, 0)
        );
        v2.check_invariants()
            .expect("v2 blob info invariants violated");

        // Invalid converts too.
        let inv = BlobInfoV1::Invalid {
            epoch: 5,
            event: event_id_for_testing(),
        };
        assert!(matches!(
            BlobInfoV2::from(inv),
            BlobInfoV2::Invalid { epoch: 5, .. }
        ));
    }
    // --- Invalid state ---

    param_test! {
        test_v2_invalid_status_is_not_changed: [
            invalidate: (BlobInfoMergeOperand::MarkInvalid {
                epoch: 0, status_event: event_id_for_testing()
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
                false, 42, 314, event_id_for_testing(),
            )),
            delete_false: (BlobInfoMergeOperand::new_change_for_testing(
                BlobStatusChangeType::Delete { was_certified: false },
                false, 42, 314, event_id_for_testing(),
            )),
            pool_register: (pool_op(BlobStatusChangeType::Register, 1)),
            pool_certify: (pool_op(BlobStatusChangeType::Certify, 1)),
            pool_delete: (pool_op(BlobStatusChangeType::Delete { was_certified: true }, 1)),
            pool_expired: (BlobInfoMergeOperand::PoolExpired {
                storage_pool_id: pool_id(), was_certified: false,
            }),
        ]
    }
    fn test_v2_invalid_status_is_not_changed(operand: BlobInfoMergeOperand) {
        let blob_info = BlobInfoV2::Invalid {
            epoch: 42,
            event: event_id_for_testing(),
        };
        assert_eq!(blob_info, blob_info.clone().merge_with(operand));
    }

    // --- BlobInfo enum: V1->V2 upgrade routing ---
    #[test]
    fn blob_info_upgrades_v1_to_v2_on_pool_operand() {
        let v1 = BlobInfo::V1(BlobInfoV1::Valid(ValidBlobInfoV1 {
            count_deletable_total: 2,
            count_deletable_certified: 1,
            initial_certified_epoch: Some(3),
            latest_seen_deletable_registered_end_epoch: Some(10),
            latest_seen_deletable_certified_end_epoch: Some(10),
            ..Default::default()
        }));
        let result = v1.merge_with(pool_op(BlobStatusChangeType::Register, 1));
        let BlobInfo::V2(BlobInfoV2::Valid(v)) = &result else {
            panic!("expected V2, got {result:?}")
        };
        assert_eq!(v.count_deletable_total, 2);
        assert_eq!(v.count_pooled_refs_total, 1);
        v.check_invariants().unwrap();
    }

    #[test]
    fn blob_info_stays_v1_for_regular_operand() {
        let v1 = BlobInfo::V1(BlobInfoV1::Valid(Default::default()));
        let result = v1.merge_with(BlobInfoMergeOperand::new_change_for_testing(
            BlobStatusChangeType::Register,
            true,
            1,
            10,
            event_id_for_testing(),
        ));
        assert!(matches!(result, BlobInfo::V1(_)));
    }

    #[test]
    fn blob_info_merge_new_creates_v2_for_pool_v1_for_regular() {
        let pool = BlobInfo::merge_new(pool_op(BlobStatusChangeType::Register, 1)).unwrap();
        assert!(matches!(pool, BlobInfo::V2(_)));

        let regular = BlobInfo::merge_new(BlobInfoMergeOperand::new_change_for_testing(
            BlobStatusChangeType::Register,
            true,
            1,
            10,
            event_id_for_testing(),
        ))
        .unwrap();
        assert!(matches!(regular, BlobInfo::V1(_)));
    }

    // --- CertifiedBlobInfoApi on V2 ---
    param_test! {
        test_v2_is_certified: [
            before_certified_epoch: (
                ValidBlobInfoV2 {
                    count_pooled_refs_total: 1, count_pooled_refs_certified: 1,
                    initial_certified_epoch: Some(5), ..Default::default()
                }, 4, false,
            ),
            at_certified_epoch: (
                ValidBlobInfoV2 {
                    count_pooled_refs_total: 1, count_pooled_refs_certified: 1,
                    initial_certified_epoch: Some(5), ..Default::default()
                }, 5, true,
            ),
            no_certified_refs: (
                ValidBlobInfoV2 {
                    count_pooled_refs_total: 1, ..Default::default()
                }, 0, false,
            ),
            regular_deletable_certified: (
                ValidBlobInfoV2 {
                    count_deletable_total: 1, count_deletable_certified: 1,
                    initial_certified_epoch: Some(2), ..Default::default()
                }, 3, true,
            ),
        ]
    }
    fn test_v2_is_certified(info: ValidBlobInfoV2, epoch: Epoch, expected: bool) {
        info.check_invariants().unwrap();
        assert_eq!(BlobInfoV2::Valid(info).is_certified(epoch), expected);
    }

    // --- BlobInfoApi on V2 ---
    param_test! {
        test_v2_is_registered: [
            pool_refs: (
                ValidBlobInfoV2 { count_pooled_refs_total: 1, ..Default::default() },
                9999, true,
            ),
            deletable_refs: (
                ValidBlobInfoV2 { count_deletable_total: 1, ..Default::default() },
                9999, true,
            ),
            permanent_before_expiry: (
                ValidBlobInfoV2 {
                    permanent_total: Some(PermanentBlobInfoV1::new_fixed_for_testing(1, 10, 0)),
                    ..Default::default()
                }, 9, true,
            ),
            permanent_at_expiry: (
                ValidBlobInfoV2 {
                    permanent_total: Some(PermanentBlobInfoV1::new_fixed_for_testing(1, 10, 0)),
                    ..Default::default()
                }, 10, false,
            ),
            empty: (ValidBlobInfoV2::default(), 0, false),
        ]
    }
    fn test_v2_is_registered(info: ValidBlobInfoV2, epoch: Epoch, expected: bool) {
        info.check_invariants().unwrap();
        assert_eq!(BlobInfoV2::Valid(info).is_registered(epoch), expected);
    }

    #[test]
    fn v2_to_blob_status_combines_regular_and_pool_counts() {
        let info = BlobInfoV2::Valid(ValidBlobInfoV2 {
            count_deletable_total: 2,
            count_deletable_certified: 1,
            count_pooled_refs_total: 3,
            count_pooled_refs_certified: 2,
            initial_certified_epoch: Some(1),
            ..Default::default()
        });
        let BlobStatus::Deletable {
            deletable_counts, ..
        } = info.to_blob_status(0)
        else {
            panic!("expected Deletable")
        };
        assert_eq!(deletable_counts.count_deletable_total, 5);
        assert_eq!(deletable_counts.count_deletable_certified, 3);
    }

    // --- V2 invariant violation detection ---
    param_test! {
        test_v2_invariant_violations: [
            pool_total_lt_certified: (ValidBlobInfoV2 {
                count_pooled_refs_certified: 1,
                initial_certified_epoch: Some(1), ..Default::default()
            }),
            certified_epoch_without_refs: (ValidBlobInfoV2 {
                initial_certified_epoch: Some(1), ..Default::default()
            }),
            certified_refs_without_epoch: (ValidBlobInfoV2 {
                count_pooled_refs_total: 1, count_pooled_refs_certified: 1,
                ..Default::default()
            }),
        ]
    }
    fn test_v2_invariant_violations(info: ValidBlobInfoV2) {
        assert!(info.check_invariants().is_err());
    }

    // --- PerObjectBlobInfoV2 ---
    fn make_per_object_v2(end_epoch_info: EndEpochInfo) -> PerObjectBlobInfoV2 {
        PerObjectBlobInfoV2 {
            blob_id: walrus_core::test_utils::blob_id_from_u64(42),
            registered_epoch: 1,
            certified_epoch: None,
            end_epoch_info,
            deletable: true,
            event: event_id_for_testing(),
            deleted: false,
        }
    }

    #[test]
    fn per_object_v2_certify_and_delete_merge() {
        let info = make_per_object_v2(EndEpochInfo::StoragePool(pool_id()));
        let cert_operand = PerObjectBlobInfoMergeOperand {
            change_type: BlobStatusChangeType::Certify,
            change_info: BlobStatusChangeInfo {
                blob_id: walrus_core::test_utils::blob_id_from_u64(42),
                deletable: true,
                epoch: 3,
                end_epoch: 0,
                status_event: event_id_for_testing(),
            },
        };
        let certified = info.merge_with(cert_operand);
        assert_eq!(certified.certified_epoch, Some(3));
        assert!(!certified.deleted);

        let del_operand = PerObjectBlobInfoMergeOperand {
            change_type: BlobStatusChangeType::Delete {
                was_certified: true,
            },
            change_info: BlobStatusChangeInfo {
                blob_id: walrus_core::test_utils::blob_id_from_u64(42),
                deletable: true,
                epoch: 5,
                end_epoch: 0,
                status_event: event_id_for_testing(),
            },
        };
        let deleted = certified.merge_with(del_operand);
        assert!(deleted.deleted);
        assert!(!deleted.is_registered(0));
    }
}

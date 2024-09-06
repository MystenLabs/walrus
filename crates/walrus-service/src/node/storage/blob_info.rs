// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Keeping track of the status of blob IDs and on-chain `Blob` objects.

use std::num::NonZeroU32;

use enum_dispatch::enum_dispatch;
use rocksdb::MergeOperands;
use serde::{Deserialize, Serialize};
use sui_types::{base_types::ObjectID, event::EventID};
use tracing::Level;
use walrus_core::Epoch;
use walrus_sdk::api::{BlobStatus, DeletableStatus};
use walrus_sui::types::{BlobCertified, BlobEvent, BlobRegistered, InvalidBlobId};

pub(super) trait Mergeable: Sized {
    type MergeOperand;

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
    /// Returns true iff the blob is invalid.
    fn is_invalid(&self) -> bool;

    /// Returns the epoch at which this blob was first certified.
    ///
    /// Returns `None` if it isn't certified.
    fn initial_certified_epoch(&self) -> Option<Epoch>;
    /// Returns the event through which this blob was marked invalid.
    ///
    /// Returns `None` if it isn't invalid.
    fn invalidation_event(&self) -> Option<EventID>;

    /// Converts the blob information to a `BlobStatus` object.
    fn to_blob_status(&self) -> BlobStatus;
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

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Clone)]
pub(super) struct BlobStatusChangeInfo {
    pub(super) object_id: ObjectID,
    pub(super) deletable: bool,
    pub(super) epoch: Epoch,
    pub(super) end_epoch: Epoch,
    pub(super) status_event: EventID,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Clone, Copy)]
pub(super) enum BlobStatusChangeType {
    Register,
    Certify,
    Extend,
    Delete { was_certified: bool },
}

impl BlobInfoMergeOperand {
    pub fn to_bytes(&self) -> Vec<u8> {
        bcs::to_bytes(self).expect("blob info merge operand can always be BCS encoded")
    }

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
                object_id: walrus_sui::test_utils::object_id_for_testing(),
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
            ..
        } = value;
        Self::ChangeStatus {
            change_info: BlobStatusChangeInfo {
                object_id: ObjectID::ZERO, // TODO(mlegner): update with new event structs (#762).
                deletable: false,          // TODO(mlegner): update with new event structs (#762).
                epoch: *epoch,
                end_epoch: *end_epoch,
                status_event: *event_id,
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
            ..
        } = value;
        Self::ChangeStatus {
            change_info: BlobStatusChangeInfo {
                object_id: ObjectID::ZERO, // TODO(mlegner): update with new event structs (#762).
                deletable: false,          // TODO(mlegner): update with new event structs (#762).
                epoch: *epoch,
                end_epoch: *end_epoch,
                status_event: *event_id,
            },
            change_type: BlobStatusChangeType::Certify,
        }
    }
}

impl From<&InvalidBlobId> for BlobInfoMergeOperand {
    fn from(value: &InvalidBlobId) -> Self {
        let InvalidBlobId {
            epoch, event_id, ..
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
            BlobEvent::InvalidBlobID(event) => event.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum BlobInfoV1 {
    Invalid {
        epoch: Epoch,
        event: EventID,
    },
    // INV: count_deletable_total > count_deletable_certified
    // INV: permanent_total.is_none() => permanent_certified.is_none()
    // INV: permanent_total.count > permanent_certified.count
    // INV: permanent_total.end_epoch > permanent_certified.end_epoch
    // INV: initial_certified_epoch.is_some()
    //      <=> count_deletable_certified > 0 || permanent_certified.is_some()
    Valid {
        is_metadata_stored: bool,
        count_deletable_total: u32,
        count_deletable_certified: u32,
        permanent_total: Option<PermanentBlobInfoV1>,
        permanent_certified: Option<PermanentBlobInfoV1>,
        initial_certified_epoch: Option<Epoch>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct PermanentBlobInfoV1 {
    /// The total number of `Blob` objects for that blob ID with the given status.
    pub count: NonZeroU32,
    /// The latest expiration epoch among these objects.
    pub end_epoch: u64,
    /// The ID of the object with the latest expiration epoch.
    ///
    /// If there are multiple, this contains the object with the highest status ('certified' >
    /// 'registered') and the earliest `status_changing_epoch`.
    pub object_id: ObjectID,
    /// The epoch in which that object attained its current status.
    pub status_changing_epoch: Epoch,
    /// The ID of the latest blob event of that object.
    pub event: EventID,
}

impl PermanentBlobInfoV1 {
    /// Updates `self` with the change info.
    ///
    /// # Panics
    ///
    /// Panics if the change info has `deletable == true`.
    fn update(&mut self, change_info: &BlobStatusChangeInfo) {
        assert!(!change_info.deletable);

        self.count = self.count.saturating_add(1);
        if change_info.end_epoch > self.end_epoch {
            *self = PermanentBlobInfoV1 {
                count: self.count,
                end_epoch: change_info.end_epoch,
                object_id: change_info.object_id,
                status_changing_epoch: change_info.epoch,
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
            object_id: new_object_id,
            epoch: new_epoch,
            end_epoch: new_end_epoch,
            status_event: new_status_event,
            deletable,
        } = change_info;
        assert!(!deletable);

        match existing_info {
            None => {
                *existing_info = Some(PermanentBlobInfoV1 {
                    count: NonZeroU32::new(1).unwrap(),
                    end_epoch: *new_end_epoch,
                    object_id: *new_object_id,
                    status_changing_epoch: *new_epoch,
                    event: *new_status_event,
                })
            }
            Some(permanent_blob_info) => permanent_blob_info.update(change_info),
        }
    }

    #[cfg(test)]
    fn new_for_testing(count: u32, end_epoch: Epoch, status_changing_epoch: Epoch) -> Self {
        use walrus_sui::test_utils::{fixed_event_id_for_testing, object_id_for_testing};

        Self {
            count: NonZeroU32::new(count).unwrap(),
            end_epoch,
            status_changing_epoch,
            object_id: object_id_for_testing(),
            event: fixed_event_id_for_testing(),
        }
    }
}

impl BlobInfoApi for BlobInfoV1 {
    fn is_metadata_stored(&self) -> bool {
        matches!(
            self,
            Self::Valid {
                is_metadata_stored: true,
                ..
            }
        )
    }

    fn is_invalid(&self) -> bool {
        matches!(self, Self::Invalid { .. })
    }

    fn initial_certified_epoch(&self) -> Option<Epoch> {
        if let Self::Valid {
            initial_certified_epoch,
            ..
        } = self
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

    fn to_blob_status(&self) -> BlobStatus {
        match self {
            BlobInfoV1::Invalid { event, .. } => BlobStatus::Invalid { event: *event },
            BlobInfoV1::Valid {
                count_deletable_total,
                count_deletable_certified,
                permanent_total,
                permanent_certified,
                ..
            } => {
                let deletable_status = DeletableStatus {
                    count_deletable_total: *count_deletable_total,
                    count_deletable_certified: *count_deletable_certified,
                };
                if let Some(PermanentBlobInfoV1 {
                    end_epoch,
                    object_id,
                    event,
                    ..
                }) = permanent_certified.as_ref().or(permanent_total.as_ref())
                {
                    BlobStatus::Permanent {
                        end_epoch: *end_epoch,
                        is_certified: permanent_certified.is_some(),
                        object_id: *object_id,
                        status_event: *event,
                        deletable_status,
                    }
                } else {
                    BlobStatus::Deletable(deletable_status)
                }
            }
        }
    }
}

impl BlobInfoV1 {
    #[tracing::instrument]
    fn update_status(
        mut self,
        change_type: BlobStatusChangeType,
        change_info: BlobStatusChangeInfo,
    ) -> Self {
        let Self::Valid {
            is_metadata_stored: _,
            count_deletable_total,
            count_deletable_certified,
            permanent_total,
            permanent_certified,
            initial_certified_epoch,
        } = &mut self
        else {
            return self;
        };

        if change_info.deletable {
            match change_type {
                BlobStatusChangeType::Register => *count_deletable_total += 1,
                BlobStatusChangeType::Certify => {
                    if count_deletable_total <= count_deletable_certified {
                        tracing::error!(
                            "attempt to certify a deletable blob before corresponding register"
                        );
                        return self;
                    }
                    *count_deletable_certified += 1;
                }
                BlobStatusChangeType::Extend => (),
                BlobStatusChangeType::Delete { was_certified } => {
                    Self::decrement_deletable_counter_on_deletion(count_deletable_total);
                    if was_certified {
                        Self::decrement_deletable_counter_on_deletion(count_deletable_certified);
                    }
                }
            }
        } else {
            match change_type {
                BlobStatusChangeType::Register => {
                    Self::register_permanent(permanent_total, &change_info);
                }
                BlobStatusChangeType::Certify => {
                    if !Self::certify_permanent(permanent_total, permanent_certified, &change_info)
                    {
                        // Return early to prevent updating the `initial_certified_epoch` below.
                        return self;
                    }
                }
                BlobStatusChangeType::Extend => {
                    Self::extend_permanent(permanent_certified, &change_info);
                }
                BlobStatusChangeType::Delete { was_certified } => {
                    Self::delete_permanent(permanent_total, permanent_certified, was_certified);
                }
            }
        }

        // Update initial certified epoch.
        match change_type {
            BlobStatusChangeType::Certify => {
                Self::update_initial_certified_epoch(initial_certified_epoch, change_info.epoch);
            }
            BlobStatusChangeType::Delete { .. } => {
                self.maybe_unset_initial_certified_epoch();
            }
            BlobStatusChangeType::Register | BlobStatusChangeType::Extend => (),
        }

        self
    }

    fn update_initial_certified_epoch(
        current_initial_certified_epoch: &mut Option<Epoch>,
        new_certified_epoch: Epoch,
    ) {
        if current_initial_certified_epoch
            .map_or(true, |existing_epoch| existing_epoch > new_certified_epoch)
        {
            *current_initial_certified_epoch = Some(new_certified_epoch);
        }
    }

    fn maybe_unset_initial_certified_epoch(&mut self) {
        if let Self::Valid {
            count_deletable_certified: 0,
            permanent_certified: None,
            initial_certified_epoch,
            ..
        } = self
        {
            *initial_certified_epoch = None;
        }
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
        permanent_certified: &mut Option<PermanentBlobInfoV1>,
        change_info: &BlobStatusChangeInfo,
    ) {
        let Some(permanent_certified) = permanent_certified else {
            tracing::error!("attempt to extend a permanent blob when none is tracked");
            return;
        };

        permanent_certified.update(change_info);
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
}

impl Mergeable for BlobInfoV1 {
    type MergeOperand = BlobInfoMergeOperand;

    fn merge_with(mut self, operand: Self::MergeOperand) -> Self {
        match &mut self {
            // If the blob is already marked as invalid, do not update the status.
            Self::Invalid { .. } => (),

            Self::Valid {
                ref mut is_metadata_stored,
                ..
            } => match operand {
                BlobInfoMergeOperand::MarkMetadataStored(new_is_metadata_stored) => {
                    *is_metadata_stored = new_is_metadata_stored;
                }
                BlobInfoMergeOperand::MarkInvalid {
                    epoch,
                    status_event,
                } => {
                    return Self::Invalid {
                        epoch,
                        event: status_event,
                    }
                }
                BlobInfoMergeOperand::ChangeStatus {
                    change_type,
                    change_info,
                } => return self.update_status(change_type, change_info),
            },
        }
        self
    }

    fn merge_new(operand: Self::MergeOperand) -> Option<Self> {
        let BlobInfoMergeOperand::ChangeStatus {
            change_type: BlobStatusChangeType::Register,
            change_info:
                BlobStatusChangeInfo {
                    object_id,
                    deletable,
                    epoch,
                    end_epoch,
                    status_event,
                },
        } = operand
        else {
            tracing::error!(
                ?operand,
                "encountered an update other than 'register' for an untracked blob ID"
            );
            return None;
        };

        let (count_deletable_total, permanent_total) = if deletable {
            (1, None)
        } else {
            (
                0,
                Some(PermanentBlobInfoV1 {
                    count: NonZeroU32::new(1).unwrap(),
                    end_epoch,
                    object_id,
                    status_changing_epoch: epoch,
                    event: status_event,
                }),
            )
        };
        let (count_deletable_certified, permanent_certified, initial_certified_epoch) =
            (0, None, None);

        Some(BlobInfoV1::Valid {
            is_metadata_stored: false,
            count_deletable_total,
            count_deletable_certified,
            permanent_total,
            permanent_certified,
            initial_certified_epoch,
        })
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

/// Internal representation of the blob information for use in the database etc. Use
/// [`walrus_sdk::api::BlobStatus`] for anything public facing (e.g., communication to the client).
#[enum_dispatch(BlobInfoApi)]
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Clone)]
pub(crate) enum BlobInfo {
    V1(BlobInfoV1),
}

impl BlobInfo {
    pub fn to_bytes(&self) -> Vec<u8> {
        bcs::to_bytes(self).expect("blob info can always be BCS encoded")
    }

    /// Creates a new (permanent) blob for testing purposes.
    #[cfg(test)]
    pub(super) fn new_for_testing(
        end_epoch: Epoch,
        status: BlobCertificationStatus,
        current_status_event: EventID,
        registered_epoch: Option<Epoch>,
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
                    object_id: walrus_sui::test_utils::object_id_for_testing(),
                    status_changing_epoch: registered_epoch
                        .unwrap_or_else(|| certified_epoch.unwrap()),
                    event: current_status_event,
                };
                let permanent_certified = matches!(status, BlobCertificationStatus::Certified)
                    .then(|| PermanentBlobInfoV1 {
                        status_changing_epoch: certified_epoch.unwrap(),
                        ..permanent_total
                    });
                BlobInfoV1::Valid {
                    is_metadata_stored: false,
                    count_deletable_total: 0,
                    count_deletable_certified: 0,
                    permanent_total: Some(permanent_total),
                    permanent_certified,
                    initial_certified_epoch: certified_epoch,
                }
            }
        };
        Self::V1(blob_info)
    }
}

impl From<BlobInfo> for BlobStatus {
    fn from(value: BlobInfo) -> Self {
        value.to_blob_status()
    }
}

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

#[tracing::instrument(
    level = Level::DEBUG,
    skip(existing_val, operands),
    fields(existing_val = existing_val.is_some())
)]
pub(super) fn merge_blob_info(
    key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let mut current_val: Option<BlobInfo> = existing_val.and_then(deserialize_from_db);

    for operand_bytes in operands {
        let Some(operand) = deserialize_from_db::<BlobInfoMergeOperand>(operand_bytes) else {
            continue;
        };
        tracing::debug!("updating {current_val:?} with {operand:?}");

        current_val = BlobInfo::merge(current_val, operand);
    }

    current_val.as_ref().map(BlobInfo::to_bytes)
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

#[cfg(test)]
mod tests {
    use walrus_sui::test_utils::{event_id_for_testing, fixed_event_id_for_testing};
    use walrus_test_utils::param_test;

    use super::*;

    fn check_invariants(blob_info: &BlobInfoV1) {
        let BlobInfoV1::Valid {
            is_metadata_stored: _,
            count_deletable_total,
            count_deletable_certified,
            permanent_total,
            permanent_certified,
            initial_certified_epoch,
        } = blob_info
        else {
            // Nothing to check.
            return;
        };

        assert!(count_deletable_total >= count_deletable_certified);
        match initial_certified_epoch {
            None => assert!(*count_deletable_certified == 0 && permanent_certified.is_none()),
            Some(_) => assert!(*count_deletable_certified > 0 || permanent_certified.is_some()),
        }

        match (permanent_total, permanent_certified) {
            (None, None) | (Some(_), None) => (),
            (None, Some(_)) => panic!("permanent_total.is_none() => permanent_certified.is_none()"),
            (Some(total_inner), Some(certified_inner)) => {
                assert!(total_inner.end_epoch >= certified_inner.end_epoch);
                assert!(total_inner.count >= certified_inner.count);
            }
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct BlobInfoForTesting(BlobInfoV1);

    impl BlobInfoForTesting {
        fn new() -> Self {
            Self(BlobInfoV1::Valid {
                is_metadata_stored: false,
                count_deletable_total: 0,
                count_deletable_certified: 0,
                permanent_total: None,
                permanent_certified: None,
                initial_certified_epoch: None,
            })
        }

        fn new_invalid(epoch: Epoch) -> Self {
            Self(BlobInfoV1::Invalid {
                epoch,
                event: fixed_event_id_for_testing(),
            })
        }

        fn get(self) -> BlobInfoV1 {
            self.0
        }

        fn with_metadata_stored(mut self) -> Self {
            if let BlobInfoV1::Valid {
                is_metadata_stored, ..
            } = &mut self.0
            {
                *is_metadata_stored = true;
            };
            self
        }

        fn with_deletable_total(mut self, count: u32) -> Self {
            if let BlobInfoV1::Valid {
                count_deletable_total,
                ..
            } = &mut self.0
            {
                *count_deletable_total = count;
            };
            self
        }

        fn with_deletable_certified(mut self, count: u32) -> Self {
            if let BlobInfoV1::Valid {
                count_deletable_certified,
                ..
            } = &mut self.0
            {
                *count_deletable_certified = count;
            };
            self
        }

        fn with_permanent_total(
            mut self,
            count: u32,
            status_changing_epoch: Epoch,
            end_epoch: Epoch,
        ) -> Self {
            if let BlobInfoV1::Valid {
                permanent_total, ..
            } = &mut self.0
            {
                *permanent_total = Some(PermanentBlobInfoV1::new_for_testing(
                    count,
                    end_epoch,
                    status_changing_epoch,
                ));
            };
            self
        }

        fn with_permanent_certified(
            mut self,
            count: u32,
            status_changing_epoch: Epoch,
            end_epoch: Epoch,
        ) -> Self {
            if let BlobInfoV1::Valid {
                permanent_certified,
                ..
            } = &mut self.0
            {
                *permanent_certified = Some(PermanentBlobInfoV1::new_for_testing(
                    count,
                    end_epoch,
                    status_changing_epoch,
                ));
            };
            self
        }

        fn with_initial_certified_epoch(mut self, epoch: Epoch) -> Self {
            if let BlobInfoV1::Valid {
                initial_certified_epoch,
                ..
            } = &mut self.0
            {
                *initial_certified_epoch = Some(epoch);
            };
            self
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
            empty: (BlobInfoForTesting::new()),
            deletable: (BlobInfoForTesting::new().with_deletable_total(2)),
            deletable_certified: (BlobInfoForTesting::new()
                .with_deletable_total(2)
                .with_deletable_certified(1)
                .with_initial_certified_epoch(0)
            ),
            permanent: (BlobInfoForTesting::new().with_permanent_total(1, 0, 2)),
            permanent_certified: (BlobInfoForTesting::new()
                .with_permanent_total(2, 0, 2)
                .with_permanent_certified(1, 1, 2)
                .with_initial_certified_epoch(0)
            ),
            invalid: (BlobInfoForTesting::new_invalid(1)),
        ]
    }
    fn test_mark_metadata_stored_keeps_everything_else_unchanged(
        preexisting_info: BlobInfoForTesting,
    ) {
        let preexisting_info_clone = preexisting_info.clone().get();
        check_invariants(&preexisting_info_clone);
        let updated_info =
            preexisting_info_clone.merge_with(BlobInfoMergeOperand::MarkMetadataStored(true));
        assert_eq!(preexisting_info.with_metadata_stored().get(), updated_info);
    }

    param_test! {
        test_mark_invalid_marks_everything_invalid: [
            empty: (BlobInfoForTesting::new()),
            deletable: (BlobInfoForTesting::new().with_deletable_total(2)),
            deletable_certified: (BlobInfoForTesting::new()
                .with_deletable_total(2)
                .with_deletable_certified(1)
                .with_initial_certified_epoch(0)
            ),
            permanent: (BlobInfoForTesting::new().with_permanent_total(1, 0, 2)),
            permanent_certified: (BlobInfoForTesting::new()
                .with_permanent_total(2, 0, 2)
                .with_permanent_certified(1, 1, 2)
                .with_initial_certified_epoch(0)
            ),
        ]
    }
    fn test_mark_invalid_marks_everything_invalid(preexisting_info: BlobInfoForTesting) {
        let preexisting_info_clone = preexisting_info.clone().get();
        check_invariants(&preexisting_info_clone);
        let event = event_id_for_testing();
        let updated_info = preexisting_info_clone.merge_with(BlobInfoMergeOperand::MarkInvalid {
            epoch: 2,
            status_event: event,
        });
        assert_eq!(BlobInfoV1::Invalid { epoch: 2, event }, updated_info);
    }

    param_test! {
        test_merge_preexisting_expected_successes: [
            register_first_deletable: (
                BlobInfoForTesting::new(),
                BlobInfoMergeOperand::new_change_for_testing(
                    BlobStatusChangeType::Register, true, 1, 2, event_id_for_testing()
                ),
                BlobInfoForTesting::new().with_deletable_total(1),
            ),
            register_additional_deletable: (
                BlobInfoForTesting::new().with_deletable_total(3),
                BlobInfoMergeOperand::new_change_for_testing(
                    BlobStatusChangeType::Register, true, 1, 2, event_id_for_testing()
                ),
                BlobInfoForTesting::new().with_deletable_total(4),
            ),
            certify_first_deletable: (
                BlobInfoForTesting::new().with_deletable_total(3),
                BlobInfoMergeOperand::new_change_for_testing(
                    BlobStatusChangeType::Certify, true, 1, 2, event_id_for_testing()
                ),
                BlobInfoForTesting::new()
                    .with_deletable_total(3)
                    .with_deletable_certified(1)
                    .with_initial_certified_epoch(1),
            ),
            certify_additional_deletable1: (
                BlobInfoForTesting::new()
                    .with_deletable_total(3)
                    .with_deletable_certified(1)
                    .with_initial_certified_epoch(0),
                BlobInfoMergeOperand::new_change_for_testing(
                    BlobStatusChangeType::Certify, true, 1, 2, event_id_for_testing()
                ),
                BlobInfoForTesting::new()
                    .with_deletable_total(3)
                    .with_deletable_certified(2)
                    .with_initial_certified_epoch(0),
            ),
            certify_additional_deletable2: (
                BlobInfoForTesting::new()
                    .with_deletable_total(3)
                    .with_deletable_certified(1)
                    .with_initial_certified_epoch(1),
                BlobInfoMergeOperand::new_change_for_testing(
                    BlobStatusChangeType::Certify, true, 0, 2, event_id_for_testing()
                ),
                BlobInfoForTesting::new()
                    .with_deletable_total(3)
                    .with_deletable_certified(2)
                    .with_initial_certified_epoch(0),
            ),
            register_first_permanent: (
                BlobInfoForTesting::new(),
                BlobInfoMergeOperand::new_change_for_testing(
                    BlobStatusChangeType::Register, false, 1, 2, fixed_event_id_for_testing()
                ),
                BlobInfoForTesting::new().with_permanent_total(1, 1, 2),
            ),
            // TODO(mlegner): Add some more cases for permanent blobs and deletions!
        ]
    }
    fn test_merge_preexisting_expected_successes(
        preexisting_info: BlobInfoForTesting,
        operand: BlobInfoMergeOperand,
        expected_info: BlobInfoForTesting,
    ) {
        let preexisting_info = preexisting_info.get();
        let expected_info = expected_info.get();
        check_invariants(&preexisting_info);
        let updated_info = BlobInfoV1::merge_with(preexisting_info, operand);
        check_invariants(&updated_info);
        assert_eq!(updated_info, expected_info);
    }

    param_test! {
        test_merge_preexisting_expected_failures: [
            certify_permanent_without_register: (
                BlobInfoForTesting::new().get(),
                BlobInfoMergeOperand::new_change_for_testing(
                    BlobStatusChangeType::Certify, false, 42, 314, event_id_for_testing()
                ),
            ),
            extend_permanent_without_certify: (
                BlobInfoForTesting::new().get(),
                BlobInfoMergeOperand::new_change_for_testing(
                    BlobStatusChangeType::Extend, false, 42, 314, event_id_for_testing()
                ),
            ),
            certify_deletable_without_register: (
                BlobInfoForTesting::new().get(),
                BlobInfoMergeOperand::new_change_for_testing(
                    BlobStatusChangeType::Certify, true, 42, 314, event_id_for_testing()
                ),
            ),
        ]
    }
    fn test_merge_preexisting_expected_failures(
        preexisting_info: BlobInfoV1,
        operand: BlobInfoMergeOperand,
    ) {
        check_invariants(&preexisting_info);
        let blob_info = BlobInfoV1::merge_with(preexisting_info.clone(), operand);
        assert_eq!(preexisting_info, blob_info);
    }
}

// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Keeping track of the status of blob IDs and on-chain `Blob` objects.

use std::{cmp::Ordering, num::NonZeroU32};

use enum_dispatch::enum_dispatch;
use rocksdb::MergeOperands;
use serde::{Deserialize, Serialize};
use sui_types::{base_types::ObjectID, event::EventID};
use tracing::Level;
use walrus_core::Epoch;
use walrus_sdk::api::BlobStatus;
use walrus_sui::types::{BlobCertified, BlobEvent, BlobRegistered, InvalidBlobId};

pub(crate) trait Mergeable: Sized {
    type MergeOperand;

    /// Updates the existing blob info with the provided merge operand and returns the result.
    ///
    /// Returns the preexisting value if the merge fails. An error is logged in this case.
    #[must_use]
    fn merge_preexisting(self, operand: Self::MergeOperand) -> Self;

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
            Some(existing_val) => Some(Self::merge_preexisting(existing_val, operand)),
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

    // Question(mlegner): Do we actually need these functions? Currently, they are unused.
    /// Returns true iff there is a corresponding certified permanent blob object.
    #[allow(dead_code)]
    fn is_certified_permanent(&self) -> bool;
    /// Returns true iff there is a corresponding registered or certified permanent blob object.
    #[allow(dead_code)]
    fn is_registered_permanent(&self) -> bool;
    /// Returns true iff there is any corresponding certified blob object.
    #[allow(dead_code)]
    fn is_certified(&self) -> bool;
    /// Returns true iff there is any corresponding registered or certified blob object.
    #[allow(dead_code)]
    fn is_registered(&self) -> bool;
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Clone)]
pub(crate) enum BlobInfoMergeOperand {
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
pub struct BlobStatusChangeInfo {
    pub(super) object_id: ObjectID,
    pub(super) deletable: bool,
    pub(super) epoch: Epoch,
    pub(super) end_epoch: Epoch,
    pub(super) status_event: EventID,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Clone, Copy)]
pub(crate) enum BlobStatusChangeType {
    Register,
    Certify,
    Extend,
    Delete { was_certified: bool },
}

impl BlobInfoMergeOperand {
    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        bcs::to_bytes(self).expect("blob info merge operand can always be BCS encoded")
    }

    #[cfg(test)]
    pub fn new_change_status_for_testing(
        change_type: BlobStatusChangeType,
        epoch: Epoch,
        end_epoch: Epoch,
        status_event: EventID,
    ) -> Self {
        Self::ChangeStatus {
            change_type,
            change_info: BlobStatusChangeInfo {
                object_id: walrus_sui::test_utils::object_id_for_testing(),
                deletable: false,
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
    Valid {
        is_metadata_stored: bool,
        count_deletable_total: u32,
        count_deletable_certified: u32,
        permanent_total: Option<BlobInfoV1Inner>,
        permanent_certified: Option<BlobInfoV1Inner>,
        initial_certified_epoch: Option<Epoch>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BlobInfoV1Inner {
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

    fn is_certified_permanent(&self) -> bool {
        matches!(
            self,
            Self::Valid {
                permanent_certified: Some(_),
                ..
            }
        )
    }

    fn is_registered_permanent(&self) -> bool {
        matches!(
            self,
            Self::Valid {
                permanent_total: Some(_),
                ..
            }
        )
    }

    fn is_certified(&self) -> bool {
        let is_certified = self.is_certified_permanent()
            || matches!(
                self,
                Self::Valid {
                    count_deletable_certified, ..
                } if *count_deletable_certified > 0
            );
        debug_assert_eq!(is_certified, self.initial_certified_epoch().is_some());
        is_certified
    }

    fn is_registered(&self) -> bool {
        self.is_registered_permanent()
            || matches!(
                self,
                Self::Valid {
                    count_deletable_total, ..
                } if *count_deletable_total > 0
            )
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
                if let Some(BlobInfoV1Inner {
                    end_epoch,
                    object_id,
                    event,
                    ..
                }) = permanent_certified
                {
                    BlobStatus::Permanent {
                        end_epoch: *end_epoch,
                        is_certified: true,
                        object_id: *object_id,
                        status_event: *event,
                        count_deletable_total: *count_deletable_total,
                        count_deletable_certified: *count_deletable_certified,
                    }
                } else if let Some(BlobInfoV1Inner {
                    end_epoch,
                    object_id,
                    event,
                    ..
                }) = permanent_total
                {
                    BlobStatus::Permanent {
                        end_epoch: *end_epoch,
                        is_certified: false,
                        object_id: *object_id,
                        status_event: *event,
                        count_deletable_total: *count_deletable_total,
                        count_deletable_certified: *count_deletable_certified,
                    }
                } else {
                    BlobStatus::Deletable {
                        count_deletable_total: *count_deletable_total,
                        count_deletable_certified: *count_deletable_certified,
                    }
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
                    let Some(permanent_total) = permanent_total else {
                        tracing::error!("attempt to certify a permanent blob when none is tracked");
                        return self;
                    };
                    Self::certify_permanent(permanent_total, permanent_certified, &change_info);
                }
                BlobStatusChangeType::Extend => {
                    let Some(permanent_certified) = permanent_certified else {
                        tracing::error!("attempt to extend a permanent blob when none is tracked");
                        return self;
                    };
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
                Self::update_certified_epoch(initial_certified_epoch, change_info.epoch);
            }
            BlobStatusChangeType::Delete { .. } => {
                self.maybe_unset_initial_certified_epoch();
            }
            BlobStatusChangeType::Register | BlobStatusChangeType::Extend => (),
        }

        self
    }

    fn update_certified_epoch(
        current_initial_certified_epoch: &mut Option<Epoch>,
        new_certified_epoch: Epoch,
    ) {
        match current_initial_certified_epoch {
            None => *current_initial_certified_epoch = Some(new_certified_epoch),
            Some(current_certified_epoch) if *current_certified_epoch > new_certified_epoch => {
                *current_initial_certified_epoch = Some(new_certified_epoch)
            }
            _ => (),
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
    /// Logs an error if the counter is 0.
    // Question(mlegner): Do we actually have to handle the cases where those counts are already 0?
    // This should not happen if the system works properly, but one never knows...
    fn decrement_deletable_counter_on_deletion(counter: &mut u32) {
        *counter = counter.checked_sub(1).unwrap_or_else(|| {
            tracing::error!("attempt to delete blob when count was already 0");
            0
        });
    }

    /// Processes a register status change on the [`Option<BlobInfoV1Inner>`] object representing
    /// all  permanent blobs.
    fn register_permanent(
        permanent_total: &mut Option<BlobInfoV1Inner>,
        change_info: &BlobStatusChangeInfo,
    ) {
        Self::update_optional_blob_info_inner(permanent_total, change_info)
    }

    /// Processes a certify status change on the [`BlobInfoV1Inner`] objects representing all and
    /// the certified permanent blobs.
    fn certify_permanent(
        permanent_total: &BlobInfoV1Inner,
        permanent_certified: &mut Option<BlobInfoV1Inner>,
        change_info: &BlobStatusChangeInfo,
    ) {
        let registered_end_epoch = permanent_total.end_epoch;
        let certified_end_epoch = change_info.end_epoch;
        if certified_end_epoch > registered_end_epoch {
            tracing::error!(
                registered_end_epoch,
                certified_end_epoch,
                "attempt to certify a permanent blob with later end epoch than any registered blob",
            );
        }
        Self::update_optional_blob_info_inner(permanent_certified, change_info);
    }

    /// Processes an extend status change on the [`BlobInfoV1Inner`] object representing the
    /// certified permanent blobs.
    fn extend_permanent(
        permanent_certified: &mut BlobInfoV1Inner,
        change_info: &BlobStatusChangeInfo,
    ) {
        Self::update_blob_info_inner(permanent_certified, change_info);
    }

    fn update_optional_blob_info_inner(
        blob_info_inner: &mut Option<BlobInfoV1Inner>,
        change_info: &BlobStatusChangeInfo,
    ) {
        let BlobStatusChangeInfo {
            object_id: new_object_id,
            epoch: new_epoch,
            end_epoch: new_end_epoch,
            status_event: new_status_event,
            deletable,
        } = change_info;
        debug_assert!(!deletable);

        match blob_info_inner {
            None => {
                *blob_info_inner = Some(BlobInfoV1Inner {
                    count: NonZeroU32::new(1).unwrap(),
                    end_epoch: *new_end_epoch,
                    object_id: *new_object_id,
                    status_changing_epoch: *new_epoch,
                    event: *new_status_event,
                })
            }
            Some(blob_info_inner) => Self::update_blob_info_inner(blob_info_inner, change_info),
        }
    }

    fn update_blob_info_inner(
        blob_info_inner: &mut BlobInfoV1Inner,
        change_info: &BlobStatusChangeInfo,
    ) {
        debug_assert!(!change_info.deletable);

        blob_info_inner.count = blob_info_inner.count.saturating_add(1);
        if change_info.end_epoch > blob_info_inner.end_epoch {
            *blob_info_inner = BlobInfoV1Inner {
                count: blob_info_inner.count,
                end_epoch: change_info.end_epoch,
                object_id: change_info.object_id,
                status_changing_epoch: change_info.epoch,
                event: change_info.status_event,
            };
        }
    }

    /// Processes a delete status change on the [`BlobInfoV1Inner`] objects representing all and
    /// the certified permanent blobs.
    ///
    /// This is called when blobs expire at the end of an epoch.
    fn delete_permanent(
        permanent_total: &mut Option<BlobInfoV1Inner>,
        permanent_certified: &mut Option<BlobInfoV1Inner>,
        was_certified: bool,
    ) {
        Self::decrement_blob_info_inner(permanent_total);
        if was_certified {
            Self::decrement_blob_info_inner(permanent_certified);
        }
    }

    fn decrement_blob_info_inner(blob_info_inner: &mut Option<BlobInfoV1Inner>) {
        match blob_info_inner {
            None => tracing::error!("attempt to delete a permanent blob when none is tracked"),
            Some(BlobInfoV1Inner { count, .. }) => {
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

    fn merge_preexisting(mut self, operand: Self::MergeOperand) -> Self {
        match &mut self {
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
            change_type:
                change_type @ BlobStatusChangeType::Register
                | change_type @ BlobStatusChangeType::Certify,
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
                "encountered an update other than 'register' or 'certify' for an untracked blob ID"
            );
            return None;
        };

        let (count_deletable_total, permanent_total) = if deletable {
            (1, None)
        } else {
            (
                0,
                Some(BlobInfoV1Inner {
                    count: NonZeroU32::new(1).unwrap(),
                    end_epoch,
                    object_id,
                    status_changing_epoch: epoch,
                    event: status_event,
                }),
            )
        };
        let (count_deletable_certified, permanent_certified, initial_certified_epoch) =
            if change_type == BlobStatusChangeType::Certify {
                (count_deletable_total, permanent_total.clone(), Some(epoch))
            } else {
                (0, None, None)
            };

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
#[cfg(any(test, feature = "test-utils"))]
pub(crate) enum BlobCertificationStatus {
    Registered,
    Certified,
    Invalid,
}

impl PartialOrd for BlobCertificationStatus {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BlobCertificationStatus {
    fn cmp(&self, other: &Self) -> Ordering {
        use BlobCertificationStatus::*;

        match (self, other) {
            (Registered, Certified) | (Registered, Invalid) | (Certified, Invalid) => {
                Ordering::Less
            }
            (left, right) if left == right => Ordering::Equal,
            _ => Ordering::Greater,
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
    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        bcs::to_bytes(self).expect("blob info can always be BCS encoded")
    }

    /// Creates a new (permanent) blob for testing purposes.
    #[cfg(test)]
    pub fn new_for_testing(
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
                let permanent_total = BlobInfoV1Inner {
                    count: NonZeroU32::new(1).unwrap(),
                    end_epoch,
                    object_id: walrus_sui::test_utils::object_id_for_testing(),
                    status_changing_epoch: registered_epoch
                        .unwrap_or_else(|| certified_epoch.unwrap()),
                    event: current_status_event,
                };
                let permanent_certified = matches!(status, BlobCertificationStatus::Certified)
                    .then(|| BlobInfoV1Inner {
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

    fn merge_preexisting(self, operand: Self::MergeOperand) -> Self {
        match self {
            Self::V1(value) => Self::V1(value.merge_preexisting(operand)),
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

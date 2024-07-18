// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Blob status for the walrus shard storage
//!

use std::cmp::Ordering;

use enum_dispatch::enum_dispatch;
use serde::{Deserialize, Serialize};
use sui_types::event::EventID;
use walrus_core::{encoding::EncodingAxis, EncodingType, Epoch, ShardIndex, SliverType};
use walrus_sdk::api::{BlobCertificationStatus as SdkBlobCertificationStatus, BlobStatus};
use walrus_sui::types::{BlobCertified, BlobEvent, BlobRegistered, InvalidBlobId};

pub(crate) trait Mergeable {
    type MergeOperand;

    #[must_use]
    fn merge(self, operand: Self::MergeOperand) -> Self;
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Copy)]
#[repr(u8)]
pub enum BlobCertificationStatus {
    // INV: The indices of these must not change after release.
    // BCS uses the index in encoding enums. Changing the index will change
    // the database representation.
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

impl From<BlobCertificationStatus> for SdkBlobCertificationStatus {
    fn from(value: BlobCertificationStatus) -> Self {
        use BlobCertificationStatus::*;
        match value {
            Registered => Self::Registered,
            Certified => Self::Certified,
            Invalid => Self::Invalid,
        }
    }
}

#[enum_dispatch]
/// The `BlobInfoApi` trait defines methods for retrieving information about a blob.
pub trait BlobInfoApi {
    /// Returns the end epoch of the blob.
    fn end_epoch(&self) -> Epoch;

    /// Returns the certification status of the blob.
    fn status(&self) -> BlobCertificationStatus;

    /// Returns the event ID of the current status of the blob.
    fn current_status_event(&self) -> EventID;

    /// Returns a boolean indicating whether the metadata of the blob is stored.
    fn is_metadata_stored(&self) -> bool;

    /// Returns the registered epoch of the blob, if available.
    fn registered_epoch(&self) -> Option<Epoch>;

    /// Returns the certified epoch of the blob, if available.
    fn certified_epoch(&self) -> Option<Epoch>;

    /// Returns the invalidated epoch of the blob, if available.
    fn invalidated_epoch(&self) -> Option<Epoch>;

    /// Returns true if the blob is certified.
    fn is_certified(&self) -> bool {
        self.status() == BlobCertificationStatus::Certified
    }

    /// Returns true if the blob is invalid.
    fn is_invalid(&self) -> bool {
        self.status() == BlobCertificationStatus::Invalid
    }

    /// Returns true if the blob is expired given the current epoch.
    fn is_expired(&self, current_epoch: Epoch) -> bool {
        self.end_epoch() <= current_epoch
    }

    /// Converts the blob information to a `BlobStatus` object.
    fn to_blob_status(&self) -> BlobStatus;
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Clone, Copy)]
pub struct BlobInfoV1 {
    pub end_epoch: Epoch,
    pub status: BlobCertificationStatus,
    pub current_status_event: EventID,
    pub is_metadata_stored: bool,
    pub registered_epoch: Option<Epoch>,
    pub certified_epoch: Option<Epoch>,
    pub invalidated_epoch: Option<Epoch>,
}

impl BlobInfoV1 {
    /// Creates a new `BlobInfoV1` instance.
    fn new(
        end_epoch: Epoch,
        status: BlobCertificationStatus,
        current_status_event: EventID,
        registered_epoch: Option<Epoch>,
        certified_epoch: Option<Epoch>,
        invalidated_epoch: Option<Epoch>,
    ) -> Self {
        Self {
            end_epoch,
            status,
            current_status_event,
            is_metadata_stored: false,
            registered_epoch,
            certified_epoch,
            invalidated_epoch,
        }
    }

    /// Updates the status of the blob.
    /// If the new status is higher than the current status, the status is updated.
    /// If the new status is the same as the current status, the status with the higher epoch is kept.
    fn update_status(
        &mut self,
        end_epoch: u64,
        status: BlobCertificationStatus,
        status_event: EventID,
    ) {
        if self.status < status || self.status == status && self.end_epoch < end_epoch {
            self.status = status;
            self.current_status_event = status_event;
            self.end_epoch = end_epoch;
        }
    }

    /// Updates the epoch where the status changed.
    fn update_status_changing_epoch(
        &mut self,
        status: BlobCertificationStatus,
        status_changing_epoch: Epoch,
    ) {
        match status {
            BlobCertificationStatus::Registered => {
                self.registered_epoch = Some(status_changing_epoch);
            }
            BlobCertificationStatus::Certified => {
                self.certified_epoch = Some(status_changing_epoch);
            }
            BlobCertificationStatus::Invalid => {
                self.invalidated_epoch = Some(status_changing_epoch);
            }
        }
    }
}

impl BlobInfoApi for BlobInfoV1 {
    fn end_epoch(&self) -> Epoch {
        self.end_epoch
    }

    fn status(&self) -> BlobCertificationStatus {
        self.status
    }

    fn current_status_event(&self) -> EventID {
        self.current_status_event
    }

    fn is_metadata_stored(&self) -> bool {
        self.is_metadata_stored
    }

    fn registered_epoch(&self) -> Option<Epoch> {
        self.registered_epoch
    }

    fn certified_epoch(&self) -> Option<Epoch> {
        self.certified_epoch
    }

    fn invalidated_epoch(&self) -> Option<Epoch> {
        self.invalidated_epoch
    }

    fn to_blob_status(&self) -> BlobStatus {
        BlobStatus::Existent {
            end_epoch: self.end_epoch,
            status: self.status.into(),
            status_event: self.current_status_event,
        }
    }
}

impl Mergeable for BlobInfoV1 {
    type MergeOperand = BlobInfoMergeOperand;

    fn merge(mut self, operand: Self::MergeOperand) -> Self {
        match operand {
            BlobInfoMergeOperand::ChangeStatus {
                status_changing_epoch,
                end_epoch,
                status,
                status_event,
            } => {
                self.update_status_changing_epoch(status, status_changing_epoch);
                self.update_status(end_epoch, status, status_event);
            }
            BlobInfoMergeOperand::MarkMetadataStored(stored) => {
                self.is_metadata_stored = stored;
            }
        };
        self
    }
}

/// Internal representation of the BlobInfo for use in the database etc.
/// Use [`BlobStatus`] for anything public facing (e.g. communication to the client).
#[enum_dispatch(BlobInfoApi)]
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Clone)]
pub enum BlobInfo {
    V1(BlobInfoV1),
}

impl BlobInfo {
    pub fn new(
        status_changing_epoch: Epoch,
        end_epoch: Epoch,
        status: BlobCertificationStatus,
        current_status_event: EventID,
    ) -> Self {
        let mut blob_info = Self::V1(BlobInfoV1::new(
            end_epoch,
            status,
            current_status_event,
            None,
            None,
            None,
        ));

        let BlobInfo::V1(inner) = &mut blob_info;
        inner.update_status_changing_epoch(status, status_changing_epoch);

        blob_info
    }

    pub fn new_for_testing(
        end_epoch: Epoch,
        status: BlobCertificationStatus,
        current_status_event: EventID,
        registered_epoch: Option<Epoch>,
        certified_epoch: Option<Epoch>,
        invalidated_epoch: Option<Epoch>,
    ) -> Self {
        Self::V1(BlobInfoV1::new(
            end_epoch,
            status,
            current_status_event,
            registered_epoch,
            certified_epoch,
            invalidated_epoch,
        ))
    }

    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        bcs::to_bytes(self).expect("blob info can always be BCS encoded")
    }
}

impl Mergeable for BlobInfo {
    type MergeOperand = BlobInfoMergeOperand;

    fn merge(mut self, operand: Self::MergeOperand) -> Self {
        match self {
            Self::V1(info) => Self::V1(info.merge(operand)),
        }
    }
}

impl From<BlobInfo> for BlobStatus {
    fn from(value: BlobInfo) -> Self {
        value.to_blob_status()
    }
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Clone, Copy)]
pub enum BlobInfoMergeOperand {
    ChangeStatus {
        status_changing_epoch: Epoch,
        end_epoch: Epoch,
        status: BlobCertificationStatus,
        status_event: EventID,
    },
    MarkMetadataStored(bool),
}

impl BlobInfoMergeOperand {
    pub(crate) fn to_bytes(self) -> Vec<u8> {
        bcs::to_bytes(&self).expect("blob info merge operand can always be BCS encoded")
    }
}

impl From<&BlobRegistered> for BlobInfoMergeOperand {
    fn from(value: &BlobRegistered) -> Self {
        Self::ChangeStatus {
            status_changing_epoch: value.epoch,
            end_epoch: value.end_epoch,
            status: BlobCertificationStatus::Registered,
            status_event: value.event_id,
        }
    }
}

impl From<&BlobCertified> for BlobInfoMergeOperand {
    fn from(value: &BlobCertified) -> Self {
        Self::ChangeStatus {
            status_changing_epoch: value.epoch,
            end_epoch: value.end_epoch,
            status: BlobCertificationStatus::Certified,
            status_event: value.event_id,
        }
    }
}

impl From<&InvalidBlobId> for BlobInfoMergeOperand {
    fn from(value: &InvalidBlobId) -> Self {
        Self::ChangeStatus {
            status_changing_epoch: value.epoch,
            end_epoch: value.epoch,
            status: BlobCertificationStatus::Invalid,
            status_event: value.event_id,
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

#[cfg(test)]
mod tests {
    use std::iter;

    use walrus_sui::test_utils::event_id_for_testing;
    use walrus_test_utils::{assert_unordered_eq, param_test};
    use BlobCertificationStatus::*;

    use super::*;

    param_test! {
        blob_info_transitions_to_new_status: [
            registered: (Registered, &[Certified, Invalid]),
            certified: (Certified, &[Invalid]),
            invalid: (Invalid, &[]),
        ]
    }
    fn blob_info_transitions_to_new_status(
        initial: BlobCertificationStatus,
        must_transition_to: &[BlobCertificationStatus],
    ) {
        const EPOCHS: &[Epoch] = &[2, 3, 4];
        const ALL_STATUSES: &[BlobCertificationStatus] = &[Registered, Certified, Invalid];
        let initial_blob_info = BlobInfo::new(1, 3, initial, event_id_for_testing());

        // Must transition to every "higher" status and the current status with higher epoch
        let must_cases = must_transition_to
            .iter()
            .flat_map(|status| {
                EPOCHS.iter().map(|&end_epoch| {
                    create_merge_operand_and_expected_blob_info(
                        &initial_blob_info,
                        end_epoch,
                        *status, // Higher status
                        7,
                        true,
                    )
                })
            })
            .chain(iter::once(create_merge_operand_and_expected_blob_info(
                &initial_blob_info,
                10, // Higher end epoch
                initial,
                7,
                true,
            )));

        // Must not transition to "lower" statuses, or the same status with lower epoch
        let never_cases = ALL_STATUSES
            .iter()
            .filter(|status| status < &&initial)
            .flat_map(|status| {
                EPOCHS.iter().map(|&end_epoch| {
                    create_merge_operand_and_expected_blob_info(
                        &initial_blob_info,
                        10,
                        *status, // Lower status
                        7,
                        false,
                    )
                })
            })
            .chain(iter::once(create_merge_operand_and_expected_blob_info(
                &initial_blob_info,
                2, // Lower end epoch
                initial,
                7,
                false,
            )));

        for (new, expected) in must_cases.chain(never_cases) {
            let actual = initial_blob_info.clone().merge(new);
            assert_eq!(
                actual, expected,
                "'{initial:?}' updated with '{new:?}' should be '{expected:?}': got '{actual:?}'"
            );
        }
    }

    // Helper function to create a merge operand and the expected blob info after merging.
    // `to_end_epoch` `to_status` and `to_status_epoch` are the status the blob wants to change to.
    // `status_change` indicates whether the status should change.
    fn create_merge_operand_and_expected_blob_info(
        initial_blob_info: &BlobInfo,
        to_end_epoch: Epoch,
        to_status: BlobCertificationStatus,
        to_status_epoch: Epoch,
        status_change: bool,
    ) -> (BlobInfoMergeOperand, BlobInfo) {
        let status_event = event_id_for_testing();
        let mut expected = if status_change {
            BlobInfo::new_for_testing(to_end_epoch, to_status, status_event, None, None, None)
        } else {
            BlobInfo::new_for_testing(
                initial_blob_info.end_epoch(),
                initial_blob_info.status(),
                initial_blob_info.current_status_event(),
                None,
                None,
                None,
            )
        };

        let BlobInfo::V1(inner) = &mut expected;

        // Initial status' changing epoch should be preserved.
        inner.registered_epoch = initial_blob_info.registered_epoch();
        inner.certified_epoch = initial_blob_info.certified_epoch();
        inner.invalidated_epoch = initial_blob_info.invalidated_epoch();

        // Update new status' changing epoch.
        match to_status {
            Registered => inner.registered_epoch = Some(to_status_epoch),
            Certified => inner.certified_epoch = Some(to_status_epoch),
            Invalid => inner.invalidated_epoch = Some(to_status_epoch),
        }

        (
            BlobInfoMergeOperand::ChangeStatus {
                status_changing_epoch: to_status_epoch,
                end_epoch: to_end_epoch,
                status: to_status,
                status_event,
            },
            expected,
        )
    }
}

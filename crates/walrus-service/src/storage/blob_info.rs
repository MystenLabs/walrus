// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Blob status for the walrus shard storage
//!

use std::cmp::Ordering;

use serde::{Deserialize, Serialize};
use sui_types::event::EventID;
use walrus_core::{encoding::EncodingAxis, EncodingType, Epoch, ShardIndex, SliverType};
use walrus_sdk::api::{BlobCertificationStatus as SdkBlobCertificationStatus, BlobStatus};
use walrus_sui::types::{BlobCertified, BlobEvent, BlobRegistered, InvalidBlobID};

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

/// Internal representation of the BlobInfo for use in the database etc.
/// Use [`BlobStatus`] for anything public facing (e.g. communication to the client).
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Clone, Copy)]
pub struct BlobInfo {
    pub end_epoch: Epoch,
    pub status: BlobCertificationStatus,
    pub current_status_event: EventID,
    pub is_metadata_stored: bool,
}

impl BlobInfo {
    pub fn new(
        end_epoch: Epoch,
        status: BlobCertificationStatus,
        current_status_event: EventID,
    ) -> Self {
        Self {
            end_epoch,
            status,
            current_status_event,
            is_metadata_stored: false,
        }
    }

    /// Returns true if the blob is certified
    pub fn is_certified(&self) -> bool {
        self.status == BlobCertificationStatus::Certified
    }

    pub(crate) fn to_bytes(self) -> Vec<u8> {
        bcs::to_bytes(&self).expect("blob info can always be BCS encoded")
    }
}

impl From<BlobInfo> for BlobStatus {
    fn from(value: BlobInfo) -> Self {
        Self {
            end_epoch: value.end_epoch,
            status: value.status.into(),
            status_event: value.current_status_event,
        }
    }
}

impl Mergeable for BlobInfo {
    type MergeOperand = BlobInfoMergeOperand;

    fn merge(mut self, operand: Self::MergeOperand) -> Self {
        match operand {
            BlobInfoMergeOperand::ChangeStatus {
                end_epoch,
                status,
                status_event,
            } => {
                if self.status < status || self.status == status && self.end_epoch < end_epoch {
                    self.status = status;
                    self.current_status_event = status_event;
                    self.end_epoch = end_epoch;
                }
            }
            BlobInfoMergeOperand::MarkMetadataStored => {
                self.is_metadata_stored = true;
            }
        };
        self
    }
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Clone, Copy)]
pub enum BlobInfoMergeOperand {
    ChangeStatus {
        end_epoch: Epoch,
        status: BlobCertificationStatus,
        status_event: EventID,
    },
    MarkMetadataStored,
}

impl BlobInfoMergeOperand {
    pub(crate) fn to_bytes(self) -> Vec<u8> {
        bcs::to_bytes(&self).expect("blob info merge operand can always be BCS encoded")
    }
}

impl From<&BlobRegistered> for BlobInfoMergeOperand {
    fn from(value: &BlobRegistered) -> Self {
        Self::ChangeStatus {
            end_epoch: value.end_epoch,
            status: BlobCertificationStatus::Registered,
            status_event: value.event_id,
        }
    }
}

impl From<&BlobCertified> for BlobInfoMergeOperand {
    fn from(value: &BlobCertified) -> Self {
        Self::ChangeStatus {
            end_epoch: value.end_epoch,
            status: BlobCertificationStatus::Certified,
            status_event: value.event_id,
        }
    }
}

impl From<&InvalidBlobID> for BlobInfoMergeOperand {
    fn from(value: &InvalidBlobID) -> Self {
        Self::ChangeStatus {
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
        const EPOCHS: &[Epoch] = &[1, 2, 3];
        const ALL_STATUSES: &[BlobCertificationStatus] = &[Registered, Certified, Invalid];

        let initial_blob_info = BlobInfo::new(2, initial, event_id_for_testing());

        // Must transition to every "higher" status and the current status with higher epoch
        let must_cases = must_transition_to
            .iter()
            .flat_map(|status| {
                EPOCHS
                    .iter()
                    .map(|&end_epoch| create_blob_info_tuple(end_epoch, *status, None))
            })
            .chain(iter::once(create_blob_info_tuple(3, initial, None)));
        // Must not transition to "lower" statuses, or the same status with lower epoch
        let never_cases = ALL_STATUSES
            .iter()
            .filter(|status| !must_transition_to.contains(status) && **status != initial)
            .flat_map(|status| {
                EPOCHS.iter().map(|&end_epoch| {
                    create_blob_info_tuple(end_epoch, *status, Some(initial_blob_info))
                })
            })
            .chain(iter::once(create_blob_info_tuple(
                1,
                initial,
                Some(initial_blob_info),
            )));

        for (new, expected) in must_cases.chain(never_cases) {
            let actual = initial_blob_info.merge(new);
            assert_eq!(
                actual, expected,
                "'{initial:?}' updated with '{new:?}' should be '{expected:?}': got '{actual:?}'"
            );
        }
    }

    fn create_blob_info_tuple(
        end_epoch: Epoch,
        status: BlobCertificationStatus,
        expected: Option<BlobInfo>,
    ) -> (BlobInfoMergeOperand, BlobInfo) {
        let status_event = event_id_for_testing();
        let expected = expected.unwrap_or_else(|| BlobInfo::new(end_epoch, status, status_event));
        (
            BlobInfoMergeOperand::ChangeStatus {
                end_epoch,
                status,
                status_event,
            },
            expected,
        )
    }
}

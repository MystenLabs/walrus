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
pub trait BlobInfoAPI {
    fn registered_epoch(&self) -> Epoch;
    fn certified_epoch(&self) -> Option<Epoch>;
    fn end_epoch(&self) -> Epoch;
    fn invalidated_epoch(&self) -> Option<Epoch>;
    fn status(&self) -> BlobCertificationStatus;
    fn current_status_event(&self) -> EventID;
    fn is_metadata_stored(&self) -> bool;

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
}

/// Internal representation of the BlobInfo for use in the database etc.
/// Use [`BlobStatus`] for anything public facing (e.g. communication to the client).
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Clone, Copy)]
pub struct BlobInfoV1 {
    pub registered_epoch: Epoch,
    pub certified_epoch: Option<Epoch>,
    pub end_epoch: Epoch,
    pub invalidated_epoch: Option<Epoch>,
    pub status: BlobCertificationStatus,
    pub current_status_event: EventID,
    pub is_metadata_stored: bool,
}

impl BlobInfoV1 {
    pub fn new(
        registered_epoch: Epoch,
        end_epoch: Epoch,
        status: BlobCertificationStatus,
        current_status_event: EventID,
    ) -> Self {
        Self {
            registered_epoch,
            certified_epoch: None,
            end_epoch,
            invalidated_epoch: None,
            status,
            current_status_event,
            is_metadata_stored: false,
        }
    }

    pub fn new_for_testing(
        registered_epoch: Epoch,
        certified_epoch: Option<Epoch>,
        end_epoch: Epoch,
        invalidated_epoch: Option<Epoch>,
        status: BlobCertificationStatus,
        current_status_event: EventID,
    ) -> Self {
        Self {
            registered_epoch,
            certified_epoch,
            end_epoch,
            invalidated_epoch,
            status,
            current_status_event,
            is_metadata_stored: false,
        }
    }

    fn update_info(
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
}

impl BlobInfoAPI for BlobInfoV1 {
    fn registered_epoch(&self) -> Epoch {
        self.registered_epoch
    }

    fn certified_epoch(&self) -> Option<Epoch> {
        self.certified_epoch
    }

    fn end_epoch(&self) -> Epoch {
        self.end_epoch
    }

    fn invalidated_epoch(&self) -> Option<Epoch> {
        self.invalidated_epoch
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
}

impl Mergeable for BlobInfoV1 {
    type MergeOperand = BlobInfoMergeOperand;

    fn merge(mut self, operand: Self::MergeOperand) -> Self {
        match operand {
            BlobInfoMergeOperand::RegisterBlob {
                registered_epoch,
                end_epoch,
                status,
                status_event,
            } => {
                self.registered_epoch = registered_epoch;
                self.update_info(end_epoch, status, status_event);
            }
            BlobInfoMergeOperand::CertifyBlob {
                certified_epoch,
                end_epoch,
                status,
                status_event,
            } => {
                self.certified_epoch = Some(certified_epoch);
                self.update_info(end_epoch, status, status_event);
            }
            BlobInfoMergeOperand::InvalidateBlob {
                invalidated_epoch,
                end_epoch,
                status,
                status_event,
            } => {
                self.invalidated_epoch = Some(invalidated_epoch);
                self.update_info(end_epoch, status, status_event);
            }
            BlobInfoMergeOperand::MarkMetadataStored(stored) => {
                self.is_metadata_stored = stored;
            }
        };
        self
    }
}

#[enum_dispatch(BlobInfoAPI)]
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Clone)]
pub enum BlobInfo {
    V1(BlobInfoV1),
}

impl BlobInfo {
    pub fn new(
        registered_epoch: Epoch,
        end_epoch: Epoch,
        status: BlobCertificationStatus,
        current_status_event: EventID,
    ) -> Self {
        Self::V1(BlobInfoV1::new(
            registered_epoch,
            end_epoch,
            status,
            current_status_event,
        ))
    }

    pub fn new_for_testing(
        registered_epoch: Epoch,
        certified_epoch: Option<Epoch>,
        end_epoch: Epoch,
        invalidated_epoch: Option<Epoch>,
        status: BlobCertificationStatus,
        current_status_event: EventID,
    ) -> Self {
        Self::V1(BlobInfoV1::new_for_testing(
            registered_epoch,
            certified_epoch,
            end_epoch,
            invalidated_epoch,
            status,
            current_status_event,
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

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Clone, Copy)]
pub enum BlobInfoMergeOperand {
    RegisterBlob {
        registered_epoch: Epoch,
        end_epoch: Epoch,
        status: BlobCertificationStatus,
        status_event: EventID,
    },
    CertifyBlob {
        certified_epoch: Epoch,
        end_epoch: Epoch,
        status: BlobCertificationStatus,
        status_event: EventID,
    },
    InvalidateBlob {
        invalidated_epoch: Epoch,
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
        Self::RegisterBlob {
            registered_epoch: value.epoch,
            end_epoch: value.end_epoch,
            status: BlobCertificationStatus::Registered,
            status_event: value.event_id,
        }
    }
}

impl From<&BlobCertified> for BlobInfoMergeOperand {
    fn from(value: &BlobCertified) -> Self {
        Self::CertifyBlob {
            certified_epoch: value.epoch,
            end_epoch: value.end_epoch,
            status: BlobCertificationStatus::Certified,
            status_event: value.event_id,
        }
    }
}

impl From<&InvalidBlobId> for BlobInfoMergeOperand {
    fn from(value: &InvalidBlobId) -> Self {
        Self::InvalidateBlob {
            invalidated_epoch: value.epoch,
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
            registered: (Registered, &[Certified]),
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

        let initial_blob_info = match initial {
            Registered => {
                BlobInfo::new_for_testing(1, None, 3, None, initial, event_id_for_testing())
            }
            Certified => {
                BlobInfo::new_for_testing(1, Some(2), 3, None, initial, event_id_for_testing())
            }
            Invalid => {
                BlobInfo::new_for_testing(1, Some(2), 3, Some(10), initial, event_id_for_testing())
            }
        };

        // Must transition to every "higher" status and the current status with higher epoch
        let must_cases = must_transition_to
            .iter()
            .flat_map(|status| {
                EPOCHS
                    .iter()
                    .map(|&end_epoch| create_blob_info_tuple(1, 2, end_epoch, 10, *status, None))
            })
            .chain(iter::once(create_blob_info_tuple(
                1, 2, 4, 10, initial, None,
            )));
        // Must not transition to "lower" statuses, or the same status with lower epoch
        let never_cases = ALL_STATUSES
            .iter()
            .filter(|status| status < &&initial)
            .flat_map(|status| {
                EPOCHS.iter().map(|&end_epoch| {
                    create_blob_info_tuple(
                        1,
                        2,
                        end_epoch,
                        10,
                        *status,
                        Some(initial_blob_info.clone()),
                    )
                })
            })
            .chain(iter::once(create_blob_info_tuple(
                1,
                2,
                2,
                10,
                initial,
                Some(initial_blob_info.clone()),
            )));

        for (new, expected) in must_cases.chain(never_cases) {
            let actual = initial_blob_info.clone().merge(new);
            assert_eq!(
                actual, expected,
                "'{initial:?}' updated with '{new:?}' should be '{expected:?}': got '{actual:?}'"
            );
        }
    }

    fn create_blob_info_tuple(
        registered_epoch: Epoch,
        certified_epoch: Epoch,
        end_epoch: Epoch,
        invalidated_epoch: Epoch,
        status: BlobCertificationStatus,
        expected: Option<BlobInfo>,
    ) -> (BlobInfoMergeOperand, BlobInfo) {
        let status_event = event_id_for_testing();
        let expected = expected.unwrap_or_else(|| match status {
            Registered => BlobInfo::new_for_testing(
                registered_epoch,
                None,
                end_epoch,
                None,
                status,
                status_event,
            ),
            Certified => BlobInfo::new_for_testing(
                registered_epoch,
                Some(certified_epoch),
                end_epoch,
                None,
                status,
                status_event,
            ),
            Invalid => BlobInfo::new_for_testing(
                registered_epoch,
                Some(certified_epoch),
                end_epoch,
                Some(invalidated_epoch),
                status,
                status_event,
            ),
        });
        let operand = match status {
            Registered => BlobInfoMergeOperand::RegisterBlob {
                registered_epoch,
                end_epoch,
                status,
                status_event,
            },
            Certified => BlobInfoMergeOperand::CertifyBlob {
                certified_epoch,
                end_epoch,
                status,
                status_event,
            },
            Invalid => BlobInfoMergeOperand::InvalidateBlob {
                invalidated_epoch,
                end_epoch,
                status,
                status_event,
            },
        };
        (operand, expected)
    }
}

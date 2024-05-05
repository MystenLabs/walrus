// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Blob status for the walrus shard storage
//!

use std::cmp::Ordering;

use serde::{Deserialize, Serialize};
use walrus_core::{EncodingType, Epoch, ShardIndex, SliverType};
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

impl BlobCertificationStatus {
    pub(crate) fn update_status(self, new_status: Self) -> Self {
        use BlobCertificationStatus::*;

        match (self, new_status) {
            (Registered, Certified) | (Registered, Invalid) | (Certified, Invalid) => new_status,
            _ => self,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Clone, Copy)]
pub enum StorageStatus {
    RedStuff(RedStuffStorageStatus),
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Clone, Copy)]
pub enum StorageStatusMergeOperand {
    RedStuff(<RedStuffStorageStatus as Mergeable>::MergeOperand),
}

impl Mergeable for StorageStatus {
    type MergeOperand = StorageStatusMergeOperand;

    fn merge(mut self, operand: Self::MergeOperand) -> Self {
        match (self, operand) {
            (StorageStatus::RedStuff(inner), Self::MergeOperand::RedStuff(operand)) => {
                StorageStatus::RedStuff(inner.merge(operand))
            }
        }
    }
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Clone, Copy, Default)]
pub struct RedStuffStorageStatus {
    pub primary: bool,
    pub secondary: bool,
}

impl Mergeable for RedStuffStorageStatus {
    type MergeOperand = SliverType;

    fn merge(mut self, operand: Self::MergeOperand) -> Self {
        match operand {
            SliverType::Primary => self.primary = true,
            SliverType::Secondary => self.secondary = true,
        }
        self
    }
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Clone, Copy)]
pub struct BlobInfo {
    pub end_epoch: Epoch,
    pub status: BlobCertificationStatus,
    pub is_metadata_stored: bool,
    pub storage_status: StorageStatus,
}

impl BlobInfo {
    pub fn new(end_epoch: Epoch, status: BlobCertificationStatus) -> Self {
        Self {
            end_epoch,
            status,
            is_metadata_stored: false,
            storage_status: StorageStatus::RedStuff(RedStuffStorageStatus::default()),
        }
    }

    /// Returns true if the blob is certified
    pub fn is_certified(&self) -> bool {
        self.status == BlobCertificationStatus::Certified
    }

    pub fn is_all_stored(&self) -> bool {
        self.is_metadata_stored
            && match self.storage_status {
                StorageStatus::RedStuff(RedStuffStorageStatus {
                    primary: true,
                    secondary: true,
                }) => true,
                StorageStatus::RedStuff(_) => false,
            }
    }

    pub(crate) fn to_bytes(self) -> Vec<u8> {
        bcs::to_bytes(&self).expect("blob info can always be BCS encoded")
    }
}

impl Mergeable for BlobInfo {
    type MergeOperand = BlobInfoMergeOperand;

    fn merge(mut self, operand: Self::MergeOperand) -> Self {
        match operand {
            BlobInfoMergeOperand::ChangeStatus { end_epoch, status } => {
                self.end_epoch = self.end_epoch.max(end_epoch);
                self.status = self.status.update_status(status);
            }
            BlobInfoMergeOperand::MarkMetadataStored => {
                self.is_metadata_stored = true;
            }
            BlobInfoMergeOperand::MarkEncodedDataStored(operand) => {
                self.storage_status = self.storage_status.merge(operand);
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
    },
    MarkMetadataStored,
    MarkEncodedDataStored(StorageStatusMergeOperand),
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
        }
    }
}

impl From<&BlobCertified> for BlobInfoMergeOperand {
    fn from(value: &BlobCertified) -> Self {
        Self::ChangeStatus {
            end_epoch: value.end_epoch,
            status: BlobCertificationStatus::Certified,
        }
    }
}

impl From<&InvalidBlobID> for BlobInfoMergeOperand {
    fn from(value: &InvalidBlobID) -> Self {
        Self::ChangeStatus {
            end_epoch: value.epoch,
            status: BlobCertificationStatus::Invalid,
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
    use walrus_test_utils::{assert_unordered_eq, param_test};
    use BlobCertificationStatus::*;

    use super::*;

    param_test! {
        certification_status_transitions_to_new_status: [
            registered: (Registered, &[Certified, Invalid]),
            certified: (Certified, &[Invalid]),
            invalid: (Invalid, &[]),
        ]
    }
    fn certification_status_transitions_to_new_status(
        initial: BlobCertificationStatus,
        must_transition_to: &[BlobCertificationStatus],
    ) {
        const ALL_STATUSES: &[BlobCertificationStatus] = &[Registered, Certified, Invalid];

        let must_cases = must_transition_to.iter().map(|status| (status, status));
        let never_cases = ALL_STATUSES
            .iter()
            .filter(|status| !must_transition_to.contains(status))
            .map(|status| (status, &initial));

        for (&status, &expected) in must_cases.chain(never_cases) {
            let actual = initial.update_status(status);
            assert_eq!(
                actual, expected,
                "'{initial:?}' updated with '{status:?}' should be '{expected:?}': got '{actual:?}'"
            );
        }
    }
}

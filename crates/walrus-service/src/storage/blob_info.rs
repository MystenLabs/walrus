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

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Clone, Copy)]
#[repr(u8)]
pub enum BlobCertificationStatus {
    Registered = 0,
    Certified = 1,
    Invalid = 2,
}

impl PartialOrd for BlobCertificationStatus {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

// Explicitly implement `Ord` to prevent issues from extending or otherwise
// changing `BlobCertificationStatus` in the future.
impl Ord for BlobCertificationStatus {
    fn cmp(&self, other: &Self) -> Ordering {
        match self {
            BlobCertificationStatus::Invalid => match other {
                BlobCertificationStatus::Invalid => Ordering::Equal,
                _ => Ordering::Greater,
            },
            BlobCertificationStatus::Certified => match other {
                BlobCertificationStatus::Invalid => Ordering::Less,
                BlobCertificationStatus::Certified => Ordering::Equal,
                BlobCertificationStatus::Registered => Ordering::Greater,
            },
            BlobCertificationStatus::Registered => match other {
                BlobCertificationStatus::Registered => Ordering::Equal,
                _ => Ordering::Less,
            },
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
                self.status = self.status.max(status);
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

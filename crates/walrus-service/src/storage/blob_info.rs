// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Blob status for the walrus shard storage
//!

use std::cmp::Ordering;

use serde::{Deserialize, Serialize};
use walrus_core::{Epoch, ShardIndex};
use walrus_sui::types::{BlobCertified, BlobEvent, BlobRegistered, InvalidBlobID};

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

/// Explicitly implement `Ord` to prevent issues from extending or otherwise
/// changing `BlobCertificationStatus` in the future.
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
pub struct BlobInfo {
    pub end_epoch: Epoch,
    pub status: BlobCertificationStatus,
}

impl From<&BlobRegistered> for BlobInfo {
    fn from(value: &BlobRegistered) -> Self {
        Self {
            end_epoch: value.end_epoch,
            status: BlobCertificationStatus::Registered,
        }
    }
}

impl From<&BlobCertified> for BlobInfo {
    fn from(value: &BlobCertified) -> Self {
        Self {
            end_epoch: value.end_epoch,
            status: BlobCertificationStatus::Certified,
        }
    }
}

impl From<&InvalidBlobID> for BlobInfo {
    fn from(value: &InvalidBlobID) -> Self {
        Self {
            end_epoch: value.epoch,
            status: BlobCertificationStatus::Invalid,
        }
    }
}

impl From<&BlobEvent> for BlobInfo {
    fn from(value: &BlobEvent) -> Self {
        match value {
            BlobEvent::Registered(event) => event.into(),
            BlobEvent::Certified(event) => event.into(),
            BlobEvent::InvalidBlobID(event) => event.into(),
        }
    }
}

impl BlobInfo {
    /// Returns true if the blob is certified
    pub fn is_certified(&self) -> bool {
        self.status == BlobCertificationStatus::Certified
    }
}

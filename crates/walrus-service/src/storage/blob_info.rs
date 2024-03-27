// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Blob status for the walrus shard storage
//!

use serde::{Deserialize, Serialize};
use walrus_core::{Epoch, ShardIndex};
use walrus_sui::types::{BlobCertified, BlobEvent, BlobRegistered};

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Clone, Copy)]
#[repr(u8)]
pub enum BlobCertificationStatus {
    Registered = 0,
    Certified = 1,
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

impl From<&BlobEvent> for BlobInfo {
    fn from(value: &BlobEvent) -> Self {
        match value {
            BlobEvent::Registered(event) => event.into(),
            BlobEvent::Certified(event) => event.into(),
        }
    }
}

impl BlobInfo {
    /// Returns true if the blob is certified
    pub fn is_certified(&self) -> bool {
        self.status == BlobCertificationStatus::Certified
    }
}

// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Storage pool lifetime info tracking.

use std::cmp::max;

use serde::{Deserialize, Serialize};
use sui_types::base_types::ObjectID;
use walrus_core::Epoch;

use super::{Mergeable, ToBytes};

/// Storage pool lifetime info, tracking when a storage pool was created and when it expires.
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Clone)]
pub(crate) enum StoragePoolInfo {
    V1(StoragePoolInfoV1),
}

impl StoragePoolInfo {
    /// Creates a new storage pool info with the given start and end epochs.
    pub fn new(start_epoch: Epoch, end_epoch: Epoch) -> Self {
        Self::V1(StoragePoolInfoV1 {
            start_epoch,
            end_epoch,
        })
    }

    /// Returns the end epoch of the storage pool.
    pub fn end_epoch(&self) -> Epoch {
        match self {
            Self::V1(v1) => v1.end_epoch,
        }
    }
}

/// V1 of storage pool info.
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Clone)]
pub(crate) struct StoragePoolInfoV1 {
    /// The epoch in which the storage pool was created.
    pub start_epoch: Epoch,
    /// The epoch at which the storage pool expires.
    pub end_epoch: Epoch,
}

impl ToBytes for StoragePoolInfo {}

/// Merge operand for the storage pool info table.
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Clone)]
pub(crate) enum StoragePoolInfoMergeOperand {
    /// Create a new storage pool with the given start and end epochs.
    Create {
        start_epoch: Epoch,
        end_epoch: Epoch,
    },
    /// Extend the storage pool to a new end epoch.
    Extend { end_epoch: Epoch },
}

impl ToBytes for StoragePoolInfoMergeOperand {}

impl Mergeable for StoragePoolInfo {
    type MergeOperand = StoragePoolInfoMergeOperand;
    type Key = ObjectID;

    fn merge_with(self, operand: Self::MergeOperand) -> Self {
        match operand {
            StoragePoolInfoMergeOperand::Extend { end_epoch } => {
                let Self::V1(mut v1) = self;
                v1.end_epoch = max(v1.end_epoch, end_epoch);
                Self::V1(v1)
            }
            StoragePoolInfoMergeOperand::Create {
                start_epoch,
                end_epoch,
            } => {
                unreachable!(
                    "create operand should not be applied to an existing storage pool: \
                    start_epoch: {start_epoch}, end_epoch: {end_epoch}"
                );
            }
        }
    }

    fn merge_new(operand: Self::MergeOperand) -> Option<Self> {
        match operand {
            StoragePoolInfoMergeOperand::Create {
                start_epoch,
                end_epoch,
            } => Some(Self::new(start_epoch, end_epoch)),
            StoragePoolInfoMergeOperand::Extend { end_epoch } => {
                unreachable!("received extend operand for non-existent storage pool: {end_epoch}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_and_extend() {
        let info = StoragePoolInfo::merge_new(StoragePoolInfoMergeOperand::Create {
            start_epoch: 1,
            end_epoch: 10,
        })
        .expect("should create pool");
        assert_eq!(info, StoragePoolInfo::new(1, 10));

        // Extending to a higher epoch updates end_epoch.
        let extended = info
            .clone()
            .merge_with(StoragePoolInfoMergeOperand::Extend { end_epoch: 20 });
        assert_eq!(extended, StoragePoolInfo::new(1, 20));

        // Extending to a lower epoch keeps the existing end_epoch.
        let not_shrunk = info.merge_with(StoragePoolInfoMergeOperand::Extend { end_epoch: 5 });
        assert_eq!(not_shrunk, StoragePoolInfo::new(1, 10));
    }

    #[test]
    #[should_panic(expected = "received extend operand for non-existent storage pool")]
    fn extend_non_existent_returns_none() {
        let operand = StoragePoolInfoMergeOperand::Extend { end_epoch: 10 };
        assert_eq!(StoragePoolInfo::merge_new(operand), None);
    }
}

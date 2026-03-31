// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Per-object blob info for storage pool blobs.

use std::fmt::Debug;

use serde::{Deserialize, Serialize};
use sui_types::{base_types::ObjectID, event::EventID};
use walrus_core::{BlobId, Epoch};

use super::{
    BlobStatusChangeType,
    Mergeable,
    PooledBlobChangeInfo,
    PooledChangeTypeAndInfo,
    ToBytes,
};

/// Per-object pooled blob info merge operand.
///
/// This is an enum to allow future extensibility: new operand shapes can be added as variants
/// without breaking deserialization of existing operands in RocksDB.
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Clone)]
pub(crate) enum PerObjectPooledBlobInfoMergeOperand {
    V1(PerObjectPooledBlobInfoMergeOperandV1),
}

impl ToBytes for PerObjectPooledBlobInfoMergeOperand {}

impl<T: PooledChangeTypeAndInfo> From<&T> for PerObjectPooledBlobInfoMergeOperand {
    fn from(event: &T) -> Self {
        Self::V1(PerObjectPooledBlobInfoMergeOperandV1 {
            change_type: event.change_type(),
            change_info: event.change_info(),
        })
    }
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Clone)]
pub(crate) struct PerObjectPooledBlobInfoMergeOperandV1 {
    pub change_type: BlobStatusChangeType,
    pub change_info: PooledBlobChangeInfo,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Clone)]
pub(crate) struct PerObjectPooledBlobInfoV1 {
    /// The blob ID.
    pub blob_id: BlobId,
    /// The epoch in which the blob has been registered.
    pub registered_epoch: Epoch,
    /// The epoch in which the blob was first certified, `None` if the blob is uncertified.
    pub certified_epoch: Option<Epoch>,
    /// The storage pool this blob belongs to.
    pub storage_pool_id: ObjectID,
    /// The ID of the last blob event related to this object.
    pub event: EventID,
    // TODO(WAL-1186): this field is not used. When blob is deleted, the node directly deletes the
    // entry from the table. Removing this field.
    /// Whether the blob has been deleted.
    pub deleted: bool,
}

impl ToBytes for PerObjectPooledBlobInfoV1 {}

impl Mergeable for PerObjectPooledBlobInfoV1 {
    type MergeOperand = PerObjectPooledBlobInfoMergeOperand;
    type Key = ObjectID;

    fn merge_with(
        mut self,
        PerObjectPooledBlobInfoMergeOperand::V1(PerObjectPooledBlobInfoMergeOperandV1 {
            change_type,
            change_info,
        }): PerObjectPooledBlobInfoMergeOperand,
    ) -> Self {
        assert_eq!(
            self.blob_id, change_info.blob_id,
            "blob ID mismatch in merge operand"
        );
        self.event = change_info.status_event;
        match change_type {
            // Only Certify is a valid merge operation for an existing pooled blob entry.
            BlobStatusChangeType::Certify => {
                assert!(
                    self.certified_epoch.is_none(),
                    "cannot certify an already certified pooled blob {}",
                    self.blob_id
                );
                self.certified_epoch = Some(change_info.epoch);
            }
            // Register should never be applied to an existing entry.
            BlobStatusChangeType::Register => {
                unreachable!(
                    "cannot register an already registered pooled blob {}",
                    self.blob_id
                );
            }
            // Extend doesn't apply to pooled blobs (pool controls lifetime).
            BlobStatusChangeType::Extend => {
                unreachable!(
                    "cannot extend a pooled blob {}; pool lifetime is managed by the pool",
                    self.blob_id
                );
            }
            // Delete removes the entry directly via batch.delete_batch, not via merge.
            BlobStatusChangeType::Delete { .. } => {
                unreachable!(
                    "blob deletion deletes the entry directly, not via merge operation; \
                    blob ID: {}",
                    self.blob_id
                );
            }
        }
        self
    }

    fn merge_new(operand: Self::MergeOperand) -> Option<Self> {
        let PerObjectPooledBlobInfoMergeOperand::V1(PerObjectPooledBlobInfoMergeOperandV1 {
            change_type: BlobStatusChangeType::Register,
            change_info:
                PooledBlobChangeInfo {
                    blob_id,
                    epoch,
                    storage_pool_id,
                    status_event,
                },
        }) = operand
        else {
            tracing::error!(
                ?operand,
                "encountered an update other than 'register' for an untracked pooled blob object"
            );
            debug_assert!(
                false,
                "encountered an update other than 'register' for an untracked pooled blob object: \
                {operand:?}"
            );
            return None;
        };
        Some(Self {
            blob_id,
            registered_epoch: epoch,
            certified_epoch: None,
            storage_pool_id,
            event: status_event,
            deleted: false,
        })
    }
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Clone)]
pub(crate) enum PerObjectPooledBlobInfo {
    V1(PerObjectPooledBlobInfoV1),
}

impl From<PerObjectPooledBlobInfoV1> for PerObjectPooledBlobInfo {
    fn from(v: PerObjectPooledBlobInfoV1) -> Self {
        Self::V1(v)
    }
}

impl ToBytes for PerObjectPooledBlobInfo {}

impl Mergeable for PerObjectPooledBlobInfo {
    type MergeOperand = PerObjectPooledBlobInfoMergeOperand;
    type Key = ObjectID;

    fn merge_with(self, operand: Self::MergeOperand) -> Self {
        match self {
            Self::V1(value) => Self::V1(value.merge_with(operand)),
        }
    }

    fn merge_new(operand: Self::MergeOperand) -> Option<Self> {
        PerObjectPooledBlobInfoV1::merge_new(operand).map(Self::from)
    }
}

#[cfg(test)]
mod tests {
    use walrus_sui::test_utils::event_id_for_testing;

    use super::*;

    fn pool_id() -> ObjectID {
        walrus_sui::test_utils::object_id_for_testing()
    }

    fn make_register_operand(blob_id: BlobId, epoch: Epoch) -> PerObjectPooledBlobInfoMergeOperand {
        PerObjectPooledBlobInfoMergeOperand::V1(PerObjectPooledBlobInfoMergeOperandV1 {
            change_type: BlobStatusChangeType::Register,
            change_info: PooledBlobChangeInfo {
                blob_id,
                epoch,
                storage_pool_id: pool_id(),
                status_event: event_id_for_testing(),
            },
        })
    }

    fn make_operand(
        change_type: BlobStatusChangeType,
        blob_id: BlobId,
        epoch: Epoch,
    ) -> PerObjectPooledBlobInfoMergeOperand {
        PerObjectPooledBlobInfoMergeOperand::V1(PerObjectPooledBlobInfoMergeOperandV1 {
            change_type,
            change_info: PooledBlobChangeInfo {
                blob_id,
                epoch,
                storage_pool_id: pool_id(),
                status_event: event_id_for_testing(),
            },
        })
    }

    #[test]
    fn register_and_certify() {
        let blob_id = walrus_core::test_utils::blob_id_from_u64(1);
        let info = PerObjectPooledBlobInfo::merge_new(make_register_operand(blob_id, 5))
            .expect("should create entry");
        let PerObjectPooledBlobInfo::V1(v1) = &info;
        assert_eq!(v1.blob_id, blob_id);
        assert_eq!(v1.registered_epoch, 5);
        assert_eq!(v1.certified_epoch, None);
        assert_eq!(v1.storage_pool_id, pool_id());

        let info = info.merge_with(make_operand(BlobStatusChangeType::Certify, blob_id, 7));
        let PerObjectPooledBlobInfo::V1(v1) = &info;
        assert_eq!(v1.certified_epoch, Some(7));
    }

    #[test]
    #[should_panic(expected = "encountered an update other than 'register'")]
    fn non_register_merge_new_panics() {
        let blob_id = walrus_core::test_utils::blob_id_from_u64(1);
        let operand = make_operand(BlobStatusChangeType::Certify, blob_id, 1);
        let _ = PerObjectPooledBlobInfo::merge_new(operand);
    }

    #[test]
    #[should_panic(expected = "cannot extend a pooled blob")]
    fn extend_panics() {
        let blob_id = walrus_core::test_utils::blob_id_from_u64(1);
        let info = PerObjectPooledBlobInfo::merge_new(make_register_operand(blob_id, 1))
            .expect("should create entry");
        let info = info.merge_with(make_operand(BlobStatusChangeType::Certify, blob_id, 2));
        let _ = info.merge_with(make_operand(BlobStatusChangeType::Extend, blob_id, 3));
    }

    #[test]
    #[should_panic(expected = "blob deletion deletes the entry directly")]
    fn delete_via_merge_panics() {
        let blob_id = walrus_core::test_utils::blob_id_from_u64(1);
        let info = PerObjectPooledBlobInfo::merge_new(make_register_operand(blob_id, 1))
            .expect("should create entry");
        let _ = info.merge_with(make_operand(
            BlobStatusChangeType::Delete {
                was_certified: false,
            },
            blob_id,
            2,
        ));
    }
}

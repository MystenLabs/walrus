// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Blob pool.

use std::collections::HashMap;

use rand::{Rng, seq::IteratorRandom};
use walrus_core::{BlobId, Epoch, EpochCount};
use walrus_sdk::ObjectID;

use super::client_op_generator::WalrusClientOp;

pub(crate) struct BlobDataAndInfo {
    blob: Vec<u8>,
    blob_object_id: ObjectID,
    deletable: bool,
    end_epoch: Epoch,
}

pub(crate) struct BlobPool {
    blobs: HashMap<BlobId, BlobDataAndInfo>,
}

impl BlobPool {
    pub fn new() -> Self {
        Self {
            blobs: HashMap::new(),
        }
    }

    pub fn select_random_blob_id<R: Rng>(&self, rng: &mut R) -> Option<BlobId> {
        self.blobs.keys().choose(rng).cloned()
    }

    pub fn select_random_deletable_blob_id<R: Rng>(&self, rng: &mut R) -> Option<BlobId> {
        self.blobs
            .iter()
            .filter(|(_, blob_data)| blob_data.deletable)
            .choose(rng)
            .map(|(blob_id, _)| *blob_id)
    }

    pub fn update_blob_pool(
        &mut self,
        blob_id: BlobId,
        blob_object_id: Option<ObjectID>,
        op: WalrusClientOp,
    ) {
        match op {
            WalrusClientOp::Write {
                blob,
                deletable,
                store_length,
            } => {
                self.add_new_blob(
                    blob_id,
                    blob_object_id.expect("write op must set object id"),
                    blob,
                    deletable,
                    store_length,
                );
            }
            WalrusClientOp::Delete { blob_id } => {
                self.delete_blob(blob_id);
            }
            WalrusClientOp::Extend {
                blob_id,
                object_id: _object_id,
                store_length,
            } => {
                self.extend_blob(blob_id, store_length);
            }
            WalrusClientOp::Read { blob_id: _blob_id } => {
                // Do nothing.
            }
        }
    }

    fn add_new_blob(
        &mut self,
        blob_id: BlobId,
        blob_object_id: ObjectID,
        blob: Vec<u8>,
        deletable: bool,
        end_epoch: Epoch,
    ) {
        self.blobs.insert(
            blob_id,
            BlobDataAndInfo {
                blob,
                blob_object_id,
                deletable,
                end_epoch,
            },
        );
    }

    fn delete_blob(&mut self, blob_id: BlobId) {
        self.blobs.remove(&blob_id);
    }

    fn extend_blob(&mut self, blob_id: BlobId, additional_epochs: EpochCount) {
        let blob_data = self.blobs.get_mut(&blob_id).expect("blob must exist");
        blob_data.end_epoch += additional_epochs;
    }

    pub fn assert_blob_data(&self, blob_id: BlobId, blob: &[u8]) {
        let blob_data = self.blobs.get(&blob_id).expect("blob must exist");
        assert_eq!(blob_data.blob, blob);
    }

    pub fn expire_blobs_in_new_epoch(&mut self, epoch: Epoch) {
        let expired_blob_ids: Vec<BlobId> = self
            .blobs
            .iter()
            .filter(|(_, blob_data)| blob_data.end_epoch <= epoch)
            .map(|(blob_id, _)| *blob_id)
            .collect();

        for blob_id in expired_blob_ids {
            self.blobs.remove(&blob_id);
        }
    }

    pub fn get_blob_object_id(&self, blob_id: BlobId) -> Option<ObjectID> {
        self.blobs
            .get(&blob_id)
            .map(|blob_data| blob_data.blob_object_id)
    }

    pub fn is_empty(&self) -> bool {
        self.blobs.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use rand::thread_rng;
    use walrus_core::BlobId;
    use walrus_sdk::ObjectID;

    use super::*;

    fn create_test_blob_id() -> BlobId {
        BlobId([1; 32])
    }

    fn create_test_object_id() -> ObjectID {
        ObjectID::new([2; 32])
    }

    fn create_test_blob_data() -> Vec<u8> {
        vec![1, 2, 3, 4, 5]
    }

    #[test]
    fn test_update_blob_pool_write_operation() {
        let mut pool = BlobPool::new();
        let blob_id = create_test_blob_id();
        let object_id = create_test_object_id();
        let blob_data = create_test_blob_data();

        let write_op = WalrusClientOp::Write {
            blob: blob_data.clone(),
            deletable: true,
            store_length: 10,
        };

        pool.update_blob_pool(blob_id, Some(object_id), write_op);

        assert!(!pool.is_empty());
        assert_eq!(pool.blobs.len(), 1);
        assert!(pool.blobs.contains_key(&blob_id));

        let stored_blob = &pool.blobs[&blob_id];
        assert_eq!(stored_blob.blob, blob_data);
        assert_eq!(stored_blob.blob_object_id, object_id);
        assert!(stored_blob.deletable);
        assert_eq!(stored_blob.end_epoch, 10);
    }

    #[test]
    fn test_update_blob_pool_delete_operation() {
        let mut pool = BlobPool::new();
        let blob_id = create_test_blob_id();
        let object_id = create_test_object_id();

        // First add a blob
        let write_op = WalrusClientOp::Write {
            blob: create_test_blob_data(),
            deletable: true,
            store_length: 10,
        };
        pool.update_blob_pool(blob_id, Some(object_id), write_op);
        assert!(!pool.is_empty());

        // Then delete it
        let delete_op = WalrusClientOp::Delete { blob_id };
        pool.update_blob_pool(blob_id, None, delete_op);

        assert!(pool.is_empty());
        assert!(!pool.blobs.contains_key(&blob_id));
    }

    #[test]
    fn test_update_blob_pool_extend_operation() {
        let mut pool = BlobPool::new();
        let blob_id = create_test_blob_id();
        let object_id = create_test_object_id();

        // First add a blob
        let write_op = WalrusClientOp::Write {
            blob: create_test_blob_data(),
            deletable: true,
            store_length: 10,
        };
        pool.update_blob_pool(blob_id, Some(object_id), write_op);

        // Then extend it
        let extend_op = WalrusClientOp::Extend {
            blob_id,
            object_id,
            store_length: 5,
        };
        pool.update_blob_pool(blob_id, None, extend_op);

        let stored_blob = &pool.blobs[&blob_id];
        assert_eq!(stored_blob.end_epoch, 15); // 10 + 5
    }

    #[test]
    fn test_update_blob_pool_read_operation() {
        let mut pool = BlobPool::new();
        let blob_id = create_test_blob_id();
        let object_id = create_test_object_id();

        // First add a blob
        let write_op = WalrusClientOp::Write {
            blob: create_test_blob_data(),
            deletable: true,
            store_length: 10,
        };
        pool.update_blob_pool(blob_id, Some(object_id), write_op);

        // Read operation should not change anything
        let read_op = WalrusClientOp::Read { blob_id };
        pool.update_blob_pool(blob_id, None, read_op);

        assert_eq!(pool.blobs.len(), 1);
        let stored_blob = &pool.blobs[&blob_id];
        assert_eq!(stored_blob.end_epoch, 10); // Unchanged
    }

    #[test]
    fn test_assert_blob_data_success() {
        let mut pool = BlobPool::new();
        let blob_id = create_test_blob_id();
        let object_id = create_test_object_id();
        let blob_data = create_test_blob_data();

        let write_op = WalrusClientOp::Write {
            blob: blob_data.clone(),
            deletable: true,
            store_length: 10,
        };
        pool.update_blob_pool(blob_id, Some(object_id), write_op);

        // Should not panic
        pool.assert_blob_data(blob_id, &blob_data);
    }

    #[test]
    #[should_panic]
    fn test_assert_blob_data_failure() {
        let mut pool = BlobPool::new();
        let blob_id = create_test_blob_id();
        let object_id = create_test_object_id();

        let write_op = WalrusClientOp::Write {
            blob: create_test_blob_data(),
            deletable: true,
            store_length: 10,
        };
        pool.update_blob_pool(blob_id, Some(object_id), write_op);

        // Should panic because data doesn't match
        pool.assert_blob_data(blob_id, &[9, 8, 7]);
    }

    #[test]
    fn test_expire_blobs_in_new_epoch() {
        let mut pool = BlobPool::new();

        // Add blobs with different expiration epochs
        let blob_id1 = BlobId([1; 32]);
        let blob_id2 = BlobId([2; 32]);
        let blob_id3 = BlobId([3; 32]);
        let object_id = create_test_object_id();

        let write_op1 = WalrusClientOp::Write {
            blob: vec![1],
            deletable: true,
            store_length: 5,
        };
        let write_op2 = WalrusClientOp::Write {
            blob: vec![2],
            deletable: true,
            store_length: 10,
        };
        let write_op3 = WalrusClientOp::Write {
            blob: vec![3],
            deletable: true,
            store_length: 15,
        };

        pool.update_blob_pool(blob_id1, Some(object_id), write_op1);
        pool.update_blob_pool(blob_id2, Some(object_id), write_op2);
        pool.update_blob_pool(blob_id3, Some(object_id), write_op3);

        assert_eq!(pool.blobs.len(), 3);

        // Expire blobs at epoch 10
        pool.expire_blobs_in_new_epoch(10);

        // Only blob3 should remain (expires at epoch 15)
        assert_eq!(pool.blobs.len(), 1);
        assert!(pool.blobs.contains_key(&blob_id3));
        assert!(!pool.blobs.contains_key(&blob_id1));
        assert!(!pool.blobs.contains_key(&blob_id2));
    }

    #[test]
    fn test_multiple_blobs_with_mixed_deletable_flags() {
        let mut pool = BlobPool::new();
        let mut rng = thread_rng();

        // Add deletable blob
        let deletable_blob_id = BlobId([1; 32]);
        let write_op1 = WalrusClientOp::Write {
            blob: vec![1],
            deletable: true,
            store_length: 10,
        };
        pool.update_blob_pool(deletable_blob_id, Some(create_test_object_id()), write_op1);

        // Add non-deletable blob
        let permanent_blob_id = BlobId([2; 32]);
        let write_op2 = WalrusClientOp::Write {
            blob: vec![2],
            deletable: false,
            store_length: 10,
        };
        pool.update_blob_pool(permanent_blob_id, Some(create_test_object_id()), write_op2);

        assert_eq!(pool.blobs.len(), 2);

        // Random blob selection should return either blob
        let selected = pool.select_random_blob_id(&mut rng);
        assert!(selected == Some(deletable_blob_id) || selected == Some(permanent_blob_id));

        // Random deletable blob selection should only return the deletable one
        assert_eq!(
            pool.select_random_deletable_blob_id(&mut rng),
            Some(deletable_blob_id)
        );
    }
}

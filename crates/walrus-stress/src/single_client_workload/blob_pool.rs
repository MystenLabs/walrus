// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Blob pool.

use std::collections::HashMap;

use rand::{Rng, seq::IteratorRandom};
use walrus_core::{BlobId, Epoch, EpochCount};
use walrus_sdk::ObjectID;

use super::client_op_generator::WalrusNodeClientOp;

/// Data and info of a blob.
pub(crate) struct BlobDataAndInfo {
    /// The user blob data. Only stored if `store_blob_data` is true in BlobPool.
    blob: Option<Vec<u8>>,
    /// The blob objects.
    blob_objects: HashMap<ObjectID, BlobObjectInfo>,
}

/// Info of a blob object.
pub(crate) struct BlobObjectInfo {
    /// Whether the blob is deletable.
    deletable: bool,
    /// The epoch at which the blob will be expired.
    end_epoch: Epoch,
}

/// Manages a pool of blobs that are live in the system.
pub(crate) struct BlobPool {
    /// The blobs in the pool.
    blobs: HashMap<BlobId, BlobDataAndInfo>,
    /// Whether to store the blob data.
    store_blob_data: bool,
    /// The maximum number of blobs in the pool.
    max_blobs_in_pool: usize,
}

impl BlobPool {
    pub fn new(store_blob_data: bool, max_blobs_in_pool: usize) -> Self {
        Self {
            blobs: HashMap::new(),
            store_blob_data,
            max_blobs_in_pool,
        }
    }

    pub fn select_random_blob_id<R: Rng>(&self, rng: &mut R) -> Option<BlobId> {
        self.blobs.keys().choose(rng).cloned()
    }

    pub fn select_random_deletable_blob_id<R: Rng>(
        &self,
        rng: &mut R,
    ) -> Option<(BlobId, ObjectID)> {
        self.blobs
            .iter()
            .flat_map(|(blob_id, blob_data)| {
                blob_data
                    .blob_objects
                    .iter()
                    .filter(|(_, blob_object_info)| blob_object_info.deletable)
                    .map(move |(object_id, _)| (*blob_id, *object_id))
            })
            .choose(rng)
    }

    /// Returns the object id of a blob.
    pub fn select_random_blob_object_id<R: Rng>(
        &self,
        blob_id: BlobId,
        rng: &mut R,
    ) -> Option<ObjectID> {
        self.blobs
            .get(&blob_id)
            .and_then(|blob_data| blob_data.blob_objects.keys().choose(rng).cloned())
    }

    pub fn select_random_blob_data<R: Rng>(&self, rng: &mut R) -> Option<Vec<u8>> {
        self.blobs
            .values()
            .flat_map(|blob_data| blob_data.blob.clone())
            .choose(rng)
    }

    /// Updates the blob pool with a client operation.
    pub fn update_blob_pool(
        &mut self,
        blob_id: BlobId,
        blob_object_id: Option<ObjectID>,
        op: WalrusNodeClientOp,
    ) {
        match op {
            WalrusNodeClientOp::Write {
                blob,
                deletable,
                store_epoch_ahead,
            } => {
                self.add_new_blob(
                    blob_id,
                    blob_object_id.expect("write op must set object id"),
                    blob,
                    deletable,
                    store_epoch_ahead,
                );
            }
            WalrusNodeClientOp::Delete { blob_id, object_id } => {
                self.delete_blob(blob_id, object_id);
            }
            WalrusNodeClientOp::Extend {
                blob_id,
                object_id,
                store_epoch_ahead,
            } => {
                self.extend_blob(blob_id, object_id, store_epoch_ahead);
            }
            WalrusNodeClientOp::Read {
                blob_id: _blob_id,
                sliver_type: _sliver_type,
            } => {
                // Do nothing.
            }
            WalrusNodeClientOp::None => {
                // Do nothing.
            }
        }
    }

    /// Adds a new blob to the pool.
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
                blob: if self.store_blob_data {
                    Some(blob)
                } else {
                    None
                },
                blob_objects: HashMap::from([(
                    blob_object_id,
                    BlobObjectInfo {
                        deletable,
                        end_epoch,
                    },
                )]),
            },
        );
    }

    /// Deletes a blob from the pool.
    fn delete_blob(&mut self, blob_id: BlobId, object_id: ObjectID) {
        self.blobs
            .get_mut(&blob_id)
            .expect("blob must exist")
            .blob_objects
            .remove(&object_id);

        // If there are no more blob objects, remove the blob from the pool.
        if self.blobs[&blob_id].blob_objects.is_empty() {
            self.blobs.remove(&blob_id);
        }
    }

    /// Extends the end epoch of a blob.
    fn extend_blob(&mut self, blob_id: BlobId, object_id: ObjectID, additional_epochs: EpochCount) {
        let blob_data = self
            .blobs
            .get_mut(&blob_id)
            .expect("blob must exist")
            .blob_objects
            .get_mut(&object_id)
            .expect("blob object must exist");
        blob_data.end_epoch += additional_epochs;
    }

    /// Asserts that the blob data matches the expected data.
    pub fn assert_blob_data(&self, blob_id: BlobId, blob: &[u8]) {
        assert!(self.store_blob_data);
        let blob_data = self.blobs.get(&blob_id).expect("blob must exist");
        assert_eq!(blob_data.blob.as_ref().expect("blob must be stored"), blob);
    }

    /// Expire blobs that have expired at the given epoch.
    pub fn expire_blobs_in_new_epoch(&mut self, epoch: Epoch) {
        let mut blobs_to_remove = Vec::new();

        for (blob_id, blob_data) in self.blobs.iter_mut() {
            // Remove expired objects from this blob
            blob_data
                .blob_objects
                .retain(|_, blob_object| blob_object.end_epoch > epoch);

            // If no objects remain, mark the blob for removal
            if blob_data.blob_objects.is_empty() {
                blobs_to_remove.push(*blob_id);
            }
        }

        // Remove blobs that have no remaining objects
        for blob_id in blobs_to_remove {
            self.blobs.remove(&blob_id);
        }
    }

    /// Returns true if the blob pool is empty.
    pub fn is_empty(&self) -> bool {
        self.blobs.is_empty()
    }

    /// Returns true if the blob pool is full.
    pub fn is_full(&self) -> bool {
        self.blobs.len() >= self.max_blobs_in_pool
    }
}

#[cfg(test)]
mod tests {
    use rand::thread_rng;
    use walrus_core::{BlobId, SliverType};
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
        let mut pool = BlobPool::new(true, 1000);
        let blob_id = create_test_blob_id();
        let object_id = create_test_object_id();
        let blob_data = create_test_blob_data();

        let write_op = WalrusNodeClientOp::Write {
            blob: blob_data.clone(),
            deletable: true,
            store_epoch_ahead: 10,
        };

        pool.update_blob_pool(blob_id, Some(object_id), write_op);

        assert!(!pool.is_empty());
        assert_eq!(pool.blobs.len(), 1);
        assert!(pool.blobs.contains_key(&blob_id));

        let stored_blob = &pool.blobs[&blob_id];
        assert_eq!(stored_blob.blob, Some(blob_data));
        assert_eq!(stored_blob.blob_objects.keys().next().unwrap(), &object_id);
        assert!(stored_blob.blob_objects.values().next().unwrap().deletable);
        assert_eq!(
            stored_blob.blob_objects.values().next().unwrap().end_epoch,
            10
        );
    }

    #[test]
    fn test_update_blob_pool_delete_operation() {
        let mut pool = BlobPool::new(false, 1000);
        let blob_id = create_test_blob_id();
        let object_id = create_test_object_id();

        // First add a blob
        let write_op = WalrusNodeClientOp::Write {
            blob: create_test_blob_data(),
            deletable: true,
            store_epoch_ahead: 10,
        };
        pool.update_blob_pool(blob_id, Some(object_id), write_op);
        assert!(!pool.is_empty());

        // Then delete it
        let delete_op = WalrusNodeClientOp::Delete { blob_id, object_id };
        pool.update_blob_pool(blob_id, Some(object_id), delete_op);

        assert!(pool.is_empty());
        assert!(!pool.blobs.contains_key(&blob_id));
    }

    #[test]
    fn test_update_blob_pool_extend_operation() {
        let mut pool = BlobPool::new(false, 1000);
        let blob_id = create_test_blob_id();
        let object_id = create_test_object_id();

        // First add a blob
        let write_op = WalrusNodeClientOp::Write {
            blob: create_test_blob_data(),
            deletable: true,
            store_epoch_ahead: 10,
        };
        pool.update_blob_pool(blob_id, Some(object_id), write_op);

        // Then extend it
        let extend_op = WalrusNodeClientOp::Extend {
            blob_id,
            object_id,
            store_epoch_ahead: 5,
        };
        pool.update_blob_pool(blob_id, None, extend_op);

        let stored_blob = &pool.blobs[&blob_id];
        assert_eq!(
            stored_blob.blob_objects.values().next().unwrap().end_epoch,
            15
        ); // 10 + 5
    }

    #[test]
    fn test_update_blob_pool_read_operation() {
        let mut pool = BlobPool::new(false, 1000);
        let blob_id = create_test_blob_id();
        let object_id = create_test_object_id();

        // First add a blob
        let write_op = WalrusNodeClientOp::Write {
            blob: create_test_blob_data(),
            deletable: true,
            store_epoch_ahead: 10,
        };
        pool.update_blob_pool(blob_id, Some(object_id), write_op);

        // Read operation should not change anything
        let read_op = WalrusNodeClientOp::Read {
            blob_id,
            sliver_type: SliverType::Primary,
        };
        pool.update_blob_pool(blob_id, None, read_op);

        assert_eq!(pool.blobs.len(), 1);
        let stored_blob = &pool.blobs[&blob_id];
        assert_eq!(
            stored_blob.blob_objects.values().next().unwrap().end_epoch,
            10
        ); // Unchanged
    }

    #[test]
    fn test_assert_blob_data_success() {
        let mut pool = BlobPool::new(true, 1000);
        let blob_id = create_test_blob_id();
        let object_id = create_test_object_id();
        let blob_data = create_test_blob_data();

        let write_op = WalrusNodeClientOp::Write {
            blob: blob_data.clone(),
            deletable: true,
            store_epoch_ahead: 10,
        };
        pool.update_blob_pool(blob_id, Some(object_id), write_op);

        // Should not panic
        pool.assert_blob_data(blob_id, &blob_data);
    }

    #[test]
    #[should_panic]
    fn test_assert_blob_data_failure() {
        let mut pool = BlobPool::new(true, 1000);
        let blob_id = create_test_blob_id();
        let object_id = create_test_object_id();

        let write_op = WalrusNodeClientOp::Write {
            blob: create_test_blob_data(),
            deletable: true,
            store_epoch_ahead: 10,
        };
        pool.update_blob_pool(blob_id, Some(object_id), write_op);

        // Should panic because data doesn't match
        pool.assert_blob_data(blob_id, &[9, 8, 7]);
    }

    #[test]
    fn test_expire_blobs_in_new_epoch() {
        let mut pool = BlobPool::new(false, 1000);

        // Add blobs with different expiration epochs
        let blob_id1 = BlobId([1; 32]);
        let blob_id2 = BlobId([2; 32]);
        let blob_id3 = BlobId([3; 32]);
        let object_id = create_test_object_id();

        let write_op1 = WalrusNodeClientOp::Write {
            blob: vec![1],
            deletable: true,
            store_epoch_ahead: 5,
        };
        let write_op2 = WalrusNodeClientOp::Write {
            blob: vec![2],
            deletable: true,
            store_epoch_ahead: 10,
        };
        let write_op3 = WalrusNodeClientOp::Write {
            blob: vec![3],
            deletable: true,
            store_epoch_ahead: 15,
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
        let mut pool = BlobPool::new(false, 1000);
        let mut rng = thread_rng();

        // Add deletable blob
        let deletable_blob_id = BlobId([1; 32]);
        let write_op1 = WalrusNodeClientOp::Write {
            blob: vec![1],
            deletable: true,
            store_epoch_ahead: 10,
        };
        let deletable_object_id = create_test_object_id();
        pool.update_blob_pool(deletable_blob_id, Some(deletable_object_id), write_op1);

        // Add non-deletable blob
        let permanent_blob_id = BlobId([2; 32]);
        let write_op2 = WalrusNodeClientOp::Write {
            blob: vec![2],
            deletable: false,
            store_epoch_ahead: 10,
        };
        pool.update_blob_pool(permanent_blob_id, Some(create_test_object_id()), write_op2);

        assert_eq!(pool.blobs.len(), 2);

        // Random blob selection should return either blob
        let selected = pool.select_random_blob_id(&mut rng);
        assert!(selected == Some(deletable_blob_id) || selected == Some(permanent_blob_id));

        // Random deletable blob selection should only return the deletable one
        assert_eq!(
            pool.select_random_deletable_blob_id(&mut rng),
            Some((deletable_blob_id, deletable_object_id))
        );
    }

    #[test]
    fn test_expire_blob_objects_independently() {
        let mut pool = BlobPool::new(false, 1000);
        let blob_id = BlobId([1; 32]);

        // Manually create a blob with multiple objects having different expiration epochs
        let object_id1 = ObjectID::new([1; 32]);
        let object_id2 = ObjectID::new([2; 32]);
        let object_id3 = ObjectID::new([3; 32]);

        let mut blob_objects = HashMap::new();
        blob_objects.insert(
            object_id1,
            BlobObjectInfo {
                deletable: true,
                end_epoch: 5, // Expires at epoch 5
            },
        );
        blob_objects.insert(
            object_id2,
            BlobObjectInfo {
                deletable: false,
                end_epoch: 10, // Expires at epoch 10
            },
        );
        blob_objects.insert(
            object_id3,
            BlobObjectInfo {
                deletable: true,
                end_epoch: 15, // Expires at epoch 15
            },
        );

        pool.blobs.insert(
            blob_id,
            BlobDataAndInfo {
                blob: None,
                blob_objects,
            },
        );

        // Verify initial state
        assert_eq!(pool.blobs.len(), 1);
        assert_eq!(pool.blobs[&blob_id].blob_objects.len(), 3);

        // Expire at epoch 5 - should remove object_id1
        pool.expire_blobs_in_new_epoch(5);
        assert_eq!(pool.blobs.len(), 1); // Blob still exists
        assert_eq!(pool.blobs[&blob_id].blob_objects.len(), 2); // One object removed
        assert!(!pool.blobs[&blob_id].blob_objects.contains_key(&object_id1));
        assert!(pool.blobs[&blob_id].blob_objects.contains_key(&object_id2));
        assert!(pool.blobs[&blob_id].blob_objects.contains_key(&object_id3));

        // Expire at epoch 10 - should remove object_id2
        pool.expire_blobs_in_new_epoch(10);
        assert_eq!(pool.blobs.len(), 1); // Blob still exists
        assert_eq!(pool.blobs[&blob_id].blob_objects.len(), 1); // Another object removed
        assert!(!pool.blobs[&blob_id].blob_objects.contains_key(&object_id1));
        assert!(!pool.blobs[&blob_id].blob_objects.contains_key(&object_id2));
        assert!(pool.blobs[&blob_id].blob_objects.contains_key(&object_id3));

        // Expire at epoch 15 - should remove object_id3 and the entire blob
        pool.expire_blobs_in_new_epoch(15);
        assert_eq!(pool.blobs.len(), 0); // Blob removed when all objects expire
    }
}

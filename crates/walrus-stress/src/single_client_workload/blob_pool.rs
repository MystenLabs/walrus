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
        let blob_data = self.blobs.get_mut(&blob_id).unwrap();
        blob_data.end_epoch += additional_epochs;
    }

    pub fn assert_blob_data(&self, blob_id: BlobId, blob: &[u8]) {
        let blob_data = self.blobs.get(&blob_id).unwrap();
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
}

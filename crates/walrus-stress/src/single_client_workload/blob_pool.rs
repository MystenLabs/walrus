// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Blob pool.

use std::collections::HashMap;

use rand::{Rng, seq::IteratorRandom};
use walrus_core::{BlobId, Epoch};

use super::client_op_generator::WalrusClientOp;

pub(crate) struct BlobDataAndInfo {
    blob: Vec<u8>,
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

    pub fn update_blob_pool(&mut self, op: WalrusClientOp) {
        unimplemented!()
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
}

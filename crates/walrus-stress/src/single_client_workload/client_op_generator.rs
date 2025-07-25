// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Client operation generator.

use rand::Rng;
use walrus_core::{BlobId, EpochCount, SliverType};
use walrus_sdk::ObjectID;

use super::{
    blob_generator::BlobGenerator,
    blob_pool::BlobPool,
    epoch_length_generator::EpochLengthGenerator,
    single_client_workload_config::{
        RequestType,
        RequestTypeDistributionConfig,
        SizeDistributionConfig,
        StoreLengthDistributionConfig,
    },
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WalrusClientOp {
    Read {
        blob_id: BlobId,
        sliver_type: SliverType,
    },
    Write {
        blob: Vec<u8>,
        deletable: bool,
        store_epoch_ahead: EpochCount,
    },
    Delete {
        blob_id: BlobId,
    },
    Extend {
        blob_id: BlobId,
        object_id: ObjectID,
        store_epoch_ahead: EpochCount,
    },
}

pub(crate) struct ClientOpGenerator {
    request_type_distribution: RequestTypeDistributionConfig,
    blob_generator: BlobGenerator,
    epoch_length_generator: EpochLengthGenerator,
}

impl ClientOpGenerator {
    pub fn new(
        request_type_distribution: RequestTypeDistributionConfig,
        size_distribution: SizeDistributionConfig,
        store_length_distribution: StoreLengthDistributionConfig,
    ) -> Self {
        let blob_generator = BlobGenerator::new(size_distribution);
        let epoch_length_generator = EpochLengthGenerator::new(store_length_distribution);
        Self {
            request_type_distribution,
            blob_generator,
            epoch_length_generator,
        }
    }

    pub fn generate_client_op<R: Rng>(&self, blob_pool: &BlobPool, rng: &mut R) -> WalrusClientOp {
        let request_type = self.request_type_distribution.sample(rng);
        match request_type {
            RequestType::Read => {
                if !blob_pool.is_empty() {
                    self.generate_read_op(blob_pool, rng)
                } else {
                    self.generate_write_op(false, rng)
                }
            }
            RequestType::WritePermanent => self.generate_write_op(false, rng),
            RequestType::WriteDeletable => self.generate_write_op(true, rng),
            RequestType::Delete => self.generate_delete_op(blob_pool, rng),
            RequestType::Extend => self.generate_extend_op(blob_pool, rng),
        }
    }

    fn generate_read_op<R: Rng>(&self, blob_pool: &BlobPool, rng: &mut R) -> WalrusClientOp {
        let blob_id = blob_pool
            .select_random_blob_id(rng)
            .expect("blob must exist");
        let sliver_type = if rng.gen_bool(0.5) {
            SliverType::Primary
        } else {
            SliverType::Secondary
        };
        WalrusClientOp::Read {
            blob_id,
            sliver_type,
        }
    }

    // TODO(WAL-946): generate write to existing blob.
    fn generate_write_op<R: Rng>(&self, deletable: bool, rng: &mut R) -> WalrusClientOp {
        let blob = self.blob_generator.generate_blob(rng);
        let store_epoch_ahead = self.epoch_length_generator.generate_epoch_length(rng);
        WalrusClientOp::Write {
            blob,
            deletable,
            store_epoch_ahead,
        }
    }

    fn generate_delete_op<R: Rng>(&self, blob_pool: &BlobPool, rng: &mut R) -> WalrusClientOp {
        let blob_id = blob_pool
            .select_random_deletable_blob_id(rng)
            .expect("deletable blob must exist");
        WalrusClientOp::Delete { blob_id }
    }

    fn generate_extend_op<R: Rng>(&self, blob_pool: &BlobPool, rng: &mut R) -> WalrusClientOp {
        let blob_id = blob_pool
            .select_random_blob_id(rng)
            .expect("blob must exist");
        let store_epoch_ahead = self.epoch_length_generator.generate_epoch_length(rng);
        WalrusClientOp::Extend {
            blob_id,
            object_id: blob_pool
                .get_blob_object_id(blob_id)
                .expect("blob should exist in the blob pool"),
            store_epoch_ahead,
        }
    }
}

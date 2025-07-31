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
            RequestType::WritePermanent => {
                if blob_pool.is_full() {
                    tracing::info!("pool is full, generating read op instead of write permanent");
                    self.generate_read_op(blob_pool, rng)
                } else {
                    self.generate_write_op(false, rng)
                }
            }
            RequestType::WriteDeletable => {
                if blob_pool.is_full() {
                    tracing::info!("pool is full, generating read op instead of write deletable");
                    self.generate_read_op(blob_pool, rng)
                } else {
                    self.generate_write_op(true, rng)
                }
            }
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

#[cfg(test)]
mod tests {
    use rand::{SeedableRng, rngs::StdRng};
    use walrus_core::BlobId;
    use walrus_sdk::ObjectID;

    use super::*;
    use crate::single_client_workload::{
        blob_pool::BlobPool,
        single_client_workload_config::{
            RequestTypeDistributionConfig,
            SizeDistributionConfig,
            StoreLengthDistributionConfig,
        },
    };

    fn create_test_blob_id() -> BlobId {
        BlobId([1; 32])
    }

    fn create_test_object_id() -> ObjectID {
        ObjectID::new([2; 32])
    }

    fn create_test_blob_data() -> Vec<u8> {
        vec![1, 2, 3, 4, 5]
    }

    fn create_full_blob_pool() -> BlobPool {
        let mut pool = BlobPool::new(true, 1); // max_blobs_in_pool = 1
        let blob_id = create_test_blob_id();
        let object_id = create_test_object_id();
        let blob_data = create_test_blob_data();

        let write_op = WalrusClientOp::Write {
            blob: blob_data,
            deletable: true,
            store_epoch_ahead: 10,
        };

        pool.update_blob_pool(blob_id, Some(object_id), write_op);
        assert!(pool.is_full());
        pool
    }

    fn create_client_op_generator_favoring_writes() -> ClientOpGenerator {
        let request_type_distribution = RequestTypeDistributionConfig {
            read_weight: 1,
            write_permanent_weight: 100, // Heavy weight for write permanent
            write_deletable_weight: 100, // Heavy weight for write deletable
            delete_weight: 1,
            extend_weight: 1,
        };

        let size_distribution = SizeDistributionConfig::Uniform {
            min_size_bytes: 10,
            max_size_bytes: 100,
        };

        let store_length_distribution = StoreLengthDistributionConfig::Uniform {
            min_epochs: 1,
            max_epochs: 10,
        };

        ClientOpGenerator::new(
            request_type_distribution,
            size_distribution,
            store_length_distribution,
        )
    }

    #[test]
    fn test_no_write_ops_when_blob_pool_is_full() {
        let generator = create_client_op_generator_favoring_writes();
        let blob_pool = create_full_blob_pool();
        let mut rng = StdRng::seed_from_u64(12345);

        // Generate many operations to test probabilistically
        // With the high weights for write operations, we should see writes if the pool wasn't full
        for _ in 0..100 {
            let op = generator.generate_client_op(&blob_pool, &mut rng);

            // When pool is full, WritePermanent and WriteDeletable should become Read operations
            // Only Read, Delete, and Extend operations should be generated
            match op {
                WalrusClientOp::Write { .. } => {
                    panic!("Write operation generated when blob pool is full");
                }
                WalrusClientOp::Read { .. } => {
                    // This is expected when pool is full and WritePermanent/WriteDeletable are
                    // sampled.
                }
                WalrusClientOp::Delete { .. } => {
                    // This is fine - delete operations are still allowed when pool is full
                }
                WalrusClientOp::Extend { .. } => {
                    // This is fine - extend operations are still allowed when pool is full
                }
            }
        }
    }
}

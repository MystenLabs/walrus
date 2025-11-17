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
pub(crate) enum WalrusNodeClientOp {
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
        object_id: ObjectID,
    },
    Extend {
        blob_id: BlobId,
        object_id: ObjectID,
        store_epoch_ahead: EpochCount,
    },
    None,
}

pub(crate) struct ClientOpGenerator {
    request_type_distribution: RequestTypeDistributionConfig,
    blob_generator: BlobGenerator,
    epoch_length_generator: EpochLengthGenerator,
    write_same_data_ratio: f64,
}

impl ClientOpGenerator {
    pub fn new(
        request_type_distribution: RequestTypeDistributionConfig,
        size_distribution: SizeDistributionConfig,
        store_length_distribution: StoreLengthDistributionConfig,
        write_same_data_ratio: f64,
    ) -> Self {
        let blob_generator = BlobGenerator::new(size_distribution);
        let epoch_length_generator = EpochLengthGenerator::new(store_length_distribution);
        Self {
            request_type_distribution,
            blob_generator,
            epoch_length_generator,
            write_same_data_ratio,
        }
    }

    pub fn generate_client_op<R: Rng>(
        &self,
        blob_pool: &BlobPool,
        rng: &mut R,
    ) -> WalrusNodeClientOp {
        let request_type = self.request_type_distribution.sample(rng);
        match request_type {
            RequestType::Read => self.generate_read_op(blob_pool, rng),
            RequestType::WritePermanent => self.generate_write_op(blob_pool, false, rng),
            RequestType::WriteDeletable => self.generate_write_op(blob_pool, true, rng),
            RequestType::Delete => self.generate_delete_op(blob_pool, rng),
            RequestType::Extend => self.generate_extend_op(blob_pool, rng),
        }
    }

    fn generate_read_op<R: Rng>(&self, blob_pool: &BlobPool, rng: &mut R) -> WalrusNodeClientOp {
        let blob_id = blob_pool.select_random_blob_id(rng);
        if blob_id.is_none() {
            tracing::info!("no blob found, generating none op");
            return WalrusNodeClientOp::None;
        }

        let blob_id = blob_id.expect("blob must exist");
        let sliver_type = if rng.gen_bool(0.5) {
            SliverType::Primary
        } else {
            SliverType::Secondary
        };
        WalrusNodeClientOp::Read {
            blob_id,
            sliver_type,
        }
    }

    fn generate_write_op<R: Rng>(
        &self,
        blob_pool: &BlobPool,
        deletable: bool,
        rng: &mut R,
    ) -> WalrusNodeClientOp {
        if rng.gen_bool(self.write_same_data_ratio) {
            // Select a random blob from the pool to write again.
            let blob = blob_pool.select_random_blob_data(rng);
            if blob.is_none() {
                tracing::info!("no blob found, generating none op");
                return WalrusNodeClientOp::None;
            }
            let blob = blob.expect("blob must exist");
            let store_epoch_ahead = self.epoch_length_generator.generate_epoch_length(rng);
            WalrusNodeClientOp::Write {
                blob,
                deletable,
                store_epoch_ahead,
            }
        } else {
            if blob_pool.is_full() {
                tracing::info!(
                    "pool is full, generating none op instead of write, deletable: {}, \
                    pool size: {}",
                    deletable,
                    blob_pool.size(),
                );
                return WalrusNodeClientOp::None;
            }

            let blob = self.blob_generator.generate_blob(rng);
            let store_epoch_ahead = self.epoch_length_generator.generate_epoch_length(rng);
            WalrusNodeClientOp::Write {
                blob,
                deletable,
                store_epoch_ahead,
            }
        }
    }

    /// Generates a write operation for pool initialization. The blob lifetime follows the same
    /// distribution as the input parameters.
    ///
    /// This creates permanent blobs that will persist for read-heavy workloads.
    pub(crate) fn generate_write_op_for_pool_initialization<R: Rng>(
        &self,
        rng: &mut R,
    ) -> WalrusNodeClientOp {
        let blob = self.blob_generator.generate_blob(rng);
        let store_epoch_ahead = self.epoch_length_generator.generate_epoch_length(rng);

        // Generate write operation with half chance of being deletable.
        WalrusNodeClientOp::Write {
            blob,
            deletable: rng.gen_bool(0.5),
            store_epoch_ahead,
        }
    }

    fn generate_delete_op<R: Rng>(&self, blob_pool: &BlobPool, rng: &mut R) -> WalrusNodeClientOp {
        let result = blob_pool.select_random_deletable_blob_id(rng);
        if let Some((blob_id, object_id)) = result {
            WalrusNodeClientOp::Delete { blob_id, object_id }
        } else {
            tracing::info!("no deletable blob found, generating none op");
            WalrusNodeClientOp::None
        }
    }

    fn generate_extend_op<R: Rng>(&self, blob_pool: &BlobPool, rng: &mut R) -> WalrusNodeClientOp {
        let blob_id = blob_pool.select_random_blob_id(rng);
        if let Some(blob_id) = blob_id {
            let store_epoch_ahead = self.epoch_length_generator.generate_epoch_length(rng);
            WalrusNodeClientOp::Extend {
                blob_id,
                object_id: blob_pool
                    .select_random_blob_object_id(blob_id, rng)
                    .expect("blob should exist in the blob pool"),
                store_epoch_ahead,
            }
        } else {
            tracing::info!("no blob found, generating none op");
            WalrusNodeClientOp::None
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

        let write_op = WalrusNodeClientOp::Write {
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
            0.0, // write_same_data_ratio
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
                WalrusNodeClientOp::Write { .. } => {
                    panic!("Write operation generated when blob pool is full");
                }
                WalrusNodeClientOp::Read { .. } => {
                    // This is expected when pool is full and WritePermanent/WriteDeletable are
                    // sampled.
                }
                WalrusNodeClientOp::Delete { .. } => {
                    // This is fine - delete operations are still allowed when pool is full
                }
                WalrusNodeClientOp::Extend { .. } => {
                    // This is fine - extend operations are still allowed when pool is full
                }
                WalrusNodeClientOp::None => {
                    // This is fine - none operations are still allowed when pool is full
                }
            }
        }
    }

    #[test]
    fn test_generate_write_op_with_same_data() {
        // Create a blob pool with stored blob data
        let mut pool = BlobPool::new(true, 100);
        let mut rng = StdRng::seed_from_u64(12345);

        // Add multiple blobs to the pool with different data
        let blob_data_1 = vec![1, 2, 3, 4, 5];
        let blob_data_2 = vec![6, 7, 8, 9, 10];
        let blob_data_3 = vec![11, 12, 13, 14, 15];

        let blob_id_1 = BlobId([1; 32]);
        let blob_id_2 = BlobId([2; 32]);
        let blob_id_3 = BlobId([3; 32]);

        let object_id_1 = ObjectID::new([1; 32]);
        let object_id_2 = ObjectID::new([2; 32]);
        let object_id_3 = ObjectID::new([3; 32]);

        pool.update_blob_pool(
            blob_id_1,
            Some(object_id_1),
            WalrusNodeClientOp::Write {
                blob: blob_data_1.clone(),
                deletable: true,
                store_epoch_ahead: 10,
            },
        );
        pool.update_blob_pool(
            blob_id_2,
            Some(object_id_2),
            WalrusNodeClientOp::Write {
                blob: blob_data_2.clone(),
                deletable: true,
                store_epoch_ahead: 10,
            },
        );
        pool.update_blob_pool(
            blob_id_3,
            Some(object_id_3),
            WalrusNodeClientOp::Write {
                blob: blob_data_3.clone(),
                deletable: true,
                store_epoch_ahead: 10,
            },
        );

        // Create a generator with 100% write_same_data_ratio
        let request_type_distribution = RequestTypeDistributionConfig {
            read_weight: 0,
            write_permanent_weight: 1,
            write_deletable_weight: 0,
            delete_weight: 0,
            extend_weight: 0,
        };

        let size_distribution = SizeDistributionConfig::Uniform {
            min_size_bytes: 10,
            max_size_bytes: 100,
        };

        let store_length_distribution = StoreLengthDistributionConfig::Uniform {
            min_epochs: 1,
            max_epochs: 10,
        };

        let generator = ClientOpGenerator::new(
            request_type_distribution,
            size_distribution,
            store_length_distribution,
            1.0, // 100% write_same_data_ratio
        );

        // Generate multiple write operations and verify they all use existing blob data
        let expected_blobs = [blob_data_1, blob_data_2, blob_data_3];

        for _ in 0..20 {
            let op = generator.generate_client_op(&pool, &mut rng);
            match op {
                WalrusNodeClientOp::Write { blob, .. } => {
                    // Verify that the blob data matches one of the existing blobs in the pool
                    assert!(
                        expected_blobs.contains(&blob),
                        "Generated write op should contain data from existing blobs in pool"
                    );
                }
                _ => panic!("Expected Write operation"),
            }
        }
    }
}

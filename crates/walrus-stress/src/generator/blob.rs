// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use rand::{Rng, SeedableRng, rngs::StdRng, thread_rng};
use walrus_core::{encoding::QuiltStoreBlob, EpochCount};
use walrus_test_utils::generate_random_data;

const TAG: &[u8] = b"TESTBLOB";

#[derive(Debug, Clone)]
pub struct WriteBlobConfig {
    min_size_log2: u8,
    max_size_log2: u8,
    min_epochs_to_store: EpochCount,
    max_epochs_to_store: EpochCount,
}

impl WriteBlobConfig {
    pub fn new(
        min_size_log2: u8,
        max_size_log2: u8,
        min_epochs_to_store: EpochCount,
        max_epochs_to_store: EpochCount,
    ) -> Self {
        Self {
            min_size_log2,
            max_size_log2,
            min_epochs_to_store,
            max_epochs_to_store,
        }
    }

    /// Returns a random number of epochs to store between `min_epochs_to_store` and
    /// `max_epochs_to_store`.
    pub fn get_random_epochs_to_store(&self) -> EpochCount {
        thread_rng().gen_range(self.min_epochs_to_store..=self.max_epochs_to_store)
    }
}

#[derive(Debug)]
pub(crate) struct BlobData {
    bytes: Vec<u8>,
    rng: StdRng,
    epochs_to_store: EpochCount,
    config: WriteBlobConfig,
}

impl BlobData {
    /// Create a random blob of a given size.
    pub async fn random(mut rng: StdRng, config: WriteBlobConfig) -> Self {
        let mut new_rng = StdRng::from_seed(rng.r#gen());
        let size = 2_usize.pow(config.max_size_log2 as u32);
        let n_additional_bytes = size - TAG.len();
        let bytes = tokio::spawn(async move {
            TAG.iter()
                .cloned()
                .chain((0..n_additional_bytes).map(|_| new_rng.r#gen::<u8>()))
                .collect()
        })
        .await
        .expect("should be able to join spawned task");

        Self {
            bytes,
            rng,
            epochs_to_store: config.get_random_epochs_to_store(),
            config,
        }
    }

    /// Returns the number of epochs to store the blob for.
    pub fn epochs_to_store(&self) -> EpochCount {
        self.epochs_to_store
    }

    /// Changes the blob by incrementing (wrapping) a randomly chosen byte excluding
    /// the `TESTBLOB` tag at the start of the blob data.
    ///
    /// Also updates the epoch to store to a new random value.
    pub fn refresh(&mut self) {
        let index = self.rng.gen_range(TAG.len()..self.bytes.len());
        self.bytes[index] = self.bytes[index].wrapping_add(1);
        self.epochs_to_store = self.config.get_random_epochs_to_store();
    }

    /// Returns a slice of the blob with a size `2^x`, where `x` is chosen uniformly at random
    /// between `min_size_log2` and `max_size_log2`.
    pub fn random_size_slice(&self) -> &[u8] {
        let blob_size_min = 2_usize.pow(self.config.min_size_log2 as u32);
        let blob_size_max = 2_usize.pow(self.config.max_size_log2 as u32);
        let blob_size = thread_rng().gen_range(blob_size_min..=blob_size_max);
        &self.bytes[..blob_size]
    }
}

impl AsRef<[u8]> for BlobData {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

#[derive(Debug, Clone)]
pub struct QuiltStoreBlobConfig {
    pub min_num_blobs_per_quilt: u16,
    pub max_num_blobs_per_quilt: u16,
}

impl QuiltStoreBlobConfig {
    pub fn new(
        min_num_blobs_per_quilt: u16,
        max_num_blobs_per_quilt: u16,
    ) -> Self {
        Self {
            min_num_blobs_per_quilt: min_num_blobs_per_quilt.min(max_num_blobs_per_quilt),
            max_num_blobs_per_quilt,
        }
    }

    /// Returns a random number of blobs to store in a quilt between `min_num_blobs_per_quilt` and
    /// `max_num_blobs_per_quilt`.
    pub fn get_random_num_blobs_per_quilt(&self) -> u16 {
        thread_rng().gen_range(self.min_num_blobs_per_quilt..=self.max_num_blobs_per_quilt)
    }
}

pub(crate) struct QuiltData {
    blobs: Vec<Vec<u8>>,
    quilt_config: QuiltStoreBlobConfig,
    blob_config: WriteBlobConfig,
}

impl QuiltData {
    pub fn new(quilt_config: QuiltStoreBlobConfig, blob_config: WriteBlobConfig) -> Self {
        let num_blobs = quilt_config.max_num_blobs_per_quilt;
        let min_size = 2_usize.pow(blob_config.min_size_log2 as u32);
        let max_size = 2_usize.pow(blob_config.max_size_log2 as u32);
        let blobs = generate_random_data(num_blobs as usize, max_size, min_size);

        Self {
            blobs,
            quilt_config,
            blob_config,
        }
    }

    pub fn get_random_batch(&self) -> Vec<&[u8]> {
        let num_blobs = self.quilt_config.get_random_num_blobs_per_quilt();
        let mut rng = thread_rng();
        let mut blobs = Vec::new();

        for _ in 0..num_blobs {
            blobs.push(self.blobs[rng.gen_range(0..self.blobs.len())].as_slice());
        }
        blobs
    }
}
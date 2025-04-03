// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use rand::{rngs::StdRng, thread_rng, Rng, SeedableRng};
use walrus_core::EpochCount;

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
        let mut new_rng = StdRng::from_seed(rng.gen());
        let size = 2_usize.pow(config.max_size_log2 as u32);
        let n_additional_bytes = size - TAG.len();
        let bytes = tokio::spawn(async move {
            TAG.iter()
                .cloned()
                .chain((0..n_additional_bytes).map(|_| new_rng.gen::<u8>()))
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

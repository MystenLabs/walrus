// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use rand::{Rng, SeedableRng, rngs::StdRng, thread_rng};
use walrus_core::EpochCount;

const TAG: &[u8] = b"TESTBLOB";

#[derive(Debug, Clone)]
pub struct WriteBlobConfig {
    min_size_log2: u8,
    max_size_log2: u8,
    min_epochs_to_store: EpochCount,
    max_epochs_to_store: EpochCount,
    fixed_size_bytes: Option<usize>,
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
            fixed_size_bytes: None,
        }
    }

    /// Override the logarithmic bounds with an exact size, including the test tag.
    pub fn with_fixed_size_bytes(mut self, size: usize) -> anyhow::Result<Self> {
        anyhow::ensure!(
            size > TAG.len(),
            "blob size must exceed {} bytes",
            TAG.len()
        );
        self.fixed_size_bytes = Some(size);
        Ok(self)
    }

    fn size_bounds(&self) -> (usize, usize) {
        match self.fixed_size_bytes {
            Some(size) => (size, size),
            None => (
                2_usize.pow(u32::from(self.min_size_log2)),
                2_usize.pow(u32::from(self.max_size_log2)),
            ),
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
        let (_, size) = config.size_bounds();
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

    /// Returns a slice with a uniformly sampled byte length within the configured bounds,
    /// or the exact requested size when a fixed size is configured.
    pub fn random_size_slice(&self) -> Vec<u8> {
        let (blob_size_min, blob_size_max) = self.config.size_bounds();
        let blob_size = thread_rng().gen_range(blob_size_min..=blob_size_max);
        self.bytes[..blob_size].to_vec()
    }
}

impl AsRef<[u8]> for BlobData {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn exact_blob_size_survives_refresh() {
        let size = 5 * 1024 * 1024;
        let config = WriteBlobConfig::new(10, 20, 1, 10)
            .with_fixed_size_bytes(size)
            .unwrap();
        let mut blob = BlobData::random(StdRng::seed_from_u64(42), config).await;
        let original = blob.random_size_slice();
        assert_eq!(original.len(), size);
        blob.refresh();
        let refreshed = blob.random_size_slice();
        assert_eq!(refreshed.len(), size);
        assert_eq!(&refreshed[..TAG.len()], TAG);
        assert_ne!(original, refreshed);
        assert!((1..=10).contains(&blob.epochs_to_store()));
    }

    #[test]
    fn fixed_size_requires_room_to_refresh() {
        for size in [0, TAG.len() - 1, TAG.len()] {
            assert!(
                WriteBlobConfig::new(10, 20, 1, 10)
                    .with_fixed_size_bytes(size)
                    .is_err()
            );
        }
        assert_eq!(
            WriteBlobConfig::new(10, 20, 1, 10).size_bounds(),
            (1024, 1048576)
        );
    }
}

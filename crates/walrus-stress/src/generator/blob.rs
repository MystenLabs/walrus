// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use rand::{rngs::StdRng, thread_rng, Rng, SeedableRng};

const TAG: &[u8] = b"TESTBLOB";

#[derive(Debug)]
pub(crate) struct BlobData {
    bytes: Vec<u8>,
    rng: StdRng,
    min_size_log2: u8,
    max_size_log2: u8,
}

impl BlobData {
    /// Create a random blob of a given size.
    pub async fn random(mut rng: StdRng, min_size_log2: u8, max_size_log2: u8) -> Self {
        let mut new_rng = StdRng::from_seed(rng.gen());
        let size = 2_usize.pow(max_size_log2 as u32);
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
            min_size_log2,
            max_size_log2,
        }
    }

    /// Changes the blob by incrementing (wrapping) a randomly chosen byte excluding
    /// the `TESTBLOB` tag at the start of the blob data.
    pub fn refresh(&mut self) {
        let index = self.rng.gen_range(TAG.len()..self.bytes.len());
        self.bytes[index] = self.bytes[index].wrapping_add(1);
    }

    /// Returns a slice of the blob with a size `2^x`, where `x` is chosen uniformly at random
    /// between `min_size_log2` and `max_size_log2`.
    pub fn random_size_slice(&self) -> &[u8] {
        let blob_size_log2 = thread_rng().gen_range(self.min_size_log2..=self.max_size_log2);
        let blob_size = 2_usize.pow(blob_size_log2 as u32);
        &self.bytes[..blob_size]
    }

    pub fn refresh_and_get_random_slice(&mut self) -> &[u8] {
        self.refresh();
        self.random_size_slice()
    }
}

impl AsRef<[u8]> for BlobData {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

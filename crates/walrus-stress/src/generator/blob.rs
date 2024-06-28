// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use rand::{rngs::StdRng, Rng, SeedableRng};

#[derive(Debug)]
pub(crate) struct BlobData {
    bytes: Vec<u8>,
    rng: StdRng,
}

impl BlobData {
    /// Create a random blob of a given size.
    pub async fn random(mut rng: StdRng, size: usize) -> Self {
        let mut new_rng = StdRng::from_seed(rng.gen());
        let bytes = tokio::spawn(async move { (0..size).map(|_| new_rng.gen::<u8>()).collect() })
            .await
            .expect("should be able to join spawned task");

        Self { bytes, rng }
    }

    /// Changes the blob by incrementing (wrapping) a randomly chosen byte.
    pub fn refresh(&mut self) {
        let index = self.rng.gen_range(0..self.bytes.len());
        self.bytes[index] = self.bytes[index].wrapping_add(1);
    }
}

impl AsRef<[u8]> for BlobData {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

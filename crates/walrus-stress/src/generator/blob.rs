// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use rand::{rngs::StdRng, Rng};

#[derive(Debug)]
pub(crate) struct BlobData {
    bytes: Vec<u8>,
    rng: StdRng,
}

impl BlobData {
    /// Create a random blob of a given size.
    pub fn random(mut rng: StdRng, size: usize) -> Self {
        Self {
            bytes: (0..size).map(|_| rng.gen::<u8>()).collect(),
            rng,
        }
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

// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;
use std::num::NonZeroU16;

use crate::client::Client;
use walrus_sui::client::SuiContractClient;
use walrus_core::encoding::{BlobDecoder, BlobEncoder, EncodingConfig, SliverPair};
use walrus_core::metadata::VerifiedBlobMetadataWithId;

// use super::error::QuiltError;
use serde::{Serialize, Deserialize};
use std::path::PathBuf;
use walrus_core::BlobId;

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct Quilt {
    quilt_blob_id: Option<BlobId>,
    blobs: Vec<(BlobId, u64)>,
    quilt_path: Option<PathBuf>,
}

#[derive(Debug)]
struct QuiltFactory {
    n_shards: NonZeroU16,
    encoding_config: EncodingConfig,
    client: Arc<Client<SuiContractClient>>,
    blobs: Vec<BlobId>,
}

impl QuiltFactory {
    pub fn new(n_shards: NonZeroU16, client: Arc<Client<SuiContractClient>>, blobs: &[BlobId]) -> Self {
        Self { n_shards, encoding_config: EncodingConfig::new(n_shards), client, blobs: blobs.to_vec() }
    }

    pub fn construct_quilt(&self) -> anyhow::Result<Quilt> {
        let quilt = Quilt::default();
        Ok(quilt)
    }

    pub fn encode(&self, blob: &[u8]) -> anyhow::Result<(Vec<SliverPair>, VerifiedBlobMetadataWithId)> {
        let encoder = BlobEncoder::new(&self.encoding_config, blob)?;
        let (sliver_pairs, metadata) = encoder.encode_with_metadata();
        Ok((sliver_pairs, metadata))
    }
}

/// Returns the next systematic size that can accommodate the given length.
///
/// The size is calculated based on the number of shards (`n_shards`). It uses an initial base
/// value computed from `n_shards` and then finds the smallest power of two multiplier such that
/// the product is at least `len`. If `len` is 0, returns 0.
pub(crate) fn next_systematic_size(n_shards: usize, len: usize) -> usize {
    // Base value is n_shards - (n_shards - 1) / 3.
    debug_assert!((n_shards - 1) % 3 == 0);
    let base = n_shards - (n_shards - 1) / 3;

    // If len is 0, return 0.
    if len == 0 {
        return 0;
    }

    // Find the smallest multiplier (power of 2) that gives at least len.
    let mut result = base;
    while result < len {
        result *= 2;
    }

    result
}

/// Constructs a quilt from a list of blob slices.
///
/// Each blob is padded to the next systematic size determined by the maximum blob length and
/// the number of shards (`n_shards`). The padded blobs are then concatenated into a single blob.
///
/// # Arguments
///
/// * `n_shards` - Number of shards used to calculate the systematic size.
/// * `blobs` - A slice of blob slices to be concatenated.
///
/// # Returns
///
/// A vector containing the concatenated quilt blob.
pub fn construct_quilt(n_shards: usize, blobs: &[&[u8]]) -> anyhow::Result<Vec<u8>> {
    // Validate input parameters
    if n_shards == 0 {
        return Err(anyhow::anyhow!("Invalid shard count"));
    }

    // Find the maximum length among all blobs.
    let max_len = blobs.iter().map(|blob| blob.len()).max().unwrap_or(0);

    // Get the next systematic size that can accommodate the max length.
    let target_len = next_systematic_size(n_shards, max_len);

    // Create a vector to store the concatenated quilt blob.
    let mut concatenated_blob = Vec::new();

    // Process each blob: pad it to the target length and append it.
    for blob in blobs {
        let mut padded = Vec::from(*blob);
        padded.resize(target_len, 0);  // Pad with zeros to target length.
        concatenated_blob.extend_from_slice(&padded);
    }

    Ok(concatenated_blob)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_next_systematic_size() {
        assert_eq!(next_systematic_size(13, 0), 0);
        assert_eq!(next_systematic_size(13, 100), 128);
        assert_eq!(next_systematic_size(13, 1000), 1024);
    }

    #[test]
    fn test_construct_quilt() {
        let blobs = vec![&[1, 2, 3][..], &[4, 5][..], &[6][..]];
        let result = construct_quilt(13, &blobs).unwrap();
        assert!(!result.is_empty());
    }
} 
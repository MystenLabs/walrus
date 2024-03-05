// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/// Trait for encoding and decoding blobs and slivers.
pub trait ErasureCode {
    /// Encode the instance into a vector of byte vectors.
    ///
    /// # Returns
    ///
    /// A vector of byte vectors representing the encoded instance.
    fn encode(&self) -> Vec<Vec<u8>>;

    /// Decode the given bytes into an instance of `Self`.
    ///
    /// # Arguments
    ///
    /// * `bytes` - The bytes to decode.
    ///
    /// # Returns
    ///
    /// An instance of `Self` decoded from the bytes.
    fn decode(bytes: &[Vec<u8>]) -> Self;
}

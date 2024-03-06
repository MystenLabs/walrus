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

/// Trait for data over which we can compute a commitment.
pub trait Metadata {
    /// The commitment to the data.
    type Commitment;
    /// The error type returner upon failure of the commitment verification.
    type Error;

    /// Return a commitment.
    fn commit(&self) -> Self::Commitment;

    /// Verify the the commitment .
    ///
    /// # Arguments
    ///
    /// * `metadata` - The metadata to verify against.
    ///
    /// # Returns
    ///
    /// An empty `Result` if the verification succeeds, otherwise an error of type `Self::Error`.
    fn verify(&self, commitment: &Self::Commitment) -> Result<(), Self::Error>;
}

// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The errors for the storage client and the communication with storage nodes.

use fastcrypto::error::FastCryptoError;
use reqwest::StatusCode;
use walrus_core::{
    encoding::{RecoveryError, WrongSliverVariantError},
    metadata::{SliverPairIndex, VerificationError as MetadataVerificationError},
    SliverType,
};

/// Errors returned during the communication with a storage node.
#[derive(Debug, thiserror::Error)]
pub enum NodeCommunicationError {
    /// Errors in sliver verification.
    #[error(transparent)]
    SliverVerificationFailed(#[from] SliverVerificationError),
    /// The sliver could not be stored on the node.
    #[error("the following slivers could not be stored on the node: {0:?}")]
    SliverStoreFailed(Vec<(SliverPairIndex, SliverType)>),
    /// The metadata could not be stored on the node.
    #[error("the metadata could not be stored on the node")]
    MetadataStoreFailed,
    /// Errors in the communication with the storage node.
    #[error(transparent)]
    ReqwestError(#[from] reqwest::Error),
    /// The service response received from the storage node contains an error.
    #[error(
        "the request to the node completed, but the service response was error {code}: {message}"
    )]
    ServiceResponseError { code: u16, message: String },
    /// The HTTP request completed, but returned an error code.
    #[error("the HTTP request to the node completed, but was not successful: {0}")]
    HttpInsuccess(StatusCode),
    /// The storage node sent the wrong sliver variant, wrt what was requested.
    #[error(transparent)]
    WrongSliverVariant(#[from] WrongSliverVariantError),
    /// The signature verification on the storage confirmation failed.
    #[error(transparent)]
    ConfirmationVerificationFailed(#[from] ConfirmationVerificationError),
    /// The metadata verification failed.
    #[error(transparent)]
    MetadataVerificationFailed(#[from] MetadataVerificationError),
}

/// Error returned when the client fails to verify a sliver fetched from a storage node.
#[derive(Debug, thiserror::Error)]
pub enum SliverVerificationError {
    /// The shard index provided is too large for the number of shards in the metadata.
    #[error("the shard index provided is too large for the number of shards in the metadata")]
    ShardIndexTooLarge,
    /// The number of hashes in the metadata does not match the number of shards.
    #[error("the number of hashes in the metadata does not match the number of shards")]
    WrongNumberOfHashes,
    /// The length of the provided sliver does not match the number of source symbols in the
    /// metadata.
    #[error("the length of the provided sliver does not match the metadata")]
    SliverSizeMismatch,
    /// The symbol size of the provided sliver does not match the symbol size that can be computed
    /// from the metadata.
    #[error("the symbol size of the provided sliver does not match the metadata")]
    SymbolSizeMismatch,
    /// The recomputed Merkle root of the provided sliver does not match the root stored in the
    /// metadata.
    #[error("the recomputed Merkle root of the provided sliver does not match the metadata")]
    MerkleRootMismatch,
    /// Error resulting from the Merkle tree computation. The Merkle root could not be computed.
    #[error(transparent)]
    RecoveryFailed(#[from] RecoveryError),
}

#[derive(Debug, thiserror::Error)]
pub enum ConfirmationVerificationError {
    /// The confirmation could not be deserialized from BCS bytes.
    #[error(transparent)]
    DeserializationFailed(#[from] bcs::Error),
    /// The storage confirmation is for the wrong blob ID or epoch.
    #[error("the storage confirmation is for the wrong blob ID or epoch")]
    EpochBlobIdMismatch,
    /// The signature verification on the storage confirmation failed.
    #[error(transparent)]
    SignatureVerification(#[from] FastCryptoError),
}

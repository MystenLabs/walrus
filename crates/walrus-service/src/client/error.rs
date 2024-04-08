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

/// Storing the metadata and the set of sliver pairs onto the storage node, and retrieving the
/// storage confirmation, failed.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// One ore more slivers could not be stored on the node
    #[error("one ore more slivers could not be stored on the node")]
    SliverStore(Vec<SliverStoreError>),
    /// The sliver could not be stored on the node.
    #[error(transparent)]
    MetadataStore(#[from] MetadataStoreError),
    /// A valid storage confirmation could not retrieved from the node.
    #[error(transparent)]
    ConfirmationRetrieve(#[from] ConfirmationRetrieveError),
}

/// The sliver could not be stored on the node.
#[derive(Debug, thiserror::Error)]
#[error("the sliver could not be stored on the node")]
pub struct SliverStoreError {
    pub pair_idx: SliverPairIndex,
    pub sliver_type: SliverType,
    pub error: CommunicationError,
}

/// The metadata could not be stored on the node.
#[derive(Debug, thiserror::Error)]
pub enum MetadataStoreError {
    /// The communication failed.
    #[error(transparent)]
    CommunicationFailed(#[from] CommunicationError),
}

/// The sliver could not be retrieved from the node.
#[derive(Debug, thiserror::Error)]
pub enum SliverRetrieveError {
    /// The communication failed.
    #[error(transparent)]
    CommunicationFailed(#[from] CommunicationError),
    /// There were errors in sliver verification.
    #[error(transparent)]
    SliverVerificationFailed(#[from] SliverVerificationError),
    /// The storage node sent the wrong sliver variant, wrt what was requested.
    #[error(transparent)]
    WrongSliverVariant(#[from] WrongSliverVariantError),
}

/// The metadata could not be retrieved from the node.
#[derive(Debug, thiserror::Error)]
pub enum MetadataRetrieveError {
    /// The communication failed.
    #[error(transparent)]
    CommunicationFailed(#[from] CommunicationError),
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
pub enum ConfirmationRetrieveError {
    /// The confirmation could not be deserialized from BCS bytes.
    #[error(transparent)]
    DeserializationFailed(#[from] bcs::Error),
    /// The storage confirmation is for the wrong blob ID or epoch.
    #[error("the storage confirmation is for the wrong blob ID or epoch")]
    EpochBlobIdMismatch,
    /// The signature verification on the storage confirmation failed.
    #[error(transparent)]
    SignatureVerification(#[from] FastCryptoError),
    /// The communication failed.
    #[error(transparent)]
    CommunicationFailed(#[from] CommunicationError),
}

/// Errors returned during the communication with a storage node.
#[derive(Debug, thiserror::Error)]
pub enum CommunicationError {
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
    HttpFailure(StatusCode),
}

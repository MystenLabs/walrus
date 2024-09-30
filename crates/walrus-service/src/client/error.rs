// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The errors for the storage client and the communication with storage nodes.

use walrus_core::{BlobId, SliverPairIndex, SliverType};
use walrus_sdk::error::{ClientBuildError, NodeError};
use walrus_sui::client::SuiClientError;

/// Storing the metadata and the set of sliver pairs onto the storage node, and retrieving the
/// storage confirmation, failed.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// The metadata could not be stored on the node.
    #[error("the metadata could not be stored")]
    Metadata(NodeError),
    /// One or more slivers could not be stored on the node.
    #[error(transparent)]
    SliverStore(#[from] SliverStoreError),
    /// A valid storage confirmation could not retrieved from the node.
    #[error("the storage confirmation could not be retrieved")]
    Confirmation(NodeError),
}

/// The sliver could not be stored on the node.
#[derive(Debug, thiserror::Error)]
#[error("the sliver could not be stored")]
pub struct SliverStoreError {
    pub pair_index: SliverPairIndex,
    pub sliver_type: SliverType,
    pub error: NodeError,
}

/// Error raised by a client interacting with the storage system.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct ClientError {
    /// The inner kind of the error.
    #[from]
    kind: ClientErrorKind,
}

impl ClientError {
    /// Returns the corresponding [`ClientErrorKind`] for this object.
    pub fn kind(&self) -> &ClientErrorKind {
        &self.kind
    }

    /// Converts an error to a [`ClientError`] with `kind` [`ClientErrorKind::Other`].
    pub fn other<E>(err: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        ClientError {
            kind: ClientErrorKind::Other(err.into()),
        }
    }

    /// Whether the error is an out-of-gas error.
    pub fn is_out_of_coin_error(&self) -> bool {
        matches!(
            &self.kind,
            ClientErrorKind::NoCompatiblePaymentCoin | ClientErrorKind::NoCompatibleGasCoins
        )
    }
}

impl From<SuiClientError> for ClientError {
    fn from(value: SuiClientError) -> Self {
        let kind = match value {
            SuiClientError::NoCompatiblePaymentCoin => ClientErrorKind::NoCompatiblePaymentCoin,
            SuiClientError::NoCompatibleGasCoins(_) => ClientErrorKind::NoCompatibleGasCoins,
            error => ClientErrorKind::Other(error.into()),
        };
        Self { kind }
    }
}

/// Inner error type, raised when the client operation fails.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub enum ClientErrorKind {
    /// The certification of the blob failed.
    #[error("blob certification failed: {0}")]
    CertificationFailed(SuiClientError),
    /// The client could not retrieve sufficient confirmations to certify the blob.
    #[error(
        "could not retrieve enough confirmations to certify the blob: {0} / {1} required; \
        this usually indicates a misconfiguration"
    )]
    NotEnoughConfirmations(usize, usize),
    /// The client could not retrieve enough slivers to reconstruct the blob.
    #[error("could not retrieve enough slivers to reconstruct the blob")]
    NotEnoughSlivers,
    /// The blob ID is not certified on Walrus.
    ///
    /// This is deduced because either:
    ///   - the client received enough "not found" messages to confirm that the blob ID does not
    ///     exist; or
    ///   - the client could not obtain the certification epoch of the blob by reading the events.
    #[error("the blob ID does not exist")]
    BlobIdDoesNotExist,
    /// The client could not retrieve the metadata from the storage nodes.
    ///
    /// This error differs from the [`ClientErrorKind::BlobIdDoesNotExist`] version in the fact that
    /// other errors occurred, and the client cannot confirm that the blob does not exist.
    #[error("could not retrieve the metadata from the storage nodes")]
    NoMetadataReceived,
    /// The client not receive a valid blob status from the quorum of nodes.
    #[error("did not receive a valid blob status from the quorum of nodes")]
    NoValidStatusReceived,
    /// The config provided to the client was invalid.
    #[error("the client config provided was invalid")]
    InvalidConfig,
    /// The blob ID is blocked.
    #[error("the blob ID {0} is blocked")]
    BlobIdBlocked(BlobId),
    /// No matching payment coin found for the transaction.
    #[error("no compatible payment coin found")]
    NoCompatiblePaymentCoin,
    /// No gas coins with sufficient balance found for the transaction.
    #[error("no compatible gas coins with sufficient total balance found")]
    NoCompatibleGasCoins,
    /// The client was unable to open connections to any storage node.
    #[error("connecting to all storage nodes failed: {0}")]
    AllConnectionsFailed(ClientBuildError),
    /// A failure internal to the node.
    #[error("client internal error: {0}")]
    Other(Box<dyn std::error::Error + Send + Sync + 'static>),
}

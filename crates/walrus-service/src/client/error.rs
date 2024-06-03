// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The errors for the storage client and the communication with storage nodes.

use walrus_core::{SliverPairIndex, SliverType};
use walrus_sdk::error::NodeError;
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
    pub(crate) fn other<E>(err: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        ClientError {
            kind: ClientErrorKind::Other(err.into()),
        }
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
    #[error("could not retrieve enough confirmations to certify the blob: {0} / {1} required")]
    NotEnoughConfirmations(usize, usize),
    /// The client could not retrieve enough slivers to reconstruct the blob.
    #[error("could not retrieve enough slivers to reconstruct the blob")]
    NotEnoughSlivers,
    /// The client received enough "not found" messages to confirm that the blob ID does not exist.
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
    /// A failure internal to the node.
    #[error("client internal error: {0}")]
    Other(Box<dyn std::error::Error + Send + Sync + 'static>),
}

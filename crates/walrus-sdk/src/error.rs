// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! The errors for the storage client and the communication with storage nodes.

use walrus_core::{BlobId, EncodingType, Epoch, SliverPairIndex, SliverType};
use walrus_rest_client::error::{ClientBuildError, NodeError};
use walrus_sui::client::{MIN_STAKING_THRESHOLD, SuiClientError};

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

/// The JWT secret could not be decoded from the provided string.
#[derive(Debug, thiserror::Error, PartialEq)]
#[error("the JWT secret could not be decoded from the provided string")]
pub struct JwtDecodeError;

/// The sliver could not be stored on the node.
#[derive(Debug, thiserror::Error)]
#[error("the sliver could not be stored")]
pub struct SliverStoreError {
    /// The sliver's pair index.
    pub pair_index: SliverPairIndex,
    /// The sliver's type.
    pub sliver_type: SliverType,
    /// The error raised by the node.
    pub error: NodeError,
}

/// A helper type for the client to handle errors.
pub type ClientResult<T> = Result<T, ClientError>;

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

    /// Constructs a [`ClientError`] with `kind` [`ClientErrorKind::StoreBlobInternal`].
    pub fn store_blob_internal(err: String) -> Self {
        ClientError {
            kind: ClientErrorKind::StoreBlobInternal(err),
        }
    }

    /// Whether the error is an out-of-gas error.
    pub fn is_out_of_coin_error(&self) -> bool {
        matches!(
            &self.kind,
            ClientErrorKind::NoCompatiblePaymentCoin | ClientErrorKind::NoCompatibleGasCoins(_)
        )
    }

    /// Returns `true` if the error is a `NoValidStatusReceived` error.
    pub fn is_no_valid_status_received(&self) -> bool {
        matches!(&self.kind, ClientErrorKind::NoValidStatusReceived)
    }

    /// Returns `true` if the error may have been caused by epoch change.
    pub fn may_be_caused_by_epoch_change(&self) -> bool {
        matches!(
            &self.kind,
            // Cannot get confirmations.
            ClientErrorKind::NotEnoughConfirmations(_, _)
                // Cannot certify the blob on chain.
                | ClientErrorKind::CertificationFailed(_)
                // Cannot get the correct read epoch during epoch change.
                | ClientErrorKind::BehindCurrentEpoch { .. }
                // Cannot get metadata because we are behind by several epochs.
                | ClientErrorKind::NoMetadataReceived
                // Cannot get slivers because we are behind by several epochs.
                | ClientErrorKind::NotEnoughSlivers
                // The client was notified that the committee has changed.
                | ClientErrorKind::CommitteeChangeNotified
        )
    }
}

impl From<SuiClientError> for ClientError {
    fn from(value: SuiClientError) -> Self {
        let kind = match value {
            SuiClientError::NoCompatibleWalCoins => ClientErrorKind::NoCompatiblePaymentCoin,
            SuiClientError::NoCompatibleGasCoins(desired_amount) => {
                ClientErrorKind::NoCompatibleGasCoins(desired_amount)
            }
            SuiClientError::StakeBelowThreshold(amount) => {
                ClientErrorKind::StakeBelowThreshold(amount)
            }
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
    #[error("could not retrieve enough confirmations to certify the blob: {0} / {1} required;")]
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
    #[error("could not find WAL coins with sufficient balance")]
    NoCompatiblePaymentCoin,
    /// No gas coins with sufficient balance found for the transaction.
    #[error("could not find SUI coins with sufficient balance [requested_amount={0:?}]")]
    NoCompatibleGasCoins(Option<u128>),
    /// The client was unable to open connections to any storage node.
    #[error("connecting to all storage nodes failed: {0}")]
    AllConnectionsFailed(ClientBuildError),
    /// The client seems to be behind the current epoch.
    #[error(
        "the client's current epoch is {client_epoch}, \
        but received a certification for epoch {certified_epoch}"
    )]
    BehindCurrentEpoch {
        /// The client's current epoch.
        client_epoch: Epoch,
        /// The epoch the blob was certified in.
        certified_epoch: Epoch,
    },
    /// The encoding type is not supported.
    #[error("unsupported encoding type: {0}")]
    UnsupportedEncodingType(EncodingType),
    /// The client was notified that the committee has changed.
    #[error("the client was notified that the committee has changed")]
    CommitteeChangeNotified,
    /// The committee has no members.
    #[error("the committee has no members; most likely, the system is in the genesis epoch")]
    EmptyCommittee,
    /// The amount of stake is below the threshold for staking.
    #[error(
        "the stake amount {0} FROST is below the minimum threshold of {MIN_STAKING_THRESHOLD} \
        FROST for staking"
    )]
    StakeBelowThreshold(u64),
    /// Unable to load trusted certificates from the OS.
    #[error("unable to load trusted certificates from the OS: {0:?}")]
    FailedToLoadCerts(Vec<rustls_native_certs::Error>),
    /// A failure internal to the node.
    #[error("client internal error: {0}")]
    Other(Box<dyn std::error::Error + Send + Sync + 'static>),
    /// An internal error occurred while storing a blob, usually indicating a bug.
    #[error("store blob internal error: {0}")]
    StoreBlobInternal(String),
}

// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Client for the Walrus service.

use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    path::PathBuf,
    sync::Arc,
    time::Instant,
};

use anyhow::anyhow;
use cli::{styled_progress_bar, styled_spinner};
use communication::NodeCommunicationFactory;
use futures::{Future, FutureExt};
use indicatif::{HumanDuration, MultiProgress};
use rand::{rngs::ThreadRng, RngCore as _};
use rayon::{iter::IntoParallelIterator, prelude::*};
use refresh::are_current_previous_different;
use resource::{PriceComputation, RegisterBlobOp, ResourceManager, StoreOp};
use responses::BlobStoreResultWithPath;
use sui_types::base_types::ObjectID;
use tokio::{sync::Semaphore, time::Duration};
use tracing::{Instrument as _, Level, Span};
use utils::WeightedResult;
use walrus_core::{
    bft,
    encoding::{
        BlobDecoderEnum,
        EncodingAxis,
        EncodingConfig,
        EncodingConfigTrait as _,
        SliverData,
        SliverPair,
    },
    ensure,
    messages::{BlobPersistenceType, ConfirmationCertificate, SignedStorageConfirmation},
    metadata::{BlobMetadataApi as _, VerifiedBlobMetadataWithId},
    BlobId,
    EncodingType,
    Epoch,
    EpochCount,
    ShardIndex,
    Sliver,
};
use walrus_sdk::{api::BlobStatus, error::NodeError};
use walrus_sui::{
    client::{
        BlobPersistence,
        CertifyAndExtendBlobParams,
        CertifyAndExtendBlobResult,
        ExpirySelectionPolicy,
        PostStoreAction,
        ReadClient,
        SuiContractClient,
    },
    types::{move_structs::BlobWithAttribute, Blob, BlobEvent, StakedWal},
};
use walrus_utils::backoff::BackoffStrategy;

use self::{
    communication::NodeResult,
    config::CommunicationLimits,
    responses::BlobStoreResult,
    utils::{CompletedReasonWeight, WeightedFutures},
};
use crate::common::active_committees::ActiveCommittees;

pub mod cli;
pub mod responses;

pub use crate::common::blocklist::Blocklist;

mod communication;

pub(crate) mod config;
pub use config::{default_configuration_paths, ClientCommunicationConfig, Config};

mod daemon;
pub use daemon::{auth::Claim, ClientDaemon, PublisherQuery, WalrusWriteClient};

mod error;
pub use error::{ClientError, ClientErrorKind};

mod refresh;
pub use refresh::{
    CommitteesRefreshConfig,
    CommitteesRefresher,
    CommitteesRefresherHandle,
    RequestKind,
};
mod resource;

mod utils;
pub use utils::string_prefix;

pub mod metrics;

mod refill;
pub use refill::{RefillHandles, Refiller};
mod multiplexer;

type ClientResult<T> = Result<T, ClientError>;

/// The log level for all WalrusStoreBlob spans
pub(crate) const BLOB_SPAN_LEVEL: Level = Level::INFO;

/// Represents a blob to be stored to the Walrus system.
///
/// The blob can be in different phases of the storage process.
///
/// The different phases are:
///
/// - Unencoded: The blob is in its raw form.
/// - Encoded: The blob is encoded.
/// - WithStatus: A status is obtained if available.
/// - Registered: The blob is registered.
/// - WithCertificate: The blob is certified.
/// - Completed: The blob is stored and the result is available,
///   or an error occurred.
///
/// The typical life cycle of a blob is as follows (symbolically):
///
/// - unencoded_blobs = WalrusStoreBlob::from_bytes( .. )
/// - encoded_blobs = client.encode_blobs( unencoded_blobs )
/// - blobs_with_statuses = client.get_statuses( encoded_blobs )
/// - registered_blobs = client.register_blobs( blobs_with_statuses )
/// - blobs_with_certificates = client.get_certificates( registered_blobs )
/// - cert_results = client.certify_blobs( blobs_with_certificates )
/// - final_result = blobs_with_certificates.complete( cert_results )
///
#[derive(Clone)]
pub enum WalrusStoreBlob<'a, T: Display + Send + Sync> {
    /// Initial phase with raw blob data and identifier.
    Unencoded {
        /// The raw blob data to be stored.
        blob: &'a [u8],
        /// A unique identifier for this blob.
        identifier: T,
        /// The span for this blob's lifecycle.
        span: Span,
    },
    /// After encoding, contains the encoded data and metadata.
    Encoded {
        /// The raw blob data that was encoded.
        blob: &'a [u8],
        /// A unique identifier for this blob.
        identifier: T,
        /// The encoded sliver pairs generated from the blob.
        pairs: Arc<Vec<SliverPair>>,
        /// The verified metadata associated with the encoded blob.
        metadata: Arc<VerifiedBlobMetadataWithId>,
        /// The span for this blob's lifecycle.
        span: Span,
    },
    /// After status check, includes the blob status.
    WithStatus {
        /// The raw blob data.
        blob: &'a [u8],
        /// A unique identifier for this blob.
        identifier: T,
        /// The encoded sliver pairs.
        pairs: Arc<Vec<SliverPair>>,
        /// The verified metadata associated with the blob.
        metadata: Arc<VerifiedBlobMetadataWithId>,
        /// The current status of the blob in the system.
        status: BlobStatus,
        /// The span for this blob's lifecycle.
        span: Span,
    },
    /// After registration, includes the store operation.
    Registered {
        /// The raw blob data.
        blob: &'a [u8],
        /// A unique identifier for this blob.
        identifier: T,
        /// The encoded sliver pairs.
        pairs: Arc<Vec<SliverPair>>,
        /// The verified metadata associated with the blob.
        metadata: Arc<VerifiedBlobMetadataWithId>,
        /// The current status of the blob in the system.
        status: BlobStatus,
        /// The store operation to be performed.
        operation: StoreOp,
        /// The span for this blob's lifecycle.
        span: Span,
    },
    /// After certificate, includes the certificate.
    WithCertificate {
        /// The raw blob data.
        blob: &'a [u8],
        /// A unique identifier for this blob.
        identifier: T,
        /// The encoded sliver pairs.
        pairs: Arc<Vec<SliverPair>>,
        /// The verified metadata associated with the blob.
        metadata: Arc<VerifiedBlobMetadataWithId>,
        /// The current status of the blob in the system.
        status: BlobStatus,
        /// The store operation to be performed.
        operation: StoreOp,
        /// The confirmation certificate for the blob.
        certificate: ConfirmationCertificate,
        /// The span for this blob's lifecycle.
        span: Span,
    },
    /// Final phase with the complete result.
    Completed {
        /// The raw blob data.
        blob: &'a [u8],
        /// A unique identifier for this blob.
        identifier: T,
        /// The final result of the store operation.
        result: BlobStoreResult,
        /// The span for this blob's lifecycle.
        span: Span,
    },
}

impl<'a, T: Display + Send + Sync> WalrusStoreBlob<'a, T> {
    /// Creates a new unencoded blob.
    pub fn new_unencoded(blob: &'a [u8], identifier: T) -> Self {
        let span = tracing::span!(
            BLOB_SPAN_LEVEL,
            "blob_lifecycle",
            blob_id = %BlobId::ZERO,
            identifier = %identifier
        );

        WalrusStoreBlob::Unencoded {
            blob,
            identifier,
            span,
        }
    }

    /// Returns true if the blob is in the Encoded state.
    pub fn is_encoded(&self) -> bool {
        matches!(self, WalrusStoreBlob::Encoded { .. })
    }

    /// Returns true if the blob has a certificate.
    pub fn is_with_certificate(&self) -> bool {
        matches!(self, WalrusStoreBlob::WithCertificate { .. })
    }

    /// Returns true if the blob operation is completed.
    pub fn is_completed(&self) -> bool {
        matches!(self, WalrusStoreBlob::Completed { .. })
    }

    /// Returns true if the blob has a status.
    pub fn is_with_status(&self) -> bool {
        matches!(self, WalrusStoreBlob::WithStatus { .. })
    }

    /// Returns true if the blob needs to be certified based on its current
    /// state and operation type.
    pub fn needs_certify(&self) -> bool {
        if let WalrusStoreBlob::Registered {
            operation: StoreOp::RegisterNew { operation, .. },
            ..
        } = self
        {
            match operation {
                RegisterBlobOp::ReuseRegistration { .. }
                | RegisterBlobOp::ReuseAndExtend { .. } => false,
                RegisterBlobOp::RegisterFromScratch { .. }
                | RegisterBlobOp::ReuseStorage { .. }
                | RegisterBlobOp::ReuseAndExtendNonCertified { .. } => true,
            }
        } else {
            false
        }
    }

    /// Returns true if the blob needs to be extended based on its current state and operation type.
    pub fn needs_extend(&self) -> bool {
        if let WalrusStoreBlob::Registered {
            operation: StoreOp::RegisterNew { operation, .. },
            ..
        } = self
        {
            matches!(operation, RegisterBlobOp::ReuseAndExtend { .. })
        } else {
            false
        }
    }

    /// Returns a string representation of the current state of the blob.
    pub fn get_state(&self) -> &str {
        match self {
            WalrusStoreBlob::Unencoded { .. } => "Unencoded",
            WalrusStoreBlob::Encoded { .. } => "Encoded",
            WalrusStoreBlob::WithStatus { .. } => "WithStatus",
            WalrusStoreBlob::Registered { .. } => "Registered",
            WalrusStoreBlob::WithCertificate { .. } => "WithCertificate",
            WalrusStoreBlob::Completed { .. } => "Completed",
        }
    }

    /// Returns a reference to the raw blob data.
    pub fn get_blob(&self) -> &'a [u8] {
        match self {
            WalrusStoreBlob::Unencoded { blob, .. } => blob,
            WalrusStoreBlob::Encoded { blob, .. } => blob,
            WalrusStoreBlob::WithStatus { blob, .. } => blob,
            WalrusStoreBlob::Registered { blob, .. } => blob,
            WalrusStoreBlob::WithCertificate { blob, .. } => blob,
            WalrusStoreBlob::Completed { blob, .. } => blob,
        }
    }

    /// Returns a reference to the blob's identifier.
    pub fn get_identifier(&self) -> &T {
        match self {
            WalrusStoreBlob::Unencoded { identifier, .. } => identifier,
            WalrusStoreBlob::Encoded { identifier, .. } => identifier,
            WalrusStoreBlob::WithStatus { identifier, .. } => identifier,
            WalrusStoreBlob::Registered { identifier, .. } => identifier,
            WalrusStoreBlob::WithCertificate { identifier, .. } => identifier,
            WalrusStoreBlob::Completed { identifier, .. } => identifier,
        }
    }

    /// Returns the error message if the blob is in the Completed state and the result is an error.
    pub fn get_error_msg(&self) -> Option<&str> {
        match self {
            WalrusStoreBlob::Completed {
                result: BlobStoreResult::Error { error_msg, .. },
                ..
            } => Some(error_msg),
            _ => None,
        }
    }

    /// Returns the length of the unencoded blob data in bytes.
    pub fn unencoded_length(&self) -> usize {
        match self {
            WalrusStoreBlob::Unencoded { blob, .. } => blob.len(),
            WalrusStoreBlob::Encoded { blob, .. } => blob.len(),
            WalrusStoreBlob::WithStatus { blob, .. } => blob.len(),
            WalrusStoreBlob::Registered { blob, .. } => blob.len(),
            WalrusStoreBlob::WithCertificate { blob, .. } => blob.len(),
            WalrusStoreBlob::Completed { blob, .. } => blob.len(),
        }
    }

    /// Returns the blob ID associated with this blob.
    pub fn get_blob_id(&self) -> BlobId {
        match self {
            WalrusStoreBlob::Unencoded { .. } => BlobId::ZERO,
            WalrusStoreBlob::Encoded { metadata, .. } => *metadata.blob_id(),
            WalrusStoreBlob::WithStatus { metadata, .. } => *metadata.blob_id(),
            WalrusStoreBlob::Registered { metadata, .. } => *metadata.blob_id(),
            WalrusStoreBlob::WithCertificate { metadata, .. } => *metadata.blob_id(),
            WalrusStoreBlob::Completed { result, .. } => *result.blob_id(),
        }
    }

    /// Returns the object ID if available from the current operation.
    pub fn get_object_id(&self) -> Option<ObjectID> {
        self.get_operation().and_then(|op| match op {
            StoreOp::RegisterNew { blob, .. } => Some(blob.id),
            StoreOp::NoOp(_) => None,
        })
    }

    /// Returns a reference to the sliver pairs if available in the current state.
    pub fn get_sliver_pairs(&self) -> Option<&Vec<SliverPair>> {
        match self {
            WalrusStoreBlob::Unencoded { .. } => None,
            WalrusStoreBlob::Encoded { pairs, .. } => Some(pairs),
            WalrusStoreBlob::WithStatus { pairs, .. } => Some(pairs),
            WalrusStoreBlob::Registered { pairs, .. } => Some(pairs),
            WalrusStoreBlob::WithCertificate { pairs, .. } => Some(pairs),
            WalrusStoreBlob::Completed { .. } => None,
        }
    }

    /// Returns a reference to the verified metadata if available in the current state.
    pub fn get_metadata(&self) -> Option<&VerifiedBlobMetadataWithId> {
        match self {
            WalrusStoreBlob::Unencoded { .. } => None,
            WalrusStoreBlob::Encoded { metadata, .. } => Some(metadata),
            WalrusStoreBlob::WithStatus { metadata, .. } => Some(metadata),
            WalrusStoreBlob::Registered { metadata, .. } => Some(metadata),
            WalrusStoreBlob::WithCertificate { metadata, .. } => Some(metadata),
            WalrusStoreBlob::Completed { .. } => None,
        }
    }

    /// Returns a reference to the blob status if available in the current state.
    pub fn get_status(&self) -> Option<&BlobStatus> {
        match self {
            WalrusStoreBlob::Unencoded { .. } | WalrusStoreBlob::Encoded { .. } => None,
            WalrusStoreBlob::WithStatus { status, .. } => Some(status),
            WalrusStoreBlob::Registered { status, .. } => Some(status),
            WalrusStoreBlob::WithCertificate { status, .. } => Some(status),
            WalrusStoreBlob::Completed { .. } => None,
        }
    }

    /// Returns a reference to the store operation if available in the current state.
    pub fn get_operation(&self) -> Option<&StoreOp> {
        match self {
            WalrusStoreBlob::Unencoded { .. }
            | WalrusStoreBlob::Encoded { .. }
            | WalrusStoreBlob::WithStatus { .. } => None,
            WalrusStoreBlob::Registered { operation, .. } => Some(operation),
            WalrusStoreBlob::WithCertificate { operation, .. } => Some(operation),
            WalrusStoreBlob::Completed { .. } => None,
        }
    }

    /// Returns a reference to the final result if the blob is in the Completed state.
    pub fn get_result(&self) -> Option<&BlobStoreResult> {
        match self {
            WalrusStoreBlob::Completed { result, .. } => Some(result),
            _ => None,
        }
    }

    /// Processes the encoding result and transitions the blob to the appropriate next state.<'b>
    pub fn with_encode_result(
        self,
        result: Result<(Vec<SliverPair>, VerifiedBlobMetadataWithId), ClientError>,
    ) -> Self {
        let new_state = match self {
            WalrusStoreBlob::Unencoded {
                blob,
                identifier,
                span,
            } => match result {
                Ok((pairs, metadata)) => WalrusStoreBlob::Encoded {
                    blob,
                    identifier,
                    pairs: Arc::new(pairs),
                    metadata: Arc::new(metadata),
                    span: span.clone(),
                },
                Err(e) => WalrusStoreBlob::Completed {
                    blob,
                    identifier,
                    result: BlobStoreResult::Error {
                        blob_id: BlobId::ZERO,
                        error_msg: e.to_string(),
                    },
                    span: span.clone(),
                },
            },
            _ => self,
        };

        // Use the span from the new state for logging
        {
            let _enter = new_state.get_span().enter();
            tracing::event!(BLOB_SPAN_LEVEL, operation = "encode");
            tracing::event!(BLOB_SPAN_LEVEL, operation = "encode completed", state = ?new_state);
        }

        new_state
    }

    /// Processes the status result and transitions the blob to the appropriate next state.
    pub fn with_status(self, status: Result<BlobStatus, ClientError>) -> Self {
        let new_state = match self {
            WalrusStoreBlob::Encoded {
                blob,
                identifier,
                pairs,
                metadata,
                span,
            } => match status {
                Ok(status) => WalrusStoreBlob::WithStatus {
                    blob,
                    identifier,
                    pairs,
                    metadata,
                    status,
                    span: span.clone(),
                },
                Err(e) => {
                    let blob_id = *metadata.blob_id();
                    WalrusStoreBlob::Completed {
                        blob,
                        identifier,
                        result: BlobStoreResult::Error {
                            blob_id,
                            error_msg: e.to_string(),
                        },
                        span: span.clone(),
                    }
                }
            },
            _ => self,
        };

        {
            let _enter = new_state.get_span().enter();
            tracing::event!(BLOB_SPAN_LEVEL, operation = "update status");
            tracing::event!(
                BLOB_SPAN_LEVEL,
                operation = "update status completed",
                state = ?new_state
            );
        }

        new_state
    }

    /// Updates the blob with the result of the register operation and transitions
    /// to the appropriate next state.
    pub fn with_register_result(self, result: Result<StoreOp, ClientError>) -> Self {
        let new_state = match self {
            WalrusStoreBlob::WithStatus {
                blob,
                identifier,
                pairs,
                metadata,
                status,
                span,
            } => match result {
                Ok(StoreOp::NoOp(result)) => WalrusStoreBlob::Completed {
                    blob,
                    identifier,
                    result,
                    span,
                },
                Ok(store_op) => WalrusStoreBlob::Registered {
                    blob,
                    identifier,
                    pairs,
                    metadata,
                    status,
                    operation: store_op,
                    span,
                },
                Err(e) => {
                    let blob_id = *metadata.blob_id();
                    WalrusStoreBlob::Completed {
                        blob,
                        identifier,
                        result: BlobStoreResult::Error {
                            blob_id,
                            error_msg: e.to_string(),
                        },
                        span,
                    }
                }
            },
            _ => panic!(
                "Invalid state for with_register_result {:?}, register result: {:?}",
                self, result
            ),
        };

        {
            let _enter = new_state.get_span().enter();
            tracing::event!(BLOB_SPAN_LEVEL, operation = "register");
            tracing::event!(BLOB_SPAN_LEVEL, operation = "register completed", state = ?new_state);
        }

        new_state
    }

    /// Updates the blob with the result of the certificate operation and transitions
    /// to the appropriate next state.
    pub fn with_certificate_result(
        self,
        certificate_result: ClientResult<ConfirmationCertificate>,
    ) -> Self {
        let new_state = match self {
            WalrusStoreBlob::Registered {
                blob,
                identifier,
                pairs,
                metadata,
                status,
                operation,
                span,
            } => match certificate_result {
                Ok(certificate) => WalrusStoreBlob::WithCertificate {
                    blob,
                    identifier,
                    pairs,
                    metadata,
                    status,
                    operation,
                    certificate,
                    span,
                },
                Err(e) => {
                    let blob_id = *metadata.blob_id();
                    WalrusStoreBlob::Completed {
                        blob,
                        identifier,
                        result: BlobStoreResult::Error {
                            blob_id,
                            error_msg: e.to_string(),
                        },
                        span,
                    }
                }
            },
            _ => self,
        };

        {
            let _enter = new_state.get_span().enter();
            tracing::event!(BLOB_SPAN_LEVEL, operation = "certificate");
            tracing::event!(
                BLOB_SPAN_LEVEL,
                operation = "certificate completed",
                state = ?new_state
            );
        }

        new_state
    }

    /// Converts the current blob state to a Completed state with an error result.
    pub fn with_error(self, error: ClientError) -> Self {
        let blob_id = self.get_blob_id();
        let (blob, identifier, span) = match self {
            WalrusStoreBlob::Unencoded {
                blob,
                identifier,
                span,
            } => (blob, identifier, span),
            WalrusStoreBlob::Encoded {
                blob,
                identifier,
                span,
                ..
            } => (blob, identifier, span),
            WalrusStoreBlob::WithStatus {
                blob,
                identifier,
                span,
                ..
            } => (blob, identifier, span),
            WalrusStoreBlob::Registered {
                blob,
                identifier,
                span,
                ..
            } => (blob, identifier, span),
            WalrusStoreBlob::WithCertificate {
                blob,
                identifier,
                span,
                ..
            } => (blob, identifier, span),
            WalrusStoreBlob::Completed {
                blob,
                identifier,
                span,
                ..
            } => (blob, identifier, span),
        };

        WalrusStoreBlob::Completed {
            blob,
            identifier,
            result: BlobStoreResult::Error {
                blob_id,
                error_msg: error.to_string(),
            },
            span,
        }
    }

    /// Updates the blob with the provided result and transitions to the Completed state.
    pub fn complete_with(self, result: BlobStoreResult) -> Self {
        let (blob, identifier, span) = match self {
            WalrusStoreBlob::Registered {
                blob,
                identifier,
                span,
                ..
            }
            | WalrusStoreBlob::WithCertificate {
                blob,
                identifier,
                span,
                ..
            }
            | WalrusStoreBlob::WithStatus {
                blob,
                identifier,
                span,
                ..
            }
            | WalrusStoreBlob::Encoded {
                blob,
                identifier,
                span,
                ..
            }
            | WalrusStoreBlob::Unencoded {
                blob,
                identifier,
                span,
            }
            | WalrusStoreBlob::Completed {
                blob,
                identifier,
                span,
                ..
            } => (blob, identifier, span),
        };

        let new_state = WalrusStoreBlob::Completed {
            blob,
            identifier,
            result,
            span,
        };

        {
            let _enter = new_state.get_span().enter();
            tracing::event!(BLOB_SPAN_LEVEL, operation = "complete");
            tracing::event!(
                BLOB_SPAN_LEVEL,
                operation = "complete completed",
                state = ?new_state
            );
        }

        new_state
    }

    /// Updates the blob with the result of the complete operation and
    /// transitions to the Completed state.
    pub fn complete(
        self,
        result: CertifyAndExtendBlobResult,
        price_computation: &PriceComputation,
    ) -> Self {
        let store_blob_result = self.compute_store_blob_result(result, price_computation);
        self.complete_with(store_blob_result)
    }

    /// Computes the final store result based on the current state and operation.
    fn compute_store_blob_result(
        &self,
        result: CertifyAndExtendBlobResult,
        price_computation: &PriceComputation,
    ) -> BlobStoreResult {
        match self {
            WalrusStoreBlob::WithCertificate { operation, .. } => {
                if let StoreOp::RegisterNew { operation, blob } = operation {
                    BlobStoreResult::NewlyCreated {
                        blob_object: blob.clone(),
                        resource_operation: operation.clone(),
                        cost: price_computation.operation_cost(operation),
                        shared_blob_object: result.shared_blob_object(),
                    }
                } else {
                    panic!(
                        "Invalid state for computing store blob result {:?}",
                        operation
                    );
                }
            }
            WalrusStoreBlob::Registered { operation, .. } => match operation {
                StoreOp::RegisterNew { operation, blob } => BlobStoreResult::NewlyCreated {
                    blob_object: blob.clone(),
                    resource_operation: operation.clone(),
                    cost: price_computation.operation_cost(operation),
                    shared_blob_object: result.shared_blob_object(),
                },
                _ => {
                    panic!(
                        "Invalid state for computing store blob result {:?}",
                        operation
                    );
                }
            },
            _ => {
                panic!("Invalid state for computing store blob result");
            }
        }
    }

    /// Returns the parameters needed for certifying and extending the blob based on its
    /// current state.
    pub fn get_certify_and_extend_params(&self) -> Option<CertifyAndExtendBlobParams> {
        match self {
            WalrusStoreBlob::WithCertificate {
                operation:
                    StoreOp::RegisterNew {
                        operation:
                            RegisterBlobOp::ReuseAndExtend { .. }
                            | RegisterBlobOp::ReuseRegistration { .. },
                        ..
                    },
                ..
            } => panic!("ReuseAndExtend is not supported"),

            WalrusStoreBlob::WithCertificate {
                operation:
                    StoreOp::RegisterNew {
                        operation:
                            RegisterBlobOp::ReuseAndExtendNonCertified {
                                epochs_extended, ..
                            },
                        blob,
                    },
                certificate,
                ..
            } => Some(CertifyAndExtendBlobParams {
                blob,
                certificate: Some(certificate.clone()),
                epochs_extended: Some(*epochs_extended),
            }),

            WalrusStoreBlob::WithCertificate {
                operation:
                    StoreOp::RegisterNew {
                        operation:
                            RegisterBlobOp::RegisterFromScratch { .. }
                            | RegisterBlobOp::ReuseStorage { .. },
                        blob,
                    },
                certificate,
                ..
            } => Some(CertifyAndExtendBlobParams {
                blob,
                certificate: Some(certificate.clone()),
                epochs_extended: None,
            }),

            WalrusStoreBlob::Registered {
                operation:
                    StoreOp::RegisterNew {
                        operation:
                            RegisterBlobOp::ReuseAndExtend {
                                epochs_extended, ..
                            },
                        blob,
                    },
                ..
            } => Some(CertifyAndExtendBlobParams {
                blob,
                certificate: None,
                epochs_extended: Some(*epochs_extended),
            }),

            WalrusStoreBlob::WithCertificate {
                operation: StoreOp::NoOp(_),
                ..
            } => None,
            WalrusStoreBlob::Registered {
                operation: StoreOp::NoOp(_),
                ..
            } => None,
            WalrusStoreBlob::Registered {
                operation:
                    StoreOp::RegisterNew {
                        operation:
                            RegisterBlobOp::RegisterFromScratch { .. }
                            | RegisterBlobOp::ReuseStorage { .. }
                            | RegisterBlobOp::ReuseRegistration { .. }
                            | RegisterBlobOp::ReuseAndExtendNonCertified { .. },
                        ..
                    },
                ..
            } => None,
            WalrusStoreBlob::Unencoded { .. } => None,
            WalrusStoreBlob::Encoded { .. } => None,
            WalrusStoreBlob::WithStatus { .. } => None,
            WalrusStoreBlob::Completed { .. } => None,
        }
    }

    fn get_span(&self) -> &Span {
        match self {
            WalrusStoreBlob::Unencoded { span, .. } => span,
            WalrusStoreBlob::Encoded { span, .. } => span,
            WalrusStoreBlob::WithStatus { span, .. } => span,
            WalrusStoreBlob::Registered { span, .. } => span,
            WalrusStoreBlob::WithCertificate { span, .. } => span,
            WalrusStoreBlob::Completed { span, .. } => span,
        }
    }
}

impl<T: Display + Send + Sync> std::fmt::Debug for WalrusStoreBlob<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let _enter = self.get_span().enter();
        write!(
            f,
            "blob_id: {}, identifier: {}, state: {}, operation: {:?}",
            self.get_blob_id(),
            self.get_identifier(),
            self.get_state(),
            self.get_operation()
        )
    }
}

/// Represents how the store operation should be carried out by the client.
#[derive(Debug, Clone, Copy)]
pub enum StoreWhen {
    /// Store the blob if not stored, but do not check the resources in the current wallet.
    ///
    /// With this command, the client does not check for usable registrations or
    /// storage space. This is useful when using the publisher, to avoid wasting multiple round
    /// trips to the fullnode.
    NotStoredIgnoreResources,
    /// Check the status of the blob before storing it, and store it only if it is not already.
    NotStored,
    /// Store the blob always, without checking the status.
    Always,
    /// Store the blob always, without checking the status, and ignore the resources in the wallet.
    AlwaysIgnoreResources,
}

impl StoreWhen {
    /// Returns `true` if the operation ignore the blob status.
    ///
    /// If `true`, the client should store the blob, even if the blob is already stored on Walrus
    /// for a sufficient number of epochs.
    pub fn is_ignore_status(&self) -> bool {
        matches!(self, Self::Always | Self::AlwaysIgnoreResources)
    }

    /// Returns `true` if the operation should ignore the resources in the wallet.
    pub fn is_ignore_resources(&self) -> bool {
        matches!(
            self,
            Self::NotStoredIgnoreResources | Self::AlwaysIgnoreResources
        )
    }

    /// Returns [`Self`] based on the value of the `force` and `ignore-resources` flags.
    pub fn from_flags(force: bool, ignore_resources: bool) -> Self {
        match (force, ignore_resources) {
            (true, true) => Self::AlwaysIgnoreResources,
            (true, false) => Self::Always,
            (false, true) => Self::NotStoredIgnoreResources,
            (false, false) => Self::NotStored,
        }
    }
}

/// A client to communicate with Walrus shards and storage nodes.
#[derive(Debug, Clone)]
pub struct Client<T> {
    config: Config,
    sui_client: T,
    communication_limits: CommunicationLimits,
    committees_handle: CommitteesRefresherHandle,
    // The `Arc` is used to share the encoding config with the `communication_factory` without
    // introducing lifetimes.
    encoding_config: Arc<EncodingConfig>,
    blocklist: Option<Blocklist>,
    communication_factory: NodeCommunicationFactory,
}

impl Client<()> {
    /// Creates a new Walrus client without a Sui client.
    pub async fn new(
        config: Config,
        committees_handle: CommitteesRefresherHandle,
    ) -> ClientResult<Self> {
        tracing::debug!(?config, "running client");

        // Request the committees and price computation from the cache.
        let (committees, _) = committees_handle
            .send_committees_and_price_request(RequestKind::Get)
            .await
            .map_err(ClientError::other)?;

        let encoding_config = EncodingConfig::new(committees.n_shards());
        let communication_limits =
            CommunicationLimits::new(&config.communication_config, encoding_config.n_shards());

        let encoding_config = Arc::new(encoding_config);

        Ok(Self {
            sui_client: (),
            encoding_config: encoding_config.clone(),
            communication_limits,
            committees_handle,
            blocklist: None,
            communication_factory: NodeCommunicationFactory::new(
                config.communication_config.clone(),
                encoding_config,
            )?,
            config,
        })
    }

    /// Converts `self` to a [`Client::<T>`] by adding the `sui_client`.
    pub async fn with_client<C>(self, sui_client: C) -> Client<C> {
        let Self {
            config,
            sui_client: _,
            committees_handle,
            encoding_config,
            communication_limits,
            blocklist,
            communication_factory: node_client_factory,
        } = self;
        Client::<C> {
            config,
            sui_client,
            committees_handle,
            encoding_config,
            communication_limits,
            blocklist,
            communication_factory: node_client_factory,
        }
    }
}

impl<T: ReadClient> Client<T> {
    /// Creates a new read client starting from a config file.
    pub async fn new_read_client(
        config: Config,
        committees_handle: CommitteesRefresherHandle,
        sui_read_client: T,
    ) -> ClientResult<Self> {
        Ok(Client::new(config, committees_handle)
            .await?
            .with_client(sui_read_client)
            .await)
    }

    /// Creates a new read client, and starts a committes refresher process in the background.
    ///
    /// This is useful when only one client is needed, and the refresher handle is not useful.
    pub async fn new_read_client_with_refresher(
        config: Config,
        sui_read_client: T,
    ) -> ClientResult<Self>
    where
        T: ReadClient + Clone + 'static,
    {
        let committees_handle = config
            .refresh_config
            .build_refresher_and_run(sui_read_client.clone())
            .await
            .map_err(|e| ClientError::from(ClientErrorKind::Other(e.into())))?;
        Ok(Client::new(config, committees_handle)
            .await?
            .with_client(sui_read_client)
            .await)
    }

    /// Reconstructs the blob by reading slivers from Walrus shards.
    ///
    /// The operation is retried if epoch it fails due to epoch change.
    pub async fn read_blob_retry_committees<U>(&self, blob_id: &BlobId) -> ClientResult<Vec<u8>>
    where
        U: EncodingAxis,
        SliverData<U>: TryFrom<Sliver>,
    {
        self.retry_if_notified_epoch_change(|| self.read_blob::<U>(blob_id))
            .await
    }

    /// Reconstructs the blob by reading slivers from Walrus shards.
    #[tracing::instrument(level = Level::ERROR, skip_all, fields(%blob_id))]
    pub async fn read_blob<U>(&self, blob_id: &BlobId) -> ClientResult<Vec<u8>>
    where
        U: EncodingAxis,
        SliverData<U>: TryFrom<Sliver>,
    {
        self.read_blob_internal(blob_id, None).await
    }

    /// Reconstructs the blob by reading slivers from Walrus shards with the given status.
    #[tracing::instrument(level = Level::ERROR, skip_all, fields(%blob_id))]
    pub async fn read_blob_with_status<U>(
        &self,
        blob_id: &BlobId,
        blob_status: BlobStatus,
    ) -> ClientResult<Vec<u8>>
    where
        U: EncodingAxis,
        SliverData<U>: TryFrom<Sliver>,
    {
        self.read_blob_internal(blob_id, Some(blob_status)).await
    }

    /// Internal method to handle the common logic for reading blobs.
    async fn read_blob_internal<U>(
        &self,
        blob_id: &BlobId,
        blob_status: Option<BlobStatus>,
    ) -> ClientResult<Vec<u8>>
    where
        U: EncodingAxis,
        SliverData<U>: TryFrom<Sliver>,
    {
        tracing::debug!("starting to read blob");
        self.check_blob_id(blob_id)?;
        let committees = self.get_committees().await?;

        let certified_epoch = if committees.is_change_in_progress() {
            tracing::info!("epoch change in progress, reading from initial certified epoch");
            let blob_status = match blob_status {
                Some(status) => status,
                None => {
                    self.get_blob_status_with_retries(blob_id, &self.sui_client)
                        .await?
                }
            };
            blob_status
                .initial_certified_epoch()
                .ok_or_else(|| ClientError::from(ClientErrorKind::BlobIdDoesNotExist))?
        } else {
            // We are not during epoch change, we can read from the current epoch directly.
            committees.epoch()
        };

        // Return early if the committee is behind.
        let current_epoch = committees.epoch();
        if certified_epoch > current_epoch {
            return Err(ClientError::from(ClientErrorKind::BehindCurrentEpoch {
                client_epoch: current_epoch,
                certified_epoch,
            }));
        }

        self.read_metadata_and_slivers::<U>(certified_epoch, blob_id)
            .await
    }

    async fn read_metadata_and_slivers<U>(
        &self,
        certified_epoch: Epoch,
        blob_id: &BlobId,
    ) -> ClientResult<Vec<u8>>
    where
        U: EncodingAxis,
        SliverData<U>: TryFrom<Sliver>,
    {
        let metadata = self.retrieve_metadata(certified_epoch, blob_id).await?;
        self.request_slivers_and_decode::<U>(certified_epoch, &metadata)
            .await
    }

    /// Retries the given function if the client gets notified that the committees have changed.
    ///
    /// This function should not be used to retry function `func` that cannot be interrupted at
    /// arbitrary await points. Most importantly, functions that use the wallet to sign and execute
    /// transactions on Sui.
    async fn retry_if_notified_epoch_change<F, R, Fut>(&self, func: F) -> ClientResult<R>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = ClientResult<R>>,
    {
        let func_check_notify = || self.await_while_checking_notification(func());
        self.retry_if_error_epoch_change(func_check_notify).await
    }

    /// Retries the given function if the function returns with an error that may be related to
    /// epoch change.
    async fn retry_if_error_epoch_change<F, R, Fut>(&self, func: F) -> ClientResult<R>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = ClientResult<R>>,
    {
        let mut attempts = 0;

        let mut backoff = self
            .config
            .communication_config
            .committee_change_backoff
            .get_strategy(ThreadRng::default().next_u64());

        // Retry the given function N-1 times; if it does not succeed after N-1 times, then the
        // last try is made outside the loop.
        while let Some(delay) = backoff.next_delay() {
            match func().await {
                Ok(result) => return Ok(result),
                Err(error) => {
                    if error.may_be_caused_by_epoch_change() {
                        tracing::warn!(
                            %error,
                            "operation aborted; maybe because of epoch change; retrying"
                        );
                        if !matches!(*error.kind(), ClientErrorKind::CommitteeChangeNotified) {
                            // If the error is CommitteeChangeNotified, we do not need to refresh
                            // the committees.
                            tracing::warn!("forcing committee refresh before retrying");
                            self.force_refresh_committees().await?;
                        }
                    } else {
                        tracing::warn!(%error, "operation failed; not retrying");
                        return Err(error);
                    }
                }
            };

            attempts += 1;
            tracing::info!(
                ?attempts,
                ?delay,
                "committee change detected; retrying after a delay",
            );
            tokio::time::sleep(delay).await;
        }

        // The last try.
        tracing::warn!(?attempts, "retries exhausted; conduct one last try");
        func().await
    }

    async fn get_blob_by_object_id(
        &self,
        blob_object_id: &ObjectID,
    ) -> ClientResult<BlobWithAttribute> {
        self.sui_client
            .get_blob_by_object_id(blob_object_id)
            .await
            .map_err(|e| {
                if e.to_string()
                    .contains("response does not contain object data")
                {
                    ClientError::from(ClientErrorKind::BlobIdDoesNotExist)
                } else {
                    ClientError::other(e)
                }
            })
    }

    /// Executes the function while also awaiiting on the change notification.
    ///
    /// Returns a [`ClientErrorKind::CommitteeChangeNotified`] error if the client is notified that
    /// the committee has changed.
    async fn await_while_checking_notification<Fut, R>(&self, future: Fut) -> ClientResult<R>
    where
        Fut: Future<Output = ClientResult<R>>,
    {
        tokio::select! {
            _ = self.committees_handle.change_notified() => {
                Err(ClientError::from(ClientErrorKind::CommitteeChangeNotified))
            },
            result = future => result,
        }
    }
}

impl Client<SuiContractClient> {
    /// Creates a new client starting from a config file.
    pub async fn new_contract_client(
        config: Config,
        committees_handle: CommitteesRefresherHandle,
        sui_client: SuiContractClient,
    ) -> ClientResult<Self> {
        Ok(Client::new(config, committees_handle)
            .await?
            .with_client(sui_client)
            .await)
    }

    /// Creates a new client, and starts a committes refresher process in the background.
    ///
    /// This is useful when only one client is needed, and the refresher handle is not useful.
    pub async fn new_contract_client_with_refresher(
        config: Config,
        sui_client: SuiContractClient,
    ) -> ClientResult<Self> {
        let committees_handle = config
            .refresh_config
            .build_refresher_and_run(sui_client.read_client().clone())
            .await
            .map_err(|e| ClientError::from(ClientErrorKind::Other(e.into())))?;
        Ok(Client::new(config, committees_handle)
            .await?
            .with_client(sui_client)
            .await)
    }

    /// Stores a list of blobs to Walrus, retrying if it fails because of epoch change.
    #[tracing::instrument(skip_all, fields(blob_id))]
    pub async fn reserve_and_store_blobs_retry_committees(
        &self,
        blobs: &[&[u8]],
        encoding_type: EncodingType,
        epochs_ahead: EpochCount,
        store_when: StoreWhen,
        persistence: BlobPersistence,
        post_store: PostStoreAction,
    ) -> ClientResult<Vec<BlobStoreResult>> {
        let blobs_with_identifiers = blobs
            .iter()
            .enumerate()
            .map(|(i, blob)| WalrusStoreBlob::new_unencoded(blob, format!("blob_{}", i)))
            .collect::<Vec<_>>();

        let encoded_blobs = self.encode_blobs_to_pairs_and_metadata_with_identifiers(
            blobs_with_identifiers,
            encoding_type,
        )?;

        let results = self
            .retry_if_error_epoch_change(|| {
                self.reserve_and_store_encoded_blobs(
                    encoded_blobs.clone(),
                    epochs_ahead,
                    store_when,
                    persistence,
                    post_store,
                )
            })
            .await?;

        Ok(results
            .into_iter()
            .filter_map(|blob| blob.get_result().cloned())
            .collect())
    }

    /// Stores a list of blobs to Walrus, retrying if it fails because of epoch change.
    /// Similar to `[Client::reserve_and_store_blobs_retry_committees]`, except the result
    /// includes the corresponding path for blob.
    #[tracing::instrument(skip_all, fields(blob_id))]
    pub async fn reserve_and_store_blobs_retry_committees_with_path(
        &self,
        blobs_with_paths: &[(PathBuf, Vec<u8>)],
        encoding_type: EncodingType,
        epochs_ahead: EpochCount,
        store_when: StoreWhen,
        persistence: BlobPersistence,
        post_store: PostStoreAction,
    ) -> ClientResult<Vec<BlobStoreResultWithPath>> {
        let path_map = blobs_with_paths
            .iter()
            .enumerate()
            .map(|(i, (path, _))| (path, format!("blob_{}", i)))
            .collect::<HashMap<_, _>>();
        let blobs_with_identifiers = blobs_with_paths
            .iter()
            .map(|(path, blob)| WalrusStoreBlob::new_unencoded(blob, path_map[path].clone()))
            .collect::<Vec<_>>();

        let encoded_blobs = self.encode_blobs_to_pairs_and_metadata_with_identifiers(
            blobs_with_identifiers,
            encoding_type,
        )?;

        tracing::info!("debug-store encode results: {:?}", encoded_blobs);

        let store_results = self
            .retry_if_error_epoch_change(|| {
                self.reserve_and_store_encoded_blobs(
                    encoded_blobs.clone(),
                    epochs_ahead,
                    store_when,
                    persistence,
                    post_store,
                )
            })
            .await?;

        assert_eq!(store_results.len(), encoded_blobs.len());

        let results: Vec<BlobStoreResultWithPath> = blobs_with_paths
            .iter()
            .map(|(path, _)| {
                let identifier = &path_map[path];
                let store_result = store_results
                    .iter()
                    .find(|blob| blob.get_identifier() == identifier)
                    .and_then(|blob| blob.get_result())
                    .cloned()
                    .unwrap_or_else(|| BlobStoreResult::Error {
                        blob_id: BlobId::ZERO,
                        error_msg: "No result found".to_string(),
                    });

                BlobStoreResultWithPath {
                    path: path.clone(),
                    blob_store_result: store_result,
                }
            })
            .collect();

        debug_assert_eq!(results.len(), blobs_with_paths.len());

        Ok(results)
    }

    /// Encodes the blob, reserves & registers the space on chain, and stores the slivers to the
    /// storage nodes. Finally, the function aggregates the storage confirmations and posts the
    /// [`ConfirmationCertificate`] on chain.
    #[tracing::instrument(skip_all, fields(blob_id))]
    pub async fn reserve_and_store_blobs(
        &self,
        blobs: &[&[u8]],
        encoding_type: EncodingType,
        epochs_ahead: EpochCount,
        store_when: StoreWhen,
        persistence: BlobPersistence,
        post_store: PostStoreAction,
    ) -> ClientResult<Vec<BlobStoreResult>> {
        let blobs_with_identifiers = blobs
            .iter()
            .enumerate()
            .map(|(i, blob)| WalrusStoreBlob::new_unencoded(blob, format!("blob_{}", i)))
            .collect::<Vec<_>>();

        let encoded_blobs = self.encode_blobs_to_pairs_and_metadata_with_identifiers(
            blobs_with_identifiers,
            encoding_type,
        )?;

        let mut results = self
            .reserve_and_store_encoded_blobs(
                encoded_blobs,
                epochs_ahead,
                store_when,
                persistence,
                post_store,
            )
            .await?;

        results.sort_by_key(|blob| blob.get_identifier().to_string());

        Ok(results
            .into_iter()
            .filter_map(|blob| blob.get_result().cloned())
            .collect())
    }

    // async fn encode_blobs_to_pairs_and_metadata_with_path<'a> (
    //     &self,
    //     blobs_with_paths: &'a [(PathBuf, Vec<u8>)],
    //     encoding_type: EncodingType,
    // ) -> ClientResult<Vec<WalrusStoreBlob<'a, Path>>> {
    //     let blobs_with_identifiers: Vec<_> = blobs_with_paths
    //         .iter()
    //         .map(|(path, b)| BlobWithIdentifier {
    //             identifier: path.to_str().unwrap_or("invalid path"),
    //             blob: b.as_slice(),
    //         })
    //         .collect();
    //     let results = self.encode_blobs_to_pairs_and_metadata_with_identifiers(
    //         &blobs_with_identifiers,
    //         encoding_type,
    //     )?;

    //     debug_assert_eq!(
    //         results.len(),
    //         blobs_with_paths.len(),
    //         "the number of encoded blobs and the number of blobs with paths must be the same"
    //     );

    //     tracing::info!("encoded files to metadata: {:?}", results);

    //     Ok(results)
    // }

    /// Encodes multiple blobs into sliver pairs and metadata.
    pub fn encode_blobs_to_pairs_and_metadata(
        &self,
        blobs: &[&[u8]],
        encoding_type: EncodingType,
    ) -> ClientResult<Vec<(Vec<SliverPair>, VerifiedBlobMetadataWithId)>> {
        let blobs_with_identifiers = blobs
            .iter()
            .enumerate()
            .map(|(i, blob)| WalrusStoreBlob::new_unencoded(blob, format!("blob_{}", i)))
            .collect::<Vec<_>>();

        let encoded_blobs = self.encode_blobs_to_pairs_and_metadata_with_identifiers(
            blobs_with_identifiers,
            encoding_type,
        )?;

        debug_assert_eq!(
            encoded_blobs.len(),
            blobs.len(),
            "the number of encoded blobs and the number of blobs must be the same"
        );

        let result = encoded_blobs
            .into_iter()
            .filter_map(|encoded_blob| {
                if let WalrusStoreBlob::Encoded {
                    pairs, metadata, ..
                } = encoded_blob
                {
                    Some((pairs.as_ref().clone(), metadata.as_ref().clone()))
                } else {
                    None
                }
            })
            .collect();

        Ok(result)
    }

    /// Encodes multiple blobs into sliver pairs and metadata, filtering out any that fail.
    /// Returns the successful list of sliver and metadata and logs the indices of failed blobs.
    pub fn encode_blobs_to_pairs_and_metadata_with_identifiers<'a, T: Display + Send + Sync>(
        &self,
        blobs_with_identifiers: Vec<WalrusStoreBlob<'a, T>>,
        encoding_type: EncodingType,
    ) -> ClientResult<Vec<WalrusStoreBlob<'a, T>>> {
        if blobs_with_identifiers.is_empty() {
            return Ok(Vec::new());
        }

        if blobs_with_identifiers.len() > 1 {
            let total_blob_size = blobs_with_identifiers
                .iter()
                .map(|blob| blob.unencoded_length())
                .sum::<usize>();
            let max_total_blob_size = self.config().communication_config.max_total_blob_size;
            if total_blob_size > max_total_blob_size {
                return Err(ClientError::from(ClientErrorKind::Other(
                    format!(
                        "total blob size {} exceeds the maximum limit of {}",
                        total_blob_size, max_total_blob_size
                    )
                    .into(),
                )));
            }
        }

        let multi_pb = Arc::new(MultiProgress::new());

        // Encode each blob into sliver pairs and metadata. Filters out failed blobs and continue.
        let results = blobs_with_identifiers
            .into_par_iter()
            .map(|blob| {
                let multi_pb_clone = multi_pb.clone();
                let unencoded_blob = blob.get_blob();
                blob.with_encode_result(self.encode_pairs_and_metadata(
                    unencoded_blob,
                    encoding_type,
                    multi_pb_clone.as_ref(),
                ))
            })
            .collect::<Vec<_>>();

        Ok(results)
    }

    fn encode_pairs_and_metadata(
        &self,
        blob: &[u8],
        encoding_type: EncodingType,
        multi_pb: &MultiProgress,
    ) -> ClientResult<(Vec<SliverPair>, VerifiedBlobMetadataWithId)> {
        let spinner = multi_pb.add(styled_spinner());
        spinner.set_message("encoding the blob");

        let encode_start_timer = Instant::now();

        let (pairs, metadata) = self
            .encoding_config
            .get_for_type(encoding_type)
            .encode_with_metadata(blob)
            .map_err(ClientError::other)?;

        let duration = encode_start_timer.elapsed();
        let pair = pairs.first().expect("the encoding produces sliver pairs");
        let symbol_size = pair.primary.symbols.symbol_size().get();
        tracing::info!(
            symbol_size,
            primary_sliver_size = pair.primary.symbols.len() * usize::from(symbol_size),
            secondary_sliver_size = pair.secondary.symbols.len() * usize::from(symbol_size),
            ?duration,
            "encoded sliver pairs and metadata"
        );
        spinner.finish_with_message(format!("blob encoded; blob ID: {}", metadata.blob_id()));

        Ok((pairs, metadata))
    }

    /// Stores the blobs on Walrus, reserving space or extending registered blobs, if necessary.
    ///
    /// Returns a [`ClientErrorKind::CommitteeChangeNotified`] error if, during the registration or
    /// store operations, the client is notified that the committee has changed.
    async fn reserve_and_store_encoded_blobs<'a, T: Display + Send + Sync + 'a>(
        &'a self,
        encoded_blobs: Vec<WalrusStoreBlob<'a, T>>,
        epochs_ahead: EpochCount,
        store_when: StoreWhen,
        persistence: BlobPersistence,
        post_store: PostStoreAction,
    ) -> ClientResult<Vec<WalrusStoreBlob<'a, T>>> {
        tracing::info!("storing {} blobs", encoded_blobs.len());
        let status_start_timer = Instant::now();
        let committees = self.get_committees().await?;
        let num_encoded_blobs = encoded_blobs.len();

        // Retrieve the blob status, checking if the committee has changed in the meantime.
        // This operation can be safely interrupted as it does not require a wallet.
        let encoded_blobs_with_status = self
            .await_while_checking_notification(self.get_blob_statuses(encoded_blobs))
            .await?;

        let num_encoded_blobs_with_status = encoded_blobs_with_status.len();
        debug_assert_eq!(
            num_encoded_blobs_with_status, num_encoded_blobs,
            "the number of blob statuses and the number of blobs to store must be the same"
        );
        tracing::info!(
            duration = ?status_start_timer.elapsed(),
            "retrieved {} blob statuses",
            num_encoded_blobs_with_status
        );

        let store_op_timer = Instant::now();
        // Register blobs if they are not registered, and get the store operations.
        let registered_blobs = self
            .resource_manager(&committees)
            .await
            .register_walrus_store_blobs(
                encoded_blobs_with_status,
                epochs_ahead,
                persistence,
                store_when,
            )
            .await?;
        tracing::info!(
            duration = ?store_op_timer.elapsed(),
            "{} blob resources obtained\n{}",
            registered_blobs.len(),
            registered_blobs
                .iter()
                .map(|blob| format!("{:?}", blob.get_operation()))
                .collect::<Vec<_>>()
                .join("\n")
        );
        tracing::info!("debug-store: registered_blobs: {:?}", registered_blobs);
        let num_registered_blobs = registered_blobs.len();
        debug_assert_eq!(
            num_registered_blobs, num_encoded_blobs,
            "the number of registered blobs and the number of blobs to store must be the same \
            (num_registered_blobs = {}, num_encoded_blobs = {})",
            num_registered_blobs, num_encoded_blobs
        );

        let mut final_result: Vec<WalrusStoreBlob<'_, T>> = Vec::with_capacity(num_encoded_blobs);
        let mut to_be_certified: Vec<WalrusStoreBlob<'_, T>> = Vec::new();
        let mut to_be_extended: Vec<WalrusStoreBlob<'_, T>> = Vec::new();

        registered_blobs.into_iter().for_each(|registered_blob| {
            if registered_blob.is_completed() {
                final_result.push(registered_blob);
            } else if registered_blob.needs_extend() {
                to_be_extended.push(registered_blob);
            } else if registered_blob.needs_certify() {
                to_be_certified.push(registered_blob);
            } else {
                panic!("unexpected blob state {:?}", registered_blob);
            }
        });

        let num_to_be_certified = to_be_certified.len();
        debug_assert_eq!(
            num_to_be_certified + to_be_extended.len() + final_result.len(),
            num_registered_blobs,
            "the number of blobs to certify, extend, and store must be the same"
        );

        // Check if the committee has changed while registering the blobs.
        if are_current_previous_different(
            committees.as_ref(),
            self.get_committees().await?.as_ref(),
        ) {
            tracing::warn!("committees have changed while registering blobs");
            return Err(ClientError::from(ClientErrorKind::CommitteeChangeNotified));
        }

        tracing::info!("debug-store: to_be_certified: {:?}", to_be_certified);
        tracing::info!("debug-store: to_be_extended: {:?}", to_be_extended);
        tracing::info!("debug-store: final_result: {:?}", final_result);

        // Get the blob certificates, possibly storing slivers, while checking if the committee has
        // changed in the meantime.
        // This operation can be safely interrupted as it does not require a wallet.
        let blobs_with_certificates = self
            .await_while_checking_notification(self.get_all_blob_certificates(to_be_certified))
            .await?;
        tracing::info!(
            "debug-store blobs with certificates: {:?}, blobs_completed: {:?}",
            blobs_with_certificates.len(),
            final_result.len()
        );
        assert_eq!(blobs_with_certificates.len(), num_to_be_certified);

        // Move completed blobs to final_result and keep only non-completed ones
        let (completed_blobs, to_be_certified): (Vec<_>, Vec<_>) = blobs_with_certificates
            .into_iter()
            .partition(|blob| blob.is_completed());

        final_result.extend(completed_blobs);

        let cert_and_extend_params: Vec<CertifyAndExtendBlobParams> = to_be_extended
            .iter()
            .chain(to_be_certified.iter())
            .filter_map(|blob| blob.get_certify_and_extend_params())
            .collect();

        // Certify all blobs on Sui.
        let sui_cert_timer = Instant::now();
        let cert_and_extend_results = self
            .sui_client
            .certify_and_extend_blobs(&cert_and_extend_params, post_store)
            .await
            .map_err(|e| {
                tracing::warn!(error = %e, "Failure occurred while certifying and extending \
                blobs on Sui");
                ClientError::from(ClientErrorKind::CertificationFailed(e))
            })?;
        tracing::info!(
            duration = ?sui_cert_timer.elapsed(),
            "certified {} blobs on Sui",
            cert_and_extend_params.len()
        );
        tracing::info!(
            "debug-store: cert_and_extend_results: {:?}",
            cert_and_extend_results
        );
        // Build map from BlobId to CertifyAndExtendBlobResult
        let result_map: HashMap<ObjectID, CertifyAndExtendBlobResult> = cert_and_extend_results
            .into_iter()
            .map(|result| (result.blob_object_id, result))
            .collect();

        // Get price computation for completing blobs
        let price_computation = self.get_price_computation().await?;

        // Complete to_be_extended blobs.
        to_be_extended.into_iter().for_each(|blob| {
            let Some(object_id) = blob.get_object_id() else {
                panic!("Invalid blob state {:?}", blob);
            };
            if let Some(result) = result_map.get(&object_id) {
                final_result.push(blob.complete(result.clone(), &price_computation));
            } else {
                panic!("Invalid blob state {:?}", blob);
            }
        });

        // Complete to_be_certified blobs.
        to_be_certified.into_iter().for_each(|blob| {
            let Some(object_id) = blob.get_object_id() else {
                panic!("Invalid blob state {:?}", blob);
            };
            if let Some(result) = result_map.get(&object_id) {
                final_result.push(blob.complete(result.clone(), &price_computation));
            } else {
                panic!("Invalid blob state {:?}", blob);
            }
        });

        // A short circuit if all blobs have errors.
        // This is not ideal since the actual error is wrapped in a `Other` error as a string.
        let mut num_errors = 0;
        let error_msgs: HashSet<_> = final_result
            .iter()
            .filter_map(|blob| {
                let msg = blob.get_error_msg();
                if msg.is_some() {
                    num_errors += 1;
                }
                msg
            })
            .collect();

        if num_errors == final_result.len() && error_msgs.len() == 1 {
            return Err(ClientError::from(ClientErrorKind::Other(
                error_msgs.iter().next().unwrap().to_string().into(),
            )));
        }

        Ok(final_result)
    }

    /// Fetches the status of each blob, and returns a mapping of blob ID to
    /// (pair, metadata, status).
    async fn get_blob_statuses<'a, T: Display + Send + Sync>(
        &'a self,
        encoded_blobs: Vec<WalrusStoreBlob<'a, T>>,
    ) -> ClientResult<Vec<WalrusStoreBlob<'a, T>>> {
        encoded_blobs.iter().for_each(|blob| {
            debug_assert!(blob.is_encoded());
        });

        Ok(
            futures::future::join_all(encoded_blobs.into_iter().map(|encode_blob| async move {
                let blob_id = encode_blob.get_blob_id();
                if let Err(e) = self.check_blob_id(&blob_id) {
                    return encode_blob.with_error(e);
                }
                let result = self
                    .get_blob_status_with_retries(&blob_id, &self.sui_client)
                    .await;
                encode_blob.with_status(result)
            }))
            .await
            .into_iter()
            .collect::<Vec<_>>(),
        )
    }

    /// Fetches the certificates for all the blobs, and returns a vector of
    /// (blob, certificate) pair.
    async fn get_all_blob_certificates<'a, T: Display + Send + Sync>(
        &'a self,
        blobs_to_be_certified: Vec<WalrusStoreBlob<'a, T>>,
    ) -> ClientResult<Vec<WalrusStoreBlob<'a, T>>> {
        if blobs_to_be_certified.is_empty() {
            return Ok(vec![]);
        }

        let get_cert_timer = Instant::now();

        // TODO(joy): add concurrency limit with semaphore.
        let multi_pb = Arc::new(MultiProgress::new());
        let blobs_with_certificates =
            futures::future::join_all(blobs_to_be_certified.into_iter().map(|registered_blob| {
                let operation = registered_blob.get_operation().cloned();
                let Some(StoreOp::RegisterNew { blob, operation }) = operation else {
                    panic!("Expected a RegisterNew operation");
                };
                let multi_pb_arc = Arc::clone(&multi_pb);
                async move {
                    let (Some(pairs), Some(metadata), Some(blob_status)) = (
                        registered_blob.get_sliver_pairs(),
                        registered_blob.get_metadata(),
                        registered_blob.get_status(),
                    ) else {
                        panic!("Expected a blob with status");
                    };
                    let pairs_ref = pairs.iter().collect::<Vec<_>>();
                    let certificate_result = self
                        .get_blob_certificate(
                            &blob,
                            &operation,
                            &pairs_ref,
                            metadata,
                            blob_status,
                            multi_pb_arc.as_ref(),
                        )
                        .await;
                    registered_blob.with_certificate_result(certificate_result)
                }
            }))
            .await;

        tracing::info!(
            duration = ?get_cert_timer.elapsed(),
            "get {} blobs certificates",
            blobs_with_certificates.iter().filter(|blob| blob.is_with_certificate()).count()
        );

        Ok(blobs_with_certificates)
    }

    async fn get_blob_certificate(
        &self,
        blob_object: &Blob,
        resource_operation: &RegisterBlobOp,
        pairs: &[&SliverPair],
        metadata: &VerifiedBlobMetadataWithId,
        blob_status: &BlobStatus,
        multi_pb: &MultiProgress,
    ) -> ClientResult<ConfirmationCertificate> {
        let committees = self.get_committees().await?;

        match blob_status.initial_certified_epoch() {
            Some(certified_epoch) if !committees.is_change_in_progress() => {
                // If the blob is already certified on chain and there is no committee change in
                // progress, all nodes already have the slivers.
                self.get_certificate_standalone(
                    &blob_object.blob_id,
                    certified_epoch,
                    &blob_object.blob_persistence_type(),
                )
                .await
            }
            _ => {
                // If the blob is not certified, we need to store the slivers. Also, during
                // epoch change we may need to store the slivers again for an already certified
                // blob, as the current committee may not have synced them yet.
                if (resource_operation.is_registration() || resource_operation.is_reuse_storage())
                    && !blob_status.is_registered()
                {
                    tracing::debug!(
                        delay=?self.config.communication_config.registration_delay,
                        "waiting to ensure that all storage nodes have seen the registration"
                    );
                    tokio::time::sleep(self.config.communication_config.registration_delay).await;
                }
                let certify_start_timer = Instant::now();
                let result = self
                    .send_blob_data_and_get_certificate(
                        metadata,
                        pairs,
                        &blob_object.blob_persistence_type(),
                        multi_pb,
                    )
                    .await?;
                let duration = certify_start_timer.elapsed();
                let blob_size = blob_object.size;
                tracing::info!(
                    blob_id = %metadata.blob_id(),
                    ?duration,
                    blob_size,
                    "finished sending blob data and collecting certificate"
                );
                Ok(result)
            }
        }
    }

    /// Creates a resource manager for the client.
    pub async fn resource_manager(&self, committees: &ActiveCommittees) -> ResourceManager {
        ResourceManager::new(&self.sui_client, committees.write_committee().epoch)
    }

    // Blob deletion

    /// Returns an iterator over the list of blobs that can be deleted, based on the blob ID.
    pub async fn deletable_blobs_by_id<'a>(
        &self,
        blob_id: &'a BlobId,
    ) -> ClientResult<impl Iterator<Item = Blob> + 'a> {
        let owned_blobs = self
            .sui_client
            .owned_blobs(None, ExpirySelectionPolicy::Valid);
        Ok(owned_blobs
            .await?
            .into_iter()
            .filter(|blob| blob.blob_id == *blob_id && blob.deletable))
    }

    #[tracing::instrument(skip_all, fields(blob_id))]
    /// Deletes all owned blobs that match the blob ID, and returns the number of deleted objects.
    pub async fn delete_owned_blob(&self, blob_id: &BlobId) -> ClientResult<usize> {
        let mut deleted = 0;
        for blob in self.deletable_blobs_by_id(blob_id).await? {
            self.delete_owned_blob_by_object(blob.id).await?;
            deleted += 1;
        }
        Ok(deleted)
    }

    /// Deletes the owned _deletable_ blob on Walrus, specified by Sui Object ID.
    pub async fn delete_owned_blob_by_object(&self, blob_object_id: ObjectID) -> ClientResult<()> {
        tracing::debug!(%blob_object_id, "deleting blob object");
        self.sui_client.delete_blob(blob_object_id).await?;
        Ok(())
    }

    /// For each entry in `node_ids_with_amounts`, stakes the amount of WAL specified by the
    /// second element of the pair with the node represented by the first element of the pair.
    pub async fn stake_with_node_pools(
        &self,
        node_ids_with_amounts: &[(ObjectID, u64)],
    ) -> ClientResult<Vec<StakedWal>> {
        let staked_wal = self
            .sui_client
            .stake_with_pools(node_ids_with_amounts)
            .await?;
        Ok(staked_wal)
    }

    /// Stakes the specified amount of WAL with the node represented by `node_id`.
    pub async fn stake_with_node_pool(&self, node_id: ObjectID, amount: u64) -> ClientResult<()> {
        self.stake_with_node_pools(&[(node_id, amount)]).await?;
        Ok(())
    }

    /// Exchanges the provided amount of SUI (in MIST) for WAL using the specified exchange.
    pub async fn exchange_sui_for_wal(
        &self,
        exchange_id: ObjectID,
        amount: u64,
    ) -> ClientResult<()> {
        Ok(self
            .sui_client
            .exchange_sui_for_wal(exchange_id, amount)
            .await?)
    }

    /// Returns the latest committees from the chain.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn get_latest_committees_in_test(&self) -> Result<ActiveCommittees, ClientError> {
        Ok(ActiveCommittees::from_committees_and_state(
            self.sui_client
                .get_committees_and_state()
                .await
                .map_err(ClientError::other)?,
        ))
    }
}

impl<T> Client<T> {
    /// Adds a [`Blocklist`] to the client that will be checked when storing or reading blobs.
    ///
    /// This can be called again to replace the blocklist.
    pub fn with_blocklist(mut self, blocklist: Blocklist) -> Self {
        self.blocklist = Some(blocklist);
        self
    }

    /// Stores the already-encoded metadata and sliver pairs for a blob into Walrus, by sending
    /// sliver pairs to at least 2f+1 shards.
    ///
    /// Assumes the blob ID has already been registered, with an appropriate blob size.
    #[tracing::instrument(skip_all)]
    pub async fn send_blob_data_and_get_certificate(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
        pairs: &[&SliverPair],
        blob_persistence_type: &BlobPersistenceType,
        multi_pb: &MultiProgress,
    ) -> ClientResult<ConfirmationCertificate> {
        tracing::info!(blob_id = %metadata.blob_id(), "starting to send data to storage nodes");
        let committees = self.get_committees().await?;
        let mut pairs_per_node = self
            .pairs_per_node(metadata.blob_id(), pairs, &committees)
            .await;
        let sliver_write_limit = self
            .communication_limits
            .max_concurrent_sliver_writes_for_blob_size(
                metadata.metadata().unencoded_length(),
                &self.encoding_config,
                metadata.metadata().encoding_type(),
            );
        tracing::debug!(
            blob_id = %metadata.blob_id(),
            communication_limits = sliver_write_limit,
            "establishing node communications"
        );

        let comms = self
            .communication_factory
            .node_write_communications(&committees, Arc::new(Semaphore::new(sliver_write_limit)))?;

        let progress_bar = {
            let pb = styled_progress_bar(bft::min_n_correct(committees.n_shards()).get().into());
            pb.set_message(format!("sending slivers ({})", metadata.blob_id()));
            multi_pb.add(pb)
        };

        let mut requests = WeightedFutures::new(comms.iter().map(|n| {
            n.store_metadata_and_pairs(
                metadata,
                pairs_per_node
                    .remove(&n.node_index)
                    .expect("there are shards for each node"),
                blob_persistence_type,
            )
            .inspect({
                let value = progress_bar.clone();
                move |result| {
                    if result.is_ok() && !value.is_finished() {
                        value.inc(result.1.try_into().expect("the weight fits a usize"))
                    }
                }
            })
        }));
        let start = Instant::now();

        // We do not limit the number of concurrent futures awaited here, because the number of
        // connections is limited through a semaphore depending on the [`max_data_in_flight`][]
        if let CompletedReasonWeight::FuturesConsumed(weight) = requests
            .execute_weight(
                &|weight| {
                    committees
                        .write_committee()
                        .is_at_least_min_n_correct(weight)
                },
                committees.n_shards().get().into(),
            )
            .await
        {
            tracing::debug!(
                elapsed_time = ?start.elapsed(),
                executed_weight = weight,
                responses = ?requests.into_results(),
                blob_id = %metadata.blob_id(),
                "all futures consumed before reaching a threshold of successful responses"
            );
            return Err(self
                .not_enough_confirmations_error(weight, &committees)
                .await);
        }
        tracing::debug!(
            elapsed_time = ?start.elapsed(),
            blob_id = %metadata.blob_id(),
            "stored metadata and slivers onto a quorum of nodes"
        );

        progress_bar.finish_with_message(format!("slivers sent ({})", metadata.blob_id()));

        let extra_time = self
            .config
            .communication_config
            .sliver_write_extra_time
            .extra_time(start.elapsed());

        let spinner = {
            let pb = styled_spinner();
            pb.set_message(format!(
                "waiting at most {} more, to store on additional nodes ({})",
                HumanDuration(extra_time),
                metadata.blob_id()
            ));
            multi_pb.add(pb)
        };

        // Allow extra time for the client to store the slivers.
        let completed_reason = requests
            .execute_time(
                self.config
                    .communication_config
                    .sliver_write_extra_time
                    .extra_time(start.elapsed()),
                committees.n_shards().get().into(),
            )
            .await;
        tracing::debug!(
            elapsed_time = ?start.elapsed(),
            blob_id = %metadata.blob_id(),
            %completed_reason,
            "stored metadata and slivers onto additional nodes"
        );

        spinner.finish_with_message(format!(
            "additional slivers stored ({})",
            metadata.blob_id()
        ));

        let results = requests.into_results();

        self.confirmations_to_certificate(results, &committees)
            .await
    }

    /// Fetches confirmations for a blob from a quorum of nodes and returns the certificate.
    async fn get_certificate_standalone(
        &self,
        blob_id: &BlobId,
        certified_epoch: Epoch,
        blob_persistence_type: &BlobPersistenceType,
    ) -> ClientResult<ConfirmationCertificate> {
        let committees = self.get_committees().await?;
        let comms = self
            .communication_factory
            .node_read_communications(&committees, certified_epoch)?;

        let mut requests = WeightedFutures::new(comms.iter().map(|n| {
            n.get_confirmation_with_retries(blob_id, committees.epoch(), blob_persistence_type)
        }));

        requests
            .execute_weight(
                &|weight| committees.is_quorum(weight),
                self.communication_limits.max_concurrent_sliver_reads,
            )
            .await;
        let results = requests.into_results();

        self.confirmations_to_certificate(results, &committees)
            .await
    }

    /// Combines the received storage confirmations into a single certificate.
    ///
    /// This function _does not_ check that the received confirmations match the current epoch and
    /// blob ID, as it assumes that the storage confirmations were received through
    /// [`NodeCommunication::store_metadata_and_pairs`], which internally verifies it to check the
    /// blob ID, epoch, and blob persistence type.
    async fn confirmations_to_certificate<E: Display>(
        &self,
        confirmations: Vec<NodeResult<SignedStorageConfirmation, E>>,
        committees: &ActiveCommittees,
    ) -> ClientResult<ConfirmationCertificate> {
        let mut aggregate_weight = 0;
        let mut signers = Vec::with_capacity(confirmations.len());
        let mut signed_messages = Vec::with_capacity(confirmations.len());

        for NodeResult(_, weight, node, result) in confirmations {
            match result {
                Ok(confirmation) => {
                    aggregate_weight += weight;
                    signed_messages.push(confirmation);
                    signers.push(
                        u16::try_from(node)
                            .expect("the node index is computed from the vector of members"),
                    );
                }
                Err(error) => tracing::info!(node, %error, "storing metadata and pairs failed"),
            }
        }

        ensure!(
            committees
                .write_committee()
                .is_at_least_min_n_correct(aggregate_weight),
            self.not_enough_confirmations_error(aggregate_weight, committees)
                .await
        );

        let cert =
            ConfirmationCertificate::from_signed_messages_and_indices(signed_messages, signers)
                .map_err(ClientError::other)?;
        Ok(cert)
    }

    async fn not_enough_confirmations_error(
        &self,
        weight: usize,
        committees: &ActiveCommittees,
    ) -> ClientError {
        ClientErrorKind::NotEnoughConfirmations(weight, committees.min_n_correct()).into()
    }

    /// Requests the slivers and decodes them into a blob.
    ///
    /// Returns a [`ClientError`] of kind [`ClientErrorKind::BlobIdDoesNotExist`] if it receives a
    /// quorum (at least 2f+1) of "not found" error status codes from the storage nodes.
    #[tracing::instrument(level = Level::ERROR, skip_all)]
    async fn request_slivers_and_decode<U>(
        &self,
        certified_epoch: Epoch,
        metadata: &VerifiedBlobMetadataWithId,
    ) -> ClientResult<Vec<u8>>
    where
        U: EncodingAxis,
        SliverData<U>: TryFrom<Sliver>,
    {
        let committees = self.get_committees().await?;
        // Create a progress bar to track the progress of the sliver retrieval.
        let progress_bar: indicatif::ProgressBar = styled_progress_bar(
            self.encoding_config
                .get_for_type(metadata.metadata().encoding_type())
                .n_source_symbols::<U>()
                .get()
                .into(),
        );
        progress_bar.set_message("requesting slivers");

        let comms = self
            .communication_factory
            .node_read_communications(&committees, certified_epoch)?;
        // Create requests to get all slivers from all nodes.
        let futures = comms.iter().flat_map(|n| {
            // NOTE: the cloned here is needed because otherwise the compiler complains about the
            // lifetimes of `s`.
            n.node.shard_ids.iter().cloned().map(|s| {
                n.retrieve_verified_sliver::<U>(metadata, s)
                    .instrument(n.span.clone())
                    // Increment the progress bar if the sliver is successfully retrieved.
                    .inspect({
                        let value = progress_bar.clone();
                        move |result| {
                            if result.is_ok() {
                                value.inc(1)
                            }
                        }
                    })
            })
        });
        let mut decoder = self
            .encoding_config
            .get_for_type(metadata.metadata().encoding_type())
            .get_blob_decoder::<U>(metadata.metadata().unencoded_length())
            .map_err(ClientError::other)?;
        // Get the first ~1/3 or ~2/3 of slivers directly, and decode with these.
        let mut requests = WeightedFutures::new(futures);
        let enough_source_symbols = |weight| {
            weight
                >= self
                    .encoding_config
                    .get_for_type(metadata.metadata().encoding_type())
                    .n_source_symbols::<U>()
                    .get()
                    .into()
        };
        requests
            .execute_weight(
                &enough_source_symbols,
                self.communication_limits
                    .max_concurrent_sliver_reads_for_blob_size(
                        metadata.metadata().unencoded_length(),
                        &self.encoding_config,
                        metadata.metadata().encoding_type(),
                    ),
            )
            .await;

        progress_bar.finish_with_message("slivers received");

        let mut n_not_found = 0; // Counts the number of "not found" status codes received.
        let mut n_forbidden = 0; // Counts the number of "forbidden" status codes received.
        let slivers = requests
            .take_results()
            .into_iter()
            .filter_map(|NodeResult(_, _, node, result)| {
                result
                    .map_err(|error| {
                        tracing::debug!(%node, %error, "retrieving sliver failed");
                        if error.is_status_not_found() {
                            n_not_found += 1;
                        } else if error.is_blob_blocked() {
                            n_forbidden += 1;
                        }
                    })
                    .ok()
            })
            .collect::<Vec<_>>();

        if committees.is_quorum(n_not_found + n_forbidden) {
            return if n_not_found > n_forbidden {
                Err(ClientErrorKind::BlobIdDoesNotExist.into())
            } else {
                Err(ClientErrorKind::BlobIdBlocked(*metadata.blob_id()).into())
            };
        }

        if let Some((blob, _meta)) = decoder
            .decode_and_verify(metadata.blob_id(), slivers)
            .map_err(ClientError::other)?
        {
            // We have enough to decode the blob.
            Ok(blob)
        } else {
            // We were not able to decode. Keep requesting slivers and try decoding as soon as every
            // new sliver is received.
            tracing::info!(
                "blob decoding with initial set of slivers failed; requesting additional slivers"
            );
            self.decode_sliver_by_sliver(
                &mut requests,
                &mut decoder,
                metadata,
                n_not_found,
                n_forbidden,
            )
            .await
        }
    }

    /// Decodes the blob of given blob ID by requesting slivers and trying to decode at each new
    /// sliver it receives.
    #[tracing::instrument(level = Level::ERROR, skip_all)]
    async fn decode_sliver_by_sliver<'a, I, Fut, U>(
        &self,
        requests: &mut WeightedFutures<I, Fut, NodeResult<SliverData<U>, NodeError>>,
        decoder: &mut BlobDecoderEnum<'a, U>,
        metadata: &VerifiedBlobMetadataWithId,
        mut n_not_found: usize,
        mut n_forbidden: usize,
    ) -> ClientResult<Vec<u8>>
    where
        U: EncodingAxis,
        I: Iterator<Item = Fut>,
        Fut: Future<Output = NodeResult<SliverData<U>, NodeError>>,
    {
        while let Some(NodeResult(_, _, node, result)) = requests
            .next(
                self.communication_limits
                    .max_concurrent_sliver_reads_for_blob_size(
                        metadata.metadata().unencoded_length(),
                        &self.encoding_config,
                        metadata.metadata().encoding_type(),
                    ),
            )
            .await
        {
            match result {
                Ok(sliver) => {
                    let result = decoder
                        .decode_and_verify(metadata.blob_id(), [sliver])
                        .map_err(ClientError::other)?;
                    if let Some((blob, _meta)) = result {
                        return Ok(blob);
                    }
                }
                Err(error) => {
                    tracing::debug!(%node, %error, "retrieving sliver failed");
                    if error.is_status_not_found() {
                        n_not_found += 1;
                    } else if error.is_blob_blocked() {
                        n_forbidden += 1;
                    }
                    if self
                        .get_committees()
                        .await?
                        .is_quorum(n_not_found + n_forbidden)
                    {
                        return if n_not_found > n_forbidden {
                            Err(ClientErrorKind::BlobIdDoesNotExist.into())
                        } else {
                            Err(ClientErrorKind::BlobIdBlocked(*metadata.blob_id()).into())
                        };
                    }
                }
            }
        }
        // We have exhausted all the slivers but were not able to reconstruct the blob.
        Err(ClientErrorKind::NotEnoughSlivers.into())
    }

    /// Requests the metadata from storage nodes, and keeps the first reply that correctly verifies.
    ///
    /// At a high level:
    /// 1. The function requests a random subset of nodes amounting to at least a quorum (2f+1)
    ///    stake for the metadata.
    /// 1. If the function receives valid metadata for the blob, then it returns the metadata.
    /// 1. Otherwise:
    ///    1. If it received f+1 "not found" status responses, it can conclude that the blob ID was
    ///       not certified and returns an error of kind [`ClientErrorKind::BlobIdDoesNotExist`].
    ///    1. Otherwise, there is some major problem with the network and returns an error of kind
    ///       [`ClientErrorKind::NoMetadataReceived`].
    ///
    /// This procedure works because:
    /// 1. If the blob ID was never certified: Then at least f+1 of the 2f+1 nodes by stake that
    ///    were contacted are correct and have returned a "not found" status response.
    /// 1. If the blob ID was certified: Considering the worst possible case where it was certified
    ///    by 2f+1 stake, of which f was malicious, and the remaining f honest did not receive the
    ///    metadata and have yet to recover it. Then, by quorum intersection, in the 2f+1 that reply
    ///    to the client at least 1 is honest and has the metadata. This one node will provide it
    ///    and the client will know the blob exists.
    ///
    /// Note that if a faulty node returns _valid_ metadata for a blob ID that was however not
    /// certified yet, the client proceeds even if the blob ID was possibly not certified yet. This
    /// instance is not considered problematic, as the client will just continue to retrieving the
    /// slivers and fail there.
    ///
    /// The general problem in this latter case is the difficulty to distinguish correct nodes that
    /// have received the certification of the blob before the others, from malicious nodes that
    /// pretend the certification exists.
    pub async fn retrieve_metadata(
        &self,
        certified_epoch: Epoch,
        blob_id: &BlobId,
    ) -> ClientResult<VerifiedBlobMetadataWithId> {
        let committees = self.get_committees().await?;
        let comms = self
            .communication_factory
            .node_read_communications_quorum(&committees, certified_epoch)?;
        let futures = comms.iter().map(|n| {
            n.retrieve_verified_metadata(blob_id)
                .instrument(n.span.clone())
        });
        // Wait until the first request succeeds
        let mut requests = WeightedFutures::new(futures);
        let just_one = |weight| weight >= 1;
        requests
            .execute_weight(
                &just_one,
                self.communication_limits.max_concurrent_metadata_reads,
            )
            .await;

        let mut n_not_found = 0;
        let mut n_forbidden = 0;
        for NodeResult(_, weight, node, result) in requests.into_results() {
            match result {
                Ok(metadata) => {
                    tracing::debug!(?node, "metadata received");
                    return Ok(metadata);
                }
                Err(error) => {
                    let res = {
                        if error.is_status_not_found() {
                            n_not_found += weight;
                        } else if error.is_blob_blocked() {
                            n_forbidden += weight;
                        }
                        committees.is_quorum(n_not_found + n_forbidden)
                    };
                    if res {
                        // Return appropriate error based on which response type was more common
                        return if n_not_found > n_forbidden {
                            // TODO(giac): now that we check that the blob is certified before
                            // starting to read, this error should not technically happen unless (1)
                            // the client was disconnected while reading, or (2) the bft threshold
                            // was exceeded.
                            Err(ClientErrorKind::BlobIdDoesNotExist.into())
                        } else {
                            Err(ClientErrorKind::BlobIdBlocked(*blob_id).into())
                        };
                    }
                }
            }
        }
        Err(ClientErrorKind::NoMetadataReceived.into())
    }

    /// Retries to get the verified blob status.
    ///
    /// Retries are implemented with backoff, until the fetch succeeds or the maximum number of
    /// retries is reached. If the maximum number of retries is reached, the function returns an
    /// error of kind [`ClientErrorKind::NoValidStatusReceived`].
    #[tracing::instrument(skip_all, fields(%blob_id), err(level = Level::WARN))]
    pub async fn get_blob_status_with_retries<U: ReadClient>(
        &self,
        blob_id: &BlobId,
        read_client: &U,
    ) -> ClientResult<BlobStatus> {
        // The backoff is both the interval between retries and the maximum duration of the retry.
        let backoff = self
            .config
            .backoff_config()
            .get_strategy(ThreadRng::default().next_u64());

        let mut peekable = backoff.peekable();

        while let Some(delay) = peekable.next() {
            let maybe_status = self
                .get_verified_blob_status(blob_id, read_client, delay)
                .await;

            match maybe_status {
                Ok(_) => {
                    return maybe_status;
                }
                Err(client_error)
                    if matches!(client_error.kind(), &ClientErrorKind::BlobIdDoesNotExist) =>
                {
                    return Err(client_error)
                }
                Err(_) => (),
            };

            if peekable.peek().is_some() {
                tracing::debug!(?delay, "fetching blob status failed; retrying after delay");
                tokio::time::sleep(delay).await;
            } else {
                tracing::warn!("fetching blob status failed; no more retries");
            }
        }

        return Err(ClientErrorKind::NoValidStatusReceived.into());
    }

    /// Gets the blob status from multiple nodes and returns the latest status that can be verified.
    ///
    /// The nodes are selected such that at least one correct node is contacted. This function reads
    /// from the latest committee, because, during epoch change, it is the committee that will have
    /// the most up-to-date information on the old and newly certified blobs.
    #[tracing::instrument(skip_all, fields(%blob_id), err(level = Level::WARN))]
    pub async fn get_verified_blob_status<U: ReadClient>(
        &self,
        blob_id: &BlobId,
        read_client: &U,
        timeout: Duration,
    ) -> ClientResult<BlobStatus> {
        tracing::debug!(?timeout, "trying to get blob status");
        let committees = self.get_committees().await?;

        let comms = self
            .communication_factory
            .node_read_communications(&committees, committees.write_committee().epoch)?;
        let futures = comms
            .iter()
            .map(|n| n.get_blob_status(blob_id).instrument(n.span.clone()));
        let mut requests = WeightedFutures::new(futures);
        requests
            .execute_until(
                &|weight| committees.is_quorum(weight),
                timeout,
                self.communication_limits.max_concurrent_status_reads,
            )
            .await;

        // If 2f+1 nodes return a 404 status, we know the blob does not exist.
        let n_not_found = requests
            .inner_err()
            .iter()
            .filter(|err| err.is_status_not_found())
            .count();
        if committees.is_quorum(n_not_found) {
            return Err(ClientErrorKind::BlobIdDoesNotExist.into());
        }

        // Check the received statuses.
        let statuses = requests.take_unique_results_with_aggregate_weight();
        tracing::debug!(?statuses, "received blob statuses from storage nodes");
        let mut statuses_list: Vec<_> = statuses.keys().copied().collect();

        // Going through statuses from later (invalid) to earlier (nonexistent), see implementation
        // of `Ord` and `PartialOrd` for `BlobStatus`.
        statuses_list.sort_unstable();
        for status in statuses_list.into_iter().rev() {
            if committees
                .write_committee()
                .is_above_validity(statuses[&status])
                || verify_blob_status_event(blob_id, status, read_client)
                    .await
                    .is_ok()
            {
                return Ok(status);
            }
        }

        Err(ClientErrorKind::NoValidStatusReceived.into())
    }

    /// Returns a [`ClientError`] with [`ClientErrorKind::BlobIdBlocked`] if the provided blob ID is
    /// contained in the blocklist.
    fn check_blob_id(&self, blob_id: &BlobId) -> ClientResult<()> {
        if let Some(blocklist) = &self.blocklist {
            if blocklist.is_blocked(blob_id) {
                tracing::debug!(%blob_id, "encountered blocked blob ID");
                return Err(ClientErrorKind::BlobIdBlocked(*blob_id).into());
            }
        }
        Ok(())
    }

    /// Returns the shards of the given node in the write committee.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn shards_of(
        &self,
        node_names: &[String],
        committees: &ActiveCommittees,
    ) -> Vec<ShardIndex> {
        committees
            .write_committee()
            .members()
            .iter()
            .filter(|node| node_names.contains(&node.name))
            .flat_map(|node| node.shard_ids.clone())
            .collect::<Vec<_>>()
    }

    /// Maps the sliver pairs to the node in the write committee that holds their shard.
    async fn pairs_per_node<'a>(
        &'a self,
        blob_id: &'a BlobId,
        pairs: &'a [&'a SliverPair],
        committees: &ActiveCommittees,
    ) -> HashMap<usize, Vec<&'a SliverPair>> {
        committees
            .write_committee()
            .members()
            .iter()
            .map(|node| {
                pairs
                    .iter()
                    .filter(|pair| {
                        node.shard_ids
                            .contains(&pair.index().to_shard_index(committees.n_shards(), blob_id))
                    })
                    .copied()
                    .collect::<Vec<_>>()
            })
            .enumerate()
            .collect()
    }

    /// Returns a reference to the encoding config in use.
    pub fn encoding_config(&self) -> &EncodingConfig {
        &self.encoding_config
    }

    /// Returns the inner sui client.
    pub fn sui_client(&self) -> &T {
        &self.sui_client
    }

    /// Returns the inner sui client as mutable reference.
    pub fn sui_client_mut(&mut self) -> &mut T {
        &mut self.sui_client
    }

    /// Returns the config used by the client.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Gets the current active committees and price computation from the cache.
    pub async fn get_committees_and_price(
        &self,
    ) -> ClientResult<(Arc<ActiveCommittees>, PriceComputation)> {
        self.committees_handle
            .send_committees_and_price_request(RequestKind::Get)
            .await
            .map_err(ClientError::other)
    }

    /// Forces a refresh of the committees and price computation.
    pub async fn force_refresh_committees(
        &self,
    ) -> ClientResult<(Arc<ActiveCommittees>, PriceComputation)> {
        self.committees_handle
            .send_committees_and_price_request(RequestKind::Refresh)
            .await
            .map_err(ClientError::other)
    }

    /// Gets the current active committees from the cache.
    pub async fn get_committees(&self) -> ClientResult<Arc<ActiveCommittees>> {
        let (committees, _) = self.get_committees_and_price().await?;
        Ok(committees)
    }

    /// Gets the current price computation from the cache.
    pub async fn get_price_computation(&self) -> ClientResult<PriceComputation> {
        let (_, price_computation) = self.get_committees_and_price().await?;
        Ok(price_computation)
    }
}

/// Verifies the [`BlobStatus`] using the on-chain event.
///
/// This only verifies the [`BlobStatus::Invalid`] and [`BlobStatus::Permanent`] variants and does
/// not check the quoted counts for deletable blobs.
#[tracing::instrument(skip(sui_read_client), err(level = Level::WARN))]
async fn verify_blob_status_event(
    blob_id: &BlobId,
    status: BlobStatus,
    sui_read_client: &impl ReadClient,
) -> Result<(), anyhow::Error> {
    let event = match status {
        BlobStatus::Invalid { event } => event,
        BlobStatus::Permanent { status_event, .. } => status_event,
        BlobStatus::Nonexistent | BlobStatus::Deletable { .. } => return Ok(()),
    };
    tracing::debug!(?event, "verifying blob status with on-chain event");

    let blob_event = sui_read_client.get_blob_event(event).await?;
    anyhow::ensure!(blob_id == &blob_event.blob_id(), "blob ID mismatch");

    match (status, blob_event) {
        (
            BlobStatus::Permanent {
                end_epoch,
                is_certified: false,
                ..
            },
            BlobEvent::Registered(event),
        ) => {
            anyhow::ensure!(end_epoch == event.end_epoch, "end epoch mismatch");
            event.blob_id
        }
        (
            BlobStatus::Permanent {
                end_epoch,
                is_certified: true,
                ..
            },
            BlobEvent::Certified(event),
        ) => {
            anyhow::ensure!(end_epoch == event.end_epoch, "end epoch mismatch");
            event.blob_id
        }
        (BlobStatus::Invalid { .. }, BlobEvent::InvalidBlobID(event)) => event.blob_id,
        (_, _) => Err(anyhow!("blob event does not match status"))?,
    };

    Ok(())
}

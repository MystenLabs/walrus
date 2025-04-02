// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Blob lifecycle in the client.

use std::{fmt::Display, sync::Arc};

use sui_types::base_types::ObjectID;
use tracing::{Level, Span};
use walrus_core::{
    encoding::SliverPair,
    messages::ConfirmationCertificate,
    metadata::{BlobMetadataApi as _, VerifiedBlobMetadataWithId},
    BlobId,
};
use walrus_sdk::api::BlobStatus;
use walrus_sui::client::{CertifyAndExtendBlobParams, CertifyAndExtendBlobResult};

use super::{
    resource::{PriceComputation, RegisterBlobOp, StoreOp},
    responses::BlobStoreResult,
    ClientError,
    ClientResult,
};

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
pub enum WalrusStoreBlob<'a, T: Display + Clone + Send + Sync> {
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
    /// Error occurred during the blob lifecycle.
    Error {
        /// The raw blob data.
        blob: &'a [u8],
        /// A unique identifier for this blob.
        identifier: T,
        /// The blob ID.
        blob_id: Option<BlobId>,
        /// The phase where the error occurred.
        failure_phase: String,
        /// The error.
        error: ClientError,
        /// The span for this blob's lifecycle.
        span: Span,
    },
}

impl<'a, T: Display + Clone + Send + Sync> WalrusStoreBlob<'a, T> {
    /// Creates a new unencoded blob.
    pub fn new_unencoded(blob: &'a [u8], identifier: T) -> Self {
        let span = tracing::span!(
            BLOB_SPAN_LEVEL,
            "store_blob_tracing",
            blob_id = %BlobId::ZERO,  // Initial placeholder value
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

    /// Returns true if the blob has a status.
    pub fn is_with_status(&self) -> bool {
        matches!(self, WalrusStoreBlob::WithStatus { .. })
    }

    /// Returns true if the blob is registered.
    pub fn is_registered(&self) -> bool {
        matches!(self, WalrusStoreBlob::Registered { .. })
    }

    /// Returns true if the blob has a certificate.
    pub fn is_with_certificate(&self) -> bool {
        matches!(self, WalrusStoreBlob::WithCertificate { .. })
    }

    /// Returns true if the store blob operation is completed.
    pub fn is_completed(&self) -> bool {
        matches!(
            self,
            WalrusStoreBlob::Completed { .. } | WalrusStoreBlob::Error { .. }
        )
    }

    /// Returns true if the blob needs to be certified based on its current
    /// state and operation type.
    pub fn needs_certificate(&self) -> bool {
        if let WalrusStoreBlob::Registered {
            operation: StoreOp::RegisterNew { operation, blob },
            ..
        } = self
        {
            if blob.certified_epoch.is_some() {
                return false;
            }

            match operation {
                RegisterBlobOp::ReuseAndExtend { .. } => false,
                // The registered blobs with a longer lifetime have been converted to
                // Complete state.
                RegisterBlobOp::ReuseRegistration { .. }
                | RegisterBlobOp::RegisterFromScratch { .. }
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
            operation: StoreOp::RegisterNew { operation, blob },
            ..
        } = self
        {
            if blob.certified_epoch.is_none() {
                return false;
            }
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
            WalrusStoreBlob::Error { .. } => "Error",
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
            WalrusStoreBlob::Error { blob, .. } => blob,
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
            WalrusStoreBlob::Error { identifier, .. } => identifier,
        }
    }

    /// Returns the error if the blob is in the Error state.
    pub fn get_error(&self) -> Option<&ClientError> {
        match self {
            WalrusStoreBlob::Error { error, .. } => Some(error),
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
            WalrusStoreBlob::Error { blob, .. } => blob.len(),
        }
    }

    /// Returns the encoded length of the blob.
    pub fn encoded_size(&self) -> Option<u64> {
        self.get_metadata()
            .and_then(|metadata| metadata.metadata().encoded_size())
    }

    /// Returns the blob ID associated with this blob.
    pub fn get_blob_id(&self) -> Option<BlobId> {
        match self {
            WalrusStoreBlob::Unencoded { .. } => None,
            WalrusStoreBlob::Encoded { metadata, .. } => Some(*metadata.blob_id()),
            WalrusStoreBlob::WithStatus { metadata, .. } => Some(*metadata.blob_id()),
            WalrusStoreBlob::Registered { metadata, .. } => Some(*metadata.blob_id()),
            WalrusStoreBlob::WithCertificate { metadata, .. } => Some(*metadata.blob_id()),
            WalrusStoreBlob::Completed { result, .. } => Some(*result.blob_id()),
            WalrusStoreBlob::Error { blob_id, .. } => *blob_id,
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
            WalrusStoreBlob::Completed { .. } | WalrusStoreBlob::Error { .. } => None,
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
            WalrusStoreBlob::Completed { .. } | WalrusStoreBlob::Error { .. } => None,
        }
    }

    /// Returns a reference to the blob status if available in the current state.
    pub fn get_status(&self) -> Option<&BlobStatus> {
        match self {
            WalrusStoreBlob::Unencoded { .. } | WalrusStoreBlob::Encoded { .. } => None,
            WalrusStoreBlob::WithStatus { status, .. } => Some(status),
            WalrusStoreBlob::Registered { status, .. } => Some(status),
            WalrusStoreBlob::WithCertificate { status, .. } => Some(status),
            WalrusStoreBlob::Completed { .. } | WalrusStoreBlob::Error { .. } => None,
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
            WalrusStoreBlob::Completed { .. } | WalrusStoreBlob::Error { .. } => None,
        }
    }

    /// Returns a reference to the final result if the blob is in the Completed state.
    pub fn get_result(&self) -> Option<BlobStoreResult> {
        match self {
            WalrusStoreBlob::Completed { result, .. } => Some(result.clone()),
            WalrusStoreBlob::Error { blob_id, error, .. } => Some(BlobStoreResult::Error {
                blob_id: *blob_id,
                error_msg: error.to_string(),
            }),
            _ => None,
        }
    }

    /// Processes the encoding result and transitions the blob to the appropriate next state.
    ///
    /// If the encoding succeeds, the blob is transitioned to the Encoded state.
    /// If the encoding fails, the blob is transitioned to the Completed state with an error.
    pub fn with_encode_result(
        self,
        result: Result<(Vec<SliverPair>, VerifiedBlobMetadataWithId), ClientError>,
    ) -> ClientResult<Self> {
        {
            let _enter = self.get_span().enter();
            tracing::event!(
                BLOB_SPAN_LEVEL,
                operation = "with_encode_result",
                result = ?result
            );
        }

        let new_state = if let WalrusStoreBlob::Unencoded {
            blob,
            identifier,
            span,
        } = self
        {
            match result {
                Ok((pairs, metadata)) => {
                    let blob_id = *metadata.blob_id();
                    span.record("blob_id", blob_id.to_string());
                    WalrusStoreBlob::Encoded {
                        blob,
                        identifier,
                        pairs: Arc::new(pairs),
                        metadata: Arc::new(metadata),
                        span: span.clone(),
                    }
                }
                Err(error) => WalrusStoreBlob::Error {
                    blob,
                    identifier,
                    blob_id: None,
                    failure_phase: "encode".to_string(),
                    error,
                    span: span.clone(),
                },
            }
        } else {
            return Err(ClientError::store_blob_internal(format!(
                "Invalid operation for blob {:?}, \
                    encode_result: {:?}",
                self, result
            )));
        };

        {
            let _enter = new_state.get_span().enter();
            tracing::event!(BLOB_SPAN_LEVEL,
                operation = "with_encode_result completed",
                state = ?new_state
            );
        }

        Ok(new_state)
    }

    /// Processes the status result and transitions the blob to the appropriate next state.
    pub fn with_status(self, status: Result<BlobStatus, ClientError>) -> ClientResult<Self> {
        {
            let _enter = self.get_span().enter();
            tracing::event!(
                BLOB_SPAN_LEVEL,
                operation = "with_status",
                status = ?status
            );
        }

        let new_state = if let WalrusStoreBlob::Encoded {
            blob,
            identifier,
            pairs,
            metadata,
            span,
        } = self
        {
            match status {
                Ok(status) => WalrusStoreBlob::WithStatus {
                    blob,
                    identifier,
                    pairs,
                    metadata,
                    status,
                    span: span.clone(),
                },
                Err(error) => {
                    if Self::should_fail_early(&error) {
                        return Err(error);
                    }
                    let blob_id = *metadata.blob_id();
                    WalrusStoreBlob::Error {
                        blob,
                        identifier,
                        blob_id: Some(blob_id),
                        failure_phase: "status".to_string(),
                        error,
                        span: span.clone(),
                    }
                }
            }
        } else {
            return Err(ClientError::store_blob_internal(format!(
                "Invalid operation for blob {:?}, \
                    status: {:?}",
                self, status
            )));
        };

        {
            let _enter = new_state.get_span().enter();
            tracing::event!(
                BLOB_SPAN_LEVEL,
                operation = "with_status completed",
                state = ?new_state
            );
        }

        Ok(new_state)
    }

    /// Updates the blob with the result of the register operation and transitions
    /// to the appropriate next state.
    pub fn with_register_result(self, result: Result<StoreOp, ClientError>) -> ClientResult<Self> {
        {
            let _enter = self.get_span().enter();
            tracing::event!(
                BLOB_SPAN_LEVEL,
                operation = "with_register_result",
                result = ?result
            );
        }
        let new_state = if let WalrusStoreBlob::WithStatus {
            blob,
            identifier,
            pairs,
            metadata,
            status,
            span,
        } = self
        {
            match result {
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
                Err(error) => {
                    if Self::should_fail_early(&error) {
                        return Err(error);
                    }
                    let blob_id = *metadata.blob_id();
                    WalrusStoreBlob::Error {
                        blob,
                        identifier,
                        blob_id: Some(blob_id),
                        failure_phase: "register".to_string(),
                        error,
                        span: span.clone(),
                    }
                }
            }
        } else {
            return Err(ClientError::store_blob_internal(format!(
                "Invalid operation for blob {:?}, \
                    register_result: {:?}",
                self, result
            )));
        };

        {
            let _enter = new_state.get_span().enter();
            tracing::event!(
                BLOB_SPAN_LEVEL,
                operation = "with_register_result completed",
                state = ?new_state
            );
        }

        Ok(new_state)
    }

    /// Updates the blob with the result of the certificate operation and transitions
    /// to the appropriate next state.
    pub fn with_certificate_result(
        self,
        certificate_result: ClientResult<ConfirmationCertificate>,
    ) -> ClientResult<Self> {
        {
            let _enter = self.get_span().enter();
            tracing::event!(
                BLOB_SPAN_LEVEL,
                operation = "with_certificate_result",
                certificate_result = ?certificate_result
            );
        }

        let new_state = if let WalrusStoreBlob::Registered {
            blob,
            identifier,
            pairs,
            metadata,
            status,
            operation,
            span,
        } = self
        {
            match certificate_result {
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
                Err(error) => {
                    if Self::should_fail_early(&error) {
                        return Err(error);
                    }
                    let blob_id = *metadata.blob_id();
                    WalrusStoreBlob::Error {
                        blob,
                        identifier,
                        blob_id: Some(blob_id),
                        failure_phase: "certificate".to_string(),
                        error,
                        span: span.clone(),
                    }
                }
            }
        } else {
            return Err(ClientError::store_blob_internal(format!(
                "Invalid operation for blob {:?}, \
                    certificate_result: {:?}",
                self, certificate_result
            )));
        };

        {
            let _enter = new_state.get_span().enter();
            tracing::event!(
                BLOB_SPAN_LEVEL,
                operation = "certificate completed",
                state = ?new_state
            );
        }

        Ok(new_state)
    }

    /// Converts the current blob state to a Completed state with an error result.
    pub fn with_error(self, error: ClientError) -> ClientResult<Self> {
        {
            let _enter = self.get_span().enter();
            tracing::event!(
                BLOB_SPAN_LEVEL,
                operation = "with_error",
                error = ?error
            );
        }
        if Self::should_fail_early(&error) {
            return Err(error);
        }
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
            WalrusStoreBlob::Error {
                blob,
                identifier,
                span,
                ..
            } => (blob, identifier, span),
        };

        let new_state = WalrusStoreBlob::Error {
            blob,
            identifier,
            blob_id,
            failure_phase: "with_error".to_string(),
            error,
            span,
        };

        {
            let _enter = new_state.get_span().enter();
            tracing::event!(BLOB_SPAN_LEVEL,
                operation = "with_error completed", state = ?new_state);
        }

        Ok(new_state)
    }

    /// Updates the blob with the provided result and transitions to the Completed state.
    ///
    /// This update is forced, even if the blob is in the Error state.
    pub fn complete_with(self, result: BlobStoreResult) -> Self {
        {
            let _enter = self.get_span().enter();
            tracing::event!(
                BLOB_SPAN_LEVEL,
                operation = "complete_with",
                result = ?result
            );
        }

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
            }
            | WalrusStoreBlob::Error {
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
    ) -> ClientResult<Self> {
        let store_blob_result = self.compute_store_blob_result(result, price_computation)?;
        Ok(self.complete_with(store_blob_result))
    }

    /// Computes the final store result based on the current state and operation.
    fn compute_store_blob_result(
        &self,
        result: CertifyAndExtendBlobResult,
        price_computation: &PriceComputation,
    ) -> ClientResult<BlobStoreResult> {
        match self {
            WalrusStoreBlob::WithCertificate { operation, .. } => {
                if let StoreOp::RegisterNew { operation, blob } = operation {
                    Ok(BlobStoreResult::NewlyCreated {
                        blob_object: blob.clone(),
                        resource_operation: operation.clone(),
                        cost: price_computation.operation_cost(operation),
                        shared_blob_object: result.shared_blob_object(),
                    })
                } else {
                    Err(ClientError::store_blob_internal(format!(
                        "Invalid operation for blob {:?}, operation: {:?}",
                        self, operation
                    )))
                }
            }
            WalrusStoreBlob::Registered { operation, .. } => match operation {
                StoreOp::RegisterNew { operation, blob } => Ok(BlobStoreResult::NewlyCreated {
                    blob_object: blob.clone(),
                    resource_operation: operation.clone(),
                    cost: price_computation.operation_cost(operation),
                    shared_blob_object: result.shared_blob_object(),
                }),
                _ => Err(ClientError::store_blob_internal(format!(
                    "Invalid operation for blob {:?}, operation: {:?}",
                    self, operation
                ))),
            },
            _ => Err(ClientError::store_blob_internal(format!(
                "Invalid state for computing store blob result {:?}",
                self
            ))),
        }
    }

    /// Returns the parameters needed for certifying and extending the blob based on its
    /// current state.
    pub fn get_certify_and_extend_params(&self) -> Option<CertifyAndExtendBlobParams> {
        match self {
            WalrusStoreBlob::WithCertificate {
                operation:
                    StoreOp::RegisterNew {
                        operation: RegisterBlobOp::ReuseAndExtend { .. },
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
                            | RegisterBlobOp::ReuseStorage { .. }
                            | RegisterBlobOp::ReuseRegistration { .. },
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
            WalrusStoreBlob::Error { .. } => None,
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
            WalrusStoreBlob::Error { span, .. } => span,
        }
    }

    /// Returns true if we should fail early based on the error.
    fn should_fail_early(error: &ClientError) -> bool {
        error.may_be_caused_by_epoch_change() || error.is_no_valid_status_received()
    }
}

impl<T: Display + Clone + Send + Sync> std::fmt::Debug for WalrusStoreBlob<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let _enter = self.get_span().enter();
        let mut debug = f.debug_struct("WalrusStoreBlob");

        debug.field("blob_id", &self.get_blob_id());
        debug.field("identifier", &self.get_identifier().to_string());
        debug.field("state", &self.get_state());

        if let Some(op) = self.get_operation() {
            debug.field("operation", op);
        }
        if let Some(status) = self.get_status() {
            debug.field("status", status);
        }
        if let Some(result) = self.get_result() {
            debug.field("result", &result);
        }

        debug.finish()
    }
}

impl<T: Display + Clone + Send + Sync> Clone for WalrusStoreBlob<'_, T> {
    fn clone(&self) -> Self {
        match self {
            WalrusStoreBlob::Unencoded {
                blob,
                identifier,
                span,
            } => WalrusStoreBlob::Unencoded {
                blob,
                identifier: identifier.clone(),
                span: span.clone(),
            },
            WalrusStoreBlob::Encoded {
                blob,
                identifier,
                pairs,
                metadata,
                span,
            } => WalrusStoreBlob::Encoded {
                blob,
                identifier: identifier.clone(),
                pairs: pairs.clone(),
                metadata: metadata.clone(),
                span: span.clone(),
            },
            _ => panic!("Clone not supported for this variant"),
        }
    }
}

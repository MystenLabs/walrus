// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Blob lifecycle in the client.

use std::{fmt::Debug, sync::Arc};

use enum_dispatch::enum_dispatch;
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
    responses::{BlobStoreResult, EventOrObjectId},
    ClientError,
    ClientResult,
};

/// The log level for all WalrusStoreBlob spans
pub(crate) const BLOB_SPAN_LEVEL: Level = Level::INFO;

/// API for all blob storage variants.
#[enum_dispatch]
pub trait WalrusStoreBlobApi<'a, T: Debug + Clone + Send + Sync> {
    /// Returns a reference to the raw blob data.
    fn get_blob(&self) -> &'a [u8];

    /// Returns a reference to the blob's identifier.
    fn get_identifier(&self) -> &T;

    /// Returns the encoded size of the blob if available.
    fn encoded_size(&self) -> Option<u64>;

    /// Returns the length of the unencoded blob data in bytes.
    fn unencoded_length(&self) -> usize;

    /// Returns a reference to the verified metadata if available.
    fn get_metadata(&self) -> Option<&VerifiedBlobMetadataWithId>;

    /// Returns a reference to the sliver pairs if available.
    fn get_sliver_pairs(&self) -> Option<&Vec<SliverPair>>;

    /// Returns a reference to the blob status if available.
    fn get_status(&self) -> Option<&BlobStatus>;

    /// Returns a reference to the store operation if available.
    fn get_operation(&self) -> Option<&StoreOp>;

    /// Returns the error if available.
    fn get_error(&self) -> Option<&ClientError>;

    /// Returns the final result if available.
    fn get_result(&self) -> Option<BlobStoreResult>;

    /// Returns a string representation of the current state.
    fn get_state(&self) -> &str;

    /// Returns the object ID if available from the current operation.
    fn get_object_id(&self) -> Option<ObjectID>;

    /// Returns true if the blob needs to be certified.
    fn needs_certificate(&self) -> bool;

    /// Returns true if the blob needs to be extended.
    fn needs_extend(&self) -> bool;

    /// Returns the parameters for certifying and extending the blob.
    fn get_certify_and_extend_params(&self) -> Result<CertifyAndExtendBlobParams, ClientError>;

    /// Returns the blob ID if available.
    fn get_blob_id(&self) -> Option<BlobId>;

    /// Processes the encoding result and transitions the blob to the appropriate next state.
    ///
    /// If the encoding succeeds, the blob is transitioned to the Encoded state.
    /// If the encoding fails, the blob is transitioned to the Completed state with an error.
    fn with_encode_result(
        self,
        result: Result<(Vec<SliverPair>, VerifiedBlobMetadataWithId), ClientError>,
    ) -> ClientResult<WalrusStoreBlob<'a, T>>;

    /// Processes the status result and transitions the blob to the appropriate next state.
    fn with_status(
        self,
        status: Result<BlobStatus, ClientError>,
    ) -> ClientResult<WalrusStoreBlob<'a, T>>;

    /// Tries to complete the blob if it is certified beyond the given epoch.
    fn try_complete_if_certified_beyond_epoch(
        self,
        target_epoch: u32,
    ) -> ClientResult<WalrusStoreBlob<'a, T>>;

    /// Updates the blob with the result of the register operation and transitions
    /// to the appropriate next state.
    fn with_register_result(
        self,
        result: Result<StoreOp, ClientError>,
    ) -> ClientResult<WalrusStoreBlob<'a, T>>;

    /// Updates the blob with the result of the certificate operation and transitions
    /// to the appropriate next state.
    fn with_certificate_result(
        self,
        certificate_result: ClientResult<ConfirmationCertificate>,
    ) -> ClientResult<WalrusStoreBlob<'a, T>>;

    /// Converts the current blob state to a Completed state with an error result.
    fn with_error(self, error: ClientError) -> ClientResult<WalrusStoreBlob<'a, T>>;

    /// Updates the blob with the provided result and transitions to the Completed state.
    ///
    /// This update is forced, even if the blob is in the Error state.
    fn complete_with(self, result: BlobStoreResult) -> WalrusStoreBlob<'a, T>;

    /// Updates the blob with the result of the complete operation and
    /// transitions to the Completed state.
    fn with_certify_and_extend_result(
        self,
        result: CertifyAndExtendBlobResult,
        price_computation: &PriceComputation,
    ) -> ClientResult<WalrusStoreBlob<'a, T>>;
}

/// A blob that is being stored in Walrus, representing its current phase in the lifecycle.
#[enum_dispatch(WalrusStoreBlobApi<T>)]
#[derive(Clone, Debug)]
pub enum WalrusStoreBlob<'a, T: Debug + Clone + Send + Sync> {
    /// Initial state before encoding.
    Unencoded(UnencodedBlob<'a, T>),
    /// After encoding, contains the encoded data and metadata.
    Encoded(EncodedBlob<'a, T>),
    /// After status check, includes the blob status.
    WithStatus(BlobWithStatus<'a, T>),
    /// After registration, includes the store operation.
    Registered(RegisteredBlob<'a, T>),
    /// After certificate, includes the certificate.
    WithCertificate(BlobWithCertificate<'a, T>),
    /// Final phase with the complete result.
    Completed(CompletedBlob<'a, T>),
    /// Error occurred during the blob lifecycle.
    Error(FailedBlob<'a, T>),
}

impl<'a, T: Debug + Clone + Send + Sync> WalrusStoreBlob<'a, T> {
    /// Returns true if we should fail early based on the error.
    fn should_fail_early(error: &ClientError) -> bool {
        error.may_be_caused_by_epoch_change() || error.is_no_valid_status_received()
    }

    /// Creates a new unencoded blob.
    pub fn new_unencoded(blob: &'a [u8], identifier: T) -> Self {
        let span = tracing::span!(
            BLOB_SPAN_LEVEL,
            "store_blob_tracing",
            blob_id = %BlobId::ZERO,  // Initial placeholder value
            identifier = ?identifier
        );

        WalrusStoreBlob::Unencoded(UnencodedBlob {
            blob,
            identifier,
            span,
        })
    }

    /// Returns true if the blob is in the Encoded state.
    pub fn is_encoded(&self) -> bool {
        matches!(self, WalrusStoreBlob::Encoded(..))
    }

    /// Returns true if the blob has a status.
    pub fn is_with_status(&self) -> bool {
        matches!(self, WalrusStoreBlob::WithStatus(..))
    }

    /// Returns true if the blob is registered.
    pub fn is_registered(&self) -> bool {
        matches!(self, WalrusStoreBlob::Registered(..))
    }

    /// Returns true if the blob has a certificate.
    pub fn is_with_certificate(&self) -> bool {
        matches!(self, WalrusStoreBlob::WithCertificate(..))
    }

    /// Returns true if the store blob operation is completed.
    pub fn is_completed(&self) -> bool {
        matches!(
            self,
            WalrusStoreBlob::Completed(..) | WalrusStoreBlob::Error(..)
        )
    }

    /// Returns true if the store blob operation is failed.
    pub fn is_failed(&self) -> bool {
        matches!(self, WalrusStoreBlob::Error(..))
    }

    /// Returns true if the blob is in the Error state.
    fn get_span(&self) -> &Span {
        match self {
            WalrusStoreBlob::Unencoded(inner) => &inner.span,
            WalrusStoreBlob::Encoded(inner) => &inner.span,
            WalrusStoreBlob::WithStatus(inner) => &inner.span,
            WalrusStoreBlob::Registered(inner) => &inner.span,
            WalrusStoreBlob::WithCertificate(inner) => &inner.span,
            WalrusStoreBlob::Completed(inner) => &inner.span,
            WalrusStoreBlob::Error(inner) => &inner.span,
        }
    }
}

/// Unencoded blob.
#[derive(Clone)]
pub struct UnencodedBlob<'a, T: Debug + Clone + Send + Sync> {
    /// The raw blob data to be stored.
    pub blob: &'a [u8],
    /// A unique identifier for this blob.
    pub identifier: T,
    /// The span for this blob's lifecycle.
    pub span: Span,
}

impl<'a, T: Debug + Clone + Send + Sync> WalrusStoreBlobApi<'a, T> for UnencodedBlob<'a, T> {
    fn get_blob(&self) -> &'a [u8] {
        self.blob
    }

    fn get_identifier(&self) -> &T {
        &self.identifier
    }

    fn get_blob_id(&self) -> Option<BlobId> {
        None
    }

    fn get_state(&self) -> &str {
        "Unencoded"
    }

    fn get_status(&self) -> Option<&BlobStatus> {
        None
    }

    fn encoded_size(&self) -> Option<u64> {
        None
    }

    fn unencoded_length(&self) -> usize {
        self.blob.len()
    }

    fn get_metadata(&self) -> Option<&VerifiedBlobMetadataWithId> {
        None
    }

    fn get_sliver_pairs(&self) -> Option<&Vec<SliverPair>> {
        None
    }

    fn get_operation(&self) -> Option<&StoreOp> {
        None
    }

    fn get_error(&self) -> Option<&ClientError> {
        None
    }

    fn get_result(&self) -> Option<BlobStoreResult> {
        None
    }

    fn get_object_id(&self) -> Option<ObjectID> {
        None
    }

    fn needs_certificate(&self) -> bool {
        false
    }

    fn needs_extend(&self) -> bool {
        false
    }

    fn get_certify_and_extend_params(&self) -> Result<CertifyAndExtendBlobParams, ClientError> {
        Err(ClientError::store_blob_internal(format!(
            "Invalid operation for blob {:?}",
            self,
        )))
    }

    fn with_encode_result(
        self,
        result: Result<(Vec<SliverPair>, VerifiedBlobMetadataWithId), ClientError>,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        {
            let _enter = self.span.enter();
            tracing::event!(
                BLOB_SPAN_LEVEL,
                operation = "with_encode_result",
                result = ?result
            );
        }

        let new_state = match result {
            Ok((pairs, metadata)) => {
                let blob_id = *metadata.blob_id();
                self.span.record("blob_id", blob_id.to_string());
                WalrusStoreBlob::Encoded(EncodedBlob {
                    blob: self.blob,
                    identifier: self.identifier,
                    pairs: Arc::new(pairs),
                    metadata: Arc::new(metadata),
                    span: self.span.clone(),
                })
            }
            Err(error) => WalrusStoreBlob::Error(FailedBlob {
                blob: self.blob,
                identifier: self.identifier,
                blob_id: None,
                failure_phase: "encode".to_string(),
                error,
                span: self.span.clone(),
            }),
        };

        {
            let _enter = new_state.get_span().enter();
            tracing::event!(
                BLOB_SPAN_LEVEL,
                operation = "with_encode_result completed",
                state = ?new_state
            );
        }

        Ok(new_state)
    }

    fn complete_with(self, result: BlobStoreResult) -> WalrusStoreBlob<'a, T> {
        WalrusStoreBlob::Completed(CompletedBlob {
            blob: self.get_blob(),
            identifier: self.get_identifier().clone(),
            result,
            span: self.get_span().clone(),
        })
    }

    fn with_status(
        self,
        status: Result<BlobStatus, ClientError>,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(ClientError::store_blob_internal(format!(
            "Invalid operation for blob {:?}, status: {:?}",
            self, status,
        )))
    }

    fn try_complete_if_certified_beyond_epoch(
        self,
        target_epoch: u32,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(ClientError::store_blob_internal(format!(
            "Invalid operation for blob {:?}, target_epoch: {:?}",
            self, target_epoch,
        )))
    }

    fn with_register_result(
        self,
        result: Result<StoreOp, ClientError>,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(ClientError::store_blob_internal(format!(
            "Invalid operation for blob {:?}, result: {:?}",
            self, result,
        )))
    }

    fn with_certificate_result(
        self,
        certificate_result: ClientResult<ConfirmationCertificate>,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(ClientError::store_blob_internal(format!(
            "Invalid operation for blob {:?}, certificate_result: {:?}",
            self, certificate_result,
        )))
    }

    fn with_error(self, error: ClientError) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(ClientError::store_blob_internal(format!(
            "Invalid operation for blob {:?}, error: {:?}",
            self, error,
        )))
    }

    fn with_certify_and_extend_result(
        self,
        result: CertifyAndExtendBlobResult,
        price_computation: &PriceComputation,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(ClientError::store_blob_internal(format!(
            "Invalid operation for blob {:?}, result: {:?}, price_computation: {:?}",
            self, result, price_computation,
        )))
    }
}

impl<T: Debug + Clone + Send + Sync> UnencodedBlob<'_, T> {
    fn get_span(&self) -> &Span {
        &self.span
    }
}

impl<T: Debug + Clone + Send + Sync> std::fmt::Debug for UnencodedBlob<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnencodedBlob")
            .field("identifier", &self.identifier)
            .field("blob_len", &self.blob.len())
            .finish()
    }
}

/// Encoded blob.
#[derive(Clone)]
pub struct EncodedBlob<'a, T: Debug + Clone + Send + Sync> {
    /// The raw blob data that was encoded.
    pub blob: &'a [u8],
    /// A unique identifier for this blob.
    pub identifier: T,
    /// The encoded sliver pairs generated from the blob.
    pub pairs: Arc<Vec<SliverPair>>,
    /// The verified metadata associated with the encoded blob.
    pub metadata: Arc<VerifiedBlobMetadataWithId>,
    /// The span for this blob's lifecycle.
    pub span: Span,
}

impl<'a, T: Debug + Clone + Send + Sync> WalrusStoreBlobApi<'a, T> for EncodedBlob<'a, T> {
    fn get_blob(&self) -> &'a [u8] {
        self.blob
    }

    fn get_identifier(&self) -> &T {
        &self.identifier
    }

    fn get_blob_id(&self) -> Option<BlobId> {
        Some(*self.metadata.blob_id())
    }

    fn get_metadata(&self) -> Option<&VerifiedBlobMetadataWithId> {
        Some(&self.metadata)
    }

    fn get_sliver_pairs(&self) -> Option<&Vec<SliverPair>> {
        Some(&self.pairs)
    }

    fn get_state(&self) -> &str {
        "Encoded"
    }

    fn encoded_size(&self) -> Option<u64> {
        self.metadata.metadata().encoded_size()
    }

    fn unencoded_length(&self) -> usize {
        self.blob.len()
    }

    fn get_operation(&self) -> Option<&StoreOp> {
        None
    }

    fn get_error(&self) -> Option<&ClientError> {
        None
    }

    fn get_result(&self) -> Option<BlobStoreResult> {
        None
    }

    fn get_object_id(&self) -> Option<ObjectID> {
        None
    }

    fn needs_certificate(&self) -> bool {
        false
    }

    fn needs_extend(&self) -> bool {
        false
    }

    fn get_certify_and_extend_params(&self) -> Result<CertifyAndExtendBlobParams, ClientError> {
        Err(ClientError::store_blob_internal(format!(
            "Invalid operation for blob {:?}",
            self,
        )))
    }

    fn get_status(&self) -> Option<&BlobStatus> {
        None
    }

    fn with_status(
        self,
        status: Result<BlobStatus, ClientError>,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(ClientError::store_blob_internal(format!(
            "Invalid operation for blob {:?}, status: {:?}",
            self, status,
        )))
    }

    fn try_complete_if_certified_beyond_epoch(
        self,
        target_epoch: u32,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(ClientError::store_blob_internal(format!(
            "Invalid operation for blob {:?}, target_epoch: {:?}",
            self, target_epoch,
        )))
    }

    fn with_register_result(
        self,
        result: Result<StoreOp, ClientError>,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(ClientError::store_blob_internal(format!(
            "Invalid operation for blob {:?}, result: {:?}",
            self, result,
        )))
    }

    fn with_certificate_result(
        self,
        certificate_result: ClientResult<ConfirmationCertificate>,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(ClientError::store_blob_internal(format!(
            "Invalid operation for blob {:?}, certificate_result: {:?}",
            self, certificate_result,
        )))
    }

    fn with_error(self, error: ClientError) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(ClientError::store_blob_internal(format!(
            "Invalid operation for blob {:?}, error: {:?}",
            self, error,
        )))
    }

    fn with_certify_and_extend_result(
        self,
        result: CertifyAndExtendBlobResult,
        price_computation: &PriceComputation,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(ClientError::store_blob_internal(format!(
            "Invalid operation for blob {:?}, result: {:?}, price_computation: {:?}",
            self, result, price_computation,
        )))
    }

    fn with_encode_result(
        self,
        result: Result<(Vec<SliverPair>, VerifiedBlobMetadataWithId), ClientError>,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(ClientError::store_blob_internal(format!(
            "Invalid operation for blob {:?}, result: {:?}",
            self, result,
        )))
    }

    fn complete_with(self, result: BlobStoreResult) -> WalrusStoreBlob<'a, T> {
        WalrusStoreBlob::Completed(CompletedBlob {
            blob: self.get_blob(),
            identifier: self.get_identifier().clone(),
            result,
            span: self.get_span().clone(),
        })
    }
}

impl<T: Debug + Clone + Send + Sync> EncodedBlob<'_, T> {
    fn get_span(&self) -> &Span {
        &self.span
    }
}

impl<T: Debug + Clone + Send + Sync> std::fmt::Debug for EncodedBlob<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncodedBlob")
            .field("identifier", &self.identifier)
            .field("blob_id", &self.metadata.blob_id())
            .field("blob_len", &self.blob.len())
            .finish()
    }
}

/// Encoded blob with status information.
#[derive(Clone)]
pub struct BlobWithStatus<'a, T: Debug + Clone + Send + Sync> {
    /// The raw blob data.
    pub blob: &'a [u8],
    /// A unique identifier for this blob.
    pub identifier: T,
    /// The encoded sliver pairs.
    pub pairs: Arc<Vec<SliverPair>>,
    /// The verified metadata associated with the blob.
    pub metadata: Arc<VerifiedBlobMetadataWithId>,
    /// The current status of the blob in the system.
    pub status: BlobStatus,
    /// The span for this blob's lifecycle.
    pub span: Span,
}

impl<'a, T: Debug + Clone + Send + Sync> WalrusStoreBlobApi<'a, T> for BlobWithStatus<'a, T> {
    fn get_blob(&self) -> &'a [u8] {
        self.blob
    }

    fn get_identifier(&self) -> &T {
        &self.identifier
    }

    fn get_blob_id(&self) -> Option<BlobId> {
        Some(*self.metadata.blob_id())
    }

    fn get_metadata(&self) -> Option<&VerifiedBlobMetadataWithId> {
        Some(&self.metadata)
    }

    fn get_sliver_pairs(&self) -> Option<&Vec<SliverPair>> {
        Some(&self.pairs)
    }

    fn get_status(&self) -> Option<&BlobStatus> {
        Some(&self.status)
    }

    fn get_state(&self) -> &str {
        "WithStatus"
    }

    fn encoded_size(&self) -> Option<u64> {
        self.metadata.metadata().encoded_size()
    }

    fn unencoded_length(&self) -> usize {
        self.blob.len()
    }

    fn get_operation(&self) -> Option<&StoreOp> {
        None
    }

    fn get_error(&self) -> Option<&ClientError> {
        None
    }

    fn get_result(&self) -> Option<BlobStoreResult> {
        None
    }

    fn get_object_id(&self) -> Option<ObjectID> {
        None
    }

    fn needs_certificate(&self) -> bool {
        false
    }

    fn needs_extend(&self) -> bool {
        false
    }

    fn get_certify_and_extend_params(&self) -> Result<CertifyAndExtendBlobParams, ClientError> {
        Err(ClientError::store_blob_internal(format!(
            "Invalid operation for blob {:?}",
            self,
        )))
    }

    fn with_status(
        self,
        status: Result<BlobStatus, ClientError>,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        {
            let _enter = self.span.enter();
            tracing::event!(
                BLOB_SPAN_LEVEL,
                operation = "with_status",
                status = ?status
            );
        }

        let new_state = match status {
            Ok(status) => WalrusStoreBlob::WithStatus(BlobWithStatus {
                blob: self.blob,
                identifier: self.identifier,
                pairs: self.pairs.clone(),
                metadata: self.metadata.clone(),
                status,
                span: self.span.clone(),
            }),
            Err(error) => {
                if WalrusStoreBlob::<'_, T>::should_fail_early(&error) {
                    return Err(error);
                }
                let blob_id = *self.metadata.blob_id();
                WalrusStoreBlob::Error(FailedBlob {
                    blob: self.blob,
                    identifier: self.identifier,
                    blob_id: Some(blob_id),
                    failure_phase: "status".to_string(),
                    error,
                    span: self.span.clone(),
                })
            }
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

    fn try_complete_if_certified_beyond_epoch(
        self,
        target_epoch: u32,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        {
            let _enter = self.span.enter();
            tracing::event!(
                BLOB_SPAN_LEVEL,
                operation = "try_complete_if_certified_beyond_epoch",
                target_epoch = ?target_epoch
            );
        }

        let status = self.status;
        let blob_id = *self.metadata.blob_id();
        let new_state = match status {
            BlobStatus::Permanent {
                end_epoch,
                is_certified: true,
                status_event,
                ..
            } => {
                if end_epoch >= target_epoch {
                    tracing::event!(
                        BLOB_SPAN_LEVEL,
                        operation = "try_complete_if_certified_beyond_epoch completed",
                        end_epoch = ?end_epoch,
                        blob_id = ?self.metadata.blob_id()
                    );
                    self.complete_with(BlobStoreResult::AlreadyCertified {
                        blob_id,
                        event_or_object: EventOrObjectId::Event(status_event),
                        end_epoch,
                    })
                } else {
                    tracing::event!(
                        BLOB_SPAN_LEVEL,
                        operation = "try_complete_if_certified_beyond_epoch completed",
                        end_epoch = ?end_epoch,
                        blob_id = ?self.metadata.blob_id()
                    );
                    WalrusStoreBlob::WithStatus(self)
                }
            }
            BlobStatus::Invalid { event } => {
                tracing::event!(
                    BLOB_SPAN_LEVEL,
                    operation = "try_complete_if_certified_beyond_epoch completed",
                    blob_id = ?self.metadata.blob_id()
                );
                self.complete_with(BlobStoreResult::MarkedInvalid { blob_id, event })
            }
            _ => {
                tracing::event!(
                    BLOB_SPAN_LEVEL,
                    operation = "try_complete_if_certified_beyond_epoch completed",
                    blob_id = ?self.metadata.blob_id(),
                    "no corresponding permanent certified `Blob` object exists"
                );
                WalrusStoreBlob::WithStatus(self)
            }
        };

        {
            let _enter = new_state.get_span().enter();
            tracing::event!(
                BLOB_SPAN_LEVEL,
                operation = "try_complete_if_certified_beyond_epoch completed",
                state = ?new_state
            );
        }

        Ok(new_state)
    }

    fn with_register_result(
        self,
        result: Result<StoreOp, ClientError>,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        {
            let _enter = self.span.enter();
            tracing::event!(
                BLOB_SPAN_LEVEL,
                operation = "with_register_result",
                result = ?result
            );
        }

        let new_state = match result {
            Ok(StoreOp::NoOp(result)) => WalrusStoreBlob::Completed(CompletedBlob {
                blob: self.blob,
                identifier: self.identifier,
                result,
                span: self.span,
            }),
            Ok(store_op) => WalrusStoreBlob::Registered(RegisteredBlob {
                blob: self.blob,
                identifier: self.identifier,
                pairs: self.pairs.clone(),
                metadata: self.metadata.clone(),
                status: self.status,
                operation: store_op,
                span: self.span,
            }),
            Err(error) => {
                if WalrusStoreBlob::<'_, T>::should_fail_early(&error) {
                    return Err(error);
                }
                let blob_id = *self.metadata.blob_id();
                WalrusStoreBlob::Error(FailedBlob {
                    blob: self.blob,
                    identifier: self.identifier,
                    blob_id: Some(blob_id),
                    failure_phase: "register".to_string(),
                    error,
                    span: self.span.clone(),
                })
            }
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

    fn with_certificate_result(
        self,
        certificate_result: ClientResult<ConfirmationCertificate>,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(ClientError::store_blob_internal(format!(
            "Invalid operation for blob {:?}, certificate_result: {:?}",
            self, certificate_result,
        )))
    }

    fn with_error(self, error: ClientError) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(ClientError::store_blob_internal(format!(
            "Invalid operation for blob {:?}, error: {:?}",
            self, error,
        )))
    }

    fn with_certify_and_extend_result(
        self,
        result: CertifyAndExtendBlobResult,
        price_computation: &PriceComputation,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(ClientError::store_blob_internal(format!(
            "Invalid operation for blob {:?}, result: {:?}, price_computation: {:?}",
            self, result, price_computation,
        )))
    }

    fn with_encode_result(
        self,
        result: Result<(Vec<SliverPair>, VerifiedBlobMetadataWithId), ClientError>,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(ClientError::store_blob_internal(format!(
            "Invalid operation for blob {:?}, result: {:?}",
            self, result,
        )))
    }

    fn complete_with(self, result: BlobStoreResult) -> WalrusStoreBlob<'a, T> {
        WalrusStoreBlob::Completed(CompletedBlob {
            blob: self.get_blob(),
            identifier: self.get_identifier().clone(),
            result,
            span: self.get_span().clone(),
        })
    }
}

impl<T: Debug + Clone + Send + Sync> BlobWithStatus<'_, T> {
    fn get_span(&self) -> &Span {
        &self.span
    }
}

impl<T: Debug + Clone + Send + Sync> std::fmt::Debug for BlobWithStatus<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlobWithStatus")
            .field("identifier", &self.identifier)
            .field("blob_id", &self.metadata.blob_id())
            .field("status", &self.status)
            .field("blob_len", &self.blob.len())
            .finish()
    }
}

/// Registered blob.
#[derive(Clone)]
pub struct RegisteredBlob<'a, T> {
    /// The raw blob data.
    pub blob: &'a [u8],
    /// A unique identifier for this blob.
    pub identifier: T,
    /// The encoded sliver pairs.
    pub pairs: Arc<Vec<SliverPair>>,
    /// The verified metadata associated with the blob.
    pub metadata: Arc<VerifiedBlobMetadataWithId>,
    /// The current status of the blob in the system.
    pub status: BlobStatus,
    /// The store operation to be performed.
    pub operation: StoreOp,
    /// The span for this blob's lifecycle.
    pub span: Span,
}

impl<'a, T: Debug + Clone + Send + Sync> WalrusStoreBlobApi<'a, T> for RegisteredBlob<'a, T> {
    fn get_blob(&self) -> &'a [u8] {
        self.blob
    }

    fn get_identifier(&self) -> &T {
        &self.identifier
    }

    fn get_blob_id(&self) -> Option<BlobId> {
        Some(*self.metadata.blob_id())
    }

    fn get_metadata(&self) -> Option<&VerifiedBlobMetadataWithId> {
        Some(&self.metadata)
    }

    fn get_sliver_pairs(&self) -> Option<&Vec<SliverPair>> {
        Some(&self.pairs)
    }

    fn get_status(&self) -> Option<&BlobStatus> {
        Some(&self.status)
    }

    fn get_operation(&self) -> Option<&StoreOp> {
        Some(&self.operation)
    }

    fn get_state(&self) -> &str {
        "Registered"
    }

    fn encoded_size(&self) -> Option<u64> {
        self.metadata.metadata().encoded_size()
    }

    fn unencoded_length(&self) -> usize {
        self.blob.len()
    }

    fn get_error(&self) -> Option<&ClientError> {
        None
    }

    fn get_result(&self) -> Option<BlobStoreResult> {
        None
    }

    fn get_object_id(&self) -> Option<ObjectID> {
        let StoreOp::RegisterNew { blob, .. } = &self.operation else {
            return None;
        };

        Some(blob.id)
    }

    fn needs_certificate(&self) -> bool {
        let StoreOp::RegisterNew { operation, blob } = &self.operation else {
            return false;
        };

        match operation {
            RegisterBlobOp::ReuseAndExtend { .. } => false,
            RegisterBlobOp::ReuseRegistration { .. }
            | RegisterBlobOp::RegisterFromScratch { .. }
            | RegisterBlobOp::ReuseStorage { .. }
            | RegisterBlobOp::ReuseAndExtendNonCertified { .. } => {
                debug_assert!(blob.certified_epoch.is_none());
                true
            }
        }
    }

    fn needs_extend(&self) -> bool {
        let StoreOp::RegisterNew { operation, blob } = &self.operation else {
            return false;
        };

        if blob.certified_epoch.is_none() {
            return false;
        }

        matches!(operation, RegisterBlobOp::ReuseAndExtend { .. })
    }

    fn get_certify_and_extend_params(&self) -> Result<CertifyAndExtendBlobParams, ClientError> {
        let StoreOp::RegisterNew { operation, blob } = &self.operation else {
            return Err(ClientError::store_blob_internal(format!(
                "Invalid operation for blob {:?}",
                self,
            )));
        };

        if let RegisterBlobOp::ReuseAndExtend {
            epochs_extended, ..
        } = operation
        {
            Ok(CertifyAndExtendBlobParams {
                blob,
                certificate: None,
                epochs_extended: Some(*epochs_extended),
            })
        } else {
            Err(ClientError::store_blob_internal(format!(
                "Invalid operation for blob {:?}",
                self,
            )))
        }
    }

    fn with_status(
        self,
        status: Result<BlobStatus, ClientError>,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(ClientError::store_blob_internal(format!(
            "Invalid operation for blob {:?}, status: {:?}",
            self, status,
        )))
    }

    fn try_complete_if_certified_beyond_epoch(
        self,
        target_epoch: u32,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(ClientError::store_blob_internal(format!(
            "Invalid operation for blob {:?}, target_epoch: {:?}",
            self, target_epoch,
        )))
    }

    fn with_register_result(
        self,
        result: Result<StoreOp, ClientError>,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        {
            let _enter = self.span.enter();
            tracing::event!(
                BLOB_SPAN_LEVEL,
                operation = "with_register_result",
                result = ?result
            );
        }

        let new_state = match result {
            Ok(StoreOp::NoOp(result)) => WalrusStoreBlob::Completed(CompletedBlob {
                blob: self.blob,
                identifier: self.identifier,
                result,
                span: self.span,
            }),
            Ok(store_op) => WalrusStoreBlob::Registered(RegisteredBlob {
                blob: self.blob,
                identifier: self.identifier,
                pairs: self.pairs.clone(),
                metadata: self.metadata.clone(),
                status: self.status,
                operation: store_op,
                span: self.span,
            }),
            Err(error) => {
                if WalrusStoreBlob::<'_, T>::should_fail_early(&error) {
                    return Err(error);
                }
                let blob_id = *self.metadata.blob_id();
                WalrusStoreBlob::Error(FailedBlob {
                    blob: self.blob,
                    identifier: self.identifier,
                    blob_id: Some(blob_id),
                    failure_phase: "register".to_string(),
                    error,
                    span: self.span.clone(),
                })
            }
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

    fn with_certificate_result(
        self,
        certificate_result: ClientResult<ConfirmationCertificate>,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        {
            let _enter = self.span.enter();
            tracing::event!(
                BLOB_SPAN_LEVEL,
                operation = "with_certificate_result",
                certificate_result = ?certificate_result
            );
        }

        let new_state = match certificate_result {
            Ok(certificate) => WalrusStoreBlob::WithCertificate(BlobWithCertificate {
                blob: self.blob,
                identifier: self.identifier,
                pairs: self.pairs.clone(),
                metadata: self.metadata.clone(),
                status: self.status,
                operation: self.operation,
                certificate,
                span: self.span,
            }),
            Err(error) => {
                if WalrusStoreBlob::<'_, T>::should_fail_early(&error) {
                    return Err(error);
                }
                let blob_id = *self.metadata.blob_id();
                WalrusStoreBlob::Error(FailedBlob {
                    blob: self.blob,
                    identifier: self.identifier,
                    blob_id: Some(blob_id),
                    failure_phase: "certificate".to_string(),
                    error,
                    span: self.span.clone(),
                })
            }
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

    fn with_error(self, error: ClientError) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(ClientError::store_blob_internal(format!(
            "Invalid operation for blob {:?}, error: {:?}",
            self, error,
        )))
    }

    fn with_certify_and_extend_result(
        self,
        result: CertifyAndExtendBlobResult,
        price_computation: &PriceComputation,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(ClientError::store_blob_internal(format!(
            "Invalid operation for blob {:?}, result: {:?}, price_computation: {:?}",
            self, result, price_computation,
        )))
    }

    fn with_encode_result(
        self,
        result: Result<(Vec<SliverPair>, VerifiedBlobMetadataWithId), ClientError>,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(ClientError::store_blob_internal(format!(
            "Invalid operation for blob {:?}, result: {:?}",
            self, result,
        )))
    }

    fn complete_with(self, result: BlobStoreResult) -> WalrusStoreBlob<'a, T> {
        WalrusStoreBlob::Completed(CompletedBlob {
            blob: self.get_blob(),
            identifier: self.get_identifier().clone(),
            result,
            span: self.get_span().clone(),
        })
    }
}

impl<T: Debug + Clone + Send + Sync> RegisteredBlob<'_, T> {
    fn get_span(&self) -> &Span {
        &self.span
    }
}

impl<T: Debug + Clone + Send + Sync> std::fmt::Debug for RegisteredBlob<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegisteredBlob")
            .field("identifier", &self.identifier)
            .field("blob_id", &self.metadata.blob_id())
            .field("status", &self.status)
            .field("operation", &self.operation)
            .finish()
    }
}

/// Blob with certificate
#[derive(Clone)]
pub struct BlobWithCertificate<'a, T> {
    /// The raw blob data.
    pub blob: &'a [u8],
    /// A unique identifier for this blob.
    pub identifier: T,
    /// The encoded sliver pairs.
    pub pairs: Arc<Vec<SliverPair>>,
    /// The verified metadata associated with the blob.
    pub metadata: Arc<VerifiedBlobMetadataWithId>,
    /// The current status of the blob in the system.
    pub status: BlobStatus,
    /// The store operation to be performed.
    pub operation: StoreOp,
    /// The confirmation certificate for the blob.
    pub certificate: ConfirmationCertificate,
    /// The span for this blob's lifecycle.
    pub span: Span,
}

impl<'a, T: Debug + Clone + Send + Sync> WalrusStoreBlobApi<'a, T> for BlobWithCertificate<'a, T> {
    fn get_blob(&self) -> &'a [u8] {
        self.blob
    }

    fn get_identifier(&self) -> &T {
        &self.identifier
    }

    fn get_blob_id(&self) -> Option<BlobId> {
        Some(*self.metadata.blob_id())
    }

    fn get_metadata(&self) -> Option<&VerifiedBlobMetadataWithId> {
        Some(&self.metadata)
    }

    fn get_sliver_pairs(&self) -> Option<&Vec<SliverPair>> {
        Some(&self.pairs)
    }

    fn get_status(&self) -> Option<&BlobStatus> {
        Some(&self.status)
    }

    fn get_operation(&self) -> Option<&StoreOp> {
        Some(&self.operation)
    }

    fn get_state(&self) -> &str {
        "WithCertificate"
    }

    fn encoded_size(&self) -> Option<u64> {
        self.metadata.metadata().encoded_size()
    }

    fn unencoded_length(&self) -> usize {
        self.blob.len()
    }

    fn get_error(&self) -> Option<&ClientError> {
        None
    }

    fn get_result(&self) -> Option<BlobStoreResult> {
        None
    }

    fn get_object_id(&self) -> Option<ObjectID> {
        let StoreOp::RegisterNew { blob, .. } = &self.operation else {
            return None;
        };

        Some(blob.id)
    }

    fn needs_certificate(&self) -> bool {
        false
    }

    fn needs_extend(&self) -> bool {
        false
    }

    fn get_certify_and_extend_params(&self) -> Result<CertifyAndExtendBlobParams, ClientError> {
        Err(ClientError::store_blob_internal(format!(
            "Invalid operation for blob {:?}",
            self,
        )))
    }

    fn with_status(
        self,
        status: Result<BlobStatus, ClientError>,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(ClientError::store_blob_internal(format!(
            "Invalid operation for blob {:?}, status: {:?}",
            self, status,
        )))
    }

    fn try_complete_if_certified_beyond_epoch(
        self,
        target_epoch: u32,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(ClientError::store_blob_internal(format!(
            "Invalid operation for blob {:?}, target_epoch: {:?}",
            self, target_epoch,
        )))
    }

    fn with_register_result(
        self,
        result: Result<StoreOp, ClientError>,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(ClientError::store_blob_internal(format!(
            "Invalid operation for blob {:?}, result: {:?}",
            self, result,
        )))
    }

    fn with_certificate_result(
        self,
        certificate_result: ClientResult<ConfirmationCertificate>,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        {
            let _enter = self.span.enter();
            tracing::event!(
                BLOB_SPAN_LEVEL,
                operation = "with_certificate_result",
                certificate_result = ?certificate_result
            );
        }

        let new_state = match certificate_result {
            Ok(certificate) => WalrusStoreBlob::WithCertificate(BlobWithCertificate {
                blob: self.blob,
                identifier: self.identifier,
                pairs: self.pairs.clone(),
                metadata: self.metadata.clone(),
                status: self.status,
                operation: self.operation,
                certificate,
                span: self.span,
            }),
            Err(error) => {
                if WalrusStoreBlob::<'_, T>::should_fail_early(&error) {
                    return Err(error);
                }
                let blob_id = *self.metadata.blob_id();
                WalrusStoreBlob::Error(FailedBlob {
                    blob: self.blob,
                    identifier: self.identifier,
                    blob_id: Some(blob_id),
                    failure_phase: "certificate".to_string(),
                    error,
                    span: self.span.clone(),
                })
            }
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

    fn with_error(self, error: ClientError) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(ClientError::store_blob_internal(format!(
            "Invalid operation for blob {:?}, error: {:?}",
            self, error,
        )))
    }

    fn with_certify_and_extend_result(
        self,
        result: CertifyAndExtendBlobResult,
        price_computation: &PriceComputation,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(ClientError::store_blob_internal(format!(
            "Invalid operation for blob {:?}, result: {:?}, price_computation: {:?}",
            self, result, price_computation,
        )))
    }

    fn with_encode_result(
        self,
        result: Result<(Vec<SliverPair>, VerifiedBlobMetadataWithId), ClientError>,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(ClientError::store_blob_internal(format!(
            "Invalid operation for blob {:?}, result: {:?}",
            self, result,
        )))
    }

    fn complete_with(self, result: BlobStoreResult) -> WalrusStoreBlob<'a, T> {
        WalrusStoreBlob::Completed(CompletedBlob {
            blob: self.get_blob(),
            identifier: self.get_identifier().clone(),
            result,
            span: self.get_span().clone(),
        })
    }
}

impl<T: Debug + Clone + Send + Sync> BlobWithCertificate<'_, T> {
    fn get_span(&self) -> &Span {
        &self.span
    }
}

impl<T: Debug + Clone + Send + Sync> std::fmt::Debug for BlobWithCertificate<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlobWithCertificate")
            .field("identifier", &self.identifier)
            .field("blob_id", &self.metadata.blob_id())
            .field("status", &self.status)
            .field("operation", &self.operation)
            .finish()
    }
}

/// Blob in completed state
#[derive(Clone)]
pub struct CompletedBlob<'a, T: Debug + Clone + Send + Sync> {
    /// The raw blob data.
    pub blob: &'a [u8],
    /// A unique identifier for this blob.
    pub identifier: T,
    /// The final result of the store operation.
    pub result: BlobStoreResult,
    /// The span for this blob's lifecycle.
    pub span: Span,
}

impl<'a, T: Debug + Clone + Send + Sync> WalrusStoreBlobApi<'a, T> for CompletedBlob<'a, T> {
    fn get_blob(&self) -> &'a [u8] {
        self.blob
    }

    fn get_identifier(&self) -> &T {
        &self.identifier
    }

    fn get_blob_id(&self) -> Option<BlobId> {
        Some(*self.result.blob_id())
    }

    fn get_result(&self) -> Option<BlobStoreResult> {
        Some(self.result.clone())
    }

    fn get_state(&self) -> &str {
        "Completed"
    }

    fn encoded_size(&self) -> Option<u64> {
        None
    }

    fn unencoded_length(&self) -> usize {
        self.blob.len()
    }

    fn get_metadata(&self) -> Option<&VerifiedBlobMetadataWithId> {
        None
    }

    fn get_sliver_pairs(&self) -> Option<&Vec<SliverPair>> {
        None
    }

    fn get_status(&self) -> Option<&BlobStatus> {
        None
    }

    fn get_operation(&self) -> Option<&StoreOp> {
        None
    }

    fn get_error(&self) -> Option<&ClientError> {
        None
    }

    fn get_object_id(&self) -> Option<ObjectID> {
        let BlobStoreResult::NewlyCreated { blob_object, .. } = &self.result else {
            return None;
        };

        Some(blob_object.id)
    }

    fn needs_certificate(&self) -> bool {
        false
    }

    fn needs_extend(&self) -> bool {
        false
    }

    fn get_certify_and_extend_params(&self) -> Result<CertifyAndExtendBlobParams, ClientError> {
        Err(ClientError::store_blob_internal(format!(
            "Invalid operation for blob {:?}",
            self,
        )))
    }

    fn with_status(
        self,
        status: Result<BlobStatus, ClientError>,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(ClientError::store_blob_internal(format!(
            "Invalid operation for blob {:?}, status: {:?}",
            self, status,
        )))
    }

    fn try_complete_if_certified_beyond_epoch(
        self,
        target_epoch: u32,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(ClientError::store_blob_internal(format!(
            "Invalid operation for blob {:?}, target_epoch: {:?}",
            self, target_epoch,
        )))
    }

    fn with_register_result(
        self,
        result: Result<StoreOp, ClientError>,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(ClientError::store_blob_internal(format!(
            "Invalid operation for blob {:?}, result: {:?}",
            self, result,
        )))
    }

    fn with_certificate_result(
        self,
        certificate_result: ClientResult<ConfirmationCertificate>,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(ClientError::store_blob_internal(format!(
            "Invalid operation for blob {:?}, certificate_result: {:?}",
            self, certificate_result,
        )))
    }

    fn with_error(self, error: ClientError) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(ClientError::store_blob_internal(format!(
            "Invalid operation for blob {:?}, error: {:?}",
            self, error,
        )))
    }

    fn with_certify_and_extend_result(
        self,
        result: CertifyAndExtendBlobResult,
        price_computation: &PriceComputation,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(ClientError::store_blob_internal(format!(
            "Invalid operation for blob {:?}, result: {:?}, price_computation: {:?}",
            self, result, price_computation,
        )))
    }

    fn with_encode_result(
        self,
        result: Result<(Vec<SliverPair>, VerifiedBlobMetadataWithId), ClientError>,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(ClientError::store_blob_internal(format!(
            "Invalid operation for blob {:?}, result: {:?}",
            self, result,
        )))
    }

    fn complete_with(self, result: BlobStoreResult) -> WalrusStoreBlob<'a, T> {
        WalrusStoreBlob::Completed(CompletedBlob {
            blob: self.get_blob(),
            identifier: self.get_identifier().clone(),
            result,
            span: self.get_span().clone(),
        })
    }
}

impl<T: Debug + Clone + Send + Sync> CompletedBlob<'_, T> {
    fn get_span(&self) -> &Span {
        &self.span
    }
}

impl<T: Debug + Clone + Send + Sync> std::fmt::Debug for CompletedBlob<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompletedBlob")
            .field("identifier", &self.identifier)
            .field("result", &self.result)
            .finish()
    }
}

/// Failure occurred during the blob lifecycle.
pub struct FailedBlob<'a, T: Debug + Clone + Send + Sync> {
    /// The raw blob data.
    pub blob: &'a [u8],
    /// A unique identifier for this blob.
    pub identifier: T,
    /// The blob ID.
    pub blob_id: Option<BlobId>,
    /// The phase where the error occurred.
    pub failure_phase: String,
    /// The error.
    pub error: ClientError,
    /// The span for this blob's lifecycle.
    pub span: Span,
}

impl<'a, T: Debug + Clone + Send + Sync> WalrusStoreBlobApi<'a, T> for FailedBlob<'a, T> {
    fn get_blob(&self) -> &'a [u8] {
        self.blob
    }

    fn get_identifier(&self) -> &T {
        &self.identifier
    }

    fn get_blob_id(&self) -> Option<BlobId> {
        self.blob_id
    }

    fn get_error(&self) -> Option<&ClientError> {
        Some(&self.error)
    }

    fn get_result(&self) -> Option<BlobStoreResult> {
        Some(BlobStoreResult::Error {
            blob_id: self.blob_id,
            error_msg: self.error.to_string(),
        })
    }

    fn get_state(&self) -> &str {
        "Error"
    }

    fn encoded_size(&self) -> Option<u64> {
        None
    }

    fn unencoded_length(&self) -> usize {
        self.blob.len()
    }

    fn get_metadata(&self) -> Option<&VerifiedBlobMetadataWithId> {
        None
    }

    fn get_sliver_pairs(&self) -> Option<&Vec<SliverPair>> {
        None
    }

    fn get_status(&self) -> Option<&BlobStatus> {
        None
    }

    fn get_operation(&self) -> Option<&StoreOp> {
        None
    }

    fn get_object_id(&self) -> Option<ObjectID> {
        None
    }

    fn needs_certificate(&self) -> bool {
        false
    }

    fn needs_extend(&self) -> bool {
        false
    }

    fn get_certify_and_extend_params(&self) -> Result<CertifyAndExtendBlobParams, ClientError> {
        Err(ClientError::store_blob_internal(format!(
            "Invalid operation for blob {:?}",
            self,
        )))
    }

    fn with_status(
        self,
        status: Result<BlobStatus, ClientError>,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(ClientError::store_blob_internal(format!(
            "Invalid operation for blob {:?}, status: {:?}",
            self, status,
        )))
    }

    fn try_complete_if_certified_beyond_epoch(
        self,
        target_epoch: u32,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(ClientError::store_blob_internal(format!(
            "Invalid operation for blob {:?}, target_epoch: {:?}",
            self, target_epoch,
        )))
    }

    fn with_register_result(
        self,
        result: Result<StoreOp, ClientError>,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(ClientError::store_blob_internal(format!(
            "Invalid operation for blob {:?}, result: {:?}",
            self, result,
        )))
    }

    fn with_certificate_result(
        self,
        certificate_result: ClientResult<ConfirmationCertificate>,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(ClientError::store_blob_internal(format!(
            "Invalid operation for blob {:?}, certificate_result: {:?}",
            self, certificate_result,
        )))
    }

    fn with_error(self, error: ClientError) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(ClientError::store_blob_internal(format!(
            "Invalid operation for blob {:?}, error: {:?}",
            self, error,
        )))
    }

    fn with_certify_and_extend_result(
        self,
        result: CertifyAndExtendBlobResult,
        price_computation: &PriceComputation,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(ClientError::store_blob_internal(format!(
            "Invalid operation for blob {:?}, result: {:?}, price_computation: {:?}",
            self, result, price_computation,
        )))
    }

    fn with_encode_result(
        self,
        result: Result<(Vec<SliverPair>, VerifiedBlobMetadataWithId), ClientError>,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(ClientError::store_blob_internal(format!(
            "Invalid operation for blob {:?}, result: {:?}",
            self, result,
        )))
    }

    fn complete_with(self, result: BlobStoreResult) -> WalrusStoreBlob<'a, T> {
        WalrusStoreBlob::Completed(CompletedBlob {
            blob: self.get_blob(),
            identifier: self.get_identifier().clone(),
            result,
            span: self.get_span().clone(),
        })
    }
}

impl<T: Debug + Clone + Send + Sync> FailedBlob<'_, T> {
    fn get_span(&self) -> &Span {
        &self.span
    }
}

impl<T: Debug + Clone + Send + Sync> Clone for FailedBlob<'_, T> {
    fn clone(&self) -> Self {
        panic!("FailedBlob should not be cloned");
    }
}

impl<T: Debug + Clone + Send + Sync> std::fmt::Debug for FailedBlob<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FailedBlob")
            .field("identifier", &self.identifier)
            .field("blob_id", &self.blob_id)
            .field("failure_phase", &self.failure_phase)
            .field("error", &self.error.to_string())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_enum_dispatch_for_walrus_store_blob() {
        // Create a sample UnencodedBlob
        let blob_data = b"sample data";
        let identifier = "sample_id".to_string();

        // Wrap it in the WalrusStoreBlob enum
        let walrus_blob: WalrusStoreBlob<String> =
            WalrusStoreBlob::new_unencoded(blob_data, identifier.clone());

        // Import the trait directly to bring the methods into scope
        use super::WalrusStoreBlobApi;

        // Now the methods will be available
        assert_eq!(walrus_blob.get_identifier(), &identifier);
        assert_eq!(walrus_blob.get_status(), None);
        assert_eq!(walrus_blob.encoded_size(), None);
        assert_eq!(walrus_blob.unencoded_length(), blob_data.len());

        assert_eq!(walrus_blob.get_blob(), blob_data);
    }
}

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

/// The log level for all WalrusStoreBlob spans.
pub(crate) const BLOB_SPAN_LEVEL: Level = Level::DEBUG;

/// API for a blob that is being stored to Walrus.
#[enum_dispatch]
pub trait WalrusStoreBlobApi<'a, T: Debug + Clone + Send + Sync> {
    /// Returns a string representation of the current state.
    fn get_state(&self) -> &'static str;

    /// Returns a reference to the raw blob data.
    fn get_blob(&self) -> &'a [u8];

    /// Returns a reference to the blob's identifier.
    fn get_identifier(&self) -> &T;

    /// Returns the length of the unencoded blob data in bytes.
    fn unencoded_length(&self) -> usize;

    /// Returns the size of the encoded blob if available.
    fn encoded_size(&self) -> Option<u64>;

    /// Returns a reference to the verified metadata if available.
    fn get_metadata(&self) -> Option<&VerifiedBlobMetadataWithId>;

    /// Returns the blob ID if available.
    fn get_blob_id(&self) -> Option<BlobId>;

    /// Returns a reference to the sliver pairs if available.
    fn get_sliver_pairs(&self) -> Option<&Vec<SliverPair>>;

    /// Returns a reference to the blob status if available.
    fn get_status(&self) -> Option<&BlobStatus>;

    /// Returns the corresponding Sui object ID if available.
    fn get_object_id(&self) -> Option<ObjectID>;

    /// Returns a reference to the store operation if available.
    fn get_operation(&self) -> Option<&StoreOp>;

    /// Returns the error if available.
    fn get_error(&self) -> Option<&ClientError>;

    /// Returns the final result if available.
    fn get_result(&self) -> Option<BlobStoreResult>;

    /// Returns true if a blob is ready to be stored to nodes.
    fn ready_to_store_to_nodes(&self) -> bool;

    /// Returns true if the blob is ready to be extended.
    fn ready_to_extend(&self) -> bool;

    /// Returns the parameters for certifying and extending the blob.
    fn get_certify_and_extend_params(&self) -> Result<CertifyAndExtendBlobParams, ClientError>;

    /// Transitions the blob to the next state based on the encoding result.
    ///
    /// If the encoding succeeds, the blob is transitioned to the Encoded state.
    /// If the encoding fails, the blob is transitioned to the Failed state with an error.
    fn with_encode_result(
        self,
        result: Result<(Vec<SliverPair>, VerifiedBlobMetadataWithId), ClientError>,
    ) -> ClientResult<WalrusStoreBlob<'a, T>>;

    /// Transitions the blob to the next state based on the status result.
    ///
    /// If the status is obtained, the blob is transitioned to the WithStatus state.
    /// If the get_status operation fails, the blob is transitioned to the Failed
    /// state with an error.
    fn with_status(
        self,
        status: Result<BlobStatus, ClientError>,
    ) -> ClientResult<WalrusStoreBlob<'a, T>>;

    /// Tries to complete the blob if it is certified beyond the given epoch.
    ///
    /// The existing blob is re-used and no new blob will be created.
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

    /// Updates the blob with the result of storing it to the Walrus storage nodes
    /// and transitions to the appropriate next state.
    fn with_get_certificate_result(
        self,
        certificate_result: ClientResult<ConfirmationCertificate>,
    ) -> ClientResult<WalrusStoreBlob<'a, T>>;

    /// Updates the blob with the result of the complete operation and
    /// transitions to the Completed state.
    fn with_certify_and_extend_result(
        self,
        result: CertifyAndExtendBlobResult,
        price_computation: &PriceComputation,
    ) -> ClientResult<WalrusStoreBlob<'a, T>>;

    /// Updates the blob with the provided result and transitions to the Completed state.
    ///
    /// This update is forced, even if the blob is in the Error state.
    fn complete_with(self, result: BlobStoreResult) -> WalrusStoreBlob<'a, T>;

    /// Converts the current blob state to a Failed state.
    fn with_error(self, error: ClientError) -> ClientResult<WalrusStoreBlob<'a, T>>;
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
    /// Create a list of UnencodedBlobs with default identifiers in the form of "blob_{:06}".
    pub fn default_unencoded_blobs_from_slice(
        blobs: &'a [&[u8]],
    ) -> Vec<WalrusStoreBlob<'a, String>> {
        blobs
            .iter()
            .enumerate()
            .map(|(i, blob)| WalrusStoreBlob::new_unencoded(blob, format!("blob_{:06}", i)))
            .collect()
    }

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

    /// Returns the span for this blob's lifecycle.
    fn get_span(&self) -> &Span {
        match self {
            WalrusStoreBlob::Unencoded(inner) => &inner.span,
            WalrusStoreBlob::Encoded(inner) => &inner.input_blob.span,
            WalrusStoreBlob::WithStatus(inner) => &inner.input_blob.span,
            WalrusStoreBlob::Registered(inner) => &inner.input_blob.span,
            WalrusStoreBlob::WithCertificate(inner) => &inner.input_blob.span,
            WalrusStoreBlob::Completed(inner) => &inner.input_blob.span,
            WalrusStoreBlob::Error(inner) => &inner.input_blob.span,
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
    fn get_state(&self) -> &'static str {
        "Unencoded"
    }

    fn get_blob(&self) -> &'a [u8] {
        self.blob
    }

    fn get_identifier(&self) -> &T {
        &self.identifier
    }

    fn unencoded_length(&self) -> usize {
        self.blob.len()
    }

    fn encoded_size(&self) -> Option<u64> {
        None
    }

    fn get_metadata(&self) -> Option<&VerifiedBlobMetadataWithId> {
        None
    }

    fn get_blob_id(&self) -> Option<BlobId> {
        None
    }

    fn get_sliver_pairs(&self) -> Option<&Vec<SliverPair>> {
        None
    }

    fn get_status(&self) -> Option<&BlobStatus> {
        None
    }

    fn get_object_id(&self) -> Option<ObjectID> {
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

    fn ready_to_store_to_nodes(&self) -> bool {
        false
    }

    fn ready_to_extend(&self) -> bool {
        false
    }

    fn get_certify_and_extend_params(&self) -> ClientResult<CertifyAndExtendBlobParams> {
        Err(ClientError::store_blob_internal(format!(
            "Invalid operation for blob {:?}",
            self,
        )))
    }

    fn with_encode_result(
        self,
        result: ClientResult<(Vec<SliverPair>, VerifiedBlobMetadataWithId)>,
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
                    input_blob: self,
                    pairs: Arc::new(pairs),
                    metadata: Arc::new(metadata),
                })
            }
            Err(error) => WalrusStoreBlob::Error(FailedBlob {
                input_blob: self,
                blob_id: None,
                failure_phase: "encode".to_string(),
                error,
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

    fn with_get_certificate_result(
        self,
        certificate_result: ClientResult<ConfirmationCertificate>,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(ClientError::store_blob_internal(format!(
            "Invalid operation for blob {:?}, certificate_result: {:?}",
            self, certificate_result,
        )))
    }

    fn with_certify_and_extend_result(
        self,
        result: CertifyAndExtendBlobResult,
        _price_computation: &PriceComputation,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(invalid_operation_for_blob(
            &self,
            format!("with_certify_and_extend_result: {:?}", result),
        ))
    }

    fn complete_with(self, result: BlobStoreResult) -> WalrusStoreBlob<'a, T> {
        WalrusStoreBlob::Completed(CompletedBlob {
            input_blob: self,
            result,
        })
    }

    fn with_error(self, error: ClientError) -> ClientResult<WalrusStoreBlob<'a, T>> {
        if WalrusStoreBlob::<'_, T>::should_fail_early(&error) {
            return Err(error);
        }

        Ok(WalrusStoreBlob::Error(FailedBlob {
            input_blob: self,
            blob_id: None,
            failure_phase: "with_error".to_string(),
            error,
        }))
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
    /// The base unencoded blob.
    pub input_blob: UnencodedBlob<'a, T>,
    /// The encoded sliver pairs generated from the blob.
    pub pairs: Arc<Vec<SliverPair>>,
    /// The verified metadata associated with the encoded blob.
    pub metadata: Arc<VerifiedBlobMetadataWithId>,
}

impl<'a, T: Debug + Clone + Send + Sync> WalrusStoreBlobApi<'a, T> for EncodedBlob<'a, T> {
    fn get_state(&self) -> &'static str {
        "Encoded"
    }

    fn get_blob(&self) -> &'a [u8] {
        self.input_blob.get_blob()
    }

    fn get_identifier(&self) -> &T {
        self.input_blob.get_identifier()
    }

    fn unencoded_length(&self) -> usize {
        self.input_blob.unencoded_length()
    }

    fn encoded_size(&self) -> Option<u64> {
        self.metadata.metadata().encoded_size()
    }

    fn get_metadata(&self) -> Option<&VerifiedBlobMetadataWithId> {
        Some(&self.metadata)
    }

    fn get_blob_id(&self) -> Option<BlobId> {
        Some(*self.metadata.blob_id())
    }

    fn get_sliver_pairs(&self) -> Option<&Vec<SliverPair>> {
        Some(&self.pairs)
    }

    fn get_status(&self) -> Option<&BlobStatus> {
        None
    }

    fn get_object_id(&self) -> Option<ObjectID> {
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

    fn ready_to_store_to_nodes(&self) -> bool {
        false
    }

    fn ready_to_extend(&self) -> bool {
        false
    }

    fn get_certify_and_extend_params(&self) -> Result<CertifyAndExtendBlobParams, ClientError> {
        Err(invalid_operation_for_blob(
            &self,
            "get_certify_and_extend_params".to_string(),
        ))
    }

    fn with_encode_result(
        self,
        result: Result<(Vec<SliverPair>, VerifiedBlobMetadataWithId), ClientError>,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(invalid_operation_for_blob(
            &self,
            format!("with_encode_result: {:?}", result),
        ))
    }

    fn with_status(
        self,
        status: Result<BlobStatus, ClientError>,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        {
            let _enter = self.input_blob.span.enter();
            tracing::event!(
                BLOB_SPAN_LEVEL,
                operation = "with_status",
                status = ?status
            );
        }

        let new_state = match status {
            Ok(status) => WalrusStoreBlob::WithStatus(BlobWithStatus {
                input_blob: self.input_blob,
                pairs: self.pairs,
                metadata: self.metadata,
                status,
            }),
            Err(error) => {
                if WalrusStoreBlob::<'_, T>::should_fail_early(&error) {
                    return Err(error);
                }
                let blob_id = *self.metadata.blob_id();
                WalrusStoreBlob::Error(FailedBlob {
                    input_blob: self.input_blob,
                    blob_id: Some(blob_id),
                    failure_phase: "status".to_string(),
                    error,
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
        Err(invalid_operation_for_blob(
            &self,
            format!("try_complete_if_certified_beyond_epoch: {:?}", target_epoch),
        ))
    }

    fn with_register_result(
        self,
        result: Result<StoreOp, ClientError>,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(invalid_operation_for_blob(
            &self,
            format!("with_register_result: {:?}", result),
        ))
    }

    fn with_get_certificate_result(
        self,
        certificate_result: ClientResult<ConfirmationCertificate>,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(invalid_operation_for_blob(
            &self,
            format!("with_get_certificate_result: {:?}", certificate_result),
        ))
    }

    fn with_certify_and_extend_result(
        self,
        result: CertifyAndExtendBlobResult,
        _price_computation: &PriceComputation,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(invalid_operation_for_blob(
            &self,
            format!("with_certify_and_extend_result: {:?}", result),
        ))
    }

    fn complete_with(self, result: BlobStoreResult) -> WalrusStoreBlob<'a, T> {
        WalrusStoreBlob::Completed(CompletedBlob {
            input_blob: self.input_blob,
            result,
        })
    }

    fn with_error(self, error: ClientError) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(invalid_operation_for_blob(
            &self,
            format!("with_error: {:?}", error),
        ))
    }
}

impl<T: Debug + Clone + Send + Sync> std::fmt::Debug for EncodedBlob<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncodedBlob")
            .field("identifier", &self.input_blob.identifier)
            .field("blob_id", &self.metadata.blob_id())
            .field("blob_len", &self.input_blob.blob.len())
            .finish()
    }
}

/// Encoded blob with status information.
#[derive(Clone)]
pub struct BlobWithStatus<'a, T: Debug + Clone + Send + Sync> {
    /// The base unencoded blob.
    pub input_blob: UnencodedBlob<'a, T>,
    /// The encoded sliver pairs.
    pub pairs: Arc<Vec<SliverPair>>,
    /// The verified metadata associated with the blob.
    pub metadata: Arc<VerifiedBlobMetadataWithId>,
    /// The current status of the blob in the system.
    pub status: BlobStatus,
}

impl<'a, T: Debug + Clone + Send + Sync> WalrusStoreBlobApi<'a, T> for BlobWithStatus<'a, T> {
    fn get_state(&self) -> &'static str {
        "WithStatus"
    }

    fn get_blob(&self) -> &'a [u8] {
        self.input_blob.blob
    }

    fn get_identifier(&self) -> &T {
        &self.input_blob.identifier
    }

    fn unencoded_length(&self) -> usize {
        self.input_blob.blob.len()
    }

    fn encoded_size(&self) -> Option<u64> {
        self.metadata.metadata().encoded_size()
    }

    fn get_metadata(&self) -> Option<&VerifiedBlobMetadataWithId> {
        Some(&self.metadata)
    }

    fn get_blob_id(&self) -> Option<BlobId> {
        Some(*self.metadata.blob_id())
    }

    fn get_sliver_pairs(&self) -> Option<&Vec<SliverPair>> {
        Some(&self.pairs)
    }

    fn get_status(&self) -> Option<&BlobStatus> {
        Some(&self.status)
    }

    fn get_object_id(&self) -> Option<ObjectID> {
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

    fn ready_to_store_to_nodes(&self) -> bool {
        false
    }

    fn ready_to_extend(&self) -> bool {
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
        Err(invalid_operation_for_blob(
            &self,
            format!("with_encode_result: {:?}", result),
        ))
    }

    fn with_status(self, status: ClientResult<BlobStatus>) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(invalid_operation_for_blob(
            &self,
            format!("with_status: {:?}", status),
        ))
    }

    fn try_complete_if_certified_beyond_epoch(
        self,
        target_epoch: u32,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        {
            let _enter = self.input_blob.span.enter();
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
                    self.complete_with(BlobStoreResult::AlreadyCertified {
                        blob_id,
                        event_or_object: EventOrObjectId::Event(status_event),
                        end_epoch,
                    })
                } else {
                    WalrusStoreBlob::WithStatus(self)
                }
            }
            BlobStatus::Invalid { event } => {
                self.complete_with(BlobStoreResult::MarkedInvalid { blob_id, event })
            }
            _ => WalrusStoreBlob::WithStatus(self),
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

    // TODO: Replace StoreOp with RegisterBlobOp and blob.
    fn with_register_result(
        self,
        result: ClientResult<StoreOp>,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        {
            let _enter = self.input_blob.span.enter();
            tracing::event!(
                BLOB_SPAN_LEVEL,
                operation = "with_register_result",
                result = ?result
            );
        }

        let new_state = match result {
            Ok(StoreOp::NoOp(result)) => WalrusStoreBlob::Completed(CompletedBlob {
                input_blob: self.input_blob,
                result,
            }),
            Ok(store_op) => WalrusStoreBlob::Registered(RegisteredBlob {
                input_blob: self.input_blob,
                pairs: self.pairs,
                metadata: self.metadata,
                status: self.status,
                operation: store_op,
            }),
            Err(error) => {
                if WalrusStoreBlob::<'_, T>::should_fail_early(&error) {
                    return Err(error);
                }
                let blob_id = *self.metadata.blob_id();
                WalrusStoreBlob::Error(FailedBlob {
                    input_blob: self.input_blob,
                    blob_id: Some(blob_id),
                    failure_phase: "register".to_string(),
                    error,
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

    fn with_get_certificate_result(
        self,
        certificate_result: ClientResult<ConfirmationCertificate>,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(invalid_operation_for_blob(
            &self,
            format!("with_get_certificate_result: {:?}", certificate_result),
        ))
    }

    fn with_certify_and_extend_result(
        self,
        result: CertifyAndExtendBlobResult,
        _price_computation: &PriceComputation,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(invalid_operation_for_blob(
            &self,
            format!("with_certify_and_extend_result: {:?}", result),
        ))
    }

    fn complete_with(self, result: BlobStoreResult) -> WalrusStoreBlob<'a, T> {
        WalrusStoreBlob::Completed(CompletedBlob {
            input_blob: self.input_blob,
            result,
        })
    }

    fn with_error(self, error: ClientError) -> ClientResult<WalrusStoreBlob<'a, T>> {
        if WalrusStoreBlob::<'_, T>::should_fail_early(&error) {
            return Err(error);
        }

        Ok(WalrusStoreBlob::Error(FailedBlob {
            input_blob: self.input_blob,
            blob_id: Some(*self.metadata.blob_id()),
            failure_phase: "register".to_string(),
            error,
        }))
    }
}

impl<T: Debug + Clone + Send + Sync> std::fmt::Debug for BlobWithStatus<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlobWithStatus")
            .field("identifier", &self.input_blob.identifier)
            .field("blob_id", &self.metadata.blob_id())
            .field("status", &self.status)
            .field("blob_len", &self.input_blob.blob.len())
            .finish()
    }
}

/// Registered blob.
#[derive(Clone)]
pub struct RegisteredBlob<'a, T: Debug + Clone + Send + Sync> {
    /// The base unencoded blob.
    pub input_blob: UnencodedBlob<'a, T>,
    /// The encoded sliver pairs.
    pub pairs: Arc<Vec<SliverPair>>,
    /// The verified metadata associated with the blob.
    pub metadata: Arc<VerifiedBlobMetadataWithId>,
    /// The current status of the blob in the system.
    pub status: BlobStatus,
    /// The store operation to be performed.
    pub operation: StoreOp,
}

impl<'a, T: Debug + Clone + Send + Sync> WalrusStoreBlobApi<'a, T> for RegisteredBlob<'a, T> {
    fn get_state(&self) -> &'static str {
        "Registered"
    }

    fn get_blob(&self) -> &'a [u8] {
        self.input_blob.blob
    }

    fn get_identifier(&self) -> &T {
        &self.input_blob.identifier
    }

    fn unencoded_length(&self) -> usize {
        self.input_blob.blob.len()
    }

    fn encoded_size(&self) -> Option<u64> {
        self.metadata.metadata().encoded_size()
    }

    fn get_metadata(&self) -> Option<&VerifiedBlobMetadataWithId> {
        Some(&self.metadata)
    }

    fn get_blob_id(&self) -> Option<BlobId> {
        Some(*self.metadata.blob_id())
    }

    fn get_sliver_pairs(&self) -> Option<&Vec<SliverPair>> {
        Some(&self.pairs)
    }

    fn get_status(&self) -> Option<&BlobStatus> {
        Some(&self.status)
    }

    // TODO: Return the object ID from the blob.
    fn get_object_id(&self) -> Option<ObjectID> {
        let StoreOp::RegisterNew { blob, .. } = &self.operation else {
            return None;
        };

        Some(blob.id)
    }

    fn get_operation(&self) -> Option<&StoreOp> {
        Some(&self.operation)
    }

    fn get_error(&self) -> Option<&ClientError> {
        None
    }

    fn get_result(&self) -> Option<BlobStoreResult> {
        None
    }

    fn ready_to_store_to_nodes(&self) -> bool {
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

    fn ready_to_extend(&self) -> bool {
        let StoreOp::RegisterNew { operation, blob } = &self.operation else {
            return false;
        };

        if blob.certified_epoch.is_none() {
            return false;
        }

        matches!(operation, RegisterBlobOp::ReuseAndExtend { .. })
    }

    fn get_certify_and_extend_params(&self) -> ClientResult<CertifyAndExtendBlobParams> {
        if let StoreOp::RegisterNew {
            operation:
                RegisterBlobOp::ReuseAndExtend {
                    epochs_extended, ..
                },
            blob,
        } = &self.operation
        {
            Ok(CertifyAndExtendBlobParams {
                blob,
                certificate: None,
                epochs_extended: Some(*epochs_extended),
            })
        } else {
            Err(invalid_operation_for_blob(
                &self,
                format!("get_certify_and_extend_params: {:?}", self.operation),
            ))
        }
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
        result: ClientResult<StoreOp>,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(invalid_operation_for_blob(
            &self,
            format!("with_register_result: {:?}", result),
        ))
    }

    fn with_get_certificate_result(
        self,
        certificate_result: ClientResult<ConfirmationCertificate>,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        {
            let _enter = self.input_blob.span.enter();
            tracing::event!(
                BLOB_SPAN_LEVEL,
                operation = "with_get_certificate_result",
                certificate_result = ?certificate_result
            );
        }

        let new_state = match certificate_result {
            Ok(certificate) => WalrusStoreBlob::WithCertificate(BlobWithCertificate {
                input_blob: self.input_blob,
                pairs: self.pairs,
                metadata: self.metadata,
                status: self.status,
                operation: self.operation,
                certificate,
            }),
            Err(error) => {
                if WalrusStoreBlob::<'_, T>::should_fail_early(&error) {
                    return Err(error);
                }
                let blob_id = *self.metadata.blob_id();
                WalrusStoreBlob::Error(FailedBlob {
                    input_blob: self.input_blob,
                    blob_id: Some(blob_id),
                    failure_phase: "certificate".to_string(),
                    error,
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

    fn with_certify_and_extend_result(
        self,
        result: CertifyAndExtendBlobResult,
        price_computation: &PriceComputation,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        {
            let _enter = self.input_blob.span.enter();
            tracing::event!(
                BLOB_SPAN_LEVEL,
                operation = "with_certify_and_extend_result",
                result = ?result
            );
        }

        let StoreOp::RegisterNew { operation, blob } = &self.operation else {
            return Err(invalid_operation_for_blob(
                &self,
                format!("with_certify_and_extend_result: {:?}", self.operation),
            ));
        };

        if !matches!(operation, RegisterBlobOp::ReuseAndExtend { .. }) {
            return Err(invalid_operation_for_blob(
                &self,
                format!("with_certify_and_extend_result: {:?}", self.operation),
            ));
        }

        let resource_operation = operation.clone();
        let blob_object = blob.clone();
        let new_state = self.complete_with(BlobStoreResult::NewlyCreated {
            cost: price_computation.operation_cost(&resource_operation),
            blob_object,
            resource_operation,
            // TODO: pass error back to the caller.
            shared_blob_object: result.shared_blob_object(),
        });

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

    fn complete_with(self, result: BlobStoreResult) -> WalrusStoreBlob<'a, T> {
        WalrusStoreBlob::Completed(CompletedBlob {
            input_blob: self.input_blob,
            result,
        })
    }

    fn with_error(self, error: ClientError) -> ClientResult<WalrusStoreBlob<'a, T>> {
        if WalrusStoreBlob::<'_, T>::should_fail_early(&error) {
            Err(error)
        } else {
            Ok(WalrusStoreBlob::Error(FailedBlob {
                input_blob: self.input_blob,
                blob_id: Some(*self.metadata.blob_id()),
                failure_phase: "error".to_string(),
                error,
            }))
        }
    }
}

impl<T: Debug + Clone + Send + Sync> std::fmt::Debug for RegisteredBlob<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegisteredBlob")
            .field("identifier", &self.input_blob.identifier)
            .field("blob_id", &self.metadata.blob_id())
            .field("status", &self.status)
            .field("operation", &self.operation)
            .finish()
    }
}

/// Blob with certificate
#[derive(Clone)]
pub struct BlobWithCertificate<'a, T: Debug + Clone + Send + Sync> {
    /// The base unencoded blob.
    pub input_blob: UnencodedBlob<'a, T>,
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
}

impl<'a, T: Debug + Clone + Send + Sync> WalrusStoreBlobApi<'a, T> for BlobWithCertificate<'a, T> {
    fn get_state(&self) -> &'static str {
        "WithCertificate"
    }

    fn get_blob(&self) -> &'a [u8] {
        self.input_blob.blob
    }

    fn get_identifier(&self) -> &T {
        &self.input_blob.identifier
    }

    fn unencoded_length(&self) -> usize {
        self.input_blob.blob.len()
    }

    fn encoded_size(&self) -> Option<u64> {
        self.metadata.metadata().encoded_size()
    }

    fn get_metadata(&self) -> Option<&VerifiedBlobMetadataWithId> {
        Some(&self.metadata)
    }

    fn get_blob_id(&self) -> Option<BlobId> {
        Some(*self.metadata.blob_id())
    }

    fn get_sliver_pairs(&self) -> Option<&Vec<SliverPair>> {
        Some(&self.pairs)
    }

    fn get_status(&self) -> Option<&BlobStatus> {
        Some(&self.status)
    }

    fn get_object_id(&self) -> Option<ObjectID> {
        let StoreOp::RegisterNew { blob, .. } = &self.operation else {
            return None;
        };

        Some(blob.id)
    }

    fn get_operation(&self) -> Option<&StoreOp> {
        Some(&self.operation)
    }

    fn get_error(&self) -> Option<&ClientError> {
        None
    }

    fn get_result(&self) -> Option<BlobStoreResult> {
        None
    }

    fn ready_to_store_to_nodes(&self) -> bool {
        false
    }

    fn ready_to_extend(&self) -> bool {
        false
    }

    fn get_certify_and_extend_params(&self) -> ClientResult<CertifyAndExtendBlobParams> {
        let StoreOp::RegisterNew { operation, blob } = &self.operation else {
            return Err(invalid_operation_for_blob(
                &self,
                format!("get_certify_and_extend_params: {:?}", self.operation),
            ));
        };
        match operation {
            RegisterBlobOp::ReuseAndExtend { .. } => Err(invalid_operation_for_blob(
                &self,
                format!("get_certify_and_extend_params: {:?}", self.operation),
            )),
            RegisterBlobOp::ReuseAndExtendNonCertified {
                epochs_extended, ..
            } => Ok(CertifyAndExtendBlobParams {
                blob,
                certificate: Some(self.certificate.clone()),
                epochs_extended: Some(*epochs_extended),
            }),
            RegisterBlobOp::RegisterFromScratch { .. }
            | RegisterBlobOp::ReuseStorage { .. }
            | RegisterBlobOp::ReuseRegistration { .. } => Ok(CertifyAndExtendBlobParams {
                blob,
                certificate: Some(self.certificate.clone()),
                epochs_extended: None,
            }),
        }
    }

    fn with_encode_result(
        self,
        result: Result<(Vec<SliverPair>, VerifiedBlobMetadataWithId), ClientError>,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(invalid_operation_for_blob(
            &self,
            format!("with_encode_result: {:?}", result),
        ))
    }

    fn with_status(
        self,
        status: Result<BlobStatus, ClientError>,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(invalid_operation_for_blob(
            &self,
            format!("with_status: {:?}", status),
        ))
    }

    fn try_complete_if_certified_beyond_epoch(
        self,
        target_epoch: u32,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(invalid_operation_for_blob(
            &self,
            format!("try_complete_if_certified_beyond_epoch: {:?}", target_epoch),
        ))
    }

    fn with_register_result(
        self,
        result: Result<StoreOp, ClientError>,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(invalid_operation_for_blob(
            &self,
            format!("with_register_result: {:?}", result),
        ))
    }

    fn with_get_certificate_result(
        self,
        certificate_result: ClientResult<ConfirmationCertificate>,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(invalid_operation_for_blob(
            &self,
            format!("with_get_certificate_result: {:?}", certificate_result),
        ))
    }

    fn with_certify_and_extend_result(
        self,
        result: CertifyAndExtendBlobResult,
        price_computation: &PriceComputation,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        {
            let _enter = self.input_blob.span.enter();
            tracing::event!(
                BLOB_SPAN_LEVEL,
                operation = "with_certify_and_extend_result",
                result = ?result
            );
        }

        let StoreOp::RegisterNew { operation, blob } = &self.operation else {
            return Err(invalid_operation_for_blob(
                &self,
                format!("with_certify_and_extend_result: {:?}", result),
            ));
        };

        let store_result = BlobStoreResult::NewlyCreated {
            blob_object: blob.clone(),
            resource_operation: operation.clone(),
            cost: price_computation.operation_cost(operation),
            shared_blob_object: result.shared_blob_object(),
        };

        let new_state = self.complete_with(store_result);

        {
            let _enter = new_state.get_span().enter();
            tracing::event!(
                BLOB_SPAN_LEVEL,
                operation = "with_certify_and_extend_result",
                new_state = ?new_state,
                result = ?result
            );
        }

        Ok(new_state)
    }

    fn complete_with(self, result: BlobStoreResult) -> WalrusStoreBlob<'a, T> {
        WalrusStoreBlob::Completed(CompletedBlob {
            input_blob: self.input_blob,
            result,
        })
    }

    fn with_error(self, error: ClientError) -> ClientResult<WalrusStoreBlob<'a, T>> {
        if WalrusStoreBlob::<'_, T>::should_fail_early(&error) {
            Err(error)
        } else {
            let blob_id = self.get_blob_id();
            Ok(WalrusStoreBlob::Error(FailedBlob {
                input_blob: self.input_blob,
                blob_id,
                failure_phase: "with_certificate".to_string(),
                error,
            }))
        }
    }
}

impl<T: Debug + Clone + Send + Sync> std::fmt::Debug for BlobWithCertificate<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlobWithCertificate")
            .field("identifier", &self.input_blob.identifier)
            .field("blob_id", &self.metadata.blob_id())
            .field("status", &self.status)
            .field("operation", &self.operation)
            .finish()
    }
}

/// Blob in completed state
#[derive(Clone)]
pub struct CompletedBlob<'a, T: Debug + Clone + Send + Sync> {
    /// The base unencoded blob.
    pub input_blob: UnencodedBlob<'a, T>,
    /// The final result of the store operation.
    pub result: BlobStoreResult,
}

impl<'a, T: Debug + Clone + Send + Sync> WalrusStoreBlobApi<'a, T> for CompletedBlob<'a, T> {
    fn get_state(&self) -> &'static str {
        "Completed"
    }

    fn get_blob(&self) -> &'a [u8] {
        self.input_blob.get_blob()
    }

    fn get_identifier(&self) -> &T {
        self.input_blob.get_identifier()
    }

    fn unencoded_length(&self) -> usize {
        self.input_blob.unencoded_length()
    }

    fn encoded_size(&self) -> Option<u64> {
        None
    }

    fn get_metadata(&self) -> Option<&VerifiedBlobMetadataWithId> {
        None
    }

    fn get_blob_id(&self) -> Option<BlobId> {
        self.result.blob_id()
    }

    fn get_sliver_pairs(&self) -> Option<&Vec<SliverPair>> {
        None
    }

    fn get_status(&self) -> Option<&BlobStatus> {
        None
    }

    fn get_object_id(&self) -> Option<ObjectID> {
        let BlobStoreResult::NewlyCreated { blob_object, .. } = &self.result else {
            return None;
        };

        Some(blob_object.id)
    }

    fn get_operation(&self) -> Option<&StoreOp> {
        None
    }

    fn get_error(&self) -> Option<&ClientError> {
        None
    }

    fn get_result(&self) -> Option<BlobStoreResult> {
        Some(self.result.clone())
    }

    fn ready_to_store_to_nodes(&self) -> bool {
        false
    }

    fn ready_to_extend(&self) -> bool {
        false
    }

    fn get_certify_and_extend_params(&self) -> ClientResult<CertifyAndExtendBlobParams> {
        Err(invalid_operation_for_blob(
            &self,
            "get_certify_and_extend_params".to_string(),
        ))
    }

    fn with_encode_result(
        self,
        result: Result<(Vec<SliverPair>, VerifiedBlobMetadataWithId), ClientError>,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(invalid_operation_for_blob(
            &self,
            format!("with_encode_result: {:?}", result),
        ))
    }

    fn with_status(
        self,
        status: Result<BlobStatus, ClientError>,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(invalid_operation_for_blob(
            &self,
            format!("with_status: {:?}", status),
        ))
    }

    fn try_complete_if_certified_beyond_epoch(
        self,
        target_epoch: u32,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(invalid_operation_for_blob(
            &self,
            format!("try_complete_if_certified_beyond_epoch: {:?}", target_epoch),
        ))
    }

    fn with_register_result(
        self,
        result: Result<StoreOp, ClientError>,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(invalid_operation_for_blob(
            &self,
            format!("with_register_result: {:?}", result),
        ))
    }

    fn with_get_certificate_result(
        self,
        certificate_result: ClientResult<ConfirmationCertificate>,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(invalid_operation_for_blob(
            &self,
            format!("with_get_certificate_result: {:?}", certificate_result),
        ))
    }

    fn with_certify_and_extend_result(
        self,
        result: CertifyAndExtendBlobResult,
        _price_computation: &PriceComputation,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(invalid_operation_for_blob(
            &self,
            format!("with_certify_and_extend_result: {:?}", result),
        ))
    }

    fn complete_with(self, result: BlobStoreResult) -> WalrusStoreBlob<'a, T> {
        WalrusStoreBlob::Completed(CompletedBlob {
            input_blob: self.input_blob,
            result,
        })
    }

    fn with_error(self, error: ClientError) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(invalid_operation_for_blob(
            &self,
            format!("with_error: {:?}", error),
        ))
    }
}

impl<T: Debug + Clone + Send + Sync> std::fmt::Debug for CompletedBlob<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompletedBlob")
            .field("identifier", &self.input_blob.identifier)
            .field("result", &self.result)
            .finish()
    }
}

/// Failure occurred during the blob lifecycle.
pub struct FailedBlob<'a, T: Debug + Clone + Send + Sync> {
    /// The base unencoded blob.
    pub input_blob: UnencodedBlob<'a, T>,
    /// The blob ID.
    pub blob_id: Option<BlobId>,
    /// The phase where the error occurred.
    pub failure_phase: String,
    /// The error.
    pub error: ClientError,
}

impl<'a, T: Debug + Clone + Send + Sync> WalrusStoreBlobApi<'a, T> for FailedBlob<'a, T> {
    fn get_state(&self) -> &'static str {
        "Error"
    }

    fn get_blob(&self) -> &'a [u8] {
        self.input_blob.get_blob()
    }

    fn get_identifier(&self) -> &T {
        &self.input_blob.identifier
    }

    fn unencoded_length(&self) -> usize {
        self.input_blob.unencoded_length()
    }

    fn encoded_size(&self) -> Option<u64> {
        None
    }

    fn get_metadata(&self) -> Option<&VerifiedBlobMetadataWithId> {
        None
    }

    fn get_blob_id(&self) -> Option<BlobId> {
        self.blob_id
    }

    fn get_sliver_pairs(&self) -> Option<&Vec<SliverPair>> {
        None
    }

    fn get_status(&self) -> Option<&BlobStatus> {
        None
    }

    fn get_object_id(&self) -> Option<ObjectID> {
        None
    }

    fn get_operation(&self) -> Option<&StoreOp> {
        None
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

    fn ready_to_store_to_nodes(&self) -> bool {
        false
    }

    fn ready_to_extend(&self) -> bool {
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
        Err(invalid_operation_for_blob(
            &self,
            format!("with_encode_result: {:?}", result),
        ))
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

    fn with_get_certificate_result(
        self,
        certificate_result: ClientResult<ConfirmationCertificate>,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(ClientError::store_blob_internal(format!(
            "Invalid operation for blob {:?}, certificate_result: {:?}",
            self, certificate_result,
        )))
    }

    fn with_certify_and_extend_result(
        self,
        result: CertifyAndExtendBlobResult,
        _price_computation: &PriceComputation,
    ) -> ClientResult<WalrusStoreBlob<'a, T>> {
        Err(invalid_operation_for_blob(
            &self,
            format!("with_certify_and_extend_result: {:?}", result),
        ))
    }

    fn complete_with(self, result: BlobStoreResult) -> WalrusStoreBlob<'a, T> {
        WalrusStoreBlob::Completed(CompletedBlob {
            input_blob: self.input_blob,
            result,
        })
    }

    fn with_error(self, error: ClientError) -> ClientResult<WalrusStoreBlob<'a, T>> {
        if WalrusStoreBlob::<'_, T>::should_fail_early(&error) {
            Err(error)
        } else {
            let blob_id = self.get_blob_id();
            Ok(WalrusStoreBlob::Error(FailedBlob {
                input_blob: self.input_blob,
                blob_id,
                failure_phase: "with_certificate".to_string(),
                error,
            }))
        }
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
            .field("identifier", &self.input_blob.identifier)
            .field("blob_id", &self.blob_id)
            .field("failure_phase", &self.failure_phase)
            .field("error", &self.error.to_string())
            .finish()
    }
}

fn invalid_operation_for_blob<B: Debug>(blob: &B, operation: String) -> ClientError {
    ClientError::store_blob_internal(format!(
        "Invalid operation for blob {:?}, operation: {:?}",
        blob, operation,
    ))
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

// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Owned-blob lifecycle states after registration.

use walrus_core::messages::ConfirmationCertificate;
use walrus_sui::{
    client::{CertifyAndExtendBlobParams, CertifyAndExtendBlobResult},
    types::Blob,
};

use super::*;
use crate::node_client::{
    resource::{PriceComputation, RegisterBlobOp},
    responses::EventOrObjectId,
};

impl BlobWithStatus {
    /// Converts the blob with status information to a registered owned blob based on the given
    /// blob object and operation.
    pub fn with_register_result(
        self,
        blob_object: Blob,
        operation: RegisterBlobOp,
    ) -> WalrusStoreBlobState<OwnedRegisteredBlob> {
        tracing::event!(
            BLOB_SPAN_LEVEL,
            ?blob_object,
            ?operation,
            "entering with_register_result"
        );

        if blob_object.is_certified() && operation.is_reuse_registration() {
            WalrusStoreBlobState::Finished(BlobStoreResult::AlreadyCertified {
                blob_id: blob_object.blob_id,
                event_or_object: EventOrObjectId::Object(blob_object.id),
                end_epoch: blob_object.storage.end_epoch,
            })
        } else {
            WalrusStoreBlobState::Unfinished(OwnedRegisteredBlob {
                encoded_blob: self.encoded_blob,
                status: self.status,
                blob_object,
                operation,
            })
        }
    }
}

/// An owned blob object that has been registered and is ready for the next owned-store step.
#[derive(Debug, Clone, PartialEq)]
pub struct OwnedRegisteredBlob {
    /// The encoded blob.
    pub encoded_blob: EncodedBlob,
    /// The current status of the blob in the system.
    pub status: BlobStatus,
    /// The blob object that was registered.
    pub blob_object: Blob,
    /// The operation that is performed to store the blob.
    pub operation: RegisterBlobOp,
}

impl WalrusStoreEncodedBlobApi for OwnedRegisteredBlob {
    const STATE: &'static str = "OwnedRegistered";

    fn blob_id(&self) -> BlobId {
        self.encoded_blob.blob_id()
    }
}

impl OwnedRegisteredBlob {
    /// Classifies the registered owned blob into either an upload or finalize step.
    pub fn classify(self) -> Either<OwnedBlobAwaitingUpload, OwnedBlobPendingCertifyAndExtend> {
        match self.operation {
            RegisterBlobOp::ReuseAndExtend {
                epochs_extended, ..
            } => Either::Right(OwnedBlobPendingCertifyAndExtend {
                blob_object: self.blob_object,
                operation: self.operation,
                certificate: None,
                epochs_extended: Some(epochs_extended),
            }),
            _ => {
                let epochs_extended = if let RegisterBlobOp::ReuseAndExtendNonCertified {
                    epochs_extended,
                    ..
                } = &self.operation
                {
                    Some(*epochs_extended)
                } else {
                    None
                };
                Either::Left(OwnedBlobAwaitingUpload {
                    encoded_blob: self.encoded_blob,
                    status: self.status,
                    blob_object: self.blob_object,
                    operation: self.operation,
                    epochs_extended,
                })
            }
        }
    }
}

/// An owned blob that still needs a storage certificate.
// INV: The operation is NOT `ReuseAndExtend`.
// INV: The epochs_extended is `Some` iff the operation is `ReuseAndExtendNonCertified`.
#[derive(Debug, Clone, PartialEq)]
pub struct OwnedBlobAwaitingUpload {
    /// The encoded blob.
    pub encoded_blob: EncodedBlob,
    /// The current status of the blob in the system.
    pub status: BlobStatus,
    /// The blob object that was registered.
    pub blob_object: Blob,
    /// The operation that is performed to store the blob.
    pub operation: RegisterBlobOp,
    /// The number of epochs by which to extend the blob after certification.
    pub epochs_extended: Option<EpochCount>,
}

impl WalrusStoreBlobUnfinished<OwnedBlobAwaitingUpload> {
    /// Converts the blob to a blob ready for certification and optional extension.
    pub fn with_certificate_result(
        self,
        certificate: ClientResult<ConfirmationCertificate>,
    ) -> ClientResult<WalrusStoreBlobMaybeFinished<OwnedBlobPendingCertifyAndExtend>> {
        let certificate = certificate?;
        self.into_maybe_finished().map(
            move |blob| {
                Ok(OwnedBlobPendingCertifyAndExtend {
                    blob_object: blob.blob_object,
                    operation: blob.operation,
                    certificate: Some(Arc::new(certificate)),
                    epochs_extended: blob.epochs_extended,
                })
            },
            "with_certificate_result",
        )
    }
}

impl WalrusStoreEncodedBlobApi for OwnedBlobAwaitingUpload {
    const STATE: &'static str = "OwnedAwaitingUpload";

    fn blob_id(&self) -> BlobId {
        self.encoded_blob.blob_id()
    }
}

/// An owned blob that is ready to be certified and/or extended on Sui.
// INV: The certificate is `None` iff the operation is `ReuseAndExtend`.
// INV: The epochs_extended is `Some` iff the operation is `ReuseAndExtend` or
// `ReuseAndExtendNonCertified`.
#[derive(Clone, PartialEq, Debug)]
pub struct OwnedBlobPendingCertifyAndExtend {
    /// The blob object that was registered.
    pub blob_object: Blob,
    /// The operation that is performed to store the blob.
    pub operation: RegisterBlobOp,
    /// The certificate for the blob.
    pub certificate: Option<Arc<ConfirmationCertificate>>,
    /// The number of epochs by which to extend the blob.
    pub epochs_extended: Option<EpochCount>,
}

impl WalrusStoreEncodedBlobApi for OwnedBlobPendingCertifyAndExtend {
    const STATE: &'static str = "OwnedPendingCertifyAndExtend";

    fn blob_id(&self) -> BlobId {
        self.blob_object.blob_id
    }
}

impl<'a> WalrusStoreBlobUnfinished<OwnedBlobPendingCertifyAndExtend> {
    /// Returns the parameters for the Sui transaction to certify and extend the blob.
    pub fn get_certify_and_extend_params(&'a self) -> CertifyAndExtendBlobParams<'a> {
        CertifyAndExtendBlobParams {
            blob: &self.state.blob_object,
            attribute: &self.common.attribute,
            certificate: self.state.certificate.clone(),
            epochs_extended: self.state.epochs_extended,
        }
    }
}

impl OwnedBlobPendingCertifyAndExtend {
    /// Converts the blob to a blob store result based on the given certify-and-extend result.
    pub fn with_certify_and_extend_result(
        self,
        certify_and_extend_result: Option<&CertifyAndExtendBlobResult>,
        price_computation: &PriceComputation,
    ) -> BlobStoreResult {
        let Some(certify_and_extend_result) = certify_and_extend_result else {
            return BlobStoreResult::Error {
                blob_id: Some(self.blob_object.blob_id),
                failure_phase: "with_certify_and_extend_result".to_string(),
                error_msg: "Sui transaction result did not contain a result for the blob"
                    .to_string(),
            };
        };

        let cost = price_computation.operation_cost(&self.operation);
        let shared_blob_object = certify_and_extend_result.shared_blob_object();
        BlobStoreResult::NewlyCreated {
            blob_object: self.blob_object,
            resource_operation: self.operation,
            cost,
            shared_blob_object,
        }
    }
}

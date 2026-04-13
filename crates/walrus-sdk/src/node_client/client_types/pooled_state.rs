// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Pooled-blob lifecycle states after registration.

use std::sync::Arc;

use walrus_core::messages::ConfirmationCertificate;
use walrus_sui::types::PooledBlob;

use super::*;
use crate::node_client::responses::PooledBlobStoreResult;

/// A pooled blob that still needs a storage certificate.
#[derive(Debug, Clone, PartialEq)]
pub struct PooledBlobAwaitingUpload {
    /// The encoded blob.
    pub encoded_blob: EncodedBlob,
    /// The pooled blob object created in the storage pool.
    pub pooled_blob: PooledBlob,
}

impl WalrusStoreEncodedBlobApi for PooledBlobAwaitingUpload {
    const STATE: &'static str = "PooledAwaitingUpload";

    fn blob_id(&self) -> BlobId {
        self.encoded_blob.blob_id()
    }
}

impl WalrusStoreBlobUnfinished<PooledBlobAwaitingUpload> {
    /// Converts the blob to a blob ready for certification.
    pub fn with_certificate_result(
        self,
        certificate: ClientResult<ConfirmationCertificate>,
    ) -> ClientResult<WalrusStoreBlobMaybeFinished<PooledBlobPendingCertify, PooledBlobStoreResult>>
    {
        let certificate = certificate?;
        self.into_maybe_finished_as().map(
            move |blob| {
                Ok(PooledBlobPendingCertify {
                    pooled_blob: blob.pooled_blob,
                    certificate: Arc::new(certificate),
                })
            },
            "with_pooled_certificate_result",
        )
    }
}

/// A pooled blob that is ready to be certified on Sui.
#[derive(Debug, Clone, PartialEq)]
pub struct PooledBlobPendingCertify {
    /// The pooled blob object created in the storage pool.
    pub pooled_blob: PooledBlob,
    /// The certificate for the blob.
    pub certificate: Arc<ConfirmationCertificate>,
}

impl WalrusStoreEncodedBlobApi for PooledBlobPendingCertify {
    const STATE: &'static str = "PooledPendingCertify";

    fn blob_id(&self) -> BlobId {
        self.pooled_blob.blob_id
    }
}

impl PooledBlobPendingCertify {
    /// Replaces the pooled blob with the post-certification version fetched from Sui.
    pub fn with_updated_pooled_blob(self, pooled_blob: PooledBlob) -> Self {
        Self {
            pooled_blob,
            certificate: self.certificate,
        }
    }

    /// Converts the pooled blob to the pooled store result after successful certification.
    pub fn with_certify_result(self) -> PooledBlobStoreResult {
        PooledBlobStoreResult::NewlyCreated {
            pooled_blob_object: self.pooled_blob,
        }
    }
}

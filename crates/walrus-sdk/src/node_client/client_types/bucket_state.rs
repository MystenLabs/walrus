// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Blob-bucket lifecycle states after registration.

use std::sync::Arc;

use walrus_core::messages::ConfirmationCertificate;
use walrus_sui::types::PooledBlob;

use super::*;
use crate::node_client::responses::BlobBucketStoreResult;

/// A pooled blob that still needs a storage certificate.
#[derive(Debug, Clone, PartialEq)]
pub struct BucketBlobAwaitingUpload {
    /// The encoded blob.
    pub encoded_blob: EncodedBlob,
    /// The pooled blob object created in the bucket.
    pub pooled_blob: PooledBlob,
}

impl WalrusStoreEncodedBlobApi for BucketBlobAwaitingUpload {
    const STATE: &'static str = "BucketAwaitingUpload";

    fn blob_id(&self) -> BlobId {
        self.encoded_blob.blob_id()
    }
}

impl WalrusStoreBlobUnfinished<BucketBlobAwaitingUpload> {
    /// Converts the blob to a blob ready for certification.
    pub fn with_certificate_result(
        self,
        certificate: ClientResult<ConfirmationCertificate>,
    ) -> ClientResult<WalrusStoreBlobMaybeFinished<BucketBlobPendingCertify, BlobBucketStoreResult>>
    {
        let certificate = certificate?;
        self.into_maybe_finished().map(
            move |blob| {
                Ok(BucketBlobPendingCertify {
                    pooled_blob: blob.pooled_blob,
                    certificate: Arc::new(certificate),
                })
            },
            "with_bucket_certificate_result",
        )
    }
}

/// A pooled blob that is ready to be certified on Sui.
#[derive(Debug, Clone, PartialEq)]
pub struct BucketBlobPendingCertify {
    /// The pooled blob object created in the bucket.
    pub pooled_blob: PooledBlob,
    /// The certificate for the blob.
    pub certificate: Arc<ConfirmationCertificate>,
}

impl WalrusStoreEncodedBlobApi for BucketBlobPendingCertify {
    const STATE: &'static str = "BucketPendingCertify";

    fn blob_id(&self) -> BlobId {
        self.pooled_blob.blob_id
    }
}

impl BucketBlobPendingCertify {
    /// Replaces the pooled blob with the post-certification version fetched from Sui.
    pub fn with_updated_pooled_blob(self, pooled_blob: PooledBlob) -> Self {
        Self {
            pooled_blob,
            certificate: self.certificate,
        }
    }

    /// Converts the pooled blob to the bucket store result after successful certification.
    pub fn with_certify_result(self) -> BlobBucketStoreResult {
        BlobBucketStoreResult::NewlyCreated {
            pooled_blob_object: self.pooled_blob,
        }
    }
}

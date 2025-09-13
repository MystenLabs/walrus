// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Wrapper for async blob upload that runs the entire upload in background and returns after quorum.

use tokio::sync::oneshot;
use walrus_core::messages::ConfirmationCertificate;

use crate::error::ClientResult;

/// Wrapper that allows the upload to continue in background after quorum is reached.
#[derive(Debug)]
pub struct AsyncUploadWrapper {
    /// Receiver for the certificate once quorum is reached
    cert_receiver: oneshot::Receiver<ClientResult<ConfirmationCertificate>>,
}

impl AsyncUploadWrapper {
    /// Create a new async upload wrapper with a channel pair
    pub fn new() -> (Self, oneshot::Sender<ClientResult<ConfirmationCertificate>>) {
        let (cert_sender, cert_receiver) = oneshot::channel();
        (Self { cert_receiver }, cert_sender)
    }

    /// Wait for the certificate to be sent from the background task
    pub async fn wait_for_certificate(self) -> ClientResult<ConfirmationCertificate> {
        match self.cert_receiver.await {
            Ok(result) => result,
            Err(_) => Err(crate::error::ClientError::store_blob_internal(
                "Failed to receive certificate from background upload task".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_async_upload_wrapper_creation() {
        let (_wrapper, _sender) = AsyncUploadWrapper::new();
        // Should be able to create wrapper successfully without panicking
    }

    #[tokio::test]
    async fn test_async_upload_wrapper_error() {
        let (wrapper, sender) = AsyncUploadWrapper::new();

        // Send error via channel
        let _ = sender.send(Err(crate::error::ClientError::store_blob_internal(
            "test error".to_string(),
        )));

        // Wait for certificate
        let result = wrapper.wait_for_certificate().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_async_upload_wrapper_dropped_sender() {
        let (wrapper, sender) = AsyncUploadWrapper::new();

        // Drop the sender without sending anything
        drop(sender);

        // Wait for certificate - should get error about failed to receive
        let result = wrapper.wait_for_certificate().await;
        assert!(result.is_err());
        let error_message = result.unwrap_err().to_string();
        assert!(
            error_message.contains("Failed to receive certificate from background upload task")
        );
    }
}

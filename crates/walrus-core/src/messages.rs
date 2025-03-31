// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Signed off-chain messages.

#[allow(unused)]
#[cfg(feature = "utoipa")]
use alloc::string::String;
use alloc::vec::Vec;
use core::{fmt::Debug, marker::PhantomData};

use fastcrypto::{
    bls12381::min_pk::BLS12381Signature,
    error::FastCryptoError,
    traits::VerifyingKey,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

mod proof_of_possession;
pub use proof_of_possession::{ProofOfPossession, ProofOfPossessionBody, ProofOfPossessionMsg};

mod storage_confirmation;
pub use storage_confirmation::{
    BlobPersistenceType,
    Confirmation,
    SignedStorageConfirmation,
    StorageConfirmation,
};

mod invalid_blob_id;
pub use invalid_blob_id::{InvalidBlobIdAttestation, InvalidBlobIdMsg};

mod sync_shard;
pub use sync_shard::{SignedSyncShardRequest, SyncShardMsg, SyncShardRequest, SyncShardResponse};

mod certificate;
pub use certificate::{CertificateError, ConfirmationCertificate, InvalidBlobCertificate};

use crate::{ensure, wrapped_uint, Epoch, PublicKey};

/// Message format for messages sent to the System Contracts.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProtocolMessage<T> {
    intent: Intent,
    /// The epoch in which this message is generated.
    epoch: Epoch,
    message_contents: T,
}

impl<T> ProtocolMessage<T> {
    /// The epoch in which the message was generated.
    pub fn epoch(&self) -> Epoch {
        self.epoch
    }

    /// The message contents.
    pub fn contents(&self) -> &T {
        &self.message_contents
    }
}

/// A signed message from a storage node.
#[serde_with::serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct SignedMessage<T> {
    /// The BCS-encoded message.
    ///
    /// This is serialized as a base64 string in human-readable encoding formats such as JSON.
    #[serde_as(as = "serde_with::IfIsHumanReadable<serde_with::base64::Base64>")]
    #[cfg_attr(feature = "utoipa", schema(format = Byte))]
    pub serialized_message: Vec<u8>,
    /// The signature over the BCS encoded message.
    #[cfg_attr(feature = "utoipa", schema(format = Byte, value_type = [u8]))]
    pub signature: BLS12381Signature,
    #[serde(skip)]
    message_type: PhantomData<T>,
}

impl<T> SignedMessage<T> {
    /// Returns a signed message, given the serialized message and the signature.
    pub fn new_from_encoded(serialized_message: Vec<u8>, signature: BLS12381Signature) -> Self {
        Self {
            serialized_message,
            signature,
            message_type: PhantomData,
        }
    }
}

impl<T> SignedMessage<T> {
    /// Extracts the underlying message and verifies the signature on this message under
    /// `public_key`.
    pub fn verify_signature_and_get_message<I>(
        &self,
        public_key: &PublicKey,
    ) -> Result<T, MessageVerificationError>
    where
        T: AsRef<ProtocolMessage<I>> + DeserializeOwned,
    {
        let message: T = bcs::from_bytes(&self.serialized_message)?;
        public_key.verify(&self.serialized_message, &self.signature)?;

        Ok(message)
    }

    /// Verifies that the signature on this message is valid for the specified public key and
    /// that the message contains the expected contents.
    pub fn verify_signature_and_contents<I>(
        &self,
        public_key: &PublicKey,
        epoch: Epoch,
        message_contents: &I,
    ) -> Result<T, MessageVerificationError>
    where
        T: AsRef<ProtocolMessage<I>> + DeserializeOwned,
        I: PartialEq,
    {
        let message: T = bcs::from_bytes(&self.serialized_message)?;

        ensure!(
            message.as_ref().epoch() == epoch,
            MessageVerificationError::EpochMismatch {
                actual: message.as_ref().epoch(),
                expected: epoch,
            }
        );

        ensure!(
            message.as_ref().message_contents == *message_contents,
            MessageVerificationError::MessageContent
        );

        // Verify signature last as the most expensive step.
        public_key.verify(&self.serialized_message, &self.signature)?;
        Ok(message)
    }
}

/// Error for invalid intents.
#[derive(Debug, thiserror::Error)]
#[error("expected intent ({expected:?}) does not match that of the message: {actual:?}")]
pub struct InvalidIntent {
    expected: Intent,
    actual: Intent,
}

/// Error returned when unable to verify a protocol message.
#[derive(Debug, thiserror::Error)]
pub enum MessageVerificationError {
    /// The message could not be decoded.
    #[error(transparent)]
    Decode(#[from] bcs::Error),
    /// The epoch did not match the expected epoch.
    #[error("expected epoch ({expected}) does not match that of the message: {actual}")]
    EpochMismatch {
        /// Epoch for which the confirmation was expected.
        expected: Epoch,
        /// Epoch of the confirmation
        actual: Epoch,
    },
    /// The signature verification on the message failed.
    #[error(transparent)]
    SignatureVerification(#[from] FastCryptoError),
    /// The message content is not as expected.
    #[error("message content does not match the expected content")]
    MessageContent,
    /// The message intent is incorrect.
    #[error(transparent)]
    InvalidIntent(#[from] InvalidIntent),
}

wrapped_uint! {
    /// Type for the intent type of signed messages.
    pub struct IntentType(pub u8) {
        /// Intent type for proof of possession messages.
        pub const PROOF_OF_POSSESSION_MSG: Self = Self(0);
        /// Intent type for blob-certification messages.
        pub const BLOB_CERT_MSG: Self = Self(1);
        /// Intent type for invalid blob id messages.
        pub const INVALID_BLOB_ID_MSG: Self = Self(2);
        /// Intent type for invalid blob id messages.
        /// Note that this message is only used for communication between storage nodes.
        pub const SYNC_SHARD_MSG: Self = Self(3);
    }
}

wrapped_uint! {
    /// Type for the intent version of signed messages.
    #[derive(Default)]
    pub struct IntentVersion(pub u8) {
        /// Intent type for storage-certification messages.
        pub const DEFAULT: Self = Self(0);
    }
}

wrapped_uint! {
    /// Type used to identify the app associated with a signed message.
    pub struct IntentAppId(pub u8) {
        /// Walrus App ID.
        pub const STORAGE: Self = Self(3);
    }
}

/// Message intent prepended to signed messages.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Intent {
    /// The intent of the signed message.
    pub r#type: IntentType,
    /// The intent version.
    pub version: IntentVersion,
    /// The app ID, usually [`IntentAppId::STORAGE`] for Walrus messages.
    pub app_id: IntentAppId,
}

impl Intent {
    /// Creates a new intent with [`IntentAppId::STORAGE`] for the specified [`IntentType`].
    pub const fn storage(r#type: IntentType) -> Self {
        Self {
            r#type,
            version: IntentVersion::DEFAULT,
            app_id: IntentAppId::STORAGE,
        }
    }
}

#[cfg(test)]
mod tests {
    use storage_confirmation::{BlobPersistenceType, StorageConfirmationBody};
    use walrus_test_utils::{param_test, Result as TestResult};

    use super::*;
    use crate::{
        messages::{IntentAppId, IntentVersion},
        BlobId,
        Epoch,
    };

    const EPOCH: Epoch = 21;
    const BLOB_ID: BlobId = BlobId([7; 32]);

    impl AsMut<ProtocolMessage<StorageConfirmationBody>> for Confirmation {
        fn as_mut(&mut self) -> &mut ProtocolMessage<StorageConfirmationBody> {
            &mut self.0
        }
    }

    impl AsMut<ProtocolMessage<BlobId>> for InvalidBlobIdMsg {
        fn as_mut(&mut self) -> &mut ProtocolMessage<BlobId> {
            &mut self.0
        }
    }

    param_test! {
        decoding_fails_for_incorrect_message_type -> TestResult: [
            confirmation: (Confirmation::new(EPOCH, BLOB_ID, BlobPersistenceType::Permanent)),
            invalid_blob: (InvalidBlobIdMsg::new(EPOCH, BLOB_ID)),
        ]
    }
    fn decoding_fails_for_incorrect_message_type<T, I>(mut message: T) -> TestResult
    where
        T: AsRef<ProtocolMessage<I>>
            + AsMut<ProtocolMessage<I>>
            + Serialize
            + DeserializeOwned
            + Debug,
    {
        const OTHER_MSG_TYPE: IntentType = IntentType(99);
        assert_ne!(message.as_ref().intent.r#type, OTHER_MSG_TYPE);

        message.as_mut().intent.r#type = OTHER_MSG_TYPE;
        let invalid_message = bcs::to_bytes(&message).expect("successful encoding");

        bcs::from_bytes::<T>(&invalid_message).expect_err("decoding must fail");

        Ok(())
    }

    param_test! {
        decoding_fails_for_unsupported_intent_version -> TestResult: [
            confirmation: (Confirmation::new(EPOCH, BLOB_ID, BlobPersistenceType::Permanent)),
        invalid_blob: (InvalidBlobIdMsg::new(EPOCH, BLOB_ID)),
        ]
    }
    fn decoding_fails_for_unsupported_intent_version<T, I>(mut message: T) -> TestResult
    where
        T: AsRef<ProtocolMessage<I>>
            + AsMut<ProtocolMessage<I>>
            + Serialize
            + DeserializeOwned
            + Debug,
    {
        const UNSUPPORTED_INTENT_VERSION: IntentVersion = IntentVersion(99);
        assert_ne!(UNSUPPORTED_INTENT_VERSION, IntentVersion::default());

        message.as_mut().intent.version = UNSUPPORTED_INTENT_VERSION;
        let invalid_message = bcs::to_bytes(&message).expect("successful encoding");

        bcs::from_bytes::<T>(&invalid_message).expect_err("decoding must fail");

        Ok(())
    }

    param_test! {
        decoding_fails_for_invalid_app_id -> TestResult: [
            confirmation: (Confirmation::new(EPOCH, BLOB_ID, BlobPersistenceType::Permanent)),
            invalid_blob: (InvalidBlobIdMsg::new(EPOCH, BLOB_ID)),
        ]
    }
    fn decoding_fails_for_invalid_app_id<T, I>(mut message: T) -> TestResult
    where
        T: AsRef<ProtocolMessage<I>>
            + AsMut<ProtocolMessage<I>>
            + Serialize
            + DeserializeOwned
            + Debug,
    {
        const INVALID_APP_ID: IntentAppId = IntentAppId(44);
        assert_ne!(INVALID_APP_ID, IntentAppId::STORAGE);

        message.as_mut().intent.app_id = INVALID_APP_ID;
        let invalid_message = bcs::to_bytes(&message).expect("successful encoding");

        bcs::from_bytes::<T>(&invalid_message).expect_err("decoding must fail");

        Ok(())
    }

    param_test! {
        decoding_succeeds_for_valid_message -> TestResult: [
            confirmation: (Confirmation::new(EPOCH, BLOB_ID, BlobPersistenceType::Permanent)),
            invalid_blob: (InvalidBlobIdMsg::new(EPOCH, BLOB_ID)),
        ]
    }
    fn decoding_succeeds_for_valid_message<T>(message: T) -> TestResult
    where
        T: Serialize + DeserializeOwned + PartialEq + Debug,
    {
        let encoded_message = bcs::to_bytes(&message).expect("successful encoding");

        let decoded_message = bcs::from_bytes(&encoded_message).expect("decoding must succeed");

        assert_eq!(message, decoded_message);

        Ok(())
    }
}

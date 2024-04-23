// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use fastcrypto::{
    bls12381::min_pk::BLS12381Signature,
    error::FastCryptoError,
    traits::VerifyingKey,
};
use serde::{de::Error as _, Deserialize, Serialize};

use super::Intent;
use crate::{
    ensure,
    messages::{IntentAppId, IntentType, IntentVersion},
    BlobId,
    Epoch,
    PublicKey,
};

/// Confirmation from a storage node that it has stored the sliver pairs for a given blob.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum StorageConfirmation {
    /// Confirmation based on the storage node's signature.
    Signed(SignedStorageConfirmation),
}

/// A signed [`Confirmation`] from a storage node.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SignedStorageConfirmation {
    /// The BCS-encoded [`Confirmation`].
    pub confirmation: Vec<u8>,
    /// The signature over the BCS encoded confirmation.
    pub signature: BLS12381Signature,
}

impl SignedStorageConfirmation {
    /// Verifies that this confirmation is valid for the specified public key, epoch, and blob.
    pub fn verify(
        &self,
        public_key: &PublicKey,
        blob_id: &BlobId,
        epoch: Epoch,
    ) -> Result<Confirmation, VerificationError> {
        let intent: Confirmation = bcs::from_bytes(&self.confirmation)?;

        // TODO(giac): when the chain integration is added, ensure that the Epoch checks are
        // consistent and do not cause problems at epoch change.
        ensure!(intent.blob_id == *blob_id, VerificationError::InvalidBlobId);
        ensure!(
            intent.epoch == epoch,
            VerificationError::EpochMismatch {
                actual: intent.epoch,
                expected: epoch,
            }
        );

        // Perform the asymmetric verification last, as it is the most expensive operation.
        public_key.verify(&self.confirmation, &self.signature)?;

        Ok(intent)
    }
}

/// Error raised by [`SignedStorageConfirmation::verify`] when unable to verify
/// the storage confirmation.
#[derive(Debug, thiserror::Error)]
pub enum VerificationError {
    /// The confirmation could not be decoded.
    #[error(transparent)]
    Decode(#[from] bcs::Error),
    /// The epoch did not match the expected epoch.
    #[error("expected epoch ({expected}) does not match that of the confirmation: {actual}")]
    EpochMismatch {
        /// Epoch for which the confirmation was expected.
        expected: Epoch,
        /// Epoch of the confirmation
        actual: Epoch,
    },
    /// The confirmation was not for the expected blob ID.
    #[error("the confirmation was not for the expected blob ID")]
    InvalidBlobId,
    /// The signature verification on the storage confirmation failed.
    #[error(transparent)]
    Signature(#[from] FastCryptoError),
}

/// A non-empty list of shards, confirmed as storing their sliver
/// pairs for the given blob_id, as of the specified epoch.
#[derive(Debug, Deserialize, Serialize)]
pub struct Confirmation {
    #[serde(deserialize_with = "deserialize_storage_confirmation_intent")]
    intent: Intent,
    /// The epoch in which this confirmation is generated.
    pub epoch: Epoch,
    /// The ID of the Blob whose sliver pairs are confirmed as being stored.
    pub blob_id: BlobId,
}

impl Confirmation {
    /// Creates a new confirmation message for the provided blob ID.
    pub fn new(epoch: Epoch, blob_id: BlobId) -> Self {
        Self {
            intent: Intent::storage(IntentType::BLOB_CERT_MSG),
            epoch,
            blob_id,
        }
    }
}

fn deserialize_storage_confirmation_intent<'de, D>(deserializer: D) -> Result<Intent, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let unverified_intent = Intent::deserialize(deserializer)?;

    if unverified_intent.r#type != IntentType::BLOB_CERT_MSG {
        return Err(D::Error::custom(format!(
            "invalid intent type for storage confirmation: {:?}",
            unverified_intent.r#type
        )));
    }
    if unverified_intent.version != IntentVersion::DEFAULT {
        return Err(D::Error::custom(format!(
            "invalid intent version for storage confirmation: {:?}",
            unverified_intent.version
        )));
    }

    if unverified_intent.app_id != IntentAppId::STORAGE {
        return Err(D::Error::custom(format!(
            "invalid App ID for storage confirmation: {:?}",
            unverified_intent.app_id
        )));
    }

    Ok(unverified_intent)
}

#[cfg(test)]
mod tests {
    use walrus_test_utils::Result as TestResult;

    use super::*;

    const EPOCH: Epoch = 21;
    const BLOB_ID: BlobId = BlobId([7; 32]);

    #[test]
    fn confirmation_has_correct_header() {
        let confirmation = Confirmation::new(EPOCH, BLOB_ID);
        let encoded = bcs::to_bytes(&confirmation).expect("successful encoding");

        assert_eq!(
            encoded[..3],
            [
                IntentType::BLOB_CERT_MSG.0,
                IntentVersion::default().0,
                IntentAppId::STORAGE.0
            ]
        );
        assert_eq!(encoded[3..11], EPOCH.to_le_bytes());
    }

    #[test]
    fn decoding_fails_for_incorrect_message_type() -> TestResult {
        const OTHER_MSG_TYPE: IntentType = IntentType(0);
        assert_ne!(IntentType::BLOB_CERT_MSG, OTHER_MSG_TYPE);

        let mut confirmation = Confirmation::new(EPOCH, BLOB_ID);
        confirmation.intent.r#type = OTHER_MSG_TYPE;
        let invalid_message = bcs::to_bytes(&confirmation).expect("successful encoding");

        bcs::from_bytes::<Confirmation>(&invalid_message).expect_err("decoding must fail");

        Ok(())
    }

    #[test]
    fn decoding_fails_for_unsupported_intent_version() -> TestResult {
        const UNSUPPORTED_INTENT_VERSION: IntentVersion = IntentVersion(99);
        assert_ne!(UNSUPPORTED_INTENT_VERSION, IntentVersion::default());

        let mut confirmation = Confirmation::new(EPOCH, BLOB_ID);
        confirmation.intent.version = UNSUPPORTED_INTENT_VERSION;
        let invalid_message = bcs::to_bytes(&confirmation).expect("successful encoding");

        bcs::from_bytes::<Confirmation>(&invalid_message).expect_err("decoding must fail");

        Ok(())
    }

    #[test]
    fn decoding_fails_for_invalid_app_id() -> TestResult {
        const INVALID_APP_ID: IntentAppId = IntentAppId(44);
        assert_ne!(INVALID_APP_ID, IntentAppId::STORAGE);

        let mut confirmation = Confirmation::new(EPOCH, BLOB_ID);
        confirmation.intent.app_id = INVALID_APP_ID;
        let invalid_message = bcs::to_bytes(&confirmation).expect("successful encoding");

        bcs::from_bytes::<Confirmation>(&invalid_message).expect_err("decoding must fail");

        Ok(())
    }

    #[test]
    fn decoding_succeeds_for_valid_message() -> TestResult {
        let confirmation = Confirmation::new(EPOCH, BLOB_ID);
        let message = bcs::to_bytes(&confirmation).expect("successful encoding");

        let confirmation =
            bcs::from_bytes::<Confirmation>(&message).expect("decoding must succeed");

        assert_eq!(confirmation.blob_id, BLOB_ID);
        assert_eq!(confirmation.epoch, EPOCH);

        Ok(())
    }
}

use fastcrypto::bls12381::min_pk::BLS12381Signature;
use serde::{de::Error as _, Deserialize, Serialize};

use crate::{
    messages::{IntentAppId, IntentType, IntentVersion},
    BlobId,
    Epoch,
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

/// A non-empty list of shards, confirmed as storing their sliver
/// pairs for the given blob_id, as of the specified epoch.
// Uses serde(remote) to allow validation of the confirmation header fields.
// See https://github.com/serde-rs/serde/issues/1220 for more info.
#[derive(Debug, Deserialize, Serialize)]
#[serde(remote = "Self")]
pub struct Confirmation {
    header: MessageHeader,
    /// The ID of the Blob whose sliver pairs are confirmed as being stored.
    blob_id: BlobId,
}

impl<'de> Deserialize<'de> for Confirmation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let to_verify = Confirmation::deserialize(deserializer)?;
        let MessageHeader {
            intent,
            version,
            app_id,
            ..
        } = to_verify.header;

        if intent != IntentType::STORAGE_CERT_MSG {
            return Err(D::Error::custom(format!(
                "invalid intent type for storage confirmation: {:?}",
                intent
            )));
        }
        if version != IntentVersion::default() {
            return Err(D::Error::custom(format!(
                "invalid intent version for storage confirmation: {:?}",
                version
            )));
        }

        if app_id != IntentAppId::STORAGE {
            return Err(D::Error::custom(format!(
                "invalid App ID for storage confirmation: {:?}",
                app_id
            )));
        }

        Ok(to_verify)
    }
}

impl serde::ser::Serialize for Confirmation {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        Confirmation::serialize(self, serializer)
    }
}

impl Confirmation {
    /// Creates a new confirmation message for the provided blob ID.
    pub fn new(epoch: Epoch, blob_id: BlobId) -> Self {
        Self {
            header: MessageHeader {
                intent: IntentType::STORAGE_CERT_MSG,
                version: IntentVersion::default(),
                app_id: IntentAppId::STORAGE,
                epoch,
            },
            blob_id,
        }
    }

    /// Returns the Walrus epoch in which this message was generated.
    pub fn epoch(&self) -> Epoch {
        self.header.epoch
    }

    /// Returns the blob id associated with this confirmation.
    pub fn blob_id(&self) -> &BlobId {
        &self.blob_id
    }
}

/// Message header prepended to signed messages.
#[derive(Debug, Serialize, Deserialize)]
pub struct MessageHeader {
    /// The intent of the signed message.
    pub intent: IntentType,
    /// The intent version.
    pub version: IntentVersion,
    /// The app ID, usually [`IntentAppId::STORAGE`] for Walrus messages.
    pub app_id: IntentAppId,
    /// The epoch in which this confirmation is generated.
    pub epoch: Epoch,
}

#[cfg(test)]
mod tests {
    use walrus_test_utils::Result as TestResult;

    use super::*;

    const EPOCH: Epoch = 21;
    const BLOB_ID: BlobId = [7; 32];

    #[test]
    fn confirmation_has_correct_header() {
        let confirmation = Confirmation::new(EPOCH, BLOB_ID);
        let encoded = bcs::to_bytes(&confirmation).expect("successful encoding");

        assert_eq!(
            encoded[..3],
            [
                IntentType::STORAGE_CERT_MSG.0,
                IntentVersion::default().0,
                IntentAppId::STORAGE.0
            ]
        );
        assert_eq!(encoded[3..11], EPOCH.to_le_bytes());
    }

    #[test]
    fn decoding_fails_for_incorrect_message_type() -> TestResult {
        const OTHER_MSG_TYPE: IntentType = IntentType(0);
        assert_ne!(IntentType::STORAGE_CERT_MSG, OTHER_MSG_TYPE);

        let mut confirmation = Confirmation::new(EPOCH, BLOB_ID);
        confirmation.header.intent = OTHER_MSG_TYPE;
        let invalid_message = bcs::to_bytes(&confirmation).expect("successful encoding");

        bcs::from_bytes::<Confirmation>(&invalid_message).expect_err("decoding must fail");

        Ok(())
    }

    #[test]
    fn decoding_fails_for_unsupported_intent_version() -> TestResult {
        const UNSUPPORTED_INTENT_VERSION: IntentVersion = IntentVersion(99);
        assert_ne!(UNSUPPORTED_INTENT_VERSION, IntentVersion::default());

        let mut confirmation = Confirmation::new(EPOCH, BLOB_ID);
        confirmation.header.version = UNSUPPORTED_INTENT_VERSION;
        let invalid_message = bcs::to_bytes(&confirmation).expect("successful encoding");

        bcs::from_bytes::<Confirmation>(&invalid_message).expect_err("decoding must fail");

        Ok(())
    }

    #[test]
    fn decoding_fails_for_invalid_app_id() -> TestResult {
        const INVALID_APP_ID: IntentAppId = IntentAppId(44);
        assert_ne!(INVALID_APP_ID, IntentAppId::STORAGE);

        let mut confirmation = Confirmation::new(EPOCH, BLOB_ID);
        confirmation.header.app_id = INVALID_APP_ID;
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

        assert_eq!(confirmation.blob_id(), &BLOB_ID);
        assert_eq!(confirmation.epoch(), EPOCH);

        Ok(())
    }
}

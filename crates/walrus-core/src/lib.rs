//! Core functionality for Walrus.
pub mod merkle;

use fastcrypto::bls12381::min_pk::BLS12381Signature;
use serde::{de::Error as _, Deserialize, Serialize};

/// Erasure encoding and decoding.
pub mod encoding;

/// The epoch number.
pub type Epoch = u64;
/// The ID of a blob.
pub type BlobId = [u8; 32];
/// Represents the index of a shard.
pub type ShardIndex = u32;

/// Type for the intent type of signed messages.
pub type IntentType = u8;
/// Type for the intent version used in signed messages.
pub type IntentVersion = u8;
/// Type used to identify the app associated with a signed message.
pub type IntentAppId = u8;

/// Intent type for storage-certification messages.
pub const STORAGE_CERT_MSG_TYPE: IntentType = 2;

/// Current and default intent version.
pub const INTENT_VERSION: IntentVersion = 0;
/// Walrus APP ID.
pub const STORAGE_APP_ID: IntentAppId = 3;

/// Confirmation from a storage node that it has stored the sliver pairs for a given blob.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum StorageConfirmation {
    /// Confirmation based on the storage node's signature.
    Signed(SignedConfirmation),
}

/// A signed [`Confirmation`] from a storage node.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SignedConfirmation {
    /// The BCS-encoded [`Confirmation`].
    pub confirmation: Vec<u8>,
    /// The signature over the BCS encoded confirmation.
    pub signature: BLS12381Signature,
}

/// A non-empty list of shards, confirmed as storing their sliver pairs for the given blob_id,
/// as of the specified epoch.
#[derive(Debug, Deserialize, Serialize)]
#[serde(remote = "Self")]
pub struct Confirmation {
    header: SignedMessageHeader,
    /// The ID of the Blob whose sliver pairs are confirmed as being stored.
    blob_id: BlobId,
    /// The shards that are confirmed to be storing their slivers.
    // INV: non-empty
    shards: Vec<ShardIndex>,
}

impl<'de> Deserialize<'de> for Confirmation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let to_verify = Confirmation::deserialize(deserializer)?;
        let SignedMessageHeader {
            intent,
            version,
            app_id,
            ..
        } = to_verify.header;

        if intent != STORAGE_CERT_MSG_TYPE {
            return Err(D::Error::custom(format!(
                "invalid intent type for storage confirmation: {}",
                intent
            )));
        }
        if version != INTENT_VERSION {
            return Err(D::Error::custom(format!(
                "invalid intent version for storage confirmation: {}",
                version
            )));
        }

        if app_id != STORAGE_APP_ID {
            return Err(D::Error::custom(format!(
                "invalid App ID for storage confirmation: {}",
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
    /// Maximum number of shards in a confirmation.
    pub const MAX_SHARDS: usize = u16::MAX as usize;

    /// Creates a new confirmation message for the provided blob ID and shards.
    ///
    /// # Panics
    ///
    /// Panics if the list of shards is empty, or contains more than [`Self::MAX_SHARDS`] shard IDs.
    pub fn new(epoch: Epoch, blob_id: BlobId, shards: &[ShardIndex]) -> Self {
        assert!(!shards.is_empty(), "shards must be non-empty");
        assert!(shards.len() <= Self::MAX_SHARDS, "too many shards");

        Self {
            header: SignedMessageHeader {
                intent: STORAGE_CERT_MSG_TYPE,
                version: INTENT_VERSION,
                app_id: STORAGE_APP_ID,
                epoch,
            },
            blob_id,
            shards: shards.into(),
        }
    }

    /// Returns the Walrus epoch in which this message was generated.
    pub fn epoch(&self) -> Epoch {
        self.header.epoch
    }

    /// Returns the shards confirmed to be storing their sliver pairs for the blob ID.
    pub fn shards(&self) -> &[ShardIndex] {
        self.shards.as_ref()
    }

    /// Returns the blob id associated with this confirmation.
    pub fn blob_id(&self) -> &BlobId {
        &self.blob_id
    }
}

/// Message header pre-prended to signed messages.
#[derive(Debug, Serialize, Deserialize)]
pub struct SignedMessageHeader {
    /// The intent of the signed message.
    pub intent: IntentType,
    /// The intent version.
    pub version: IntentVersion,
    /// The app ID, usually [`STORAGE_APP_ID`] for Walrus messages.
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
    const SHARD_INDEX: ShardIndex = 17;
    const OTHER_SHARD_INDEX: ShardIndex = 831;

    #[test]
    fn confirmation_has_correct_header() {
        let confirmation = Confirmation::new(EPOCH, BLOB_ID, &[SHARD_INDEX, OTHER_SHARD_INDEX]);
        let encoded = bcs::to_bytes(&confirmation).expect("successful encoding");

        assert_eq!(
            encoded[..3],
            [STORAGE_CERT_MSG_TYPE, INTENT_VERSION, STORAGE_APP_ID]
        );
        assert_eq!(encoded[3..11], EPOCH.to_le_bytes());
    }

    #[test]
    fn decoding_fails_for_incorrect_message_type() -> TestResult {
        const OTHER_MSG_TYPE: IntentType = 0;
        assert_ne!(STORAGE_CERT_MSG_TYPE, OTHER_MSG_TYPE);

        let mut confirmation = Confirmation::new(EPOCH, BLOB_ID, &[SHARD_INDEX, OTHER_SHARD_INDEX]);
        confirmation.header.intent = OTHER_MSG_TYPE;
        let invalid_message = bcs::to_bytes(&confirmation).expect("successful encoding");

        bcs::from_bytes::<Confirmation>(&invalid_message).expect_err("decoding must fail");

        Ok(())
    }

    #[test]
    fn decoding_fails_for_unsupported_intent_version() -> TestResult {
        const UNSUPPORTED_INTENT_VERSION: IntentVersion = 99;
        assert_ne!(UNSUPPORTED_INTENT_VERSION, INTENT_VERSION);

        let mut confirmation = Confirmation::new(EPOCH, BLOB_ID, &[SHARD_INDEX, OTHER_SHARD_INDEX]);
        confirmation.header.version = UNSUPPORTED_INTENT_VERSION;
        let invalid_message = bcs::to_bytes(&confirmation).expect("successful encoding");

        bcs::from_bytes::<Confirmation>(&invalid_message).expect_err("decoding must fail");

        Ok(())
    }

    #[test]
    fn decoding_fails_for_invalid_app_id() -> TestResult {
        const INVALID_APP_ID: IntentVersion = 44;
        assert_ne!(INVALID_APP_ID, STORAGE_APP_ID);

        let mut confirmation = Confirmation::new(EPOCH, BLOB_ID, &[SHARD_INDEX, OTHER_SHARD_INDEX]);
        confirmation.header.app_id = INVALID_APP_ID;
        let invalid_message = bcs::to_bytes(&confirmation).expect("successful encoding");

        bcs::from_bytes::<Confirmation>(&invalid_message).expect_err("decoding must fail");

        Ok(())
    }

    #[test]
    fn decoding_succeeds_for_valid_message() -> TestResult {
        let confirmation = Confirmation::new(EPOCH, BLOB_ID, &[SHARD_INDEX, OTHER_SHARD_INDEX]);
        let message = bcs::to_bytes(&confirmation).expect("successful encoding");

        let confirmation =
            bcs::from_bytes::<Confirmation>(&message).expect("decoding must succeed");

        assert_eq!(confirmation.blob_id(), &BLOB_ID);
        assert_eq!(confirmation.shards(), confirmation.shards());
        assert_eq!(confirmation.epoch(), EPOCH);

        Ok(())
    }
}

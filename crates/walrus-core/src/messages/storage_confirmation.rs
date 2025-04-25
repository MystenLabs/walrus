// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

#[allow(unused)]
#[cfg(feature = "utoipa")]
use alloc::{format, string::String, vec::Vec};

use serde::{Deserialize, Serialize};

use super::{Intent, InvalidIntent, MessageVerificationError, ProtocolMessage, SignedMessage};
use crate::{BlobId, Epoch, PublicKey, SuiObjectId, messages::IntentType};

/// Confirmation from a storage node that it has stored the sliver pairs for a given blob.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub enum StorageConfirmation {
    /// Confirmation based on the storage node's signature.
    #[cfg_attr(feature = "utoipa", schema(value_type = SignedMessage::<u8>))]
    Signed(SignedStorageConfirmation),
}

/// Indicates the persistence of a blob.
///
/// For deletable blobs the object ID of the associated Sui object is included.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlobPersistenceType {
    /// The blob is permanent.
    Permanent,
    /// The blob is deletable and has the given object ID.
    Deletable {
        /// The object ID of the associated Sui object.
        object_id: SuiObjectId,
    },
}

/// The message body for a [`Confirmation`],
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageConfirmationBody {
    /// The blob id of the blob that is being confirmed.
    pub blob_id: BlobId,
    /// Whether the blob is permanent or deletable.
    /// For deletable blobs, the object id of the blob is included.
    pub blob_type: BlobPersistenceType,
}

/// A Confirmation that a storage node has stored all respective slivers
/// of a blob in their shards.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
#[serde(try_from = "ProtocolMessage<StorageConfirmationBody>")]
pub struct Confirmation(pub(crate) ProtocolMessage<StorageConfirmationBody>);

impl Confirmation {
    const INTENT: Intent = Intent::storage(IntentType::BLOB_CERT_MSG);

    /// Creates a new confirmation message for the provided blob ID.
    pub fn new(epoch: Epoch, blob_id: BlobId, blob_type: BlobPersistenceType) -> Self {
        let message_contents = StorageConfirmationBody { blob_id, blob_type };
        Self(ProtocolMessage {
            intent: Intent::storage(IntentType::BLOB_CERT_MSG),
            epoch,
            message_contents,
        })
    }
}

impl TryFrom<ProtocolMessage<StorageConfirmationBody>> for Confirmation {
    type Error = InvalidIntent;
    fn try_from(
        protocol_message: ProtocolMessage<StorageConfirmationBody>,
    ) -> Result<Self, Self::Error> {
        if protocol_message.intent == Self::INTENT {
            Ok(Self(protocol_message))
        } else {
            Err(InvalidIntent {
                expected: Self::INTENT,
                actual: protocol_message.intent,
            })
        }
    }
}

impl AsRef<ProtocolMessage<StorageConfirmationBody>> for Confirmation {
    fn as_ref(&self) -> &ProtocolMessage<StorageConfirmationBody> {
        &self.0
    }
}

/// A signed [`Confirmation`] from a storage node.
pub type SignedStorageConfirmation = SignedMessage<Confirmation>;

impl SignedStorageConfirmation {
    /// Verifies that this confirmation is valid for the specified public key, epoch, and blob.
    pub fn verify(
        &self,
        public_key: &PublicKey,
        epoch: Epoch,
        blob_id: BlobId,
        blob_type: BlobPersistenceType,
    ) -> Result<Confirmation, MessageVerificationError> {
        self.verify_signature_and_contents(
            public_key,
            epoch,
            &StorageConfirmationBody { blob_id, blob_type },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messages::{IntentAppId, IntentVersion};

    const EPOCH: Epoch = 21;
    const BLOB_ID: BlobId = BlobId([7; 32]);

    #[test]
    fn confirmation_is_correctly_encoded_permanent() {
        let confirmation = Confirmation::new(EPOCH, BLOB_ID, BlobPersistenceType::Permanent);
        let encoded = bcs::to_bytes(&confirmation).expect("successful encoding");

        assert_eq!(
            encoded[..3],
            [
                IntentType::BLOB_CERT_MSG.0,
                IntentVersion::default().0,
                IntentAppId::STORAGE.0
            ]
        );
        assert_eq!(encoded[3..7], EPOCH.to_le_bytes());
        assert_eq!(
            encoded[7..39],
            bcs::to_bytes(&BLOB_ID).expect("successful encoding")
        );
        assert_eq!(
            encoded[39..],
            // BlobPersistenceType::Permanent should be encoded as 0
            bcs::to_bytes(&0u8).expect("successful encoding")
        );
    }

    #[test]
    fn confirmation_is_correctly_encoded_deletable() {
        let object_id = SuiObjectId([42; 32]);
        let confirmation =
            Confirmation::new(EPOCH, BLOB_ID, BlobPersistenceType::Deletable { object_id });
        let encoded = bcs::to_bytes(&confirmation).expect("successful encoding");

        assert_eq!(
            encoded[..3],
            [
                IntentType::BLOB_CERT_MSG.0,
                IntentVersion::default().0,
                IntentAppId::STORAGE.0
            ]
        );
        assert_eq!(encoded[3..7], EPOCH.to_le_bytes());
        assert_eq!(
            encoded[7..39],
            bcs::to_bytes(&BLOB_ID).expect("successful encoding")
        );
        assert_eq!(
            encoded[39..40],
            // BlobPersistenceType::Deletable should be encoded as 1, followed by the object ID
            bcs::to_bytes(&1u8).expect("successful encoding")
        );
        assert_eq!(
            encoded[40..],
            bcs::to_bytes(&object_id).expect("successful encoding")
        );
    }
}

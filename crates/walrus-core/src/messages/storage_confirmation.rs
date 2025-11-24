// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

#[allow(unused)]
#[cfg(feature = "utoipa")]
use alloc::{format, string::String, vec::Vec};

use serde::{Deserialize, Serialize};

use super::{Intent, InvalidIntent, MessageVerificationError, ProtocolMessage, SignedMessage};
use crate::{BlobId, Epoch, PublicKey, SuiObjectId, ensure, messages::IntentType};

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
/// For managed blobs, the BlobManager ID and optional ManagedBlob object ID are included.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlobPersistenceType {
    /// The blob is permanent.
    Permanent,
    /// The blob is deletable and has the given object ID.
    Deletable {
        /// The object ID of the associated Sui object.
        object_id: SuiObjectId,
    },
    /// The blob is managed by a BlobManager.
    Managed {
        /// The object ID of the BlobManager.
        blob_manager_id: SuiObjectId,
        /// Whether the managed blob is deletable.
        deletable: bool,
        /// The object ID of the ManagedBlob.
        /// - Zero/null ObjectID when client sends request (client doesn't know it yet).
        /// - Actual ManagedBlob object_id when server returns confirmation
        ///   (for both deletable and permanent).
        blob_object_id: SuiObjectId,
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
        // For Managed blobs, use custom verification logic.
        if let BlobPersistenceType::Managed {
            blob_manager_id: client_manager_id,
            deletable: client_deletable,
            ..
        } = blob_type
        {
            // First verify the signature and deserialize the message.
            let message = self.verify_signature_and_get_message(public_key)?;

            // Verify epoch and blob_id.
            ensure!(
                message.0.message_contents.blob_id == blob_id,
                MessageVerificationError::MessageContent
            );
            ensure!(
                message.0.epoch == epoch,
                MessageVerificationError::EpochMismatch {
                    actual: message.0.epoch,
                    expected: epoch,
                }
            );

            // For Managed blobs, verify blob_manager_id and deletable flag.
            if let BlobPersistenceType::Managed {
                blob_manager_id: server_manager_id,
                deletable: server_deletable,
                blob_object_id: server_object_id,
            } = message.0.message_contents.blob_type
            {
                // Verify blob_manager_id matches.
                ensure!(
                    server_manager_id == client_manager_id,
                    MessageVerificationError::MessageContent
                );
                // Verify deletable flag matches.
                ensure!(
                    server_deletable == client_deletable,
                    MessageVerificationError::MessageContent
                );
                // Verify blob_object_id is populated for all managed blobs.
                // Both deletable and permanent managed blobs have real ManagedBlob objects.
                ensure!(
                    server_object_id != SuiObjectId::ZERO,
                    MessageVerificationError::MessageContent
                );
            } else {
                // Server returned non-Managed type when we expected Managed.
                return Err(MessageVerificationError::MessageContent);
            }

            Ok(message)
        } else {
            // For non-Managed blobs, use exact match verification.
            self.verify_signature_and_contents(
                public_key,
                epoch,
                &StorageConfirmationBody { blob_id, blob_type },
            )
        }
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

    #[test]
    fn confirmation_is_correctly_encoded_managed_permanent() {
        use std::println;
        let blob_manager_id = SuiObjectId([42; 32]);
        let blob_object_id = SuiObjectId([99; 32]);
        let confirmation = Confirmation::new(
            EPOCH,
            BLOB_ID,
            BlobPersistenceType::Managed {
                blob_manager_id,
                deletable: false,
                blob_object_id,
            },
        );
        let encoded = bcs::to_bytes(&confirmation).expect("successful encoding");

        println!("Managed permanent BCS encoding:");
        println!("  Full length: {}", encoded.len());
        println!("  Variant index (byte 39): {}", encoded[39]);
        println!("  First 10 bytes after variant: {:?}", &encoded[40..50]);

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
        // BlobPersistenceType::Managed should be encoded as 2.
        assert_eq!(encoded[39], 2u8);
        // Followed by blob_manager_id.
        assert_eq!(
            encoded[40..72],
            bcs::to_bytes(&blob_manager_id).expect("successful encoding")
        );
        // Followed by deletable bool.
        assert_eq!(encoded[72], 0u8); // false
        // Followed by blob_object_id (real object ID, not zero).
        assert_eq!(
            encoded[73..105],
            bcs::to_bytes(&blob_object_id).expect("successful encoding")
        );
        assert_eq!(encoded.len(), 105);
    }

    #[test]
    fn confirmation_is_correctly_encoded_managed_deletable() {
        use std::println;
        let blob_manager_id = SuiObjectId([42; 32]);
        let blob_object_id = SuiObjectId([99; 32]);
        let confirmation = Confirmation::new(
            EPOCH,
            BLOB_ID,
            BlobPersistenceType::Managed {
                blob_manager_id,
                deletable: true,
                blob_object_id,
            },
        );
        let encoded = bcs::to_bytes(&confirmation).expect("successful encoding");

        println!("Managed deletable BCS encoding:");
        println!("  Full length: {}", encoded.len());
        println!("  Variant index (byte 39): {}", encoded[39]);
        println!("  First 10 bytes after variant: {:?}", &encoded[40..50]);

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
        // BlobPersistenceType::Managed should be encoded as 2.
        assert_eq!(encoded[39], 2u8);
        // Followed by blob_manager_id.
        assert_eq!(
            encoded[40..72],
            bcs::to_bytes(&blob_manager_id).expect("successful encoding")
        );
        // Followed by deletable bool.
        assert_eq!(encoded[72], 1u8); // true
        // Followed by blob_object_id.
        assert_eq!(
            encoded[73..105],
            bcs::to_bytes(&blob_object_id).expect("successful encoding")
        );
        assert_eq!(encoded.len(), 105);
    }
}

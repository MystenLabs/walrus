// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

use super::{Intent, InvalidIntent, MessageVerificationError, ProtocolMessage, SignedMessage};
use crate::{messages::IntentType, BlobId, Epoch, PublicKey};

/// Confirmation from a storage node that it has stored the sliver pairs for a given blob.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum StorageConfirmation {
    /// Confirmation based on the storage node's signature.
    Signed(SignedStorageConfirmation),
}

/// A Confirmation that a storage node has stored all respective slivers
/// of a blob in their shards.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "ProtocolMessage<BlobId>")]
pub struct Confirmation(pub(crate) ProtocolMessage<BlobId>);

impl Confirmation {
    const INTENT: Intent = Intent::storage(IntentType::BLOB_CERT_MSG);

    /// Creates a new confirmation message for the provided blob ID.
    pub fn new(epoch: Epoch, blob_id: BlobId) -> Self {
        Self(ProtocolMessage {
            intent: Intent::storage(IntentType::BLOB_CERT_MSG),
            epoch,
            message_contents: blob_id,
        })
    }
}

impl TryFrom<ProtocolMessage<BlobId>> for Confirmation {
    type Error = InvalidIntent;
    fn try_from(protocol_message: ProtocolMessage<BlobId>) -> Result<Self, Self::Error> {
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

impl AsRef<ProtocolMessage<BlobId>> for Confirmation {
    fn as_ref(&self) -> &ProtocolMessage<BlobId> {
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
        blob_id: &BlobId,
    ) -> Result<Confirmation, MessageVerificationError> {
        self.verify_signature_and_contents(public_key, epoch, blob_id)
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::messages::{IntentAppId, IntentVersion};

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
}

// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

use super::{Intent, InvalidIntent, MessageVerificationError, ProtocolMessage, SignedMessage};
use crate::{messages::IntentType, BlobId, Epoch, PublicKey};

/// A message stating that a Blob Id is invalid.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[serde(try_from = "ProtocolMessage<BlobId>")]
pub struct InvalidBlobIdMsg(pub(crate) ProtocolMessage<BlobId>);

impl InvalidBlobIdMsg {
    const INTENT: Intent = Intent::storage(IntentType::INVALID_BLOB_ID_MSG);

    /// Creates a new InvalidBlobIdMsg message for the provided blob ID.
    pub fn new(epoch: Epoch, blob_id: BlobId) -> Self {
        Self(ProtocolMessage {
            intent: Intent::storage(IntentType::INVALID_BLOB_ID_MSG),
            epoch,
            message_contents: blob_id,
        })
    }
}

impl TryFrom<ProtocolMessage<BlobId>> for InvalidBlobIdMsg {
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

impl AsRef<ProtocolMessage<BlobId>> for InvalidBlobIdMsg {
    fn as_ref(&self) -> &ProtocolMessage<BlobId> {
        &self.0
    }
}

/// A signed [`InvalidBlobIdMsg`] from a storage node.
pub type InvalidBlobIdAttestation = SignedMessage<InvalidBlobIdMsg>;

impl InvalidBlobIdAttestation {
    /// Verifies that this invalid blob attestation is valid for the specified public key,
    /// epoch, and blob id.
    pub fn verify(
        &self,
        public_key: &PublicKey,
        epoch: Epoch,
        blob_id: &BlobId,
    ) -> Result<InvalidBlobIdMsg, MessageVerificationError> {
        self.verify_signature_and_contents(public_key, epoch, blob_id)
    }
}

#[cfg(test)]
mod tests {
    use alloc::vec::Vec;

    use super::*;
    use crate::messages::{IntentAppId, IntentVersion};

    const EPOCH: Epoch = 21;

    #[test]
    fn msg_has_correct_header() {
        let blob_id = BlobId((0..32).collect::<Vec<_>>().try_into().unwrap());
        let msg = InvalidBlobIdMsg::new(EPOCH, blob_id);
        let encoded = bcs::to_bytes(&msg).expect("successful encoding");

        assert_eq!(
            encoded[..3],
            [
                IntentType::INVALID_BLOB_ID_MSG.0,
                IntentVersion::default().0,
                IntentAppId::STORAGE.0
            ]
        );
        assert_eq!(encoded[3..7], EPOCH.to_le_bytes());
        assert_eq!(encoded[7..], blob_id.0);
    }
}

// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use serde::Serialize;

use super::{Intent, InvalidIntent, ProtocolMessage, SignedMessage};
use crate::{Epoch, PublicKey, messages::IntentType};

/// The message body for a `ProofOfPossessionMsg`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProofOfPossessionBody {
    /// The sui address of the signer as bytes.
    pub sui_address: [u8; 32],
    /// The bls public key of the signer.
    pub bls_public_key: PublicKey,
}

/// A message to create a proof of possession of the associated private key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProofOfPossessionMsg(pub(crate) ProtocolMessage<ProofOfPossessionBody>);

impl ProofOfPossessionMsg {
    const INTENT: Intent = Intent::storage(IntentType::PROOF_OF_POSSESSION_MSG);

    /// Creates a new `ProofOfPossessionMsg` with the given epoch, sui address, and BLS public key.
    pub fn new(epoch: Epoch, sui_address: [u8; 32], bls_public_key: PublicKey) -> Self {
        Self(ProtocolMessage {
            intent: Intent::storage(IntentType::PROOF_OF_POSSESSION_MSG),
            epoch,
            message_contents: ProofOfPossessionBody {
                sui_address,
                bls_public_key,
            },
        })
    }
}

impl TryFrom<ProtocolMessage<ProofOfPossessionBody>> for ProofOfPossessionMsg {
    type Error = InvalidIntent;
    fn try_from(
        protocol_message: ProtocolMessage<ProofOfPossessionBody>,
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

impl AsRef<ProtocolMessage<ProofOfPossessionBody>> for ProofOfPossessionMsg {
    fn as_ref(&self) -> &ProtocolMessage<ProofOfPossessionBody> {
        &self.0
    }
}

/// A signed [`ProofOfPossessionMsg`] from a storage node.
pub type ProofOfPossession = SignedMessage<ProofOfPossessionMsg>;

#[cfg(test)]
mod tests {
    use fastcrypto::traits::InsecureDefault;

    use super::*;
    use crate::messages::{IntentAppId, IntentVersion};

    const EPOCH: Epoch = 21;

    #[test]
    fn message_has_correct_header() {
        let sui_address = [42; 32];
        let message = ProofOfPossessionMsg::new(EPOCH, sui_address, PublicKey::insecure_default());
        let encoded = bcs::to_bytes(&message).expect("successful encoding");

        assert_eq!(
            encoded[..3],
            [
                IntentType::PROOF_OF_POSSESSION_MSG.0,
                IntentVersion::default().0,
                IntentAppId::STORAGE.0
            ]
        );
        assert_eq!(encoded[3..7], EPOCH.to_le_bytes());
    }
}

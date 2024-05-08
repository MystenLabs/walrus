// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

use super::{Intent, ProtocolMessage, SignedMessage};
use crate::{messages::IntentType, BlobId, Epoch};

/// A signed [`InvalidBlobIdMsg`] from a storage node.
pub type InvalidBlobIdAttestation = SignedMessage<InvalidBlobIdMsg>;

/// Message type stating that `blob_id` is invalid.
#[derive(Debug, Deserialize, Serialize)]
pub struct InvalidBlobIdMsg {
    intent: Intent,
    /// The epoch in which this message is generated.
    pub epoch: Epoch,
    /// The ID of the Blob marked as invalid.
    pub blob_id: BlobId,
}

impl InvalidBlobIdMsg {
    /// Creates a new confirmation message for the provided blob ID.
    pub fn new(epoch: Epoch, blob_id: BlobId) -> Self {
        Self {
            intent: Intent::storage(IntentType::INVALID_BLOB_ID_MSG),
            epoch,
            blob_id,
        }
    }
}

impl ProtocolMessage for InvalidBlobIdMsg {}

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
        assert_eq!(encoded[3..11], EPOCH.to_le_bytes());
        assert_eq!(encoded[11..], blob_id.0);
    }
}

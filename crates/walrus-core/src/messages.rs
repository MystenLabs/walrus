// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Signed off-chain messages.

use std::marker::PhantomData;

use fastcrypto::bls12381::min_pk::BLS12381Signature;
use serde::{Deserialize, Serialize};

mod storage_confirmation;
pub use storage_confirmation::{
    Confirmation,
    SignedStorageConfirmation,
    StorageConfirmation,
    VerificationError,
};

mod invalid_blob_id;
pub use invalid_blob_id::{InvalidBlobIdAttestation, InvalidBlobIdMsg};

mod certificate;
pub use certificate::{ConfirmationCertificate, InvalidBlobCertificate};

/// Trait implemented by system messages.
pub trait ProtocolMessage: Serialize + for<'de> Deserialize<'de> {}

/// A signed message from a storage node.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SignedMessage<T> {
    /// The BCS-encoded message.
    pub serialized_message: Vec<u8>,
    /// The signature over the BCS encoded message.
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

use crate::wrapped_uint;

wrapped_uint! {
    /// Type for the intent type of signed messages.
    pub struct IntentType(pub u8) {
        /// Intent type for blob-certification messages.
        pub const BLOB_CERT_MSG: Self = Self(1);
        /// Intent type for invalid blob id messages.
        pub const INVALID_BLOB_ID_MSG: Self = Self(2);
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
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
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
    pub fn storage(r#type: IntentType) -> Self {
        Self {
            r#type,
            version: IntentVersion::DEFAULT,
            app_id: IntentAppId::STORAGE,
        }
    }
}

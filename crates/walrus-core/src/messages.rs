// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Signed off-chain messages.

use serde::{Deserialize, Serialize};

mod storage_confirmation;
pub use storage_confirmation::{Confirmation, SignedStorageConfirmation, StorageConfirmation};

mod certificate;
pub use certificate::ConfirmationCertificate;

use crate::wrapped_uint;

wrapped_uint! {
    /// Type for the intent type of signed messages.
    pub struct IntentType(pub u8) {
        /// Intent type for blob-certification messages.
        pub const BLOB_CERT_MSG: Self = Self(1);
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
#[derive(Debug, Serialize, Deserialize)]
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

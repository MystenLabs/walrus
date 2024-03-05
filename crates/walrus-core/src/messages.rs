//! Signed off-chain messages.

use serde::{Deserialize, Serialize};

mod storage_confirmation;
pub use storage_confirmation::{
    Confirmation,
    MessageHeader,
    SignedStorageConfirmation,
    StorageConfirmation,
};

macro_rules! wrapped_uint {
    (
        $(#[$outer:meta])*
        $vis:vis struct $name:ident($visinner:vis $uint:ty);
    ) => {
        $(#[$outer])*
        #[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
        #[repr(transparent)]
        $vis struct $name($visinner $uint);

        impl From<$name> for $uint {
            fn from(value: $name) -> Self {
                value.0
            }
        }

        impl From<$uint> for $name {
            fn from(value: $uint) -> Self {
                Self(value)
            }
        }
    };
}

wrapped_uint! {
    /// Type for the intent type of signed messages.
    pub struct IntentType(pub u8);
}

impl IntentType {
    /// Intent type for storage-certification messages.
    pub const STORAGE_CERT_MSG: Self = Self(2);
}

wrapped_uint! {
    /// Type for the intent version of signed messages.
    #[derive(Default)]
    pub struct IntentVersion(pub u8);
}

wrapped_uint! {
    /// Type used to identify the app associated with a signed message.
    pub struct IntentAppId(pub u8);
}

impl IntentAppId {
    /// Walrus App ID.
    pub const STORAGE: Self = Self(3);
}

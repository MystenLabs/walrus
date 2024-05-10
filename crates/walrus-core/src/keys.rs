// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Keys used with Walrus.

use alloc::{boxed::Box, string::String, sync::Arc, vec::Vec};
use core::str::FromStr;

use fastcrypto::{
    bls12381::min_pk::BLS12381KeyPair,
    encoding::{Base64, Encoding},
    traits::{AllowedRng, KeyPair, Signer, ToFromBytes},
};
use serde::{
    de::{Error, Unexpected},
    Deserialize,
    Serialize,
};
use serde_with::{
    base64::Base64 as SerdeWithBase64,
    ser::SerializeAsWrap,
    DeserializeAs,
    SerializeAs,
};

use crate::messages::{ProtocolMessage, SignedMessage};

/// Identifier for the type of public key being loaded from file.
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[repr(u8)]
pub enum SignatureScheme {
    /// Identifies a BLS12-381 public key
    BLS12381 = 0x04,
}

impl SignatureScheme {
    /// Returns the enum variant as a u8 value.
    pub const fn to_u8(&self) -> u8 {
        *self as u8
    }
}

/// Key pair used in Walrus protocol messages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolKeyPair(pub Arc<BLS12381KeyPair>);

impl ProtocolKeyPair {
    const KEY_LENGTH: usize = 32;
    const ENCODED_LENGTH: usize = ProtocolKeyPair::KEY_LENGTH + 1;

    /// Create a new `ProtocolKeyPair` from a [`BLS12381KeyPair`].
    pub fn new(keypair: BLS12381KeyPair) -> Self {
        Self(Arc::new(keypair))
    }

    /// Encodes the keypair as `flag || bytes` in base64.
    pub fn to_base64(&self) -> String {
        Base64::encode(Vec::from(self))
    }

    /// Generates a new key-pair using the specified random number generator.
    pub fn generate_with_rng(rng: &mut impl AllowedRng) -> Self {
        Self::new(BLS12381KeyPair::generate(rng))
    }

    /// Generates a new key-pair using thread-local randomness.
    pub fn generate() -> Self {
        Self::generate_with_rng(&mut rand::thread_rng())
    }

    /// Sign `message` and return the resulting [`SignedMessage`].
    pub fn sign_message<T>(&self, message: &T) -> SignedMessage<T>
    where
        T: ProtocolMessage,
    {
        let serialized_message =
            bcs::to_bytes(message).expect("bcs encoding a message should not fail");

        let signature = self.as_ref().sign(&serialized_message);
        SignedMessage::new_from_encoded(serialized_message, signature)
    }
}

impl From<BLS12381KeyPair> for ProtocolKeyPair {
    fn from(value: BLS12381KeyPair) -> Self {
        Self::new(value)
    }
}

impl TryFrom<Vec<u8>> for ProtocolKeyPair {
    type Error = bcs::Error;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        bcs::from_bytes(&value)
    }
}

impl From<&ProtocolKeyPair> for Vec<u8> {
    fn from(value: &ProtocolKeyPair) -> Self {
        bcs::to_bytes(value).expect("should never fail")
    }
}

impl AsRef<BLS12381KeyPair> for ProtocolKeyPair {
    fn as_ref(&self) -> &BLS12381KeyPair {
        &self.0
    }
}

impl Serialize for ProtocolKeyPair {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut bytes = [0u8; Self::ENCODED_LENGTH];
        bytes[0] = SignatureScheme::BLS12381.to_u8();
        bytes[1..].copy_from_slice(self.0.as_ref().as_bytes());

        <[serde_with::Same; Self::ENCODED_LENGTH]>::serialize_as(&bytes, serializer)
    }
}

impl<'de> Deserialize<'de> for ProtocolKeyPair {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let decoded_bytes: [u8; Self::ENCODED_LENGTH] =
            <[serde_with::Same; Self::ENCODED_LENGTH]>::deserialize_as(deserializer)?;

        if decoded_bytes[0] == SignatureScheme::BLS12381.to_u8() {
            let keypair =
                BLS12381KeyPair::from_bytes(&decoded_bytes[1..]).map_err(D::Error::custom)?;
            Ok(Self::new(keypair))
        } else {
            Err(D::Error::invalid_value(
                Unexpected::Unsigned(decoded_bytes[0].into()),
                &"a flag byte with value SignatureScheme::BLS12381",
            ))
        }
    }
}

/// Error returned when trying to parse a [`ProtocolKeyPair`] from a string.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct ProtocolKeyPairParseError(Box<dyn std::error::Error>);

impl FromStr for ProtocolKeyPair {
    type Err = ProtocolKeyPairParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let bytes = Base64::decode(s).map_err(|err| ProtocolKeyPairParseError(err.into()))?;
        bcs::from_bytes(&bytes).map_err(|err| ProtocolKeyPairParseError(err.into()))
    }
}

impl SerializeAs<ProtocolKeyPair> for SerdeWithBase64 {
    fn serialize_as<S>(source: &ProtocolKeyPair, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        SerializeAsWrap::<Vec<u8>, SerdeWithBase64>::new(&Vec::from(source)).serialize(serializer)
    }
}

#[cfg(test)]
mod tests {
    use fastcrypto::{encoding::Hex, traits::ToFromBytes};
    use serde_test::Token;
    use walrus_test_utils::{param_test, Result as TestResult};

    use super::*;
    use crate::test_utils;

    #[test]
    fn deserializes_valid() -> TestResult {
        let expected_keypair = test_utils::key_pair();
        let mut bytes = Vec::from(expected_keypair.as_ref().as_bytes());
        bytes.insert(0, SignatureScheme::BLS12381.to_u8());

        let deserialized_keypair: ProtocolKeyPair = bcs::from_bytes(&bytes)?;
        assert_eq!(deserialized_keypair, expected_keypair);

        Ok(())
    }

    param_test! {
        deserializes_fails_for_invalid_flag: [
            flag_0x03: (0x03),
            flag_0x05: (0x05),
        ]
    }
    fn deserializes_fails_for_invalid_flag(invalid_flag: u8) {
        let expected_keypair = test_utils::key_pair();
        let mut bytes = Vec::from(expected_keypair.as_ref().as_bytes());
        bytes.insert(0, invalid_flag);

        bcs::from_bytes::<ProtocolKeyPair>(&bytes).expect_err("must fail with invalid flag");
    }

    #[test]
    fn serializes_to_flag_byte_then_key() -> TestResult {
        let keypair = test_utils::key_pair();
        let serialized = bcs::to_bytes(&keypair)?;

        assert_eq!(
            serialized.len(),
            ProtocolKeyPair::ENCODED_LENGTH,
            "invalid encoded length"
        );
        assert_eq!(serialized[0], SignatureScheme::BLS12381.to_u8());
        assert_eq!(&serialized[1..], keypair.as_ref().as_bytes());

        Ok(())
    }

    #[test]
    fn parses_base64() -> TestResult {
        let base64_string = "BBlm7tRefoPuaKoVoxVtnUBBDCfy+BGPREM8B6oSkOEj";
        let expected_key_bytes =
            Hex::decode("1966eed45e7e83ee68aa15a3156d9d40410c27f2f8118f44433c07aa1290e123")?;

        let value = ProtocolKeyPair::from_str(base64_string)?;
        assert_eq!(value.as_ref().as_bytes(), expected_key_bytes);

        Ok(())
    }

    #[test]
    fn serialize_as_base64_uses_33_bytes() {
        let keypair = test_utils::key_pair();
        let base64_wrapper = SerializeAsWrap::<ProtocolKeyPair, SerdeWithBase64>::new(&keypair);

        serde_test::assert_ser_tokens(
            &base64_wrapper,
            &[Token::String(keypair.to_base64().leak())],
        );
    }
}

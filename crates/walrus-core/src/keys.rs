// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Keys used with Walrus.
use alloc::{boxed::Box, string::String, sync::Arc, vec::Vec};
use core::{fmt, str::FromStr};

use fastcrypto::{
    bls12381::min_pk::BLS12381KeyPair,
    encoding::{Base64, Encoding},
    secp256r1::Secp256r1KeyPair,
    traits::{AllowedRng, KeyPair, Signer, SigningKey, ToFromBytes},
};
use p256::pkcs8::{self, DecodePrivateKey, EncodePrivateKey, der::zeroize::Zeroizing};
use serde::{
    Deserialize,
    Serialize,
    de::{Error, SeqAccess, Unexpected, Visitor},
    ser::SerializeTuple,
};
use serde_with::{SerializeAs, base64::Base64 as SerdeWithBase64, ser::SerializeAsWrap};

use crate::messages::{ProtocolMessage, SignedMessage};

/// Key pair used in protocol messages.
pub type ProtocolKeyPair = TaggedKeyPair<BLS12381KeyPair>;

/// Key pair used to authenticate network communication.
pub type NetworkKeyPair = TaggedKeyPair<Secp256r1KeyPair>;

/// Identifier for the type of public key being loaded from file.
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[repr(u8)]
pub enum SignatureScheme {
    /// Identifies a NIST P-256 EC key.
    Secp256r1 = 0x02,
    /// Identifies a BLS12-381 key.
    BLS12381 = 0x04,
}

/// Marker trait for [`KeyPair`]s which are used in the system.
pub trait SupportedKeyPair: KeyPair + ToFromBytes {
    /// The [`SignatureScheme`] associated with the [`KeyPair`].
    const SCHEME: SignatureScheme;
}

impl SupportedKeyPair for BLS12381KeyPair {
    const SCHEME: SignatureScheme = SignatureScheme::BLS12381;
}

impl SupportedKeyPair for Secp256r1KeyPair {
    const SCHEME: SignatureScheme = SignatureScheme::Secp256r1;
}

impl SignatureScheme {
    /// Returns the enum variant as a u8 value.
    pub const fn to_u8(&self) -> u8 {
        *self as u8
    }
}

const ENCODING_BUFFER_LENGTH: usize = 33;

/// A [`KeyPair`] that is serialized, pre-pended with it's associated [`SignatureScheme`].
#[derive(Debug, PartialEq, Eq)]
pub struct TaggedKeyPair<T>(pub Arc<T>);

impl<T: SupportedKeyPair> TaggedKeyPair<T> {
    const ENCODED_LENGTH: usize = T::PrivKey::LENGTH + 1;

    /// The length of the inner private key.
    pub const LENGTH: usize = T::PrivKey::LENGTH;

    /// Create a scoped new key pair.
    pub fn new(keypair: T) -> Self {
        Self(Arc::new(keypair))
    }

    /// The public key associated with the key pair.
    pub fn public(&'_ self) -> &'_ T::PubKey {
        self.0.public()
    }

    /// Generates a new key-pair using the specified random number generator.
    pub fn generate_with_rng(rng: &mut impl AllowedRng) -> Self {
        Self::new(T::generate(rng))
    }

    /// Generates a new key-pair using thread-local randomness.
    pub fn generate() -> Self {
        Self::generate_with_rng(&mut rand::thread_rng())
    }

    /// Encodes the keypair as `flag || bytes` in base64.
    pub fn to_base64(&self) -> String {
        let mut buffer = [0u8; ENCODING_BUFFER_LENGTH];
        let key_bytes = &mut buffer[..Self::ENCODED_LENGTH];
        self.encode_to_buffer(key_bytes);
        Base64::encode(key_bytes)
    }

    fn encode_to_buffer(&self, mut buffer: &mut [u8]) {
        bcs::serialize_into(&mut buffer, self).expect("should never fail");
    }
}

impl<T> TaggedKeyPair<T>
where
    T: SupportedKeyPair,
    TaggedKeyPair<T>: EncodePrivateKey,
{
    /// Serializes the key-pair as a PKCS#8 PEM string.
    pub fn to_pem(&self) -> Zeroizing<String> {
        self.to_pkcs8_pem(pkcs8::LineEnding::default())
            .expect("supported keys that implement `EncodePrivateKey` must encode without failure")
    }
}

impl<T> Clone for TaggedKeyPair<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T: SupportedKeyPair> From<T> for TaggedKeyPair<T> {
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl<T: SupportedKeyPair> TryFrom<Vec<u8>> for TaggedKeyPair<T> {
    type Error = bcs::Error;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        bcs::from_bytes(&value)
    }
}

impl<T: SupportedKeyPair> From<&TaggedKeyPair<T>> for Vec<u8> {
    fn from(value: &TaggedKeyPair<T>) -> Self {
        bcs::to_bytes(value).expect("should never fail")
    }
}

impl<T: SupportedKeyPair> AsRef<T> for TaggedKeyPair<T> {
    fn as_ref(&self) -> &T {
        &self.0
    }
}

impl EncodePrivateKey for TaggedKeyPair<Secp256r1KeyPair> {
    fn to_pkcs8_der(&self) -> Result<pkcs8::SecretDocument, pkcs8::Error> {
        self.as_ref().secret.privkey.to_pkcs8_der()
    }
}

impl DecodePrivateKey for TaggedKeyPair<Secp256r1KeyPair> {
    fn from_pkcs8_der(bytes: &[u8]) -> Result<Self, pkcs8::Error> {
        let privkey = p256::ecdsa::SigningKey::from_pkcs8_der(bytes)?;
        let bytes = privkey.to_bytes();
        let private_key =
            Secp256r1KeyPair::from_bytes(&bytes).expect("key serialized above is always valid");
        Ok(TaggedKeyPair::new(private_key))
    }
}

/// Error returned when trying to parse a [`ProtocolKeyPair`] from a string.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct KeyPairParseError(Box<dyn core::error::Error + Send + Sync>);

impl<T: SupportedKeyPair> FromStr for TaggedKeyPair<T> {
    type Err = KeyPairParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let bytes = Base64::decode(s).map_err(|err| KeyPairParseError(err.into()))?;
        bcs::from_bytes(&bytes).map_err(|err| KeyPairParseError(err.into()))
    }
}

impl<T: SupportedKeyPair> Serialize for TaggedKeyPair<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut tuple = serializer.serialize_tuple(Self::ENCODED_LENGTH)?;

        tuple.serialize_element(&T::SCHEME.to_u8())?;
        for byte in self.0.as_bytes() {
            tuple.serialize_element(byte)?;
        }
        tuple.end()
    }
}

impl<T: SupportedKeyPair> SerializeAs<TaggedKeyPair<T>> for SerdeWithBase64 {
    fn serialize_as<S>(source: &TaggedKeyPair<T>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        SerializeAsWrap::<Vec<u8>, SerdeWithBase64>::new(&Vec::from(source)).serialize(serializer)
    }
}

/// Serde serialization visitor for writing an array of known length into a pre-allocated buffer.
struct BinaryBufferVisitor<const N: usize> {
    length: usize,
}

impl<const N: usize> BinaryBufferVisitor<N> {
    fn new(length: usize) -> Self {
        Self { length }
    }
}

impl<'de, const N: usize> Visitor<'de> for BinaryBufferVisitor<N> {
    type Value = [u8; N];

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "an array of length {}", self.length)
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut result = [0u8; N];
        for element in &mut result[..self.length] {
            match seq.next_element()? {
                Some(e) => *element = e,
                None => return Err(Error::invalid_length(self.length, &self)),
            }
        }
        Ok(result)
    }
}

impl<'de, T: SupportedKeyPair> Deserialize<'de> for TaggedKeyPair<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let decoded_bytes: [u8; ENCODING_BUFFER_LENGTH] = deserializer.deserialize_tuple(
            Self::ENCODED_LENGTH,
            BinaryBufferVisitor::new(Self::ENCODED_LENGTH),
        )?;
        let decoded_bytes = &decoded_bytes[..Self::ENCODED_LENGTH];

        if decoded_bytes[0] == T::SCHEME.to_u8() {
            let keypair = T::from_bytes(&decoded_bytes[1..]).map_err(D::Error::custom)?;
            Ok(Self::new(keypair))
        } else {
            Err(D::Error::invalid_value(
                Unexpected::Unsigned(decoded_bytes[0].into()),
                &"a correct flag byte for the key type",
            ))
        }
    }
}

impl ProtocolKeyPair {
    /// Sign `message` and return the resulting [`SignedMessage`].
    pub fn sign_message<T, I>(&self, message: &T) -> SignedMessage<T>
    where
        T: AsRef<ProtocolMessage<I>> + Serialize,
    {
        let serialized_message =
            bcs::to_bytes(message).expect("bcs encoding a message should not fail");

        let signature = self.as_ref().sign(&serialized_message);
        SignedMessage::new_from_encoded(serialized_message, signature)
    }
}

#[cfg(test)]
mod tests {
    use fastcrypto::{encoding::Hex, traits::ToFromBytes};
    use serde_test::Token;
    use walrus_test_utils::{Result as TestResult, param_test};

    use super::*;
    use crate::test_utils;

    #[test]
    fn deserializes_valid() -> TestResult {
        let expected_keypair = test_utils::protocol_key_pair();
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
        let expected_keypair = test_utils::protocol_key_pair();
        let mut bytes = Vec::from(expected_keypair.as_ref().as_bytes());
        bytes.insert(0, invalid_flag);

        bcs::from_bytes::<ProtocolKeyPair>(&bytes).expect_err("must fail with invalid flag");
    }

    #[test]
    fn serializes_to_flag_byte_then_key() -> TestResult {
        let keypair = test_utils::protocol_key_pair();
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
        let keypair = test_utils::protocol_key_pair();
        let base64_wrapper = SerializeAsWrap::<ProtocolKeyPair, SerdeWithBase64>::new(&keypair);

        serde_test::assert_ser_tokens(
            &base64_wrapper,
            &[Token::String(keypair.to_base64().leak())],
        );
    }
}

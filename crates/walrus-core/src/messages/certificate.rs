// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use alloc::vec::Vec;
use core::marker::PhantomData;

use fastcrypto::{
    bls12381::min_pk::BLS12381AggregateSignature, error::FastCryptoError,
    traits::AggregateAuthenticator,
};
use serde::{Deserialize, Serialize};

use super::{Confirmation, InvalidBlobIdMsg, SignedMessage};

/// A certificate from storage nodes over a [`super::storage_confirmation::Confirmation`].
pub type ConfirmationCertificate = ProtocolMessageCertificate<Confirmation>;

/// A certificate from storage nodes over a [`super::invalid_blob_id::InvalidBlobIdMsg`].
pub type InvalidBlobCertificate = ProtocolMessageCertificate<InvalidBlobIdMsg>;

/// A certificate from storage nodes over a protocol message.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct ProtocolMessageCertificate<T> {
    /// The indices of the signing nodes.
    pub signers: Vec<u16>,
    /// The BCS-encoded message.
    pub serialized_message: Vec<u8>,
    /// The aggregate signature over the BCS encoded confirmation.
    pub signature: BLS12381AggregateSignature,
    #[serde(skip)]
    message_type: PhantomData<T>,
}

impl<T> ProtocolMessageCertificate<T> {
    /// Constructs a new certificate from a list of signer indices, a serialized message
    /// and an aggregate signature over that message.
    pub fn new(
        signers: Vec<u16>,
        serialized_message: Vec<u8>,
        signature: BLS12381AggregateSignature,
    ) -> Self {
        Self {
            signers,
            serialized_message,
            signature,
            message_type: PhantomData,
        }
    }

    /// Constructs a new certificate from a collection of signed messages and a list of signer
    /// indices.
    pub fn from_signed_messages_and_indices<I>(
        signed_messages: I,
        signer_indices: Vec<u16>,
    ) -> Result<Self, CertificateError>
    where
        I: IntoIterator<Item = SignedMessage<T>>,
    {
        let mut signed_messages = signed_messages.into_iter();
        let Some(first) = signed_messages.next() else {
            return Err(CertificateError::MessageMismatch);
        };
        let signatures: Vec<_> = core::iter::once(Ok(first.signature))
            .chain(signed_messages.map(|message| {
                (first.serialized_message == message.serialized_message)
                    .then_some(message.signature)
                    .ok_or(CertificateError::MessageMismatch)
            }))
            .collect::<Result<_, _>>()?;

        Ok(Self {
            signers: signer_indices,
            serialized_message: first.serialized_message,
            signature: BLS12381AggregateSignature::aggregate(&signatures)?,
            message_type: PhantomData,
        })
    }
}

/// Error returned a certificate cannot be created from the provided signed messages.
#[derive(Debug, thiserror::Error)]
pub enum CertificateError {
    /// The signature aggregation failed.
    #[error(transparent)]
    SignatureAggregation(#[from] FastCryptoError),
    /// The message content is not as expected.
    #[error("the signed messages differ")]
    MessageMismatch,
}

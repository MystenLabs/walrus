// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::str::FromStr;

use base64::{DecodeError, Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error};
use serde_with::{DisplayFromStr, serde_as};
use sui_types::digests::TransactionDigest;
use utoipa::IntoParams;
use walrus_sdk::{ObjectID, core::BlobId};

use crate::client::AuthPackage;

/// The query parameters for the fanout proxy.
#[serde_as]
#[derive(Debug, Deserialize, Serialize, IntoParams)]
#[serde(deny_unknown_fields)]
pub(crate) struct Params {
    /// The blob ID of the blob to be sent to the storage nodes.
    #[serde_as(as = "DisplayFromStr")]
    pub blob_id: BlobId,
    /// The bytes (encoded as Base64URL) of the transaction that registers the blob and sends
    /// the tip to the proxy.
    #[param(value_type = String)]
    pub tx_id: TransactionDigest,
    /// The bytes (encoded as Base64URL) of the signature over the transaction.
    pub auth_package: AuthPackage,
    /// The blob object_id.
    // TODO: pull this directly from the ptb.
    #[param(value_type = String)]
    pub object_id: ObjectID,
}

/// A representation of a byte vector, URL-encoded without padding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct B64UrlEncodedBytes(Vec<u8>);

impl B64UrlEncodedBytes {
    /// Creates a new B64 wrapper for bytes.
    #[cfg(feature = "test-client")]
    pub(crate) fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    /// Returns a reference to the bytes.
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.0
    }

    /// Returns the bytes, consuming `self`.
    #[allow(unused)]
    pub(crate) fn into_bytes(self) -> Vec<u8> {
        self.0
    }
}

impl std::fmt::Display for B64UrlEncodedBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", URL_SAFE_NO_PAD.encode(&self.0))
    }
}

impl FromStr for B64UrlEncodedBytes {
    type Err = DecodeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(B64UrlEncodedBytes(URL_SAFE_NO_PAD.decode(s)?))
    }
}

impl Serialize for B64UrlEncodedBytes {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for B64UrlEncodedBytes {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        B64UrlEncodedBytes::from_str(&s).map_err(D::Error::custom)
    }
}

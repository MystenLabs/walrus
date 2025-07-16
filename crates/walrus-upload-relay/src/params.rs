// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_with::{DisplayFromStr, serde_as};
use sui_types::digests::TransactionDigest;
use utoipa::IntoParams;
use walrus_sdk::{
    ObjectID,
    core::{BlobId, EncodingType},
    sui::ObjectIdSchema,
};

use crate::error::WalrusUploadRelayError;

pub(crate) const DIGEST_LEN: usize = 32;

/// Schema for the `TransactionDigest` type.
#[derive(Debug, utoipa::ToSchema)]
#[schema(
    as = TransactionDigest,
    value_type = String,
    title = "Sui transaction digest",
    description = "Sui transaction digest (or transaction ID) as a Base58 string",
    examples("EmcFpdozbobqH61w76T4UehhC4UGaAv32uZpv6c4CNyg"),
)]
pub(crate) struct TransactionDigestSchema(());

/// Schema for 32-byte digests.
#[derive(Debug, utoipa::ToSchema)]
#[schema(
    as = BlobDigest,
    value_type = String,
    format = Byte,
    title = "Digest",
    description = "A 32-byte digest (or hash) as a URL-encoded (without padding) Base64 string",
    examples("rw8xIuqxwMpdOcF_3jOprsD9TtPWfXK97tT_lWr1teQ"),
)]
pub(crate) struct DigestSchema(());

/// The query parameters for the Walrus Upload Relay.
#[serde_as]
#[derive(Debug, Deserialize, Serialize, IntoParams)]
#[into_params(parameter_in = Query, style = Form)]
#[serde(deny_unknown_fields)]
pub(crate) struct Params {
    /// The blob ID of the blob to be sent to the storage nodes.
    #[serde_as(as = "DisplayFromStr")]
    pub blob_id: BlobId,
    /// The Base58 transaction ID of the transaction that sends the tip to the proxy.
    ///
    /// This field is _required_ in the case where the proxy requires a tip.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[param(value_type = Option<TransactionDigestSchema>)]
    pub tx_id: Option<TransactionDigest>,
    /// The nonce, the preimage of the hash added to the transaction inputs.
    ///
    /// This field is _required_ in the case where the proxy requires a tip.
    #[serde(
        default,
        with = "b64_option_digest",
        skip_serializing_if = "Option::is_none"
    )]
    #[param(value_type = Option<DigestSchema>)]
    pub nonce: Option<[u8; DIGEST_LEN]>,
    /// The object ID of the deletable blob to be stored.
    ///
    /// If the blob is to be stored as a permanent one, this parameter should not be specified.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[param(value_type = Option<ObjectIdSchema>)]
    pub deletable_blob_object: Option<ObjectID>,
    /// The encoding type for the blob.
    ///
    /// If omitted, RS2 is used by default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encoding_type: Option<EncodingType>,
}

impl Params {
    /// Checks that the `Params` contain the `tx_id` and the `nonce`, and returns an instance of
    /// `PaidParams`.
    pub(crate) fn to_paid_params(&self) -> Result<PaidTipParams, WalrusUploadRelayError> {
        let Params {
            tx_id,
            nonce,
            encoding_type,
            ..
        } = *self;

        Ok(PaidTipParams {
            tx_id: tx_id.ok_or(WalrusUploadRelayError::MissingTxIdOrNonce)?,
            nonce: nonce.ok_or(WalrusUploadRelayError::MissingTxIdOrNonce)?,
            encoding_type,
        })
    }
}

/// The subset of query parameters of the Walrus Upload Relay, necessary to check the tip.
///
/// Compared to `Params`, the `tx_id` and the `nonce` are not optional, and `blob_id` and
/// `deletable_blob_object` are not necessary.
#[derive(Debug, Clone)]
pub(crate) struct PaidTipParams {
    pub tx_id: TransactionDigest,
    pub nonce: [u8; DIGEST_LEN],
    pub encoding_type: Option<EncodingType>,
}

/// The tip authentication structure.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct AuthPackage {
    /// The SHA256 hash of the blob data.
    pub blob_digest: [u8; DIGEST_LEN],
    /// A nonce.
    pub nonce: [u8; DIGEST_LEN],
    /// The length in bytes of the unencoded blob.
    pub unencoded_length: u64,
}

/// The tip authentication structure, with hashed nonce.
///
/// Helper struct to serialize/deserialize with the hashed nonce.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct HashedAuthPackage {
    /// The SHA256 hash of the blob data.
    pub blob_digest: [u8; DIGEST_LEN],
    /// The digest of a nonce.
    pub nonce_digest: [u8; DIGEST_LEN],
    /// The length in bytes of the unencoded blob.
    pub unencoded_length: u64,
}

pub(crate) mod b64_option_digest {
    use anyhow::Result;
    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
    use serde::{Deserialize, Deserializer, Serializer, de::Error};

    use super::DIGEST_LEN;

    pub(crate) fn serialize<S: Serializer>(
        bytes: &Option<[u8; DIGEST_LEN]>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        if let Some(bytes) = bytes {
            let b64 = URL_SAFE_NO_PAD.encode(bytes);
            serializer.serialize_str(&b64)
        } else {
            serializer.serialize_none()
        }
    }

    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<Option<[u8; DIGEST_LEN]>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt: Option<String> = Option::deserialize(deserializer)?;
        if let Some(b64) = opt {
            let data = URL_SAFE_NO_PAD.decode(b64).map_err(D::Error::custom)?;
            let digest = data
                .try_into()
                .map_err(|_| D::Error::custom("failed to fit deserialized vector into array"))?;
            Ok(Some(digest))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {

    use std::str::FromStr;

    use axum::{extract::Query, http::Uri};
    use reqwest::Url;
    use sui_types::digests::TransactionDigest;
    use walrus_sdk::{ObjectID, core::BlobId};

    use crate::{
        params::{DIGEST_LEN, Params},
        shared::blob_upload_relay_url,
    };

    #[test]
    fn test_upload_relay_parse_query() {
        let blob_id =
            BlobId::from_str("efshm0WcBczCA_GVtB0itHbbSXLT5VMeQDl0A1b2_0Y").expect("valid blob id");
        let tx_id = TransactionDigest::new([13; DIGEST_LEN]);
        let nonce = [23; DIGEST_LEN];
        let params = Params {
            blob_id,
            nonce: Some(nonce),
            deletable_blob_object: Some(ObjectID::from_single_byte(42)),
            tx_id: Some(tx_id),
            encoding_type: None,
        };

        let url =
            blob_upload_relay_url(&Url::parse("http://localhost").expect("valid url"), &params)
                .expect("valid parameters");

        let uri = Uri::from_str(url.as_ref()).expect("valid conversion");
        let result = Query::<Params>::try_from_uri(&uri).expect("parsing the uri works");

        assert_eq!(params.blob_id, result.blob_id);
        assert_eq!(params.nonce, result.nonce);
        assert_eq!(params.tx_id, result.tx_id);
        assert_eq!(params.deletable_blob_object, result.deletable_blob_object);
    }
}

// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use fastcrypto::hash::Digest;
use serde::{Deserialize, Serialize};
use serde_with::{DisplayFromStr, serde_as};
use sui_types::digests::TransactionDigest;
use utoipa::IntoParams;
use walrus_sdk::{
    ObjectID,
    core::{BlobId, EncodingType},
    sui::ObjectIdSchema,
};

use crate::utils::compute_blob_digest_sha256;

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

/// Schema for the blob digest.
#[derive(Debug, utoipa::ToSchema)]
#[schema(
    as = BlobDigest,
    value_type = String,
    format = Byte,
    title = "Blob digest",
    description = "Blob digest (or hash) as a URL-encoded (without padding) Base64 string",
    examples("rw8xIuqxwMpdOcF_3jOprsD9TtPWfXK97tT_lWr1teQ"),
)]
pub(crate) struct BlobDigestSchema(());

/// The query parameters for the fanout proxy.
#[serde_as]
#[derive(Debug, Deserialize, Serialize, IntoParams)]
#[into_params(parameter_in = Query, style = Form)]
#[serde(deny_unknown_fields)]
pub(crate) struct Params {
    /// The blob ID of the blob to be sent to the storage nodes.
    #[serde_as(as = "DisplayFromStr")]
    pub blob_id: BlobId,
    /// The Base58 transaction ID of the transaction that sends the tip to the proxy.
    #[param(value_type = TransactionDigestSchema)]
    pub tx_id: TransactionDigest,
    /// The object ID of the deletable blob to be stored.
    ///
    /// If the blob is to be stored as a permanent one, this parameter should not be specified.
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[param(value_type = Option<ObjectIdSchema>)]
    pub deletable_blob_object: Option<ObjectID>,
    /// The encoding type for the blob.
    ///
    /// If omitted, RS2 is used by default.
    #[serde(default)]
    pub encoding_type: Option<EncodingType>,
}

impl Params {
    pub(crate) fn encoding_type_or_default(&self) -> EncodingType {
        self.encoding_type
            .unwrap_or(walrus_sdk::core::DEFAULT_ENCODING)
    }
}

/// The authentication structure for a blob store request.
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema, IntoParams)]
#[into_params(parameter_in = Query, style = Form)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub(crate) struct AuthPackage {
    /// The SHA256 hash of the blob data.
    #[serde(with = "b64urlencode_bytes")]
    #[param(value_type = BlobDigestSchema)]
    pub blob_digest: [u8; DIGEST_LEN],
}

impl AuthPackage {
    /// Creates an authentication package for the blob.
    pub(crate) fn new(blob: &[u8]) -> Result<Self> {
        let blob_digest = compute_blob_digest_sha256(blob);

        Ok(Self {
            blob_digest: blob_digest.into(),
        })
    }

    /// Returns the digest of the authentication package, that is to be placed in the first input of
    /// the transaction.
    ///
    /// Note that this, for the moment, is equivalent to the `blob_digest`.
    pub(crate) fn to_digest(&self) -> Result<Digest<DIGEST_LEN>> {
        Ok(Digest::new(self.blob_digest))
    }
}

pub(crate) mod b64urlencode_bytes {
    use anyhow::Result;
    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
    use serde::{Deserialize, Deserializer, Serializer, de::Error};

    use super::DIGEST_LEN;

    pub(crate) fn serialize<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        let b64 = URL_SAFE_NO_PAD.encode(bytes);
        serializer.serialize_str(&b64)
    }

    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<[u8; DIGEST_LEN], D::Error>
    where
        D: Deserializer<'de>,
    {
        let b64 = String::deserialize(deserializer)?;
        let data = URL_SAFE_NO_PAD.decode(b64).map_err(D::Error::custom)?;
        data.try_into()
            .map_err(|_| D::Error::custom("failed to fit deserialized vector into array"))
    }
}

#[cfg(test)]
mod tests {

    use std::str::FromStr;

    use axum::{extract::Query, http::Uri};
    use reqwest::Url;
    use sui_types::digests::TransactionDigest;
    use walrus_sdk::{ObjectID, core::BlobId};

    use crate::{client::fan_out_blob_url, params::Params};

    #[test]
    fn test_fanout_parse_query() {
        let blob_id =
            BlobId::from_str("efshm0WcBczCA_GVtB0itHbbSXLT5VMeQDl0A1b2_0Y").expect("valid blob id");
        let tx_id = TransactionDigest::new([13; 32]);
        let params = Params {
            blob_id,
            deletable_blob_object: Some(ObjectID::from_single_byte(42)),
            tx_id,
            encoding_type: None,
        };

        let url = fan_out_blob_url(&Url::parse("http://localhost").expect("valid url"), &params)
            .expect("valid parameters");

        let uri = Uri::from_str(url.as_ref()).expect("valid conversion");
        dbg!(&uri);
        let result = Query::<Params>::try_from_uri(&uri).expect("parsing the uri works");

        assert_eq!(params.blob_id, result.blob_id);
        assert_eq!(params.tx_id, result.tx_id);
        assert_eq!(params.deletable_blob_object, result.deletable_blob_object);
    }
}

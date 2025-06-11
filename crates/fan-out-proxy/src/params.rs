// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use fastcrypto::hash::Digest;
use serde::{Deserialize, Serialize};
use serde_with::{DisplayFromStr, serde_as};
use sui_types::digests::TransactionDigest;
use utoipa::IntoParams;
use walrus_sdk::{ObjectID, core::BlobId};

use crate::utils::compute_blob_digest_sha256;

pub(crate) const DIGEST_LEN: usize = 32;

/// The query parameters for the fanout proxy.
#[serde_as]
#[derive(Debug, Deserialize, Serialize, IntoParams)]
#[into_params(parameter_in = Query, style = Form)]
#[serde(deny_unknown_fields)]
pub(crate) struct Params {
    /// The blob ID of the blob to be sent to the storage nodes.
    #[serde_as(as = "DisplayFromStr")]
    pub blob_id: BlobId,
    /// The object ID of the deletable blob to be stored.
    ///
    /// If the blob is to be stored as a permanent one, this parameter should not be specified.
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[param(value_type = String)]
    pub deletable_blob_object: Option<ObjectID>,
    /// The bytes (encoded as Base64URL) of the transaction that registers the blob and sends
    /// the tip to the proxy.
    #[param(value_type = String)]
    pub tx_id: TransactionDigest,
    /// The bytes (encoded as Base64URL) of the [`AuthPackage`].
    #[serde(flatten)]
    #[param(inline)]
    pub auth_package: AuthPackage,
}

/// The authentication structure for a blob store request.
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema, IntoParams)]
#[into_params(parameter_in = Query, style = Form)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub(crate) struct AuthPackage {
    /// The SHA256 hash of the blob data.
    #[serde(with = "b64urlencode_bytes")]
    #[param(value_type = String)]
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
        Ok(Digest::new(self.blob_digest.clone()))
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

    use crate::{
        client::fan_out_blob_url,
        params::{AuthPackage, Params},
    };

    #[test]
    fn test_fanout_parse_query() {
        let blob_id =
            BlobId::from_str("efshm0WcBczCA_GVtB0itHbbSXLT5VMeQDl0A1b2_0Y").expect("valid blob id");
        let tx_id = TransactionDigest::new([13; 32]);
        let auth_package = AuthPackage::new(&[1, 2, 3]).expect("this is a valid package");
        let params = Params {
            blob_id,
            deletable_blob_object: Some(ObjectID::from_single_byte(42)),
            tx_id,
            auth_package,
        };

        let url = fan_out_blob_url(&Url::parse("http://localhost").expect("valid url"), &params)
            .expect("valid parameters");

        let uri = Uri::from_str(url.as_ref()).expect("valid conversion");
        dbg!(&uri);
        let result = Query::<Params>::try_from_uri(&uri).expect("parsing the uri works");

        assert_eq!(params.blob_id, result.blob_id);
        assert_eq!(params.tx_id, result.tx_id);
        assert_eq!(
            params.auth_package.blob_digest,
            result.auth_package.blob_digest
        );
        assert_eq!(params.deletable_blob_object, result.deletable_blob_object);
    }
}

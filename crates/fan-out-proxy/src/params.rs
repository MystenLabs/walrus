// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use fastcrypto::hash::{Digest, HashFunction, Sha256};
use rand::{Rng, SeedableRng, rngs::StdRng};
use serde::{Deserialize, Serialize};
use serde_with::{DisplayFromStr, serde_as};
use sui_types::digests::TransactionDigest;
use utoipa::IntoParams;
use walrus_sdk::core::BlobId;

use crate::utils::compute_blob_digest_sha256;

const DIGEST_LEN: usize = 32;
/// The query parameters for the fanout proxy.
#[serde_as]
#[derive(Debug, Deserialize, Serialize, IntoParams)]
#[into_params(parameter_in = Query, style = Form)]
#[serde(deny_unknown_fields)]
pub(crate) struct Params {
    /// The blob ID of the blob to be sent to the storage nodes.
    #[serde_as(as = "DisplayFromStr")]
    pub blob_id: BlobId,
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
    /// Random bytes known only to the client. Shared with fan-out proxy to prevent malicious replay
    /// attacks by other users.
    #[serde(with = "b64urlencode_bytes")]
    #[param(value_type = String)]
    pub nonce: [u8; DIGEST_LEN],
    /// The timestamp just prior to blob registration as known to the client user. This is used to
    /// expire fan-out requests after some time.
    #[serde_as(as = "DisplayFromStr")]
    pub timestamp_ms: u64,
}

impl AuthPackage {
    /// Creates an authentication package for the blob.
    pub(crate) fn new(blob: &[u8]) -> Result<Self> {
        let std_rng = StdRng::from_rng(&mut rand::thread_rng())?;
        let mut rng = std_rng;
        let nonce: [u8; 32] = rng.r#gen();

        let blob_digest = compute_blob_digest_sha256(blob);
        let timestamp_ms = u64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("the clocks are set in the present")
                .as_millis(),
        )?;

        Ok(Self {
            blob_digest: blob_digest.into(),
            nonce,
            timestamp_ms,
        })
    }

    /// Returns the digest (SHA256) for the authentication package.
    pub(crate) fn to_digest(&self) -> Result<Digest<DIGEST_LEN>> {
        let mut auth_hash = Sha256::new();
        auth_hash.update(self.blob_digest);
        auth_hash.update(self.nonce);
        auth_hash.update(bcs::to_bytes(&self.timestamp_ms)?);
        Ok(auth_hash.finalize())
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
    use walrus_sdk::core::BlobId;

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
            tx_id,
            auth_package,
        };

        let url = fan_out_blob_url(&Url::parse("http://localhost").expect("valid url"), &params)
            .expect("valid parameters");

        let uri = Uri::from_str(url.as_ref()).expect("valid conversion");
        let result = Query::<Params>::try_from_uri(&uri).expect("parsing the uri works");

        assert_eq!(params.blob_id, result.blob_id);
        assert_eq!(params.tx_id, result.tx_id);
        assert_eq!(
            params.auth_package.blob_digest,
            result.auth_package.blob_digest
        );
        assert_eq!(params.auth_package.nonce, result.auth_package.nonce);
        assert_eq!(
            params.auth_package.timestamp_ms,
            result.auth_package.timestamp_ms
        );
    }
}

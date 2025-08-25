// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Walrus Upload Relay crate.

pub mod controller;
mod error;
mod metrics;
mod tip;
mod utils;

pub use controller::{DEFAULT_SERVER_ADDRESS, UploadRelayHandle, start_upload_relay};

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use axum::{extract::Query, http::Uri};
    use reqwest::Url;
    use sui_types::digests::TransactionDigest;
    use walrus_sdk::{
        ObjectID,
        core::BlobId,
        upload_relay::{
            blob_upload_relay_url,
            params::{DIGEST_LEN, Params},
        },
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

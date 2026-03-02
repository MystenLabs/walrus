// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Upload Relay shared types and constants.

pub mod params;
pub mod tip_config;

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use reqwest::Url;
use serde::{Deserialize, Serialize};

use crate::{
    core::{BlobId, messages::ConfirmationCertificate},
    node_client::upload_relay_client::UploadRelayClientError,
};

/// The route to upload blobs using the relay.
pub const BLOB_UPLOAD_RELAY_ROUTE: &str = "/v1/blob-upload-relay";
/// The route to fetch the update-relay's tip configuration.
pub const TIP_CONFIG_ROUTE: &str = "/v1/tip-config";
/// The route for the update-relay's OpenAPI docs.
pub const API_DOCS: &str = "/v1/api";

/// The response of the Walrus Upload Relay, containing the blob ID and the corresponding
/// certificate.
#[derive(Serialize, Debug, Deserialize)]
pub struct ResponseType {
    /// The blob ID of the blob that was uploaded.
    pub blob_id: BlobId,
    /// The confirmation certificate for the uploaded blob.
    pub confirmation_certificate: ConfirmationCertificate,
}

/// Constructs the URL for the Walrus Upload Relay API with the given parameters.
pub fn blob_upload_relay_url(
    server_url: &Url,
    params: &params::Params,
) -> Result<Url, UploadRelayClientError> {
    let mut url = server_url
        .join(BLOB_UPLOAD_RELAY_ROUTE)
        .map_err(UploadRelayClientError::UrlEndocodingFailed)?;

    // Scope the query_pairs mut variable.
    {
        let mut query_pairs = url.query_pairs_mut();
        query_pairs.append_pair("blob_id", &params.blob_id.to_string());

        if let Some(object_id) = params.deletable_blob_object {
            query_pairs.append_pair("deletable_blob_object", &object_id.to_string());
        };

        if let Some(tx_id) = params.tx_id {
            query_pairs.append_pair("tx_id", &tx_id.to_string());
        }

        if let Some(nonce) = params.nonce {
            query_pairs.append_pair("nonce", &URL_SAFE_NO_PAD.encode(nonce));
        }
    }

    Ok(url)
}

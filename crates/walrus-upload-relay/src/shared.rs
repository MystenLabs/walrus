#[cfg(test)]
use anyhow::Result;
#[cfg(test)]
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
#[cfg(test)]
use reqwest::Url;
use serde::{Deserialize, Serialize};
use walrus_sdk::core::{BlobId, messages::ConfirmationCertificate};

#[cfg(test)]
use crate::params::Params;

pub(crate) const BLOB_UPLOAD_RELAY_ROUTE: &str = "/v1/blob-upload-relay";
pub(crate) const TIP_CONFIG_ROUTE: &str = "/v1/tip-config";
pub(crate) const API_DOCS: &str = "/v1/api";

/// The response of the Walrus Upload Relay, containing the blob ID and the corresponding
/// certificate.
#[derive(Serialize, Debug, Deserialize)]
pub(crate) struct ResponseType {
    pub blob_id: BlobId,
    pub confirmation_certificate: ConfirmationCertificate,
}

#[cfg(test)]
pub(crate) fn blob_upload_relay_url(server_url: &Url, params: &Params) -> Result<Url> {
    let mut url = server_url.join(BLOB_UPLOAD_RELAY_ROUTE)?;
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
    drop(query_pairs);

    // TODO: add encoding type serialization.

    Ok(url)
}

// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};
use serde_with::{DisplayFromStr, serde_as};
use sui_types::digests::TransactionDigest;
use utoipa::IntoParams;
use walrus_sdk::core::BlobId;

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
    /// The bytes (encoded as Base64URL) of the [`AuthPackage`].
    pub auth_package: AuthPackage,
}

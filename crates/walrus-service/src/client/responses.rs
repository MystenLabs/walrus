// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    num::NonZeroU64,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use serde_with::{base64::Base64, serde_as, DisplayFromStr};
use sui_types::event::EventID;
use utoipa::ToSchema;
use walrus_core::{metadata::VerifiedBlobMetadataWithId, BlobId, Epoch};
use walrus_sdk::api::BlobStatus;
use walrus_sui::types::Blob;

/// Result when attempting to store a blob.
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum BlobStoreResult {
    /// The blob already exists within Walrus, was certified, and is stored for at least the
    /// intended duration.
    AlreadyCertified {
        /// The blob ID.
        #[serde_as(as = "DisplayFromStr")]
        blob_id: BlobId,
        /// The event where the blob was certified.
        event: EventID,
        /// The epoch until which the blob is stored (exclusive).
        #[schema(value_type = u64)]
        end_epoch: Epoch,
    },
    /// The blob was newly created; this contains the newly created Sui object associated with the
    /// blob.
    NewlyCreated {
        /// The Sui blob object that holds the newly created blob.
        blob_object: Blob,
        /// The encoded size, including metadata.
        encoded_size: u64,
        /// The storage cost, excluding gas.
        cost: u64,
    },
    /// The blob is known to Walrus but was marked as invalid.
    ///
    /// This indicates a bug within the client, the storage nodes, or more than a third malicious
    /// storage nodes.
    MarkedInvalid {
        /// The blob ID.
        #[serde_as(as = "DisplayFromStr")]
        blob_id: BlobId,
        /// The event where the blob was marked as invalid.
        event: EventID,
    },
}

impl BlobStoreResult {
    /// Returns the blob ID.
    pub fn blob_id(&self) -> &BlobId {
        match self {
            Self::AlreadyCertified { blob_id, .. } => blob_id,
            Self::MarkedInvalid { blob_id, .. } => blob_id,
            Self::NewlyCreated {
                blob_object: Blob { blob_id, .. },
                ..
            } => blob_id,
        }
    }
}

/// The output of the `read` command.
#[serde_as]
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadOutput {
    #[serde(skip_serializing_if = "std::option::Option::is_none")]
    pub(crate) out: Option<PathBuf>,
    #[serde_as(as = "DisplayFromStr")]
    pub(crate) blob_id: BlobId,
    // When serializing to JSON, the blob is encoded as Base64 string.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde_as(as = "Base64")]
    pub(crate) blob: Vec<u8>,
}

impl ReadOutput {
    /// Creates a new [`ReadOutput`] object.
    pub fn new(out: Option<PathBuf>, blob_id: BlobId, orig_blob: Vec<u8>) -> Self {
        // Avoid serializing the blob if there is an output file.
        let blob = if out.is_some() { vec![] } else { orig_blob };
        Self { out, blob_id, blob }
    }
}

/// The output of the `blob-id` command.
#[serde_as]
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BlobIdOutput {
    #[serde_as(as = "DisplayFromStr")]
    pub(crate) blob_id: BlobId,
    pub(crate) file: PathBuf,
    pub(crate) unencoded_length: NonZeroU64,
}

impl BlobIdOutput {
    /// Creates a new [`BlobIdOutput`] object.
    pub fn new(file: &Path, metadata: &VerifiedBlobMetadataWithId) -> Self {
        Self {
            blob_id: *metadata.blob_id(),
            file: file.to_owned(),
            unencoded_length: metadata.metadata().unencoded_length,
        }
    }
}

/// The output of the `store --dry-run` command.
#[serde_as]
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DryRunOutput {
    #[serde_as(as = "DisplayFromStr")]
    /// The blob ID.
    pub blob_id: BlobId,
    /// The size of the unencoded blob (in bytes).
    pub unencoded_size: u64,
    /// The size of the encoded blob (in bytes).
    pub encoded_size: u64,
    /// The storage cost (in MIST).
    pub storage_cost: u64,
}

/// The output of the `blob-status` command.
#[serde_as]
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BlobStatusOutput {
    /// The blob ID.
    #[serde_as(as = "DisplayFromStr")]
    pub blob_id: BlobId,
    /// The file from which the blob was read.
    #[serde(skip_serializing_if = "std::option::Option::is_none")]
    pub file: Option<PathBuf>,
    /// The blob's status.
    pub status: BlobStatus,
}

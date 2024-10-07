// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Structures of client results returned by the daemon or through the JSON API.

use std::{
    fmt::Display,
    num::NonZeroU16,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use serde_with::{base64::Base64, serde_as, DisplayFromStr};
use sui_types::{base_types::ObjectID, event::EventID};
use utoipa::ToSchema;
use walrus_core::{
    bft,
    encoding::{
        encoded_blob_length_for_n_shards,
        encoded_slivers_length_for_n_shards,
        max_blob_size_for_n_shards,
        max_sliver_size_for_n_secondary,
        metadata_length_for_n_shards,
        source_symbols_for_n_shards,
    },
    metadata::VerifiedBlobMetadataWithId,
    BlobId,
    Epoch,
    NetworkPublicKey,
    PublicKey,
};
use walrus_sdk::api::BlobStatus;
use walrus_sui::{
    client::ReadClient,
    types::{Blob, Committee, NetworkAddress, StakedWal, StorageNode},
    utils::{price_for_encoded_length, storage_units_from_size, BYTES_PER_UNIT_SIZE},
};

use super::{
    cli::{BlobIdDecimal, HumanReadableBytes},
    resource::RegisterBlobOp,
};
use crate::client::cli::{format_event_id, HumanReadableFrost};

/// Either an event ID or an object ID.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub enum EventOrObjectId {
    /// The variant representing an event ID.
    Event(EventID),
    /// The variant representing an object ID.
    Object(ObjectID),
}

impl Display for EventOrObjectId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventOrObjectId::Event(event_id) => {
                write!(f, "Certification event ID: {}", format_event_id(event_id))
            }
            EventOrObjectId::Object(object_id) => {
                write!(f, "Owned Blob registration object ID: {}", object_id)
            }
        }
    }
}

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
        /// The event where the blob was certified, or the object ID of the registered blob.
        ///
        /// The object ID of the registered blob is used in place of the event ID when the blob is
        /// deletable, already certified, and owned by the client.
        event_or_object: EventOrObjectId,
        /// The epoch until which the blob is stored (exclusive).
        #[schema(value_type = u64)]
        end_epoch: Epoch,
    },
    /// The blob was newly created; this contains the newly created Sui object associated with the
    /// blob.
    NewlyCreated {
        /// The Sui blob object that holds the newly created blob.
        blob_object: Blob,
        /// The operation that created the blob.
        resource_operation: RegisterBlobOp,
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
pub(crate) struct ReadOutput {
    #[serde(skip_serializing_if = "Option::is_none")]
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
pub(crate) struct BlobIdOutput {
    #[serde_as(as = "DisplayFromStr")]
    pub(crate) blob_id: BlobId,
    pub(crate) file: PathBuf,
    pub(crate) unencoded_length: u64,
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

/// The output of the `convert-blob-id` command.
#[serde_as]
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BlobIdConversionOutput(#[serde_as(as = "DisplayFromStr")] pub BlobId);

impl From<BlobIdDecimal> for BlobIdConversionOutput {
    fn from(value: BlobIdDecimal) -> Self {
        Self(value.into())
    }
}

/// The output of the `store --dry-run` command.
#[serde_as]
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DryRunOutput {
    /// The blob ID.
    #[serde_as(as = "DisplayFromStr")]
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
pub(crate) struct BlobStatusOutput {
    /// The blob ID.
    #[serde_as(as = "DisplayFromStr")]
    pub blob_id: BlobId,
    /// The file from which the blob was read.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file: Option<PathBuf>,
    /// The blob's status.
    pub status: BlobStatus,
}

/// The output of the `info` command.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct InfoOutput {
    pub(crate) current_epoch: Epoch,
    pub(crate) n_shards: NonZeroU16,
    pub(crate) n_nodes: usize,
    pub(crate) storage_unit_size: u64,
    pub(crate) price_per_unit_size: u64,
    pub(crate) max_blob_size: u64,
    pub(crate) marginal_size: u64,
    pub(crate) metadata_price: u64,
    pub(crate) marginal_price: u64,
    pub(crate) example_blobs: Vec<ExampleBlobInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) dev_info: Option<InfoDevOutput>,
}

/// Additional dev info for the `info` command.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct InfoDevOutput {
    pub(crate) n_primary_source_symbols: NonZeroU16,
    pub(crate) n_secondary_source_symbols: NonZeroU16,
    pub(crate) max_sliver_size: u64,
    pub(crate) metadata_storage_size: u64,
    pub(crate) max_encoded_blob_size: u64,
    pub(crate) max_faulty_shards: u16,
    pub(crate) min_correct_shards: u16,
    pub(crate) quorum_threshold: u16,
    pub(crate) storage_nodes: Vec<StorageNodeInfo>,
    #[serde(skip_serializing)]
    pub(crate) committee: Committee,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct StorageNodeInfo {
    pub(crate) name: String,
    pub(crate) network_address: NetworkAddress,
    pub(crate) public_key: PublicKey,
    pub(crate) network_public_key: NetworkPublicKey,
    pub(crate) n_shards: usize,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExampleBlobInfo {
    unencoded_size: u64,
    encoded_size: u64,
    price: u64,
}

impl ExampleBlobInfo {
    pub(crate) fn new(
        unencoded_size: u64,
        n_shards: NonZeroU16,
        price_per_unit_size: u64,
    ) -> Option<Self> {
        let encoded_size = encoded_blob_length_for_n_shards(n_shards, unencoded_size)?;
        let price = price_for_encoded_length(encoded_size, price_per_unit_size, 1);
        Some(Self {
            unencoded_size,
            encoded_size,
            price,
        })
    }

    pub(crate) fn cli_output(&self) -> String {
        format!(
            "{} unencoded ({} encoded): {} per epoch",
            HumanReadableBytes(self.unencoded_size),
            HumanReadableBytes(self.encoded_size),
            HumanReadableFrost::from(self.price)
        )
    }
}

impl From<StorageNode> for StorageNodeInfo {
    fn from(value: StorageNode) -> Self {
        let StorageNode {
            name,
            node_id: _,
            network_address,
            public_key,
            network_public_key,
            shard_ids,
        } = value;
        Self {
            name,
            network_address,
            public_key,
            network_public_key,
            n_shards: shard_ids.len(),
        }
    }
}

impl InfoOutput {
    /// Computes the Walrus system information after reading relevant data from the Walrus system
    /// object on chain.
    pub async fn get_system_info(
        sui_read_client: &impl ReadClient,
        dev: bool,
    ) -> anyhow::Result<Self> {
        let committee = sui_read_client.current_committee().await?;
        let price_per_unit_size = sui_read_client.storage_price_per_unit_size().await?;

        let current_epoch = committee.epoch;
        let n_shards = committee.n_shards();
        let (n_primary_source_symbols, n_secondary_source_symbols) =
            source_symbols_for_n_shards(n_shards);

        let n_nodes = committee.n_members();
        let max_blob_size = max_blob_size_for_n_shards(n_shards);

        let metadata_storage_size =
            (n_shards.get() as u64) * metadata_length_for_n_shards(n_shards);
        let metadata_price = storage_units_from_size(metadata_storage_size) * price_per_unit_size;

        // Make sure our marginal size can actually be encoded.
        let mut marginal_size = 1024 * 1024; // Start with 1 MiB.
        while marginal_size > max_blob_size {
            marginal_size /= 4;
        }
        let marginal_price = storage_units_from_size(
            encoded_slivers_length_for_n_shards(n_shards, marginal_size)
                .expect("we can encode 1 MiB"),
        ) * price_per_unit_size;

        let example_blob_0 = max_blob_size.next_power_of_two() / 1024;
        let example_blob_1 = example_blob_0 * 32;
        let example_blobs = [example_blob_0, example_blob_1, max_blob_size]
            .into_iter()
            .map(|unencoded_size| {
                ExampleBlobInfo::new(unencoded_size, n_shards, price_per_unit_size)
                    .expect("we can encode the given examples")
            })
            .collect();

        let dev_info = dev.then_some({
            let max_sliver_size = max_sliver_size_for_n_secondary(n_secondary_source_symbols);
            let max_encoded_blob_size = encoded_blob_length_for_n_shards(n_shards, max_blob_size)
                .expect("we can compute the encoded length of the max blob size");
            let f = bft::max_n_faulty(n_shards);
            let storage_nodes = committee
                .members()
                .iter()
                .cloned()
                .map(StorageNodeInfo::from)
                .collect();
            InfoDevOutput {
                n_primary_source_symbols,
                n_secondary_source_symbols,
                metadata_storage_size,
                max_sliver_size,
                max_encoded_blob_size,
                max_faulty_shards: f,
                min_correct_shards: n_shards.get() - f,
                quorum_threshold: 2 * f + 1,
                storage_nodes,
                committee,
            }
        });

        Ok(Self {
            storage_unit_size: BYTES_PER_UNIT_SIZE,
            price_per_unit_size,
            current_epoch,
            n_shards,
            n_nodes,
            max_blob_size,
            metadata_price,
            marginal_size,
            marginal_price,
            example_blobs,
            dev_info,
        })
    }
}

#[serde_as]
#[serde_with::skip_serializing_none]
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeleteOutput {
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub(crate) blob_id: Option<BlobId>,
    pub(crate) file: Option<PathBuf>,
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub(crate) object_id: Option<ObjectID>,
    pub(crate) deleted_blobs: Vec<Blob>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
/// The output of the `walrus stake` command.
pub struct StakeOutput {
    /// The staked WAL after staking.
    pub staked_wal: StakedWal,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
/// The output of the `walrus get-wal` command.
pub struct ExchangeOutput {
    /// The amount of SUI exchanged (in MIST).
    pub amount_sui: u64,
}

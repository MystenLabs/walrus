// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Structures of client results returned by the daemon or through the JSON API.

use std::{
    collections::HashMap,
    fmt::Display,
    num::NonZeroU16,
    path::{Path, PathBuf},
    time::Duration,
};

use serde::{Deserialize, Serialize};
use serde_with::{base64::Base64, serde_as, DisplayFromStr};
use sui_types::{
    base_types::{ObjectID, SuiAddress},
    event::EventID,
};
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
    metadata::{BlobMetadataApi as _, VerifiedBlobMetadataWithId},
    BlobId,
    Epoch,
    NetworkPublicKey,
    PublicKey,
    ShardIndex,
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
#[serde(rename_all = "camelCase")]
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

/// Blob store result with its file path.
#[derive(Serialize, Deserialize, Debug)]
pub struct BlobStoreResultWithPath {
    /// The result of the store operation.
    pub blob_store_result: BlobStoreResult,
    /// The file path to the blob.
    pub path: PathBuf,
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
        #[serde(flatten)]
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
        /// The shared blob object ID if created.
        #[serde_as(as = "Option<DisplayFromStr>")]
        #[serde(skip_serializing_if = "Option::is_none")]
        shared_blob_object: Option<ObjectID>,
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
            unencoded_length: metadata.metadata().unencoded_length(),
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
    /// The file path to the blob.
    pub path: PathBuf,
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
    pub(crate) storage_price_per_unit_size: u64,
    pub(crate) write_price_per_unit_size: u64,
    pub(crate) max_blob_size: u64,
    pub(crate) marginal_size: u64,
    pub(crate) metadata_price: u64,
    pub(crate) marginal_price: u64,
    pub(crate) example_blobs: Vec<ExampleBlobInfo>,
    pub(crate) epoch_duration: Duration,
    pub(crate) max_epochs_ahead: u32,
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
    pub(crate) next_storage_nodes: Option<Vec<StorageNodeInfo>>,
    #[serde(skip_serializing)]
    pub(crate) committee: Committee,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct StorageNodeInfo {
    pub(crate) name: String,
    pub(crate) network_address: NetworkAddress,
    pub(crate) public_key: PublicKey,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) next_epoch_public_key: Option<PublicKey>,
    pub(crate) network_public_key: NetworkPublicKey,
    pub(crate) n_shards: usize,
    pub(crate) shard_ids: Vec<ShardIndex>,
    pub(crate) node_id: ObjectID,
    pub(crate) stake: u64,
}

impl StorageNodeInfo {
    fn from_node_and_stake(value: StorageNode, stake: u64) -> Self {
        let StorageNode {
            name,
            node_id,
            network_address,
            public_key,
            next_epoch_public_key,
            network_public_key,
            shard_ids,
            ..
        } = value;
        Self {
            name,
            network_address,
            public_key,
            next_epoch_public_key,
            network_public_key,
            n_shards: shard_ids.len(),
            shard_ids,
            node_id,
            stake,
        }
    }
}

fn merge_nodes_and_stake(
    committee: &Committee,
    stake_assignment: &HashMap<ObjectID, u64>,
) -> Vec<StorageNodeInfo> {
    committee
        .members()
        .iter()
        .cloned()
        .map(|node| {
            let stake = *stake_assignment
                .get(&node.node_id)
                .expect("a node in the committee must have stake");
            StorageNodeInfo::from_node_and_stake(node, stake)
        })
        .collect()
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

impl InfoOutput {
    /// Computes the Walrus system information after reading relevant data from the Walrus system
    /// object on chain.
    pub async fn get_system_info(
        sui_read_client: &impl ReadClient,
        dev: bool,
    ) -> anyhow::Result<Self> {
        // TODO(giac): reduce the number of RPC calls by exposing the system and staking objects
        // directly (#1222).
        let committee = sui_read_client.current_committee().await?;
        let (storage_price_per_unit_size, write_price_per_unit_size) = sui_read_client
            .storage_and_write_price_per_unit_size()
            .await?;
        let fixed_params = sui_read_client.fixed_system_parameters().await?;
        let next_committee = sui_read_client.next_committee().await?;
        let stake_assignment = sui_read_client.stake_assignment().await?;

        let current_epoch = committee.epoch;
        let n_shards = committee.n_shards();
        let (n_primary_source_symbols, n_secondary_source_symbols) =
            source_symbols_for_n_shards(n_shards);

        let n_nodes = committee.n_members();
        let max_blob_size = max_blob_size_for_n_shards(n_shards);

        let metadata_storage_size =
            (n_shards.get() as u64) * metadata_length_for_n_shards(n_shards);
        let metadata_price =
            storage_units_from_size(metadata_storage_size) * storage_price_per_unit_size;

        // Make sure our marginal size can actually be encoded.
        let mut marginal_size = 1024 * 1024; // Start with 1 MiB.
        while marginal_size > max_blob_size {
            marginal_size /= 4;
        }
        let marginal_price = storage_units_from_size(
            encoded_slivers_length_for_n_shards(n_shards, marginal_size)
                .expect("we can encode 1 MiB"),
        ) * storage_price_per_unit_size;

        let example_blob_0 = max_blob_size.next_power_of_two() / 1024;
        let example_blob_1 = example_blob_0 * 32;
        let example_blobs = [example_blob_0, example_blob_1, max_blob_size]
            .into_iter()
            .map(|unencoded_size| {
                ExampleBlobInfo::new(unencoded_size, n_shards, storage_price_per_unit_size)
                    .expect("we can encode the given examples")
            })
            .collect();

        let dev_info = dev.then_some({
            let max_sliver_size = max_sliver_size_for_n_secondary(n_secondary_source_symbols);
            let max_encoded_blob_size = encoded_blob_length_for_n_shards(n_shards, max_blob_size)
                .expect("we can compute the encoded length of the max blob size");
            let f = bft::max_n_faulty(n_shards);
            let storage_nodes = merge_nodes_and_stake(&committee, &stake_assignment);

            let next_storage_nodes = next_committee
                .as_ref()
                .map(|next_committee| merge_nodes_and_stake(next_committee, &stake_assignment));

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
                next_storage_nodes,
                committee,
            }
        });

        Ok(Self {
            storage_unit_size: BYTES_PER_UNIT_SIZE,
            storage_price_per_unit_size,
            write_price_per_unit_size,
            current_epoch,
            n_shards,
            n_nodes,
            max_blob_size,
            metadata_price,
            marginal_size,
            marginal_price,
            example_blobs,
            epoch_duration: fixed_params.epoch_duration,
            max_epochs_ahead: fixed_params.max_epochs_ahead,
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
    pub(crate) post_deletion_status: Option<BlobStatus>,
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
/// The output of the `walrus generate-sui-wallet` command.
pub struct WalletOutput {
    /// The address of the generated wallet.
    pub wallet_address: SuiAddress,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
/// The output of the `walrus get-wal` command.
pub struct ExchangeOutput {
    /// The amount of SUI exchanged (in MIST).
    pub amount_sui: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
/// The output of the `walrus share` command.
pub struct ShareBlobOutput {
    /// The shared blob object ID.
    pub shared_blob_object_id: ObjectID,
    /// The amount of FROST if funded.
    pub amount: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
/// The output of the `walrus fund-shared-blob` command.
pub struct FundSharedBlobOutput {
    /// The amount of FROST funded.
    pub amount: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
/// The output of the `walrus extend` command.
pub struct ExtendBlobOutput {
    /// The number of epochs extended by.
    pub epochs_ahead: u32,
}

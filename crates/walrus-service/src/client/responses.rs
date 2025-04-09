// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Structures of client results returned by the daemon or through the JSON API.

use std::{
    collections::HashMap,
    fmt::Display,
    num::NonZeroU16,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow;
use chrono::{DateTime, Utc};
use futures::{stream, StreamExt as _};
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
    EncodingType,
    Epoch,
    EpochCount,
    NetworkPublicKey,
    PublicKey,
    ShardIndex,
    DEFAULT_ENCODING,
};
use walrus_rest_client::api::{BlobStatus, ServiceHealthInfo};
use walrus_sui::{
    client::ReadClient,
    types::{
        move_structs::{Blob, BlobAttribute, EpochState},
        Committee,
        NetworkAddress,
        StakedWal,
        StorageNode,
    },
    utils::{price_for_encoded_length, storage_units_from_size, BYTES_PER_UNIT_SIZE},
    EventIdSchema,
    ObjectIdSchema,
};

use super::{
    cli::{BlobIdDecimal, BlobIdentity, HumanReadableBytes},
    communication::NodeCommunicationFactory,
    resource::RegisterBlobOp,
};
use crate::client::cli::{format_event_id, HealthSortBy, HumanReadableFrost, NodeSortBy, SortBy};

/// Either an event ID or an object ID.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum EventOrObjectId {
    /// The variant representing an event ID.
    #[schema(value_type = EventIdSchema)]
    Event(EventID),
    /// The variant representing an object ID.
    #[schema(value_type = ObjectIdSchema)]
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
#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct BlobStoreResultWithPath {
    /// The result of the store operation.
    pub blob_store_result: BlobStoreResult,
    /// The file path to the blob.
    pub path: PathBuf,
}

/// Result when attempting to store a blob.
#[serde_as]
#[derive(Debug, Clone, Serialize, ToSchema)]
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
        #[schema(value_type = Option<ObjectIdSchema>)]
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
        #[schema(value_type = EventIdSchema)]
        event: EventID,
    },
    /// Operation failed.
    Error {
        /// The blob ID.
        #[serde_as(as = "Option<DisplayFromStr>")]
        blob_id: Option<BlobId>,
        /// The error message.
        error_msg: String,
    },
}

impl BlobStoreResult {
    /// Returns the blob ID.
    pub fn blob_id(&self) -> Option<BlobId> {
        match self {
            Self::AlreadyCertified { blob_id, .. } => Some(*blob_id),
            Self::MarkedInvalid { blob_id, .. } => Some(*blob_id),
            Self::NewlyCreated {
                blob_object: Blob { blob_id, .. },
                ..
            } => Some(*blob_id),
            Self::Error { blob_id, .. } => *blob_id,
        }
    }

    /// Returns the end epoch of the blob.
    pub fn end_epoch(&self) -> Option<Epoch> {
        match self {
            Self::AlreadyCertified { end_epoch, .. } => Some(*end_epoch),
            Self::NewlyCreated { blob_object, .. } => Some(blob_object.storage.end_epoch),
            Self::MarkedInvalid { .. } => None,
            Self::Error { .. } => None,
        }
    }

    /// Returns true if the blob is not stored.
    pub fn is_not_stored(&self) -> bool {
        matches!(self, Self::MarkedInvalid { .. } | Self::Error { .. })
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
    pub(crate) encoding_type: EncodingType,
}

impl BlobIdOutput {
    /// Creates a new [`BlobIdOutput`] object.
    pub fn new(file: &Path, metadata: &VerifiedBlobMetadataWithId) -> Self {
        Self {
            blob_id: *metadata.blob_id(),
            file: file.to_owned(),
            unencoded_length: metadata.metadata().unencoded_length(),
            encoding_type: metadata.metadata().encoding_type(),
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
    /// The encoding type used for the blob.
    pub encoding_type: EncodingType,
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
    /// The estimated expiry timestamp of the blob, present only for permanent blob.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_expiry_timestamp: Option<DateTime<Utc>>,
}

/// The output of the `info` command.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct InfoOutput {
    pub(crate) epoch_info: InfoEpochOutput,
    pub(crate) storage_info: InfoStorageOutput,
    pub(crate) size_info: InfoSizeOutput,
    pub(crate) price_info: InfoPriceOutput,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) committee_info: Option<InfoCommitteeOutput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) bft_info: Option<InfoBftOutput>,
}

impl InfoOutput {
    pub async fn get_system_info(
        sui_read_client: &impl ReadClient,
        dev: bool,
        sort: SortBy<NodeSortBy>,
        encoding_types: &[EncodingType],
    ) -> anyhow::Result<Self> {
        let epoch_info = InfoEpochOutput::get_epoch_info(sui_read_client).await?;
        let storage_info = InfoStorageOutput::get_storage_info(sui_read_client).await?;
        let size_info = InfoSizeOutput::get_size_info(sui_read_client).await?;
        let price_info = InfoPriceOutput::get_price_info(sui_read_client, encoding_types).await?;
        let committee_info: Option<InfoCommitteeOutput> = if dev {
            Some(InfoCommitteeOutput::get_committee_info(sui_read_client, sort).await?)
        } else {
            None
        };
        let bft_info = if dev {
            Some(InfoBftOutput::get_bft_info(sui_read_client).await?)
        } else {
            None
        };

        Ok(Self {
            epoch_info,
            storage_info,
            size_info,
            price_info,
            committee_info,
            bft_info,
        })
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) enum EpochTimeOrMessage {
    DateTime(DateTime<Utc>),
    Message(String),
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct InfoEpochOutput {
    pub(crate) current_epoch: Epoch,
    pub(crate) start_of_current_epoch: EpochTimeOrMessage,
    pub(crate) epoch_duration: Duration,
    pub(crate) max_epochs_ahead: EpochCount,
}

impl InfoEpochOutput {
    pub async fn get_epoch_info(sui_read_client: &impl ReadClient) -> anyhow::Result<Self> {
        let current_epoch = sui_read_client.current_epoch().await?;
        let fixed_params = sui_read_client.fixed_system_parameters().await?;
        let epoch_duration = fixed_params.epoch_duration;
        let max_epochs_ahead = fixed_params.max_epochs_ahead;
        let epoch_state = sui_read_client.epoch_state().await?;
        let start_of_current_epoch = match epoch_state {
            EpochState::EpochChangeDone(epoch_start)
            | EpochState::NextParamsSelected(epoch_start) => {
                EpochTimeOrMessage::DateTime(epoch_start)
            }
            EpochState::EpochChangeSync(_) => EpochTimeOrMessage::Message(format!(
                "Epoch change is currently in progress... Expected epoch end time is {}",
                Utc::now() + epoch_duration
            )),
        };

        Ok(Self {
            current_epoch,
            start_of_current_epoch,
            epoch_duration,
            max_epochs_ahead,
        })
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct InfoStorageOutput {
    pub(crate) n_shards: NonZeroU16,
    pub(crate) n_nodes: usize,
}

impl InfoStorageOutput {
    pub async fn get_storage_info(sui_read_client: &impl ReadClient) -> anyhow::Result<Self> {
        let committee = sui_read_client.current_committee().await?;

        Ok(Self {
            n_shards: committee.n_shards(),
            n_nodes: committee.n_members(),
        })
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct InfoSizeOutput {
    pub(crate) storage_unit_size: u64,
    pub(crate) max_blob_size: u64,
}

impl InfoSizeOutput {
    pub async fn get_size_info(sui_read_client: &impl ReadClient) -> anyhow::Result<Self> {
        let committee = sui_read_client.current_committee().await?;

        Ok(Self {
            storage_unit_size: BYTES_PER_UNIT_SIZE,
            max_blob_size: max_blob_size_for_n_shards(committee.n_shards(), DEFAULT_ENCODING),
        })
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct EncodingDependentPriceInfo {
    pub(crate) marginal_size: u64,
    pub(crate) metadata_price: u64,
    pub(crate) marginal_price: u64,
    pub(crate) example_blobs: Vec<ExampleBlobInfo>,
    pub(crate) encoding_type: EncodingType,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct InfoPriceOutput {
    pub(crate) storage_price_per_unit_size: u64,
    pub(crate) write_price_per_unit_size: u64,
    pub(crate) encoding_dependent_price_info: Vec<EncodingDependentPriceInfo>,
}

impl EncodingDependentPriceInfo {
    pub(crate) fn new(
        n_shards: NonZeroU16,
        storage_price_per_unit_size: u64,
        encoding_type: EncodingType,
    ) -> Self {
        // Calculate marginal size and price
        let mut marginal_size = 1024 * 1024; // Start with 1 MiB
        while marginal_size > max_blob_size_for_n_shards(n_shards, encoding_type) {
            marginal_size /= 4;
        }

        let metadata_storage_size =
            (n_shards.get() as u64) * metadata_length_for_n_shards(n_shards);
        let metadata_price =
            storage_units_from_size(metadata_storage_size) * storage_price_per_unit_size;

        let marginal_price = storage_units_from_size(
            encoded_slivers_length_for_n_shards(n_shards, marginal_size, encoding_type)
                .expect("we can encode 1 MiB"),
        ) * storage_price_per_unit_size;

        // Calculate example blobs
        let example_blob_0 =
            max_blob_size_for_n_shards(n_shards, encoding_type).next_power_of_two() / 1024;
        let example_blob_1 = example_blob_0 * 32;
        let example_blobs = [
            example_blob_0,
            example_blob_1,
            max_blob_size_for_n_shards(n_shards, encoding_type),
        ]
        .into_iter()
        .map(|unencoded_size| {
            ExampleBlobInfo::new(
                unencoded_size,
                n_shards,
                storage_price_per_unit_size,
                encoding_type,
            )
            .expect("we can encode the given examples")
        })
        .collect();

        Self {
            marginal_size,
            metadata_price,
            marginal_price,
            example_blobs,
            encoding_type,
        }
    }
}

impl InfoPriceOutput {
    pub async fn get_price_info(
        sui_read_client: &impl ReadClient,
        encoding_types: &[EncodingType],
    ) -> anyhow::Result<Self> {
        let committee = sui_read_client.current_committee().await?;
        let (storage_price_per_unit_size, write_price_per_unit_size) = sui_read_client
            .storage_and_write_price_per_unit_size()
            .await?;
        let n_shards = committee.n_shards();

        let encoding_dependent_price_info = encoding_types
            .iter()
            .map(|&encoding_type| {
                EncodingDependentPriceInfo::new(
                    n_shards,
                    storage_price_per_unit_size,
                    encoding_type,
                )
            })
            .collect();

        Ok(Self {
            storage_price_per_unit_size,
            write_price_per_unit_size,
            encoding_dependent_price_info,
        })
    }
}

/// Committee information.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct InfoCommitteeOutput {
    pub(crate) n_shards: NonZeroU16,
    pub(crate) n_primary_source_symbols: NonZeroU16,
    pub(crate) n_secondary_source_symbols: NonZeroU16,
    pub(crate) max_sliver_size: u64,
    pub(crate) metadata_storage_size: u64,
    pub(crate) max_encoded_blob_size: u64,
    pub(crate) storage_nodes: Vec<StorageNodeInfo>,
    pub(crate) next_storage_nodes: Option<Vec<StorageNodeInfo>>,
}

impl InfoCommitteeOutput {
    pub async fn get_committee_info(
        sui_read_client: &impl ReadClient,
        sort: SortBy<NodeSortBy>,
    ) -> anyhow::Result<Self> {
        let committee = sui_read_client.current_committee().await?;
        let next_committee = sui_read_client.next_committee().await?;
        let stake_assignment = sui_read_client.stake_assignment().await?;

        let n_shards = committee.n_shards();
        let (n_primary_source_symbols, n_secondary_source_symbols) =
            source_symbols_for_n_shards(n_shards, DEFAULT_ENCODING);

        let max_sliver_size =
            max_sliver_size_for_n_secondary(n_secondary_source_symbols, DEFAULT_ENCODING);
        let max_blob_size = max_blob_size_for_n_shards(n_shards, DEFAULT_ENCODING);
        let max_encoded_blob_size =
            encoded_blob_length_for_n_shards(n_shards, max_blob_size, DEFAULT_ENCODING)
                .expect("we can compute the encoded length of the max blob size");

        let mut storage_nodes = merge_nodes_and_stake(&committee, &stake_assignment);
        let mut next_storage_nodes = next_committee
            .as_ref()
            .map(|next_committee| merge_nodes_and_stake(next_committee, &stake_assignment));

        // Sort nodes if sort_by is specified
        let cmp = |a: &StorageNodeInfo, b: &StorageNodeInfo| match sort.sort_by {
            Some(NodeSortBy::Id) => a.node_id.cmp(&b.node_id),
            Some(NodeSortBy::Url) => a
                .network_address
                .0
                .to_lowercase()
                .cmp(&b.network_address.0.to_lowercase()),
            Some(NodeSortBy::Name) => a.name.to_lowercase().cmp(&b.name.to_lowercase()),
            None => a.name.to_lowercase().cmp(&b.name.to_lowercase()),
        };

        storage_nodes.sort_by(|a, b| if sort.desc { cmp(b, a) } else { cmp(a, b) });

        if let Some(ref mut nodes) = next_storage_nodes {
            nodes.sort_by(|a, b| if sort.desc { cmp(b, a) } else { cmp(a, b) });
        }

        let metadata_storage_size =
            (n_shards.get() as u64) * metadata_length_for_n_shards(n_shards);

        Ok(Self {
            n_shards,
            n_primary_source_symbols,
            n_secondary_source_symbols,
            metadata_storage_size,
            max_sliver_size,
            max_encoded_blob_size,
            storage_nodes,
            next_storage_nodes,
        })
    }
}

/// BFT system information.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct InfoBftOutput {
    pub(crate) max_faulty_shards: u16,
    pub(crate) min_correct_shards: u16,
    pub(crate) quorum_threshold: u16,
    pub(crate) min_nodes_above: usize,
    pub(crate) shards_above: usize,
}

impl InfoBftOutput {
    pub async fn get_bft_info(sui_read_client: &impl ReadClient) -> anyhow::Result<Self> {
        let committee = sui_read_client.current_committee().await?;
        let n_shards = committee.n_shards();
        let f = bft::max_n_faulty(n_shards);
        let min_correct_shards = n_shards.get() - f;
        let quorum_threshold = 2 * f + 1;
        let (min_nodes_above, shards_above) = committee.min_nodes_above_f();
        Ok(Self {
            max_faulty_shards: f,
            min_correct_shards,
            quorum_threshold,
            min_nodes_above,
            shards_above,
        })
    }
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
    encoding_type: EncodingType,
}

impl ExampleBlobInfo {
    pub(crate) fn new(
        unencoded_size: u64,
        n_shards: NonZeroU16,
        price_per_unit_size: u64,
        encoding_type: EncodingType,
    ) -> Option<Self> {
        let encoded_size =
            encoded_blob_length_for_n_shards(n_shards, unencoded_size, encoding_type)?;
        let price = price_for_encoded_length(encoded_size, price_per_unit_size, 1);
        Some(Self {
            unencoded_size,
            encoded_size,
            price,
            encoding_type,
        })
    }

    pub(crate) fn cli_output(&self) -> String {
        format!(
            "  - {} unencoded ({} encoded): {} per epoch",
            HumanReadableBytes(self.unencoded_size),
            HumanReadableBytes(self.encoded_size),
            HumanReadableFrost::from(self.price)
        )
    }
}

#[serde_as]
#[serde_with::skip_serializing_none]
#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeleteOutput {
    pub(crate) blob_identity: BlobIdentity,
    pub(crate) deleted_blobs: Vec<Blob>,
    pub(crate) post_deletion_status: Option<BlobStatus>,
    pub(crate) no_blob_found: bool,
    pub(crate) error: Option<String>,
    pub(crate) aborted: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
/// The output of the `walrus stake` command.
pub struct StakeOutput {
    /// The staked WAL after staking.
    pub staked_wal: Vec<StakedWal>,
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
/// The output of the `walrus get-blob-attribute` command.
pub struct GetBlobAttributeOutput {
    /// The attribute of the blob.
    pub attribute: Option<BlobAttribute>,
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
    pub epochs_extended: EpochCount,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
/// The health information of a storage node.
pub(crate) struct NodeHealthOutput {
    pub node_id: ObjectID,
    pub node_url: String,
    pub node_name: String,
    pub network_public_key: NetworkPublicKey,
    pub health_info: Result<ServiceHealthInfo, String>,
}

impl NodeHealthOutput {
    pub async fn new(
        node: StorageNode,
        detail: bool,
        node_communication_factory: &NodeCommunicationFactory,
    ) -> Self {
        let client = node_communication_factory.create_client(&node);
        let health_info = match client {
            Ok(client) => client
                .get_server_health_info(detail)
                .await
                .map_err(|err| format!("failed to get health info: {:?}", err)),
            Err(err) => Err(format!("failed to build client: {:?}", err)),
        };

        Self {
            node_id: node.node_id,
            node_url: node.network_address.0.clone(),
            node_name: node.name,
            network_public_key: node.network_public_key,
            health_info,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
/// The output of the `walrus health` command.
pub(crate) struct ServiceHealthInfoOutput {
    pub health_info: Vec<NodeHealthOutput>,
    pub latest_seq: Option<u64>,
}

impl ServiceHealthInfoOutput {
    /// Collects the health information of the storage nodes by querying their health endpoints.
    pub async fn new_for_nodes(
        nodes: impl IntoIterator<Item = StorageNode>,
        communication_factory: &NodeCommunicationFactory,
        latest_seq: Option<u64>,
        detail: bool,
        sort: SortBy<HealthSortBy>,
    ) -> anyhow::Result<Self> {
        let mut health_info = stream::iter(nodes)
            .map(|node| NodeHealthOutput::new(node, detail, communication_factory))
            .buffer_unordered(10)
            .collect::<Vec<_>>()
            .await;

        // Sort health_info directly based on sort_by
        health_info.sort_by(|a, b| match sort.sort_by {
            Some(HealthSortBy::Name) => a.cmp_by_name(b),
            Some(HealthSortBy::Id) => a.cmp_by_id(b),
            Some(HealthSortBy::Url) => a.cmp_by_url(b),
            Some(HealthSortBy::Status) => a.cmp_by_status(b),
            None => a.cmp_by_status(b),
        });

        if sort.desc {
            health_info.reverse();
        }

        Ok(Self {
            health_info,
            latest_seq,
        })
    }
}

impl NodeHealthOutput {
    pub fn cmp_by_name(&self, other: &Self) -> std::cmp::Ordering {
        self.node_name
            .to_lowercase()
            .cmp(&other.node_name.to_lowercase())
    }

    pub fn cmp_by_id(&self, other: &Self) -> std::cmp::Ordering {
        self.node_id.cmp(&other.node_id)
    }

    pub fn cmp_by_url(&self, other: &Self) -> std::cmp::Ordering {
        self.node_url
            .to_lowercase()
            .cmp(&other.node_url.to_lowercase())
    }

    pub fn cmp_by_status(&self, other: &Self) -> std::cmp::Ordering {
        match (&self.health_info, &other.health_info) {
            (Ok(info_a), Ok(info_b)) => info_a
                .node_status
                .to_lowercase()
                .cmp(&info_b.node_status.to_lowercase())
                .then_with(|| self.cmp_by_name(other)),
            (Err(err_a), Err(err_b)) => err_a.cmp(err_b).then_with(|| self.cmp_by_name(other)),
            (Err(_), Ok(_)) => std::cmp::Ordering::Greater,
            (Ok(_), Err(_)) => std::cmp::Ordering::Less,
        }
    }
}

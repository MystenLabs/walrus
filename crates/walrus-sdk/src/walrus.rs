// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! The primary entrypoint for Walrus SDK users.

use std::{path::PathBuf, sync::Arc};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
pub use epochs::EpochArg;
use sui_sdk::wallet_context::WalletContext;
use sui_types::base_types::ObjectID;
use walrus_core::{
    BlobId,
    EncodingType,
    EpochCount,
    encoding::{EncodingConfig, EncodingConfigTrait as _},
};
use walrus_storage_node_client::api::BlobStatus;
use walrus_sui::{
    client::{BlobPersistence, PostStoreAction},
    config::WalletConfig,
    types::move_structs::StakedWal,
};

use crate::{client::ClientConfig, config::load_configuration, store_when::StoreWhen};
pub mod epochs;

/// The output of the `store_blobs_dry_run` method.
#[derive(Debug, Clone)]
pub struct DryRunDetails {
    /// The blob ID.
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

/// An enum representing either a file or a blob ID.
#[derive(Debug, Clone)]
pub enum FileOrBlobId {
    /// A file containing a blob.
    File(PathBuf),
    /// A blob ID.
    BlobId(BlobId),
}

impl FileOrBlobId {
    /// Returns the blob ID if it is already present, or computes it from the file.
    pub fn get_or_compute_blob_id(
        self,
        encoding_config: &EncodingConfig,
        encoding_type: EncodingType,
    ) -> Result<BlobId> {
        match self {
            Self::BlobId(blob_id) => Ok(blob_id),
            Self::File(filename) => {
                tracing::debug!(
                    file = %filename.display(),
                    "checking status of blob read from the filesystem"
                );
                let data = std::fs::read(&filename)
                    .context(format!("unable to read blob from '{}'", filename.display()))?;
                Ok(*encoding_config
                    .get_for_type(encoding_type)
                    .compute_metadata(&data)?
                    .blob_id())
            }
        }
    }
}

/// The output of the [`Walrus::fetch_blob_status_info`] method.
#[derive(Debug, Clone)]
pub struct BlobStatusInfo {
    /// The blob ID.
    pub blob_id: BlobId,
    /// The file from which the blob might have been read.
    pub file: Option<PathBuf>,
    /// The blob's status.
    pub status: BlobStatus,
    /// The estimated expiry timestamp of the blob, present only for permanent blob.
    pub estimated_expiry_timestamp: Option<DateTime<Utc>>,
}

/// The output of the [`Walrus::list_blobs`] method.
#[derive(Debug)]
pub struct BlobInfo {
    /// The blob ID.
    pub blob_id: BlobId,
    /// The blob's size (in bytes).
    pub unencoded_size: u64,
    /// Whether the blob has been certified.
    pub certified: bool,
    /// Whether the blob is deletable.
    pub deletable: bool,
    /// The blob's expiry epoch.
    pub expiry_epoch: u64,
    /// The blob's object ID.
    pub object_id: Option<ObjectID>,
}

/// An enum representing a blob content address within the system.
#[derive(Debug)]
pub enum BlobSpecifier {
    /// The blob ID.
    BlobId(BlobId),
    /// The blob's object ID.
    ObjectId(ObjectID),
}

/// The main entrypoint for the Walrus SDK.
pub struct Walrus {
    /// The Sui wallet for the client.
    _wallet_context: Arc<WalletContext>,
    /// The config for the client.
    config: ClientConfig,
}

impl std::fmt::Debug for Walrus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Walrus")
            .field("config", &self.config)
            .finish()
    }
}

impl Walrus {
    /// Creates a high-level Walrus network client, loading the configuration and wallet context.
    pub fn new(
        config: &Option<PathBuf>,
        context: Option<&str>,
        wallet_override: &Option<PathBuf>,
    ) -> Result<Self> {
        let config = load_configuration(config.as_ref(), context)?;
        let wallet_config = wallet_override
            .as_ref()
            .map(WalletConfig::from_path)
            .or_else(|| config.wallet_config.clone());
        let wallet_context = Arc::new(WalletConfig::load_wallet_context(
            wallet_config.as_ref(),
            config.communication_config.sui_client_request_timeout,
        )?);

        Ok(Self {
            _wallet_context: wallet_context,
            config,
        })
    }

    // Implementations of client commands.

    /// Read a blob from the Walrus network.
    pub async fn read_blob(&self, _blob_id: BlobId) -> Result<Vec<u8>> {
        todo!()
    }

    /// Write one or more blobs to the Walrus network.
    ///
    /// Returns the blob IDs of the blobs that were written, in the order of the input blobs.
    #[allow(clippy::too_many_arguments)]
    pub async fn store_blobs(
        &self,
        _blobs: Vec<Vec<u8>>,
        _epoch_arg: EpochArg,
        _store_when: StoreWhen,
        _persistence: BlobPersistence,
        _post_store: PostStoreAction,
        _encoding_type: Option<EncodingType>,
    ) -> Result<Vec<BlobId>> {
        todo!()
    }

    /// Compute the details related to storing several blobs. This method does not actually store
    /// the blobs, but returns the details of what would happen if they were stored.
    pub async fn store_blobs_dry_run(
        &self,
        _files: Vec<PathBuf>,
        _encoding_type: EncodingType,
        _epochs_ahead: EpochCount,
        _json: bool,
    ) -> Result<Vec<DryRunDetails>> {
        todo!()
    }

    /// Fetches the status of a blob given a [`FileOrBlobId`] and an optional encoding type.
    pub async fn fetch_blob_status_info(
        &self,
        _file_or_blob_id: FileOrBlobId,
        _encoding_type: Option<EncodingType>,
    ) -> Result<BlobStatusInfo> {
        todo!()
    }

    /// Lists all blobs owned by the wallet.
    pub async fn list_blobs(&self, _include_expired: bool) -> Result<Vec<BlobInfo>> {
        todo!()
    }

    /// Delete a blob from the Walrus network.
    pub async fn delete_blob(&self, _blobs: BlobSpecifier) -> Result<()> {
        todo!()
    }

    /// For each entry in `node_ids_with_amounts`, stakes the amount of WAL specified by the
    /// corresponding second element of the pair with the node represented by the first element of
    /// the pair.
    pub async fn stake_with_node_pools(
        &self,
        _node_ids_with_amounts: Vec<(ObjectID, u64)>,
    ) -> Result<Vec<StakedWal>> {
        todo!()
    }
}

// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! The primary entrypoint for Walrus SDK users.

use std::{
    path::PathBuf,
    sync::{Arc, LazyLock},
};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
pub use epochs::EpochArg;
use sui_sdk::wallet_context::WalletContext;
use sui_types::base_types::ObjectID;
use tokio::sync::Mutex;
use walrus_core::{
    BlobId,
    EncodingType,
    EpochCount,
    encoding::{EncodingConfig, EncodingConfigTrait as _, Primary},
};
use walrus_storage_node_client::api::BlobStatus;
use walrus_sui::{
    client::{BlobPersistence, PostStoreAction, SuiReadClient, retry_client::RetriableSuiClient},
    config::WalletConfig,
    types::move_structs::StakedWal,
};
use walrus_utils::backoff::ExponentialBackoffConfig;

use crate::{
    client::{Blocklist, Client, ClientConfig, refresh::CommitteesRefresherHandle},
    config::load_configuration,
    store_when::StoreWhen,
};
pub mod epochs;

/// The handle to the global refresher. This is a singleton.
static REFRESH_HANDLE: LazyLock<Mutex<Option<CommitteesRefresherHandle>>> =
    LazyLock::new(|| Mutex::new(None));

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
    /// A blocklist of blobs to avoid.
    blocklist: Option<Blocklist>,
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
        blocklist: Option<Blocklist>,
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
            blocklist,
        })
    }

    // Implementations of client commands.

    /// Read a blob from the Walrus network.
    pub async fn read_blob(&self, blob_id: BlobId) -> Result<Vec<u8>> {
        let client = {
            let sui_read_client = {
                let sui_client = RetriableSuiClient::new_for_rpc_urls(
                    &self.config.rpc_urls,
                    ExponentialBackoffConfig::default(),
                    self.config.communication_config.sui_client_request_timeout,
                )
                .await
                .context(format!(
                    "cannot connect to Sui RPC nodes at {}",
                    self.config.rpc_urls.join(", ")
                ))?;

                self.config.new_read_client(sui_client).await?
            };

            let mut client = Client::new_read_client(
                self.config.clone(),
                self.get_refresh_handle(sui_read_client.clone()).await?,
                sui_read_client,
            )
            .await?;

            if let Some(blocklist) = self.blocklist.as_ref() {
                client = client.with_blocklist(blocklist.clone());
            }

            client
        };

        let start_timer = std::time::Instant::now();
        let blob = client.read_blob::<Primary>(&blob_id).await?;
        let blob_size = blob.len();
        let elapsed = start_timer.elapsed();

        tracing::debug!(%blob_id, ?elapsed, blob_size, "finished reading blob");
        Ok(blob)
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

    // Get access to the global committees refresher handle. Create one if it doesn't exist.
    async fn get_refresh_handle(
        &self,
        sui_read_client: SuiReadClient,
    ) -> Result<CommitteesRefresherHandle> {
        let mut global_refresher_handle = REFRESH_HANDLE.lock().await;
        match global_refresher_handle.as_ref() {
            Some(handle) => Ok(handle.clone()),
            None => {
                let new_handle = self
                    .config
                    .refresh_config
                    .build_refresher_and_run(sui_read_client)
                    .await?;
                *global_refresher_handle = Some(new_handle.clone());
                Ok(new_handle)
            }
        }
    }
}

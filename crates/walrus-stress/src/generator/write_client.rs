// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use indicatif::MultiProgress;
use rand::{Rng, SeedableRng, rngs::StdRng, seq::SliceRandom};
use sui_sdk::types::base_types::SuiAddress;
use walrus_core::{
    BlobId,
    DEFAULT_ENCODING,
    EpochCount,
    SliverPairIndex,
    encoding::{
        EncodingConfigTrait as _,
        quilt_encoding::{QuiltStoreBlob, QuiltVersionV1},
    },
    merkle::Node,
    metadata::VerifiedBlobMetadataWithId,
    test_utils::generate_random_quilt_store_blobs,
};
use walrus_sdk::{
    client::{
        Client,
        metrics::ClientMetrics,
        quilt_client::QuiltClientConfig,
        refresh::CommitteesRefresherHandle,
        responses::QuiltStoreResult,
    },
    error::ClientError,
    store_optimizations::StoreOptimizations,
};
use walrus_service::client::{ClientConfig, Refiller};
use walrus_sui::{
    client::{
        BlobPersistence,
        PostStoreAction,
        ReadClient,
        SuiContractClient,
        retry_client::{RetriableSuiClient, retriable_sui_client::LazySuiClientBuilder},
    },
    test_utils::temp_dir_wallet,
    utils::SuiNetwork,
    wallet::Wallet,
};
use walrus_test_utils::WithTempDir;

use super::blob::{BlobData, QuiltData, QuiltStoreBlobConfig, WriteBlobConfig};

/// Client for writing test blobs to storage nodes
#[derive(Debug)]
pub(crate) struct WriteClient {
    client: WithTempDir<Client<SuiContractClient>>,
    blob: BlobData,
    quilt_pool: QuiltData,
    metrics: Arc<ClientMetrics>,
}

impl WriteClient {
    /// Creates a new WriteClient with the given configuration
    #[allow(clippy::too_many_arguments)]
    #[tracing::instrument(err, skip_all)]
    pub async fn new(
        config: &ClientConfig,
        network: &SuiNetwork,
        gas_budget: Option<u64>,
        blob_config: WriteBlobConfig,
        quilt_config: QuiltStoreBlobConfig,
        refresher_handle: CommitteesRefresherHandle,
        refiller: Refiller,
        metrics: Arc<ClientMetrics>,
    ) -> anyhow::Result<Self> {
        let blob = BlobData::random(StdRng::from_entropy(), blob_config.clone()).await;
        let client = new_client(config, network, gas_budget, refresher_handle, refiller).await?;
        Ok(Self {
            client,
            blob,
            quilt_pool: QuiltData::new(quilt_config, blob_config),
            metrics,
        })
    }

    async fn handle_read_result(
        &self,
        selected_blobs: &[&QuiltStoreBlob<'_>],
        quilt_blobs: &[QuiltStoreBlob<'static>],
        quilt_store_result: &QuiltStoreResult,
    ) -> Result<(), ClientError> {
        for read_blob in quilt_blobs.iter() {
            if let Some(original_blob) = selected_blobs
                .iter()
                .find(|b| b.identifier() == read_blob.identifier())
            {
                if read_blob.data() != original_blob.data() {
                    self.dump_mismatch_data(selected_blobs, quilt_blobs, quilt_store_result)
                        .await;
                }
            } else {
                self.dump_mismatch_data(selected_blobs, quilt_blobs, quilt_store_result)
                    .await;
            }
        }
        Ok(())
    }

    async fn read_random_quilt_by_identifier(
        &self,
        quilt_store_result: &QuiltStoreResult,
        quilt_store_blobs: &[QuiltStoreBlob<'_>],
        metrics: &ClientMetrics,
    ) -> Result<(), ClientError> {
        let quilt_client = self
            .client
            .as_ref()
            .quilt_client(QuiltClientConfig::default());

        let mut rng = StdRng::from_entropy();
        let num_to_read = rng.gen_range(1..=3);

        let quilt_id = quilt_store_result
            .blob_store_result
            .blob_id()
            .expect("blob id should be present");
        let selected_blobs = quilt_store_blobs
            .choose_multiple(&mut rng, num_to_read)
            .collect::<Vec<&QuiltStoreBlob<'_>>>();
        let start = Instant::now();
        let read_result = quilt_client
            .get_blobs_by_identifiers(
                &quilt_id,
                &selected_blobs
                    .iter()
                    .map(|blob| blob.identifier())
                    .collect::<Vec<_>>(),
            )
            .await;
        let duration = start.elapsed();
        metrics.observe_quilt_read_latency(duration, num_to_read, read_result.is_ok());

        if let Ok(quilt_blobs) = read_result {
            self.handle_read_result(&selected_blobs, &quilt_blobs, quilt_store_result)
                .await?;
        }

        Ok(())
    }

    /// Returns the active address of the client.
    pub fn address(&mut self) -> SuiAddress {
        self.client.as_mut().sui_client_mut().address()
    }

    pub async fn write_fresh_quilt(
        &mut self,
        metrics: &ClientMetrics,
    ) -> Result<(BlobId, Duration), ClientError> {
        let now = Instant::now();
        let quilt_data = self.quilt_pool.get_random_batch();

        let max_string_length = 100;
        let include_tags = rand::thread_rng().gen_bool(0.5);
        let max_num_tags = rand::thread_rng().gen_range(1..5);
        let quilt_store_blobs = generate_random_quilt_store_blobs(
            &quilt_data,
            max_string_length,
            include_tags,
            max_num_tags,
        );

        // Calculate total size.
        let total_size: usize = quilt_store_blobs.iter().map(|blob| blob.data().len()).sum();
        let num_blobs = quilt_store_blobs.len();

        let result = self.write_quilt(&quilt_store_blobs).await?;
        let duration = now.elapsed();

        // Record the metric.
        metrics.observe_quilt_upload_latency(duration, num_blobs, total_size);

        // Select random quilt patch IDs and read them back.
        if !result.stored_quilt_blobs.is_empty() {
            self.read_random_quilt_by_identifier(&result, &quilt_store_blobs, metrics)
                .await?;
        }

        Ok((
            result
                .blob_store_result
                .blob_id()
                .expect("blob id should be present"),
            duration,
        ))
    }

    pub async fn write_quilt(
        &self,
        quilt_store_blobs: &[QuiltStoreBlob<'_>],
    ) -> Result<QuiltStoreResult, ClientError> {
        let quilt_client = self
            .client
            .as_ref()
            .quilt_client(QuiltClientConfig::default());
        let quilt = quilt_client
            .construct_quilt::<QuiltVersionV1>(quilt_store_blobs, DEFAULT_ENCODING)
            .await?;
        let result = self
            .client
            .as_ref()
            .quilt_client(QuiltClientConfig::default())
            .reserve_and_store_quilt::<QuiltVersionV1>(
                &quilt,
                DEFAULT_ENCODING,
                self.blob.epochs_to_store(),
                StoreOptimizations::none(),
                BlobPersistence::Permanent,
                PostStoreAction::Keep,
            )
            .await?;

        tracing::info!(
            blob_id = ?result.blob_store_result.blob_id(),
            num_blobs = quilt_store_blobs.len(),
            "stored quilt successfully"
        );

        Ok(result)
    }

    /// Dumps mismatch data to files for debugging.
    async fn dump_mismatch_data(
        &self,
        original_blobs: &[&QuiltStoreBlob<'_>],
        read_blobs: &[QuiltStoreBlob<'static>],
        result: &QuiltStoreResult,
    ) {
        use std::fs;

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("should be able to get duration since UNIX_EPOCH")
            .as_secs();

        let dump_dir = format!("quilt_mismatch_{}", timestamp);
        if let Err(e) = fs::create_dir_all(&dump_dir) {
            tracing::error!("Failed to create dump directory: {}", e);
            return;
        }

        // Dump original quilt store blobs
        for original_blob in original_blobs.iter() {
            let file_path = format!("{}/original_{}.bin", dump_dir, original_blob.identifier());
            if let Err(e) = fs::write(&file_path, original_blob.data()) {
                tracing::error!(
                    "Failed to write original blob '{}': {}",
                    original_blob.identifier(),
                    e
                );
            }
        }

        // Dump read blobs
        for read_blob in read_blobs.iter() {
            let file_path = format!("{}/read_{}.bin", dump_dir, read_blob.identifier());
            if let Err(e) = fs::write(&file_path, read_blob.data()) {
                tracing::error!(
                    "Failed to write read blob '{}': {}",
                    read_blob.identifier(),
                    e
                );
            }
        }

        // Dump metadata
        let mut metadata_content = String::new();
        metadata_content.push_str(&format!(
            "Quilt Blob ID: {:?}\n",
            result.blob_store_result.blob_id()
        ));
        metadata_content.push_str(&format!("Original Blob Count: {}\n", original_blobs.len()));
        metadata_content.push_str(&format!("Read Blob Count: {}\n", read_blobs.len()));
        metadata_content.push_str("Stored Quilt Blobs:\n");

        for stored_blob in result.stored_quilt_blobs.iter() {
            metadata_content.push_str(&format!(
                "  identifier='{}', quilt_patch_id={}\n",
                stored_blob.identifier, stored_blob.quilt_patch_id
            ));
        }

        let metadata_path = format!("{}/metadata.txt", dump_dir);
        if let Err(e) = fs::write(&metadata_path, metadata_content) {
            tracing::error!("Failed to write metadata: {}", e);
        }

        tracing::error!("Quilt mismatch data dumped to directory: {}", dump_dir);
    }

    /// Stores a fresh consistent blob and returns the blob id and elapsed time.
    pub async fn write_fresh_blob(&mut self) -> Result<(BlobId, Duration), ClientError> {
        self.write_fresh_blob_with_epochs(None).await
    }

    /// Stores a fresh blob and returns the blob id and elapsed time.
    ///
    /// If `epochs_to_store` is not provided, the blob will be stored for the number of epochs
    /// randomly chosen between `min_epochs_to_store` and `max_epochs_to_store` specified in the
    /// blob config.
    pub async fn write_fresh_blob_with_epochs(
        &mut self,
        epochs_to_store: Option<EpochCount>,
    ) -> Result<(BlobId, Duration), ClientError> {
        // Refresh the blob data, and get a new random number of epochs to store.
        self.blob.refresh();
        let blob = self.blob.random_size_slice();
        let epochs_to_store = epochs_to_store.unwrap_or(self.blob.epochs_to_store());

        let now = Instant::now();
        let blob_id = self
            .client
            .as_ref()
            // TODO(giac): add also some deletable blobs in the mix (#800).
            .reserve_and_store_blobs_retry_committees(
                &[blob],
                DEFAULT_ENCODING,
                epochs_to_store,
                StoreOptimizations::none(),
                BlobPersistence::Permanent,
                PostStoreAction::Keep,
                Some(&self.metrics),
            )
            .await?
            .first()
            .expect("should have one blob store result")
            .blob_id()
            .expect("blob id should be present");

        tracing::info!(
            duration = now.elapsed().as_secs(),
            ?blob_id,
            epochs_to_store,
            "wrote blob to store",
        );
        Ok((blob_id, now.elapsed()))
    }

    /// Stores a fresh blob that is inconsistent in primary sliver 0 and returns
    /// the blob id and elapsed time.
    pub async fn write_fresh_inconsistent_blob(
        &mut self,
    ) -> Result<(BlobId, Duration), ClientError> {
        self.blob.refresh();
        let blob = self.blob.random_size_slice();
        let now = Instant::now();
        let blob_id = self
            .reserve_and_store_inconsistent_blob(blob, self.blob.epochs_to_store())
            .await?;
        Ok((blob_id, now.elapsed()))
    }

    /// Stores an inconsistent blob.
    ///
    /// If there are enough storage nodes to achieve a quorum even without two nodes, the blob
    /// will be inconsistent in two slivers, s.t. each of them is held by a different storage
    /// node, if the shards are distributed equally and assigned sequentially.
    async fn reserve_and_store_inconsistent_blob(
        &self,
        blob: &[u8],
        epochs_to_store: EpochCount,
    ) -> Result<BlobId, ClientError> {
        // Encode the blob with false metadata for one shard.
        let (pairs, metadata) = self
            .client
            .as_ref()
            .encoding_config()
            .get_for_type(DEFAULT_ENCODING)
            .encode_with_metadata(blob)
            .map_err(ClientError::other)?;

        let mut metadata = metadata.metadata().to_owned();
        let n_members = self
            .client
            .as_ref()
            .sui_client()
            .read_client
            .current_committee()
            .await?
            .n_members();
        let n_shards = self.client.as_ref().encoding_config().n_shards();

        // Make primary sliver 0 inconsistent.
        metadata.mut_inner().hashes[0].primary_hash = Node::Digest([0; 32]);

        // Make second sliver inconsistent if enough committee members
        if n_members >= 7 {
            // Sliver `n_shards/2` will be held by a different node if the shards are assigned
            // sequentially.
            metadata.mut_inner().hashes[(n_shards.get() / 2) as usize].primary_hash =
                Node::Digest([0; 32]);
        }

        let blob_id = BlobId::from_sliver_pair_metadata(&metadata);
        let metadata = VerifiedBlobMetadataWithId::new_verified_unchecked(blob_id, metadata);

        tracing::info!("writing inconsistent blob {blob_id} to store {epochs_to_store} epochs",);

        // Output the shard index storing the inconsistent sliver.
        tracing::debug!(
            "Shard index for inconsistent sliver: {}",
            SliverPairIndex::new(0)
                .to_shard_index(self.client.as_ref().encoding_config().n_shards(), &blob_id)
        );

        // Register blob.
        let committees = self.client.as_ref().get_committees().await?;
        let (blob_sui_object, _operation) = self
            .client
            .as_ref()
            .resource_manager(&committees)
            .await
            .get_existing_or_register(
                &[&metadata],
                epochs_to_store,
                BlobPersistence::Permanent,
                StoreOptimizations::none(),
            )
            .await?
            .into_iter()
            .next()
            .expect("should register exactly one blob");

        // Wait to ensure that the storage nodes received the registration event.
        tokio::time::sleep(Duration::from_secs(1)).await;

        let certificate = self
            .client
            .as_ref()
            .send_blob_data_and_get_certificate(
                &metadata,
                &pairs,
                &blob_sui_object.blob_persistence_type(),
                &MultiProgress::new(),
            )
            .await?;

        self.client
            .as_ref()
            .sui_client()
            .certify_blobs(&[(&blob_sui_object, certificate)], PostStoreAction::Burn)
            .await?;

        Ok(blob_id)
    }
}

/// Creates a new client with a separate wallet.
async fn new_client(
    config: &ClientConfig,
    network: &SuiNetwork,
    gas_budget: Option<u64>,
    refresher_handle: CommitteesRefresherHandle,
    refiller: Refiller,
) -> anyhow::Result<WithTempDir<Client<SuiContractClient>>> {
    // Create the client with a separate wallet
    let wallet = wallet_for_testing_from_refill(config, network, refiller).await?;
    let rpc_urls = &[wallet.as_ref().get_rpc_url()?];
    let sui_client = RetriableSuiClient::new(
        rpc_urls
            .iter()
            .map(|rpc_url| {
                LazySuiClientBuilder::new(
                    rpc_url,
                    config.communication_config.sui_client_request_timeout,
                )
            })
            .collect(),
        Default::default(),
    )
    .await?;
    let sui_read_client = config.new_read_client(sui_client).await?;
    let sui_contract_client = wallet.and_then(|wallet| {
        SuiContractClient::new_with_read_client(wallet, gas_budget, Arc::new(sui_read_client))
    })?;

    let client = sui_contract_client
        .and_then_async(|contract_client| {
            Client::new_contract_client(config.clone(), refresher_handle, contract_client)
        })
        .await?;
    Ok(client)
}

/// Creates a new wallet for testing and fills it with gas and WAL tokens
pub async fn wallet_for_testing_from_refill(
    config: &ClientConfig,
    network: &SuiNetwork,
    refiller: Refiller,
) -> anyhow::Result<WithTempDir<Wallet>> {
    let mut wallet = temp_dir_wallet(
        config.communication_config.sui_client_request_timeout,
        network.env(),
    )?;
    let address = wallet.as_mut().active_address()?;
    refiller.send_gas_request(address).await?;
    refiller.send_wal_request(address).await?;
    Ok(wallet)
}

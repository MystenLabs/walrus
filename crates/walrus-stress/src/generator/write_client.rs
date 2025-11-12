// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use indicatif::MultiProgress;
use rand::{SeedableRng, rngs::StdRng, thread_rng};
use sui_sdk::types::base_types::SuiAddress;
use walrus_core::{
    BlobId,
    DEFAULT_ENCODING,
    EpochCount,
    SliverPairIndex,
    encoding::EncodingFactory as _,
    merkle::Node,
    metadata::VerifiedBlobMetadataWithId,
};
use walrus_sdk::{
    client::{
        StoreArgs,
        WalrusNodeClient,
        metrics::ClientMetrics,
        refresh::CommitteesRefresherHandle,
    },
    error::ClientError,
    store_optimizations::StoreOptimizations,
    uploader::TailHandling,
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
    types::move_structs::BlobWithAttribute,
    utils::SuiNetwork,
    wallet::Wallet,
};
use walrus_test_utils::WithTempDir;

use super::blob::{BlobData, WriteBlobConfig};

/// Client for writing test blobs to storage nodes
#[derive(Debug)]
pub(crate) struct WriteClient {
    client: WithTempDir<WalrusNodeClient<SuiContractClient>>,
    blob: BlobData,
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
        refresher_handle: CommitteesRefresherHandle,
        refiller: Refiller,
        metrics: Arc<ClientMetrics>,
    ) -> anyhow::Result<Self> {
        let blob = BlobData::random(
            StdRng::from_rng(thread_rng()).expect("rng should be seedable from thread_rng"),
            blob_config,
        )
        .await;
        let client = new_client(config, network, gas_budget, refresher_handle, refiller).await?;
        Ok(Self {
            client,
            blob,
            metrics,
        })
    }

    /// Returns the active address of the client.
    pub fn address(&mut self) -> SuiAddress {
        self.client.as_mut().sui_client_mut().address()
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
        let store_args = StoreArgs::default_with_epochs(epochs_to_store)
            .no_store_optimizations()
            .with_metrics(self.metrics.clone());

        let blob_id = self
            .client
            .as_ref()
            // TODO(giac): add also some deletable blobs in the mix (#800).
            .reserve_and_store_blobs_retry_committees(&[blob], &[], &store_args)
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
            .get_existing_or_register(
                &[&metadata],
                epochs_to_store,
                BlobPersistence::Permanent,
                StoreOptimizations::all(),
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
                Arc::new(pairs),
                &blob_sui_object.blob_persistence_type(),
                Some(&MultiProgress::new()),
                TailHandling::Blocking,
                None,
                None,
            )
            .await?;

        let blob_with_attr = BlobWithAttribute {
            blob: blob_sui_object,
            attribute: None,
        };
        self.client
            .as_ref()
            .sui_client()
            .certify_blobs(&[(&blob_with_attr, certificate)], PostStoreAction::Burn)
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
) -> anyhow::Result<WithTempDir<WalrusNodeClient<SuiContractClient>>> {
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
    )?;
    let sui_read_client = config.new_read_client(sui_client).await?;
    let sui_contract_client = wallet.and_then(|wallet| {
        SuiContractClient::new_with_read_client(wallet, gas_budget, Arc::new(sui_read_client))
    })?;

    let client = sui_contract_client
        .and_then_async(|contract_client| {
            WalrusNodeClient::new_contract_client(config.clone(), refresher_handle, contract_client)
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
    )
    .await?;
    let address = wallet.as_mut().active_address()?;
    refiller.send_gas_request(address).await?;
    refiller.send_wal_request(address).await?;
    Ok(wallet)
}

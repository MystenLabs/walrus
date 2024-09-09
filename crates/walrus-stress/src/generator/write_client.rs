// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::time::{Duration, Instant};

use rand::{rngs::StdRng, thread_rng, SeedableRng};
use sui_sdk::{types::base_types::SuiAddress, wallet_context::WalletContext};
use tracing::instrument;
use walrus_core::{merkle::Node, metadata::VerifiedBlobMetadataWithId, BlobId, SliverPairIndex};
use walrus_service::client::{Client, ClientError, Config};
use walrus_sui::{
    client::{ContractClient, ReadClient, SuiContractClient, SuiReadClient},
    test_utils::temp_dir_wallet,
    types::Blob,
    utils::SuiNetwork,
};
use walrus_test_utils::WithTempDir;

use super::blob::BlobData;
use crate::refill::{GasRefill, Refiller};

#[derive(Debug)]
pub(crate) struct WriteClient {
    client: WithTempDir<Client<SuiContractClient>>,
    blob: BlobData,
}

impl WriteClient {
    #[instrument(err, skip_all)]
    pub async fn new<G: GasRefill + 'static>(
        config: &Config,
        network: &SuiNetwork,
        gas_budget: u64,
        min_size_log2: u8,
        max_size_log2: u8,
        refiller: Refiller<G>,
    ) -> anyhow::Result<Self> {
        let blob = BlobData::random(
            StdRng::from_rng(thread_rng()).expect("rng should be seedable from thread_rng"),
            min_size_log2,
            max_size_log2,
        )
        .await;
        let client = new_client(config, network, gas_budget, refiller).await?;
        Ok(Self { client, blob })
    }

    /// Returns the active address of the client.
    pub fn address(&mut self) -> SuiAddress {
        self.client.as_mut().sui_client_mut().address()
    }

    /// Stores a fresh consistent blob and returns the blob id and elapsed time.
    pub async fn write_fresh_blob(&mut self) -> Result<(BlobId, Duration), ClientError> {
        let blob = self.blob.refresh_and_get_random_slice();
        let now = Instant::now();
        let blob_id = self
            .client
            .as_ref()
            .reserve_and_store_blob(blob, 1, true)
            .await?
            .blob_id()
            .to_owned();
        let elapsed = now.elapsed();
        Ok((blob_id, elapsed))
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
            .reserve_and_store_inconsistent_blob(blob)
            .await?
            .blob_id;
        let elapsed = now.elapsed();
        Ok((blob_id, elapsed))
    }

    /// Stores an inconsistent blob.
    ///
    /// If there are enough storage nodes to achieve a quorum even without two nodes, the blob
    /// will be inconsistent in two slivers, s.t. each of them is held by a different storage
    /// node, if the shards are distributed equally and assigned sequentially.
    async fn reserve_and_store_inconsistent_blob(&self, blob: &[u8]) -> Result<Blob, ClientError> {
        let epochs = 1;
        // Encode the blob with false metadata for one shard.
        let (pairs, metadata) = self
            .client
            .as_ref()
            .encoding_config()
            .get_blob_encoder(blob)
            .map_err(ClientError::other)?
            .encode_with_metadata();
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
        metadata.hashes[0].primary_hash = Node::Digest([0; 32]);
        // If the committee has 7 members, make a second sliver inconsistent.
        if n_members >= 7 {
            // Sliver `n_shards/2` will be held by a different node if the shards are assigned
            // sequentially.
            metadata.hashes[(n_shards.get() / 2) as usize].primary_hash = Node::Digest([0; 32]);
        }

        let blob_id = BlobId::from_sliver_pair_metadata(&metadata);
        let metadata = VerifiedBlobMetadataWithId::new_verified_unchecked(blob_id, metadata);

        // Output the shard index storing the inconsistent sliver.
        tracing::debug!(
            "Shard index for inconsistent sliver: {}",
            SliverPairIndex::new(0)
                .to_shard_index(self.client.as_ref().encoding_config().n_shards(), &blob_id)
        );

        // Register blob.
        let blob_sui_object = self
            .client
            .as_ref()
            .reserve_and_register_blob(&metadata, epochs, false)
            .await?;

        // Wait to ensure that the storage nodes received the registration event.
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Certify blob.
        let certificate = self
            .client
            .as_ref()
            .store_metadata_and_pairs(&metadata, &pairs)
            .await?;

        let blob = self
            .client
            .as_ref()
            .sui_client()
            .certify_blob(blob_sui_object, &certificate)
            .await?;
        Ok(blob)
    }
}

async fn new_client<G: GasRefill + 'static>(
    config: &Config,
    network: &SuiNetwork,
    gas_budget: u64,
    refiller: Refiller<G>,
) -> anyhow::Result<WithTempDir<Client<SuiContractClient>>> {
    // Create the client with a separate wallet
    let wallet = wallet_for_testing_from_refill(network, refiller).await?;
    let sui_read_client = SuiReadClient::new(
        wallet.as_ref().get_client().await?,
        config.system_object,
        config.staking_object,
    )
    .await?;
    let sui_contract_client = wallet.and_then(|wallet| {
        SuiContractClient::new_with_read_client(wallet, gas_budget, sui_read_client)
    })?;

    let client = sui_contract_client
        .and_then_async(|contract_client| Client::new(config.clone(), contract_client))
        .await?;
    Ok(client)
}

pub async fn wallet_for_testing_from_refill<G: GasRefill + 'static>(
    network: &SuiNetwork,
    refiller: Refiller<G>,
) -> anyhow::Result<WithTempDir<WalletContext>> {
    let mut wallet = temp_dir_wallet(network.env())?;
    let address = wallet.as_mut().active_address()?;
    refiller.send_gas_request(address).await?;
    refiller.send_gas_request(address).await?;
    Ok(wallet)
}

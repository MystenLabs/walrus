// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{future::Future, pin::pin, time::Duration};

use anyhow::{anyhow, Context};
use fastcrypto::traits::Signer;
use mysten_metrics::RegistryService;
use sui_sdk::SuiClientBuilder;
use tokio::select;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::instrument;
use typed_store::{rocks::MetricConf, DBMetrics};
use walrus_core::{
    encoding::{EncodingConfig, RecoveryError},
    ensure,
    merkle::MerkleProof,
    messages::{Confirmation, SignedStorageConfirmation, StorageConfirmation},
    metadata::{SliverIndex, SliverPairIndex, UnverifiedBlobMetadataWithId, VerificationError},
    BlobId,
    DecodingSymbol,
    Epoch,
    ProtocolKeyPair,
    ShardIndex,
    Sliver,
    SliverType,
};
use walrus_sui::{
    client::{ReadClient, SuiReadClient},
    types::{BlobEvent, EventType},
};

use crate::{config::StorageNodeConfig, mapping::shard_index_for_pair, storage::Storage};

#[derive(Debug, thiserror::Error)]
pub enum StoreMetadataError {
    #[error(transparent)]
    InvalidMetadata(#[from] VerificationError),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
    #[error("metadata was already stored")]
    AlreadyStored,
    #[error("blob for this metadata has already expired")]
    BlobExpired,
    #[error("blob for this metadata has not been registered")]
    NotRegistered,
}

#[derive(Debug, thiserror::Error)]
pub enum RetrieveSliverError {
    #[error("Invalid shard {0:?}")]
    InvalidShard(ShardIndex),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum RetrieveSymbolError {
    #[error("Invalid shard {0:?}")]
    InvalidShard(ShardIndex),
    #[error("Symbol recovery failed for sliver {0:?}, index {0:?} in blob {2:?}")]
    RecoveryError(SliverPairIndex, SliverIndex, BlobId),
    #[error("Sliver {0:?} unavailable for recovery in blob {1:?}")]
    UnavailableSliver(SliverPairIndex, BlobId),

    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl From<RetrieveSliverError> for RetrieveSymbolError {
    fn from(value: RetrieveSliverError) -> Self {
        match value {
            RetrieveSliverError::InvalidShard(s) => Self::InvalidShard(s),
            RetrieveSliverError::Internal(e) => Self::Internal(e),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StoreSliverError {
    #[error("Missing metadata for {0:?}")]
    MissingMetadata(BlobId),
    #[error("Invalid {0} for {1:?}")]
    InvalidSliverPairId(SliverPairIndex, BlobId),
    #[error("Invalid {0} for {1:?}")]
    InvalidSliver(SliverPairIndex, BlobId),
    #[error("Invalid sliver size {0} for {1:?}")]
    IncorrectSize(usize, BlobId),
    #[error("Invalid shard type {0:?} for {1:?}")]
    InvalidSliverType(SliverType, BlobId),
    #[error("Invalid shard {0:?}")]
    InvalidShard(ShardIndex),
    #[error(transparent)]
    MalformedSliver(#[from] RecoveryError),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

pub trait ServiceState {
    /// Retrieves the metadata associated with a blob.
    fn retrieve_metadata(
        &self,
        blob_id: &BlobId,
    ) -> Result<Option<UnverifiedBlobMetadataWithId>, anyhow::Error>;

    /// Stores the metadata associated with a blob.
    fn store_metadata(
        &self,
        metadata: UnverifiedBlobMetadataWithId,
    ) -> Result<(), StoreMetadataError>;

    /// Retrieves a primary or secondary sliver for a blob for a shard held by this storage node.
    fn retrieve_sliver(
        &self,
        blob_id: &BlobId,
        sliver_pair_idx: SliverPairIndex,
        sliver_type: SliverType,
    ) -> Result<Option<Sliver>, RetrieveSliverError>;

    /// Store the primary or secondary encoding for a blob for a shard held by this storage node.
    fn store_sliver(
        &self,
        blob_id: &BlobId,
        sliver_pair_idx: SliverPairIndex,
        sliver: &Sliver,
    ) -> Result<(), StoreSliverError>;

    /// Get a signed confirmation over the identifiers of the shards storing their respective
    /// sliver-pairs for their BlobIds.
    fn compute_storage_confirmation(
        &self,
        blob_id: &BlobId,
    ) -> impl Future<Output = Result<Option<StorageConfirmation>, anyhow::Error>> + Send;

    /// Retrieves a recovery symbol for a shard held by this storage node.
    ///
    /// # Arguments:
    ///
    /// * `sliver_type` - The target type of the sliver that will be recovered
    /// * `sliver_pair_idx` - The index of the sliver pair that we want to access
    /// * `index` - The pair index of the sliver to be recovered.
    ///
    /// Returns the recovery symbol for the requested sliver.
    fn retrieve_recovery_symbol(
        &self,
        blob_id: &BlobId,
        sliver_pair_idx: SliverPairIndex,
        sliver_type: SliverType,
        index: SliverIndex,
    ) -> Result<DecodingSymbol<MerkleProof>, RetrieveSymbolError>;
}

/// A Walrus storage node, responsible for 1 or more shards on Walrus.
#[derive(Debug)]
pub struct StorageNode<T> {
    current_epoch: Epoch,
    storage: Storage,
    encoding_config: EncodingConfig,
    protocol_key_pair: ProtocolKeyPair,
    sui_read_client: T,
    event_polling_interval: Duration,
}

impl StorageNode<SuiReadClient> {
    /// Create a new storage node with the provided configuration.
    pub async fn new(
        config: &StorageNodeConfig,
        registry_service: RegistryService,
        encoding_config: EncodingConfig,
    ) -> anyhow::Result<Self> {
        DBMetrics::init(&registry_service.default_registry());
        let storage = Storage::open(config.storage_path.as_path(), MetricConf::new("storage"))?;
        let protocol_key_pair = config
            .protocol_key_pair
            .get()
            .expect("protocol keypair must already be loaded");

        let sui_config = config
            .sui
            .as_ref()
            .ok_or_else(|| anyhow!("sui config must be present"))?;

        let sui_client = SuiClientBuilder::default().build(&sui_config.rpc).await?;
        let sui_read_client =
            SuiReadClient::new(sui_client, sui_config.pkg_id, sui_config.system_object).await?;

        Ok(Self::new_with_storage(
            storage,
            encoding_config,
            protocol_key_pair.clone(),
            sui_read_client,
            sui_config.event_polling_interval,
        ))
    }
}

impl<T> StorageNode<T>
where
    T: ReadClient,
{
    /// Create a new storage node providing the storage directly.
    pub fn new_with_storage(
        storage: Storage,
        encoding_config: EncodingConfig,
        protocol_key_pair: ProtocolKeyPair,
        sui_read_client: T,
        event_polling_interval: Duration,
    ) -> Self {
        Self {
            storage,
            current_epoch: 0,
            encoding_config,
            protocol_key_pair,
            sui_read_client,
            event_polling_interval,
        }
    }
}

impl<T> StorageNode<T>
where
    T: ReadClient,
{
    /// Run the walrus-node logic until cancelled using the provided cancellation token.
    pub async fn run(&self, cancel_token: CancellationToken) -> anyhow::Result<()> {
        // TODO(jsmith): Run any subtasks such as the HTTP api, shard migration,
        // etc until cancelled.

        let mut blob_events = pin!(self.combined_blob_events().await?);
        loop {
            select! {
                blob_event = blob_events.next() => {
                    let event = blob_event.ok_or_else(
                        || anyhow!("event stream for blob events stopped")
                    )?;
                    self.storage.update_blob_info(event)?;
                }
                () = cancel_token.cancelled() => break,
            };
        }
        Ok(())
    }

    #[instrument(skip_all)]
    async fn combined_blob_events(
        &self,
    ) -> anyhow::Result<impl tokio_stream::Stream<Item = BlobEvent> + '_> {
        let registered_cursor = self.storage.get_event_cursor(EventType::Registered)?;
        tracing::info!("resuming from registered event: {registered_cursor:?}");
        let registered_events = self
            .sui_read_client
            .blob_registered_events(self.event_polling_interval, registered_cursor)
            .await?
            .map(BlobEvent::from);
        let certified_cursor = self.storage.get_event_cursor(EventType::Certified)?;
        tracing::info!("resuming from certified event: {certified_cursor:?}");
        let certified_events = self
            .sui_read_client
            .blob_certified_events(self.event_polling_interval, certified_cursor)
            .await?
            .map(BlobEvent::from);
        Ok(registered_events.merge(certified_events))
    }
}

impl<T> ServiceState for StorageNode<T>
where
    T: Sync + Send,
{
    fn retrieve_metadata(
        &self,
        blob_id: &BlobId,
    ) -> Result<Option<UnverifiedBlobMetadataWithId>, anyhow::Error> {
        let verified_metadata_with_id = self
            .storage
            .get_metadata(blob_id)
            .context("unable to retrieve metadata")?;
        // Format the metadata as unverified, as the client will have to re-verify them.
        Ok(verified_metadata_with_id.map(|metadata| metadata.into_unverified()))
    }

    fn store_metadata(
        &self,
        metadata: UnverifiedBlobMetadataWithId,
    ) -> Result<(), StoreMetadataError> {
        let Some(blob_info) = self
            .storage
            .get_blob_info(metadata.blob_id())
            .map_err(|err| anyhow!("could not retrieve blob info: {}", err))?
        else {
            return Err(StoreMetadataError::NotRegistered);
        };
        if blob_info.end_epoch <= self.current_epoch {
            return Err(StoreMetadataError::BlobExpired);
        }

        let verified_metadata_with_id = metadata.verify(&self.encoding_config)?;
        self.storage
            .put_verified_metadata(&verified_metadata_with_id)
            .context("unable to store metadata")?;
        Ok(())
    }

    fn retrieve_sliver(
        &self,
        blob_id: &BlobId,
        sliver_pair_idx: SliverPairIndex,
        sliver_type: SliverType,
    ) -> Result<Option<Sliver>, RetrieveSliverError> {
        let shard = shard_index_for_pair(
            sliver_pair_idx,
            self.encoding_config.n_shards() as usize,
            blob_id,
        );
        let sliver = self
            .storage
            .shard_storage(shard)
            .ok_or_else(|| RetrieveSliverError::InvalidShard(shard))?
            .get_sliver(blob_id, sliver_type)
            .context("unable to retrieve sliver")?;
        Ok(sliver)
    }

    fn store_sliver(
        &self,
        blob_id: &BlobId,
        sliver_pair_idx: SliverPairIndex,
        sliver: &Sliver,
    ) -> Result<(), StoreSliverError> {
        // First determine if the shard that should store this sliver is managed by this node.
        // If not, we can return early without touching the database.
        let shard = shard_index_for_pair(
            sliver_pair_idx,
            self.encoding_config.n_shards() as usize,
            blob_id,
        );
        let shard_storage = self
            .storage
            .shard_storage(shard)
            .ok_or_else(|| StoreSliverError::InvalidShard(shard))?;

        // Ensure we already received metadata for this sliver.
        let metadata = self
            .storage
            .get_metadata(blob_id)
            .context("unable to retrieve metadata")?
            .ok_or_else(|| StoreSliverError::MissingMetadata(*blob_id))?;

        // Ensure the received sliver has the expected size.
        let blob_size = metadata
            .metadata()
            .unencoded_length
            .try_into()
            .expect("The maximum blob size is smaller than `usize::MAX`");
        ensure!(
            sliver.has_correct_length(&self.encoding_config, blob_size),
            StoreSliverError::IncorrectSize(sliver.len(), *blob_id)
        );

        // Ensure the received sliver matches the metadata we have in store.
        let stored_sliver_hash = metadata
            .metadata()
            .get_sliver_hash(sliver_pair_idx, sliver.r#type())
            .ok_or_else(|| StoreSliverError::InvalidSliverPairId(sliver_pair_idx, *blob_id))?;

        let computed_sliver_hash = sliver.hash(&self.encoding_config)?;
        ensure!(
            &computed_sliver_hash == stored_sliver_hash,
            StoreSliverError::InvalidSliver(sliver_pair_idx, *blob_id)
        );

        // Finally store the sliver in the appropriate shard storage.
        shard_storage
            .put_sliver(blob_id, sliver)
            .context("unable to store sliver")?;

        Ok(())
    }

    async fn compute_storage_confirmation(
        &self,
        blob_id: &BlobId,
    ) -> Result<Option<StorageConfirmation>, anyhow::Error> {
        if self.storage.is_stored_at_all_shards(blob_id)? {
            let confirmation = Confirmation::new(self.current_epoch, *blob_id);
            sign_confirmation(confirmation, self.protocol_key_pair.clone())
                .await
                .map(|signed| Some(StorageConfirmation::Signed(signed)))
        } else {
            Ok(None)
        }
    }

    //TODO (lef): Add proof in symbol recovery
    fn retrieve_recovery_symbol(
        &self,
        blob_id: &BlobId,
        sliver_pair_idx: SliverPairIndex,
        sliver_type: SliverType,
        index: SliverIndex,
    ) -> Result<DecodingSymbol<MerkleProof>, RetrieveSymbolError> {
        let optional_sliver =
            self.retrieve_sliver(blob_id, sliver_pair_idx, sliver_type.orthogonal())?;
        let Some(sliver) = optional_sliver else {
            return Err(RetrieveSymbolError::UnavailableSliver(
                sliver_pair_idx,
                *blob_id,
            ));
        };

        Ok(match sliver {
            Sliver::Primary(inner) => {
                let symbol = inner
                    .recovery_symbol_for_sliver_with_proof(index, &self.encoding_config)
                    .map_err(|_| {
                        RetrieveSymbolError::RecoveryError(index, sliver_pair_idx, *blob_id)
                    })?;
                DecodingSymbol::Secondary(symbol)
            }
            Sliver::Secondary(inner) => {
                let symbol = inner
                    .recovery_symbol_for_sliver_with_proof(index, &self.encoding_config)
                    .map_err(|_| {
                        RetrieveSymbolError::RecoveryError(index, sliver_pair_idx, *blob_id)
                    })?;
                DecodingSymbol::Primary(symbol)
            }
        })
    }
}

async fn sign_confirmation(
    confirmation: Confirmation,
    signer: ProtocolKeyPair,
) -> Result<SignedStorageConfirmation, anyhow::Error> {
    let signed = tokio::task::spawn_blocking(move || {
        let encoded_confirmation = bcs::to_bytes(&confirmation)
            .expect("bcs encoding a confirmation to a vector should not fail");

        SignedStorageConfirmation {
            signature: signer.as_ref().sign(&encoded_confirmation),
            confirmation: encoded_confirmation,
        }
    })
    .await
    .context("unexpected error while signing a confirmation")?;

    Ok(signed)
}

#[cfg(test)]
mod tests {
    use fastcrypto::{bls12381::min_pk::BLS12381KeyPair, traits::KeyPair};
    use walrus_sui::test_utils::MockSuiReadClient;
    use walrus_test_utils::{Result as TestResult, WithTempDir};

    use super::*;
    use crate::storage::tests::{
        populated_storage,
        WhichSlivers,
        BLOB_ID,
        OTHER_SHARD_INDEX,
        SHARD_INDEX,
    };

    const OTHER_BLOB_ID: BlobId = BlobId([247; 32]);

    fn storage_node_with_storage(
        storage: WithTempDir<Storage>,
    ) -> WithTempDir<StorageNode<MockSuiReadClient>> {
        let signing_key = BLS12381KeyPair::generate(&mut rand::thread_rng());
        let sui_read_client = MockSuiReadClient::default();
        let polling_interval = Duration::from_millis(1);
        storage.map(|storage| {
            StorageNode::new_with_storage(
                storage,
                walrus_core::test_utils::encoding_config(),
                signing_key.into(),
                sui_read_client,
                polling_interval,
            )
        })
    }

    mod get_storage_confirmation {
        use fastcrypto::traits::VerifyingKey;

        use super::*;

        #[tokio::test]
        async fn returns_none_if_no_shards_store_pairs() -> TestResult {
            let storage_node = storage_node_with_storage(populated_storage(&[(
                SHARD_INDEX,
                vec![
                    (BLOB_ID, WhichSlivers::Primary),
                    (OTHER_BLOB_ID, WhichSlivers::Both),
                ],
            )])?);

            let confirmation = storage_node
                .as_ref()
                .compute_storage_confirmation(&BLOB_ID)
                .await
                .expect("should succeed");

            assert_eq!(confirmation, None);

            Ok(())
        }

        #[tokio::test]
        async fn returns_confirmation_over_nodes_storing_the_pair() -> TestResult {
            let storage_node = storage_node_with_storage(populated_storage(&[
                (SHARD_INDEX, vec![(BLOB_ID, WhichSlivers::Both)]),
                (OTHER_SHARD_INDEX, vec![(BLOB_ID, WhichSlivers::Both)]),
            ])?);

            let confirmation = storage_node
                .as_ref()
                .compute_storage_confirmation(&BLOB_ID)
                .await?
                .expect("should return Some confirmation");

            let StorageConfirmation::Signed(signed) = confirmation;

            storage_node
                .as_ref()
                .protocol_key_pair
                .as_ref()
                .public()
                .verify(&signed.confirmation, &signed.signature)
                .expect("message should be verifiable");

            let confirmation: Confirmation =
                bcs::from_bytes(&signed.confirmation).expect("message should be decodable");

            assert_eq!(confirmation.epoch, storage_node.as_ref().current_epoch);
            assert_eq!(confirmation.blob_id, BLOB_ID);

            Ok(())
        }
    }
}

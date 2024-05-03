// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::future::Future;

use anyhow::{anyhow, bail, Context};
use fastcrypto::traits::{KeyPair, Signer};
use mysten_metrics::RegistryService;
use tokio::select;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use typed_store::{rocks::MetricConf, DBMetrics};
use walrus_core::{
    encoding::{EncodingConfig, RecoveryError},
    ensure,
    merkle::MerkleProof,
    messages::{Confirmation, SignedStorageConfirmation, StorageConfirmation},
    metadata::{UnverifiedBlobMetadataWithId, VerificationError},
    BlobId,
    DecodingSymbol,
    Epoch,
    ProtocolKeyPair,
    ShardIndex,
    Sliver,
    SliverPairIndex,
    SliverType,
};
use walrus_sui::client::SuiReadClient;

use crate::{
    committee::{CommitteeService, CommitteeServiceFactory, SuiCommitteeServiceFactory},
    config::{StorageNodeConfig, SuiConfig},
    storage::Storage,
    system_events::{SuiSystemEventProvider, SystemEventProvider},
};

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
    #[error("this storage node does not currently manage shard {shard}, epoch {epoch}")]
    InvalidShard { shard: ShardIndex, epoch: Epoch },
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum RetrieveSymbolError {
    #[error("this storage node does not currently manage shard {shard}, epoch {epoch}")]
    InvalidShard { shard: ShardIndex, epoch: Epoch },
    #[error("Symbol recovery failed for sliver {0:?}, target index {0:?} in blob {2:?}")]
    RecoveryError(SliverPairIndex, SliverPairIndex, BlobId),
    #[error("Sliver {0:?} unavailable for recovery in blob {1:?}")]
    UnavailableSliver(SliverPairIndex, BlobId),

    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl From<RetrieveSliverError> for RetrieveSymbolError {
    fn from(value: RetrieveSliverError) -> Self {
        match value {
            RetrieveSliverError::InvalidShard { shard, epoch } => {
                Self::InvalidShard { shard, epoch }
            }
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
    #[error("this storage node does not currently manage shard {shard}, epoch {epoch}")]
    InvalidShard { shard: ShardIndex, epoch: Epoch },
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
        sliver_pair_index: SliverPairIndex,
        sliver_type: SliverType,
    ) -> Result<Option<Sliver>, RetrieveSliverError>;

    /// Store the primary or secondary encoding for a blob for a shard held by this storage node.
    fn store_sliver(
        &self,
        blob_id: &BlobId,
        sliver_pair_index: SliverPairIndex,
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
    /// The function creates the recovery symbol for the sliver of type `sliver_type` and of sliver
    /// pair index `target_pair_index`, starting from the sliver of the orthogonal sliver type and
    /// index `sliver_pair_index`.
    ///
    /// Returns the recovery symbol for the requested sliver.
    fn retrieve_recovery_symbol(
        &self,
        blob_id: &BlobId,
        sliver_pair_index: SliverPairIndex,
        sliver_type: SliverType,
        target_pair_index: SliverPairIndex,
    ) -> Result<DecodingSymbol<MerkleProof>, RetrieveSymbolError>;
}

/// Builder to construct a [`StorageNode`].
#[derive(Debug, Default)]
pub struct StorageNodeBuilder {
    storage: Option<Storage>,
    event_provider: Option<Box<dyn SystemEventProvider>>,
    committee_service_factory: Option<Box<dyn CommitteeServiceFactory>>,
}

impl StorageNodeBuilder {
    /// Creates a new builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the underlying storage for the node, instead of constructing one from the config.
    pub fn with_storage(mut self, storage: Storage) -> Self {
        self.storage = Some(storage);
        self
    }

    /// Sets the [`SystemEventProvider`] to be used with the node.
    pub fn with_system_event_provider(
        mut self,
        event_provider: Box<dyn SystemEventProvider>,
    ) -> Self {
        self.event_provider = Some(event_provider);
        self
    }

    /// Sets the [`CommitteeServiceFactory`] used with the node.
    pub fn with_committee_service_factory(
        mut self,
        factory: Box<dyn CommitteeServiceFactory>,
    ) -> Self {
        self.committee_service_factory = Some(factory);
        self
    }

    /// Consumes the builder and constructs a new [`StorageNode`].
    ///
    /// The constructed storage node will use dependent services provided to the builder, otherwise,
    /// it will construct a new underlying storage and [`SuiSystemEventProvider`] from parameters in
    /// the config.
    ///
    /// # Panics
    ///
    /// Panics if `config.sui` is `None` and no [`SystemEventProvider`] was configured with
    /// [`with_system_event_provider()`][Self::with_system_event_provider]; or if the
    /// `config.protocol_key_pair` has not yet been loaded into memory.
    pub async fn build(
        self,
        config: &StorageNodeConfig,
        registry_service: RegistryService,
    ) -> Result<StorageNode, anyhow::Error> {
        DBMetrics::init(&registry_service.default_registry());

        let protocol_key_pair = config
            .protocol_key_pair
            .get()
            .expect("protocol keypair must already be loaded")
            .clone();

        let storage = if let Some(storage) = self.storage {
            storage
        } else {
            Storage::open(config.storage_path.as_path(), MetricConf::new("storage"))?
        };

        let sui_config_and_client =
            if self.event_provider.is_none() || self.committee_service_factory.is_none() {
                Some(create_read_client(config).await?)
            } else {
                None
            };

        let event_provider = self.event_provider.unwrap_or_else(|| {
            let (read_client, sui_config) = sui_config_and_client.as_ref().unwrap();
            Box::new(SuiSystemEventProvider::new(
                read_client.clone(),
                sui_config.event_polling_interval,
            ))
        });

        let committee_service_factory = self.committee_service_factory.unwrap_or_else(|| {
            let (read_client, _) = sui_config_and_client.unwrap();
            Box::new(SuiCommitteeServiceFactory::new(read_client))
        });

        StorageNode::new(
            protocol_key_pair,
            storage,
            event_provider,
            committee_service_factory,
        )
        .await
    }
}

async fn create_read_client(
    config: &StorageNodeConfig,
) -> Result<(SuiReadClient, &SuiConfig), anyhow::Error> {
    let sui_config @ SuiConfig {
        rpc,
        pkg_id,
        system_object,
        ..
    } = config
        .sui
        .as_ref()
        .expect("either a sui config or event provider must be specified");

    let client = SuiReadClient::new_for_rpc(&rpc, *pkg_id, *system_object).await?;

    Ok((client, sui_config))
}

/// A Walrus storage node, responsible for 1 or more shards on Walrus.
#[derive(Debug)]
pub struct StorageNode {
    protocol_key_pair: ProtocolKeyPair,
    storage: Storage,
    encoding_config: EncodingConfig,
    event_provider: Box<dyn SystemEventProvider>,
    committee_service: Box<dyn CommitteeService>,
    _committee_service_factory: Box<dyn CommitteeServiceFactory>,
}

impl StorageNode {
    async fn new(
        key_pair: ProtocolKeyPair,
        mut storage: Storage,
        event_provider: Box<dyn SystemEventProvider>,
        committee_service_factory: Box<dyn CommitteeServiceFactory>,
    ) -> Result<Self, anyhow::Error> {
        let committee_service = committee_service_factory
            .new_for_epoch()
            .await
            .context("unable to construct a committee service for the storage node")?;

        let encoding_config = EncodingConfig::new(committee_service.get_shard_count());

        let committee = committee_service.committee();
        let managed_shards = committee.shards_for_node_public_key(key_pair.as_ref().public());
        if managed_shards.is_empty() {
            tracing::info!(epoch = committee.epoch, "node does not manage any shards");
        }

        for shard in managed_shards {
            storage
                .create_storage_for_shard(*shard)
                .with_context(|| format!("unable to initialize storage for shard {}", shard))?;
        }

        Ok(StorageNode {
            protocol_key_pair: key_pair,
            storage,
            event_provider,
            encoding_config,
            committee_service,
            _committee_service_factory: committee_service_factory,
        })
    }

    /// Creates a new [`StorageNodeBuilder`] for constructing a `StorageNode`.
    pub fn builder() -> StorageNodeBuilder {
        StorageNodeBuilder::default()
    }

    /// Run the walrus-node logic until cancelled using the provided cancellation token.
    pub async fn run(&self, cancel_token: CancellationToken) -> anyhow::Result<()> {
        select! {
            result = self.process_events() => match result {
                Ok(()) => unreachable!("process_events should never return successfully"),
                Err(err) => return Err(err),
            },
            _ = cancel_token.cancelled() => (),
        }
        Ok(())
    }

    async fn process_events(&self) -> anyhow::Result<()> {
        let cursor = self.storage.get_event_cursor()?;

        let mut blob_events = Box::into_pin(self.event_provider.events(cursor).await?);
        while let Some(event) = blob_events.next().await {
            tracing::debug!(event=?event.event_id(), "received system event");
            self.storage.update_blob_info(event)?;
        }
        bail!("event stream for blob events stopped")
    }

    fn current_epoch(&self) -> Epoch {
        self.committee_service.get_epoch()
    }

    /// Returns the shards currently owned by the storage node.
    pub fn shards(&self) -> Vec<ShardIndex> {
        self.storage.shards()
    }
}

impl ServiceState for StorageNode {
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
        if blob_info.end_epoch <= self.current_epoch() {
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
        sliver_pair_index: SliverPairIndex,
        sliver_type: SliverType,
    ) -> Result<Option<Sliver>, RetrieveSliverError> {
        let shard = sliver_pair_index.to_shard_index(self.encoding_config.n_shards(), blob_id);
        let sliver = self
            .storage
            .shard_storage(shard)
            .ok_or_else(|| RetrieveSliverError::InvalidShard {
                shard,
                epoch: self.current_epoch(),
            })?
            .get_sliver(blob_id, sliver_type)
            .context("unable to retrieve sliver")?;
        Ok(sliver)
    }

    fn store_sliver(
        &self,
        blob_id: &BlobId,
        sliver_pair_index: SliverPairIndex,
        sliver: &Sliver,
    ) -> Result<(), StoreSliverError> {
        // First determine if the shard that should store this sliver is managed by this node.
        // If not, we can return early without touching the database.
        let shard = sliver_pair_index.to_shard_index(self.encoding_config.n_shards(), blob_id);
        let shard_storage =
            self.storage
                .shard_storage(shard)
                .ok_or_else(|| StoreSliverError::InvalidShard {
                    shard,
                    epoch: self.current_epoch(),
                })?;

        // Ensure we already received metadata for this sliver.
        let metadata = self
            .storage
            .get_metadata(blob_id)
            .context("unable to retrieve metadata")?
            .ok_or_else(|| StoreSliverError::MissingMetadata(*blob_id))?;

        // Ensure the received sliver has the expected size.
        ensure!(
            sliver.has_correct_length(&self.encoding_config, metadata.metadata().unencoded_length),
            StoreSliverError::IncorrectSize(sliver.len(), *blob_id)
        );

        // Ensure the received sliver matches the metadata we have in store.
        let stored_sliver_hash = metadata
            .metadata()
            .get_sliver_hash(sliver_pair_index, sliver.r#type())
            .ok_or_else(|| StoreSliverError::InvalidSliverPairId(sliver_pair_index, *blob_id))?;
        let computed_sliver_hash = sliver.hash(&self.encoding_config)?;
        ensure!(
            &computed_sliver_hash == stored_sliver_hash,
            StoreSliverError::InvalidSliver(sliver_pair_index, *blob_id)
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
            let confirmation = Confirmation::new(self.current_epoch(), *blob_id);
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
        sliver_pair_index: SliverPairIndex,
        sliver_type: SliverType,
        target_pair_index: SliverPairIndex,
    ) -> Result<DecodingSymbol<MerkleProof>, RetrieveSymbolError> {
        let optional_sliver =
            self.retrieve_sliver(blob_id, sliver_pair_index, sliver_type.orthogonal())?;
        let Some(sliver) = optional_sliver else {
            return Err(RetrieveSymbolError::UnavailableSliver(
                sliver_pair_index,
                *blob_id,
            ));
        };

        Ok(match sliver {
            Sliver::Primary(inner) => {
                let symbol = inner
                    .recovery_symbol_for_sliver_with_proof(target_pair_index, &self.encoding_config)
                    .map_err(|_| {
                        RetrieveSymbolError::RecoveryError(
                            target_pair_index,
                            sliver_pair_index,
                            *blob_id,
                        )
                    })?;
                DecodingSymbol::Secondary(symbol)
            }
            Sliver::Secondary(inner) => {
                let symbol = inner
                    .recovery_symbol_for_sliver_with_proof(target_pair_index, &self.encoding_config)
                    .map_err(|_| {
                        RetrieveSymbolError::RecoveryError(
                            target_pair_index,
                            sliver_pair_index,
                            *blob_id,
                        )
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
    use fastcrypto::traits::KeyPair;
    use walrus_sui::{
        test_utils::EventForTesting,
        types::{BlobEvent, BlobRegistered},
    };
    use walrus_test_utils::{Result as TestResult, WithTempDir};

    use super::*;
    use crate::{
        storage::tests::{
            populated_storage,
            WhichSlivers,
            BLOB_ID,
            OTHER_SHARD_INDEX,
            SHARD_INDEX,
        },
        test_utils::StorageNodeHandle,
    };

    const OTHER_BLOB_ID: BlobId = BlobId([247; 32]);

    async fn storage_node_with_storage(storage: WithTempDir<Storage>) -> StorageNodeHandle {
        StorageNodeHandle::builder()
            .with_storage(storage)
            .build()
            .await
            .expect("storage node creation in setup should not fail")
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
            )])?)
            .await;

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
            ])?)
            .await;

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

            assert_eq!(confirmation.epoch, storage_node.as_ref().current_epoch());
            assert_eq!(confirmation.blob_id, BLOB_ID);

            Ok(())
        }
    }

    #[tokio::test]
    async fn services_slivers_for_shards_managed_according_to_committee() -> TestResult {
        let shard_for_node = ShardIndex(0);
        let node = StorageNodeHandle::builder()
            .with_system_event_provider(vec![BlobEvent::Registered(BlobRegistered::for_testing(
                BLOB_ID,
            ))])
            .with_shard_assignment(&[shard_for_node])
            .with_node_started(true)
            .build()
            .await?;
        let n_shards = node.as_ref().committee_service.get_shard_count();
        let sliver_pair_index = shard_for_node.to_pair_index(n_shards, &BLOB_ID);

        node.as_ref()
            .retrieve_sliver(&BLOB_ID, sliver_pair_index, SliverType::Primary)
            .expect("should not err, but instead return 'None'");

        Ok(())
    }
}

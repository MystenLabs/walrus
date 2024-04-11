// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::future::Future;

use anyhow::{anyhow, bail, Context};
use fastcrypto::traits::Signer;
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
    metadata::{SliverIndex, SliverPairIndex, UnverifiedBlobMetadataWithId, VerificationError},
    BlobId,
    DecodingSymbol,
    Epoch,
    ProtocolKeyPair,
    ShardIndex,
    Sliver,
    SliverType,
};
use walrus_sui::types::EventType;

use crate::{
    config::StorageNodeConfig,
    mapping::shard_index_for_pair,
    storage::Storage,
    system_events::{SuiSystemEventProvider, SystemEventCursorSet, SystemEventProvider},
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

/// Builder to construct a [`StorageNode`].
#[derive(Debug, Default)]
pub struct StorageNodeBuilder {
    storage: Option<Storage>,
    event_provider: Option<Box<dyn SystemEventProvider>>,
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
        // TODO(jsmith): Move to an optional argument once this can be fetched from the chain.
        encoding_config: EncodingConfig,
    ) -> Result<StorageNode, anyhow::Error> {
        DBMetrics::init(&registry_service.default_registry());

        let protocol_key_pair = config
            .protocol_key_pair
            .get()
            .expect("protocol keypair must already be loaded");

        let storage = if let Some(storage) = self.storage {
            storage
        } else {
            Storage::open(config.storage_path.as_path(), MetricConf::new("storage"))?
        };

        let event_provider = if let Some(event_provider) = self.event_provider {
            event_provider
        } else {
            let sui_config = config
                .sui
                .as_ref()
                .expect("either a sui config or event provider must be specified");
            Box::new(SuiSystemEventProvider::new(sui_config).await?)
        };

        Ok(StorageNode {
            current_epoch: 0,
            protocol_key_pair: protocol_key_pair.clone(),
            storage,
            event_provider,
            encoding_config,
        })
    }
}

/// A Walrus storage node, responsible for 1 or more shards on Walrus.
#[derive(Debug)]
pub struct StorageNode {
    current_epoch: Epoch,
    protocol_key_pair: ProtocolKeyPair,
    storage: Storage,
    event_provider: Box<dyn SystemEventProvider>,
    encoding_config: EncodingConfig,
}

impl StorageNode {
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
        let cursors = SystemEventCursorSet {
            registered: self.storage.get_event_cursor(EventType::Registered)?,
            certified: self.storage.get_event_cursor(EventType::Certified)?,
        };

        let mut blob_events = Box::into_pin(self.event_provider.events(cursors).await?);
        while let Some(event) = blob_events.next().await {
            tracing::debug!(event=?event.event_id(), "received system event");
            self.storage.update_blob_info(event)?;
        }
        bail!("event stream for blob events stopped")
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
            self.encoding_config.n_shards().get(),
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
            self.encoding_config.n_shards().get(),
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
    use fastcrypto::traits::KeyPair;
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
            .build(EncodingConfig::new(2, 5, 10))
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

            assert_eq!(confirmation.epoch, storage_node.as_ref().current_epoch);
            assert_eq!(confirmation.blob_id, BLOB_ID);

            Ok(())
        }
    }
}

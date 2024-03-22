// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{future::Future, num::NonZeroUsize, path::Path, sync::Arc};

use anyhow::Context;
use fastcrypto::{bls12381::min_pk::BLS12381PrivateKey, hash::HashFunction, traits::Signer};
use typed_store::rocks::MetricConf;
use walrus_core::{
    encoding::{get_encoding_config, DecodingSymbol, Primary, RecoveryError, Secondary},
    ensure,
    merkle::{MerkleProof, DIGEST_LEN},
    messages::{Confirmation, SignedStorageConfirmation, StorageConfirmation},
    metadata::{UnverifiedBlobMetadataWithId, VerificationError},
    BlobId,
    Epoch,
    ShardIndex,
    Sliver,
    SliverType,
};

use crate::{mapping::shard_index_for_pair, storage::Storage};

#[derive(Debug, thiserror::Error)]
pub enum StoreMetadataError {
    #[error(transparent)]
    InvalidMetadata(#[from] VerificationError),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
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
    #[error("Symbol Recovery Failed")]
    RecoveryError,
    #[error("Sliver is not Available for Recovery")]
    UnavailableSliver,
    #[error("Symbol Recovering the Wrong Axis")]
    WrongAxis,
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum StoreSliverError {
    #[error("Missing metadata for {0:?}")]
    MissingMetadata(BlobId),
    #[error("Invalid sliver pair id {0} for {1:?}")]
    InvalidSliverPairId(u16, BlobId),
    #[error("Invalid sliver id {0} for {1:?}")]
    InvalidSliver(u16, BlobId),
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
        sliver_pair_idx: u16,
        sliver_type: SliverType,
    ) -> Result<Option<Sliver>, RetrieveSliverError>;

    /// Store the primary or secondary encoding for a blob for a shard held by this storage node.
    fn store_sliver(
        &self,
        blob_id: &BlobId,
        sliver_pair_idx: u16,
        sliver: &Sliver,
    ) -> Result<(), StoreSliverError>;

    /// Get a signed confirmation over the identifiers of the shards storing their respective
    /// sliver-pairs for their BlobIds.
    fn compute_storage_confirmation(
        &self,
        blob_id: &BlobId,
    ) -> impl Future<Output = Result<Option<StorageConfirmation>, anyhow::Error>> + Send;

    /// Retrieves a recovery symbol for getting a primary sliver for a shard held by this storage node.
    fn retrieve_recovery_symbol_primary<U>(
        &self,
        blob_id: &BlobId,
        sliver_pair_idx: u16,
        index: u32,
    ) -> Result<DecodingSymbol<walrus_core::encoding::Primary, MerkleProof<U>>, RetrieveSymbolError>
    where
        U: HashFunction<DIGEST_LEN>;
    /// Retrieves a recovery symbol for getting a secondary sliver for a shard held by this storage node.
    fn retrieve_recovery_symbol_secondary<U>(
        &self,
        blob_id: &BlobId,
        sliver_pair_idx: u16,
        index: u32,
    ) -> Result<DecodingSymbol<walrus_core::encoding::Secondary, MerkleProof<U>>, RetrieveSymbolError>
    where
        U: HashFunction<DIGEST_LEN>;
}

/// A Walrus storage node, responsible for 1 or more shards on Walrus.
pub struct StorageNode {
    current_epoch: Epoch,
    storage: Storage,
    signer: Arc<BLS12381PrivateKey>,
    n_shards: NonZeroUsize,
}

impl StorageNode {
    /// Create a new storage node with the provided configuration.
    pub fn new(
        storage_path: &Path,
        signing_key: BLS12381PrivateKey,
    ) -> Result<Self, anyhow::Error> {
        let storage = Storage::open(storage_path, MetricConf::new("storage"))?;

        Ok(Self::new_with_storage(storage, signing_key))
    }

    fn new_with_storage(storage: Storage, signing_key: BLS12381PrivateKey) -> Self {
        Self {
            storage,
            signer: Arc::new(signing_key),
            current_epoch: 0,
            n_shards: NonZeroUsize::new(100).unwrap(),
        }
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
        let verified_metadata_with_id = metadata.verify(self.n_shards)?;
        self.storage
            .put_verified_metadata(&verified_metadata_with_id)
            .context("unable to store metadata")?;
        Ok(())
    }

    fn retrieve_sliver(
        &self,
        blob_id: &BlobId,
        sliver_pair_idx: u16,
        sliver_type: SliverType,
    ) -> Result<Option<Sliver>, RetrieveSliverError> {
        let shard = shard_index_for_pair(sliver_pair_idx, self.n_shards.get(), blob_id);
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
        sliver_pair_idx: u16,
        sliver: &Sliver,
    ) -> Result<(), StoreSliverError> {
        // First determine if the shard that should store this sliver is managed by this node.
        // If not, we can return early without touching the database.
        let shard = shard_index_for_pair(sliver_pair_idx, self.n_shards.get(), blob_id);
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
        let expected_sliver_size = match sliver {
            Sliver::Primary(_) => get_encoding_config().sliver_size_for_blob::<Primary>(blob_size),
            Sliver::Secondary(_) => {
                get_encoding_config().sliver_size_for_blob::<Secondary>(blob_size)
            }
        };
        ensure!(
            expected_sliver_size == Some(sliver.len()),
            StoreSliverError::IncorrectSize(sliver.len(), *blob_id)
        );

        // Ensure the received sliver matches the metadata we have in store.
        let stored_sliver_hash = metadata
            .metadata()
            .get_sliver_hash(sliver_pair_idx, sliver.r#type())
            .ok_or_else(|| StoreSliverError::InvalidSliverPairId(sliver_pair_idx, *blob_id))?;

        let computed_sliver_hash = sliver.hash()?;
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
            sign_confirmation(confirmation, self.signer.clone())
                .await
                .map(|signed| Some(StorageConfirmation::Signed(signed)))
        } else {
            Ok(None)
        }
    }

    fn retrieve_recovery_symbol_primary<U>(
        &self,
        blob_id: &BlobId,
        sliver_pair_idx: u16,
        index: u32,
    ) -> Result<DecodingSymbol<walrus_core::encoding::Primary, MerkleProof<U>>, RetrieveSymbolError>
    where
        U: HashFunction<DIGEST_LEN>,
    {
        let optional_sliver = self
            .retrieve_sliver(blob_id, sliver_pair_idx, SliverType::Secondary)
            .map_err(|_| {
                RetrieveSymbolError::InvalidShard(shard_index_for_pair(
                    sliver_pair_idx,
                    self.n_shards.get(),
                    blob_id,
                ))
            })?;
        match optional_sliver {
            Some(sliver) => match sliver {
                Sliver::Secondary(inner) => {
                    let symbol = inner
                        .recovery_symbol_for_sliver_with_proof(index)
                        .map_err(|_| RetrieveSymbolError::RecoveryError)?;
                    Ok(symbol)
                }
                Sliver::Primary(_) => Err(RetrieveSymbolError::WrongAxis),
            },
            None => Err(RetrieveSymbolError::UnavailableSliver),
        }
    }
    fn retrieve_recovery_symbol_secondary<U>(
        &self,
        blob_id: &BlobId,
        sliver_pair_idx: u16,
        index: u32,
    ) -> Result<DecodingSymbol<walrus_core::encoding::Secondary, MerkleProof<U>>, RetrieveSymbolError>
    where
        U: HashFunction<DIGEST_LEN>,
    {
        let optional_sliver = self
            .retrieve_sliver(blob_id, sliver_pair_idx, SliverType::Primary)
            .map_err(|_| {
                RetrieveSymbolError::InvalidShard(shard_index_for_pair(
                    sliver_pair_idx,
                    self.n_shards.get(),
                    blob_id,
                ))
            })?;
        match optional_sliver {
            Some(sliver) => match sliver {
                Sliver::Primary(inner) => {
                    let symbol = inner
                        .recovery_symbol_for_sliver_with_proof(index)
                        .map_err(|_| RetrieveSymbolError::RecoveryError)?;
                    Ok(symbol)
                }
                Sliver::Secondary(_) => Err(RetrieveSymbolError::WrongAxis),
            },
            None => Err(RetrieveSymbolError::UnavailableSliver),
        }
    }
}

async fn sign_confirmation(
    confirmation: Confirmation,
    signer: Arc<BLS12381PrivateKey>,
) -> Result<SignedStorageConfirmation, anyhow::Error> {
    let signed = tokio::task::spawn_blocking(move || {
        let encoded_confirmation = bcs::to_bytes(&confirmation)
            .expect("bcs encoding a confirmation to a vector should not fail");

        SignedStorageConfirmation {
            signature: signer.sign(&encoded_confirmation),
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

    fn storage_node_with_storage(storage: WithTempDir<Storage>) -> WithTempDir<StorageNode> {
        let signing_key = BLS12381KeyPair::generate(&mut rand::thread_rng()).private();

        WithTempDir {
            inner: StorageNode::new_with_storage(storage.inner, signing_key),
            temp_dir: storage.temp_dir,
        }
    }

    mod get_storage_confirmation {
        use fastcrypto::{bls12381::min_pk::BLS12381PublicKey, traits::VerifyingKey};

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

            BLS12381PublicKey::from(storage_node.as_ref().signer.as_ref())
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

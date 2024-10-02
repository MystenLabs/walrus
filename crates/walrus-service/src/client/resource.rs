// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Manages the storage and blob resources in the Wallet on behalf of the client.

use std::num::NonZeroU16;

use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use tracing::Level;
use walrus_core::{metadata::VerifiedBlobMetadataWithId, BlobId, Epoch, EpochCount};
use walrus_sui::{
    client::{BlobPersistence, ContractClient},
    types::Blob,
    utils::price_for_unencoded_length,
};

use super::{ClientError, ClientErrorKind, ClientResult};

#[derive(Debug, Clone)]
pub(crate) struct PriceComputation {
    write_price_per_unit_size: u64,
    storage_price_per_unit_size: u64,
    n_shards: NonZeroU16,
}

/// Struct to compute the cost of operations with blob and storage resources.
impl PriceComputation {
    pub(crate) fn new(
        write_price_per_unit_size: u64,
        storage_price_per_unit_size: u64,
        n_shards: NonZeroU16,
    ) -> Self {
        Self {
            write_price_per_unit_size,
            storage_price_per_unit_size,
            n_shards,
        }
    }

    /// Computes the cost of the operation.
    ///
    /// Returns `None` if `unencoded_length` is invalid for the current encoding and `n_shards`, and
    /// therefore the encoded length cannot be computed.
    pub(crate) fn operation_cost(&self, operation: &ResourceOperation) -> Option<u64> {
        match operation {
            ResourceOperation::RegisterFromScratch {
                unencoded_length,
                epochs_ahead,
            } => self
                .storage_fee_for_unencoded_length(*unencoded_length, *epochs_ahead)
                .map(|storage_fee| {
                    storage_fee + self.write_fee_for_unencoded_length(*unencoded_length)
                }),
            ResourceOperation::ReuseStorage { unencoded_length } => {
                Some(self.write_fee_for_unencoded_length(*unencoded_length))
            }
            ResourceOperation::ReuseRegistration => Some(0), // No cost for reusing registration
        }
    }

    /// Computes the write fee for the given unencoded length.
    pub fn write_fee_for_unencoded_length(&self, unencoded_length: u64) -> u64 {
        // The write price is independent of the number of epochs, hence the `1`.
        self.price_for_unencoded_length(unencoded_length, self.write_price_per_unit_size, 1)
            .expect("the price computation should not fail")
    }

    /// Computes the storage fee given the unencoded blob size and the number of epochs.
    pub fn storage_fee_for_unencoded_length(
        &self,
        unencoded_length: u64,
        epochs: EpochCount,
    ) -> Option<u64> {
        self.price_for_unencoded_length(unencoded_length, self.storage_price_per_unit_size, epochs)
    }

    fn price_for_unencoded_length(
        &self,
        unencoded_length: u64,
        price_per_unit_size: u64,
        epochs: EpochCount,
    ) -> Option<u64> {
        price_for_unencoded_length(unencoded_length, self.n_shards, price_per_unit_size, epochs)
    }
}

/// The operation performed on blob and storage resources to register a blob.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResourceOperation {
    /// The storage and blob resources are purchased from scratch.
    RegisterFromScratch {
        unencoded_length: u64,
        epochs_ahead: EpochCount,
    },
    /// The storage is reused, but the blob was not registered.
    ReuseStorage { unencoded_length: u64 },
    /// A registration was already present.
    ReuseRegistration,
}

/// Manages the storage and blob resources in the Wallet on behalf of the client.
#[derive(Debug)]
pub struct ResourceManager<'a, C> {
    sui_client: &'a C,
    write_committee_epoch: Epoch,
}

impl<'a, C: ContractClient> ResourceManager<'a, C> {
    /// Creates a new resource manager.
    pub fn new(sui_client: &'a C, write_committee_epoch: Epoch) -> Self {
        Self {
            sui_client,
            write_committee_epoch,
        }
    }

    /// Returns a [`Blob`] registration object for the specified metadata and number of epochs.
    ///
    /// Tries to reuse existing blob registrations or storage resources if possible.
    /// Specifically:
    /// - First, it checks if the blob is registered and returns the corresponding [`Blob`];
    /// - otherwise, it checks if there is an appropriate storage resource (with sufficient space
    ///   and for a sufficient duration) that can be used to register the blob; or
    /// - if the above fails, it purchases a new storage resource and registers the blob.
    #[tracing::instrument(skip_all, err(level = Level::DEBUG))]
    pub async fn get_blob_registration(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
        epochs_ahead: EpochCount,
        persistence: BlobPersistence,
    ) -> ClientResult<(Blob, ResourceOperation)> {
        let blob_and_op = if let Some(blob) = self
            .is_blob_registered_in_wallet(metadata.blob_id(), epochs_ahead, persistence)
            .await?
        {
            tracing::debug!(
                end_epoch=%blob.storage.end_epoch,
                "blob is already registered and valid; using the existing registration"
            );
            (blob, ResourceOperation::ReuseRegistration)
        } else if let Some(storage_resource) = self
            .sui_client
            .owned_storage_for_size_and_epoch(
                metadata.metadata().encoded_size().ok_or_else(|| {
                    ClientError::other(ClientErrorKind::Other(
                        anyhow!(
                            "the provided metadata is invalid: could not compute the encoded size"
                        )
                        .into(),
                    ))
                })?,
                epochs_ahead + self.write_committee_epoch,
            )
            .await?
        {
            tracing::debug!(
                storage_object=%storage_resource.id,
                "using an existing storage resource to register the blob"
            );
            let blob = self
                .sui_client
                .register_blob(
                    &storage_resource,
                    *metadata.blob_id(),
                    metadata.metadata().compute_root_hash().bytes(),
                    metadata.metadata().unencoded_length,
                    metadata.metadata().encoding_type,
                    persistence,
                )
                .await?;
            (
                blob,
                ResourceOperation::ReuseStorage {
                    unencoded_length: metadata.metadata().unencoded_length,
                },
            )
        } else {
            tracing::debug!(
                "the blob is not already registered or its lifetime is too short; creating new one"
            );
            let blob = self
                .sui_client
                .reserve_and_register_blob(epochs_ahead, metadata, persistence)
                .await?;
            (
                blob,
                ResourceOperation::RegisterFromScratch {
                    unencoded_length: metadata.metadata().unencoded_length,
                    epochs_ahead,
                },
            )
        };
        Ok(blob_and_op)
    }

    /// Checks if the blob is registered by the active wallet for a sufficient duration.
    ///
    /// To compute if the blob is registered for a sufficient duration, it uses the epoch of the
    /// current `write_committee`. This is because registration needs to be valid compared to a new
    /// registration that would be made now to write a new blob.
    async fn is_blob_registered_in_wallet(
        &self,
        blob_id: &BlobId,
        epochs_ahead: EpochCount,
        persistence: BlobPersistence,
    ) -> ClientResult<Option<Blob>> {
        Ok(self
            .sui_client
            .owned_blobs(false)
            .await?
            .into_iter()
            .find(|blob| {
                blob.blob_id == *blob_id
                    && blob.storage.end_epoch >= self.write_committee_epoch + epochs_ahead
                    && blob.deletable == persistence.is_deletable()
            }))
    }
}

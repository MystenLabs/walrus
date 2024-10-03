// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Manages the storage and blob resources in the Wallet on behalf of the client.

use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use tracing::Level;
use walrus_core::{metadata::VerifiedBlobMetadataWithId, BlobId, Epoch, EpochCount};
use walrus_sui::{
    client::{BlobPersistence, ContractClient},
    types::Blob,
    utils::price_for_encoded_length,
};

use super::{responses::BlobStoreResult, ClientError, ClientErrorKind, ClientResult, StoreWhen};
use crate::client::responses::EventOrObjectId;

/// Struct to compute the cost of operations with blob and storage resources.
#[derive(Debug, Clone)]
pub(crate) struct PriceComputation {
    storage_price_per_unit_size: u64,
    write_price_per_unit_size: u64,
}

impl PriceComputation {
    pub(crate) fn new(storage_price_per_unit_size: u64, write_price_per_unit_size: u64) -> Self {
        Self {
            storage_price_per_unit_size,
            write_price_per_unit_size,
        }
    }

    pub(crate) fn operation_cost(&self, operation: &RegisterBlobOp) -> u64 {
        match operation {
            RegisterBlobOp::RegisterFromScratch {
                encoded_length,
                epochs_ahead,
            } => self.storage_fee_for_encoded_length(*encoded_length, *epochs_ahead),
            RegisterBlobOp::ReuseStorage { encoded_length } => {
                self.write_fee_for_encoded_length(*encoded_length)
            }
            _ => 0, // No cost for reusing registration or no-op.
        }
    }

    /// Computes the write fee for the given encoded length.
    pub fn write_fee_for_encoded_length(&self, encoded_length: u64) -> u64 {
        // The write price is independent of the number of epochs, hence the `1`.
        price_for_encoded_length(encoded_length, self.write_price_per_unit_size, 1)
    }

    /// Computes the storage fee given the unencoded blob size and the number of epochs.
    pub fn storage_fee_for_encoded_length(&self, encoded_length: u64, epochs: EpochCount) -> u64 {
        price_for_encoded_length(encoded_length, self.storage_price_per_unit_size, epochs)
    }
}

/// The operation performed on blob and storage resources to register a blob.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RegisterBlobOp {
    /// The storage and blob resources are purchased from scratch.
    RegisterFromScratch {
        encoded_length: u64,
        epochs_ahead: EpochCount,
    },
    /// The storage is reused, but the blob was not registered.
    ReuseStorage { encoded_length: u64 },
    /// A registration was already present.
    ReuseRegistration { encoded_length: u64 },
}

impl RegisterBlobOp {
    /// Returns the encoded length of the blob.
    pub fn encoded_length(&self) -> u64 {
        match self {
            RegisterBlobOp::RegisterFromScratch { encoded_length, .. }
            | RegisterBlobOp::ReuseStorage { encoded_length }
            | RegisterBlobOp::ReuseRegistration { encoded_length } => *encoded_length,
        }
    }
}

/// The result of a store operation.
#[derive(Debug, Clone)]
pub enum StoreOp {
    /// No operation needs to be performed.
    NoOp(BlobStoreResult),
    /// A new blob registration needs to be created.
    RegisterNew(RegisterBlobOp),
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

    pub async fn get_blob_registration(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
        epochs_ahead: EpochCount,
        persistence: BlobPersistence,
        store_when: StoreWhen,
    ) -> ClientResult<(Blob, StoreOp)> {
        let (blob, op) = self
            .get_existing_registration(metadata, epochs_ahead, persistence, store_when)
            .await?;

        // If the blob is deletable and already certified, return early.
        let store_op = if blob.certified_epoch.is_some() {
            debug_assert!(
                blob.deletable && !store_when.is_store_always(),
                "get_blob_registration with StoreWhen::Always filters certified blobs"
            );
            tracing::debug!(
                "there is a deletable certified blob in the wallet, and we are not forcing a store"
            );
            StoreOp::NoOp(BlobStoreResult::AlreadyCertified {
                blob_id: *metadata.blob_id(),
                event_or_object: EventOrObjectId::Object(blob.id),
                end_epoch: blob.certified_epoch.unwrap(),
            })
        } else {
            StoreOp::RegisterNew(op)
        };

        Ok((blob, store_op))
    }

    /// Returns a [`Blob`] registration object for the specified metadata and number of epochs.
    ///
    /// Tries to reuse existing blob registrations or storage resources if possible.
    /// Specifically:
    /// - First, it checks if the blob is registered and returns the corresponding [`Blob`];
    /// - otherwise, it checks if there is an appropriate storage resource (with sufficient space
    ///   and for a sufficient duration) that can be used to register the blob; or
    /// - if the above fails, it purchases a new storage resource and registers the blob.
    ///
    /// If we are forcing a store ([`StoreWhen::Always`]), the function filters out already
    /// certified blobs owned by the wallet, such that we always create a new certification
    /// (possibly reusing storage resources or uncertified but registered blobs).
    #[tracing::instrument(skip_all, err(level = Level::DEBUG))]
    pub async fn get_existing_registration(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
        epochs_ahead: EpochCount,
        persistence: BlobPersistence,
        store_when: StoreWhen,
    ) -> ClientResult<(Blob, RegisterBlobOp)> {
        let encoded_length = metadata.metadata().encoded_size().ok_or_else(|| {
            ClientError::other(ClientErrorKind::Other(
                anyhow!("the provided metadata is invalid: could not compute the encoded size")
                    .into(),
            ))
        })?;
        let blob_and_op = if let Some(blob) = self
            .is_blob_registered_in_wallet(
                metadata.blob_id(),
                epochs_ahead,
                persistence,
                !store_when.is_store_always(),
            )
            .await?
        {
            tracing::debug!(
                end_epoch=%blob.storage.end_epoch,
                "blob is already registered and valid; using the existing registration"
            );
            (blob, RegisterBlobOp::ReuseRegistration { encoded_length })
        } else if let Some(storage_resource) = self
            .sui_client
            .owned_storage_for_size_and_epoch(
                encoded_length,
                epochs_ahead + self.write_committee_epoch,
            )
            .await?
        {
            tracing::debug!(
                storage_object=%storage_resource.id,
                "using an existing storage resource to register the blob"
            );
            // TODO(giac): consider splitting the storage before reusing it.
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
            (blob, RegisterBlobOp::ReuseStorage { encoded_length })
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
                RegisterBlobOp::RegisterFromScratch {
                    encoded_length,
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
    ///
    /// If `include_certified` is `true`, the function includes already certified blobs owned by the
    /// wallet.
    async fn is_blob_registered_in_wallet(
        &self,
        blob_id: &BlobId,
        epochs_ahead: EpochCount,
        persistence: BlobPersistence,
        include_certified: bool,
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
                    && (include_certified || blob.certified_epoch.is_none())
            }))
    }
}

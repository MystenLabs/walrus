// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Manages the storage and blob resources in the Wallet on behalf of the client.

use std::fmt::Debug;

use serde::{Deserialize, Serialize};
use tracing::Level;
use utoipa::ToSchema;
use walrus_core::{BlobId, Epoch, EpochCount, metadata::VerifiedBlobMetadataWithId};
use walrus_sui::{
    client::{BlobPersistence, SuiContractClient},
    types::Blob,
    utils::price_for_encoded_length,
};

mod owned_registration;

use owned_registration::OwnedRegistrationPlanner;

use super::{
    client_types::WalrusStoreBlobMaybeFinished,
    responses::{BlobStoreResult, EventOrObjectId},
};
use crate::{
    error::ClientResult,
    node_client::client_types::{BlobWithStatus, RegisteredBlob, WalrusStoreBlobUnfinished},
    store_optimizations::StoreOptimizations,
};

/// Struct to compute the cost of operations with blob and storage resources.
#[derive(Debug, Clone, Copy)]
pub struct PriceComputation {
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

    /// Computes the cost of the operation.
    pub fn operation_cost(&self, operation: &RegisterBlobOp) -> u64 {
        match operation {
            RegisterBlobOp::RegisterFromScratch {
                encoded_length,
                epochs_ahead,
            } => {
                self.storage_fee_for_encoded_length(*encoded_length, *epochs_ahead)
                    + self.write_fee_for_encoded_length(*encoded_length)
            }
            RegisterBlobOp::ReuseStorage { encoded_length } => {
                self.write_fee_for_encoded_length(*encoded_length)
            }
            RegisterBlobOp::ReuseAndExtend {
                encoded_length,
                epochs_extended,
            } => self.storage_fee_for_encoded_length(*encoded_length, *epochs_extended),
            RegisterBlobOp::ReuseAndExtendNonCertified {
                encoded_length,
                epochs_extended,
            } => self.storage_fee_for_encoded_length(*encoded_length, *epochs_extended),
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
#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema, PartialEq)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum RegisterBlobOp {
    /// The storage and blob resources are purchased from scratch.
    RegisterFromScratch {
        /// The size of the encoded blob in bytes.
        encoded_length: u64,
        /// The number of epochs ahead for which the blob is registered.
        #[schema(value_type = u32)]
        epochs_ahead: EpochCount,
    },
    /// The storage is reused, but the blob was not registered.
    ReuseStorage {
        /// The size of the encoded blob in bytes.
        encoded_length: u64,
    },
    /// A matching registration was already present in the wallet.
    ReuseRegistration {
        /// The size of the encoded blob in bytes.
        encoded_length: u64,
    },
    /// The blob was already certified, but its lifetime is too short.
    ReuseAndExtend {
        /// The size of the encoded blob in bytes.
        encoded_length: u64,
        /// The number of epochs extended wrt the original epoch end.
        #[schema(value_type = u32)]
        epochs_extended: EpochCount,
    },
    /// The blob was registered, but not certified, and its lifetime is shorter than
    /// the desired one.
    ReuseAndExtendNonCertified {
        /// The size of the encoded blob in bytes.
        encoded_length: u64,
        /// The number of epochs extended wrt the original epoch end.
        #[schema(value_type = u32)]
        epochs_extended: EpochCount,
    },
}

impl RegisterBlobOp {
    /// Returns the encoded length of the blob.
    pub fn encoded_length(&self) -> u64 {
        match self {
            Self::RegisterFromScratch { encoded_length, .. }
            | Self::ReuseStorage { encoded_length }
            | Self::ReuseRegistration { encoded_length, .. }
            | Self::ReuseAndExtend { encoded_length, .. }
            | Self::ReuseAndExtendNonCertified { encoded_length, .. } => *encoded_length,
        }
    }

    /// Returns if the operation involved issuing a new registration.
    pub fn is_registration(&self) -> bool {
        matches!(self, Self::RegisterFromScratch { .. })
    }

    /// Returns if the operation involved reusing storage for the registration.
    pub fn is_reuse_storage(&self) -> bool {
        matches!(self, Self::ReuseStorage { .. })
    }

    /// Returns if the operation involved reusing a registered blob.
    pub fn is_reuse_registration(&self) -> bool {
        matches!(self, Self::ReuseRegistration { .. })
    }

    /// Returns if the operation involved extending a certified blob.
    pub fn is_extend(&self) -> bool {
        matches!(self, Self::ReuseAndExtend { .. })
    }

    /// Returns if the operation involved certifying and extending a non-certified blob.
    pub fn is_certify_and_extend(&self) -> bool {
        matches!(self, Self::ReuseAndExtendNonCertified { .. })
    }

    /// Returns the number of epochs extended if the operation contains an extension.
    pub fn epochs_extended(&self) -> Option<EpochCount> {
        match self {
            Self::ReuseAndExtend {
                epochs_extended, ..
            }
            | Self::ReuseAndExtendNonCertified {
                epochs_extended, ..
            } => Some(*epochs_extended),
            _ => None,
        }
    }
}

/// The result of a store operation.
#[derive(Debug, Clone, PartialEq)]
pub enum StoreOp {
    /// No operation needs to be performed.
    NoOp(BlobStoreResult),
    /// A new blob registration needs to be created.
    RegisterNew {
        /// The blob to be registered.
        blob: Blob,
        /// The operation to be performed.
        operation: RegisterBlobOp,
    },
}

impl StoreOp {
    /// Creates a new store operation.
    pub fn new(register_op: RegisterBlobOp, blob: Blob) -> Self {
        match register_op {
            RegisterBlobOp::ReuseRegistration { .. } => {
                if blob.certified_epoch.is_some() {
                    StoreOp::NoOp(BlobStoreResult::AlreadyCertified {
                        blob_id: blob.blob_id,
                        event_or_object: EventOrObjectId::Object(blob.id),
                        end_epoch: blob.storage.end_epoch,
                    })
                } else {
                    StoreOp::RegisterNew {
                        blob,
                        operation: register_op,
                    }
                }
            }
            RegisterBlobOp::RegisterFromScratch { .. }
            | RegisterBlobOp::ReuseStorage { .. }
            | RegisterBlobOp::ReuseAndExtend { .. }
            | RegisterBlobOp::ReuseAndExtendNonCertified { .. } => StoreOp::RegisterNew {
                blob,
                operation: register_op,
            },
        }
    }

    /// Returns the blob ID of the blob.
    pub fn blob_id(&self) -> Option<BlobId> {
        match self {
            StoreOp::NoOp(result) => result.blob_id(),
            StoreOp::RegisterNew { blob, .. } => Some(blob.blob_id),
        }
    }
}

/// Manages the storage and blob resources in the Wallet on behalf of the client.
#[derive(Debug)]
pub struct ResourceManager<'a> {
    sui_client: &'a SuiContractClient,
    write_committee_epoch: Epoch,
}

impl<'a> ResourceManager<'a> {
    /// Creates a new resource manager.
    pub fn new(sui_client: &'a SuiContractClient, write_committee_epoch: Epoch) -> Self {
        Self {
            sui_client,
            write_committee_epoch,
        }
    }

    /// Returns a list of appropriate store operation for the given blobs.
    ///
    /// The function considers the requirements given to the store operation (epochs ahead,
    /// persistence, force store), the status of the blob on chain, and the available resources in
    /// the wallet.
    #[tracing::instrument(level = Level::DEBUG, skip_all)]
    pub async fn register_walrus_store_blobs(
        &self,
        encoded_blobs_with_status: Vec<WalrusStoreBlobMaybeFinished<BlobWithStatus>>,
        epochs_ahead: EpochCount,
        persistence: BlobPersistence,
        store_optimizations: StoreOptimizations,
    ) -> ClientResult<Vec<WalrusStoreBlobMaybeFinished<RegisteredBlob>>> {
        let blobs_count = encoded_blobs_with_status.len();
        let mut results = Vec::with_capacity(blobs_count);
        let mut to_be_processed = Vec::new();

        for mut blob in encoded_blobs_with_status {
            if store_optimizations.should_check_status() && !persistence.is_deletable() {
                blob.try_complete_if_certified_beyond_epoch(
                    self.write_committee_epoch + epochs_ahead,
                )?;
            };
            match blob.try_finish() {
                Ok(blob) => results.push(blob),
                Err(blob) => to_be_processed.push(blob),
            }
        }

        // If there are no blobs to be processed, return early the results.
        if to_be_processed.is_empty() {
            return Ok(results);
        }

        let num_to_be_processed = to_be_processed.len();
        tracing::debug!(blobs_count, num_to_be_processed, "registering blobs");

        let registered_blobs =
            OwnedRegistrationPlanner::new(self, epochs_ahead, persistence, store_optimizations)
                .register_or_reuse_resources(to_be_processed)
                .await?;
        debug_assert_eq!(
            registered_blobs.len(),
            num_to_be_processed,
            "the number of registered blobs and the number of blobs to store must be the same \
            (num_registered_blobs = {}, num_to_be_processed = {})",
            registered_blobs.len(),
            num_to_be_processed
        );
        results.extend(registered_blobs.into_iter());
        Ok(results)
    }

    /// Returns a list of [`Blob`] registration objects for a list of specified metadata and number
    ///
    /// Tries to reuse existing blob registrations or storage resources if possible. Specifically:
    /// - First, it checks if the blob is registered and returns the corresponding [`Blob`];
    /// - otherwise, it checks if there is an appropriate storage resource (with sufficient space
    ///   and for a sufficient duration) that can be used to register the blob; or
    /// - if the above fails, it purchases a new storage resource and registers the blob.
    ///
    /// If we are forcing a store ([`StoreOptimizations::check_status`] is `false`), the function
    /// filters out already certified blobs owned by the wallet, such that we always create a new
    /// certification (possibly reusing storage resources or uncertified but registered blobs).
    #[tracing::instrument(skip_all, err(level = Level::DEBUG))]
    pub async fn get_existing_or_register(
        &self,
        metadata_list: &[&VerifiedBlobMetadataWithId],
        epochs_ahead: EpochCount,
        persistence: BlobPersistence,
        store_optimizations: StoreOptimizations,
    ) -> ClientResult<Vec<(Blob, RegisterBlobOp)>> {
        OwnedRegistrationPlanner::new(self, epochs_ahead, persistence, store_optimizations)
            .get_existing_or_register(metadata_list)
            .await
    }

    /// Registers or reuses resources for a list of blobs.
    pub async fn register_or_reuse_resources(
        &self,
        blobs: Vec<WalrusStoreBlobUnfinished<BlobWithStatus>>,
        epochs_ahead: EpochCount,
        persistence: BlobPersistence,
        store_optimizations: StoreOptimizations,
    ) -> ClientResult<Vec<WalrusStoreBlobMaybeFinished<RegisteredBlob>>> {
        OwnedRegistrationPlanner::new(self, epochs_ahead, persistence, store_optimizations)
            .register_or_reuse_resources(blobs)
            .await
    }
}

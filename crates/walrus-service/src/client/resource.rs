// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Manages the storage and blob resources in the Wallet on behalf of the client.

use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use tracing::Level;
use utoipa::ToSchema;
use walrus_core::{metadata::VerifiedBlobMetadataWithId, BlobId, Epoch, EpochCount};
use walrus_sdk::api::BlobStatus;
use walrus_sui::{
    client::{BlobPersistence, ExpirySelectionPolicy, SuiContractClient},
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

    /// Computes the cost of the operation.
    pub(crate) fn operation_cost(&self, operation: &RegisterBlobOp) -> u64 {
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
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum RegisterBlobOp {
    /// The storage and blob resources are purchased from scratch.
    RegisterFromScratch {
        encoded_length: u64,
        #[schema(value_type = u32)]
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

    /// Returns if the operation involved issuing a new registration.
    pub fn is_registration(&self) -> bool {
        matches!(self, RegisterBlobOp::RegisterFromScratch { .. })
            // Reusing storage also requires a new registration.
            || matches!(self, RegisterBlobOp::ReuseStorage { .. })
    }
}

/// The result of a store operation.
#[derive(Debug, Clone)]
pub enum StoreOp {
    /// No operation needs to be performed.
    NoOp(BlobStoreResult),
    /// A new blob registration needs to be created.
    RegisterNew {
        blob: Blob,
        operation: RegisterBlobOp,
    },
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

    /// Returns the appropriate store operation for the given blob.
    ///
    /// The function considers the requirements given to the store operation (epochs ahead,
    /// persistence, force store), the status of the blob on chain, and the available resources in
    /// the wallet.
    pub async fn store_operation_for_blob(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
        epochs_ahead: EpochCount,
        persistence: BlobPersistence,
        store_when: StoreWhen,
        blob_status: BlobStatus,
    ) -> ClientResult<StoreOp> {
        // Return early if the blob is already certified or marked as invalid.
        // For deletable blobs, we need to check the ones that are owned by the current wallet
        // later, as there may be multiple already certified that we do not own.
        if !store_when.is_store_always() && !persistence.is_deletable() {
            if let Some(result) =
                self.blob_status_to_store_result(*metadata.blob_id(), epochs_ahead, blob_status)
            {
                return Ok(StoreOp::NoOp(result));
            }
        };

        let (blob, op) = self
            .get_existing_or_register(metadata, epochs_ahead, persistence, store_when)
            .await?;

        // If the blob is deletable and already certified, return early.
        let store_op = if blob.certified_epoch.is_some() {
            debug_assert!(
                blob.deletable && !store_when.is_store_always(),
                "get_existing_registration with StoreWhen::Always filters certified blobs"
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
            StoreOp::RegisterNew {
                blob,
                operation: op,
            }
        };

        Ok(store_op)
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
    pub async fn get_existing_or_register(
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

        if store_when.is_ignore_resources() {
            tracing::debug!(
                "ignoring existing resources and creating a new registration from scratch"
            );
            self.reserve_and_register_blob_op(encoded_length, epochs_ahead, metadata, persistence)
                .await
        } else {
            self.get_existing_or_register_with_resources(
                encoded_length,
                epochs_ahead,
                metadata,
                persistence,
                store_when,
            )
            .await
        }
    }

    async fn get_existing_or_register_with_resources(
        &self,
        encoded_length: u64,
        epochs_ahead: EpochCount,
        metadata: &VerifiedBlobMetadataWithId,
        persistence: BlobPersistence,
        store_when: StoreWhen,
    ) -> ClientResult<(Blob, RegisterBlobOp)> {
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
            // TODO(giac): consider splitting the storage before reusing it (#811).
            let blob = self
                .sui_client
                .register_blob(&storage_resource, metadata.try_into()?, persistence)
                .await?;
            (blob, RegisterBlobOp::ReuseStorage { encoded_length })
        } else {
            tracing::debug!(
                "the blob is not already registered or its lifetime is too short; creating new one"
            );
            self.reserve_and_register_blob_op(encoded_length, epochs_ahead, metadata, persistence)
                .await?
        };
        Ok(blob_and_op)
    }

    async fn reserve_and_register_blob_op(
        &self,
        encoded_length: u64,
        epochs_ahead: EpochCount,
        metadata: &VerifiedBlobMetadataWithId,
        persistence: BlobPersistence,
    ) -> ClientResult<(Blob, RegisterBlobOp)> {
        let blob = self
            .sui_client
            .reserve_and_register_blob(epochs_ahead, metadata.try_into()?, persistence)
            .await?;
        Ok((
            blob,
            RegisterBlobOp::RegisterFromScratch {
                encoded_length,
                epochs_ahead,
            },
        ))
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
            .owned_blobs(None, ExpirySelectionPolicy::Valid)
            .await?
            .into_iter()
            .find(|blob| {
                blob.blob_id == *blob_id
                    && blob.storage.end_epoch >= self.write_committee_epoch + epochs_ahead
                    && blob.deletable == persistence.is_deletable()
                    && (include_certified || blob.certified_epoch.is_none())
            }))
    }

    /// Checks if blob of the given status is already in a state for which we can return.
    fn blob_status_to_store_result(
        &self,
        blob_id: BlobId,
        epochs_ahead: EpochCount,
        blob_status: BlobStatus,
    ) -> Option<BlobStoreResult> {
        match blob_status {
            BlobStatus::Permanent {
                end_epoch,
                is_certified: true,
                status_event,
                ..
            } => {
                if end_epoch >= self.write_committee_epoch + epochs_ahead {
                    tracing::debug!(end_epoch, "blob is already certified");
                    Some(BlobStoreResult::AlreadyCertified {
                        blob_id,
                        event_or_object: EventOrObjectId::Event(status_event),
                        end_epoch,
                    })
                } else {
                    tracing::debug!(
                        end_epoch,
                        "blob is already certified but its lifetime is too short"
                    );
                    None
                }
            }
            BlobStatus::Invalid { event } => {
                tracing::debug!("blob is marked as invalid");
                Some(BlobStoreResult::MarkedInvalid { blob_id, event })
            }
            status => {
                // We intentionally don't check for "registered" blobs here: even if the blob is
                // already registered, we cannot certify it without access to the corresponding
                // Sui object. The check to see if we own the registered-but-not-certified Blob
                // object is done in `reserve_and_register_blob`.
                tracing::debug!(
                    ?status,
                    "no corresponding permanent certified `Blob` object exists"
                );
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use walrus_sui::utils::BYTES_PER_UNIT_SIZE;
    use walrus_test_utils::param_test;

    use super::*;

    param_test! {
        test_price_computation: [
            one_epoch: (BYTES_PER_UNIT_SIZE, 1, (1, 1), (2, 1)),
            two_epochs: (BYTES_PER_UNIT_SIZE, 2, (1, 1), (3, 1)),
            higher_write: (BYTES_PER_UNIT_SIZE, 1, (1, 2), (3, 2)),
            larger_blob: (2*BYTES_PER_UNIT_SIZE, 1, (1, 2), (6, 4)),
            even_larger_blob: (2*BYTES_PER_UNIT_SIZE + 1, 1, (1, 2), (9, 6)),
            more_epochs: (2*BYTES_PER_UNIT_SIZE + 1, 2, (1, 2), (12, 6)),
        ]
    }
    fn test_price_computation(
        encoded_length: u64,
        epochs_ahead: u32,
        storage_and_write_prices: (u64, u64),
        scratch_and_reuse_costs: (u64, u64),
    ) {
        let (storage_price, write_price) = storage_and_write_prices;
        let computation = PriceComputation::new(storage_price, write_price);
        let scratch = RegisterBlobOp::RegisterFromScratch {
            encoded_length,
            epochs_ahead,
        };
        let storage = RegisterBlobOp::ReuseStorage { encoded_length };
        let registration = RegisterBlobOp::ReuseRegistration { encoded_length };

        let (expected_cost_scratch, expected_cost_reuse_storage) = scratch_and_reuse_costs;
        assert_eq!(computation.operation_cost(&scratch), expected_cost_scratch);
        assert_eq!(
            computation.operation_cost(&storage),
            expected_cost_reuse_storage
        );
        assert_eq!(computation.operation_cost(&registration), 0);
    }
}

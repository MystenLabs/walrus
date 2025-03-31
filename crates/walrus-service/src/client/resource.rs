// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Manages the storage and blob resources in the Wallet on behalf of the client.

use std::{collections::HashMap, fmt::Display};

use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use tracing::Level;
use utoipa::ToSchema;
use walrus_core::{
    metadata::{BlobMetadataApi as _, VerifiedBlobMetadataWithId},
    BlobId,
    Epoch,
    EpochCount,
};
use walrus_sdk::api::BlobStatus;
use walrus_sui::{
    client::{BlobPersistence, ExpirySelectionPolicy, SuiContractClient},
    types::Blob,
    utils::price_for_encoded_length,
};

use super::{responses::BlobStoreResult, ClientError, ClientErrorKind, ClientResult, StoreWhen};
use crate::client::{responses::EventOrObjectId, WalrusStoreBlob};

/// Struct to compute the cost of operations with blob and storage resources.
#[derive(Debug, Clone)]
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
    /// The blob was already certified, but its lifetime is too short.
    ReuseAndExtend {
        encoded_length: u64,
        // The number of epochs extended wrt the original epoch end.
        #[schema(value_type = u32)]
        epochs_extended: EpochCount,
    },
    /// The blob was registered, but not certified, and its lifetime is shorter than
    /// the desired one.
    ReuseAndExtendNonCertified {
        encoded_length: u64,
        // The number of epochs extended wrt the original epoch end.
        #[schema(value_type = u32)]
        epochs_extended: EpochCount,
    },
}

impl RegisterBlobOp {
    /// Returns the encoded length of the blob.
    pub fn encoded_length(&self) -> u64 {
        match self {
            RegisterBlobOp::RegisterFromScratch { encoded_length, .. }
            | RegisterBlobOp::ReuseStorage { encoded_length }
            | RegisterBlobOp::ReuseRegistration { encoded_length }
            | RegisterBlobOp::ReuseAndExtend { encoded_length, .. }
            | RegisterBlobOp::ReuseAndExtendNonCertified { encoded_length, .. } => *encoded_length,
        }
    }

    /// Returns if the operation involved issuing a new registration.
    pub fn is_registration(&self) -> bool {
        matches!(self, RegisterBlobOp::RegisterFromScratch { .. })
    }

    /// Returns if the operation involved reusing storage for the registration.
    pub fn is_reuse_storage(&self) -> bool {
        matches!(self, RegisterBlobOp::ReuseStorage { .. })
    }

    /// Returns if the operation involved reusing a registered blob.
    pub fn is_reuse_registration(&self) -> bool {
        matches!(self, RegisterBlobOp::ReuseRegistration { .. })
    }

    /// Returns if the operation involved extending a certified blob.
    pub fn is_extend(&self) -> bool {
        matches!(self, RegisterBlobOp::ReuseAndExtend { .. })
    }

    /// Returns if the operation involved certifying and extending a non-certified blob.
    pub fn is_certify_and_extend(&self) -> bool {
        matches!(self, RegisterBlobOp::ReuseAndExtendNonCertified { .. })
    }

    /// Returns the number of epochs extended if the operation contains an extension.
    pub fn epochs_extended(&self) -> Option<EpochCount> {
        match self {
            RegisterBlobOp::ReuseAndExtend {
                epochs_extended, ..
            }
            | RegisterBlobOp::ReuseAndExtendNonCertified {
                epochs_extended, ..
            } => Some(*epochs_extended),
            _ => None,
        }
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

impl StoreOp {
    pub fn new(register_op: RegisterBlobOp, blob: Blob) -> Self {
        match register_op {
            RegisterBlobOp::ReuseRegistration { .. } => {
                if blob.certified_epoch.is_some() {
                    StoreOp::NoOp(BlobStoreResult::AlreadyCertified {
                        blob_id: blob.blob_id,
                        event_or_object: EventOrObjectId::Object(blob.id),
                        end_epoch: blob
                            .certified_epoch
                            .expect("certified blob must have a certified epoch"),
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
    pub async fn register_walrus_store_blobs<T: Display + Send + Sync>(
        &self,
        encoded_blobs_with_status: Vec<WalrusStoreBlob<'a, T>>,
        epochs_ahead: EpochCount,
        persistence: BlobPersistence,
        store_when: StoreWhen,
    ) -> ClientResult<Vec<WalrusStoreBlob<'a, T>>> {
        let mut results: Vec<WalrusStoreBlob<'a, T>> =
            Vec::with_capacity(encoded_blobs_with_status.len());
        let mut to_be_processed: Vec<WalrusStoreBlob<'a, T>> = Vec::new();
        let num_blobs = encoded_blobs_with_status.len();

        for blob in encoded_blobs_with_status {
            tracing::info!("debug-store: checking blob status: {:?}", blob);
            if blob.is_completed() {
                results.push(blob);
                continue;
            }

            match (
                &blob,
                store_when.is_ignore_status(),
                persistence.is_deletable(),
            ) {
                (
                    WalrusStoreBlob::WithStatus {
                        metadata, status, ..
                    },
                    false,
                    false,
                ) => {
                    if let Some(result) =
                        self.blob_status_to_store_result(*metadata.blob_id(), epochs_ahead, *status)
                    {
                        results.push(blob.complete_with(result));
                        continue;
                    } else {
                        to_be_processed.push(blob);
                    }
                }
                (WalrusStoreBlob::WithStatus { .. }, ..) => {
                    to_be_processed.push(blob);
                }
                _ => {
                    debug_assert!(blob.is_completed());
                    results.push(blob);
                }
            }
        }

        // If there are no blobs to be processed, return early the results.
        if to_be_processed.is_empty() {
            return Ok(results);
        }

        let num_to_be_processed = to_be_processed.len();
        tracing::info!(
            num_blobs = ?num_blobs,
            num_to_be_processed = ?num_to_be_processed,
            "debug-store: registering blobs",
        );

        let registered_blobs = self
            .register_or_reuse_resources(to_be_processed, epochs_ahead, persistence, store_when)
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
        metadata_list: &[&VerifiedBlobMetadataWithId],
        epochs_ahead: EpochCount,
        persistence: BlobPersistence,
        store_when: StoreWhen,
    ) -> ClientResult<Vec<(Blob, RegisterBlobOp)>> {
        let encoded_lengths: Result<Vec<_>, _> =
            metadata_list
                .iter()
                .map(|m| {
                    m.metadata().encoded_size().ok_or_else(|| {
                        ClientError::other(ClientErrorKind::Other(
                    anyhow!("the provided metadata is invalid: could not compute the encoded size")
                        .into(),
                ))
                    })
                })
                .collect();

        if store_when.is_ignore_resources() {
            tracing::debug!(
                "ignoring existing resources and creating a new registration from scratch"
            );
            self.reserve_and_register_blob_op(
                &encoded_lengths?,
                epochs_ahead,
                metadata_list,
                persistence,
            )
            .await
        } else {
            self.get_existing_or_register_with_resources(
                &encoded_lengths?,
                epochs_ahead,
                metadata_list,
                persistence,
                store_when,
            )
            .await
        }
    }

    /// Registers or reuses resources for a list of blobs.
    pub async fn register_or_reuse_resources<'b, T: Display + Send + Sync>(
        &self,
        blobs: Vec<WalrusStoreBlob<'b, T>>,
        epochs_ahead: EpochCount,
        persistence: BlobPersistence,
        store_when: StoreWhen,
    ) -> ClientResult<Vec<WalrusStoreBlob<'b, T>>> {
        blobs.iter().for_each(|b| {
            debug_assert!(b.is_with_status());
        });

        let encoded_lengths: Result<Vec<_>, _> = blobs
            .iter()
            .map(|b| {
                b.get_metadata()
                    .ok_or_else(|| {
                        ClientError::other(ClientErrorKind::Other(
                            anyhow!(
                                "the provided metadata is invalid: could not compute the \
                                encoded size"
                            )
                            .into(),
                        ))
                    })
                    .and_then(|metadata| {
                        metadata.metadata().encoded_size().ok_or_else(|| {
                            ClientError::other(ClientErrorKind::Other(
                                anyhow!(
                                    "the provided metadata is invalid: could not compute the
                                    encoded size"
                                )
                                .into(),
                            ))
                        })
                    })
            })
            .collect();

        let metadata_list: Vec<_> = blobs.iter().map(|b| b.get_metadata().unwrap()).collect();

        let results = if store_when.is_ignore_resources() {
            self.reserve_and_register_blob_op(
                &encoded_lengths?,
                epochs_ahead,
                &metadata_list,
                persistence,
            )
            .await?
        } else {
            self.get_existing_or_register_with_resources(
                &encoded_lengths?,
                epochs_ahead,
                &metadata_list,
                persistence,
                store_when,
            )
            .await?
        };

        debug_assert_eq!(results.len(), blobs.len());

        Ok(results
            .into_iter()
            .zip(blobs.into_iter())
            .map(|((blob, operation), blob_store)| {
                debug_assert!(
                    blob.blob_id == blob_store.get_blob_id().expect("blob ID should be present")
                );
                blob_store
                    .with_register_result(Ok(StoreOp::new(operation, blob)))
                    .expect("Should succeed with an Ok result")
            })
            .collect())
    }

    async fn get_existing_or_register_with_resources(
        &self,
        encoded_lengths: &[u64],
        epochs_ahead: EpochCount,
        metadata_list: &[&VerifiedBlobMetadataWithId],
        persistence: BlobPersistence,
        store_when: StoreWhen,
    ) -> ClientResult<Vec<(Blob, RegisterBlobOp)>> {
        let max_len = metadata_list.len();
        debug_assert!(
            encoded_lengths.len() == max_len,
            "inconsistent metadata and encoded lengths"
        );
        let mut results = Vec::with_capacity(max_len);

        let mut reused_metadata_with_storage = Vec::with_capacity(max_len);
        let mut reused_encoded_lengths = Vec::with_capacity(max_len);

        let mut new_metadata_list = Vec::with_capacity(max_len);
        let mut new_encoded_lengths = Vec::with_capacity(max_len);

        let mut extended_blobs = Vec::with_capacity(max_len);
        let mut extended_blobs_noncertified = Vec::with_capacity(max_len);

        // This keeps tracks of selected storage objects and exclude them from selecting again.
        let mut excluded = Vec::with_capacity(max_len);

        // Gets the owned blobs once for all checks, to avoid multiple calls to the RPC.
        let owned_blobs = self
            .sui_client
            .owned_blobs(None, ExpirySelectionPolicy::Valid)
            .await?;

        // For all the metadata, if the blob is registered in wallet, add it directly to results.
        // Otherwise, check if there is existing storage resource selected for the encoded length,
        // add it to reused_metadata_with_storage and its length to reused_encoded_lengths.
        // Otherwise, add it to new_metadata_list and its length to new_encoded_lengths.
        for (metadata, encoded_length) in metadata_list.iter().zip(encoded_lengths) {
            if let Some(blob) = self
                .find_blob_owned_by_wallet(
                    metadata.blob_id(),
                    persistence,
                    !store_when.is_ignore_status(),
                    &owned_blobs,
                )
                .await?
            {
                tracing::debug!(
                    end_epoch=%blob.storage.end_epoch,
                    blob_id=%blob.blob_id,
                    "blob is already registered and valid; using the existing registration"
                );
                if blob.storage.end_epoch < self.write_committee_epoch + epochs_ahead {
                    tracing::debug!(
                        blob_id=%blob.blob_id,
                        "blob is already registered but its lifetime is too short; extending it"
                    );
                    let epoch_delta =
                        self.write_committee_epoch + epochs_ahead - blob.storage.end_epoch;
                    let mut extended_blob = blob.clone();
                    extended_blob.storage.end_epoch = self.write_committee_epoch + epochs_ahead;
                    if blob.certified_epoch.is_some() {
                        extended_blobs.push((
                            extended_blob,
                            RegisterBlobOp::ReuseAndExtend {
                                encoded_length: *encoded_length,
                                epochs_extended: epoch_delta,
                            },
                        ));
                    } else {
                        extended_blobs_noncertified.push((
                            extended_blob,
                            RegisterBlobOp::ReuseAndExtendNonCertified {
                                encoded_length: *encoded_length,
                                epochs_extended: epoch_delta,
                            },
                        ));
                    }
                } else {
                    results.push((
                        blob,
                        RegisterBlobOp::ReuseRegistration {
                            encoded_length: *encoded_length,
                        },
                    ));
                }
            } else if let Some(storage_resource) = self
                .sui_client
                .owned_storage_for_size_and_epoch(
                    *encoded_length,
                    epochs_ahead + self.write_committee_epoch,
                    &excluded,
                )
                .await?
            {
                // TODO(joy): Currently select is done one at a time for each blob using `excluded`
                // to filter, this might not be efficient if the list is too long, consider better
                // storage selection strategy (WAL-363).
                // TODO(giac): consider splitting the storage before reusing it (WAL-208).
                tracing::debug!(
                    blob_id=%metadata.blob_id(),
                    storage_object=%storage_resource.id,
                    "using an existing storage resource to register the blob"
                );
                excluded.push(storage_resource.id);
                reused_metadata_with_storage.push(((*metadata).try_into()?, storage_resource));
                reused_encoded_lengths.push(*encoded_length);
            } else {
                tracing::debug!(
                    blob_id=%metadata.blob_id(),
                    "no storage resource found for the blob"
                );
                new_metadata_list.push(*metadata);
                new_encoded_lengths.push(*encoded_length);
            };
        }

        // Register all in reused_metadata_with_storage in one ptb.
        tracing::debug!(
            num_blobs=%reused_metadata_with_storage.len(),
            "registering blobs with its storage resources"
        );
        let blobs = self
            .sui_client
            .register_blobs(reused_metadata_with_storage, persistence)
            .await?;
        results.extend(blobs.into_iter().zip(reused_encoded_lengths.iter()).map(
            |(blob, &encoded_length)| (blob, RegisterBlobOp::ReuseStorage { encoded_length }),
        ));

        // Reserve space and register all in new_metadata_list in one ptb.
        tracing::debug!(
            num_blobs = ?new_metadata_list.len(),
            "blobs are not already registered or their lifetime is too short; creating new ones"
        );
        results.extend(
            self.reserve_and_register_blob_op(
                &new_encoded_lengths,
                epochs_ahead,
                &new_metadata_list,
                persistence,
            )
            .await?,
        );

        results.extend(extended_blobs);
        results.extend(extended_blobs_noncertified);

        Ok(results)
    }

    async fn reserve_and_register_blob_op(
        &self,
        encoded_lengths: &[u64],
        epochs_ahead: EpochCount,
        metadata_list: &[&VerifiedBlobMetadataWithId],
        persistence: BlobPersistence,
    ) -> ClientResult<Vec<(Blob, RegisterBlobOp)>> {
        debug_assert!(
            encoded_lengths.len() == metadata_list.len(),
            "inconsistent metadata and encoded lengths"
        );
        let blobs = self
            .sui_client
            .reserve_and_register_blobs(
                epochs_ahead,
                metadata_list
                    .iter()
                    .map(|m| (*m).try_into())
                    .collect::<Result<Vec<_>, _>>()?,
                persistence,
            )
            .await?;
        debug_assert_eq!(
            blobs.len(),
            encoded_lengths.len(),
            "the number of registered blobs and the number of encoded lengths must be the same \
            (num_registered_blobs = {}, num_encoded_lengths = {})",
            blobs.len(),
            encoded_lengths.len()
        );
        Ok(blobs
            .into_iter()
            .zip(encoded_lengths.iter())
            .map(|(blob, &encoded_length)| {
                tracing::debug!(blob_id=%blob.blob_id, "registering blob from scratch");
                (
                    blob,
                    RegisterBlobOp::RegisterFromScratch {
                        encoded_length,
                        epochs_ahead,
                    },
                )
            })
            .collect())
    }

    /// Finds a blob object with the given `blob_id` owned by the active wallet.
    ///
    /// Only includes non-expired blobs with the provided `persistence`.
    ///
    /// If `include_certified` is `true`, the function includes already certified blobs owned by the
    /// wallet.
    async fn find_blob_owned_by_wallet(
        &self,
        blob_id: &BlobId,
        persistence: BlobPersistence,
        include_certified: bool,
        owned_blobs: &[Blob],
    ) -> ClientResult<Option<Blob>> {
        Ok(owned_blobs
            .iter()
            .find(|blob| {
                blob.blob_id == *blob_id
                    && blob.storage.end_epoch > self.write_committee_epoch
                    && blob.deletable == persistence.is_deletable()
                    && (include_certified || blob.certified_epoch.is_none())
            })
            .cloned())
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
                    tracing::debug!(end_epoch, blob_id=%blob_id, "blob is already certified");
                    Some(BlobStoreResult::AlreadyCertified {
                        blob_id,
                        event_or_object: EventOrObjectId::Event(status_event),
                        end_epoch,
                    })
                } else {
                    tracing::debug!(
                        end_epoch, blob_id=%blob_id,
                        "blob is already certified but its lifetime is too short"
                    );
                    None
                }
            }
            BlobStatus::Invalid { event } => {
                tracing::debug!(blob_id=%blob_id, "blob is marked as invalid");
                Some(BlobStoreResult::MarkedInvalid { blob_id, event })
            }
            status => {
                // We intentionally don't check for "registered" blobs here: even if the blob is
                // already registered, we cannot certify it without access to the corresponding
                // Sui object. The check to see if we own the registered-but-not-certified Blob
                // object is done in `reserve_and_register_blob`.
                tracing::debug!(
                    ?status, blob_id=%blob_id,
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
        epochs_ahead: EpochCount,
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

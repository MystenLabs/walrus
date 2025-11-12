// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Manages the storage and blob resources in the Wallet on behalf of the client.
use std::{collections::HashMap, fmt::Debug};

use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use tracing::Level;
use utoipa::ToSchema;
use walrus_core::{
    BlobId,
    Epoch,
    EpochCount,
    metadata::{BlobMetadataApi as _, VerifiedBlobMetadataWithId},
};
use walrus_sui::{
    client::{BlobPersistence, ExpirySelectionPolicy, SuiContractClient},
    types::Blob,
    utils::price_for_encoded_length,
};

use super::{
    client_types::WalrusStoreBlob,
    responses::{BlobStoreResult, EventOrObjectId},
};
use crate::{
    client::WalrusStoreBlobApi,
    error::{ClientError, ClientErrorKind, ClientResult},
    store_optimizations::StoreOptimizations,
};

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
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
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
    /// A registration was already present.
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
    pub async fn register_walrus_store_blobs<T: Debug + Clone + Send + Sync>(
        &self,
        encoded_blobs_with_status: Vec<WalrusStoreBlob<'a, T>>,
        epochs_ahead: EpochCount,
        persistence: BlobPersistence,
        store_optimizations: StoreOptimizations,
    ) -> ClientResult<Vec<WalrusStoreBlob<'a, T>>> {
        let mut results: Vec<WalrusStoreBlob<'a, T>> =
            Vec::with_capacity(encoded_blobs_with_status.len());
        let mut to_be_processed: Vec<WalrusStoreBlob<'a, T>> = Vec::new();
        let num_blobs = encoded_blobs_with_status.len();

        for blob in encoded_blobs_with_status {
            let blob = if store_optimizations.should_check_status() && !persistence.is_deletable() {
                blob.try_complete_if_certified_beyond_epoch(
                    self.write_committee_epoch + epochs_ahead,
                )?
            } else {
                blob
            };
            if blob.is_completed() {
                results.push(blob);
            } else {
                to_be_processed.push(blob);
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
        );

        let registered_blobs = self
            .register_or_reuse_resources(
                to_be_processed,
                epochs_ahead,
                persistence,
                store_optimizations,
            )
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

        if store_optimizations.should_check_existing_resources() {
            self.get_existing_or_register_with_resources(
                &encoded_lengths?,
                epochs_ahead,
                metadata_list,
                persistence,
                store_optimizations,
            )
            .await
        } else {
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
        }
    }

    /// Registers or reuses resources for a list of blobs.
    ///
    /// # Panics
    ///
    /// Panics if `blobs` contains any of [`WalrusStoreBlob::Unencoded`],
    /// [`WalrusStoreBlob::Completed`], and [`WalrusStoreBlob::Error`].
    pub async fn register_or_reuse_resources<'b, T: Debug + Clone + Send + Sync>(
        &self,
        blobs: Vec<WalrusStoreBlob<'b, T>>,
        epochs_ahead: EpochCount,
        persistence: BlobPersistence,
        store_optimizations: StoreOptimizations,
    ) -> ClientResult<Vec<WalrusStoreBlob<'b, T>>> {
        blobs.iter().for_each(|b| {
            debug_assert!(b.is_with_status());
        });

        let encoded_lengths: Result<Vec<_>, _> = blobs
            .iter()
            .map(|blob| {
                blob.encoded_size().ok_or_else(|| {
                    ClientError::store_blob_internal(format!(
                        "could not compute the encoded size of the blob: {:?}",
                        blob.get_identifier()
                    ))
                })
            })
            .collect();

        let metadata_list: Vec<_> = blobs
            .iter()
            .map(|b| {
                b.get_metadata()
                    .expect("metadata is present on the allowed blob types")
            })
            .collect();

        let results = if store_optimizations.should_check_existing_resources() {
            self.get_existing_or_register_with_resources(
                &encoded_lengths?,
                epochs_ahead,
                &metadata_list,
                persistence,
                store_optimizations,
            )
            .await?
        } else {
            self.reserve_and_register_blob_op(
                &encoded_lengths?,
                epochs_ahead,
                &metadata_list,
                persistence,
            )
            .await?
        };

        debug_assert_eq!(results.len(), blobs.len());

        // TODO(WAL-754): Check if we can make sure results and blobs have the same order.
        let mut blob_id_map = HashMap::new();
        results.into_iter().for_each(|(blob, op)| {
            let blob_id = blob.blob_id;
            blob_id_map
                .entry(blob_id)
                .or_insert_with(Vec::new)
                .push((blob, op));
        });

        Ok(blobs
            .into_iter()
            .map(|blob| {
                // Get the blob ID if available
                let blob_id = blob.get_blob_id().expect("blob ID should be present");

                // Get the vec of (blob, op) pairs for this blob ID
                let Some(entries) = blob_id_map.get_mut(&blob_id) else {
                    panic!("missing blob ID: {blob_id}");
                };

                // Pop one (blob, op) pair from the vec
                if let Some((blob_obj, operation)) = entries.pop() {
                    // If vec is now empty, remove the entry from the map
                    if entries.is_empty() {
                        blob_id_map.remove(&blob_id);
                    }

                    blob.with_register_result(Ok(StoreOp::new(operation, blob_obj)))
                        .expect("should succeed on a Ok result")
                } else {
                    panic!("missing blob ID: {blob_id}");
                }
            })
            .collect())
    }

    async fn get_existing_or_register_with_resources(
        &self,
        encoded_lengths: &[u64],
        epochs_ahead: EpochCount,
        metadata_list: &[&VerifiedBlobMetadataWithId],
        persistence: BlobPersistence,
        store_optimizations: StoreOptimizations,
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

        // Gets the owned blobs once for all checks, to avoid multiple calls to the RPC.
        let owned_blobs = self
            .sui_client
            .owned_blobs(None, ExpirySelectionPolicy::Valid)
            .await?;

        let mut blob_processing_items = Vec::with_capacity(max_len);

        for (metadata, encoded_length) in metadata_list.iter().zip(encoded_lengths) {
            if let Some(blob) = self.find_blob_owned_by_wallet(
                metadata.blob_id(),
                persistence,
                store_optimizations.should_check_status(),
                &owned_blobs,
            )? {
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
            } else {
                blob_processing_items.push((*metadata, *encoded_length));
            }
        }

        // TODO(giac): consider splitting the storage before reusing it (WAL-208).
        if !blob_processing_items.is_empty() {
            let all_storage_resources = self
                .sui_client
                .owned_storage(ExpirySelectionPolicy::Valid)
                .await?;

            let target_epoch = epochs_ahead + self.write_committee_epoch;
            let mut available_resources: Vec<_> = all_storage_resources
                .into_iter()
                .filter(|storage| storage.end_epoch >= target_epoch)
                .collect();

            blob_processing_items.sort_by(|(_, size_a), (_, size_b)| size_b.cmp(size_a));

            for (metadata, encoded_length) in blob_processing_items {
                let best_resource_idx = available_resources
                    .iter()
                    .enumerate()
                    .filter(|(_, storage)| storage.storage_size >= encoded_length)
                    .min_by(|(_, storage_a), (_, storage_b)| {
                        match storage_a.storage_size.cmp(&storage_b.storage_size) {
                            std::cmp::Ordering::Equal => {
                                storage_a.end_epoch.cmp(&storage_b.end_epoch)
                            }
                            ordering => ordering,
                        }
                    })
                    .map(|(idx, _)| idx);

                if let Some(idx) = best_resource_idx {
                    let storage_resource = available_resources.swap_remove(idx);
                    tracing::debug!(
                        blob_id=%metadata.blob_id(),
                        storage_object=%storage_resource.id,
                        "using an existing storage resource to register the blob"
                    );

                    reused_metadata_with_storage.push((metadata.try_into()?, storage_resource));
                    reused_encoded_lengths.push(encoded_length);
                } else {
                    tracing::debug!(
                        blob_id=%metadata.blob_id(),
                        "no storage resource found for the blob"
                    );
                    new_metadata_list.push(metadata);
                    new_encoded_lengths.push(encoded_length);
                }
            }
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
    fn find_blob_owned_by_wallet(
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
}

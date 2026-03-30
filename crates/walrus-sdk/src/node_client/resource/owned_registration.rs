// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Planning for owned blob registration and storage reuse.

use std::collections::HashMap;

use anyhow::anyhow;
use tracing::Level;
use walrus_core::{
    BlobId,
    EpochCount,
    metadata::{BlobMetadataApi as _, VerifiedBlobMetadataWithId},
};
use walrus_sui::{
    client::{BlobPersistence, ExpirySelectionPolicy},
    types::Blob,
};

use super::{RegisterBlobOp, ResourceManager};
use crate::{
    error::{ClientError, ClientErrorKind, ClientResult},
    node_client::client_types::{
        BlobWithStatus,
        RegisteredBlob,
        WalrusStoreBlobMaybeFinished,
        WalrusStoreBlobUnfinished,
        WalrusStoreEncodedBlobApi,
    },
    store_optimizations::StoreOptimizations,
};

pub(super) struct OwnedRegistrationPlanner<'a, 'b> {
    resource_manager: &'a ResourceManager<'b>,
    epochs_ahead: EpochCount,
    persistence: BlobPersistence,
    store_optimizations: StoreOptimizations,
}

impl<'a, 'b> OwnedRegistrationPlanner<'a, 'b> {
    pub(super) fn new(
        resource_manager: &'a ResourceManager<'b>,
        epochs_ahead: EpochCount,
        persistence: BlobPersistence,
        store_optimizations: StoreOptimizations,
    ) -> Self {
        Self {
            resource_manager,
            epochs_ahead,
            persistence,
            store_optimizations,
        }
    }

    #[tracing::instrument(skip_all, err(level = Level::DEBUG))]
    pub(super) async fn get_existing_or_register(
        &self,
        metadata_list: &[&VerifiedBlobMetadataWithId],
    ) -> ClientResult<Vec<(Blob, RegisterBlobOp)>> {
        let encoded_lengths: Result<Vec<_>, _> = metadata_list
            .iter()
            .map(|metadata| {
                metadata.metadata().encoded_size().ok_or_else(|| {
                    ClientError::other(ClientErrorKind::Other(
                        anyhow!(
                            "the provided metadata is invalid: could not compute the encoded size"
                        )
                        .into(),
                    ))
                })
            })
            .collect();

        if self.store_optimizations.should_check_existing_resources() {
            self.get_existing_or_register_with_resources(&encoded_lengths?, metadata_list)
                .await
        } else {
            tracing::debug!(
                "ignoring existing resources and creating a new registration from scratch"
            );
            self.reserve_and_register_blob_op(&encoded_lengths?, metadata_list)
                .await
        }
    }

    pub(super) async fn register_or_reuse_resources(
        &self,
        blobs: Vec<WalrusStoreBlobUnfinished<BlobWithStatus>>,
    ) -> ClientResult<Vec<WalrusStoreBlobMaybeFinished<RegisteredBlob>>> {
        let encoded_lengths: Result<Vec<_>, _> = blobs
            .iter()
            .map(|blob| {
                blob.common.encoded_size().ok_or_else(|| {
                    ClientError::store_blob_internal(format!(
                        "could not compute the encoded size of the blob: {:?}",
                        blob.common.identifier
                    ))
                })
            })
            .collect();

        let metadata_list: Vec<_> = blobs
            .iter()
            .map(|blob| blob.state.encoded_blob.metadata.as_ref())
            .collect();

        let results = if self.store_optimizations.should_check_existing_resources() {
            self.get_existing_or_register_with_resources(&encoded_lengths?, &metadata_list)
                .await?
        } else {
            self.reserve_and_register_blob_op(&encoded_lengths?, &metadata_list)
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

        let results = blobs
            .into_iter()
            .map(|blob| {
                let blob_id = blob.blob_id();

                let Some(entries) = blob_id_map.get_mut(&blob_id) else {
                    return blob.fail_with(
                        ClientError::store_blob_internal(
                            "unable to register sufficient Blob objects".to_string(),
                        ),
                        "register_or_reuse_resources",
                    );
                };

                let (blob_obj, operation) = entries.pop().expect(
                    "we never insert an empty vec and remove vectors when we pop the final element",
                );

                if entries.is_empty() {
                    blob_id_map.remove(&blob_id);
                }

                blob.map_infallible(
                    |blob| blob.with_register_result(blob_obj.clone(), operation),
                    "with_register_result",
                )
            })
            .collect();
        Ok(results)
    }

    // TODO(WAL-600): This function is very long and should be split into smaller functions.
    async fn get_existing_or_register_with_resources(
        &self,
        encoded_lengths: &[u64],
        metadata_list: &[&VerifiedBlobMetadataWithId],
    ) -> ClientResult<Vec<(Blob, RegisterBlobOp)>> {
        let max_len = metadata_list.len();
        debug_assert!(
            encoded_lengths.len() == max_len,
            "inconsistent metadata and encoded lengths"
        );
        let mut results = Vec::with_capacity(max_len);

        // TODO(WAL-600): We should only allocate as much capacity as we actually need.
        let mut reused_metadata_with_storage = Vec::with_capacity(max_len);
        let mut reused_encoded_lengths = Vec::with_capacity(max_len);

        let mut new_metadata_list = Vec::with_capacity(max_len);
        let mut new_encoded_lengths = Vec::with_capacity(max_len);

        let mut extended_blobs = Vec::with_capacity(max_len);
        let mut extended_blobs_noncertified = Vec::with_capacity(max_len);

        let owned_blobs = self
            .resource_manager
            .sui_client
            .owned_blobs(None, ExpirySelectionPolicy::Valid)
            .await?;

        let mut blob_processing_items = Vec::with_capacity(max_len);

        for (metadata, encoded_length) in metadata_list.iter().zip(encoded_lengths) {
            if let Some(blob) = self.find_blob_owned_by_wallet(metadata.blob_id(), &owned_blobs)? {
                tracing::debug!(
                    end_epoch = %blob.storage.end_epoch,
                    blob_id = %blob.blob_id,
                    "blob is already registered and valid; using the existing registration"
                );
                if blob.storage.end_epoch
                    < self.resource_manager.write_committee_epoch + self.epochs_ahead
                {
                    tracing::debug!(
                        blob_id = %blob.blob_id,
                        "blob is already registered but its lifetime is too short; extending it"
                    );
                    let epoch_delta = self.resource_manager.write_committee_epoch
                        + self.epochs_ahead
                        - blob.storage.end_epoch;
                    let mut extended_blob = blob.clone();
                    extended_blob.storage.end_epoch =
                        self.resource_manager.write_committee_epoch + self.epochs_ahead;
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
                .resource_manager
                .sui_client
                .owned_storage(ExpirySelectionPolicy::Valid)
                .await?;

            let target_epoch = self.epochs_ahead + self.resource_manager.write_committee_epoch;
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
                        blob_id = %metadata.blob_id(),
                        storage_object = %storage_resource.id,
                        "using an existing storage resource to register the blob"
                    );

                    reused_metadata_with_storage.push((metadata.try_into()?, storage_resource));
                    reused_encoded_lengths.push(encoded_length);
                } else {
                    tracing::debug!(
                        blob_id = %metadata.blob_id(),
                        "no storage resource found for the blob"
                    );
                    new_metadata_list.push(metadata);
                    new_encoded_lengths.push(encoded_length);
                }
            }
        }

        tracing::debug!(
            num_blobs = %reused_metadata_with_storage.len(),
            "registering blobs with its storage resources"
        );
        let blobs = self
            .resource_manager
            .sui_client
            .register_blobs(reused_metadata_with_storage, self.persistence)
            .await?;
        results.extend(blobs.into_iter().zip(reused_encoded_lengths.iter()).map(
            |(blob, &encoded_length)| (blob, RegisterBlobOp::ReuseStorage { encoded_length }),
        ));

        tracing::debug!(
            num_blobs = ?new_metadata_list.len(),
            "blobs are not already registered or their lifetime is too short; creating new ones"
        );
        results.extend(
            self.reserve_and_register_blob_op(&new_encoded_lengths, &new_metadata_list)
                .await?,
        );

        results.extend(extended_blobs);
        results.extend(extended_blobs_noncertified);

        Ok(results)
    }

    async fn reserve_and_register_blob_op(
        &self,
        encoded_lengths: &[u64],
        metadata_list: &[&VerifiedBlobMetadataWithId],
    ) -> ClientResult<Vec<(Blob, RegisterBlobOp)>> {
        debug_assert!(
            encoded_lengths.len() == metadata_list.len(),
            "inconsistent metadata and encoded lengths"
        );
        let blobs = self
            .resource_manager
            .sui_client
            .reserve_and_register_blobs(
                self.epochs_ahead,
                metadata_list
                    .iter()
                    .map(|metadata| (*metadata).try_into())
                    .collect::<Result<Vec<_>, _>>()?,
                self.persistence,
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
                tracing::debug!(blob_id = %blob.blob_id, "registering blob from scratch");
                (
                    blob,
                    RegisterBlobOp::RegisterFromScratch {
                        encoded_length,
                        epochs_ahead: self.epochs_ahead,
                    },
                )
            })
            .collect())
    }

    fn find_blob_owned_by_wallet(
        &self,
        blob_id: &BlobId,
        owned_blobs: &[Blob],
    ) -> ClientResult<Option<Blob>> {
        Ok(owned_blobs
            .iter()
            .find(|blob| {
                blob.blob_id == *blob_id
                    && blob.storage.end_epoch > self.resource_manager.write_committee_epoch
                    && blob.deletable == self.persistence.is_deletable()
                    && (self.store_optimizations.should_check_status()
                        || blob.certified_epoch.is_none())
            })
            .cloned())
    }
}

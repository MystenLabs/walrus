// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Client for interacting with Walrus quilt.

use std::{
    collections::{HashMap, HashSet},
    fs,
    marker::PhantomData,
    num::NonZeroU16,
    path::{Path, PathBuf},
};

use bimap::BiMap;
use walrus_core::{
    BlobId,
    EncodingType,
    Epoch,
    EpochCount,
    QuiltBlobId,
    ShardIndex,
    SliverIndex,
    encoding::{
        EncodingAxis,
        Primary,
        QuiltApi,
        QuiltBlobOwned,
        QuiltConfigApi,
        QuiltDecoderApi,
        QuiltDecoderV1,
        QuiltEncoderApi,
        QuiltEnum,
        QuiltError,
        QuiltPatchApi,
        QuiltPatchIdApi,
        QuiltPatchIdV1,
        QuiltStoreBlob,
        QuiltV1,
        QuiltVersion,
        QuiltVersionEnum,
        Secondary,
        SliverData,
        get_quilt_version_enum,
    },
    metadata::{QuiltIndex, QuiltMetadata, QuiltMetadataV1, VerifiedBlobMetadataWithId},
};
use walrus_sui::client::{BlobPersistence, PostStoreAction, ReadClient, SuiContractClient};

use crate::{
    client::{Client, client_types::StoredQuiltBlob, responses::QuiltStoreResult},
    error::{ClientError, ClientErrorKind, ClientResult},
    store_when::StoreWhen,
};

// Reads all files recursively from a given path and returns them as path-content pairs.
///
/// If the path is a file, it's read directly.
/// If the path is a directory, its files are read recursively.
/// Only regular files (not directories or other special files) are included in the result.
/// If the provided path is unreadable, or not a file or directory, an error is returned.
pub fn read_blobs_from_paths<P: AsRef<Path>>(paths: &[P]) -> ClientResult<Vec<(PathBuf, Vec<u8>)>> {
    let files = get_all_files_from_paths(paths)?;

    let mut collected_files: Vec<(PathBuf, Vec<u8>)> = Vec::new();
    for file_path in files {
        let content = fs::read(&file_path).map_err(ClientError::other)?;
        collected_files.push((file_path, content));
    }

    Ok(collected_files)
}

/// Reads all files recursively from a given path and returns them as path-content pairs.
pub fn get_all_files_from_paths<P: AsRef<Path>>(paths: &[P]) -> ClientResult<Vec<PathBuf>> {
    let mut collected_files: HashSet<PathBuf> = HashSet::new();
    if paths.is_empty() {
        return Ok(Vec::new());
    }

    let mut collected_dirs: HashSet<PathBuf> = HashSet::new();
    for path in paths {
        let path = path.as_ref();
        if path.is_file() {
            collected_files.insert(path.to_path_buf());
        } else if path.is_dir() {
            let dir_entries = fs::read_dir(path).map_err(ClientError::other)?;
            for entry_result in dir_entries {
                let current_entry_path = entry_result.map_err(ClientError::other)?.path();
                if current_entry_path.is_file() {
                    collected_files.insert(current_entry_path);
                } else if current_entry_path.is_dir() {
                    collected_dirs.insert(current_entry_path);
                }
            }
        }
    }

    let files = get_all_files_from_paths(
        &collected_dirs
            .iter()
            .map(|p| p.as_path())
            .collect::<Vec<_>>(),
    )?;
    collected_files.extend(files);

    Ok(collected_files.into_iter().collect())
}

/// A set of slivers to be retrieved from Walrus.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SliverSelector<E: EncodingAxis> {
    indices_and_shards: BiMap<SliverIndex, ShardIndex>,
    _phantom: PhantomData<E>,
}

impl<E: EncodingAxis> SliverSelector<E> {
    /// Creates a new sliver selector.
    pub fn new(sliver_indices: Vec<SliverIndex>, n_shards: NonZeroU16, blob_id: &BlobId) -> Self {
        let indices_and_shards = sliver_indices
            .into_iter()
            .map(|sliver_index| {
                let pair_index = sliver_index.to_pair_index::<E>(n_shards);
                let shard_index = pair_index.to_shard_index(n_shards, blob_id);
                (sliver_index, shard_index)
            })
            .collect::<BiMap<_, _>>();

        Self {
            indices_and_shards,
            _phantom: PhantomData,
        }
    }

    /// Returns the number of slivers.
    pub fn num_slivers(&self) -> usize {
        self.indices_and_shards.len()
    }

    /// Returns true if no slivers are left.
    pub fn is_complete(&self) -> bool {
        self.indices_and_shards.is_empty()
    }

    /// Returns true if the shard contains one of the slivers.
    pub fn should_read_from_shard(&self, shard_index: &ShardIndex) -> bool {
        self.indices_and_shards.contains_right(shard_index)
    }

    /// Sliver is complete.
    pub fn complete_sliver(&mut self, sliver_index: &SliverIndex) -> bool {
        self.indices_and_shards
            .remove_by_left(sliver_index)
            .is_some()
    }
}

/// A client for interacting with Walrus quilt.
#[derive(Debug, Clone)]
pub struct QuiltClient<'a, T> {
    client: &'a Client<T>,
}

impl<'a, T> QuiltClient<'a, T> {
    /// Creates a new QuiltClient.
    pub fn new(client: &'a Client<T>) -> Self {
        Self { client }
    }
}

impl<T: ReadClient> QuiltClient<'_, T> {
    /// Retrieves the [`QuiltMetadata`].
    ///
    /// If not enough slivers can be retrieved for the index, the entire blob will be read.
    pub async fn get_quilt_metadata(&self, quilt_id: &BlobId) -> ClientResult<QuiltMetadata> {
        self.client.check_blob_id(quilt_id)?;
        let (certified_epoch, _) = self
            .client
            .get_blob_status_and_certified_epoch(quilt_id, None)
            .await?;
        let metadata = self
            .client
            .retrieve_metadata(certified_epoch, quilt_id)
            .await?;

        let quilt_index = if let Ok(quilt_index) = self
            .get_quilt_index_from_slivers(&metadata, certified_epoch)
            .await
        {
            quilt_index
        } else {
            tracing::debug!(
                "failed to get quilt metadata from slivers, trying to get quilt {}",
                quilt_id
            );
            self.get_quilt_enum(quilt_id).await?.get_quilt_index()?
        };

        match quilt_index {
            QuiltIndex::V1(quilt_index) => Ok(QuiltMetadata::V1(QuiltMetadataV1 {
                quilt_blob_id: *quilt_id,
                metadata: metadata.metadata().clone(),
                index: quilt_index.clone(),
            })),
        }
    }

    /// Decodes the [`QuiltIndex`] from the corresponding slivers.
    ///
    /// Returns error if not enough slivers can be retrieved for the index.
    async fn get_quilt_index_from_slivers(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
        certified_epoch: Epoch,
    ) -> ClientResult<QuiltIndex> {
        // Get the first sliver to determine the quilt version.
        let slivers = self
            .client
            .retrieve_slivers_with_retry::<Secondary>(
                metadata,
                &[SliverIndex::new(0)],
                certified_epoch,
                Some(1),
                None,
            )
            .await?;

        let first_sliver = slivers.first().expect("no sliver received");

        let quilt_version = QuiltVersionEnum::new_from_sliver(first_sliver.symbols.data())?;
        match quilt_version {
            QuiltVersionEnum::V1 => {
                self.get_quilt_index_v1(metadata, certified_epoch, &slivers)
                    .await
            }
        }
    }

    async fn get_quilt_index_v1(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
        certified_epoch: Epoch,
        slivers: &[SliverData<Secondary>],
    ) -> ClientResult<QuiltIndex> {
        let sliver_refs: Vec<&SliverData<Secondary>> = slivers.iter().collect();
        let mut decoder = QuiltDecoderV1::new(&sliver_refs);

        match decoder.get_or_decode_quilt_index() {
            Ok(quilt_index) => Ok(quilt_index.clone().into()),
            Err(QuiltError::MissingSlivers(indices)) => {
                let slivers = self
                    .client
                    .retrieve_slivers_with_retry::<Secondary>(
                        metadata,
                        &indices,
                        certified_epoch,
                        Some(1),
                        None,
                    )
                    .await?;
                let sliver_refs: Vec<&SliverData<Secondary>> = slivers.iter().collect();
                decoder.add_slivers(&sliver_refs);
                decoder
                    .get_or_decode_quilt_index()
                    .map_err(ClientError::other)
                    .map(|q| q.clone().into())
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Retrieves the blobs of the given identifiers from the quilt.
    pub async fn get_blobs_by_identifiers(
        &self,
        quilt_id: &BlobId,
        identifiers: &[&str],
    ) -> ClientResult<Vec<QuiltBlobOwned>> {
        let metadata = self.get_quilt_metadata(quilt_id).await?;

        match metadata {
            QuiltMetadata::V1(metadata) => {
                self.get_blobs_by_identifiers_v1(quilt_id, identifiers, &metadata)
                    .await
            }
        }
    }

    /// Retrieves blobs from QuiltV1 by identifiers.
    async fn get_blobs_by_identifiers_v1(
        &self,
        quilt_id: &BlobId,
        identifiers: &[&str],
        metadata: &QuiltMetadataV1,
    ) -> ClientResult<Vec<QuiltBlobOwned>> {
        let sliver_indices = metadata
            .index
            .get_sliver_indices_by_identifiers(identifiers)?;

        // Retrieve slivers.
        let (certified_epoch, _) = self
            .client
            .get_blob_status_and_certified_epoch(quilt_id, None)
            .await?;
        let retrieve_slivers = self
            .client
            .retrieve_slivers_with_retry::<Secondary>(
                &metadata.get_verified_metadata(),
                &sliver_indices,
                certified_epoch,
                Some(2),
                None,
            )
            .await;

        if let Ok(slivers) = retrieve_slivers {
            let sliver_refs: Vec<&SliverData<Secondary>> = slivers.iter().collect();
            let decoder =
                QuiltDecoderV1::new_with_quilt_index(&sliver_refs, metadata.index.clone());
            identifiers
                .iter()
                .map(|identifier| {
                    decoder
                        .get_blob_by_identifier(identifier)
                        .map_err(ClientError::other)
                })
                .collect::<Result<Vec<_>, _>>()
        } else {
            let quilt = futures::executor::block_on(self.get_quilt_enum(quilt_id))?;
            identifiers
                .iter()
                .map(|identifier| {
                    quilt
                        .get_blob_by_identifier(identifier)
                        .map_err(ClientError::other)
                })
                .collect::<Result<Vec<_>, _>>()
        }
    }

    /// Retrieves the list of blobs contained in a quilt.
    pub async fn get_blobs_from_quilt(
        &self,
        quilt_blob_ids: &[QuiltBlobId],
    ) -> ClientResult<Vec<QuiltBlobOwned>> {
        assert!(!quilt_blob_ids.is_empty());
        let quilt_blob_id = quilt_blob_ids.first().expect("no quilt blob id provided");
        let version_enum = quilt_blob_id.version_enum()?;
        let quilt_id = quilt_blob_id.quilt_id;

        debug_assert!(
            quilt_blob_ids
                .iter()
                .all(|quilt_blob_id| quilt_blob_id.quilt_id == quilt_id)
        );

        let (certified_epoch, _) = self
            .client
            .get_blob_status_and_certified_epoch(&quilt_id, None)
            .await?;
        let metadata = self
            .client
            .retrieve_metadata(certified_epoch, &quilt_id)
            .await?;

        match version_enum {
            QuiltVersionEnum::V1 => {
                let mut sliver_indices = Vec::new();
                for quilt_blob_id in quilt_blob_ids {
                    let id = QuiltPatchIdV1::from_bytes(&quilt_blob_id.patch_id_bytes)?;
                    sliver_indices.extend((id.start_index..id.end_index).map(SliverIndex::new));
                }
                let retrieve_slivers = self
                    .client
                    .retrieve_slivers_with_retry::<Secondary>(
                        &metadata,
                        &sliver_indices,
                        certified_epoch,
                        Some(2),
                        None,
                    )
                    .await;

                if let Ok(slivers) = retrieve_slivers {
                    let sliver_refs: Vec<&SliverData<Secondary>> = slivers.iter().collect();
                    let decoder = QuiltDecoderV1::new(&sliver_refs);
                    quilt_blob_ids
                        .iter()
                        .map(|quilt_blob_id| {
                            let id = QuiltPatchIdV1::from_bytes(&quilt_blob_id.patch_id_bytes)?;
                            decoder
                                .get_blob_by_range(id.start_index as usize, id.end_index as usize)
                                .map_err(ClientError::other)
                        })
                        .collect::<Result<Vec<_>, _>>()
                } else {
                    let quilt = self.get_quilt_enum(&quilt_id).await?;
                    let QuiltEnum::V1(quilt_v1) = quilt;
                    quilt_blob_ids
                        .iter()
                        .map(|quilt_blob_id| {
                            let id = QuiltPatchIdV1::from_bytes(&quilt_blob_id.patch_id_bytes)?;
                            quilt_v1
                                .get_blob_by_range(id.start_index as usize, id.end_index as usize)
                                .map_err(ClientError::other)
                        })
                        .collect::<Result<Vec<_>, _>>()
                }
            }
        }
    }

    /// Retrieves the list of blobs contained in a quilt.
    pub async fn get_blobs_by_ids(
        &self,
        quilt_blob_ids: &[QuiltBlobId],
    ) -> ClientResult<Vec<QuiltBlobOwned>> {
        let mut grouped_quilt_blob_ids = HashMap::new();
        for quilt_blob_id in quilt_blob_ids {
            let quilt_id = quilt_blob_id.quilt_id;
            grouped_quilt_blob_ids
                .entry(quilt_id)
                .or_insert_with(Vec::new)
                .push(quilt_blob_id.clone());
        }

        let mut futures = Vec::new();
        for quilt_blob_ids in grouped_quilt_blob_ids.values() {
            futures.push(self.get_blobs_from_quilt(quilt_blob_ids));
        }

        let results = futures::future::try_join_all(futures).await?;

        Ok(results.into_iter().flatten().collect())
    }

    async fn get_quilt_enum(&self, quilt_id: &BlobId) -> ClientResult<QuiltEnum> {
        self.client.check_blob_id(quilt_id)?;
        let (certified_epoch, _) = self
            .client
            .get_blob_status_and_certified_epoch(quilt_id, None)
            .await?;
        let metadata = self
            .client
            .retrieve_metadata(certified_epoch, quilt_id)
            .await?;
        let quilt_blob = self
            .client
            .read_blob_retry_committees::<Primary>(quilt_id)
            .await?;
        let quilt_version_enum = get_quilt_version_enum(&quilt_blob)?;
        let encoding_config_enum = self
            .client
            .encoding_config()
            .get_for_type(metadata.metadata().encoding_type());
        match quilt_version_enum {
            QuiltVersionEnum::V1 => {
                let quilt = QuiltV1::new_from_quilt_blob(quilt_blob, &encoding_config_enum)?;
                Ok(QuiltEnum::V1(quilt))
            }
        }
    }
}

impl QuiltClient<'_, SuiContractClient> {
    /// Constructs a quilt from a list of blobs.
    pub async fn construct_quilt<V: QuiltVersion>(
        &self,
        blobs: &[QuiltStoreBlob<'_>],
        encoding_type: EncodingType,
    ) -> ClientResult<V::Quilt> {
        let encoder = V::QuiltConfig::get_encoder(
            self.client.encoding_config().get_for_type(encoding_type),
            blobs,
        );

        encoder.construct_quilt().map_err(ClientError::other)
    }

    /// Constructs a quilt from a folder of files.
    pub async fn construct_quilt_from_paths<V: QuiltVersion, P: AsRef<Path>>(
        &self,
        paths: &[P],
        encoding_type: EncodingType,
    ) -> ClientResult<V::Quilt> {
        let blobs_with_paths = read_blobs_from_paths(paths)?;
        if blobs_with_paths.is_empty() {
            return Err(ClientError::from(ClientErrorKind::Other(
                "No valid files found in the specified folder".into(),
            )));
        }

        let quilt_store_blobs: Vec<_> = blobs_with_paths
            .iter()
            .map(|(path, blob)| {
                QuiltStoreBlob::new(
                    blob,
                    path.file_name()
                        .unwrap_or_default()
                        .to_str()
                        .unwrap_or_default(),
                )
            })
            .collect();

        self.construct_quilt::<V>(&quilt_store_blobs, encoding_type)
            .await
    }

    /// Stores all files from a folder as a quilt, using file names as descriptions.
    #[tracing::instrument(skip_all)]
    pub async fn reserve_and_store_quilt_from_paths<V: QuiltVersion, P: AsRef<Path>>(
        &self,
        paths: &[P],
        encoding_type: EncodingType,
        epochs_ahead: EpochCount,
        store_when: StoreWhen,
        persistence: BlobPersistence,
        post_store: PostStoreAction,
    ) -> ClientResult<QuiltStoreResult> {
        // Read files from the folder.
        let blobs_with_paths = read_blobs_from_paths(paths)?;
        if blobs_with_paths.is_empty() {
            return Err(ClientError::from(ClientErrorKind::Other(
                "No valid files found in the specified folder".into(),
            )));
        }

        let quilt_store_blobs: Vec<_> = blobs_with_paths
            .iter()
            .map(|(path, blob)| {
                QuiltStoreBlob::new(
                    blob,
                    path.file_name()
                        .unwrap_or_default()
                        .to_str()
                        .unwrap_or_default(),
                )
            })
            .collect();

        let mut result = self
            .reserve_and_store_quilt::<V>(
                &quilt_store_blobs,
                encoding_type,
                epochs_ahead,
                store_when,
                persistence,
                post_store,
            )
            .await?;

        if paths.len() == 1 && paths[0].as_ref().is_dir() {
            result.path = Some(paths[0].as_ref().to_path_buf());
        }

        Ok(result)
    }

    /// Encodes the blobs to a quilt and stores it.
    #[tracing::instrument(skip_all, fields(blob_id))]
    pub async fn reserve_and_store_quilt<V: QuiltVersion>(
        &self,
        quilt_store_blobs: &[QuiltStoreBlob<'_>],
        encoding_type: EncodingType,
        epochs_ahead: EpochCount,
        store_when: StoreWhen,
        persistence: BlobPersistence,
        post_store: PostStoreAction,
    ) -> ClientResult<QuiltStoreResult> {
        if quilt_store_blobs.is_empty() {
            return Err(ClientError::from(ClientErrorKind::StoreBlobInternal(
                "no blobs to store".to_string(),
            )));
        }

        let encoder = V::QuiltConfig::get_encoder(
            self.client.encoding_config().get_for_type(encoding_type),
            quilt_store_blobs,
        );
        let quilt = encoder.construct_quilt()?;

        tracing::info!(
            "constructed quilt, size: {:?}, symbol size: {:?}",
            quilt.data().len(),
            quilt.symbol_size()
        );

        let result = self
            .client
            .reserve_and_store_blobs_retry_committees(
                &[quilt.data()],
                encoding_type,
                epochs_ahead,
                store_when,
                persistence,
                post_store,
                None,
            )
            .await?;

        let blob_store_result = result.first().unwrap().clone();
        let blob_id = blob_store_result.blob_id().unwrap();
        let mut stored_quilt_blobs = Vec::new();
        let index = quilt.quilt_index().clone();
        for patch in index {
            stored_quilt_blobs.push(StoredQuiltBlob::new(
                blob_id,
                patch.identifier(),
                patch.quilt_patch_id(),
            ));
        }

        Ok(QuiltStoreResult {
            blob_store_result,
            stored_quilt_blobs,
            path: None,
        })
    }
}

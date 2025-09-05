// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Client for storing and retrieving quilts.

use std::{
    collections::{HashMap, HashSet},
    fs,
    marker::PhantomData,
    path::{Path, PathBuf},
    time::Duration,
};

use serde::{Deserialize, Serialize};
use walrus_core::{
    BlobId,
    EncodingType,
    Epoch,
    QuiltPatchId,
    Sliver,
    SliverIndex,
    encoding::{
        BLOB_TYPE_ATTRIBUTE_KEY,
        Primary,
        QUILT_TYPE_VALUE,
        QuiltError,
        Secondary,
        SliverData,
        quilt_encoding::*,
    },
    metadata::{QuiltIndex, QuiltMetadata, QuiltMetadataV1, VerifiedBlobMetadataWithId},
};
use walrus_sui::{
    client::{ReadClient, SuiContractClient},
    types::move_structs::BlobAttribute,
};
use walrus_utils::read_blob_from_file;

use crate::{
    client::{
        StoreArgs,
        WalrusNodeClient,
        client_types::StoredQuiltPatch,
        responses::QuiltStoreResult,
    },
    error::{ClientError, ClientErrorKind, ClientResult},
};

/// Generate identifier from path.
pub fn generate_identifier_from_path(path: &Path, index: usize) -> String {
    path.file_name()
        .and_then(|file_name| file_name.to_str())
        .map(String::from)
        .unwrap_or_else(|| format!("unnamed-blob-{index}"))
}

/// Converts a list of blobs with paths to a list of [`QuiltStoreBlob`]s.
///
/// The on-disk file names are used as identifiers for the quilt patches.
/// If the file name is not valid UTF-8, it will be replaced with "unnamed-blob-index".
//
// TODO(WAL-887): Use relative paths to deduplicate the identifiers.
pub fn assign_identifiers_with_paths(
    blobs_with_paths: impl IntoIterator<Item = (PathBuf, Vec<u8>)>,
) -> ClientResult<Vec<QuiltStoreBlob<'static>>> {
    blobs_with_paths
        .into_iter()
        .enumerate()
        .map(|(i, (path, blob))| {
            QuiltStoreBlob::new_owned(blob, generate_identifier_from_path(&path, i))
        })
        .collect::<Result<Vec<_>, QuiltError>>()
        .map_err(ClientError::from)
}

/// Reads all files recursively from a given path and returns them as path-content pairs.
///
/// If the path is a file, it's read directly.
/// If the path is a directory, its files are read recursively.
/// Returns error if path doesn't exist or is not accessible.
pub fn read_blobs_from_paths<P: AsRef<Path>>(
    paths: &[P],
) -> ClientResult<HashMap<PathBuf, Vec<u8>>> {
    if paths.is_empty() {
        return Ok(HashMap::new());
    }

    let mut collected_files: HashSet<PathBuf> = HashSet::new();
    for path in paths {
        let path = path.as_ref();

        // Validate path existence and accessibility.
        if !path.exists() {
            return Err(ClientError::from(ClientErrorKind::Other(
                format!("Path '{}' does not exist.", path.display()).into(),
            )));
        }

        collected_files.extend(get_all_files_from_path(path)?);
    }

    let mut collected_files_with_content = HashMap::with_capacity(collected_files.len());
    for file_path in collected_files {
        let content = read_blob_from_file(&file_path)
            .map_err(|e| ClientError::from(ClientErrorKind::Other(e.to_string().into())))?;
        collected_files_with_content.insert(file_path, content);
    }

    Ok(collected_files_with_content)
}

/// Get all file paths from a directory recursively.
fn get_all_files_from_path<P: AsRef<Path>>(path: P) -> ClientResult<HashSet<PathBuf>> {
    let path = path.as_ref();
    let mut collected_files = HashSet::new();

    if path.is_file() {
        collected_files.insert(path.to_owned());
    } else if path.is_dir() {
        for entry in fs::read_dir(path).map_err(ClientError::other)? {
            let current_entry_path = entry.map_err(ClientError::other)?.path();
            collected_files.extend(get_all_files_from_path(&current_entry_path)?);
        }
    }

    Ok(collected_files)
}

/// A wrapper around QuiltDecoder, slivers and quilt index.
///
/// This is used to cache the slivers and quilt index for a given quilt.
struct DecoderBasedCacheReader<V: QuiltVersion> {
    pub slivers: Vec<SliverData<V::SliverAxis>>,
    pub quilt_index: Option<QuiltIndex>,
}

impl<V: QuiltVersion> DecoderBasedCacheReader<V> {
    pub fn new(slivers: Vec<SliverData<V::SliverAxis>>, quilt_index: Option<QuiltIndex>) -> Self {
        Self {
            slivers,
            quilt_index,
        }
    }

    fn get_decoder(&self) -> V::QuiltDecoder<'_> {
        match &self.quilt_index {
            Some(quilt_index) => {
                V::QuiltConfig::get_decoder_with_quilt_index(&self.slivers, quilt_index)
            }
            None => V::QuiltConfig::get_decoder(&self.slivers),
        }
    }

    pub fn get_blobs_by_identifiers(
        &self,
        identifiers: &[&str],
    ) -> Result<Vec<QuiltStoreBlob<'static>>, QuiltError> {
        let decoder = self.get_decoder();
        decoder.get_blobs_by_identifiers(identifiers)
    }

    pub fn get_blobs_by_tag(
        &self,
        target_tag: &str,
        target_value: &str,
    ) -> Result<Vec<QuiltStoreBlob<'static>>, QuiltError> {
        let decoder = self.get_decoder();
        decoder.get_blobs_by_tag(target_tag, target_value)
    }

    pub fn get_blobs_by_patch_internal_ids(
        &self,
        patch_internal_ids: &[&[u8]],
    ) -> Result<Vec<QuiltStoreBlob<'static>>, QuiltError> {
        let decoder = V::QuiltConfig::get_decoder(&self.slivers);
        patch_internal_ids
            .iter()
            .map(|patch_internal_id| decoder.get_blob_by_patch_internal_id(patch_internal_id))
            .collect::<Result<Vec<_>, _>>()
    }
}

/// An enum to represent either a cache of slivers and quilt index or a full quilt.
///
/// It is an candidate for the future quilt cache.
enum QuiltCacheReader<V: QuiltVersion> {
    Uninitialized,
    Decoder(DecoderBasedCacheReader<V>),
    FullQuilt(QuiltEnum),
}

/// A wrapper round different types of cached quilt readers.
///
/// This hides the details of the source of the data required to read the quilt patches.
struct QuiltReader<'a, V: QuiltVersion, T: ReadClient> {
    pub reader: QuiltCacheReader<V>,
    pub client: &'a QuiltClient<'a, T>,
    pub config: QuiltClientConfig,
    pub quilt_index: Option<QuiltIndex>,
    phantom: PhantomData<V>,
}

impl<'a, V: QuiltVersion, T: ReadClient> QuiltReader<'a, V, T>
where
    SliverData<V::SliverAxis>: TryFrom<Sliver>,
{
    /// Creates a new QuiltReader.
    pub async fn new(
        client: &'a QuiltClient<'a, T>,
        config: QuiltClientConfig,
        quilt_index: Option<QuiltIndex>,
    ) -> Self {
        Self {
            reader: QuiltCacheReader::Uninitialized,
            client,
            config,
            quilt_index,
            phantom: PhantomData,
        }
    }

    /// Retrieves blobs from quilt by identifiers.
    pub async fn download_data(
        &mut self,
        sliver_indices: &[SliverIndex],
        metadata: &VerifiedBlobMetadataWithId,
        certified_epoch: Epoch,
    ) -> ClientResult<()> {
        if matches!(self.reader, QuiltCacheReader::FullQuilt(_)) {
            return Ok(());
        }

        let retrieved_slivers = self
            .client
            .client
            .retrieve_slivers_retry_committees::<V::SliverAxis>(
                metadata,
                sliver_indices,
                certified_epoch,
                self.config.max_retrieve_slivers_attempts,
                self.config.timeout,
            )
            .await;

        if let Ok(slivers) = retrieved_slivers {
            self.reader = QuiltCacheReader::Decoder(DecoderBasedCacheReader::new(
                slivers,
                self.quilt_index.clone(),
            ));
            Ok(())
        } else {
            let quilt = self
                .client
                .get_full_quilt(metadata, certified_epoch)
                .await?;
            self.reader = QuiltCacheReader::FullQuilt(quilt);
            Ok(())
        }
    }

    /// Retrieves a blob from the quilt by identifier.
    pub async fn get_blobs_by_identifiers(
        &self,
        identifiers: &[&str],
    ) -> ClientResult<Vec<QuiltStoreBlob<'static>>> {
        match &self.reader {
            QuiltCacheReader::Uninitialized => Err(ClientError::from(ClientErrorKind::Other(
                "Reader not initialized".into(),
            ))),
            QuiltCacheReader::Decoder(cache) => cache
                .get_blobs_by_identifiers(identifiers)
                .map_err(ClientError::from),
            QuiltCacheReader::FullQuilt(quilt) => quilt
                .get_blobs_by_identifiers(identifiers)
                .map_err(ClientError::from),
        }
    }

    /// Retrieves blobs from the quilt matching the given tag.
    pub async fn get_blobs_by_tag(
        &self,
        target_tag: &str,
        target_value: &str,
    ) -> ClientResult<Vec<QuiltStoreBlob<'static>>> {
        match &self.reader {
            QuiltCacheReader::Uninitialized => Err(ClientError::from(ClientErrorKind::Other(
                "Reader not initialized".into(),
            ))),
            QuiltCacheReader::Decoder(cache) => cache
                .get_blobs_by_tag(target_tag, target_value)
                .map_err(ClientError::from),
            QuiltCacheReader::FullQuilt(quilt) => quilt
                .get_blobs_by_tag(target_tag, target_value)
                .map_err(ClientError::from),
        }
    }

    /// Retrieves blobs from the quilt matching the given patch internal ids.
    pub async fn get_blobs_by_patch_internal_ids(
        &self,
        patch_internal_ids: &[&[u8]],
    ) -> ClientResult<Vec<QuiltStoreBlob<'static>>> {
        match &self.reader {
            QuiltCacheReader::Uninitialized => Err(ClientError::from(ClientErrorKind::Other(
                "Reader not initialized".into(),
            ))),
            QuiltCacheReader::Decoder(cache) => cache
                .get_blobs_by_patch_internal_ids(patch_internal_ids)
                .map_err(ClientError::from),
            QuiltCacheReader::FullQuilt(quilt) => patch_internal_ids
                .iter()
                .map(|patch_internal_id| {
                    quilt
                        .get_blob_by_patch_internal_id(patch_internal_id)
                        .map_err(ClientError::from)
                })
                .collect::<Result<Vec<_>, _>>(),
        }
    }

    /// Retrieves all the blobs from the quilt.
    pub async fn get_all_blobs(
        &mut self,
        metadata: &VerifiedBlobMetadataWithId,
        certified_epoch: Epoch,
    ) -> ClientResult<Vec<QuiltStoreBlob<'static>>> {
        match &self.reader {
            QuiltCacheReader::Uninitialized | QuiltCacheReader::Decoder(_) => {
                let quilt = self
                    .client
                    .get_full_quilt(metadata, certified_epoch)
                    .await?;
                self.reader = QuiltCacheReader::FullQuilt(quilt);
            }
            QuiltCacheReader::FullQuilt(_) => {}
        }

        let quilt = match &self.reader {
            QuiltCacheReader::FullQuilt(quilt) => quilt,
            _ => unreachable!(),
        };
        quilt.get_all_blobs().map_err(ClientError::from)
    }
}

/// Configuration for the QuiltClient.
#[serde_with::serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct QuiltClientConfig {
    /// The maximum number of attempts to retrieve slivers.
    pub max_retrieve_slivers_attempts: usize,
    /// The timeout duration for retrieving slivers.
    #[serde_as(as = "serde_with::DurationSeconds")]
    #[serde(rename = "timeout_secs")]
    pub timeout: Duration,
}

impl QuiltClientConfig {
    /// Creates a new QuiltClientConfig.
    pub fn new(max_retrieve_slivers_attempts: usize, timeout: Duration) -> Self {
        Self {
            max_retrieve_slivers_attempts,
            timeout,
        }
    }
}

impl Default for QuiltClientConfig {
    fn default() -> Self {
        Self {
            max_retrieve_slivers_attempts: 2,
            timeout: Duration::from_secs(10),
        }
    }
}

/// A facade for interacting with Walrus quilt.
#[derive(Debug, Clone)]
pub struct QuiltClient<'a, T> {
    client: &'a WalrusNodeClient<T>,
    config: QuiltClientConfig,
}

impl<'a, T> QuiltClient<'a, T> {
    /// Creates a new QuiltClient.
    pub fn new(client: &'a WalrusNodeClient<T>, config: QuiltClientConfig) -> Self {
        Self { client, config }
    }

    /// Update quilt client config.
    pub fn with_config(mut self, config: QuiltClientConfig) -> Self {
        self.config = config;
        self
    }
}

impl<T: ReadClient> QuiltClient<'_, T> {
    /// Retrieves the [`QuiltMetadata`].
    ///
    /// If not enough slivers can be retrieved for the quilt index, the entire blob will be read.
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

        // Try to retrieve the quilt index from the slivers.
        let quilt_index =
            if let Ok(quilt_index) = self.retrieve_quilt_index(&metadata, certified_epoch).await {
                quilt_index
            } else {
                // If the quilt index cannot be retrieved from the slivers, try to retrieve the
                // quilt.
                tracing::debug!(
                    "failed to retrieve index slivers, trying to get quilt instead {}",
                    quilt_id
                );
                // TODO(WAL-879): Cache the quilt.
                self.get_full_quilt(&metadata, certified_epoch)
                    .await?
                    .get_quilt_index()?
            };

        let quilt_metadata = match quilt_index {
            QuiltIndex::V1(quilt_index) => QuiltMetadata::V1(QuiltMetadataV1 {
                quilt_id: *quilt_id,
                metadata: metadata.metadata().clone(),
                index: quilt_index.clone(),
            }),
        };

        Ok(quilt_metadata)
    }

    /// Retrieves the necessary slivers and decodes the quilt index.
    ///
    /// Returns error if not enough slivers can be retrieved for the quilt index.
    async fn retrieve_quilt_index(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
        certified_epoch: Epoch,
    ) -> ClientResult<QuiltIndex> {
        // Get the first sliver to determine the quilt version.
        //
        // Since the quilt version is stored as the first byte of the Quilt, it doesn't matter
        // whether we get the first primary sliver or the first secondary sliver.
        // For now since we only support QuiltV1, we use the first secondary sliver.
        let slivers = self
            .client
            .retrieve_slivers_retry_committees::<Secondary>(
                metadata,
                &[SliverIndex::new(0)],
                certified_epoch,
                self.config.max_retrieve_slivers_attempts,
                self.config.timeout,
            )
            .await?;

        let first_sliver = slivers.first().expect("the first sliver should exist");
        let quilt_version = QuiltVersionEnum::new_from_sliver(first_sliver.symbols.data())?;

        let quilt_index = match quilt_version {
            QuiltVersionEnum::V1 => {
                self.retrieve_quilt_index_internal::<QuiltVersionV1>(
                    metadata,
                    certified_epoch,
                    first_sliver,
                )
                .await?
            }
        };

        Ok(quilt_index)
    }

    async fn retrieve_quilt_index_internal<V: QuiltVersion>(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
        certified_epoch: Epoch,
        first_sliver: &SliverData<V::SliverAxis>,
    ) -> ClientResult<QuiltIndex>
    where
        SliverData<V::SliverAxis>: TryFrom<Sliver>,
    {
        let mut all_slivers = Vec::new();
        let mut decoder = V::QuiltConfig::get_decoder(std::iter::once(first_sliver));

        let quilt_index = match decoder.get_or_decode_quilt_index() {
            Ok(quilt_index) => quilt_index,
            Err(QuiltError::MissingSlivers(indices)) => {
                all_slivers.extend(
                    self.client
                        .retrieve_slivers_retry_committees::<V::SliverAxis>(
                            metadata,
                            &indices,
                            certified_epoch,
                            self.config.max_retrieve_slivers_attempts,
                            self.config.timeout,
                        )
                        .await?,
                );
                decoder.add_slivers(&all_slivers);
                decoder.get_or_decode_quilt_index()?
            }
            Err(e) => return Err(e.into()),
        };

        Ok(quilt_index)
    }

    /// Retrieves blobs from the quilt matching the given identifiers.
    pub async fn get_blobs_by_identifiers(
        &self,
        quilt_id: &BlobId,
        identifiers: &[&str],
    ) -> ClientResult<Vec<QuiltStoreBlob<'static>>> {
        let metadata = self.get_quilt_metadata(quilt_id).await?;

        match metadata {
            QuiltMetadata::V1(metadata) => {
                // Retrieve slivers for the given identifiers.
                let sliver_indices = metadata
                    .index
                    .get_sliver_indices_for_identifiers(identifiers)?;
                let (certified_epoch, _) = self
                    .client
                    .get_blob_status_and_certified_epoch(quilt_id, None)
                    .await?;
                let mut quilt_reader = QuiltReader::<'_, QuiltVersionV1, T>::new(
                    self,
                    self.config.clone(),
                    Some(metadata.index.clone().into()),
                )
                .await;
                quilt_reader
                    .download_data(
                        &sliver_indices,
                        &metadata.get_verified_metadata(),
                        certified_epoch,
                    )
                    .await?;
                quilt_reader.get_blobs_by_identifiers(identifiers).await
            }
        }
    }

    /// Retrieves the blobs from the quilt matching the given tag.
    pub async fn get_blobs_by_tag(
        &self,
        quilt_id: &BlobId,
        target_tag: &str,
        target_value: &str,
    ) -> ClientResult<Vec<QuiltStoreBlob<'static>>> {
        let metadata = self.get_quilt_metadata(quilt_id).await?;

        match metadata {
            QuiltMetadata::V1(metadata) => {
                let sliver_indices = metadata
                    .index
                    .get_sliver_indices_for_tag(target_tag, target_value);
                if sliver_indices.is_empty() {
                    return Ok(Vec::new());
                }

                let (certified_epoch, _) = self
                    .client
                    .get_blob_status_and_certified_epoch(quilt_id, None)
                    .await?;
                let mut quilt_reader = QuiltReader::<'_, QuiltVersionV1, T>::new(
                    self,
                    self.config.clone(),
                    Some(metadata.index.clone().into()),
                )
                .await;
                quilt_reader
                    .download_data(
                        &sliver_indices,
                        &metadata.get_verified_metadata(),
                        certified_epoch,
                    )
                    .await?;
                quilt_reader
                    .get_blobs_by_tag(target_tag, target_value)
                    .await
            }
        }
    }

    /// Retrieves blobs from the quilt matching the given QuiltPatchIds.
    pub async fn get_blobs_by_ids(
        &self,
        quilt_patch_ids: &[QuiltPatchId],
    ) -> ClientResult<Vec<QuiltStoreBlob<'static>>> {
        let mut grouped_quilt_patch_ids = HashMap::new();
        for quilt_patch_id in quilt_patch_ids {
            let quilt_id = quilt_patch_id.quilt_id;
            grouped_quilt_patch_ids
                .entry(quilt_id)
                .or_insert_with(Vec::new)
                .push(quilt_patch_id.clone());
        }

        let mut futures = Vec::new();
        for quilt_patch_ids in grouped_quilt_patch_ids.values() {
            futures.push(self.get_blobs_from_quilt_by_internal_ids(quilt_patch_ids));
        }

        let results = futures::future::try_join_all(futures).await?;

        Ok(results.into_iter().flatten().collect())
    }

    async fn get_blobs_from_quilt_by_internal_ids(
        &self,
        quilt_patch_ids: &[QuiltPatchId],
    ) -> ClientResult<Vec<QuiltStoreBlob<'static>>> {
        assert!(!quilt_patch_ids.is_empty());
        let quilt_patch_id = quilt_patch_ids.first().expect("no quilt patch id provided");
        let version_enum = quilt_patch_id.version_enum()?;
        let quilt_id = quilt_patch_id.quilt_id;

        debug_assert!(
            quilt_patch_ids
                .iter()
                .all(|quilt_patch_id| quilt_patch_id.quilt_id == quilt_id)
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
                self.get_blobs_from_quilt_by_internal_ids_impl::<QuiltVersionV1>(
                    &metadata,
                    certified_epoch,
                    quilt_patch_ids,
                )
                .await
            }
        }
    }

    async fn get_blobs_from_quilt_by_internal_ids_impl<V: QuiltVersion>(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
        certified_epoch: Epoch,
        quilt_ids: &[QuiltPatchId],
    ) -> ClientResult<Vec<QuiltStoreBlob<'static>>> {
        let mut sliver_indices = Vec::new();
        for quilt_id in quilt_ids {
            let id = V::QuiltPatchInternalId::from_bytes(&quilt_id.patch_id_bytes)?;
            sliver_indices.extend(id.sliver_indices());
        }

        let mut quilt_reader =
            QuiltReader::<'_, QuiltVersionV1, T>::new(self, self.config.clone(), None).await;
        quilt_reader
            .download_data(&sliver_indices, metadata, certified_epoch)
            .await?;
        let internal_ids = quilt_ids
            .iter()
            .map(|quilt_id| quilt_id.patch_id_bytes.as_slice())
            .collect::<Vec<_>>();
        quilt_reader
            .get_blobs_by_patch_internal_ids(&internal_ids)
            .await
    }

    /// Retrieves all the blobs from the quilt.
    pub async fn get_all_blobs(
        &self,
        quilt_id: &BlobId,
    ) -> ClientResult<Vec<QuiltStoreBlob<'static>>> {
        let (certified_epoch, _) = self
            .client
            .get_blob_status_and_certified_epoch(quilt_id, None)
            .await?;
        let metadata = self
            .client
            .retrieve_metadata(certified_epoch, quilt_id)
            .await?;

        let mut quilt_reader =
            QuiltReader::<'_, QuiltVersionV1, T>::new(self, self.config.clone(), None).await;
        quilt_reader.get_all_blobs(&metadata, certified_epoch).await
    }

    /// Retrieves the quilt from Walrus.
    async fn get_full_quilt(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
        certified_epoch: Epoch,
    ) -> ClientResult<QuiltEnum> {
        let quilt = self
            .client
            .request_slivers_and_decode::<Primary>(certified_epoch, metadata)
            .await?;
        let encoding_config_enum = self
            .client
            .encoding_config()
            .get_for_type(metadata.metadata().encoding_type());

        QuiltEnum::new(quilt, &encoding_config_enum).map_err(ClientError::from)
    }
}

/// Stores quilts.
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

        encoder.construct_quilt().map_err(ClientError::from)
    }

    /// Constructs a quilt from a list of paths.
    ///
    /// The paths can be files or directories; if they are directories, their files are read
    /// recursively.
    //
    /// The on-disk file names are used as identifiers for the quilt patches.
    /// If the file name is not valid UTF-8, it will be replaced with "unnamed-blob-index".
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

        let quilt_store_blobs = assign_identifiers_with_paths(blobs_with_paths)?;

        self.construct_quilt::<V>(&quilt_store_blobs, encoding_type)
            .await
    }

    /// Stores all blobs from a list of paths as a quilt.
    #[tracing::instrument(skip_all)]
    pub async fn reserve_and_store_quilt_from_paths<V: QuiltVersion, P: AsRef<Path>>(
        &self,
        paths: &[P],
        store_args: &StoreArgs,
    ) -> ClientResult<QuiltStoreResult> {
        let quilt = self
            .construct_quilt_from_paths::<V, P>(paths, store_args.encoding_type)
            .await?;
        let result = self
            .reserve_and_store_quilt::<V>(&quilt, store_args)
            .await?;

        Ok(result)
    }

    /// Encodes the blobs to a quilt and stores it to Walrus.
    #[tracing::instrument(skip_all, fields(blob_id))]
    pub async fn reserve_and_store_quilt<V: QuiltVersion>(
        &self,
        quilt: &V::Quilt,
        store_args: &StoreArgs,
    ) -> ClientResult<QuiltStoreResult> {
        let attributes = vec![BlobAttribute::from([(
            BLOB_TYPE_ATTRIBUTE_KEY,
            QUILT_TYPE_VALUE,
        )])];
        let result = self
            .client
            .reserve_and_store_blobs_retry_committees(&[quilt.data()], &attributes, &[], store_args)
            .await?;

        let blob_store_result = result.first().expect("the first blob should exist").clone();
        let blob_id = blob_store_result
            .blob_id()
            .expect("the blob should have an id");
        let stored_quilt_blobs = quilt
            .quilt_index()?
            .patches()
            .iter()
            .map(|patch| {
                StoredQuiltPatch::new(blob_id, patch.identifier(), patch.quilt_patch_internal_id())
            })
            .collect::<Vec<_>>();

        Ok(QuiltStoreResult {
            blob_store_result,
            stored_quilt_blobs,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use rand::{Rng, thread_rng};
    use tempfile::TempDir;

    use super::*;

    fn create_random_file(dir: &Path, name: &str, size: usize) -> std::io::Result<Vec<u8>> {
        let mut rng = thread_rng();
        let mut content = vec![0u8; size];
        rng.fill(&mut content[..]);
        fs::write(dir.join(name), &content)?;
        Ok(content)
    }

    fn create_random_dir_structure(
        base_dir: &Path,
        num_files: usize,
        max_depth: usize,
        current_depth: usize,
    ) -> std::io::Result<HashMap<PathBuf, Vec<u8>>> {
        let mut rng = thread_rng();
        let mut file_contents = HashMap::new();

        // Create some random subdirectories if we haven't reached max depth.
        if current_depth < max_depth && rng.gen_bool(0.3) {
            let num_subdirs = rng.gen_range(1..=3);
            for i in 0..num_subdirs {
                let subdir_name = format!("subdir_{i}");
                let subdir_path = base_dir.join(&subdir_name);
                fs::create_dir_all(&subdir_path)?;

                // Recursively create files in subdirectory.
                let subdir_contents = create_random_dir_structure(
                    &subdir_path,
                    num_files / (current_depth + 1),
                    max_depth,
                    current_depth + 1,
                )?;
                file_contents.extend(subdir_contents);
            }
        }

        // Create some random files in current directory.
        let files_in_dir = if current_depth == max_depth {
            num_files
        } else {
            rng.gen_range(1..=num_files / 2)
        };

        for i in 0..files_in_dir {
            let file_name = format!("file_{i}.dat");
            let file_size = rng.gen_range(100..=1000);
            let content = create_random_file(base_dir, &file_name, file_size)?;
            file_contents.insert(base_dir.join(&file_name), content);
        }

        Ok(file_contents)
    }

    #[test]
    fn test_read_blobs_from_paths_complex() -> ClientResult<()> {
        // Create a temporary directory.
        let temp_dir = TempDir::new().map_err(ClientError::other)?;
        let base_path = temp_dir.path();

        // Create a complex directory structure with random files.
        let expected_files =
            create_random_dir_structure(base_path, 20, 3, 0).map_err(ClientError::other)?;

        // Read all files using read_blobs_from_paths.
        let read_files = read_blobs_from_paths(&[base_path])?;

        // Convert read files to HashMap for easy comparison.
        let read_files_map: HashMap<_, _> = read_files.into_iter().collect();

        // Verify all expected files were read with correct content.
        assert_eq!(
            read_files_map.len(),
            expected_files.len(),
            "Number of files read doesn't match expected."
        );

        for (path, expected_content) in &expected_files {
            let actual_content = read_files_map.get(path).expect("File should exist");
            assert_eq!(
                actual_content, expected_content,
                "Content mismatch for file: {path:?}."
            );
        }

        // Test with empty paths.
        let empty_result = read_blobs_from_paths::<&Path>(&[])?;
        assert!(empty_result.is_empty());

        // Test with non-existent path.
        let non_existent = base_path.join("non_existent");
        let result = read_blobs_from_paths(&[&non_existent]);
        assert!(result.is_err());

        Ok(())
    }
}

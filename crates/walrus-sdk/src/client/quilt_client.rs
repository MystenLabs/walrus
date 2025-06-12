// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Client for storing and retrieving quilts.

use std::{
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

use walrus_core::{
    BlobId,
    EncodingType,
    Epoch,
    EpochCount,
    Sliver,
    SliverIndex,
    encoding::{Primary, QuiltError, Secondary, SliverData, quilt_encoding::*},
    metadata::{QuiltIndex, QuiltMetadata, QuiltMetadataV1, VerifiedBlobMetadataWithId},
};
use walrus_sui::client::{BlobPersistence, PostStoreAction, ReadClient, SuiContractClient};

use crate::{
    client::{Client, client_types::StoredQuiltPatch, responses::QuiltStoreResult},
    error::{ClientError, ClientErrorKind, ClientResult},
    store_when::StoreWhen,
};

/// Reads all files recursively from a given path and returns them as path-content pairs.
///
/// If the path is a file, it's read directly.
/// If the path is a directory, its files are read recursively.
/// Returns error if path doesn't exist or is not accessible.
pub fn read_blobs_from_paths<P: AsRef<Path>>(paths: &[P]) -> ClientResult<Vec<(PathBuf, Vec<u8>)>> {
    if paths.is_empty() {
        return Ok(Vec::new());
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

        if path.is_file() {
            collected_files.insert(path.to_path_buf());
        } else if path.is_dir() {
            collected_files.extend(get_all_files_from_dir(path)?);
        } else {
            return Err(ClientError::from(ClientErrorKind::Other(
                format!("Path is neither a file nor a directory: {:?}.", path).into(),
            )));
        }
    }

    let mut collected_files_with_content: Vec<(PathBuf, Vec<u8>)> = Vec::new();
    for file_path in collected_files {
        let content = fs::read(&file_path).map_err(ClientError::other)?;
        collected_files_with_content.push((file_path, content));
    }

    Ok(collected_files_with_content)
}

/// Get all file paths from a directory recursively.
fn get_all_files_from_dir<P: AsRef<Path>>(path: P) -> ClientResult<Vec<PathBuf>> {
    let path = path.as_ref();
    let mut collected_files: HashSet<PathBuf> = HashSet::new();
    let dir_entries = fs::read_dir(path).map_err(ClientError::other)?;
    for entry in dir_entries {
        let current_entry_path = entry.map_err(ClientError::other)?.path();
        if current_entry_path.is_file() {
            collected_files.insert(current_entry_path);
        } else if current_entry_path.is_dir() {
            collected_files.extend(get_all_files_from_dir(&current_entry_path)?);
        }
    }

    Ok(collected_files.into_iter().collect())
}

/// A facade for interacting with Walrus quilt.
#[derive(Debug, Clone)]
pub struct QuiltClient<'a, T> {
    client: &'a Client<T>,
}

impl<'a, T> QuiltClient<'a, T> {
    const MAX_RETRIEVE_SLIVERS_ATTEMPTS: usize = 2;
    const TIMEOUT_DURATION: Duration = Duration::from_secs(10);

    /// Creates a new QuiltClient.
    pub fn new(client: &'a Client<T>) -> Self {
        Self { client }
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
                self.get_quilt_enum(&metadata, certified_epoch)
                    .await?
                    .get_quilt_index()?
            };

        let quilt_metadata = match quilt_index {
            QuiltIndex::V1(quilt_index) => QuiltMetadata::V1(QuiltMetadataV1 {
                quilt_blob_id: *quilt_id,
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
            .retrieve_slivers_with_retry::<Secondary>(
                metadata,
                &[SliverIndex::new(0)],
                certified_epoch,
                Self::MAX_RETRIEVE_SLIVERS_ATTEMPTS,
                Self::TIMEOUT_DURATION,
            )
            .await?;

        let first_sliver = slivers.first().expect("the first sliver should exist");
        let quilt_version = QuiltVersionEnum::new_from_sliver(first_sliver.symbols.data())?;

        let quilt_index = match quilt_version {
            QuiltVersionEnum::V1 => {
                self.retrieve_quilt_index_internal::<QuiltVersionV1>(
                    metadata,
                    certified_epoch,
                    &slivers,
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
        first_sliver: &[SliverData<V::SliverAxis>],
    ) -> ClientResult<QuiltIndex>
    where
        SliverData<V::SliverAxis>: TryFrom<Sliver>,
    {
        let mut all_slivers = Vec::new();
        let mut refs = Vec::new();
        let first_sliver_refs: Vec<_> = first_sliver.iter().collect();
        let mut decoder = V::QuiltConfig::get_decoder(&first_sliver_refs);

        let quilt_index = match decoder.get_or_decode_quilt_index() {
            Ok(quilt_index) => quilt_index,
            Err(QuiltError::MissingSlivers(indices)) => {
                all_slivers.extend(
                    self.client
                        .retrieve_slivers_with_retry::<V::SliverAxis>(
                            metadata,
                            &indices,
                            certified_epoch,
                            Self::MAX_RETRIEVE_SLIVERS_ATTEMPTS,
                            Self::TIMEOUT_DURATION,
                        )
                        .await?,
                );
                refs.extend(all_slivers.iter());
                decoder.add_slivers(&refs);
                decoder.get_or_decode_quilt_index()?
            }
            Err(e) => return Err(e.into()),
        };

        Ok(quilt_index)
    }

    /// Retrieves the quilt patches of the given identifiers from the quilt.
    pub async fn get_blobs_by_identifiers(
        &self,
        quilt_id: &BlobId,
        identifiers: &[&str],
    ) -> ClientResult<Vec<QuiltStoreBlobOwned>> {
        let metadata = self.get_quilt_metadata(quilt_id).await?;

        let blobs = match metadata {
            QuiltMetadata::V1(metadata) => {
                self.get_blobs_by_identifiers_impl::<QuiltVersionV1>(
                    &metadata.get_verified_metadata(),
                    &metadata.index.into(),
                    identifiers,
                )
                .await?
            }
        };

        Ok(blobs)
    }

    /// Retrieves quilt patches from QuiltV1 by identifiers.
    async fn get_blobs_by_identifiers_impl<V: QuiltVersion>(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
        index: &QuiltIndex,
        identifiers: &[&str],
    ) -> ClientResult<Vec<QuiltStoreBlobOwned>>
    where
        SliverData<V::SliverAxis>: TryFrom<Sliver>,
    {
        // Retrieve slivers for the given identifiers.
        let sliver_indices = index.get_sliver_indices_for_identifiers(identifiers)?;
        let (certified_epoch, _) = self
            .client
            .get_blob_status_and_certified_epoch(metadata.blob_id(), None)
            .await?;
        let retrieved_slivers = self
            .client
            .retrieve_slivers_with_retry::<V::SliverAxis>(
                metadata,
                &sliver_indices,
                certified_epoch,
                Self::MAX_RETRIEVE_SLIVERS_ATTEMPTS,
                Self::TIMEOUT_DURATION,
            )
            .await;

        if let Ok(slivers) = retrieved_slivers {
            let sliver_refs: Vec<_> = slivers.iter().collect();
            let decoder = V::QuiltConfig::get_decoder_with_quilt_index(&sliver_refs, index);
            identifiers
                .iter()
                .map(|identifier| {
                    decoder
                        .get_blob_by_identifier(identifier)
                        .map_err(ClientError::other)
                })
                .collect::<Result<Vec<_>, _>>()
        } else {
            let quilt = self.get_quilt_enum(metadata, certified_epoch).await?;
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

    /// Retrieves the quilt from Walrus.
    async fn get_quilt_enum(
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

        QuiltEnum::new(quilt, &encoding_config_enum).map_err(ClientError::other)
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

        encoder.construct_quilt().map_err(ClientError::other)
    }

    /// Converts a list of blobs with paths to a list of [`QuiltStoreBlob`]s.
    ///
    /// The on-disk file names are used as identifiers for the quilt patches.
    /// If the file name is not valid UTF-8, it will be replaced with "unnamed-blob-<index>".
    fn assign_identifiers_with_paths(
        blobs_with_paths: &[(PathBuf, Vec<u8>)],
    ) -> Vec<QuiltStoreBlob> {
        blobs_with_paths
            .iter()
            .enumerate()
            .map(|(i, (path, blob))| {
                QuiltStoreBlob::new(
                    blob,
                    path.file_name()
                        .and_then(|file_name| file_name.to_str())
                        .map(String::from)
                        .unwrap_or_else(|| format!("unnamed-blob-{}", i)),
                )
            })
            .collect()
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

        let quilt_store_blobs: Vec<_> = Self::assign_identifiers_with_paths(&blobs_with_paths);

        self.construct_quilt::<V>(&quilt_store_blobs, encoding_type)
            .await
    }

    /// Stores all blobs from a list of paths as a quilt.
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
        // Read blobs from the paths.
        let blobs_with_paths = read_blobs_from_paths(paths)?;
        if blobs_with_paths.is_empty() {
            return Err(ClientError::from(ClientErrorKind::Other(
                "No valid files found in the specified folder".into(),
            )));
        }

        let quilt_store_blobs: Vec<_> = Self::assign_identifiers_with_paths(&blobs_with_paths);

        let result = self
            .reserve_and_store_quilt::<V>(
                &quilt_store_blobs,
                encoding_type,
                epochs_ahead,
                store_when,
                persistence,
                post_store,
            )
            .await?;

        Ok(result)
    }

    /// Encodes the blobs to a quilt and stores it to Walrus.
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

        tracing::debug!(
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
                let subdir_name = format!("subdir_{}", i);
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
            let file_name = format!("file_{}.dat", i);
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
                "Content mismatch for file: {:?}.",
                path
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

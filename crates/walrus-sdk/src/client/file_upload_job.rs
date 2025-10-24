// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0
//! A module defining blob slicing strategies.
use std::{io::Read as _, path::Path};

use anyhow::Context;

use crate::client::{SliceSize, blob_store_id::BlobStoreId};

/// Represents either a single full blob or a large file sliced into multiple blobs.
pub enum FileUploadJob {
    /// A single full blob.
    Blob {
        /// The blob data.
        data: Vec<u8>,
    },
    /// A single file sliced into multiple blobs.
    SlicedBlobs {
        /// Slice size used to split the blobs. The last blob may be smaller than this size.
        slice_size: u64,
        /// The blob slices.
        slices: Vec<Vec<u8>>,
    },
}

impl std::fmt::Debug for FileUploadJob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FileUploadJob::Blob { data } => {
                write!(f, "FileUploadJob::Blob {{ size: {} bytes }}", data.len())
            }
            FileUploadJob::SlicedBlobs { slice_size, slices } => {
                write!(
                    f,
                    "FileUploadJob::SlicedBlobs {{ slice_size: {} bytes, slice_count: {} }}",
                    slice_size,
                    slices.len()
                )
            }
        }
    }
}

impl FileUploadJob {
    /// Returns the number of slices if this is a sliced blob, or None if it's a single blob.
    pub fn slice_count(&self) -> Option<usize> {
        match self {
            FileUploadJob::Blob { .. } => None,
            FileUploadJob::SlicedBlobs { slices, .. } => Some(slices.len()),
        }
    }
    /// Returns the total number of bytes in this blob upload job.
    pub fn total_bytes(&self) -> u64 {
        match self {
            FileUploadJob::Blob { data } => u64::try_from(data.len()).expect("blob size too large"),
            FileUploadJob::SlicedBlobs { slices, .. } => {
                u64::try_from(slices.iter().map(|s| s.len()).sum::<usize>())
                    .expect("sliced blobs size too large")
            }
        }
    }
    /// Returns an iterator over the slices in this blob upload job.
    pub fn iterate_slices(
        &self,
        file_index: usize,
    ) -> Box<dyn Iterator<Item = (BlobStoreId, &[u8])> + '_> {
        match self {
            FileUploadJob::Blob { data } => Box::new(std::iter::once((
                BlobStoreId::new_file(file_index),
                data.as_slice(),
            ))),
            FileUploadJob::SlicedBlobs { slices, .. } => {
                Box::new(slices.iter().enumerate().map(move |(slice_index, s)| {
                    (
                        BlobStoreId::new_file_slice(file_index, slice_index),
                        s.as_slice(),
                    )
                }))
            }
        }
    }
}

/// Reads a blob from a file, slicing it if necessary based on the provided slice size and
/// network max blob size.
pub fn read_blob_from_file(
    path: impl AsRef<Path>,
    slice_size: SliceSize,
    network_max_blob_size: u64,
) -> anyhow::Result<FileUploadJob> {
    // Get the file size.
    let size = std::fs::metadata(&path)
        .context(format!(
            "unable to read metadata from '{}'",
            path.as_ref().display()
        ))?
        .len();
    match slice_size {
        SliceSize::Disabled => {
            if size > network_max_blob_size {
                anyhow::bail!(
                    "blob size ({}) exceeds network max blob size ({}) and slicing is disabled. \
                    see --slice-size option to enable slicing.",
                    size,
                    network_max_blob_size
                );
            }
            let data = std::fs::read(&path).context(format!(
                "unable to read blob from '{}'",
                path.as_ref().display()
            ))?;
            Ok(FileUploadJob::Blob { data })
        }
        SliceSize::Auto => {
            if size <= network_max_blob_size {
                let data = std::fs::read(&path).context(format!(
                    "unable to read blob from '{}'",
                    path.as_ref().display()
                ))?;
                Ok(FileUploadJob::Blob { data })
            } else {
                slice_file_into_blobs(&path, size, network_max_blob_size)
            }
        }
        SliceSize::Specific(mut specific_size) => {
            specific_size = network_max_blob_size.min(specific_size);
            if size <= network_max_blob_size {
                let data = std::fs::read(&path).context(format!(
                    "unable to read blob from '{}'",
                    path.as_ref().display()
                ))?;
                Ok(FileUploadJob::Blob { data })
            } else {
                slice_file_into_blobs(&path, size, specific_size)
            }
        }
    }
}

/// Slices a file into multiple blobs of the specified size.
fn slice_file_into_blobs(
    path: impl AsRef<Path>,
    file_size: u64,
    slice_size: u64,
) -> anyhow::Result<FileUploadJob> {
    anyhow::ensure!(slice_size > 0, "slice size must be greater than 0");

    let path = path.as_ref();
    let mut file = std::fs::File::open(path).context(format!(
        "unable to open file '{}' for slicing",
        path.display()
    ))?;

    let num_slices = file_size.div_ceil(slice_size);
    let mut slices = Vec::with_capacity(usize::try_from(num_slices).expect("num_slices too large"));
    let slice_usize = usize::try_from(slice_size).expect("slice_size too large");

    let mut remaining = usize::try_from(file_size).expect("file_size too large");

    loop {
        let mut buffer = Vec::with_capacity(slice_usize.min(remaining));
        // Unsafe is used here to set the the length of the buffer without having to initialize it
        // needlessly (this could be a very large buffer.)
        let bytes_read = unsafe {
            buffer.set_len(buffer.capacity());
            file.read(&mut buffer)
                .context(format!("error reading from file '{}'", path.display()))?
        };

        if bytes_read == 0 {
            assert!(remaining == 0, "unexpected EOF while slicing file");
            break;
        }

        slices.push(buffer[..bytes_read].to_vec());
        remaining -= bytes_read;
    }

    Ok(FileUploadJob::SlicedBlobs { slice_size, slices })
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use tempfile::NamedTempFile;

    use super::*;

    /// Helper to create a temporary file with the given content.
    fn create_temp_file(content: &[u8]) -> NamedTempFile {
        let mut file = NamedTempFile::new().expect("failed to create temp file");
        file.write_all(content)
            .expect("failed to write to temp file");
        file.flush().expect("failed to flush temp file");
        file
    }

    #[test]
    fn test_slice_file_evenly_divisible() {
        // Create a 100-byte file and slice it into 25-byte chunks
        let content = vec![0u8; 100];
        let file = create_temp_file(&content);

        let result = slice_file_into_blobs(file.path(), 100, 25).expect("slicing failed");

        match result {
            FileUploadJob::SlicedBlobs { slice_size, slices } => {
                assert_eq!(slice_size, 25);
                assert_eq!(slices.len(), 4);
                for slice in slices {
                    assert_eq!(slice.len(), 25);
                }
            }
            _ => panic!("expected SlicedBlobs"),
        }
    }

    #[test]
    fn test_slice_file_with_remainder() {
        // Create a 100-byte file and slice it into 30-byte chunks
        // Should result in 3 full slices (30 bytes each) and 1 partial slice (10 bytes)
        #[allow(clippy::cast_possible_truncation)]
        let content: Vec<u8> = (0..100).map(|i| i as u8).collect();
        let file = create_temp_file(&content);

        let result = slice_file_into_blobs(file.path(), 100, 30).expect("slicing failed");

        match result {
            FileUploadJob::SlicedBlobs { slice_size, slices } => {
                assert_eq!(slice_size, 30);
                assert_eq!(slices.len(), 4);
                assert_eq!(slices[0].len(), 30);
                assert_eq!(slices[1].len(), 30);
                assert_eq!(slices[2].len(), 30);
                assert_eq!(slices[3].len(), 10); // Last slice is smaller

                // Verify data integrity
                let reconstructed: Vec<u8> = slices.into_iter().flatten().collect();
                assert_eq!(reconstructed, content);
            }
            _ => panic!("expected SlicedBlobs"),
        }
    }

    #[test]
    fn test_slice_file_single_slice() {
        // File size equals slice size
        let content = vec![42u8; 50];
        let file = create_temp_file(&content);

        let result = slice_file_into_blobs(file.path(), 50, 50).expect("slicing failed");

        match result {
            FileUploadJob::SlicedBlobs { slice_size, slices } => {
                assert_eq!(slice_size, 50);
                assert_eq!(slices.len(), 1);
                assert_eq!(slices[0], content);
            }
            _ => panic!("expected SlicedBlobs"),
        }
    }

    #[test]
    fn test_slice_file_smaller_than_slice_size() {
        // File smaller than slice size
        let content = vec![99u8; 10];
        let file = create_temp_file(&content);

        let result = slice_file_into_blobs(file.path(), 10, 100).expect("slicing failed");

        match result {
            FileUploadJob::SlicedBlobs { slice_size, slices } => {
                assert_eq!(slice_size, 100);
                assert_eq!(slices.len(), 1);
                assert_eq!(slices[0], content);
            }
            _ => panic!("expected SlicedBlobs"),
        }
    }

    #[test]
    fn test_slice_file_empty() {
        // Empty file
        let content = vec![];
        let file = create_temp_file(&content);

        let result = slice_file_into_blobs(file.path(), 0, 100).expect("slicing failed");

        match result {
            FileUploadJob::SlicedBlobs { slice_size, slices } => {
                assert_eq!(slice_size, 100);
                assert_eq!(slices.len(), 0);
            }
            _ => panic!("expected SlicedBlobs"),
        }
    }

    #[test]
    fn test_slice_file_very_small_slices() {
        // Slice a 10-byte file into 1-byte chunks
        let content: Vec<u8> = (0..10).collect();
        let file = create_temp_file(&content);

        let result = slice_file_into_blobs(file.path(), 10, 1).expect("slicing failed");

        match result {
            FileUploadJob::SlicedBlobs { slice_size, slices } => {
                assert_eq!(slice_size, 1);
                assert_eq!(slices.len(), 10);
                for (i, slice) in slices.iter().enumerate() {
                    assert_eq!(slice.len(), 1);
                    #[allow(clippy::cast_possible_truncation)]
                    {
                        assert_eq!(slice[0], i as u8);
                    }
                }
            }
            _ => panic!("expected SlicedBlobs"),
        }
    }

    #[test]
    fn test_slice_file_zero_slice_size_error() {
        // Zero slice size should error
        let content = vec![1u8; 10];
        let file = create_temp_file(&content);

        let result = slice_file_into_blobs(file.path(), 10, 0);

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("slice size must be greater than 0")
        );
    }

    #[test]
    fn test_slice_file_data_integrity() {
        // Test with varying patterns to ensure data integrity across slicing
        #[allow(clippy::cast_possible_truncation)]
        let content: Vec<u8> = (0..1000).map(|i| ((i * 7) % 256) as u8).collect();
        let file = create_temp_file(&content);

        let result = slice_file_into_blobs(file.path(), 1000, 77).expect("slicing failed");

        match result {
            FileUploadJob::SlicedBlobs {
                slice_size: _,
                slices,
            } => {
                // Reconstruct the original data
                let reconstructed: Vec<u8> = slices.into_iter().flatten().collect();
                assert_eq!(reconstructed, content);
            }
            _ => panic!("expected SlicedBlobs"),
        }
    }

    #[test]
    fn test_slice_file_large_file() {
        // Test with a larger file (1 MB sliced into 64 KB chunks)
        let size = 1024 * 1024; // 1 MB
        let slice_size = 64 * 1024; // 64 KB
        #[allow(clippy::cast_possible_truncation)]
        let content: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
        let file = create_temp_file(&content);

        let result =
            slice_file_into_blobs(file.path(), size as u64, slice_size).expect("slicing failed");

        match result {
            FileUploadJob::SlicedBlobs {
                slice_size: returned_slice_size,
                slices,
            } => {
                assert_eq!(returned_slice_size, slice_size);
                assert_eq!(slices.len(), 16); // 1 MB / 64 KB = 16

                // Verify all slices are the correct size
                for slice in &slices {
                    #[allow(clippy::cast_possible_truncation)]
                    {
                        assert_eq!(slice.len(), slice_size as usize);
                    }
                }

                // Verify data integrity
                let reconstructed: Vec<u8> = slices.into_iter().flatten().collect();
                assert_eq!(reconstructed.len(), size);
            }
            _ => panic!("expected SlicedBlobs"),
        }
    }

    #[test]
    fn test_slice_file_nonexistent_file() {
        // Try to slice a file that doesn't exist
        let result = slice_file_into_blobs("/nonexistent/path/to/file.bin", 100, 50);

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("unable to open file")
        );
    }
}

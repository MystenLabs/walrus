use std::{io::Read, path::Path, str::FromStr};

use anyhow::Context;
use serde::{Deserialize, Serialize};

/// A blob slicing strategy. Blob slicing can be used to split blobs into smaller chunks for various
/// reasons.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SliceSize {
    /// No blob slicing.
    Disabled,
    /// Slice blobs into max_blob_size chunks. (Based on the current network committee
    /// configuration.)
    Auto,
    /// Slice blobs into chunks of the given size (in bytes).
    Specific(u64),
}

impl Default for SliceSize {
    fn default() -> Self {
        Self::Disabled
    }
}

impl FromStr for SliceSize {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "auto" => Ok(SliceSize::Auto),
            "disabled" => Ok(SliceSize::Disabled),
            _ => {
                // Try to parse as a number
                let size = s.parse::<u64>().context(
                    "slice size must be either 'auto', 'disabled', or a positive integer",
                )?;
                Ok(SliceSize::Specific(size))
            }
        }
    }
}

impl std::fmt::Display for SliceSize {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SliceSize::Disabled => write!(f, "disabled"),
            SliceSize::Auto => write!(f, "auto"),
            SliceSize::Specific(size) => write!(f, "{}", size),
        }
    }
}

/// Represents either a single full blob or a large file sliced into multiple blobs.
#[derive(Debug)]
pub enum BlobUploadJob {
    /// A single full blob.
    Blob { data: Vec<u8> },
    /// A single file sliced into multiple blobs.
    SlicedBlobs {
        /// Slice size used to split the blobs. The last blob may be smaller than this size.
        slice_size: u64,
        /// The blob slices.
        slices: Vec<Vec<u8>>,
    },
}

impl BlobUploadJob {
    pub fn total_bytes(&self) -> u64 {
        match self {
            BlobUploadJob::Blob { data } => u64::try_from(data.len()).expect("blob size too large"),
            BlobUploadJob::SlicedBlobs { slices, .. } => {
                u64::try_from(slices.iter().map(|s| s.len()).sum::<usize>())
                    .expect("sliced blobs size too large")
            }
        }
    }
    pub fn iterate_slices(&self) -> Box<dyn Iterator<Item = &[u8]> + '_> {
        match self {
            BlobUploadJob::Blob { data } => Box::new(std::iter::once(data.as_slice())),
            BlobUploadJob::SlicedBlobs { slices, .. } => {
                Box::new(slices.iter().map(|s| s.as_slice()))
            }
        }
    }
}

pub fn read_blob_from_file(
    path: impl AsRef<Path>,
    slice_size: SliceSize,
    network_max_blob_size: u64,
) -> anyhow::Result<BlobUploadJob> {
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
            Ok(BlobUploadJob::Blob { data })
        }
        SliceSize::Auto => {
            if size <= network_max_blob_size {
                let data = std::fs::read(&path).context(format!(
                    "unable to read blob from '{}'",
                    path.as_ref().display()
                ))?;
                Ok(BlobUploadJob::Blob { data })
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
                Ok(BlobUploadJob::Blob { data })
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
) -> anyhow::Result<BlobUploadJob> {
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

    Ok(BlobUploadJob::SlicedBlobs { slice_size, slices })
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
            BlobUploadJob::SlicedBlobs { slice_size, slices } => {
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
            BlobUploadJob::SlicedBlobs { slice_size, slices } => {
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
            BlobUploadJob::SlicedBlobs { slice_size, slices } => {
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
            BlobUploadJob::SlicedBlobs { slice_size, slices } => {
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
            BlobUploadJob::SlicedBlobs { slice_size, slices } => {
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
            BlobUploadJob::SlicedBlobs { slice_size, slices } => {
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
            BlobUploadJob::SlicedBlobs {
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
            BlobUploadJob::SlicedBlobs {
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

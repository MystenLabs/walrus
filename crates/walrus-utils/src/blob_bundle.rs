// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{
    cell::RefCell,
    collections::HashMap,
    io::{self, Cursor, Read, Write},
    num::NonZeroU16,
};

use bytes::{BufMut, Bytes, BytesMut};
use crc32fast::Hasher;
use thiserror::Error;

/// Current version of the format
const FORMAT_VERSION: u32 = 1;

/// Byte sizes for serialized types
const MAGIC_SIZE: usize = 4;
const VERSION_SIZE: usize = 4;
const U32_SIZE: usize = 4;
const U64_SIZE: usize = 8;
const CRC32_SIZE: usize = 4;

/// Magic number for the blob bundle header
const HEADER_MAGIC: [u8; MAGIC_SIZE] = [0x57, 0x4C, 0x42, 0x44]; // "WLBD"

/// Magic number for the blob bundle footer
const FOOTER_MAGIC: [u8; MAGIC_SIZE] = [0x44, 0x42, 0x4C, 0x57]; // "DBLW" (reversed)

/// Error types for blob bundle operations
#[derive(Debug, Error)]
pub enum BlobBundleError {
    #[error("Invalid magic number in header")]
    InvalidHeaderMagic,

    #[error("Invalid magic number in footer")]
    InvalidFooterMagic,

    #[error("Unsupported format version: {0}")]
    UnsupportedVersion(u32),

    #[error("Entry CRC32 mismatch: expected {expected:#010x}, got {actual:#010x}")]
    EntryCrcMismatch { expected: u32, actual: u32 },

    #[error("Footer CRC32 is not set")]
    FooterCrcNotSet,

    #[error("Footer CRC32 mismatch: expected {expected:#010x}, got {actual:#010x}")]
    FooterCrcMismatch { expected: u32, actual: u32 },

    #[error("Index entry not found: {0}")]
    EntryNotFound(String),

    #[error("Invalid format structure")]
    InvalidFormat,

    #[error("Blob size {size} bytes exceeds maximum {max_size} bytes for {n_shards} shards")]
    BlobSizeExceeded {
        size: u64,
        max_size: u64,
        n_shards: u16,
    },

    #[error("IO error: {0}")]
    Io(#[from] io::Error),
}

/// Result type for blob bundle operations
pub type Result<T> = std::result::Result<T, BlobBundleError>;

/// Header structure for the blob bundle format
#[derive(Debug, Clone)]
struct Header {
    magic: [u8; MAGIC_SIZE],
    version: u32,
}

impl Header {
    fn new() -> Self {
        Self {
            magic: HEADER_MAGIC,
            version: FORMAT_VERSION,
        }
    }

    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_all(&self.magic)?;
        writer.write_all(&self.version.to_le_bytes())?;
        Ok(())
    }

    fn read_from<R: Read>(reader: &mut R) -> Result<Self> {
        let mut magic = [0u8; MAGIC_SIZE];
        reader.read_exact(&mut magic)?;

        if magic != HEADER_MAGIC {
            return Err(BlobBundleError::InvalidHeaderMagic);
        }

        let mut version_bytes = [0u8; VERSION_SIZE];
        reader.read_exact(&mut version_bytes)?;
        let version = u32::from_le_bytes(version_bytes);

        if version != FORMAT_VERSION {
            return Err(BlobBundleError::UnsupportedVersion(version));
        }

        Ok(Self { magic, version })
    }

    fn size() -> usize {
        MAGIC_SIZE + VERSION_SIZE // magic + version
    }
}

/// Index entry for each data segment
#[derive(Debug, Clone)]
pub struct IndexEntry {
    pub id: String,
    pub offset: u64,
    pub length: u64,
    pub crc32: u32,
}

impl IndexEntry {
    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        // Write ID length and ID
        let id_bytes = self.id.as_bytes();
        let id_len = u32::try_from(id_bytes.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "ID length too large"))?;
        writer.write_all(&id_len.to_le_bytes())?;
        writer.write_all(id_bytes)?;

        // Write offset, length, and CRC32
        writer.write_all(&self.offset.to_le_bytes())?;
        writer.write_all(&self.length.to_le_bytes())?;
        writer.write_all(&self.crc32.to_le_bytes())?;

        Ok(())
    }

    fn read_from<R: Read>(reader: &mut R) -> Result<Self> {
        // Read ID length and ID
        let mut id_len_bytes = [0u8; U32_SIZE];
        reader.read_exact(&mut id_len_bytes)?;
        let id_len = u32::from_le_bytes(id_len_bytes) as usize;

        let mut id_bytes = vec![0u8; id_len];
        reader.read_exact(&mut id_bytes)?;
        let id = String::from_utf8(id_bytes).map_err(|_| BlobBundleError::InvalidFormat)?;

        // Read offset, length, and CRC32
        let mut offset_bytes = [0u8; U64_SIZE];
        reader.read_exact(&mut offset_bytes)?;
        let offset = u64::from_le_bytes(offset_bytes);

        let mut length_bytes = [0u8; U64_SIZE];
        reader.read_exact(&mut length_bytes)?;
        let length = u64::from_le_bytes(length_bytes);

        let mut crc32_bytes = [0u8; CRC32_SIZE];
        reader.read_exact(&mut crc32_bytes)?;
        let crc32 = u32::from_le_bytes(crc32_bytes);

        Ok(Self {
            id,
            offset,
            length,
            crc32,
        })
    }
}

/// Footer structure for the blob bundle format
#[derive(Debug, Clone)]
struct Footer {
    magic: [u8; MAGIC_SIZE],
    version: u32,
    index_offset: u64,
    index_entries: u32,
    footer_crc32: Option<u32>,
}

impl Footer {
    fn new(index_offset: u64, index_entries: u32) -> Self {
        Self {
            magic: FOOTER_MAGIC,
            version: FORMAT_VERSION,
            index_offset,
            index_entries,
            footer_crc32: None, // Will be calculated before writing
        }
    }

    /// Validates that the footer's CRC32 checksum matches the calculated value
    pub fn validate_crc32(&self) -> Result<()> {
        if self.footer_crc32.is_none() {
            return Err(BlobBundleError::FooterCrcNotSet);
        }

        let footer_crc32 = self.footer_crc32.expect("Footer CRC32 must be set");

        let mut hasher = Hasher::new();
        hasher.update(&self.magic);
        hasher.update(&self.version.to_le_bytes());
        hasher.update(&self.index_offset.to_le_bytes());
        hasher.update(&self.index_entries.to_le_bytes());
        let calculated_crc32 = hasher.finalize();

        if calculated_crc32 != footer_crc32 {
            return Err(BlobBundleError::FooterCrcMismatch {
                expected: footer_crc32,
                actual: calculated_crc32,
            });
        }

        Ok(())
    }

    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        // Calculate CRC32 of footer data (excluding the CRC32 field itself)
        let mut hasher = Hasher::new();
        hasher.update(&self.magic);
        hasher.update(&self.version.to_le_bytes());
        hasher.update(&self.index_offset.to_le_bytes());
        hasher.update(&self.index_entries.to_le_bytes());
        let footer_crc32 = hasher.finalize();

        writer.write_all(&self.magic)?;
        writer.write_all(&self.version.to_le_bytes())?;
        writer.write_all(&self.index_offset.to_le_bytes())?;
        writer.write_all(&self.index_entries.to_le_bytes())?;
        writer.write_all(&footer_crc32.to_le_bytes())?;

        Ok(())
    }

    fn read_from<R: Read>(reader: &mut R) -> Result<Self> {
        let mut magic = [0u8; MAGIC_SIZE];
        reader.read_exact(&mut magic)?;

        if magic != FOOTER_MAGIC {
            return Err(BlobBundleError::InvalidFooterMagic);
        }

        let mut version_bytes = [0u8; VERSION_SIZE];
        reader.read_exact(&mut version_bytes)?;
        let version = u32::from_le_bytes(version_bytes);

        if version != FORMAT_VERSION {
            return Err(BlobBundleError::UnsupportedVersion(version));
        }

        let mut index_offset_bytes = [0u8; U64_SIZE];
        reader.read_exact(&mut index_offset_bytes)?;
        let index_offset = u64::from_le_bytes(index_offset_bytes);

        let mut index_entries_bytes = [0u8; U32_SIZE];
        reader.read_exact(&mut index_entries_bytes)?;
        let index_entries = u32::from_le_bytes(index_entries_bytes);

        let mut footer_crc32_bytes = [0u8; CRC32_SIZE];
        reader.read_exact(&mut footer_crc32_bytes)?;
        let footer_crc32 = u32::from_le_bytes(footer_crc32_bytes);

        let footer = Self {
            magic,
            version,
            index_offset,
            index_entries,
            footer_crc32: Some(footer_crc32),
        };

        // Validate CRC32 using the helper method
        footer.validate_crc32()?;

        Ok(footer)
    }

    fn size() -> usize {
        // magic + version + index_offset + index_entries + crc32
        MAGIC_SIZE + VERSION_SIZE + U64_SIZE + U32_SIZE + CRC32_SIZE
    }
}

/// Result of building a blob bundle, containing the bundled bytes and an index map
#[derive(Debug)]
pub struct BlobBundleBuildResult {
    /// The complete blob bundle as bytes
    pub bytes: Bytes,
    /// Map of id to (offset, length) in insertion order
    pub index_map: Vec<(String, (u64, u64))>,
}

/// Blob Bundle Format Structure:
/// ```text
/// +----------------------+
/// |      Header          |
/// +----------------------+
/// | Magic    [4 bytes]   | "WLBD" (0x57, 0x4C, 0x42, 0x44)
/// | Version  [4 bytes]   | Format version (currently 1)
/// +----------------------+
/// |    Data Section      |
/// +----------------------+
/// | Data Entry 1         | Raw bytes of first blob
/// | Data Entry 2         | Raw bytes of second blob
/// | ...                  | ...
/// | Data Entry N         | Raw bytes of Nth blob
/// +----------------------+
/// |   Index Section      |
/// +----------------------+
/// | Index Entry 1        |
/// |   ID Len [4 bytes]   |
/// |   ID     [varies]    |
/// |   Offset [8 bytes]   |
/// |   Length [8 bytes]   |
/// |   CRC32  [4 bytes]   |
/// +----------------------+
/// | Index Entry 2        |
/// | ...                  |
/// | Index Entry N        |
/// +----------------------+
/// |      Footer          |
/// +----------------------+
/// | Magic    [4 bytes]   | "DBLW" (0x44, 0x42, 0x4C, 0x57) - reversed
/// | Version  [4 bytes]   | Format version (must match header)
/// | Index Offset         |
/// |          [8 bytes]   | Byte offset where index section starts
/// | Index Entries        |
/// |          [4 bytes]   | Number of entries in index
/// | Footer CRC32         |
/// |          [4 bytes]   | CRC32 of footer data (excluding this field)
/// +----------------------+
/// ```
///
/// Builder for creating blob bundle with the specified format
#[derive(Debug)]
pub struct BlobBundleBuilder {
    entries: Vec<(String, Bytes)>,
    n_shards: NonZeroU16,
}

impl BlobBundleBuilder {
    /// Create a new builder with the specified number of shards
    pub fn new(n_shards: NonZeroU16) -> Self {
        Self {
            entries: Vec::new(),
            n_shards,
        }
    }

    /// Add a data entry with an ID
    pub fn add(&mut self, id: impl Into<String>, data: impl Into<Bytes>) -> &mut Self {
        self.entries.push((id.into(), data.into()));
        self
    }

    /// Get the current size of the bundle if Build() were called now.
    /// This is useful for checking whether to stop adding entries and create a new bundle.
    ///
    /// Returns the total size in bytes including:
    /// - Header (magic + version)
    /// - All data entries
    /// - Index entries (with ID strings, offsets, lengths, and CRC32s)
    /// - Footer (magic + version + index_offset + index_entries + crc32)
    pub fn current_size(&self) -> u64 {
        // Calculate data size
        let data_size: u64 = self.entries.iter().map(|(_, data)| data.len() as u64).sum();

        // Calculate header size
        let header_size = Header::size() as u64;

        // Calculate index overhead
        // Each index entry has: id_len (4) + id_bytes + offset (8) + length (8) + crc32 (4)
        let index_entry_overhead_per_item = U32_SIZE + U64_SIZE + U64_SIZE + CRC32_SIZE;
        let index_overhead: u64 = self
            .entries
            .iter()
            .map(|(id, _)| id.len() + index_entry_overhead_per_item)
            .sum::<usize>() as u64;

        // Calculate footer size
        let footer_size = Footer::size() as u64;

        // Return total size
        header_size + data_size + index_overhead + footer_size
    }

    /// Build the blob bundle with size check for RS2 encoding
    pub fn build(self) -> Result<BlobBundleBuildResult> {
        // First, calculate the total size to check against max blob size
        let data_size: u64 = self.entries.iter().map(|(_, data)| data.len() as u64).sum();

        // Calculate the overhead: header + index + footer
        let header_size = Header::size() as u64;

        // id_len + offset + length + crc32
        let index_entry_overhead_per_item = U32_SIZE + U64_SIZE + U64_SIZE + CRC32_SIZE;
        let index_overhead: u64 = self
            .entries
            .iter()
            .map(|(id, _)| id.len() + index_entry_overhead_per_item)
            .sum::<usize>() as u64;
        let footer_size = Footer::size() as u64;

        let total_size = header_size + data_size + index_overhead + footer_size;

        // Check against max blob size for RS2 encoding
        let max_size = walrus_core::encoding::max_blob_size_for_n_shards(
            self.n_shards,
            walrus_core::EncodingType::RS2,
        );

        if total_size > max_size {
            return Err(BlobBundleError::BlobSizeExceeded {
                size: total_size,
                max_size,
                n_shards: self.n_shards.get(),
            });
        }

        let mut buffer = BytesMut::new();
        let mut index_entries = Vec::new();
        let mut index_map = Vec::new();

        // Write header
        let header = Header::new();
        let mut header_bytes = Vec::new();
        header.write_to(&mut header_bytes)?;
        buffer.put_slice(&header_bytes);

        // Write data entries and build index
        let data_start_offset = Header::size() as u64;
        let mut current_offset = data_start_offset;

        for (id, data) in self.entries {
            // Calculate CRC32 for the data
            let mut hasher = Hasher::new();
            hasher.update(&data);
            let crc32 = hasher.finalize();

            let data_len = data.len() as u64;

            // Add to index map (preserving insertion order)
            index_map.push((id.clone(), (current_offset, data_len)));

            // Create index entry
            let entry = IndexEntry {
                id,
                offset: current_offset,
                length: data_len,
                crc32,
            };
            index_entries.push(entry);

            // Write data
            buffer.put(data);
            current_offset += data_len;
        }

        // Write index
        let index_offset = current_offset;
        for entry in &index_entries {
            let mut entry_bytes = Vec::new();
            entry.write_to(&mut entry_bytes)?;
            buffer.put_slice(&entry_bytes);
        }

        // Write footer
        let index_entries_count =
            u32::try_from(index_entries.len()).map_err(|_| BlobBundleError::InvalidFormat)?;
        let footer = Footer::new(index_offset, index_entries_count);
        let mut footer_bytes = Vec::new();
        footer.write_to(&mut footer_bytes)?;
        buffer.put_slice(&footer_bytes);

        Ok(BlobBundleBuildResult {
            bytes: buffer.freeze(),
            index_map,
        })
    }
}

impl Default for BlobBundleBuilder {
    fn default() -> Self {
        // Default to 1000 shards
        // SAFETY: 1000 is a non-zero value, so this is guaranteed to succeed
        Self::new(NonZeroU16::new(1000).expect("1000 is non-zero"))
    }
}

/// Reader for blob bundle format with lazy index parsing
#[derive(Debug)]
pub struct BlobBundleReader {
    data: Bytes,
    // Lazy index parsing
    index: RefCell<Option<HashMap<String, IndexEntry>>>,
}

impl BlobBundleReader {
    /// Create a new reader from blob bundle bytes
    ///
    /// This method validates:
    /// - Header magic number matches expected value
    /// - Footer magic number matches expected value
    /// - Header and footer versions match
    /// - Footer CRC32 is valid
    ///
    /// Note: The index is NOT parsed until needed by operations that require it.
    pub fn new(data: Bytes) -> Result<Self> {
        // Minimum size check: header + footer at least
        if data.len() < Header::size() + Footer::size() {
            return Err(BlobBundleError::InvalidFormat);
        }

        let mut cursor = Cursor::new(&data);

        // Read and verify header
        let header = Header::read_from(&mut cursor)?;

        // Header validation is already done in Header::read_from
        // which checks magic and version

        // Read footer to get index location
        let footer_offset = data.len() - Footer::size();
        cursor.set_position(footer_offset as u64);
        let footer = Footer::read_from(&mut cursor)?;

        // Footer validation is already done in Footer::read_from
        // which checks magic, version, and CRC32

        // Verify that header and footer versions match
        if header.version != footer.version {
            return Err(BlobBundleError::InvalidFormat);
        }

        // Validate index offset is within bounds
        // Note: index_offset can equal footer_offset when there are no entries
        if footer.index_offset < Header::size() as u64 || footer.index_offset > footer_offset as u64
        {
            return Err(BlobBundleError::InvalidFormat);
        }

        Ok(Self {
            data,
            index: RefCell::new(None),
        })
    }

    /// Parse the index if it hasn't been parsed yet
    fn ensure_index_parsed(&self) -> Result<()> {
        if self.index.borrow().is_some() {
            return Ok(());
        }

        // Get footer to find index location
        let footer_offset = self.data.len() - Footer::size();
        let mut cursor = Cursor::new(&self.data);
        cursor.set_position(footer_offset as u64);
        let footer = Footer::read_from(&mut cursor)?;

        // Parse index
        cursor.set_position(footer.index_offset);
        let mut index = HashMap::new();

        for _ in 0..footer.index_entries {
            let entry = IndexEntry::read_from(&mut cursor)?;

            // Validate that entry offset and length are within data section bounds
            let entry_end = entry.offset.saturating_add(entry.length);
            if entry.offset < Header::size() as u64 || entry_end > footer.index_offset {
                return Err(BlobBundleError::InvalidFormat);
            }

            index.insert(entry.id.clone(), entry);
        }

        *self.index.borrow_mut() = Some(index);
        Ok(())
    }

    /// Get data by ID
    pub fn get(&self, id: &str) -> Result<Bytes> {
        self.ensure_index_parsed()?;

        let index = self.index.borrow();
        let index = index
            .as_ref()
            .expect("index should be populated after ensure_index_parsed");

        let entry = index
            .get(id)
            .ok_or_else(|| BlobBundleError::EntryNotFound(id.to_string()))?;

        // Extract data slice
        let start = usize::try_from(entry.offset).map_err(|_| BlobBundleError::InvalidFormat)?;
        let length = usize::try_from(entry.length).map_err(|_| BlobBundleError::InvalidFormat)?;
        let end = start
            .checked_add(length)
            .ok_or(BlobBundleError::InvalidFormat)?;

        if end > self.data.len() {
            return Err(BlobBundleError::InvalidFormat);
        }

        let data = self.data.slice(start..end);

        // Verify CRC32
        let mut hasher = Hasher::new();
        hasher.update(&data);
        let calculated_crc32 = hasher.finalize();

        if calculated_crc32 != entry.crc32 {
            return Err(BlobBundleError::EntryCrcMismatch {
                expected: entry.crc32,
                actual: calculated_crc32,
            });
        }

        Ok(data)
    }

    /// List all IDs in the blob bundle
    pub fn list_ids(&self) -> Result<Vec<String>> {
        self.ensure_index_parsed()?;

        let index = self.index.borrow();
        let index = index
            .as_ref()
            .expect("index should be populated after ensure_index_parsed");

        Ok(index.keys().cloned().collect())
    }

    /// Get index entry by ID
    pub fn get_entry(&self, id: &str) -> Result<Option<IndexEntry>> {
        self.ensure_index_parsed()?;

        let index = self.index.borrow();
        let index = index
            .as_ref()
            .expect("index should be populated after ensure_index_parsed");

        Ok(index.get(id).cloned())
    }

    /// Get all index entries
    pub fn entries(&self) -> Result<Vec<(String, IndexEntry)>> {
        self.ensure_index_parsed()?;

        let index = self.index.borrow();
        let index = index
            .as_ref()
            .expect("index should be populated after ensure_index_parsed");

        Ok(index.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
    }

    /// Read raw bytes from the blob bundle at the specified offset and length.
    ///
    /// This method provides direct access to the underlying data without any CRC32 verification.
    /// It's useful for reading specific portions of the blob bundle, such as when you have
    /// the offset and length from the index map.
    ///
    /// # Arguments
    /// * `offset` - The byte offset to start reading from
    /// * `length` - The number of bytes to read
    ///
    /// # Returns
    /// * `Ok(Bytes)` - The requested bytes if the range is valid
    /// * `Err(InvalidFormat)` - If the requested range is out of bounds
    pub fn read_range(&self, offset: u64, length: u64) -> Result<Bytes> {
        let start = usize::try_from(offset).map_err(|_| BlobBundleError::InvalidFormat)?;
        let length_usize = usize::try_from(length).map_err(|_| BlobBundleError::InvalidFormat)?;
        let end = start.saturating_add(length_usize);

        if end > self.data.len() {
            return Err(BlobBundleError::InvalidFormat);
        }

        Ok(self.data.slice(start..end))
    }

    /// Get the total size of the blob bundle
    pub fn total_size(&self) -> usize {
        self.data.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_blob_bundle_roundtrip() {
        // Create test data
        let data1 = Bytes::from("Hello, World!");
        let data2 = Bytes::from("This is test data");
        let data3 = Bytes::from(vec![1, 2, 3, 4, 5]);

        // Build blob bundle
        let mut builder = BlobBundleBuilder::new(NonZeroU16::new(10).expect("10 is non-zero"));
        builder
            .add("entry1", data1.clone())
            .add("entry2", data2.clone())
            .add("entry3", data3.clone());

        let result = builder.build().expect("Failed to build bundle");

        // Verify index map preserves insertion order
        assert_eq!(result.index_map.len(), 3);
        assert_eq!(result.index_map[0].0, "entry1");
        assert_eq!(result.index_map[1].0, "entry2");
        assert_eq!(result.index_map[2].0, "entry3");

        // Read blob bundle
        let reader = BlobBundleReader::new(result.bytes).expect("Failed to create reader");

        // Verify all entries
        assert_eq!(reader.get("entry1").expect("Failed to get entry1"), data1);
        assert_eq!(reader.get("entry2").expect("Failed to get entry2"), data2);
        assert_eq!(reader.get("entry3").expect("Failed to get entry3"), data3);

        // Verify IDs
        let mut ids = reader.list_ids().expect("Failed to list IDs");
        ids.sort();
        assert_eq!(ids, vec!["entry1", "entry2", "entry3"]);
    }

    #[test]
    fn test_invalid_crc() {
        // Create test data
        let data = Bytes::from("Test data");

        // Build blob bundle
        let mut builder = BlobBundleBuilder::new(NonZeroU16::new(10).expect("10 is non-zero"));
        builder.add("test", data);

        let mut bundled = builder
            .build()
            .expect("Failed to build bundle")
            .bytes
            .to_vec();

        // Corrupt the data (but not the index or footer)
        bundled[10] ^= 0xFF;

        // Try to read
        let reader = BlobBundleReader::new(Bytes::from(bundled)).expect("Failed to create reader");

        // Should fail with CRC mismatch
        match reader.get("test") {
            Err(BlobBundleError::EntryCrcMismatch { .. }) => {}
            _ => panic!("Expected EntryCrcMismatch error"),
        }
    }

    #[test]
    fn test_empty_builder() {
        let builder = BlobBundleBuilder::new(NonZeroU16::new(10).expect("10 is non-zero"));
        let result = builder.build().expect("Failed to build bundle");

        assert_eq!(result.index_map.len(), 0);
        let reader = BlobBundleReader::new(result.bytes).expect("Failed to create reader");
        assert_eq!(reader.list_ids().expect("Failed to list IDs").len(), 0);
    }

    #[test]
    fn test_entry_not_found() {
        let mut builder = BlobBundleBuilder::new(NonZeroU16::new(10).expect("10 is non-zero"));
        builder.add("exists", Bytes::from("data"));

        let result = builder.build().expect("Failed to build bundle");
        let reader = BlobBundleReader::new(result.bytes).expect("Failed to create reader");

        match reader.get("not_exists") {
            Err(BlobBundleError::EntryNotFound(id)) => {
                assert_eq!(id, "not_exists");
            }
            _ => panic!("Expected EntryNotFound error"),
        }
    }

    #[test]
    fn test_footer_validate_crc32_valid() {
        // Calculate the correct CRC32
        let mut hasher = Hasher::new();
        hasher.update(&FOOTER_MAGIC);
        hasher.update(&FORMAT_VERSION.to_le_bytes());
        hasher.update(&1024u64.to_le_bytes());
        hasher.update(&5u32.to_le_bytes());
        let correct_crc32 = hasher.finalize();

        // Create footer with correct CRC32
        let footer_with_crc = Footer {
            magic: FOOTER_MAGIC,
            version: FORMAT_VERSION,
            index_offset: 1024,
            index_entries: 5,
            footer_crc32: Some(correct_crc32),
        };

        // Validation should succeed
        assert!(footer_with_crc.validate_crc32().is_ok());
    }

    #[test]
    fn test_footer_validate_crc32_invalid() {
        // Create a footer with empty CRC32
        let footer = Footer {
            magic: FOOTER_MAGIC,
            version: FORMAT_VERSION,
            index_offset: 1024,
            index_entries: 5,
            footer_crc32: None,
        };

        // Validation should fail
        match footer.validate_crc32() {
            Err(BlobBundleError::FooterCrcNotSet) => {}
            _ => panic!("Expected FooterCrcNotSet error"),
        }

        // Create a footer with incorrect CRC32
        let footer = Footer {
            magic: FOOTER_MAGIC,
            version: FORMAT_VERSION,
            index_offset: 1024,
            index_entries: 5,
            footer_crc32: Some(0xDEADBEEF), // Wrong CRC32
        };

        // Validation should fail
        match footer.validate_crc32() {
            Err(BlobBundleError::FooterCrcMismatch { expected, actual }) => {
                assert_eq!(expected, 0xDEADBEEF);
                // The actual CRC32 should be calculated correctly
                let mut hasher = Hasher::new();
                hasher.update(&FOOTER_MAGIC);
                hasher.update(&FORMAT_VERSION.to_le_bytes());
                hasher.update(&1024u64.to_le_bytes());
                hasher.update(&5u32.to_le_bytes());
                assert_eq!(actual, hasher.finalize());
            }
            _ => panic!("Expected FooterCrcMismatch error"),
        }
    }

    #[test]
    fn test_footer_crc32_roundtrip() {
        // Test that write_to and read_from produce matching CRC32
        let footer = Footer::new(2048, 10);

        // Write footer to bytes
        let mut buffer = Vec::new();
        footer
            .write_to(&mut buffer)
            .expect("Failed to write footer");

        // Read it back
        let mut cursor = Cursor::new(&buffer);
        let read_footer = Footer::read_from(&mut cursor).expect("Failed to read footer");

        // The read footer should have a valid CRC32
        assert!(read_footer.validate_crc32().is_ok());

        // Values should match
        assert_eq!(read_footer.index_offset, 2048);
        assert_eq!(read_footer.index_entries, 10);
    }

    #[test]
    fn test_build_result_index_map() {
        // Create test data with known sizes
        let data1 = Bytes::from("Hello"); // 5 bytes
        let data2 = Bytes::from("World!"); // 6 bytes
        let data3 = Bytes::from("Testing"); // 7 bytes

        // Build blob bundle
        let mut builder = BlobBundleBuilder::new(NonZeroU16::new(10).expect("10 is non-zero"));
        builder
            .add("first", data1)
            .add("second", data2)
            .add("third", data3);

        let result = builder.build().expect("Failed to build bundle");

        // Verify index map has correct order and values
        assert_eq!(result.index_map.len(), 3);

        // Check IDs are in insertion order
        assert_eq!(result.index_map[0].0, "first");
        assert_eq!(result.index_map[1].0, "second");
        assert_eq!(result.index_map[2].0, "third");

        // Check lengths
        assert_eq!(result.index_map[0].1.1, 5); // "Hello" length
        assert_eq!(result.index_map[1].1.1, 6); // "World!" length
        assert_eq!(result.index_map[2].1.1, 7); // "Testing" length

        // Check offsets are sequential (header is magic + version)
        let header_size = (MAGIC_SIZE + VERSION_SIZE) as u64;
        assert_eq!(result.index_map[0].1.0, header_size); // First data starts after header
        assert_eq!(result.index_map[1].1.0, header_size + 5); // Second after first
        assert_eq!(result.index_map[2].1.0, header_size + 11); // Third after second
    }

    #[test]
    fn test_blob_size_limit() {
        // Test with small number of shards (10) which has a smaller max blob size
        let n_shards = NonZeroU16::new(10).expect("10 is non-zero");
        let mut builder = BlobBundleBuilder::new(n_shards);

        // Calculate max blob size for 10 shards
        let max_size = walrus_core::encoding::max_blob_size_for_n_shards(
            n_shards,
            walrus_core::EncodingType::RS2,
        );

        // Try to add data that would exceed the limit
        // We need to account for overhead, so use slightly less than max
        let large_data_size =
            usize::try_from(max_size).expect("max_size should fit in usize") - 1000;
        let large_data = Bytes::from(vec![0u8; large_data_size]);

        builder.add("large", large_data.clone());

        // This should succeed
        let result = builder.build();
        assert!(result.is_ok());

        // Now try with data that definitely exceeds the limit
        let mut builder2 = BlobBundleBuilder::new(n_shards);
        let too_large_data = Bytes::from(vec![
            0u8;
            usize::try_from(max_size)
                .expect("max_size should fit in usize")
                + 1
        ]);
        builder2.add("too_large", too_large_data);

        // This should fail
        let result2 = builder2.build();
        match result2 {
            Err(BlobBundleError::BlobSizeExceeded {
                size,
                max_size: max,
                n_shards: shards,
            }) => {
                assert_eq!(shards, 10);
                assert!(size > max);
            }
            _ => panic!("Expected BlobSizeExceeded error"),
        }
    }

    #[test]
    fn test_read_range() {
        // Create test data
        let data1 = Bytes::from("Hello");
        let data2 = Bytes::from("World");
        let data3 = Bytes::from("Test");

        // Build blob bundle
        let mut builder = BlobBundleBuilder::new(NonZeroU16::new(10).expect("10 is non-zero"));
        builder
            .add("first", data1.clone())
            .add("second", data2.clone())
            .add("third", data3.clone());

        let result = builder.build().expect("Failed to build bundle");
        let reader = BlobBundleReader::new(result.bytes).expect("Failed to create reader");

        // Test reading using the index map offsets
        for (id, (offset, length)) in &result.index_map {
            let data_from_range = reader
                .read_range(*offset, *length)
                .expect("Failed to read range");

            // Compare with data retrieved through the normal get method
            let data_from_get = reader.get(id).expect("Failed to get entry");
            assert_eq!(data_from_range, data_from_get);
        }

        // Test reading specific ranges
        // Read first 5 bytes from first entry (should be "Hello")
        let first_offset = result.index_map[0].1.0;
        let hello_bytes = reader
            .read_range(first_offset, 5)
            .expect("Failed to read range");
        assert_eq!(hello_bytes, Bytes::from("Hello"));

        // Read partial data from second entry
        let second_offset = result.index_map[1].1.0;
        let partial_world = reader
            .read_range(second_offset, 3)
            .expect("Failed to read range");
        assert_eq!(partial_world, Bytes::from("Wor"));

        // Test reading header (first bytes)
        let header_bytes = reader
            .read_range(0, (MAGIC_SIZE + VERSION_SIZE) as u64)
            .expect("Failed to read header bytes");
        assert_eq!(&header_bytes[0..MAGIC_SIZE], &HEADER_MAGIC);

        // Test error case: reading beyond the end
        let total_size = reader.total_size() as u64;
        let result = reader.read_range(total_size - 1, 2);
        assert!(matches!(result, Err(BlobBundleError::InvalidFormat)));

        // Test error case: offset beyond the end
        let result = reader.read_range(total_size + 1, 1);
        assert!(matches!(result, Err(BlobBundleError::InvalidFormat)));

        // Test edge case: reading zero bytes
        let empty = reader.read_range(0, 0).expect("Failed to read empty range");
        assert_eq!(empty.len(), 0);

        // Test reading exact boundary
        let all_data = reader
            .read_range(0, total_size)
            .expect("Failed to read all data");
        assert_eq!(
            all_data.len(),
            usize::try_from(total_size).expect("total_size should fit in usize")
        );
    }

    #[test]
    fn test_read_range_with_index_map() {
        // Create test data with known content
        let data1 = Bytes::from("AAAAA"); // 5 bytes
        let data2 = Bytes::from("BBBBBBBBBB"); // 10 bytes
        let data3 = Bytes::from("CCCCCCC"); // 7 bytes

        // Build blob bundle
        let mut builder = BlobBundleBuilder::new(NonZeroU16::new(10).expect("10 is non-zero"));
        builder
            .add("aaa", data1.clone())
            .add("bbb", data2.clone())
            .add("ccc", data3.clone());

        let result = builder.build().expect("Failed to build bundle");

        // Verify index map has correct offsets and lengths
        assert_eq!(result.index_map[0].0, "aaa");
        assert_eq!(result.index_map[0].1.1, 5); // length of "AAAAA"

        assert_eq!(result.index_map[1].0, "bbb");
        assert_eq!(result.index_map[1].1.1, 10); // length of "BBBBBBBBBB"

        assert_eq!(result.index_map[2].0, "ccc");
        assert_eq!(result.index_map[2].1.1, 7); // length of "CCCCCCC"

        let reader = BlobBundleReader::new(result.bytes).expect("Failed to create reader");

        // Use index_map to read each entry directly
        let entry1 = reader
            .read_range(result.index_map[0].1.0, result.index_map[0].1.1)
            .expect("Failed to read entry1 range");
        assert_eq!(entry1, data1);

        let entry2 = reader
            .read_range(result.index_map[1].1.0, result.index_map[1].1.1)
            .expect("Failed to read entry2 range");
        assert_eq!(entry2, data2);

        let entry3 = reader
            .read_range(result.index_map[2].1.0, result.index_map[2].1.1)
            .expect("Failed to read entry3 range");
        assert_eq!(entry3, data3);
    }

    #[test]
    fn test_reader_validation() {
        // Test 1: Too small data (less than header + footer)
        let small_data = Bytes::from(vec![0u8; 10]);
        let result = BlobBundleReader::new(small_data);
        assert!(matches!(result, Err(BlobBundleError::InvalidFormat)));

        // Test 2: Invalid header magic
        let invalid_header = vec![0xFFu8; 100];
        let result = BlobBundleReader::new(Bytes::from(invalid_header));
        assert!(matches!(result, Err(BlobBundleError::InvalidHeaderMagic)));

        // Test 3: Valid bundle passes all checks
        let mut builder = BlobBundleBuilder::new(NonZeroU16::new(10).expect("10 is non-zero"));
        builder.add("test", Bytes::from("data"));
        let bundle = builder.build().expect("Failed to build bundle");
        let reader = BlobBundleReader::new(bundle.bytes.clone());
        assert!(reader.is_ok());

        // Test 4: Mismatched versions between header and footer
        let mut data = bundle.bytes.to_vec();
        // Change version in header (at offset 4-7) to be different
        data[4] = 2; // Change version from 1 to 2 in header
        let result = BlobBundleReader::new(Bytes::from(data));
        // This will fail with UnsupportedVersion error first (from Header::read_from)
        assert!(matches!(
            result,
            Err(BlobBundleError::UnsupportedVersion(_))
        ));

        // Test 5: Invalid footer magic
        let mut builder = BlobBundleBuilder::new(NonZeroU16::new(10).expect("10 is non-zero"));
        builder.add("test", Bytes::from("data"));
        let bundle = builder.build().expect("Failed to build bundle");
        let mut data = bundle.bytes.to_vec();
        // Corrupt footer magic (last 24 bytes, first 4 are magic)
        let footer_start = data.len() - Footer::size();
        data[footer_start] = 0xFF;
        let result = BlobBundleReader::new(Bytes::from(data));
        assert!(matches!(result, Err(BlobBundleError::InvalidFooterMagic)));

        // Test 6: Invalid index offset in footer
        let mut builder = BlobBundleBuilder::new(NonZeroU16::new(10).expect("10 is non-zero"));
        builder.add("test", Bytes::from("data"));
        let bundle = builder.build().expect("Failed to build bundle");
        let mut data = bundle.bytes.to_vec();

        // Set index_offset to 0 (which is invalid as it's less than header size)
        // Footer layout: magic(4) + version(4) + index_offset(8) + ...
        let footer_start = data.len() - Footer::size();
        let index_offset_pos = footer_start + 8; // After magic and version
        for i in 0..8 {
            data[index_offset_pos + i] = 0;
        }

        // Need to recalculate CRC32 for the modified footer
        let mut hasher = Hasher::new();
        hasher.update(&data[footer_start..footer_start + 4]); // magic
        hasher.update(&data[footer_start + 4..footer_start + 8]); // version
        hasher.update(&data[footer_start + 8..footer_start + 16]); // index_offset
        hasher.update(&data[footer_start + 16..footer_start + 20]); // index_entries
        let new_crc = hasher.finalize();
        data[footer_start + 20..footer_start + 24].copy_from_slice(&new_crc.to_le_bytes());

        let result = BlobBundleReader::new(Bytes::from(data));
        assert!(matches!(result, Err(BlobBundleError::InvalidFormat)));
    }

    #[test]
    fn test_footer_corrupted_data() {
        // Create a valid footer and serialize it
        let footer = Footer::new(4096, 20);
        let mut buffer = Vec::new();
        footer
            .write_to(&mut buffer)
            .expect("Failed to write footer");

        // Corrupt the index_offset bytes (but not the CRC32)
        // Footer layout: magic(4) + version(4) + index_offset(8) + index_entries(4) + crc32(4)
        buffer[8] ^= 0xFF; // Flip bits in the index_offset field (starts at byte 8)

        // Try to read the corrupted footer
        let mut cursor = Cursor::new(&buffer);
        let corrupted_footer = Footer::read_from(&mut cursor);

        // Should fail with CRC mismatch
        match corrupted_footer {
            Err(BlobBundleError::FooterCrcMismatch { .. }) => {}
            _ => panic!("Expected FooterCrcMismatch error for corrupted footer"),
        }
    }

    #[test]
    fn test_current_size() {
        // Create a builder
        let mut builder = BlobBundleBuilder::new(NonZeroU16::new(10).expect("10 is non-zero"));

        // Initial size should be just header + footer
        let empty_size = builder.current_size();
        let expected_empty_size = Header::size() as u64 + Footer::size() as u64;
        assert_eq!(
            empty_size, expected_empty_size,
            "Empty builder should have minimal size"
        );

        // Add first entry
        let data1 = Bytes::from("Hello");
        builder.add("entry1", data1.clone());

        // Size should increase by data size + index entry overhead
        let size_after_first = builder.current_size();
        let index_overhead_first =
            (U32_SIZE + "entry1".len() + U64_SIZE + U64_SIZE + CRC32_SIZE) as u64;
        let expected_after_first = expected_empty_size + data1.len() as u64 + index_overhead_first;
        assert_eq!(
            size_after_first, expected_after_first,
            "Size after first entry should match expected"
        );

        // Add second entry
        let data2 = Bytes::from("World!");
        builder.add("entry2", data2.clone());

        // Size should increase by second data size + second index entry overhead
        let size_after_second = builder.current_size();
        let index_overhead_second =
            (U32_SIZE + "entry2".len() + U64_SIZE + U64_SIZE + CRC32_SIZE) as u64;
        let expected_after_second =
            expected_after_first + data2.len() as u64 + index_overhead_second;
        assert_eq!(
            size_after_second, expected_after_second,
            "Size after second entry should match expected"
        );

        // Build and verify the actual size matches
        let result = builder.build().expect("Failed to build bundle");
        let actual_size = result.bytes.len() as u64;
        assert_eq!(
            actual_size, size_after_second,
            "Built bundle size should match current_size()"
        );
    }

    #[test]
    fn test_current_size_matches_build() {
        // Test with various data sizes to ensure current_size() accurately predicts build() size
        let test_cases = vec![
            vec![("a", "x"), ("b", "yy"), ("c", "zzz")],
            vec![("single", "some data here")],
            vec![
                ("long_id_name_here", "short"),
                ("id", "very long data string that contains many characters"),
            ],
        ];

        for entries in test_cases {
            let mut builder =
                BlobBundleBuilder::new(NonZeroU16::new(100).expect("100 is non-zero"));

            for (id, data) in &entries {
                builder.add(*id, Bytes::from(*data));
            }

            let predicted_size = builder.current_size();
            let result = builder.build().expect("Failed to build bundle");
            let actual_size = result.bytes.len() as u64;

            assert_eq!(
                predicted_size, actual_size,
                "current_size() should exactly match built bundle size for entries: {:?}",
                entries
            );
        }
    }

    #[test]
    fn test_lazy_index_parsing_and_read_range_without_index() {
        // Create test data
        let data1 = Bytes::from("Hello World!");
        let data2 = Bytes::from("This is a test");
        let data3 = Bytes::from("Lazy loading works");

        // Build blob bundle
        let mut builder = BlobBundleBuilder::new(NonZeroU16::new(10).expect("10 is non-zero"));
        builder
            .add("entry1", data1.clone())
            .add("entry2", data2.clone())
            .add("entry3", data3.clone());

        let result = builder.build().expect("Failed to build bundle");
        let reader = BlobBundleReader::new(result.bytes).expect("Failed to create reader");

        // Verify that index is not parsed initially
        assert!(
            reader.index.borrow().is_none(),
            "Index should not be parsed on creation"
        );

        // Test read_range works without parsing index - read first entry
        let first_offset = result.index_map[0].1.0;
        let first_length = result.index_map[0].1.1;
        let data_from_range = reader
            .read_range(first_offset, first_length)
            .expect("read_range should work without index");
        assert_eq!(
            data_from_range, data1,
            "Data from read_range should match original"
        );

        // Verify index is still not parsed after read_range
        assert!(
            reader.index.borrow().is_none(),
            "Index should remain unparsed after read_range"
        );

        // Test read_range with partial data - read middle of second entry
        let second_offset = result.index_map[1].1.0;
        let partial_data = reader
            .read_range(second_offset + 5, 4)
            .expect("Partial read_range should work without index");
        assert_eq!(
            partial_data,
            Bytes::from("is a"),
            "Partial read should match expected substring"
        );

        // Verify index is still not parsed
        assert!(
            reader.index.borrow().is_none(),
            "Index should still not be parsed"
        );

        // Test read_range with third entry
        let third_offset = result.index_map[2].1.0;
        let third_length = result.index_map[2].1.1;
        let third_data = reader
            .read_range(third_offset, third_length)
            .expect("read_range should work for third entry without index");
        assert_eq!(third_data, data3);

        // Index should still be unparsed
        assert!(
            reader.index.borrow().is_none(),
            "Index should remain unparsed after multiple read_range calls"
        );

        // Now call a method that requires the index
        let entry1 = reader.get("entry1").expect("Failed to get entry1");
        assert_eq!(entry1, data1);

        // Verify index is now parsed
        assert!(
            reader.index.borrow().is_some(),
            "Index should be parsed after get()"
        );

        // Verify read_range still works after index is parsed
        let data_after_index = reader
            .read_range(first_offset, first_length)
            .expect("read_range should still work after index is parsed");
        assert_eq!(data_after_index, data1);

        // Test that subsequent index-requiring calls use the cached index
        let entry2 = reader.get("entry2").expect("Failed to get entry2");
        assert_eq!(entry2, data2);

        // Verify list_ids uses the cached index
        let mut ids = reader.list_ids().expect("Failed to list IDs");
        ids.sort();
        assert_eq!(ids, vec!["entry1", "entry2", "entry3"]);
    }
}

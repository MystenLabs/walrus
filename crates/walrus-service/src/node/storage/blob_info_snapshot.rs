// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Serialization format for blob info snapshots.
//!
//! A blob info snapshot is a deterministic serialization of the blob info tables that are
//! identical across all honest nodes at the epoch boundary (after garbage-collection phase 1):
//! `per_object_blob_info`, `per_object_pooled_blob_info`, and `storage_pool_info`. The
//! `aggregate_blob_info` table is deliberately excluded: it is a materialized view that contains
//! node-local state (`is_metadata_stored`) and entries whose deletion timing depends on the
//! background GC phase 2, so it is not deterministic across nodes; it is reconstructed from the
//! per-object tables during recovery.
//!
//! The format is versioned and self-delimiting so it can be written and read in a single pass:
//!
//! ```text
//! +----------------------+
//! |    Magic (4 B, BE)   |
//! +----------------------+
//! |  Version (4 B, BE)   |
//! +----------------------+
//! | Header len (4 B, BE) |
//! +----------------------+
//! |  Header (BCS bytes)  |
//! +----------------------+
//! |  Section (tag = 1)   |  per_object_blob_info
//! +----------------------+
//! |  Section (tag = 2)   |  per_object_pooled_blob_info
//! +----------------------+
//! |  Section (tag = 3)   |  storage_pool_info
//! +----------------------+
//! |  Checksum (8 B, BE)  |  xxhash64 of all preceding bytes
//! +----------------------+
//!
//! Section := tag (1 B)
//!            { 0x01 | key len (4 B, BE) | key BCS | value len (4 B, BE) | value BCS }*
//!            0x00 | entry count (8 B, BE)
//! ```
//!
//! Entries within a section are required to be in strictly increasing key order (the natural
//! RocksDB iteration order), which makes the serialization a pure function of the table contents.
//! Any change that affects the serialized bytes (entry types, section layout, future compression)
//! MUST bump [`SNAPSHOT_FORMAT_VERSION`]: the snapshot bytes are consensus-critical, since all
//! nodes must produce bit-identical snapshots for the same epoch.

use std::{
    hash::Hasher as _,
    io::{Cursor, Read, Write},
};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sui_types::base_types::ObjectID;
use twox_hash::XxHash64;
use typed_store::TypedStoreError;
use walrus_core::{BlobId, Epoch};

use super::blob_info::{PerObjectBlobInfo, PerObjectPooledBlobInfo, StoragePoolInfo};
use crate::event::events::EventStreamCursor;

/// The magic bytes at the start of a blob info snapshot.
pub(crate) const SNAPSHOT_MAGIC: u32 = 0xB10B1F05;
/// The current format version of the blob info snapshot.
pub(crate) const SNAPSHOT_FORMAT_VERSION: u32 = 1;

const SECTION_TAG_PER_OBJECT: u8 = 1;
const SECTION_TAG_PER_OBJECT_POOLED: u8 = 2;
const SECTION_TAG_STORAGE_POOL: u8 = 3;

const ENTRY_MARKER: u8 = 0x01;
const SECTION_END_MARKER: u8 = 0x00;

const CHECKSUM_SEED: u64 = 0;
const CHECKSUM_SIZE: usize = size_of::<u64>();

/// Errors occurring during blob info snapshot (de)serialization.
#[derive(Debug, thiserror::Error)]
pub(crate) enum SnapshotError {
    /// An I/O error occurred while reading or writing the snapshot.
    #[error("I/O error during snapshot (de)serialization")]
    Io(#[from] std::io::Error),
    /// Reading from the underlying database failed.
    #[error("database error during snapshot serialization")]
    Storage(#[from] TypedStoreError),
    /// BCS (de)serialization of a header or entry failed.
    #[error("BCS (de)serialization error")]
    Encoding(#[from] bcs::Error),
    /// The snapshot does not start with the expected magic bytes.
    #[error("invalid snapshot magic: expected {SNAPSHOT_MAGIC:#010x}, found {0:#010x}")]
    InvalidMagic(u32),
    /// The snapshot has a format version that this binary cannot read.
    #[error("unsupported snapshot format version {0}")]
    UnsupportedVersion(u32),
    /// The snapshot is structurally invalid.
    #[error("corrupt snapshot: {0}")]
    Corrupt(String),
    /// The snapshot checksum does not match its contents.
    #[error("snapshot checksum mismatch")]
    ChecksumMismatch,
    /// The entries of a section are not in strictly increasing key order.
    #[error("keys are not in strictly increasing order in section with tag {0}")]
    UnsortedKeys(u8),
}

/// The header of a blob info snapshot.
///
/// The header pins the exact event-stream position the snapshot corresponds to: the snapshot
/// contains the table state after applying all events up to and including `event_cursor`, with
/// the inline GC phase 1 for `epoch` applied. The chunk fields are reserved for splitting large
/// snapshots across multiple blobs; the current writer always produces a single chunk.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SnapshotHeader {
    /// The epoch whose boundary this snapshot was taken at.
    pub epoch: Epoch,
    /// The position of the last event included in the snapshot.
    pub event_cursor: EventStreamCursor,
    /// The blob ID of the previous epoch's snapshot, or [`BlobId::ZERO`] if unknown.
    pub prev_snapshot_blob_id: BlobId,
    /// The index of this chunk; always 0 until chunking is implemented.
    pub chunk_index: u32,
    /// The total number of chunks; always 1 until chunking is implemented.
    pub chunk_count: u32,
}

impl SnapshotHeader {
    /// Creates a single-chunk snapshot header.
    pub fn new(
        epoch: Epoch,
        event_cursor: EventStreamCursor,
        prev_snapshot_blob_id: BlobId,
    ) -> Self {
        Self {
            epoch,
            event_cursor,
            prev_snapshot_blob_id,
            chunk_index: 0,
            chunk_count: 1,
        }
    }
}

/// Statistics about a written snapshot, for logging and metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct SnapshotStats {
    /// The total number of bytes written, including the checksum.
    pub bytes_written: u64,
    /// The number of entries serialized from `per_object_blob_info`.
    pub per_object_count: u64,
    /// The number of entries serialized from `per_object_pooled_blob_info`.
    pub per_object_pooled_count: u64,
    /// The number of entries serialized from `storage_pool_info`.
    pub storage_pool_count: u64,
    /// The xxhash64 checksum of the snapshot contents (also stored in the snapshot trailer).
    ///
    /// Since the serialization is deterministic, this is a fingerprint of the snapshotted
    /// table contents: nodes with identical tables produce identical checksums.
    pub checksum: u64,
}

/// The fully deserialized contents of a blob info snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SnapshotContents {
    /// The snapshot header.
    pub header: SnapshotHeader,
    /// The entries of the `per_object_blob_info` table.
    pub per_object: Vec<(ObjectID, PerObjectBlobInfo)>,
    /// The entries of the `per_object_pooled_blob_info` table.
    pub per_object_pooled: Vec<(ObjectID, PerObjectPooledBlobInfo)>,
    /// The entries of the `storage_pool_info` table.
    pub storage_pools: Vec<(ObjectID, StoragePoolInfo)>,
}

/// A writer wrapper that maintains a running xxhash64 and byte count of everything written.
struct HashingWriter<W> {
    inner: W,
    hasher: XxHash64,
    bytes_written: u64,
}

impl<W: Write> HashingWriter<W> {
    fn new(inner: W) -> Self {
        Self {
            inner,
            hasher: XxHash64::with_seed(CHECKSUM_SEED),
            bytes_written: 0,
        }
    }
}

impl<W: Write> Write for HashingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let written = self.inner.write(buf)?;
        self.hasher.write(&buf[..written]);
        self.bytes_written += u64::try_from(written).expect("usize fits in u64");
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// Serializes a blob info snapshot to `writer`.
///
/// The entry iterators must yield entries in strictly increasing key order, as produced by
/// RocksDB iteration; this is checked and [`SnapshotError::UnsortedKeys`] is returned otherwise.
pub(crate) fn write_snapshot<W: Write>(
    writer: W,
    header: &SnapshotHeader,
    per_object: impl IntoIterator<Item = Result<(ObjectID, PerObjectBlobInfo), TypedStoreError>>,
    per_object_pooled: impl IntoIterator<
        Item = Result<(ObjectID, PerObjectPooledBlobInfo), TypedStoreError>,
    >,
    storage_pools: impl IntoIterator<Item = Result<(ObjectID, StoragePoolInfo), TypedStoreError>>,
) -> Result<SnapshotStats, SnapshotError> {
    let mut writer = HashingWriter::new(writer);

    writer.write_u32::<BigEndian>(SNAPSHOT_MAGIC)?;
    writer.write_u32::<BigEndian>(SNAPSHOT_FORMAT_VERSION)?;
    let header_bytes = bcs::to_bytes(header)?;
    writer.write_u32::<BigEndian>(checked_len(header_bytes.len())?)?;
    writer.write_all(&header_bytes)?;

    let per_object_count = write_section(&mut writer, SECTION_TAG_PER_OBJECT, per_object)?;
    let per_object_pooled_count = write_section(
        &mut writer,
        SECTION_TAG_PER_OBJECT_POOLED,
        per_object_pooled,
    )?;
    let storage_pool_count = write_section(&mut writer, SECTION_TAG_STORAGE_POOL, storage_pools)?;

    let checksum = writer.hasher.finish();
    writer.write_u64::<BigEndian>(checksum)?;
    writer.flush()?;

    Ok(SnapshotStats {
        bytes_written: writer.bytes_written,
        per_object_count,
        per_object_pooled_count,
        storage_pool_count,
        checksum,
    })
}

/// Deserializes a blob info snapshot from `bytes`, verifying its structure and checksum.
pub(crate) fn read_snapshot(bytes: &[u8]) -> Result<SnapshotContents, SnapshotError> {
    verify_checksum(bytes)?;
    let mut reader = Cursor::new(&bytes[..bytes.len() - CHECKSUM_SIZE]);

    let magic = reader.read_u32::<BigEndian>()?;
    if magic != SNAPSHOT_MAGIC {
        return Err(SnapshotError::InvalidMagic(magic));
    }
    let version = reader.read_u32::<BigEndian>()?;
    if version != SNAPSHOT_FORMAT_VERSION {
        return Err(SnapshotError::UnsupportedVersion(version));
    }

    let header_len = reader.read_u32::<BigEndian>()?;
    let header_bytes = read_chunk(&mut reader, header_len)?;
    let header: SnapshotHeader = bcs::from_bytes(&header_bytes)?;

    let per_object = read_section(&mut reader, SECTION_TAG_PER_OBJECT)?;
    let per_object_pooled = read_section(&mut reader, SECTION_TAG_PER_OBJECT_POOLED)?;
    let storage_pools = read_section(&mut reader, SECTION_TAG_STORAGE_POOL)?;

    let position = reader.position();
    let remaining = u64::try_from(reader.get_ref().len()).expect("usize fits in u64") - position;
    if remaining != 0 {
        return Err(SnapshotError::Corrupt(format!(
            "{remaining} unexpected trailing bytes after the last section"
        )));
    }

    Ok(SnapshotContents {
        header,
        per_object,
        per_object_pooled,
        storage_pools,
    })
}

fn write_section<W: Write, V: Serialize>(
    writer: &mut W,
    tag: u8,
    entries: impl IntoIterator<Item = Result<(ObjectID, V), TypedStoreError>>,
) -> Result<u64, SnapshotError> {
    writer.write_u8(tag)?;
    let mut count: u64 = 0;
    let mut previous_key: Option<ObjectID> = None;

    for entry in entries {
        let (key, value) = entry?;
        if previous_key.is_some_and(|previous| previous >= key) {
            return Err(SnapshotError::UnsortedKeys(tag));
        }
        previous_key = Some(key);

        writer.write_u8(ENTRY_MARKER)?;
        let key_bytes = bcs::to_bytes(&key)?;
        writer.write_u32::<BigEndian>(checked_len(key_bytes.len())?)?;
        writer.write_all(&key_bytes)?;
        let value_bytes = bcs::to_bytes(&value)?;
        writer.write_u32::<BigEndian>(checked_len(value_bytes.len())?)?;
        writer.write_all(&value_bytes)?;
        count += 1;
    }

    writer.write_u8(SECTION_END_MARKER)?;
    writer.write_u64::<BigEndian>(count)?;
    Ok(count)
}

fn read_section<V: DeserializeOwned>(
    reader: &mut Cursor<&[u8]>,
    expected_tag: u8,
) -> Result<Vec<(ObjectID, V)>, SnapshotError> {
    let tag = reader.read_u8()?;
    if tag != expected_tag {
        return Err(SnapshotError::Corrupt(format!(
            "expected section tag {expected_tag}, found {tag}"
        )));
    }

    let mut entries = Vec::new();
    loop {
        let marker = reader.read_u8()?;
        match marker {
            SECTION_END_MARKER => break,
            ENTRY_MARKER => {
                let key_len = reader.read_u32::<BigEndian>()?;
                let key_bytes = read_chunk(reader, key_len)?;
                let key: ObjectID = bcs::from_bytes(&key_bytes)?;
                let value_len = reader.read_u32::<BigEndian>()?;
                let value_bytes = read_chunk(reader, value_len)?;
                let value: V = bcs::from_bytes(&value_bytes)?;
                entries.push((key, value));
            }
            other => {
                return Err(SnapshotError::Corrupt(format!(
                    "invalid entry marker {other} in section with tag {expected_tag}"
                )));
            }
        }
    }

    let count = reader.read_u64::<BigEndian>()?;
    let actual_count = u64::try_from(entries.len()).expect("usize fits in u64");
    if count != actual_count {
        return Err(SnapshotError::Corrupt(format!(
            "section with tag {expected_tag} declares {count} entries but contains {actual_count}"
        )));
    }
    Ok(entries)
}

fn verify_checksum(bytes: &[u8]) -> Result<(), SnapshotError> {
    let Some(content_len) = bytes.len().checked_sub(CHECKSUM_SIZE) else {
        return Err(SnapshotError::Corrupt(
            "snapshot is too short to contain a checksum".to_string(),
        ));
    };
    let mut hasher = XxHash64::with_seed(CHECKSUM_SEED);
    hasher.write(&bytes[..content_len]);
    let expected = hasher.finish();

    let mut checksum_bytes = [0u8; CHECKSUM_SIZE];
    checksum_bytes.copy_from_slice(&bytes[content_len..]);
    let found = u64::from_be_bytes(checksum_bytes);

    if expected != found {
        return Err(SnapshotError::ChecksumMismatch);
    }
    Ok(())
}

fn read_chunk(reader: &mut Cursor<&[u8]>, len: u32) -> Result<Vec<u8>, SnapshotError> {
    let mut buffer = vec![0u8; usize::try_from(len).expect("u32 fits in usize")];
    reader.read_exact(&mut buffer)?;
    Ok(buffer)
}

fn checked_len(len: usize) -> Result<u32, SnapshotError> {
    u32::try_from(len)
        .map_err(|_| SnapshotError::Corrupt(format!("entry of {len} bytes exceeds the u32 limit")))
}

#[cfg(test)]
mod tests {
    use sui_types::event::EventID;
    use walrus_core::test_utils::blob_id_from_u64;
    use walrus_sui::test_utils::{FIXED_STORAGE_POOL_ID, fixed_event_id_for_testing};

    use super::*;

    fn object_id(byte: u8) -> ObjectID {
        ObjectID::from_single_byte(byte)
    }

    fn test_header() -> SnapshotHeader {
        SnapshotHeader::new(
            7,
            EventStreamCursor::new(Some(fixed_event_id_for_testing(3)), 42),
            BlobId::ZERO,
        )
    }

    fn test_per_object_entries() -> Vec<(ObjectID, PerObjectBlobInfo)> {
        vec![
            (
                object_id(1),
                PerObjectBlobInfo::new_for_testing(
                    blob_id_from_u64(1),
                    1,
                    Some(2),
                    10,
                    true,
                    fixed_event_id_for_testing(7),
                    false,
                ),
            ),
            (
                object_id(2),
                PerObjectBlobInfo::new_for_testing(
                    blob_id_from_u64(2),
                    3,
                    None,
                    20,
                    false,
                    fixed_event_id_for_testing(8),
                    false,
                ),
            ),
        ]
    }

    fn test_pooled_entries() -> Vec<(ObjectID, PerObjectPooledBlobInfo)> {
        vec![(
            object_id(3),
            PerObjectPooledBlobInfo::new_for_testing(
                blob_id_from_u64(3),
                4,
                None,
                FIXED_STORAGE_POOL_ID,
                fixed_event_id_for_testing(9),
            ),
        )]
    }

    fn test_pool_entries() -> Vec<(ObjectID, StoragePoolInfo)> {
        vec![(object_id(4), StoragePoolInfo::new(1, 30))]
    }

    fn ok_iter<V>(
        entries: Vec<(ObjectID, V)>,
    ) -> impl Iterator<Item = Result<(ObjectID, V), TypedStoreError>> {
        entries.into_iter().map(Ok)
    }

    fn write_test_snapshot() -> (Vec<u8>, SnapshotStats) {
        let mut bytes = Vec::new();
        let stats = write_snapshot(
            &mut bytes,
            &test_header(),
            ok_iter(test_per_object_entries()),
            ok_iter(test_pooled_entries()),
            ok_iter(test_pool_entries()),
        )
        .expect("serialization should succeed");
        (bytes, stats)
    }

    #[test]
    fn roundtrip_empty_snapshot() {
        let mut bytes = Vec::new();
        let stats = write_snapshot(
            &mut bytes,
            &test_header(),
            ok_iter(vec![]),
            ok_iter(vec![]),
            ok_iter(vec![]),
        )
        .expect("serialization should succeed");
        assert_eq!(stats.per_object_count, 0);
        assert_eq!(stats.bytes_written, u64::try_from(bytes.len()).unwrap());

        let contents = read_snapshot(&bytes).expect("deserialization should succeed");
        assert_eq!(contents.header, test_header());
        assert!(contents.per_object.is_empty());
        assert!(contents.per_object_pooled.is_empty());
        assert!(contents.storage_pools.is_empty());
    }

    #[test]
    fn roundtrip_snapshot_with_entries() {
        let (bytes, stats) = write_test_snapshot();
        assert_eq!(stats.per_object_count, 2);
        assert_eq!(stats.per_object_pooled_count, 1);
        assert_eq!(stats.storage_pool_count, 1);
        assert_eq!(stats.bytes_written, u64::try_from(bytes.len()).unwrap());

        let contents = read_snapshot(&bytes).expect("deserialization should succeed");
        assert_eq!(contents.header, test_header());
        assert_eq!(contents.per_object, test_per_object_entries());
        assert_eq!(contents.per_object_pooled, test_pooled_entries());
        assert_eq!(contents.storage_pools, test_pool_entries());
    }

    #[test]
    fn serialization_is_deterministic() {
        let (first, _) = write_test_snapshot();
        let (second, _) = write_test_snapshot();
        assert_eq!(first, second);
    }

    #[test]
    fn rejects_unsorted_keys() {
        let mut entries = test_per_object_entries();
        entries.reverse();
        let result = write_snapshot(
            &mut Vec::new(),
            &test_header(),
            ok_iter(entries),
            ok_iter(vec![]),
            ok_iter(vec![]),
        );
        assert!(matches!(
            result,
            Err(SnapshotError::UnsortedKeys(SECTION_TAG_PER_OBJECT))
        ));
    }

    #[test]
    fn rejects_corrupted_bytes() {
        let (mut bytes, _) = write_test_snapshot();
        let flip_position = bytes.len() / 2;
        bytes[flip_position] ^= 0xFF;
        assert!(matches!(
            read_snapshot(&bytes),
            Err(SnapshotError::ChecksumMismatch)
        ));
    }

    #[test]
    fn rejects_truncated_snapshot() {
        let (bytes, _) = write_test_snapshot();
        assert!(read_snapshot(&bytes[..bytes.len() - 1]).is_err());
        assert!(read_snapshot(&[]).is_err());
    }

    #[test]
    fn rejects_invalid_magic_and_version() {
        let (bytes, _) = write_test_snapshot();

        let mut wrong_magic = bytes.clone();
        wrong_magic[0] ^= 0xFF;
        fix_checksum(&mut wrong_magic);
        assert!(matches!(
            read_snapshot(&wrong_magic),
            Err(SnapshotError::InvalidMagic(_))
        ));

        let mut wrong_version = bytes;
        wrong_version[7] ^= 0xFF;
        fix_checksum(&mut wrong_version);
        assert!(matches!(
            read_snapshot(&wrong_version),
            Err(SnapshotError::UnsupportedVersion(_))
        ));
    }

    /// Recomputes and overwrites the trailing checksum after a test mutated the snapshot bytes.
    fn fix_checksum(bytes: &mut [u8]) {
        let content_len = bytes.len() - CHECKSUM_SIZE;
        let mut hasher = XxHash64::with_seed(CHECKSUM_SEED);
        hasher.write(&bytes[..content_len]);
        bytes[content_len..].copy_from_slice(&hasher.finish().to_be_bytes());
    }

    /// Golden test pinning the exact serialized bytes of format version 1.
    ///
    /// If this test fails, the serialized representation has changed: this is a breaking format
    /// change that requires bumping [`SNAPSHOT_FORMAT_VERSION`], because snapshot bytes must be
    /// bit-identical across all nodes of the network for the same table contents.
    #[test]
    fn golden_bytes_format_v1() {
        let (bytes, _) = write_test_snapshot();
        let expected_hex = concat!(
            "b10b1f05000000010000005e0700000001202a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a",
            "2a2a2a2a2a2a2a2a03000000000000002a00000000000000000000000000000000000000000000000000",
            "000000000000000000000000000000000000010000000101000000200000000000000000000000000000",
            "000000000000000000000000000000000001000000590000000000000000000000000000000000000000",
            "000000000000000000000000010100000001020000000a00000001202a2a2a2a2a2a2a2a2a2a2a2a2a2a",
            "2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a070000000000000000010000002000000000000000000000",
            "000000000000000000000000000000000000000000020000005500000000000000000000000000000000",
            "000000000000000000000000000000000203000000001400000000202a2a2a2a2a2a2a2a2a2a2a2a2a2a",
            "2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a080000000000000000000000000000000002020100000020",
            "00000000000000000000000000000000000000000000000000000000000000030000006f000000000000",
            "000000000000000000000000000000000000000000000000000003040000000000000000000000000000",
            "00000000000000000000000000000000000000000063202a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a",
            "2a2a2a2a2a2a2a2a2a2a2a2a2a0900000000000000000000000000000001030100000020000000000000",
            "00000000000000000000000000000000000000000000000000040000000900010000001e000000000000",
            "000000000001fdbf5eaf0d0fecff",
        );
        assert_eq!(
            hex::encode(&bytes),
            expected_hex,
            "snapshot format v1 bytes changed; this requires a format version bump"
        );
    }

    /// The event ID used in the golden test must itself be stable.
    #[test]
    fn golden_test_inputs_are_fixed() {
        let event = fixed_event_id_for_testing(3);
        assert_eq!(
            event,
            EventID {
                tx_digest: sui_types::digests::TransactionDigest::new([42; 32]),
                event_seq: 3,
            }
        );
    }
}

// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Serialization format for blob info snapshots.
//!
//! A blob info snapshot is a deterministic serialization of three blob info tables at the epoch
//! boundary (after garbage-collection phase 1): `per_object_blob_info`,
//! `per_object_pooled_blob_info`, and `storage_pool_info`. These are event-derived, so honest nodes
//! serialize identical bytes. `aggregate_blob_info` is excluded because it carries node-local state
//! (`is_metadata_stored`) and so differs across nodes, with one exception from format version 2
//! on: the set of invalidated blob IDs, each with the epoch and event of its `InvalidBlobID` event.
//! An invalidation is recorded only in the aggregate table (the per-object tables ignore it) and
//! is never removed. Without it, a node that loads the snapshot would rebuild an invalidated blob
//! with live objects as certified and try to recover data that every other node has deleted.
//!
//! The version a node writes is chosen per boundary through [`SnapshotFormat`], from an
//! activation epoch in the writer's configuration, so that every node of a network switches at
//! the same boundary. Version 1 never reads the aggregate table.
//!
//! This module contains the writer and the reader. The reader bounds every length prefix by the
//! remaining input, so a corrupted file yields an error instead of an oversized allocation.
//!
//! The format is versioned and self-delimiting:
//!
//! ```text
//! +----------------------+
//! |    Magic (4 B, BE)   |
//! +----------------------+
//! |  Version (4 B, BE)   |
//! +----------------------+
//! | Header len (varint)  |
//! +----------------------+
//! |  Header (BCS bytes)  |
//! +----------------------+
//! |  Section (tag = 1)   |  per_object_blob_info
//! +----------------------+
//! |  Section (tag = 2)   |  storage_pool_info
//! +----------------------+
//! |  Section (tag = 3)   |  per_object_pooled_blob_info
//! +----------------------+
//! |  Section (tag = 4)   |  invalidated blob IDs (version 2 and later only)
//! +----------------------+
//!
//! Section := tag (1 B)
//!            entry count (8 B, BE)
//!            { key len (varint) | key BCS | value len (varint) | value BCS } x count
//! ```
//!
//! Each section starts with its entry count, then that many entries. The count is written as a
//! fixed-width placeholder and back-patched after the section's entries are streamed. Key and value
//! lengths are LEB128 varints; the magic, version, section tags, and per-section counts are
//! fixed-width.
//!
//! Entries within a section are in strictly increasing key order (the natural RocksDB iteration
//! order). The serialized bytes are consensus-critical. The version ([`SnapshotFormat`]) covers
//! only the framing (magic, header, sections, ordering, length encoding), not the value schema: each
//! value is BCS of an append-only versioned enum (`PerObjectBlobInfoV1`, ...), so the leading
//! variant tag identifies the version per entry and newer software still decodes older variants (as
//! the on-disk tables already do). Adding a variant needs no format bump but must be epoch-gated so
//! all nodes emit identical bytes at the same boundary. A released variant is never changed in
//! place; any change to a value's fields goes in a new variant.
//!
//! The file carries no checksum; the cross-node content digest (xxhash64 of the whole file) is
//! computed and logged by the writer, not stored.

use std::io::{Cursor, Read, Seek, SeekFrom, Write};

use anyhow::{Context as _, bail};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use integer_encoding::{VarIntReader, VarIntWriter};
use serde::{Deserialize, Serialize};
use sui_types::{base_types::ObjectID, event::EventID};
use typed_store::TypedStoreError;
use walrus_core::{BlobId, Epoch};

use super::{
    blob_info::{PerObjectBlobInfo, PerObjectPooledBlobInfo, StoragePoolInfo},
    event_cursor_table::EventIdWithProgress,
};

/// The magic bytes at the start of a blob info snapshot.
pub(crate) const SNAPSHOT_MAGIC: u32 = 0xB10B1F05;

/// The version of the snapshot framing: the envelope layout (magic, header, section set, order,
/// and tags, count and length encoding, key ordering), not the value schema. A new version is
/// added when that layout changes. A new blob info value variant does not need one, because values
/// are append-only versioned enums that are self-describing per entry; see the module docs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SnapshotFormat {
    /// The three event-derived tables.
    V1,
    /// [`V1`](Self::V1) plus the invalidated blob IDs in a fourth section.
    V2,
}

impl SnapshotFormat {
    /// Returns the version number written to the file.
    pub fn version(self) -> u32 {
        match self {
            Self::V1 => 1,
            Self::V2 => 2,
        }
    }

    /// Returns the format with the given version number, if there is one.
    pub fn from_version(version: u32) -> Option<Self> {
        match version {
            1 => Some(Self::V1),
            2 => Some(Self::V2),
            _ => None,
        }
    }
}

const SECTION_TAG_PER_OBJECT: u8 = 1;
const SECTION_TAG_STORAGE_POOL: u8 = 2;
const SECTION_TAG_PER_OBJECT_POOLED: u8 = 3;
const SECTION_TAG_INVALID_BLOB: u8 = 4;

/// Errors occurring during blob info snapshot serialization.
#[derive(Debug, thiserror::Error)]
pub(crate) enum SnapshotError {
    /// An I/O error occurred while writing the snapshot.
    #[error("I/O error during snapshot serialization")]
    Io(#[from] std::io::Error),
    /// Reading from the underlying database failed.
    #[error("database error during snapshot serialization")]
    Storage(#[from] TypedStoreError),
    /// BCS serialization of a header or entry failed.
    #[error("BCS serialization error")]
    Encoding(#[from] bcs::Error),
    /// The entries of a section are not in strictly increasing key order.
    #[error("keys are not in strictly increasing order in section with tag {0}")]
    UnsortedKeys(u8),
}

/// The header of a blob info snapshot.
///
/// The header pins the exact event-stream position the snapshot corresponds to: the snapshot
/// contains the table state after applying all events up to and including the `EpochChangeStart`
/// for `epoch`, with the inline GC phase 1 for `epoch` applied. The position is stored as
/// [`EventIdWithProgress`], the same versioned record the node persists as its event cursor, so a
/// recovering node rehydrates the cursor directly and the format evolves with it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SnapshotHeader {
    /// The new (incoming) epoch this boundary begins; the `epoch` of the `EpochChangeStart` event
    /// being processed.
    pub epoch: Epoch,
    /// The node's event cursor at the boundary: the event ID of the last included event (the
    /// `EpochChangeStart` for `epoch`) and the index of the next event to process.
    pub event_cursor: EventIdWithProgress,
}

impl SnapshotHeader {
    /// Creates a snapshot header from the boundary epoch and the cursor's two fields.
    pub fn new(epoch: Epoch, event_id: EventID, next_event_index: u64) -> Self {
        Self {
            epoch,
            event_cursor: EventIdWithProgress::new(event_id, next_event_index),
        }
    }
}

/// Statistics about a written snapshot, for logging and metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct SnapshotStats {
    /// The number of entries serialized from `per_object_blob_info`.
    pub per_object_count: u64,
    /// The number of entries serialized from `per_object_pooled_blob_info`.
    pub per_object_pooled_count: u64,
    /// The number of entries serialized from `storage_pool_info`.
    pub storage_pool_count: u64,
    /// The number of invalidated blob IDs serialized from `aggregate_blob_info`; `None` for a
    /// format that has no such section.
    pub invalid_blob_count: Option<u64>,
}

/// The invalidation of a blob ID, as recorded in a snapshot.
///
/// Only the invalidation, not the rest of the aggregate blob info entry, which is node-local.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum InvalidBlobInfo {
    V1(InvalidBlobInfoV1),
}

/// Version 1 of [`InvalidBlobInfo`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct InvalidBlobInfoV1 {
    /// The epoch of the `InvalidBlobID` event.
    pub epoch: Epoch,
    /// The `InvalidBlobID` event through which the blob ID was marked invalid.
    pub event: EventID,
}

/// A fallible `(key, value)` entry, as yielded by a RocksDB column-family iterator.
type Entry<V, K = ObjectID> = Result<(K, V), TypedStoreError>;

/// Serializes a blob info snapshot in `format` to `writer`.
///
/// The entry iterators must yield entries in strictly increasing key order, as produced by
/// RocksDB iteration; this is checked and [`SnapshotError::UnsortedKeys`] is returned otherwise.
/// `invalid_blobs` is called only for a format with an invalidated-blob section, so a format
/// without one never reads its source.
pub(crate) fn write_snapshot<W, I>(
    mut writer: W,
    header: &SnapshotHeader,
    format: SnapshotFormat,
    per_object: impl IntoIterator<Item = Entry<PerObjectBlobInfo>>,
    per_object_pooled: impl IntoIterator<Item = Entry<PerObjectPooledBlobInfo>>,
    storage_pools: impl IntoIterator<Item = Entry<StoragePoolInfo>>,
    invalid_blobs: impl FnOnce() -> Result<I, TypedStoreError>,
) -> Result<SnapshotStats, SnapshotError>
where
    W: Write + Seek,
    I: IntoIterator<Item = Entry<InvalidBlobInfo, BlobId>>,
{
    writer.write_u32::<BigEndian>(SNAPSHOT_MAGIC)?;
    writer.write_u32::<BigEndian>(format.version())?;
    let header_bytes = bcs::to_bytes(header)?;
    writer.write_varint(header_bytes.len())?;
    writer.write_all(&header_bytes)?;

    let per_object_count = write_section(&mut writer, SECTION_TAG_PER_OBJECT, per_object)?;
    let storage_pool_count = write_section(&mut writer, SECTION_TAG_STORAGE_POOL, storage_pools)?;
    let per_object_pooled_count = write_section(
        &mut writer,
        SECTION_TAG_PER_OBJECT_POOLED,
        per_object_pooled,
    )?;
    let invalid_blob_count = match format {
        SnapshotFormat::V1 => None,
        SnapshotFormat::V2 => Some(write_section(
            &mut writer,
            SECTION_TAG_INVALID_BLOB,
            invalid_blobs()?,
        )?),
    };

    writer.flush()?;

    Ok(SnapshotStats {
        per_object_count,
        per_object_pooled_count,
        storage_pool_count,
        invalid_blob_count,
    })
}

fn write_section<W: Write + Seek, K: Serialize + Ord + Copy, V: Serialize>(
    writer: &mut W,
    tag: u8,
    entries: impl IntoIterator<Item = Entry<V, K>>,
) -> Result<u64, SnapshotError> {
    writer.write_u8(tag)?;
    // The entry count leads the section, but it is only known after the streamed source is
    // exhausted, so reserve a fixed-width placeholder here and back-patch it once the section is
    // written. Fixed width (not a varint) is required so the patched value occupies the reserved
    // space exactly.
    let count_position = writer.stream_position()?;
    writer.write_u64::<BigEndian>(0)?;

    let mut count: u64 = 0;
    let mut previous_key: Option<K> = None;

    for entry in entries {
        let (key, value) = entry?;
        if previous_key.is_some_and(|previous| previous >= key) {
            return Err(SnapshotError::UnsortedKeys(tag));
        }
        previous_key = Some(key);

        let key_bytes = bcs::to_bytes(&key)?;
        writer.write_varint(key_bytes.len())?;
        writer.write_all(&key_bytes)?;
        let value_bytes = bcs::to_bytes(&value)?;
        writer.write_varint(value_bytes.len())?;
        writer.write_all(&value_bytes)?;
        count += 1;
    }

    let section_end = writer.stream_position()?;
    writer.seek(SeekFrom::Start(count_position))?;
    writer.write_u64::<BigEndian>(count)?;
    writer.seek(SeekFrom::Start(section_end))?;
    Ok(count)
}

/// The decoded contents of a blob info snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DecodedSnapshot {
    /// The format version the file was written in.
    pub format: SnapshotFormat,
    /// The header.
    pub header: SnapshotHeader,
    /// The `per_object_blob_info` entries.
    pub per_object: Vec<(ObjectID, PerObjectBlobInfo)>,
    /// The `per_object_pooled_blob_info` entries.
    pub per_object_pooled: Vec<(ObjectID, PerObjectPooledBlobInfo)>,
    /// The `storage_pool_info` entries.
    pub storage_pools: Vec<(ObjectID, StoragePoolInfo)>,
    /// The invalidated blob IDs, or `None` for a format whose files omit the invalidation
    /// history ([`SnapshotFormat::V1`]).
    pub invalid_blobs: Option<Vec<(BlobId, InvalidBlobInfo)>>,
}

impl DecodedSnapshot {
    /// Returns the invalidated blob IDs, empty for a format that omits them.
    pub fn invalid_blobs(&self) -> &[(BlobId, InvalidBlobInfo)] {
        self.invalid_blobs.as_deref().unwrap_or_default()
    }

    /// Re-encodes the snapshot in its own format. A snapshot read with [`read_snapshot`] encodes
    /// to the bytes it was read from.
    pub fn write<W: Write + Seek>(&self, writer: W) -> Result<SnapshotStats, SnapshotError> {
        write_snapshot(
            writer,
            &self.header,
            self.format,
            self.per_object.iter().cloned().map(Ok),
            self.per_object_pooled.iter().cloned().map(Ok),
            self.storage_pools.iter().cloned().map(Ok),
            || Ok(self.invalid_blobs().iter().cloned().map(Ok)),
        )
    }
}

/// Decodes a blob info snapshot file into its header and sections, mirroring [`write_snapshot`].
///
/// Every version is decoded with its own framing: a version-2 file must have the
/// invalidated-blob section, a version-1 file must not. Within a section, keys must be strictly
/// increasing, as the writer emits them, so no entry can appear twice; a file that violates this,
/// or that has trailing bytes, is rejected.
pub(crate) fn read_snapshot(bytes: &[u8]) -> anyhow::Result<DecodedSnapshot> {
    let mut reader = Cursor::new(bytes);
    let magic = reader.read_u32::<BigEndian>()?;
    if magic != SNAPSHOT_MAGIC {
        bail!("unexpected magic {magic:#010x}");
    }
    let version = reader.read_u32::<BigEndian>()?;
    let format = SnapshotFormat::from_version(version)
        .with_context(|| format!("unsupported snapshot version {version}"))?;
    let header_len: u64 = reader.read_varint()?;
    let header_bytes = read_snapshot_bytes(&mut reader, header_len)?;
    let header: SnapshotHeader =
        bcs::from_bytes(&header_bytes).context("failed to decode the snapshot header")?;

    let per_object = read_snapshot_section(&mut reader, SECTION_TAG_PER_OBJECT)?;
    let storage_pools = read_snapshot_section(&mut reader, SECTION_TAG_STORAGE_POOL)?;
    let per_object_pooled = read_snapshot_section(&mut reader, SECTION_TAG_PER_OBJECT_POOLED)?;
    let invalid_blobs = match format {
        SnapshotFormat::V1 => None,
        SnapshotFormat::V2 => Some(read_snapshot_section(
            &mut reader,
            SECTION_TAG_INVALID_BLOB,
        )?),
    };

    let position = reader.position();
    let length = u64::try_from(bytes.len()).expect("snapshot length fits in u64");
    if position != length {
        bail!(
            "{} unexpected trailing bytes after the last section",
            length - position
        );
    }
    Ok(DecodedSnapshot {
        format,
        header,
        per_object,
        per_object_pooled,
        storage_pools,
        invalid_blobs,
    })
}

fn read_snapshot_section<K, V>(
    reader: &mut Cursor<&[u8]>,
    expected_tag: u8,
) -> anyhow::Result<Vec<(K, V)>>
where
    K: serde::de::DeserializeOwned + Ord,
    V: serde::de::DeserializeOwned,
{
    let tag = reader.read_u8()?;
    if tag != expected_tag {
        bail!("expected section tag {expected_tag}, found {tag}");
    }
    let count = reader.read_u64::<BigEndian>()?;
    // The count comes from the file, so it must not size an allocation.
    let mut entries: Vec<(K, V)> = Vec::new();
    for _ in 0..count {
        let key_len: u64 = reader.read_varint()?;
        let key_bytes = read_snapshot_bytes(reader, key_len)?;
        let key: K = bcs::from_bytes(&key_bytes).context("failed to decode an entry key")?;
        if entries.last().is_some_and(|(previous, _)| *previous >= key) {
            bail!("keys are not in strictly increasing order in section with tag {expected_tag}");
        }
        let value_len: u64 = reader.read_varint()?;
        let value_bytes = read_snapshot_bytes(reader, value_len)?;
        let value: V = bcs::from_bytes(&value_bytes).context("failed to decode an entry value")?;
        entries.push((key, value));
    }
    Ok(entries)
}

/// Reads `len` bytes, rejecting a length larger than the remaining input before allocating, so a
/// corrupted length prefix returns an error instead of aborting the process on allocation.
fn read_snapshot_bytes(reader: &mut Cursor<&[u8]>, len: u64) -> anyhow::Result<Vec<u8>> {
    let remaining = reader.get_ref().len().saturating_sub(
        usize::try_from(reader.position()).context("snapshot position exceeds usize")?,
    );
    let len = usize::try_from(len).context("snapshot length prefix exceeds usize")?;
    if len > remaining {
        bail!("length prefix {len} exceeds the {remaining} remaining snapshot bytes");
    }
    let mut buffer = vec![0u8; len];
    reader.read_exact(&mut buffer)?;
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use std::hash::Hasher as _;

    use twox_hash::XxHash64;
    use walrus_core::test_utils::blob_id_from_u64;
    use walrus_sui::test_utils::fixed_event_id_for_testing;

    use super::*;

    fn sample_header() -> SnapshotHeader {
        SnapshotHeader::new(7, fixed_event_id_for_testing(3), 42)
    }

    // Rebuilt on every call so that two serializations exercise independent value instances.
    fn sample_per_object() -> Vec<Result<(ObjectID, PerObjectBlobInfo), TypedStoreError>> {
        vec![
            Ok((
                ObjectID::from_single_byte(1),
                PerObjectBlobInfo::new_for_testing(
                    blob_id_from_u64(10),
                    1,
                    Some(2),
                    5,
                    false,
                    fixed_event_id_for_testing(1),
                    false,
                ),
            )),
            Ok((
                ObjectID::from_single_byte(2),
                PerObjectBlobInfo::new_for_testing(
                    blob_id_from_u64(11),
                    3,
                    None,
                    9,
                    true,
                    fixed_event_id_for_testing(2),
                    true,
                ),
            )),
        ]
    }

    fn sample_pooled() -> Vec<Result<(ObjectID, PerObjectPooledBlobInfo), TypedStoreError>> {
        vec![Ok((
            ObjectID::from_single_byte(3),
            PerObjectPooledBlobInfo::new_for_testing(
                blob_id_from_u64(20),
                2,
                Some(4),
                ObjectID::from_single_byte(8),
                fixed_event_id_for_testing(5),
            ),
        ))]
    }

    fn sample_pools() -> Vec<Result<(ObjectID, StoragePoolInfo), TypedStoreError>> {
        vec![
            Ok((ObjectID::from_single_byte(4), StoragePoolInfo::new(1, 100))),
            Ok((ObjectID::from_single_byte(5), StoragePoolInfo::new(2, 50))),
        ]
    }

    type InvalidBlobEntries = Vec<Result<(BlobId, InvalidBlobInfo), TypedStoreError>>;

    fn sample_invalid_blobs() -> Result<InvalidBlobEntries, TypedStoreError> {
        Ok(vec![Ok((
            blob_id_from_u64(30),
            InvalidBlobInfo::V1(InvalidBlobInfoV1 {
                epoch: 6,
                event: fixed_event_id_for_testing(6),
            }),
        ))])
    }

    fn serialize_sample(format: SnapshotFormat) -> Vec<u8> {
        // `write_snapshot` back-patches the section counts, so it needs a seekable sink; `Vec` is
        // `Write` but not `Seek`, so wrap it in a `Cursor`.
        let mut cursor = std::io::Cursor::new(Vec::new());
        write_snapshot(
            &mut cursor,
            &sample_header(),
            format,
            sample_per_object(),
            sample_pooled(),
            sample_pools(),
            sample_invalid_blobs,
        )
        .expect("serialization should succeed");
        cursor.into_inner()
    }

    /// Determinism (R1): the same logical tables, built as independent instances, must serialize to
    /// byte-identical output. This is the guard that would catch a value type ever gaining an
    /// unordered collection (for example a `HashMap`), whose per-instance iteration order diverges.
    #[test]
    fn serialization_is_deterministic_across_independent_instances() {
        for format in [SnapshotFormat::V1, SnapshotFormat::V2] {
            assert_eq!(serialize_sample(format), serialize_sample(format));
        }
    }

    // TODO: this checks only that the bytes are stable, not that they decode back correctly. Add a
    // serialize -> deserialize round-trip test that asserts a decoded snapshot matches the source
    // table once the reader/decoder lands in a following PR (the round-trip exists on a separate
    // branch and passes on testnet data).
    /// Byte-stability: the on-disk encoding is consensus-critical, so any change to it must be a
    /// deliberate act. If a golden digest changes, the change belongs in a new format version and
    /// the released versions must keep their goldens.
    #[test]
    fn serialization_is_byte_stable() {
        for (format, golden) in [
            (SnapshotFormat::V1, 0x17cf49e2fd041371),
            (SnapshotFormat::V2, 0x0e8c2c7389687df5),
        ] {
            let mut hasher = XxHash64::with_seed(0);
            hasher.write(&serialize_sample(format));
            assert_eq!(
                hasher.finish(),
                golden,
                "blob info snapshot encoding of {format:?} changed; a released version must \
                keep its bytes, so put the change in a new version",
            );
        }
    }

    /// The file opens with the magic and version a reader keys off.
    #[test]
    fn serialization_starts_with_magic_and_version() {
        for format in [SnapshotFormat::V1, SnapshotFormat::V2] {
            let bytes = serialize_sample(format);
            assert_eq!(
                u32::from_be_bytes(bytes[0..4].try_into().expect("4 bytes")),
                SNAPSHOT_MAGIC,
            );
            assert_eq!(
                u32::from_be_bytes(bytes[4..8].try_into().expect("4 bytes")),
                format.version(),
            );
        }
    }

    /// Version 1 has no invalidated-blob section and must not read its source, so that a node
    /// writing it never scans the aggregate table.
    #[test]
    fn version_1_does_not_read_invalid_blobs() {
        let mut cursor = std::io::Cursor::new(Vec::new());
        let stats = write_snapshot(
            &mut cursor,
            &sample_header(),
            SnapshotFormat::V1,
            sample_per_object(),
            sample_pooled(),
            sample_pools(),
            || -> Result<InvalidBlobEntries, TypedStoreError> {
                panic!("version 1 must not read the invalidated blob IDs")
            },
        )
        .expect("serialization should succeed");
        assert_eq!(stats.invalid_blob_count, None);
        assert_eq!(cursor.into_inner(), serialize_sample(SnapshotFormat::V1));
    }

    /// Strictly ascending key order is what makes the encoding a pure function of the table
    /// contents; out-of-order keys are rejected rather than silently producing divergent bytes.
    #[test]
    fn unsorted_keys_are_rejected() {
        let no_per_object: Vec<Result<(ObjectID, PerObjectBlobInfo), TypedStoreError>> = vec![];
        let no_pooled: Vec<Result<(ObjectID, PerObjectPooledBlobInfo), TypedStoreError>> = vec![];
        let unsorted_pools = vec![
            Ok((ObjectID::from_single_byte(5), StoragePoolInfo::new(1, 2))),
            Ok((ObjectID::from_single_byte(4), StoragePoolInfo::new(1, 2))),
        ];
        let mut buf = std::io::Cursor::new(Vec::new());
        let err = write_snapshot(
            &mut buf,
            &sample_header(),
            SnapshotFormat::V2,
            no_per_object,
            no_pooled,
            unsorted_pools,
            sample_invalid_blobs,
        )
        .expect_err("unsorted keys must be rejected");
        assert!(matches!(err, SnapshotError::UnsortedKeys(_)));
    }

    /// The invalidated blob IDs are keyed by blob ID, and their section rejects unsorted keys too.
    #[test]
    fn unsorted_invalid_blob_ids_are_rejected() {
        let invalid = |blob_id| {
            Ok((
                blob_id_from_u64(blob_id),
                InvalidBlobInfo::V1(InvalidBlobInfoV1 {
                    epoch: 1,
                    event: fixed_event_id_for_testing(1),
                }),
            ))
        };
        let mut buf = std::io::Cursor::new(Vec::new());
        let err = write_snapshot(
            &mut buf,
            &sample_header(),
            SnapshotFormat::V2,
            sample_per_object(),
            sample_pooled(),
            sample_pools(),
            || Ok(vec![invalid(31), invalid(30)]),
        )
        .expect_err("unsorted keys must be rejected");
        assert!(matches!(
            err,
            SnapshotError::UnsortedKeys(SECTION_TAG_INVALID_BLOB)
        ));
    }

    /// A failure to read the invalidated blob IDs fails the snapshot instead of dropping the
    /// section, so a node never writes a file that silently lacks marks.
    #[test]
    fn invalid_blob_read_error_fails_the_snapshot() {
        let mut buf = std::io::Cursor::new(Vec::new());
        let err = write_snapshot(
            &mut buf,
            &sample_header(),
            SnapshotFormat::V2,
            sample_per_object(),
            sample_pooled(),
            sample_pools(),
            || -> Result<InvalidBlobEntries, TypedStoreError> {
                Err(TypedStoreError::RocksDBError("read failed".to_string()))
            },
        )
        .expect_err("a read error must fail the snapshot");
        assert!(matches!(err, SnapshotError::Storage(_)));
    }

    fn unwrap_entries<K, V>(entries: Vec<Result<(K, V), TypedStoreError>>) -> Vec<(K, V)> {
        entries
            .into_iter()
            .map(|entry| entry.expect("sample entries are Ok"))
            .collect()
    }

    /// Overwrites the version field of a serialized snapshot.
    fn with_version(mut bytes: Vec<u8>, version: u32) -> Vec<u8> {
        bytes[4..8].copy_from_slice(&version.to_be_bytes());
        bytes
    }

    /// Encodes a section the way the writer does, without its key-order check.
    fn raw_section<K: Serialize, V: Serialize>(tag: u8, entries: &[(K, V)]) -> Vec<u8> {
        let mut bytes = vec![tag];
        bytes.extend_from_slice(&u64::try_from(entries.len()).expect("fits").to_be_bytes());
        for (key, value) in entries {
            for field in [bcs::to_bytes(key), bcs::to_bytes(value)] {
                let field = field.expect("sample entries serialize");
                bytes
                    .write_varint(field.len())
                    .expect("writing to a vector succeeds");
                bytes.extend_from_slice(&field);
            }
        }
        bytes
    }

    /// A snapshot decodes to the entries it was written from, in its own format, and re-encodes
    /// to the same bytes; a version-1 file reports its invalidation history as omitted.
    #[test]
    fn snapshots_round_trip_in_their_own_format() {
        for format in [SnapshotFormat::V1, SnapshotFormat::V2] {
            let bytes = serialize_sample(format);
            let decoded = read_snapshot(&bytes).expect("decoding should succeed");
            assert_eq!(decoded.format, format);
            assert_eq!(decoded.header, sample_header());
            assert_eq!(decoded.per_object, unwrap_entries(sample_per_object()));
            assert_eq!(decoded.per_object_pooled, unwrap_entries(sample_pooled()));
            assert_eq!(decoded.storage_pools, unwrap_entries(sample_pools()));
            let expected_invalid_blobs = match format {
                SnapshotFormat::V1 => None,
                SnapshotFormat::V2 => Some(unwrap_entries(
                    sample_invalid_blobs().expect("sample entries are Ok"),
                )),
            };
            assert_eq!(decoded.invalid_blobs, expected_invalid_blobs);

            let mut cursor = std::io::Cursor::new(Vec::new());
            decoded
                .write(&mut cursor)
                .expect("re-encoding should succeed");
            assert_eq!(
                cursor.into_inner(),
                bytes,
                "{format:?} re-encodes to its own bytes"
            );
        }
    }

    #[test]
    fn unknown_version_is_rejected() {
        let error = read_snapshot(&with_version(serialize_sample(SnapshotFormat::V2), 3))
            .expect_err("version 3 must be rejected");
        assert!(
            error.to_string().contains("unsupported snapshot version 3"),
            "{error:#}"
        );
    }

    /// The version decides the section set: a version-1 file followed by an invalidated-blob
    /// section has trailing bytes, and a version-2 file without one is truncated.
    #[test]
    fn section_set_follows_the_version() {
        let error = read_snapshot(&with_version(serialize_sample(SnapshotFormat::V2), 1))
            .expect_err("a version-1 file with a fourth section must be rejected");
        assert!(error.to_string().contains("trailing bytes"), "{error:#}");

        read_snapshot(&with_version(serialize_sample(SnapshotFormat::V1), 2))
            .expect_err("a version-2 file without the fourth section must be rejected");
    }

    /// The reader checks the key order of every section, so a file that did not come from the
    /// writer cannot load an entry twice or out of order.
    #[test]
    fn reader_rejects_unsorted_and_duplicate_keys() {
        let invalid = |blob_id| {
            (
                blob_id_from_u64(blob_id),
                InvalidBlobInfo::V1(InvalidBlobInfoV1 {
                    epoch: 1,
                    event: fixed_event_id_for_testing(1),
                }),
            )
        };
        // The version-1 bytes are the version-2 bytes without the fourth section.
        let prefix = with_version(serialize_sample(SnapshotFormat::V1), 2);
        for (label, entries) in [
            ("unsorted", vec![invalid(31), invalid(30)]),
            ("duplicate", vec![invalid(30), invalid(30)]),
        ] {
            let mut bytes = prefix.clone();
            bytes.extend_from_slice(&raw_section(SECTION_TAG_INVALID_BLOB, &entries));
            let error = read_snapshot(&bytes).expect_err(label);
            assert!(
                error.to_string().contains("strictly increasing"),
                "{label}: {error:#}"
            );
        }
        // The same bytes in order decode.
        let mut bytes = prefix;
        bytes.extend_from_slice(&raw_section(
            SECTION_TAG_INVALID_BLOB,
            &[invalid(30), invalid(31)],
        ));
        assert_eq!(
            read_snapshot(&bytes)
                .expect("ordered keys decode")
                .invalid_blobs()
                .len(),
            2
        );
    }
}

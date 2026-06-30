// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Serialization format for blob info snapshots.
//!
//! A blob info snapshot is a deterministic serialization of three blob info tables at the epoch
//! boundary (after garbage-collection phase 1): `per_object_blob_info`,
//! `per_object_pooled_blob_info`, and `storage_pool_info`. These are event-derived, so honest nodes
//! serialize identical bytes. `aggregate_blob_info` is excluded because it carries node-local state
//! (`is_metadata_stored`) and so differs across nodes.
//!
//! This module contains only the writer; deserialization belongs to the (not yet implemented)
//! recovery workflow.
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
//! order). The serialized bytes are consensus-critical. [`SNAPSHOT_FORMAT_VERSION`] versions only
//! the framing (magic, header, sections, ordering, length encoding), not the value schema: each
//! value is BCS of an append-only versioned enum (`PerObjectBlobInfoV1`, ...), so the leading
//! variant tag identifies the version per entry and newer software still decodes older variants (as
//! the on-disk tables already do). Adding a variant needs no format bump but must be epoch-gated so
//! all nodes emit identical bytes at the same boundary. A released variant is never changed in
//! place; any change to a value's fields goes in a new variant.
//!
//! The file carries no checksum; the cross-node content digest (xxhash64 of the whole file) is
//! computed and logged by the writer, not stored.

use std::io::{Seek, SeekFrom, Write};

use byteorder::{BigEndian, WriteBytesExt};
use integer_encoding::VarIntWriter;
use serde::{Deserialize, Serialize};
use sui_types::{base_types::ObjectID, event::EventID};
use typed_store::TypedStoreError;
use walrus_core::Epoch;

use super::blob_info::{PerObjectBlobInfo, PerObjectPooledBlobInfo, StoragePoolInfo};

/// The magic bytes at the start of a blob info snapshot.
pub(crate) const SNAPSHOT_MAGIC: u32 = 0xB10B1F05;
/// Version of the snapshot framing: the envelope layout (magic, header, section set, order, and
/// tags, count and length encoding, key ordering), not the value schema. Bump it when that layout
/// changes. A new blob info value variant does not bump it, because values are append-only
/// versioned enums that are self-describing per entry; see the module docs.
pub(crate) const SNAPSHOT_FORMAT_VERSION: u32 = 1;

const SECTION_TAG_PER_OBJECT: u8 = 1;
const SECTION_TAG_STORAGE_POOL: u8 = 2;
const SECTION_TAG_PER_OBJECT_POOLED: u8 = 3;

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
/// for `epoch`, with the inline GC phase 1 for `epoch` applied. The cursor is stored as its two
/// constituent fields rather than as the `EventStreamCursor` type, so the on-disk format is not
/// coupled to that internal type's layout.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SnapshotHeader {
    /// The new (incoming) epoch this boundary begins; the `epoch` of the `EpochChangeStart` event
    /// being processed.
    pub epoch: Epoch,
    /// Event ID of the last event included in the snapshot (the `EpochChangeStart` for `epoch`).
    pub event_id: EventID,
    /// Index of the next event to process when resuming from this snapshot.
    pub next_event_index: u64,
}

impl SnapshotHeader {
    /// Creates a snapshot header.
    pub fn new(epoch: Epoch, event_id: EventID, next_event_index: u64) -> Self {
        Self {
            epoch,
            event_id,
            next_event_index,
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
}

/// A fallible `(ObjectID, value)` entry, as yielded by a RocksDB column-family iterator.
type Entry<V> = Result<(ObjectID, V), TypedStoreError>;

/// Serializes a blob info snapshot to `writer`.
///
/// The entry iterators must yield entries in strictly increasing key order, as produced by
/// RocksDB iteration; this is checked and [`SnapshotError::UnsortedKeys`] is returned otherwise.
pub(crate) fn write_snapshot<W: Write + Seek>(
    mut writer: W,
    header: &SnapshotHeader,
    per_object: impl IntoIterator<Item = Entry<PerObjectBlobInfo>>,
    per_object_pooled: impl IntoIterator<Item = Entry<PerObjectPooledBlobInfo>>,
    storage_pools: impl IntoIterator<Item = Entry<StoragePoolInfo>>,
) -> Result<SnapshotStats, SnapshotError> {
    writer.write_u32::<BigEndian>(SNAPSHOT_MAGIC)?;
    writer.write_u32::<BigEndian>(SNAPSHOT_FORMAT_VERSION)?;
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

    writer.flush()?;

    Ok(SnapshotStats {
        per_object_count,
        per_object_pooled_count,
        storage_pool_count,
    })
}

fn write_section<W: Write + Seek, V: Serialize>(
    writer: &mut W,
    tag: u8,
    entries: impl IntoIterator<Item = Entry<V>>,
) -> Result<u64, SnapshotError> {
    writer.write_u8(tag)?;
    // The entry count leads the section, but it is only known after the streamed source is
    // exhausted, so reserve a fixed-width placeholder here and back-patch it once the section is
    // written. Fixed width (not a varint) is required so the patched value occupies the reserved
    // space exactly.
    let count_position = writer.stream_position()?;
    writer.write_u64::<BigEndian>(0)?;

    let mut count: u64 = 0;
    let mut previous_key: Option<ObjectID> = None;

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

    fn serialize_sample() -> Vec<u8> {
        // `write_snapshot` back-patches the section counts, so it needs a seekable sink; `Vec` is
        // `Write` but not `Seek`, so wrap it in a `Cursor`.
        let mut cursor = std::io::Cursor::new(Vec::new());
        write_snapshot(
            &mut cursor,
            &sample_header(),
            sample_per_object(),
            sample_pooled(),
            sample_pools(),
        )
        .expect("serialization should succeed");
        cursor.into_inner()
    }

    /// Determinism (R1): the same logical tables, built as independent instances, must serialize to
    /// byte-identical output. This is the guard that would catch a value type ever gaining an
    /// unordered collection (for example a `HashMap`), whose per-instance iteration order diverges.
    #[test]
    fn serialization_is_deterministic_across_independent_instances() {
        assert_eq!(serialize_sample(), serialize_sample());
    }

    // TODO: this checks only that the bytes are stable, not that they decode back correctly. Add a
    // serialize -> deserialize round-trip test that asserts a decoded snapshot matches the source
    // table once the reader/decoder lands in a following PR (the round-trip exists on a separate
    // branch and passes on testnet data).
    /// Byte-stability: the on-disk encoding is consensus-critical, so any change to it must be a
    /// deliberate act. If this golden digest changes, update it here and, once a reader exists,
    /// bump `SNAPSHOT_FORMAT_VERSION`.
    #[test]
    fn serialization_is_byte_stable() {
        let mut hasher = XxHash64::with_seed(0);
        hasher.write(&serialize_sample());
        assert_eq!(
            hasher.finish(),
            0x3234c3cbfe7d7b4a,
            "blob info snapshot encoding changed; if intentional, update this golden digest",
        );
    }

    /// The file opens with the magic and version a reader keys off.
    #[test]
    fn serialization_starts_with_magic_and_version() {
        let bytes = serialize_sample();
        assert_eq!(
            u32::from_be_bytes(bytes[0..4].try_into().expect("4 bytes")),
            SNAPSHOT_MAGIC,
        );
        assert_eq!(
            u32::from_be_bytes(bytes[4..8].try_into().expect("4 bytes")),
            SNAPSHOT_FORMAT_VERSION,
        );
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
            no_per_object,
            no_pooled,
            unsorted_pools,
        )
        .expect_err("unsorted keys must be rejected");
        assert!(matches!(err, SnapshotError::UnsortedKeys(_)));
    }
}

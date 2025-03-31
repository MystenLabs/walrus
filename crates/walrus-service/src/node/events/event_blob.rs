// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Event blob file format.

use std::{
    fs::File,
    io,
    io::{BufReader, Read, Seek, SeekFrom, Write},
    path::Path,
};

use anyhow::{anyhow, Error, Result};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use integer_encoding::{VarInt, VarIntReader};
use sui_types::{
    digests::TransactionDigest,
    event::EventID,
    messages_checkpoint::CheckpointSequenceNumber,
};
use walrus_core::{BlobId, Epoch};

use crate::node::events::IndexedStreamEvent;

/// The encoding of an entry in the blob file.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum EntryEncoding {
    /// The entry is encoded using BCS.
    Bcs = 1,
}

impl From<EntryEncoding> for u8 {
    fn from(encoding: EntryEncoding) -> Self {
        match encoding {
            EntryEncoding::Bcs => 1,
        }
    }
}

impl TryFrom<u8> for EntryEncoding {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(EntryEncoding::Bcs),
            _ => Err(anyhow!("Invalid value for EntryEncoding: {}", value)),
        }
    }
}

/// A blob entry in the blob file.
#[derive(Debug)]
pub struct BlobEntry {
    /// The data of the blob entry.
    pub data: Vec<u8>,
    /// The encoding of the event.
    pub encoding: EntryEncoding,
}

impl BlobEntry {
    /// The maximum length of a varint in bytes. This is the maximum length of the length field of a
    /// blob.
    pub const MAX_VARINT_LENGTH: usize = 10;
    /// The number of bytes used to encode the encoding of the event. This is a single byte.
    pub const EVENT_ENCODING_BYTES: usize = 1;

    /// Encode the given value into a blob entry.
    pub fn encode(value: &IndexedStreamEvent, encoding: EntryEncoding) -> Result<Self> {
        let value_buf = bcs::to_bytes(value)?;
        let (data, encoding) = match encoding {
            EntryEncoding::Bcs => (value_buf, encoding),
        };
        Ok(BlobEntry { data, encoding })
    }

    /// Decode the blob entry into a value.
    pub fn decode(self) -> Result<IndexedStreamEvent> {
        let data = match &self.encoding {
            EntryEncoding::Bcs => self.data,
        };
        let res = bcs::from_bytes(&data)?;
        Ok(res)
    }

    /// Read a blob entry from the given reader.
    pub fn read<R: Read>(rbuf: &mut R) -> Result<Self> {
        let len = rbuf.read_varint::<u64>()? as usize;
        if len == 0 {
            return Err(anyhow!("Invalid object length of 0 in file"));
        }
        let encoding_byte = rbuf.read_u8()?;
        let encoding = EntryEncoding::try_from(encoding_byte)?;
        let mut data = vec![0u8; len];
        rbuf.read_exact(&mut data)?;
        let entry = BlobEntry { data, encoding };
        Ok(entry)
    }

    /// Write the blob entry to the given writer.
    pub fn write<W: Write>(&self, wbuf: &mut W) -> Result<usize> {
        let mut buf = [0u8; BlobEntry::MAX_VARINT_LENGTH + BlobEntry::EVENT_ENCODING_BYTES];
        let n = (self.data.len() as u64).encode_var(&mut buf);
        buf[n] = self.encoding.into();
        wbuf.write_all(&buf[0..n + BlobEntry::EVENT_ENCODING_BYTES])?;
        wbuf.write_all(&self.data)?;
        Ok(n + BlobEntry::EVENT_ENCODING_BYTES + self.data.len())
    }

    /// The size of the blob entry in bytes.
    pub fn size(&self) -> usize {
        self.data.len().required_space() + BlobEntry::EVENT_ENCODING_BYTES + self.data.len()
    }

    /// Encode the blob entry into a byte vector.
    #[allow(dead_code)]
    pub fn encode_to_bytes(&self) -> Vec<u8> {
        [vec![self.encoding.into()], self.data.clone()].concat()
    }

    /// Decode the blob entry from a byte vector.
    #[allow(dead_code)]
    pub fn decode_from_bytes(bytes: &[u8]) -> Result<IndexedStreamEvent> {
        let (encoding, data) = bytes.split_first().ok_or(anyhow!("empty bytes"))?;
        BlobEntry {
            data: data.to_vec(),
            encoding: EntryEncoding::try_from(*encoding)?,
        }
        .decode()
    }
}

/// A serialized event ID.
#[derive(Debug, Eq, PartialEq)]
pub struct SerializedEventID(pub [u8; Self::LENGTH]);

impl SerializedEventID {
    /// The length of the serialized event ID in bytes.
    pub const LENGTH: usize = 40;

    /// Create a new serialized event ID with all bytes set to zero.
    pub const ZERO: Self = Self([0u8; Self::LENGTH]);

    /// Create a new serialized event ID from the given event ID.
    pub fn encode(id: EventID) -> Self {
        let mut buf = [0u8; Self::LENGTH];
        buf[0..32].copy_from_slice(id.tx_digest.inner());
        buf[32..40].copy_from_slice(&id.event_seq.to_be_bytes());
        Self(buf)
    }

    /// Decode the serialized event ID into an event ID.
    pub fn decode(&self) -> Result<Option<EventID>> {
        if *self == Self::ZERO {
            return Ok(None);
        }
        let event_id = EventID {
            tx_digest: TransactionDigest::new(self.0[..32].try_into()?),
            event_seq: u64::from_be_bytes(self.0[32..40].try_into()?),
        };
        Ok(Some(event_id))
    }
}

#[derive(Debug)]
/// An iterator over events in a blob file.
pub struct EventBlob<'a> {
    reader: BufReader<io::Cursor<&'a [u8]>>,
    current_pos: u64,
    total_size: u64,
    prev_blob_id: BlobId,
    prev_event_id: Option<EventID>,
    start: CheckpointSequenceNumber,
    end: CheckpointSequenceNumber,
    #[allow(dead_code)]
    blob_format_version: u32,
    epoch: Epoch,
}

/// Represents an Event Blob, which stores a sequence of events in a structured format.
///
/// The blob format is as follows:
///
/// ```text
/// +------------------+ --
/// |  Magic (4 bytes) |   \
/// +------------------+    \
/// | Version (4 bytes)|     \
/// +------------------+     \
/// |  Epoch (4 bytes) |      \
/// +------------------+       > Header (84 bytes)
/// |Prev Blob ID (32B)|      /
/// +------------------+     /
/// |Prev EventID (40B)|    /
/// +------------------+ --
/// |                  |
/// |                  |
/// |   Event Data     |
/// |    (Variable)    |
/// |                  |
/// |                  |
/// +------------------+ --
/// | Start Chkpt (8B) |   \
/// +------------------+    > Trailer (16 bytes)
/// |  End Chkpt (8B)  |   /
/// +------------------+ --
/// ```
///
/// - Magic: Identifies the file as an Event Blob (0x005EA1CE)
/// - Version: Format version of the blob
/// - Epoch: Epoch of the first event in the blob
/// - Prev Blob ID: ID of the previous blob in the sequence
/// - Prev EventID: ID of the last event in the previous blob
/// - Event Data: Variable-length section containing serialized events
/// - Start/End Checkpoint: Sequence numbers of the first and last checkpoints in this blob
impl<'a> EventBlob<'a> {
    /// The magic bytes at the start of the blob file.
    pub const MAGIC: u32 = 0x005EA1CE;
    /// The size of the magic bytes in bytes.
    pub const MAGIC_BYTES_SIZE: usize = size_of::<u32>();
    /// The format version of the blob file.
    pub const FORMAT_VERSION: u32 = 1;
    /// The size of the format version in bytes.
    pub const FORMAT_BYTES_SIZE: usize = size_of::<u32>();
    /// The size of the epoch in bytes.
    pub const EPOCH_BYTES_SIZE: usize = size_of::<u32>();
    /// The size of the header of the blob file in bytes.
    pub const HEADER_SIZE: usize = EventBlob::MAGIC_BYTES_SIZE
        + EventBlob::FORMAT_BYTES_SIZE
        + EventBlob::EPOCH_BYTES_SIZE
        + BlobId::LENGTH
        + SerializedEventID::LENGTH;
    /// The offset of the blob ID in the blob file in bytes.
    pub const BLOB_ID_OFFSET: usize =
        EventBlob::MAGIC_BYTES_SIZE + EventBlob::FORMAT_BYTES_SIZE + EventBlob::EPOCH_BYTES_SIZE;
    /// The offset of the event ID in the blob file in bytes.
    pub const EVENT_ID_OFFSET: usize = EventBlob::BLOB_ID_OFFSET + BlobId::LENGTH;
    /// The size of the tail of the blob file in bytes.
    pub const TRAILER_SIZE: usize = 2 * std::mem::size_of::<u64>();
    /// The minimum size of a blob file in bytes.
    pub const MIN_SIZE: usize = EventBlob::HEADER_SIZE + EventBlob::TRAILER_SIZE;

    /// Create a new event blob from the list of events
    #[cfg(any(test, feature = "test-utils"))]
    pub fn from_events(
        epoch: Epoch,
        prev_blob_id: BlobId,
        prev_event_id: Option<EventID>,
        start: CheckpointSequenceNumber,
        end: CheckpointSequenceNumber,
        events: impl IntoIterator<Item = &'a IndexedStreamEvent>,
    ) -> Result<Vec<u8>> {
        let serialized_event_id = prev_event_id
            .map(SerializedEventID::encode)
            .unwrap_or(SerializedEventID::ZERO);
        let mut buf = Vec::new();
        buf.write_u32::<BigEndian>(EventBlob::MAGIC)?;
        buf.write_u32::<BigEndian>(EventBlob::FORMAT_VERSION)?;
        buf.write_u32::<BigEndian>(epoch)?;
        buf.write_all(&prev_blob_id.0)?;
        buf.write_all(&serialized_event_id.0)?;
        for event in events {
            let entry = BlobEntry::encode(event, EntryEncoding::Bcs)?;
            entry.write(&mut buf)?;
        }
        buf.write_u64::<BigEndian>(start)?;
        buf.write_u64::<BigEndian>(end)?;
        Ok(buf)
    }

    /// Create a new event blob from the given buffer.
    pub fn new(buf: &'a [u8]) -> Result<Self> {
        let cursor = io::Cursor::new(buf);
        let mut buf_reader = BufReader::new(cursor);
        let total_size = buf_reader.seek(SeekFrom::End(0))?;
        if total_size < EventBlob::MIN_SIZE as u64 {
            return Err(Error::from(io::Error::new(
                io::ErrorKind::InvalidData,
                "Blob file is too small",
            )));
        }
        buf_reader.seek(SeekFrom::Start(0))?;
        let magic = buf_reader.read_u32::<BigEndian>()?;
        if magic != EventBlob::MAGIC {
            return Err(Error::from(io::Error::new(
                io::ErrorKind::InvalidData,
                "Not an event blob file",
            )));
        }
        let blob_format_version = buf_reader.read_u32::<BigEndian>()?;
        if blob_format_version != EventBlob::FORMAT_VERSION {
            return Err(Error::from(io::Error::new(
                io::ErrorKind::InvalidData,
                "Unsupported blob format version",
            )));
        }
        let epoch = buf_reader.read_u32::<BigEndian>()?;
        let mut prev_blob_id = [0u8; BlobId::LENGTH];
        buf_reader.read_exact(&mut prev_blob_id)?;
        let mut prev_event_id = [0u8; SerializedEventID::LENGTH];
        buf_reader.read_exact(&mut prev_event_id)?;
        let trailer_size = Self::TRAILER_SIZE as i64;
        buf_reader.seek(SeekFrom::End(-trailer_size))?;
        let start = buf_reader.read_u64::<BigEndian>()?;
        let end = buf_reader.read_u64::<BigEndian>()?;
        buf_reader.seek(SeekFrom::Start(Self::HEADER_SIZE as u64))?;
        Ok(Self {
            reader: buf_reader,
            current_pos: Self::HEADER_SIZE as u64,
            total_size,
            prev_blob_id: BlobId(prev_blob_id),
            prev_event_id: SerializedEventID(prev_event_id).decode()?,
            start,
            end,
            blob_format_version,
            epoch,
        })
    }

    /// Previous blob ID.
    pub fn prev_blob_id(&self) -> BlobId {
        self.prev_blob_id
    }

    /// Previous event ID.
    pub fn prev_event_id(&self) -> Option<EventID> {
        self.prev_event_id
    }

    /// Epoch of the blob.
    pub fn epoch(&self) -> Epoch {
        self.epoch
    }

    /// Checkpoint sequence number of first event in the blob.
    pub fn start_checkpoint_sequence_number(&self) -> CheckpointSequenceNumber {
        self.start
    }

    /// Checkpoint sequence number of last event in the blob.
    pub fn end_checkpoint_sequence_number(&self) -> CheckpointSequenceNumber {
        self.end
    }

    /// Returns the next event in the blob.
    fn next_event(&mut self) -> Result<IndexedStreamEvent> {
        let entry = BlobEntry::read(&mut self.reader)?;
        self.current_pos += entry.size() as u64;
        entry.decode()
    }

    /// Store the blob as a file at the given path.
    pub fn store_as_file(&mut self, path: &Path) -> anyhow::Result<()> {
        let mut file = File::create(path)?;
        let mut buf = Vec::new();
        self.reader.seek(SeekFrom::Start(0))?;
        self.reader.read_to_end(&mut buf)?;
        file.write_all(&buf)?;
        Ok(())
    }
}

impl Iterator for EventBlob<'_> {
    type Item = IndexedStreamEvent;
    fn next(&mut self) -> Option<Self::Item> {
        if self.current_pos >= self.total_size {
            return None;
        }
        self.next_event().ok()
    }
}

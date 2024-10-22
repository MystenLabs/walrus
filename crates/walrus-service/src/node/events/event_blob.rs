// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Event blob file format.

use std::{
    fs::File,
    io,
    io::{BufReader, Read, Seek, SeekFrom, Write},
};

use anyhow::{anyhow, Error, Result};
use byteorder::ReadBytesExt;
use integer_encoding::{VarInt, VarIntReader};
use sui_types::messages_checkpoint::CheckpointSequenceNumber;
use walrus_core::BlobId;

use crate::node::events::IndexedStreamElement;

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
    pub fn encode(value: &IndexedStreamElement, encoding: EntryEncoding) -> Result<Self> {
        let value_buf = bcs::to_bytes(value)?;
        let (data, encoding) = match encoding {
            EntryEncoding::Bcs => (value_buf, encoding),
        };
        Ok(BlobEntry { data, encoding })
    }

    /// Decode the blob entry into a value.
    pub fn decode(self) -> Result<IndexedStreamElement> {
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
    pub fn decode_from_bytes(bytes: &[u8]) -> Result<IndexedStreamElement> {
        let (encoding, data) = bytes.split_first().ok_or(anyhow!("empty bytes"))?;
        BlobEntry {
            data: data.to_vec(),
            encoding: EntryEncoding::try_from(*encoding)?,
        }
        .decode()
    }
}

#[allow(dead_code)]
#[derive(Debug)]
/// An iterator over events in a blob file.
pub struct EventBlob {
    reader: BufReader<File>,
    current_pos: u64,
    total_size: u64,
    prev_blob_id: BlobId,
    start: CheckpointSequenceNumber,
    end: CheckpointSequenceNumber,
    blob_format_version: u32,
}

impl EventBlob {
    /// The magic bytes at the start of the blob file.
    pub const MAGIC: u32 = 0x0000DEAD;
    /// The size of the magic bytes in bytes.
    pub const MAGIC_BYTES_SIZE: usize = std::mem::size_of::<u32>();
    /// The format version of the blob file.
    pub const FORMAT_VERSION: u32 = 1;
    /// The size of the format version in bytes.
    pub const FORMAT_BYTES_SIZE: usize = std::mem::size_of::<u32>();
    /// The size of the header of the blob file in bytes.
    pub const HEADER_SIZE: usize = EventBlob::MAGIC_BYTES_SIZE + EventBlob::FORMAT_BYTES_SIZE;
    /// The size of the tail of the blob file in bytes.
    pub const TRAILER_SIZE: usize = 2 * std::mem::size_of::<u64>() + BlobId::LENGTH;
    /// The minimum size of a blob file in bytes.
    pub const MIN_SIZE: usize = EventBlob::HEADER_SIZE + EventBlob::TRAILER_SIZE;

    /// Create a new event blob from the given file.
    #[allow(dead_code)]
    pub fn new(file: File) -> Result<Self> {
        let mut buf_reader = BufReader::new(file);
        let mut total_size = buf_reader.seek(SeekFrom::End(0))?;
        if total_size < EventBlob::MIN_SIZE as u64 {
            return Err(Error::from(io::Error::new(
                io::ErrorKind::InvalidData,
                "Blob file is too small",
            )));
        }
        buf_reader.seek(SeekFrom::Start(0))?;
        let magic = buf_reader.read_u32::<byteorder::BigEndian>()?;
        if magic != EventBlob::MAGIC {
            return Err(Error::from(io::Error::new(
                io::ErrorKind::InvalidData,
                "Not an event blob file",
            )));
        }
        let blob_format_version = buf_reader.read_u32::<byteorder::BigEndian>()?;
        if blob_format_version != EventBlob::FORMAT_VERSION {
            return Err(Error::from(io::Error::new(
                io::ErrorKind::InvalidData,
                "Unsupported blob format version",
            )));
        }
        let trailer_size = Self::TRAILER_SIZE as i64;
        total_size -= trailer_size as u64;
        buf_reader.seek(SeekFrom::End(-trailer_size))?;
        let start = buf_reader.read_u64::<byteorder::BigEndian>()?;
        let end = buf_reader.read_u64::<byteorder::BigEndian>()?;
        let mut prev_blob_id = [0u8; BlobId::LENGTH];
        buf_reader.read_exact(&mut prev_blob_id)?;
        buf_reader.seek(SeekFrom::Start(Self::HEADER_SIZE as u64))?;
        Ok(Self {
            reader: buf_reader,
            current_pos: Self::HEADER_SIZE as u64,
            total_size,
            prev_blob_id: BlobId(prev_blob_id),
            start,
            end,
            blob_format_version,
        })
    }

    /// Checkpoint sequence number of first event in the blob.
    #[allow(dead_code)]
    pub fn start_checkpoint_sequence_number(&self) -> CheckpointSequenceNumber {
        self.start
    }

    /// Checkpoint sequence number of last event in the blob.
    #[allow(dead_code)]
    pub fn end_checkpoint_sequence_number(&self) -> CheckpointSequenceNumber {
        self.end
    }

    /// Return the next event.
    fn next_event(&mut self) -> Result<IndexedStreamElement> {
        let entry = BlobEntry::read(&mut self.reader)?;
        self.current_pos += entry.size() as u64;
        entry.decode()
    }
}

impl Iterator for EventBlob {
    type Item = IndexedStreamElement;
    fn next(&mut self) -> Option<Self::Item> {
        if self.current_pos >= self.total_size {
            return None;
        }
        self.next_event().ok()
    }
}

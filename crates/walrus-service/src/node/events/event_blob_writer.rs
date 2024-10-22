// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
#![allow(dead_code)]

//! Event blob writer module for writing event blobs to Walrus.

use std::{
    fs,
    fs::{File, OpenOptions},
    io::{BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result};
use byteorder::{BigEndian, WriteBytesExt};
use futures_util::future::try_join_all;
use rocksdb::Options;
use serde::{Deserialize, Serialize};
use sui_types::messages_checkpoint::CheckpointSequenceNumber;
use typed_store::{
    rocks,
    rocks::{errors::typed_store_err_from_rocks_err, DBMap, MetricConf, ReadWriteOptions},
    Map,
};
use walrus_core::{BlobId, Sliver};

use crate::node::{
    errors::StoreSliverError,
    events::{
        event_blob::{BlobEntry, EntryEncoding, EventBlob},
        EventSequenceNumber,
        IndexedStreamElement,
    },
    ServiceState,
    StorageNodeInner,
};

const EVENT_BLOB_METADATA_STORE: &str = "event_blob_metadata_store";
const NUM_CHECKPOINTS_PER_BLOB: u64 = 100;

#[derive(Eq, PartialEq, Debug, Clone, Deserialize, Serialize)]
struct EventBlobMetadata {
    /// Blob id of the event blob.
    blob_id: BlobId,
    /// Starting Sui checkpoint sequence number of the events in the blob (inclusive).
    start: CheckpointSequenceNumber,
    /// Ending Sui checkpoint sequence number of the events in the blob (inclusive).
    end: CheckpointSequenceNumber,
    /// EventSequenceNumber of the last event in the blob.
    last_event_sequence_number: EventSequenceNumber,
    /// index of the last event in the blob.
    last_event_index: u64,
}

/// EventBlobWriter writes events to event blobs.
#[derive(Debug)]
pub struct EventBlobWriter {
    /// Root directory path where the event blobs are stored.
    root_dir_path: PathBuf,
    /// Starting Sui checkpoint sequence number of the events in the current blob (inclusive).
    start: Option<CheckpointSequenceNumber>,
    /// Ending Sui checkpoint sequence number of the events in the current blob (inclusive).
    end: Option<CheckpointSequenceNumber>,
    /// Buffered writer for the current blob file.
    wbuf: BufWriter<File>,
    /// Offset in the current blob file where the next event will be written.
    buf_offset: usize,
    /// Maximum size of the current blob file before it is committed.
    number_of_checkpoints_per_blob: u64,
    /// DBMap from () to the metadata of the previous event blob.
    prev_blob_id: DBMap<(), EventBlobMetadata>,
    /// Client to store the slivers and metadata of the event blob.
    node: Arc<StorageNodeInner>,
    /// The latest event sequence number in the current blob.
    latest_sequence_number: Option<EventSequenceNumber>,
    /// The latest event index in the current blob.
    latest_event_index: Option<u64>,
    /// The last event sequence number in the prev committed blob.
    latest_committed_event_sequence_number: Option<EventSequenceNumber>,
    /// The last event index in the prev committed blob.
    latest_committed_event_index: Option<u64>,
}

impl EventBlobWriter {
    /// Create a new EventBlobWriter.
    pub fn new(root_dir_path: &Path, node: Arc<StorageNodeInner>) -> Result<Self> {
        if root_dir_path.exists() {
            fs::remove_dir_all(root_dir_path)?;
        }
        fs::create_dir_all(root_dir_path)?;
        let mut db_opts = Options::default();
        let metric_conf = MetricConf::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);
        let database = rocks::open_cf_opts(
            root_dir_path.join("event_blob_metadata_db"),
            Some(db_opts),
            metric_conf,
            &[(EVENT_BLOB_METADATA_STORE, Options::default())],
        )?;
        if database.cf_handle(EVENT_BLOB_METADATA_STORE).is_none() {
            database
                .create_cf(EVENT_BLOB_METADATA_STORE, &Options::default())
                .map_err(typed_store_err_from_rocks_err)?;
        }
        let prev_blob_id = DBMap::reopen(
            &database,
            Some(EVENT_BLOB_METADATA_STORE),
            &ReadWriteOptions::default(),
            false,
        )?;
        let file = Self::next_file(root_dir_path, EventBlob::MAGIC, EventBlob::FORMAT_VERSION)?;
        let latest_committed_event_sequence_number = prev_blob_id
            .get(&())?
            .map(|b: EventBlobMetadata| b.last_event_sequence_number);
        let latest_committed_event_index = prev_blob_id
            .get(&())?
            .map(|b: EventBlobMetadata| b.last_event_index);
        Ok(Self {
            root_dir_path: root_dir_path.to_path_buf(),
            start: None,
            end: None,
            wbuf: BufWriter::new(file),
            buf_offset: EventBlob::MAGIC_BYTES_SIZE,
            // TODO: Read this from system object on epoch change
            number_of_checkpoints_per_blob: NUM_CHECKPOINTS_PER_BLOB,
            prev_blob_id,
            node,
            latest_sequence_number: None,
            latest_event_index: None,
            latest_committed_event_sequence_number,
            latest_committed_event_index,
        })
    }

    /// Create a new file for the next blob.
    #[cfg(any(test, feature = "test-utils"))]
    fn new_for_testing(
        root_dir_path: PathBuf,
        number_of_checkpoints_per_blob: u64,
        node: Arc<StorageNodeInner>,
    ) -> Result<Self> {
        if root_dir_path.exists() {
            fs::remove_dir_all(&root_dir_path)?;
        }
        fs::create_dir_all(&root_dir_path)?;
        let mut db_opts = Options::default();
        let metric_conf = MetricConf::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);
        let database = rocks::open_cf_opts(
            root_dir_path.join("event_blob_metadata_db"),
            Some(db_opts),
            metric_conf,
            &[(EVENT_BLOB_METADATA_STORE, Options::default())],
        )?;
        if database.cf_handle(EVENT_BLOB_METADATA_STORE).is_none() {
            database
                .create_cf(EVENT_BLOB_METADATA_STORE, &Options::default())
                .map_err(typed_store_err_from_rocks_err)?;
        }
        let prev_blob_id = DBMap::reopen(
            &database,
            Some(EVENT_BLOB_METADATA_STORE),
            &ReadWriteOptions::default(),
            false,
        )?;
        let file = Self::next_file(&root_dir_path, EventBlob::MAGIC, EventBlob::FORMAT_VERSION)?;
        let latest_committed_event_sequence_number: Option<EventSequenceNumber> = prev_blob_id
            .get(&())?
            .map(|b: EventBlobMetadata| b.last_event_sequence_number);
        let latest_committed_event_index = prev_blob_id
            .get(&())?
            .map(|b: EventBlobMetadata| b.last_event_index);
        Ok(Self {
            root_dir_path,
            start: None,
            end: None,
            wbuf: BufWriter::new(file),
            buf_offset: EventBlob::MAGIC_BYTES_SIZE,
            number_of_checkpoints_per_blob,
            prev_blob_id,
            node,
            latest_sequence_number: None,
            latest_event_index: None,
            latest_committed_event_sequence_number,
            latest_committed_event_index,
        })
    }

    /// Create a new file for the next blob.
    fn next_file(dir_path: &Path, magic_bytes: u32, blob_format_version: u32) -> Result<File> {
        let next_file_path = dir_path.join("current_blob");
        let mut file = File::create(&next_file_path)?;
        file.write_u32::<BigEndian>(magic_bytes)?;
        file.write_u32::<BigEndian>(blob_format_version)?;
        drop(file);

        // File::set_len requires write, not append access rights on Windows.
        #[cfg(target_os = "windows")]
        {
            file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(next_file_path)?;
        }
        #[cfg(not(target_os = "windows"))]
        {
            file = OpenOptions::new()
                .read(true)
                .append(true)
                .open(next_file_path)?;
        }

        file.seek(SeekFrom::Start(EventBlob::HEADER_SIZE as u64))?;
        Ok(file)
    }

    /// Finalize the current blob.
    fn finalize(&mut self) -> Result<()> {
        self.wbuf
            .write_u64::<BigEndian>(self.start.unwrap_or_default())?;
        self.wbuf
            .write_u64::<BigEndian>(self.end.unwrap_or_default())?;
        self.wbuf.write_all(
            &self
                .prev_blob_id
                .get(&())?
                .map(|b| b.blob_id.0)
                .unwrap_or([0; 32])[..],
        )?;
        self.wbuf.flush()?;
        self.wbuf.get_ref().sync_data()?;
        let off = self.wbuf.get_ref().stream_position()?;
        self.wbuf.get_ref().set_len(off)?;
        Ok(())
    }

    /// Reset the writer to start writing a new blob.
    fn reset(&mut self) -> Result<()> {
        self.start = None;
        self.end = None;
        let f = Self::next_file(
            &self.root_dir_path,
            EventBlob::MAGIC,
            EventBlob::FORMAT_VERSION,
        )?;
        self.buf_offset = EventBlob::MAGIC_BYTES_SIZE + EventBlob::FORMAT_BYTES_SIZE;
        self.wbuf = BufWriter::new(f);
        Ok(())
    }

    /// Store the slivers of the blob content.
    async fn store_slivers(&mut self, content: &[u8]) -> Result<BlobId> {
        let blob_encoder = self.node.encoding_config().get_blob_encoder(content)?;
        let (sliver_pairs, blob_metadata) = blob_encoder.encode_with_metadata();
        self.node
            .storage()
            .put_verified_metadata(&blob_metadata)
            .context("unable to store metadata")?;
        // TODO: Once shard assignment per storage node will be read from walrus
        // system object at the beginning of the walrus epoch, we can only store the blob for
        // shards that are locally assigned to this node. (#682)
        try_join_all(sliver_pairs.iter().map(|sliver_pair| async {
            self.node
                .store_sliver(
                    blob_metadata.blob_id(),
                    sliver_pair.index(),
                    &Sliver::Primary(sliver_pair.primary.clone()),
                )
                .map_or_else(
                    |e| {
                        if matches!(e, StoreSliverError::ShardNotAssigned(_)) {
                            Ok(())
                        } else {
                            Err(e)
                        }
                    },
                    |_| Ok(()),
                )
        }))
        .await
        .map(|_| ())?;
        try_join_all(sliver_pairs.iter().map(|sliver_pair| async {
            self.node
                .store_sliver(
                    blob_metadata.blob_id(),
                    sliver_pair.index(),
                    &Sliver::Secondary(sliver_pair.secondary.clone()),
                )
                .map_or_else(
                    |e| {
                        if matches!(e, StoreSliverError::ShardNotAssigned(_)) {
                            Ok(())
                        } else {
                            Err(e)
                        }
                    },
                    |_| Ok(()),
                )
        }))
        .await
        .map(|_| ())?;
        Ok(*blob_metadata.blob_id())
    }

    /// Write an event to the current blob. If the event is an end of epoch event or the current
    /// blob reaches the maximum number of processed sui checkpoints, the current blob is committed
    #[cfg(not(test))]
    async fn cut(&mut self) -> Result<()> {
        self.finalize()?;
        let mut file = self.wbuf.get_ref();
        file.seek(SeekFrom::Start(0))?;
        let mut file_content = Vec::new();
        file.read_to_end(&mut file_content)?;
        let blob_id = self.store_slivers(&file_content).await?;
        // TODO: Once slivers are stored locally, invoke the new sui contract endpoint to certify
        // the event blob. (#683)
        let event_blob_metadata = EventBlobMetadata {
            blob_id,
            start: self.start.as_ref().cloned().unwrap_or_default(),
            end: self.end.as_ref().cloned().unwrap_or_default(),
            last_event_sequence_number: self.latest_sequence_number.clone().unwrap_or_default(),
            last_event_index: self.latest_committed_event_index.unwrap_or_default(),
        };
        self.prev_blob_id.insert(&(), &event_blob_metadata)?;
        self.latest_committed_event_sequence_number = self.latest_sequence_number.clone();
        self.latest_committed_event_index = self.latest_event_index;
        Ok(())
    }

    /// Write an event to the current blob. If the event is an end of epoch event or the current
    /// blob reaches the maximum number of processed sui checkpoints, the current blob is committed
    #[cfg(test)]
    async fn cut(&mut self) -> Result<()> {
        self.finalize()?;
        let mut file = self.wbuf.get_ref();
        file.seek(SeekFrom::Start(0))?;
        let mut file_content = Vec::new();
        file.read_to_end(&mut file_content)?;
        let mut tmp_file = File::create(self.root_dir_path.join("event_blob"))?;
        tmp_file.write_all(&file_content)?;
        tmp_file.flush()?;
        Ok(())
    }

    /// Write an event to the current blob. If the event is an end of epoch event or the current
    /// blob reaches the maximum number of processed sui checkpoints, the current blob is committed
    /// and a new blob is started.
    pub async fn write(&mut self, element: IndexedStreamElement, element_index: u64) -> Result<()> {
        if self
            .latest_committed_event_index()
            .map_or(false, |i| i >= element_index)
        {
            // It is possible to receive committed events upon restart
            return Ok(());
        }

        self.update_sequence_range(&element, element_index);
        self.write_event_to_buffer(&element)?;
        let cut_new_blob = element.is_end_of_epoch_event()
            || (element.is_end_of_checkpoint_marker()
                && (element.global_sequence_number.checkpoint_sequence_number + 1)
                    % self.number_of_checkpoints_per_blob
                    == 0);
        if cut_new_blob {
            self.commit().await?;
        }

        Ok(())
    }

    /// Update the sequence range of the current blob.
    fn update_sequence_range(&mut self, element: &IndexedStreamElement, element_index: u64) {
        if self.start.is_none() {
            self.start = Some(element.global_sequence_number.checkpoint_sequence_number);
        }
        self.end = Some(element.global_sequence_number.checkpoint_sequence_number);
        self.latest_sequence_number = Some(element.global_sequence_number.clone());
        self.latest_event_index = Some(element_index);
    }

    /// Write an event to the buffer.
    fn write_event_to_buffer(&mut self, event: &IndexedStreamElement) -> Result<()> {
        if event.is_end_of_checkpoint_marker() {
            return Ok(());
        }
        let entry = BlobEntry::encode(event, EntryEncoding::Bcs)?;
        self.buf_offset += entry.write(&mut self.wbuf)?;
        Ok(())
    }

    /// Commit the current blob.
    pub async fn commit(&mut self) -> Result<()> {
        self.cut().await?;
        self.reset()?;
        Ok(())
    }

    /// Get the latest committed event sequence number.
    pub fn latest_committed_event_sequence_number(&self) -> Option<EventSequenceNumber> {
        self.latest_committed_event_sequence_number.clone()
    }

    /// Get the latest committed event index.
    pub fn latest_committed_event_index(&self) -> Option<u64> {
        self.latest_committed_event_index
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use walrus_core::{BlobId, ShardIndex};
    use walrus_sui::{test_utils::EventForTesting, types::BlobCertified};

    use crate::{
        node::events::{
            event_blob::EventBlob,
            event_blob_writer::EventBlobWriter,
            EventSequenceNumber,
            EventStreamElement,
            IndexedStreamElement,
        },
        test_utils::StorageNodeHandle,
    };

    #[tokio::test]
    async fn test_e2e() -> anyhow::Result<()> {
        let dir: PathBuf = tempfile::tempdir()
            .expect("Failed to open temporary directory")
            .into_path();
        let node = StorageNodeHandle::builder()
            .with_system_event_provider(vec![])
            .with_shard_assignment(&[ShardIndex(0)])
            .with_node_started(true)
            .build()
            .await?;
        let mut blob_writer =
            EventBlobWriter::new_for_testing(dir.clone(), 100, node.storage_node.inner().clone())?;
        let event_blob_file_path = dir.join("event_blob");
        if event_blob_file_path.exists() {
            fs::remove_file(event_blob_file_path.clone())?;
        }
        let mut counter = 0;
        // Write events into the blob
        for i in 0..100 {
            let event = IndexedStreamElement {
                global_sequence_number: EventSequenceNumber::new(i as u64, 0),
                element: EventStreamElement::ContractEvent(
                    BlobCertified::for_testing(BlobId([7; 32])).into(),
                ),
            };
            blob_writer.write(event, counter).await?;
            counter += 1;
            let end_of_checkpoint_marker_event =
                IndexedStreamElement::new_checkpoint_boundary(i as u64, 1);
            blob_writer
                .write(end_of_checkpoint_marker_event, counter)
                .await?;
            counter += 1;
        }
        // Read back the events from the blob
        let file = std::fs::File::open(event_blob_file_path)?;
        let event_blob = EventBlob::new(file)?;
        assert_eq!(event_blob.start_checkpoint_sequence_number(), 0);
        assert_eq!(event_blob.end_checkpoint_sequence_number(), 99);
        let events: Vec<_> = event_blob.collect();
        assert_eq!(events.len(), 100);
        for (i, event) in events.iter().enumerate() {
            assert_eq!(
                event.global_sequence_number.checkpoint_sequence_number,
                i as u64
            );
        }
        Ok(())
    }
}

// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
#![allow(dead_code)]

use std::{
    fs,
    fs::{File, OpenOptions},
    io::{BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use anyhow::Result;
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
use url::Url;
use walrus_core::{encoding::EncodingConfig, BlobId};
use walrus_sdk::client::Client;
use walrus_service::node::config::StorageNodeConfig;

use crate::{
    checkpoint_processor::WalrusEvent,
    event_blob::{BlobEntry, EventBlob},
};

const EVENT_BLOB_METADATA_STORE: &str = "event_blob_metadata_store";

#[derive(Eq, PartialEq, Debug, Clone, Deserialize, Serialize)]
struct EventBlobMetadata {
    /// Blob id of the event blob.
    blob_id: BlobId,
    /// Starting Sui checkpoint sequence number of the events in the blob (inclusive).
    start: CheckpointSequenceNumber,
    /// Ending Sui checkpoint sequence number of the events in the blob (inclusive).
    end: CheckpointSequenceNumber,
}

struct EventBlobWriter {
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
    client: Client,
    /// Encoding configuration for the event blob.
    encoding_config: EncodingConfig,
}

impl EventBlobWriter {
    fn new(
        root_dir_path: PathBuf,
        number_of_checkpoints_per_blob: u64,
        node_config: StorageNodeConfig,
        encoding_config: EncodingConfig,
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
        let client = {
            let url = Url::parse(&format!("http://{}", node_config.rest_api_address))?;
            let inner = reqwest::Client::builder().no_proxy().build().unwrap();
            Client::from_url(url, inner)
        };
        Ok(Self {
            root_dir_path,
            start: None,
            end: None,
            wbuf: BufWriter::new(file),
            buf_offset: EventBlob::MAGIC_BYTES_SIZE,
            number_of_checkpoints_per_blob,
            prev_blob_id,
            client,
            encoding_config,
        })
    }

    #[cfg(any(test, feature = "test-utils"))]
    fn new_for_testing(
        root_dir_path: PathBuf,
        number_of_checkpoints_per_blob: u64,
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
        let encoding_config = EncodingConfig::new(100.try_into().unwrap());
        let yaml = "---\n\
        storage_path: target/storage\n\
        protocol_key_pair:\n  BBlm7tRefoPuaKoVoxVtnUBBDCfy+BGPREM8B6oSkOEj\n\
        network_key_pair:\n  AFzrKKEebJ56OhX4QHH9NQshlGKEQuwdD3HlE5d3n0jm";

        let node_config: StorageNodeConfig = serde_yaml::from_str(yaml)?;
        let client = {
            let url = Url::parse(&format!("http://{}", node_config.rest_api_address))?;
            let inner = reqwest::Client::builder().no_proxy().build().unwrap();
            Client::from_url(url, inner)
        };
        Ok(Self {
            root_dir_path,
            start: None,
            end: None,
            wbuf: BufWriter::new(file),
            buf_offset: EventBlob::MAGIC_BYTES_SIZE,
            number_of_checkpoints_per_blob,
            prev_blob_id,
            client,
            encoding_config,
        })
    }

    fn next_file(dir_path: &Path, magic_bytes: u32, blob_format_version: u32) -> Result<File> {
        let next_file_path = dir_path.join("current_blob");
        let mut file = File::create(&next_file_path)?;
        file.write_u32::<BigEndian>(magic_bytes)?;
        file.write_u32::<BigEndian>(blob_format_version)?;
        drop(file);
        file = OpenOptions::new()
            .append(true)
            .read(true)
            .open(next_file_path)?;
        file.seek(SeekFrom::Start(EventBlob::HEADER_SIZE as u64))?;
        Ok(file)
    }

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

    async fn store_slivers(&mut self, content: &[u8]) -> Result<()> {
        let blob_encoder = self.encoding_config.get_blob_encoder(content)?;
        let (sliver_pairs, blob_metadata) = blob_encoder.encode_with_metadata();
        self.client.store_metadata(&blob_metadata).await?;
        // TODO: Once shard assignment per storage node will be read from walrus
        // system object at the beginning of the walrus epoch, we can only store the blob for
        // shards that are locally assigned to this node. (#682)
        try_join_all(sliver_pairs.iter().map(|sliver_pair| async {
            self.client
                .store_sliver(
                    blob_metadata.blob_id(),
                    sliver_pair.index(),
                    &sliver_pair.primary,
                )
                .await
                .or_else(|e| {
                    if e.is_shard_not_assigned() {
                        Ok(())
                    } else {
                        Err(e)
                    }
                })
        }))
        .await
        .map(|_| ())?;
        try_join_all(sliver_pairs.iter().map(|sliver_pair| async {
            self.client
                .store_sliver(
                    blob_metadata.blob_id(),
                    sliver_pair.index(),
                    &sliver_pair.secondary,
                )
                .await
                .or_else(|e| {
                    if e.is_shard_not_assigned() {
                        Ok(())
                    } else {
                        Err(e)
                    }
                })
        }))
        .await
        .map(|_| ())?;
        // TODO: Once slivers are stored locally, invoke the new sui contract endpoint to certify
        // the event blob. (#683)
        let event_blob_metadata = EventBlobMetadata {
            blob_id: *blob_metadata.blob_id(),
            start: self.start.as_ref().cloned().unwrap_or_default(),
            end: self.end.as_ref().cloned().unwrap_or_default(),
        };
        self.prev_blob_id.insert(&(), &event_blob_metadata)?;
        Ok(())
    }

    #[cfg(not(test))]
    async fn cut(&mut self) -> Result<()> {
        self.finalize()?;
        let mut file = self.wbuf.get_ref();
        file.seek(SeekFrom::Start(0))?;
        let mut file_content = Vec::new();
        file.read_to_end(&mut file_content)?;
        self.store_slivers(&file_content).await?;
        Ok(())
    }

    #[cfg(test)]
    async fn cut(&mut self) -> Result<()> {
        self.finalize()?;
        let mut file = self.wbuf.get_ref();
        file.seek(SeekFrom::Start(0))?;
        let mut file_content = Vec::new();
        file.read_to_end(&mut file_content)?;
        let mut tmp_file = File::create("/tmp/event_blob")?;
        tmp_file.write_all(&file_content)?;
        tmp_file.flush()?;
        Ok(())
    }

    pub async fn write(&mut self, event: WalrusEvent) -> Result<()> {
        self.update_sequence_range(&event);
        self.write_event_to_buffer(&event)?;
        let cut_new_blob = event.is_end_of_walrus_epoch_event
            || (event.is_end_of_checkpoint_marker()
                && (event.event_id.sequence_number + 1) % self.number_of_checkpoints_per_blob == 0);
        if cut_new_blob {
            self.commit().await?;
        }
        Ok(())
    }

    fn update_sequence_range(&mut self, event: &WalrusEvent) {
        if self.start.is_none() {
            self.start = Some(event.event_id.sequence_number);
        }
        self.end = Some(event.event_id.sequence_number);
    }

    fn write_event_to_buffer(&mut self, event: &WalrusEvent) -> Result<()> {
        if event.is_end_of_checkpoint_marker() {
            return Ok(());
        }
        let entry = BlobEntry::encode(event, crate::event_blob::EntryEncoding::Bcs)?;
        self.buf_offset += entry.write(&mut self.wbuf)?;
        Ok(())
    }

    pub async fn commit(&mut self) -> Result<()> {
        self.cut().await?;
        self.reset()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::{Path, PathBuf},
    };

    use crate::{
        blob_writer::EventBlobWriter,
        checkpoint_processor::{WalrusEvent, WalrusEventID},
    };

    #[tokio::test]
    async fn test_e2e() {
        let dir: PathBuf = tempfile::tempdir()
            .expect("Failed to open temporary directory")
            .into_path();
        let mut blob_writer = EventBlobWriter::new_for_testing(dir, 100).unwrap();
        if Path::new("/tmp/event_blob").exists() {
            fs::remove_file("/tmp/event_blob").unwrap();
        }
        // Write events into the blob
        for i in 0..100 {
            let serialized_event = vec![0u8; 1024];
            let event = WalrusEvent {
                event_id: WalrusEventID::new(i, 0),
                serialized_event,
                is_end_of_walrus_epoch_event: false,
            };
            blob_writer.write(event).await.unwrap();
            let end_of_checkpoint_marker_event = WalrusEvent::new_end_of_checkpoint(i);
            blob_writer
                .write(end_of_checkpoint_marker_event)
                .await
                .unwrap();
        }
        // Read back the events from the blob
        let file = std::fs::File::open("/tmp/event_blob").unwrap();
        let event_blob = crate::event_blob::EventBlob::new(file).unwrap();
        assert_eq!(event_blob.start_checkpoint_sequence_number(), 0);
        assert_eq!(event_blob.end_checkpoint_sequence_number(), 99);
        let events: Vec<_> = event_blob.collect();
        assert_eq!(events.len(), 100);
        for (i, event) in events.iter().enumerate() {
            assert_eq!(event.event_id.sequence_number, i as u64);
            assert_eq!(event.serialized_event.len(), 1024);
        }
    }
}

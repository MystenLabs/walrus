// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
#![allow(dead_code)]

//! Event blob writer.

use std::{
    fs,
    fs::{File, OpenOptions},
    io,
    io::{BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result};
use byteorder::{BigEndian, WriteBytesExt};
use futures_util::future::try_join_all;
use prometheus::{register_int_gauge_with_registry, IntGauge, Registry};
use rocksdb::Options;
use serde::{Deserialize, Serialize};
use sui_types::messages_checkpoint::CheckpointSequenceNumber;
use typed_store::{
    rocks,
    rocks::{errors::typed_store_err_from_rocks_err, DBBatch, DBMap, MetricConf, ReadWriteOptions},
    Map,
};
use walrus_core::{ensure, metadata::VerifiedBlobMetadataWithId, BlobId, Epoch, Sliver};
use walrus_sui::types::{BlobEvent, ContractEvent, EpochChangeEvent};

use crate::{
    events::{
        event_blob::{BlobEntry, EntryEncoding, EventBlob},
        EventStreamCursor,
        EventStreamElement,
        IndexedStreamElement,
        InitState,
    },
    node::{
        contract_service::SystemContractService,
        errors::StoreSliverError,
        ServiceState,
        StorageNodeInner,
    },
};

const CERTIFIED: &str = "certified_blob_store";
const ATTESTED: &str = "attested_blob_store";
const PENDING: &str = "pending_blob_store";
const MAX_BLOB_SIZE: usize = 5 * 1024 * 1024;

/// Metadata for a blob that is waiting for attestation.
#[derive(Eq, PartialEq, Debug, Clone, Deserialize, Serialize)]
pub struct PendingEventBlobMetadata {
    /// The starting Sui checkpoint sequence number of the events in the blob (inclusive).
    pub start: CheckpointSequenceNumber,
    /// The ending Sui checkpoint sequence number of the events in the blob (inclusive).
    pub end: CheckpointSequenceNumber,
    /// The event cursor of the last event in the blob.
    pub event_cursor: EventStreamCursor,
    /// The epoch of the events in the blob.
    pub epoch: Epoch,
}

/// Metadata for a blob that is last attested.
#[derive(Eq, PartialEq, Debug, Clone, Deserialize, Serialize)]
pub struct AttestedBlobMetadata {
    /// The starting Sui checkpoint sequence number of the events in the blob (inclusive).
    pub start: CheckpointSequenceNumber,
    /// The ending Sui checkpoint sequence number of the events in the blob (inclusive).
    pub end: CheckpointSequenceNumber,
    /// The event cursor of the last event in the blob.
    pub event_cursor: EventStreamCursor,
    /// The epoch of the events in the blob.
    pub epoch: Epoch,
    /// The blob ID of the blob.
    pub blob_id: BlobId,
}

/// Metadata for a blob that is last certified.
#[derive(Eq, PartialEq, Debug, Clone, Deserialize, Serialize)]
pub struct CertifiedEventBlobMetadata {
    /// The blob ID of the blob.
    pub blob_id: BlobId,
    /// The event cursor of the last event in the blob.
    pub event_cursor: EventStreamCursor,
    /// The epoch of the events in the blob.
    pub epoch: Epoch,
}

impl PendingEventBlobMetadata {
    fn new(
        start: CheckpointSequenceNumber,
        end: CheckpointSequenceNumber,
        event_cursor: EventStreamCursor,
        epoch: Epoch,
    ) -> Self {
        Self {
            start,
            end,
            event_cursor,
            epoch,
        }
    }

    fn to_attested(&self, blob_metadata: VerifiedBlobMetadataWithId) -> AttestedBlobMetadata {
        AttestedBlobMetadata {
            start: self.start,
            end: self.end,
            event_cursor: self.event_cursor.clone(),
            epoch: self.epoch,
            blob_id: *blob_metadata.blob_id(),
        }
    }
}

impl AttestedBlobMetadata {
    fn to_certified(&self) -> CertifiedEventBlobMetadata {
        CertifiedEventBlobMetadata {
            blob_id: self.blob_id,
            event_cursor: self.event_cursor.clone(),
            epoch: self.epoch,
        }
    }
    fn to_pending(&self) -> PendingEventBlobMetadata {
        PendingEventBlobMetadata {
            start: self.start,
            end: self.end,
            event_cursor: self.event_cursor.clone(),
            epoch: self.epoch,
        }
    }
}

/// Metrics for the event processor.
#[derive(Clone, Debug)]
pub struct EventBlobWriterMetrics {
    /// The latest downloaded full checkpoint.
    pub latest_certified_event_index: IntGauge,
}

impl EventBlobWriterMetrics {
    /// Create a new event processor metrics.
    pub fn new(registry: &Registry) -> Self {
        Self {
            latest_certified_event_index: register_int_gauge_with_registry!(
                "event_blob_writer_latest_certified_event_index",
                "Latest certified event blob writer index",
                registry,
            )
            .expect("this is a valid metrics registration"),
        }
    }
}

/// Factory for creating event blob writers.
#[derive(Clone, Debug)]
pub struct EventBlobWriterFactory {
    /// Root directory path where the event blobs are stored.
    root_dir_path: PathBuf,
    /// Client to store the slivers and metadata of the event blob.
    node: Arc<StorageNodeInner>,
    /// Sui contract client
    system_contract_service: Arc<dyn SystemContractService>,
    /// Metrics for the event blob writer.
    metrics: Arc<EventBlobWriterMetrics>,
    /// Current event cursor.
    event_cursor: Option<EventStreamCursor>,
    /// Certified blobs metadata.
    certified: DBMap<(), CertifiedEventBlobMetadata>,
    /// Attested blobs metadata.
    attested: DBMap<(), AttestedBlobMetadata>,
    /// Pending blobs metadata.
    pending: DBMap<u64, PendingEventBlobMetadata>,
}

impl EventBlobWriterFactory {
    /// Create a new event blob writer factory.
    pub fn new(
        root_dir_path: &Path,
        node: Arc<StorageNodeInner>,
        system_contract_service: Arc<dyn SystemContractService>,
        registry: &Registry,
    ) -> Result<EventBlobWriterFactory> {
        let dirs = ["db", "blobs"];
        fs::create_dir_all(root_dir_path)?;
        for dir in dirs.iter() {
            fs::create_dir_all(root_dir_path.join(dir))?;
        }
        let mut db_opts = Options::default();
        let metric_conf = MetricConf::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);
        let database = rocks::open_cf_opts(
            root_dir_path.join("db"),
            Some(db_opts),
            metric_conf,
            &[
                (PENDING, Options::default()),
                (ATTESTED, Options::default()),
                (CERTIFIED, Options::default()),
            ],
        )?;
        if database.cf_handle(CERTIFIED).is_none() {
            database
                .create_cf(CERTIFIED, &Options::default())
                .map_err(typed_store_err_from_rocks_err)?;
        }
        if database.cf_handle(ATTESTED).is_none() {
            database
                .create_cf(ATTESTED, &Options::default())
                .map_err(typed_store_err_from_rocks_err)?;
        }
        if database.cf_handle(PENDING).is_none() {
            database
                .create_cf(PENDING, &Options::default())
                .map_err(typed_store_err_from_rocks_err)?;
        }
        let certified: DBMap<(), CertifiedEventBlobMetadata> = DBMap::reopen(
            &database,
            Some(CERTIFIED),
            &ReadWriteOptions::default(),
            false,
        )?;
        let attested: DBMap<(), AttestedBlobMetadata> = DBMap::reopen(
            &database,
            Some(ATTESTED),
            &ReadWriteOptions::default(),
            false,
        )?;
        let pending: DBMap<u64, PendingEventBlobMetadata> = DBMap::reopen(
            &database,
            Some(PENDING),
            &ReadWriteOptions::default(),
            false,
        )?;
        let event_cursor = pending
            .unbounded_iter()
            .last()
            .map(|(_, metadata)| metadata.event_cursor)
            .or_else(|| {
                attested
                    .get(&())
                    .ok()
                    .flatten()
                    .map(|metadata| metadata.event_cursor)
            })
            .or_else(|| {
                certified
                    .get(&())
                    .ok()
                    .flatten()
                    .map(|metadata| metadata.event_cursor)
            });
        Ok(Self {
            root_dir_path: root_dir_path.to_path_buf(),
            node,
            system_contract_service,
            metrics: Arc::new(EventBlobWriterMetrics::new(registry)),
            event_cursor,
            certified,
            attested,
            pending,
        })
    }

    /// Get the current event cursor.
    pub fn event_cursor(&self) -> Option<EventStreamCursor> {
        self.event_cursor.clone()
    }

    /// Create a new event blob writer.
    pub async fn create(&self, init_state: Option<InitState>) -> Result<EventBlobWriter> {
        let prev_event_blob = init_state.map(|state| CertifiedEventBlobMetadata {
            blob_id: state.prev_blob_id,
            event_cursor: state.event_cursor,
            epoch: state.epoch,
        });
        let databases = EventBlobDatabases {
            certified: self.certified.clone(),
            attested: self.attested.clone(),
            pending: self.pending.clone(),
        };
        let config = EventBlobWriterConfig {
            blob_dir_path: self.root_dir_path.clone().into(),
            prev_event_blob,
            metrics: self.metrics.clone(),
        };
        EventBlobWriter::new(
            config,
            self.node.clone(),
            self.system_contract_service.clone(),
            databases,
        )
        .await
    }
}

/// Event blob writer.
#[derive(Debug)]
pub struct EventBlobWriter {
    /// Root directory path where the event blobs are stored.
    blob_dir_path: PathBuf,
    /// Starting Sui checkpoint sequence number of the events in the current blob (inclusive).
    start: Option<CheckpointSequenceNumber>,
    /// Ending Sui checkpoint sequence number of the events in the current blob (inclusive).
    end: Option<CheckpointSequenceNumber>,
    /// Buffered writer for the current blob file.
    wbuf: BufWriter<File>,
    /// Offset in the current blob file where the next event will be written.
    buf_offset: usize,
    /// Certified blobs metadata.
    certified: DBMap<(), CertifiedEventBlobMetadata>,
    /// Attested blobs metadata.
    attested: DBMap<(), AttestedBlobMetadata>,
    /// Pending blobs metadata.
    pending: DBMap<u64, PendingEventBlobMetadata>,
    /// Client to store the slivers and metadata of the event blob.
    node: Arc<StorageNodeInner>,
    /// Sui contract client
    system_contract_service: Arc<dyn SystemContractService>,
    /// Current epoch.
    current_epoch: Epoch,
    /// Next event index.
    event_cursor: EventStreamCursor,
    /// Blob ID of the last certified blob.
    prev_certified_blob_id: BlobId,
    /// Metrics for the event blob writer.
    metrics: Arc<EventBlobWriterMetrics>,
}

/// Struct to group database-related parameters.
pub struct EventBlobDatabases {
    certified: DBMap<(), CertifiedEventBlobMetadata>,
    attested: DBMap<(), AttestedBlobMetadata>,
    pending: DBMap<u64, PendingEventBlobMetadata>,
}

/// Struct to group configuration-related parameters
pub struct EventBlobWriterConfig {
    /// Root directory path where the event blobs are stored.
    blob_dir_path: Arc<Path>,
    /// Metadata of the previous event blob.
    prev_event_blob: Option<CertifiedEventBlobMetadata>,
    /// Metrics for the event blob writer.
    metrics: Arc<EventBlobWriterMetrics>,
}

impl EventBlobWriter {
    /// Create a new event blob writer.
    pub async fn new(
        config: EventBlobWriterConfig,
        node: Arc<StorageNodeInner>,
        system_contract_service: Arc<dyn SystemContractService>,
        databases: EventBlobDatabases,
    ) -> Result<Self> {
        let event_cursor = databases
            .pending
            .unbounded_iter()
            .last()
            .map(|(_, metadata)| metadata.event_cursor)
            .or_else(|| {
                databases
                    .attested
                    .get(&())
                    .ok()
                    .flatten()
                    .map(|metadata| metadata.event_cursor)
            })
            .or_else(|| {
                databases
                    .certified
                    .get(&())
                    .ok()
                    .flatten()
                    .map(|metadata| metadata.event_cursor)
            })
            .or_else(|| {
                config
                    .prev_event_blob
                    .as_ref()
                    .map(|metadata| metadata.event_cursor.clone())
            })
            .unwrap_or(EventStreamCursor::new(None, 0));

        let current_epoch = databases
            .pending
            .unbounded_iter()
            .last()
            .map(|(_, metadata)| metadata.epoch)
            .or_else(|| {
                databases
                    .attested
                    .get(&())
                    .ok()
                    .flatten()
                    .map(|metadata| metadata.epoch)
            })
            .or_else(|| {
                databases
                    .certified
                    .get(&())
                    .ok()
                    .flatten()
                    .map(|metadata| metadata.epoch)
            })
            .or_else(|| {
                config
                    .prev_event_blob
                    .as_ref()
                    .map(|metadata| metadata.epoch)
            })
            .unwrap_or(0);

        let prev_certified_blob_id = databases
            .certified
            .get(&())
            .ok()
            .flatten()
            .map(|metadata| metadata.blob_id)
            .or_else(|| config.prev_event_blob.map(|metadata| metadata.blob_id))
            .unwrap_or(BlobId([0; 32]));

        let tmp_file = File::create(config.blob_dir_path.join("current_blob"))?;
        let mut blob_writer = Self {
            blob_dir_path: config.blob_dir_path.to_path_buf(),
            start: None,
            end: None,
            wbuf: BufWriter::new(tmp_file),
            buf_offset: 0,
            attested: databases.attested,
            certified: databases.certified,
            pending: databases.pending,
            node,
            system_contract_service,
            current_epoch,
            event_cursor,
            prev_certified_blob_id,
            metrics: config.metrics,
        };
        let file = blob_writer.next_file()?;
        let buf_writer = BufWriter::new(file);
        blob_writer.buf_offset = EventBlob::HEADER_SIZE;
        blob_writer.wbuf = buf_writer;
        blob_writer.attest_next_blob().await?;
        Ok(blob_writer)
    }

    fn next_file(&self) -> Result<File> {
        let next_file_path = self.blob_dir().join("current_blob");
        let mut file = self.create_and_initialize_file(&next_file_path)?;
        drop(file);
        file = self.reopen_file_with_permissions(&next_file_path)?;
        file.seek(SeekFrom::Start(EventBlob::HEADER_SIZE as u64))?;
        Ok(file)
    }

    fn blob_dir(&self) -> PathBuf {
        self.blob_dir_path.clone()
    }

    fn create_and_initialize_file(&self, path: &Path) -> Result<File> {
        let mut file = File::create(path)?;
        file.write_u32::<BigEndian>(EventBlob::MAGIC)?;
        file.write_u32::<BigEndian>(EventBlob::FORMAT_VERSION)?;
        file.write_u32::<BigEndian>(self.current_epoch)?;
        let zero_blob_id = BlobId([0; 32]);
        file.write_all(&zero_blob_id.0)?;
        Ok(file)
    }

    fn reopen_file_with_permissions(&self, path: &Path) -> io::Result<File> {
        #[cfg(target_os = "windows")]
        {
            OpenOptions::new().read(true).write(true).open(path)
        }
        #[cfg(not(target_os = "windows"))]
        {
            OpenOptions::new().read(true).write(true).open(path)
        }
    }

    fn rename_blob_file(&self) -> Result<()> {
        let from = self.blob_dir().join("current_blob");
        let to = self
            .blob_dir()
            .join(self.event_cursor.element_index.to_string());
        fs::rename(from, to)?;
        Ok(())
    }

    fn finalize(&mut self) -> Result<()> {
        self.wbuf
            .write_u64::<BigEndian>(self.start.unwrap_or_default())?;
        self.wbuf
            .write_u64::<BigEndian>(self.end.unwrap_or_default())?;
        self.wbuf.flush()?;
        self.wbuf.get_ref().sync_data()?;
        let off = self.wbuf.get_ref().stream_position()?;
        self.wbuf.get_ref().set_len(off)?;
        Ok(())
    }

    fn reset(&mut self) -> Result<()> {
        self.start = None;
        self.end = None;
        let f = self.next_file()?;
        self.buf_offset = EventBlob::HEADER_SIZE;
        self.wbuf = BufWriter::new(f);
        Ok(())
    }

    fn update_prev_blob_id(&self, event_index: u64, blob_id: &BlobId) -> Result<()> {
        let file_path = self.blob_dir().join(event_index.to_string());
        let mut file = self.reopen_file_with_permissions(&file_path)?;
        file.seek(SeekFrom::Start(EventBlob::BLOB_ID_OFFSET as u64))?;
        file.write_all(&blob_id.0)?;
        file.flush()?;
        file.sync_all()?;
        Ok(())
    }

    async fn store_slivers(
        &mut self,
        metadata: &PendingEventBlobMetadata,
    ) -> Result<VerifiedBlobMetadataWithId> {
        let file_path = self
            .blob_dir()
            .join(metadata.event_cursor.element_index.to_string());
        let mut file = File::open(&file_path)?;
        let mut content = Vec::new();
        file.read_to_end(&mut content)?;
        let (sliver_pairs, blob_metadata) = self
            .node
            .encoding_config()
            .get_blob_encoder(&content)?
            .encode_with_metadata();
        self.node
            .storage()
            .put_verified_metadata(&blob_metadata)
            .context("unable to store metadata")?;
        // TODO: Once shard assignment per storage node will be read from walrus
        // system object at the beginning of the walrus epoch, we can only store the blob for
        // shards that are locally assigned to this node. (#682)
        try_join_all(sliver_pairs.iter().map(|sliver_pair| async {
            self.node
                .store_sliver_unchecked(
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
                .store_sliver_unchecked(
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
        Ok(blob_metadata)
    }

    async fn cut(&mut self) -> Result<()> {
        self.finalize()?;
        self.rename_blob_file()?;
        Ok(())
    }

    async fn attest_blob(
        &self,
        metadata: VerifiedBlobMetadataWithId,
        checkpoint_sequence_number: CheckpointSequenceNumber,
    ) -> Result<()> {
        println!("attesting event blob in epoch: {}", self.current_epoch);
        match self
            .system_contract_service
            .certify_event_blob(
                *metadata.blob_id(),
                metadata.metadata().compute_root_hash().bytes(),
                metadata.metadata().unencoded_length,
                metadata.metadata().encoding_type,
                checkpoint_sequence_number,
                self.current_epoch,
            )
            .await
        {
            Ok(_) => {
                println!("attested event blob with id: {:?}", metadata.blob_id());
                Ok(())
            }
            Err(e) => {
                println!("failed to attest event blob: {:?}", e);
                tracing::error!("Failed to certify event blob: {:?}", e);
                Ok(())
            }
        }
    }

    async fn attest_next_blob(&mut self) -> Result<()> {
        if !self.attested.is_empty() {
            return Ok(());
        }

        let Some((event_index, metadata)) = self.pending.unbounded_iter().seek_to_first().next()
        else {
            return Ok(());
        };

        self.update_prev_blob_id(
            metadata.event_cursor.element_index,
            &self.prev_certified_blob_id,
        )?;
        let blob_metadata = self.store_slivers(&metadata).await?;
        let attested_metadata = metadata.to_attested(blob_metadata.clone());

        self.attest_blob(blob_metadata, metadata.end).await?;
        let mut batch = self.pending.batch();
        batch.insert_batch(&self.attested, std::iter::once(((), attested_metadata)))?;
        batch.delete_batch(&self.pending, std::iter::once(event_index))?;
        batch.write()?;

        Ok(())
    }

    /// Write an event to the current blob. If the event is an end of epoch event or the current
    /// blob reaches the maximum number of processed sui checkpoints, the current blob is committed
    /// and a new blob is started.
    pub async fn write(&mut self, element: IndexedStreamElement, element_index: u64) -> Result<()> {
        ensure!(
            element_index == self.event_cursor.element_index,
            "Invalid event index"
        );
        self.event_cursor = EventStreamCursor::new(element.element.event_id(), element_index + 1);
        self.update_sequence_range(&element);
        self.write_event_to_buffer(&element)?;

        let mut batch = self.pending.batch();
        self.update_epoch(&element, &mut batch)?;

        if self.should_cut_new_blob(&element) {
            self.cut_and_reset_blob(&mut batch, element_index).await?;
        }

        self.handle_event_blob_certification(&element, &mut batch)?;

        batch.write()?;

        self.attest_next_blob().await?;

        Ok(())
    }

    fn current_blob_size(&self) -> usize {
        self.buf_offset
    }

    fn should_cut_new_blob(&self, element: &IndexedStreamElement) -> bool {
        element.is_end_of_checkpoint_marker()
            && ((element.global_sequence_number.checkpoint_sequence_number + 1)
                % self.num_checkpoints_per_blob()
                == 0
                || self.current_blob_size() > MAX_BLOB_SIZE)
    }

    async fn cut_and_reset_blob(&mut self, batch: &mut DBBatch, element_index: u64) -> Result<()> {
        self.cut().await?;
        let blob_metadata = PendingEventBlobMetadata::new(
            self.start.unwrap(),
            self.end.unwrap(),
            self.event_cursor.clone(),
            self.current_epoch,
        );
        batch.insert_batch(
            &self.pending,
            std::iter::once((element_index, blob_metadata)),
        )?;
        self.reset()
    }

    fn handle_event_blob_certification(
        &mut self,
        element: &IndexedStreamElement,
        batch: &mut DBBatch,
    ) -> Result<()> {
        let Some(blob_id) = element
            .element
            .blob_event()
            .and_then(|blob_event| self.match_event_blob_is_certified(blob_event))
        else {
            return Ok(());
        };
        let Some(metadata) = self.attested.get(&())? else {
            return Ok(());
        };
        if metadata.blob_id != blob_id {
            return Ok(());
        }
        batch.delete_batch(&self.attested, std::iter::once(()))?;
        batch.insert_batch(
            &self.certified,
            std::iter::once(((), metadata.to_certified())),
        )?;

        let file_path = self
            .blob_dir()
            .join(metadata.event_cursor.element_index.to_string());
        if Path::new(&file_path).exists() {
            fs::remove_file(file_path)?;
        }

        self.prev_certified_blob_id = blob_id;
        Ok(())
    }

    fn update_epoch(
        &mut self,
        indexed_stream_element: &IndexedStreamElement,
        batch: &mut DBBatch,
    ) -> Result<()> {
        let EventStreamElement::ContractEvent(ContractEvent::EpochChangeEvent(
            EpochChangeEvent::EpochChangeStart(new_epoch),
        )) = &indexed_stream_element.element
        else {
            return Ok(());
        };

        ensure!(
            new_epoch.epoch == self.current_epoch + 1,
            "Updated epoch is not greater than the current epoch"
        );
        self.current_epoch = new_epoch.epoch;
        self.move_attested_blob_to_pending(batch)?;
        Ok(())
    }

    fn move_attested_blob_to_pending(&mut self, batch: &mut DBBatch) -> Result<()> {
        let Some((_, metadata)) = self.attested.unbounded_iter().seek_to_first().next() else {
            return Ok(());
        };

        batch.delete_batch(&self.attested, std::iter::once(()))?;
        batch.insert_batch(
            &self.pending,
            std::iter::once((metadata.event_cursor.element_index, metadata.to_pending())),
        )?;

        Ok(())
    }

    fn match_event_blob_is_certified(&self, blob_event: &BlobEvent) -> Option<BlobId> {
        match blob_event {
            BlobEvent::Certified(blob_certified) => Some(blob_certified.blob_id),
            _ => None,
        }
    }

    fn update_sequence_range(&mut self, element: &IndexedStreamElement) {
        if self.start.is_none() {
            self.start = Some(element.global_sequence_number.checkpoint_sequence_number);
        }
        self.end = Some(element.global_sequence_number.checkpoint_sequence_number);
    }

    fn write_event_to_buffer(&mut self, event: &IndexedStreamElement) -> Result<()> {
        let entry = BlobEntry::encode(event, EntryEncoding::Bcs)?;
        self.buf_offset += entry.write(&mut self.wbuf)?;
        Ok(())
    }

    /// Returns the current event cursor.
    pub fn event_cursor(&self) -> EventStreamCursor {
        self.event_cursor.clone()
    }

    /// Returns the number of checkpoints per blob.
    pub fn num_checkpoints_per_blob(&self) -> u64 {
        if cfg!(test) || cfg!(msim) {
            10
        } else {
            // With checkpoint generation rate of 5 per second, 5 minutes of events will be
            // written to a blob.
            10
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs::File,
        io::Read,
        path::{Path, PathBuf},
        sync::Arc,
        time::Duration,
    };

    use anyhow::Result;
    use chrono::Utc;
    use typed_store::Map;
    use walrus_core::{BlobId, ShardIndex};
    use walrus_sui::{
        client::FixedSystemParameters,
        test_utils::EventForTesting,
        types::{BlobCertified, ContractEvent},
    };

    use crate::{
        events::{
            event_blob::EventBlob,
            event_blob_writer::{EventBlobWriter, EventBlobWriterFactory, EventBlobWriterMetrics},
            EventSequenceNumber,
            IndexedStreamElement,
        },
        test_utils::{StorageNodeHandle, StubContractService},
    };

    #[tokio::test]
    async fn test_blob_writer_all_events_exist() -> Result<()> {
        const NUM_BLOBS: u64 = 10;
        const NUM_EVENTS_PER_CHECKPOINT: u64 = 2;

        let dir: PathBuf = tempfile::tempdir()?.into_path();
        let node = create_test_node().await?;
        let contract_service = Arc::new(StubContractService {
            system_parameters: FixedSystemParameters {
                epoch_duration: Duration::from_secs(600),
                epoch_zero_end: Utc::now() + Duration::from_secs(60),
            },
        });
        let blob_writer_factory = EventBlobWriterFactory::new(
            &dir,
            node.storage_node.inner().clone(),
            contract_service,
            &prometheus::default_registry(),
        )?;
        let mut blob_writer = blob_writer_factory.create(None).await?;
        let num_checkpoints: u64 = NUM_BLOBS * blob_writer.num_checkpoints_per_blob();

        generate_and_write_events(&mut blob_writer, num_checkpoints, NUM_EVENTS_PER_CHECKPOINT)
            .await?;

        let attested_blob = blob_writer
            .attested
            .get(&())?
            .expect("Attested blob should exist");

        let pending_blobs = blob_writer.pending.unbounded_iter().collect::<Vec<_>>();
        assert_eq!(pending_blobs.len() as u64, NUM_BLOBS - 1);

        let mut prev_blob_id = BlobId([0; 32]);
        while !blob_writer.attested.is_empty() {
            let attested_blob = blob_writer
                .attested
                .get(&())?
                .expect("Attested blob should exist");
            let attested_blob_id = attested_blob.blob_id.clone();
            let f = File::open(
                &dir.join("blobs")
                    .join(attested_blob.event_cursor.element_index.to_string()),
            )?;
            let mut buf = Vec::new();
            let mut buf_reader = std::io::BufReader::new(f);
            buf_reader.read_to_end(&mut buf)?;
            let blob = EventBlob::new(&buf)?;
            assert_eq!(blob.prev_blob_id(), prev_blob_id);
            prev_blob_id = attested_blob_id;
            for entry in blob {
                println!("{:?}", entry);
            }
            certify_attested_blob(&mut blob_writer, attested_blob_id, num_checkpoints).await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_blob_writer_e2e() -> Result<()> {
        const NUM_BLOBS: u64 = 10;
        const NUM_EVENTS_PER_CHECKPOINT: u64 = 1;

        let dir: PathBuf = tempfile::tempdir()?.into_path();
        let node = create_test_node().await?;
        let contract_service = Arc::new(StubContractService {
            system_parameters: FixedSystemParameters {
                epoch_duration: Duration::from_secs(600),
                epoch_zero_end: Utc::now() + Duration::from_secs(60),
            },
        });
        let blob_writer_factory = EventBlobWriterFactory::new(
            &dir,
            node.storage_node.inner().clone(),
            contract_service,
            &prometheus::default_registry(),
        )?;
        let mut blob_writer = blob_writer_factory.create(None).await?;
        let num_checkpoints: u64 = NUM_BLOBS * blob_writer.num_checkpoints_per_blob();

        generate_and_write_events(&mut blob_writer, num_checkpoints, NUM_EVENTS_PER_CHECKPOINT)
            .await?;

        let attested_blob = blob_writer
            .attested
            .get(&())?
            .expect("Attested blob should exist");
        let attested_blob_id = attested_blob.blob_id;

        let pending_blobs: Vec<_> = blob_writer.pending.unbounded_iter().collect();
        assert_eq!(pending_blobs.len() as u64, NUM_BLOBS - 1);
        let first_pending_blob_event_index = pending_blobs[0].0;

        certify_attested_blob(&mut blob_writer, attested_blob_id, num_checkpoints).await?;

        verify_certified_blob(&blob_writer, attested_blob_id)?;
        verify_next_attested_blob(
            &blob_writer,
            &dir,
            first_pending_blob_event_index,
            attested_blob_id,
        )?;

        assert_eq!(
            blob_writer.pending.unbounded_iter().count() as u64,
            NUM_BLOBS - 2
        );

        Ok(())
    }

    async fn create_test_node() -> Result<StorageNodeHandle> {
        StorageNodeHandle::builder()
            .with_system_event_provider(vec![])
            .with_shard_assignment(&[ShardIndex(0)])
            .with_node_started(true)
            .build()
            .await
    }

    async fn generate_and_write_events(
        blob_writer: &mut EventBlobWriter,
        num_checkpoints: u64,
        num_events_per_checkpoint: u64,
    ) -> Result<()> {
        for i in 0..num_checkpoints {
            for j in 0..num_events_per_checkpoint {
                let event = if j == num_events_per_checkpoint - 1 {
                    IndexedStreamElement::new_checkpoint_boundary(i, num_events_per_checkpoint - 1)
                } else {
                    IndexedStreamElement::new(
                        ContractEvent::BlobEvent(
                            BlobCertified::for_testing(BlobId([7; 32])).into(),
                        ),
                        EventSequenceNumber::new(i, j),
                    )
                };
                blob_writer
                    .write(event, i * num_events_per_checkpoint + j)
                    .await?;
            }
        }
        Ok(())
    }

    async fn certify_attested_blob(
        blob_writer: &mut EventBlobWriter,
        attested_blob_id: BlobId,
        num_checkpoints: u64,
    ) -> Result<()> {
        let certified_event = IndexedStreamElement::new(
            ContractEvent::BlobEvent(BlobCertified::for_testing(attested_blob_id).into()),
            EventSequenceNumber::new(num_checkpoints, 0),
        );
        blob_writer
            .write(certified_event, blob_writer.event_cursor.element_index)
            .await
    }

    fn verify_certified_blob(
        blob_writer: &EventBlobWriter,
        expected_blob_id: BlobId,
    ) -> Result<()> {
        let certified_blob = blob_writer
            .certified
            .get(&())?
            .expect("Certified blob should exist");
        assert_eq!(certified_blob.blob_id, expected_blob_id);
        Ok(())
    }

    fn verify_next_attested_blob(
        blob_writer: &EventBlobWriter,
        dir: &Path,
        expected_event_index: u64,
        expected_prev_blob_id: BlobId,
    ) -> Result<()> {
        let next_attested_blob = blob_writer
            .attested
            .get(&())?
            .expect("Next attested blob should exist");
        assert_eq!(
            next_attested_blob.event_cursor.element_index,
            expected_event_index
        );

        let path = dir
            .join("blobs")
            .join(next_attested_blob.event_cursor.element_index.to_string());
        let f = File::open(&path)?;
        let mut buf_reader = std::io::BufReader::new(f);
        let mut buf = Vec::new();
        buf_reader.read_to_end(&mut buf)?;
        let blob = EventBlob::new(&buf)?;
        assert_eq!(blob.epoch(), 0);
        assert_eq!(blob.prev_blob_id(), expected_prev_blob_id);

        Ok(())
    }
}

// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
#![allow(dead_code)]

//! Event blob writer.

use std::{
    fs,
    fs::{File, OpenOptions},
    io,
    io::{BufWriter, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result};
use byteorder::{BigEndian, WriteBytesExt};
use futures_util::future::try_join_all;
use prometheus::{register_int_gauge_with_registry, IntGauge, Registry};
use rocksdb::Options;
use serde::{Deserialize, Serialize};
use sui_types::{event::EventID, messages_checkpoint::CheckpointSequenceNumber};
use typed_store::{
    rocks,
    rocks::{errors::typed_store_err_from_rocks_err, DBBatch, DBMap, MetricConf, ReadWriteOptions},
    Map,
};
use walrus_core::{ensure, metadata::VerifiedBlobMetadataWithId, BlobId, Epoch, Sliver};
use walrus_sui::types::{BlobEvent, ContractEvent, EpochChangeEvent};

use crate::node::{
    errors::StoreSliverError,
    events::{
        event_blob::{BlobEntry, EntryEncoding, EventBlob, SerializedEventID},
        EventStreamCursor,
        EventStreamElement,
        IndexedStreamEvent,
        InitState,
        PositionedStreamEvent,
    },
    StorageNodeInner,
};

const CERTIFIED: &str = "certified_blob_store";
const ATTESTED: &str = "attested_blob_store";
const PENDING: &str = "pending_blob_store";
const MAX_BLOB_SIZE: usize = 100 * 1024 * 1024;
const NUM_CHECKPOINTS_PER_BLOB: u32 = 18_000;

/// Metadata for event blobs.
#[derive(Eq, PartialEq, Debug, Clone, Deserialize, Serialize)]
pub struct EventBlobMetadata<T, U> {
    /// The starting Sui checkpoint sequence number of the events in the blob (inclusive).
    pub start: T,
    /// The ending Sui checkpoint sequence number of the events in the blob (inclusive).
    pub end: T,
    /// The event cursor of the last event in the blob.
    pub event_cursor: EventStreamCursor,
    /// The Walrus epoch of the events in the blob.
    pub epoch: Epoch,
    /// The blob ID of the blob.
    pub blob_id: U,
}

/// Metadata for a blob that is waiting for attestation.
type PendingEventBlobMetadata = EventBlobMetadata<CheckpointSequenceNumber, ()>;

/// Metadata for a blob that is last attested.
type AttestedEventBlobMetadata = EventBlobMetadata<CheckpointSequenceNumber, BlobId>;

/// Metadata for a blob that is last certified.
type CertifiedEventBlobMetadata = EventBlobMetadata<(), BlobId>;

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
            blob_id: (),
        }
    }

    fn to_attested(&self, blob_metadata: VerifiedBlobMetadataWithId) -> AttestedEventBlobMetadata {
        AttestedEventBlobMetadata {
            start: self.start,
            end: self.end,
            event_cursor: self.event_cursor,
            epoch: self.epoch,
            blob_id: *blob_metadata.blob_id(),
        }
    }
}

impl AttestedEventBlobMetadata {
    fn to_certified(&self) -> CertifiedEventBlobMetadata {
        CertifiedEventBlobMetadata {
            blob_id: self.blob_id,
            event_cursor: self.event_cursor,
            epoch: self.epoch,
            start: (),
            end: (),
        }
    }

    fn to_pending(&self) -> PendingEventBlobMetadata {
        PendingEventBlobMetadata {
            start: self.start,
            end: self.end,
            event_cursor: self.event_cursor,
            epoch: self.epoch,
            blob_id: (),
        }
    }
}

impl CertifiedEventBlobMetadata {
    fn new(blob_id: BlobId, event_cursor: EventStreamCursor, epoch: Epoch) -> Self {
        Self {
            blob_id,
            event_cursor,
            epoch,
            start: (),
            end: (),
        }
    }
}

/// Metrics for the event processor.
#[derive(Clone, Debug)]
pub struct EventBlobWriterMetrics {
    /// The latest event certified in an event blob.
    pub latest_certified_event_index: IntGauge,
    /// The latest event processed in an event blob.
    pub latest_processed_event_index: IntGauge,
    /// The latest event in process in an event blob.
    pub latest_in_progress_event_index: IntGauge,
}

impl EventBlobWriterMetrics {
    /// Creates new metrics for event blob writer.
    pub fn new(registry: &Registry) -> Self {
        Self {
            latest_certified_event_index: register_int_gauge_with_registry!(
                "event_blob_writer_latest_certified_event_index",
                "Latest certified event blob writer index",
                registry,
            )
            .expect("this is a valid metrics registration"),
            latest_processed_event_index: register_int_gauge_with_registry!(
                "event_blob_writer_latest_processed_event_index",
                "Latest processed event blob writer index",
                registry,
            )
            .expect("this is a valid metrics registration"),
            latest_in_progress_event_index: register_int_gauge_with_registry!(
                "event_blob_writer_latest_in_progress_event_index",
                "Latest in progress event blob writer index",
                registry,
            )
            .expect("this is a valid metrics registration"),
        }
    }
}

/// Factory for creating and managing Event Blob Writers.
///
/// The `EventBlobWriterFactory` is responsible for initializing and maintaining the infrastructure
/// necessary for writing event blobs. It manages the directory structure, database connections,
/// and provides methods to create new `EventBlobWriter` instances.
#[derive(Clone, Debug)]
pub struct EventBlobWriterFactory {
    /// Root directory path where the event blobs are stored.
    root_dir_path: PathBuf,
    /// Client to store the slivers and metadata of the event blob.
    node: Arc<StorageNodeInner>,
    /// Metrics for the event blob writer.
    metrics: Arc<EventBlobWriterMetrics>,
    /// Current event cursor.
    event_cursor: Option<EventStreamCursor>,
    /// Current epoch.
    epoch: Option<Epoch>,
    /// Blob ID of the last certified blob.
    prev_certified_blob_id: Option<BlobId>,
    /// Certified blobs metadata.
    certified: DBMap<(), CertifiedEventBlobMetadata>,
    /// Attested blobs metadata.
    attested: DBMap<(), AttestedEventBlobMetadata>,
    /// Pending blobs metadata.
    pending: DBMap<u64, PendingEventBlobMetadata>,
    /// Number of checkpoints per blob.
    num_checkpoints_per_blob: Option<u32>,
}

impl EventBlobWriterFactory {
    /// Returns the path to the database directory.
    pub fn db_path(root_dir_path: &Path) -> PathBuf {
        root_dir_path.join("event_blob_writer").join("db")
    }

    /// Returns the path to the blobs directory.
    pub fn blobs_path(root_dir_path: &Path) -> PathBuf {
        root_dir_path.join("event_blob_writer").join("blobs")
    }

    /// Create a new event blob writer factory.
    pub fn new(
        root_dir_path: &Path,
        node: Arc<StorageNodeInner>,
        registry: &Registry,
        num_checkpoints_per_blob: Option<u32>,
    ) -> Result<EventBlobWriterFactory> {
        let db_path = Self::db_path(root_dir_path);
        fs::create_dir_all(db_path.as_path())?;

        let mut db_opts = Options::default();
        let metric_conf = MetricConf::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);
        let database = rocks::open_cf_opts(
            db_path,
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
        let attested: DBMap<(), AttestedEventBlobMetadata> = DBMap::reopen(
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
        let epoch = pending
            .unbounded_iter()
            .last()
            .map(|(_, metadata)| metadata.epoch)
            .or_else(|| {
                attested
                    .get(&())
                    .ok()
                    .flatten()
                    .map(|metadata| metadata.epoch)
            })
            .or_else(|| {
                certified
                    .get(&())
                    .ok()
                    .flatten()
                    .map(|metadata| metadata.epoch)
            });
        let prev_certified_blob_id = certified
            .get(&())
            .ok()
            .flatten()
            .map(|metadata| metadata.blob_id);
        Ok(Self {
            root_dir_path: root_dir_path.to_path_buf(),
            node,
            metrics: Arc::new(EventBlobWriterMetrics::new(registry)),
            event_cursor,
            epoch,
            prev_certified_blob_id,
            certified,
            attested,
            pending,
            num_checkpoints_per_blob,
        })
    }

    /// Returns a clone of the current event cursor position.
    ///
    /// The event cursor tracks the position of the last processed event in the stream,
    /// including its event ID and element index. This cursor is used to maintain continuity
    /// when writing events across multiple blobs.
    ///
    /// # Returns
    /// * `Option<EventStreamCursor>` - The current cursor position if one exists, or None if
    ///   no events have been processed yet.
    pub fn event_cursor(&self) -> Option<EventStreamCursor> {
        self.event_cursor
    }

    /// Create a new event blob writer.
    pub async fn create(&self) -> Result<EventBlobWriter> {
        let databases = EventBlobDatabases {
            certified: self.certified.clone(),
            attested: self.attested.clone(),
            pending: self.pending.clone(),
        };
        let event_cursor = self.event_cursor.unwrap_or(EventStreamCursor::new(None, 0));
        let epoch = self.epoch.unwrap_or(0);
        let prev_certified_blob_id = self.prev_certified_blob_id.unwrap_or(BlobId::ZERO);
        let blobs_path = Self::blobs_path(&self.root_dir_path);
        fs::create_dir_all(blobs_path.as_path())?;
        let config = EventBlobWriterConfig {
            blob_dir_path: blobs_path.into(),
            metrics: self.metrics.clone(),
            event_stream_cursor: event_cursor,
            current_epoch: epoch,
            prev_certified_blob_id,
        };
        EventBlobWriter::new(
            config,
            self.node.clone(),
            databases,
            self.num_checkpoints_per_blob
                .unwrap_or(NUM_CHECKPOINTS_PER_BLOB),
        )
        .await
    }
}

/// EventBlobWriter manages the creation, storage, and certification of event blobs.
/// ```
///                   +-------------------+
///                   |  EventBlobWriter  |
///                   +-------------------+
///                            |
///                            v
///   +------------+    +-------------+    +-------------+    +--------------+
///   |Current Blob|--->| Pending Blob|--->|Attested Blob|--->|Certified Blob|
///   +------------+    +-------------+    +-------------+    +--------------+
///         |                 ^                 |                   ^
///         |                 |                 |                   |
///         |                 |                 v                   |
///   +-----------------+    +------------------+             +-----------+
///   |Filesystem (tmp) |--->| Database Storage |<------------| System    |
///   +-----------------+    +------------------+             | Contract  |
///                                                           +-----------+
///
/// Flow: Current -> Pending -> Attested -> Certified
/// ```
///
/// Blob Lifecycle:
///
/// a. Current:
/// The events that are being written to is in this active blob.
/// It's stored as a file named "current_blob" in the filesystem. This blob hasn't been materialized
/// yet and if the system is restarted, the events in this blob will be lost (and will need to be
/// reprocessed).
///
/// b. Pending:
/// When a blob is first materialized (once the size of the current blob has reached a threshold, or
/// we have accumulated events for enough number of checkpoints), it's in a "pending" state.
/// Metadata about pending blobs is stored in the pending database and the file is renamed from
/// "current_blob" to its final name (based on event index of last event in the blob).
///
/// c. Attested:
/// If the node isn't waiting for a previous attested blob to get certified, it is ready to attest
/// the oldest pending blob.
/// Previous blob id pointer is updated in the blob file.
/// Metadata is moved from pending to attested database.
/// We send the blob for attestation to the system contract.
/// The writer waits for a certification event to mark this blob as certified. Until then, we will
/// not attest any more blobs (but keep writing events to the current blob and accumulate more
/// pending blobs).
///
/// d. Certified:
/// Once a blob is certified by the quorum, its metadata moves to the certified database.
/// The system removes the file for certified blobs from the filesystem to save space.
/// The system is now ready to attest the next pending blob.
///
/// Writing Process:
///
/// The system writes events to the active blob using a buffered writer (BufWriter).
/// The writer keeps track of the blob's size and the number of checkpoints it contains.
///
/// Epoch Changes:
///
/// The writer handles epoch changes by moving attested blob back to pending state. This is needed
/// because non-certified blobs are not synced during shard sync across epoch changes.
/// This ensures previously attested but non-certified blobs are stored on newly assigned shards and
/// get re-attested in the new epoch.
///
/// Storage:
///
/// Metadata for blobs in different states (pending, attested, certified) is stored in separate
/// database tables.
/// Actual blob data is initially stored as files in the filesystem.
/// Once certified, blob data is removed from the filesystem, and we only retain the metadata in db.
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
    attested: DBMap<(), AttestedEventBlobMetadata>,
    /// Pending blobs metadata.
    pending: DBMap<u64, PendingEventBlobMetadata>,
    /// Client to store the slivers and metadata of the event blob.
    node: Arc<StorageNodeInner>,
    /// Current epoch.
    current_epoch: Epoch,
    /// Next event index.
    event_cursor: EventStreamCursor,
    /// Blob ID of the last certified blob.
    prev_certified_blob_id: BlobId,
    /// Event ID of the last certified event.
    prev_certified_event_id: Option<EventID>,
    /// Metrics for the event blob writer.
    metrics: Arc<EventBlobWriterMetrics>,
    /// Number of checkpoints per blob.
    num_checkpoints_per_blob: u32,
}

/// Struct to group database-related parameters.
#[derive(Debug)]
pub struct EventBlobDatabases {
    certified: DBMap<(), CertifiedEventBlobMetadata>,
    attested: DBMap<(), AttestedEventBlobMetadata>,
    pending: DBMap<u64, PendingEventBlobMetadata>,
}

/// Struct to group configuration-related parameters
#[derive(Debug)]
pub struct EventBlobWriterConfig {
    /// Root directory path where the event blobs are stored.
    blob_dir_path: Arc<Path>,
    /// Event stream cursor to initialize the writer.
    event_stream_cursor: EventStreamCursor,
    /// Current epoch.
    current_epoch: Epoch,
    /// Blob ID of the last certified blob.
    prev_certified_blob_id: BlobId,
    /// Metrics for the event blob writer.
    metrics: Arc<EventBlobWriterMetrics>,
}

impl EventBlobWriter {
    /// Creates a new EventBlobWriter instance.
    ///
    /// This method initializes a new EventBlobWriter with the provided configuration,
    /// node, system contract service, and databases. It sets up the initial state,
    /// including the event cursor, current epoch, and previous certified blob ID.
    pub async fn new(
        config: EventBlobWriterConfig,
        node: Arc<StorageNodeInner>,
        databases: EventBlobDatabases,
        num_checkpoints_per_blob: u32,
    ) -> Result<Self> {
        let file = Self::next_file(&config.blob_dir_path, config.current_epoch)?;
        let mut blob_writer = Self {
            blob_dir_path: config.blob_dir_path.to_path_buf(),
            start: None,
            end: None,
            wbuf: BufWriter::new(file),
            buf_offset: EventBlob::HEADER_SIZE,
            attested: databases.attested,
            certified: databases.certified,
            pending: databases.pending,
            node,
            current_epoch: config.current_epoch,
            event_cursor: config.event_stream_cursor,
            prev_certified_blob_id: config.prev_certified_blob_id,
            prev_certified_event_id: config.event_stream_cursor.event_id,
            metrics: config.metrics,
            num_checkpoints_per_blob,
        };
        // Upon receiving a blob_certified event for an event blob, we may have crashed after making
        // the db changes but before attesting the next pending blob (since those two events are
        // not atomic).This invocation is to account for that particular scenario.
        blob_writer.attest_next_blob().await?;
        Ok(blob_writer)
    }

    /// Update the event blob writer with the initial state.
    pub async fn update(&mut self, init_state: InitState) -> Result<()> {
        self.event_cursor = init_state.event_cursor;
        self.current_epoch = init_state.epoch;
        self.prev_certified_blob_id = init_state.prev_blob_id;
        self.prev_certified_event_id = init_state.event_cursor.event_id;
        let certified_metadata = CertifiedEventBlobMetadata::new(
            self.prev_certified_blob_id,
            self.event_cursor,
            self.current_epoch,
        );
        self.certified.insert(&(), &certified_metadata)?;
        Ok(())
    }

    /// Returns the path to the blob directory.
    fn blob_dir(&self) -> PathBuf {
        self.blob_dir_path.clone()
    }

    /// Creates and initializes the next blob file.
    ///
    /// This method creates a new file for the next blob, initializes it with the
    /// necessary header information, and prepares it for writing events.
    fn next_file(dir_path: &Path, epoch: Epoch) -> Result<File> {
        let next_file_path = dir_path.join("current_blob");
        let mut file: File = Self::create_and_initialize_file(&next_file_path, epoch)?;
        drop(file);
        file = Self::reopen_file_with_permissions(&next_file_path)?;
        file.seek(SeekFrom::Start(EventBlob::HEADER_SIZE as u64))?;
        Ok(file)
    }

    /// Creates and initializes a new blob file.
    ///
    /// This method creates a new file at the specified path and writes the initial
    /// header information, including:
    /// - Magic bytes for file format identification
    /// - Format version for compatibility checks
    /// - Current epoch number
    /// - Previous blob ID (initialized to zero since it will be set later when blob is attested)
    /// - Previous event ID (initialized to zero since it will be set later when blob is attested)
    ///
    /// The blob ID and event ID are initially set to zero as placeholder values. These will be
    /// updated with the actual previous blob/event IDs when the blob is attested, creating the
    /// chain of blob references.
    fn create_and_initialize_file(path: &Path, epoch: Epoch) -> Result<File> {
        let mut file = File::create(path)?;
        file.write_u32::<BigEndian>(EventBlob::MAGIC)?;
        file.write_u32::<BigEndian>(EventBlob::FORMAT_VERSION)?;
        file.write_u32::<BigEndian>(epoch)?;
        let zero_blob_id = BlobId::ZERO;
        file.write_all(&zero_blob_id.0)?;
        let zero_event_id = SerializedEventID::ZERO;
        file.write_all(&zero_event_id.0)?;
        Ok(file)
    }

    /// Reopens a file with the necessary permissions.
    ///
    /// This method reopens an existing file with read and write permissions,
    /// which may be necessary after initial creation and writing.
    fn reopen_file_with_permissions(path: &Path) -> io::Result<File> {
        OpenOptions::new().read(true).write(true).open(path)
    }

    /// Renames the current blob file to its final name.
    ///
    /// This method renames the 'current_blob' file to a name based on the
    /// current event cursor's element index.
    fn rename_blob_file(&self) -> Result<()> {
        let from = self.current_blob_path();
        let to = self
            .blob_dir()
            .join(self.event_cursor.element_index.to_string());
        fs::rename(from, to)?;
        Ok(())
    }

    /// Finalizes the current blob by writing trailer information.
    ///
    /// This method writes the start and end checkpoint sequence numbers to the
    /// end of the blob file and ensures all data is flushed and synced to disk.
    fn finalize(&mut self) -> Result<()> {
        self.wbuf
            .write_u64::<BigEndian>(self.start.unwrap_or_default())?;
        self.wbuf
            .write_u64::<BigEndian>(self.end.unwrap_or_default())?;
        self.wbuf.flush()?;
        self.wbuf.get_ref().sync_all()?;
        let off = self.wbuf.get_ref().stream_position()?;
        self.wbuf.get_ref().set_len(off)?;
        Ok(())
    }

    /// Resets the blob writer state for a new blob.
    ///
    /// This method clears the start and end sequence numbers, creates a new file
    /// for the next blob, and resets the buffer offset.
    fn reset(&mut self) -> Result<()> {
        self.start = None;
        self.end = None;
        let f = Self::next_file(&self.blob_dir(), self.current_epoch)?;
        self.buf_offset = EventBlob::HEADER_SIZE;
        self.wbuf = BufWriter::new(f);
        Ok(())
    }

    /// Updates the previous blob ID and previous event ID in a blob file.
    ///
    /// This method writes the provided blob ID and event ID to the header of the specified blob
    /// file.
    fn update_blob_header(
        &self,
        event_index: u64,
        blob_id: &BlobId,
        event_id: &Option<EventID>,
    ) -> Result<()> {
        let file_path = self.blob_dir().join(event_index.to_string());
        let mut file = Self::reopen_file_with_permissions(&file_path)?;
        file.seek(SeekFrom::Start(EventBlob::BLOB_ID_OFFSET as u64))?;
        file.write_all(&blob_id.0)?;
        file.seek(SeekFrom::Start(EventBlob::EVENT_ID_OFFSET as u64))?;
        file.write_all(
            &event_id
                .map(SerializedEventID::encode)
                .unwrap_or(SerializedEventID::ZERO)
                .0,
        )?;
        file.flush()?;
        file.sync_all()?;
        Ok(())
    }

    /// Stores the slivers of the current blob.
    ///
    /// This method reads the blob file, encodes it into slivers, and stores these
    /// slivers along with the blob metadata in the storage node.
    async fn store_slivers(
        &mut self,
        metadata: &PendingEventBlobMetadata,
    ) -> Result<VerifiedBlobMetadataWithId> {
        let content = std::fs::read(
            self.blob_dir()
                .join(metadata.event_cursor.element_index.to_string()),
        )?;
        let (sliver_pairs, blob_metadata) = self
            .node
            .encoding_config()
            .get_blob_encoder(&content)?
            .encode_with_metadata();
        self.node
            .storage()
            .put_verified_metadata_without_blob_info(&blob_metadata)
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

    /// Cuts the current blob and prepares for a new one.
    ///
    /// This method finalizes the current blob file and renames it to its final name.
    async fn cut(&mut self) -> Result<()> {
        self.finalize()?;
        self.rename_blob_file()?;
        Ok(())
    }

    /// Attests a blob by calling the system contract.
    ///
    /// This method sends a request to the system contract to certify the event blob.
    async fn attest_blob(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
        checkpoint_sequence_number: CheckpointSequenceNumber,
    ) -> Result<()> {
        tracing::debug!(
            "attesting event blob: {} in epoch: {}",
            metadata.blob_id(),
            self.current_epoch
        );
        match self
            .node
            .contract_service
            .certify_event_blob(
                metadata.try_into()?,
                checkpoint_sequence_number,
                self.current_epoch,
                self.node.node_capability(),
            )
            .await
        {
            Ok(_) => {
                tracing::info!("attested event blob with id: {}", metadata.blob_id());
                Ok(())
            }
            Err(e) => {
                tracing::error!(
                    error = ?e,
                    blob_id = ?metadata.blob_id(),
                    "failed to attest event blob"
                );
                Ok(())
            }
        }
    }

    /// Attests the next pending blob.
    ///
    /// This method processes the next pending blob by storing its slivers,
    /// attesting it, and updating the database state.
    async fn attest_next_blob(&mut self) -> Result<()> {
        if !self.attested.is_empty() {
            return Ok(());
        }

        let Some((event_index, metadata)) = self.pending.unbounded_iter().seek_to_first().next()
        else {
            return Ok(());
        };

        self.update_blob_header(
            metadata.event_cursor.element_index,
            &self.prev_certified_blob_id,
            &self.prev_certified_event_id,
        )?;

        let blob_metadata = self.store_slivers(&metadata).await?;
        let attested_metadata = metadata.to_attested(blob_metadata.clone());

        self.attest_blob(&blob_metadata, metadata.end).await?;
        let mut batch = self.pending.batch();
        batch.insert_batch(&self.attested, std::iter::once(((), attested_metadata)))?;
        batch.delete_batch(&self.pending, std::iter::once(event_index))?;
        batch.write()?;

        Ok(())
    }

    /// Writes an event to the current blob.
    ///
    /// This method writes the provided event to the current blob, updates necessary
    /// metadata, and handles blob cutting and certification if needed.
    pub async fn write(
        &mut self,
        element: PositionedStreamEvent,
        element_index: u64,
    ) -> Result<()> {
        ensure!(
            element_index == self.event_cursor.element_index,
            "Invalid event index"
        );
        self.metrics
            .latest_in_progress_event_index
            .set(element_index as i64);
        self.event_cursor = EventStreamCursor::new(element.element.event_id(), element_index + 1);
        self.update_sequence_range(&element);
        self.write_event_to_buffer(element.clone(), element_index)?;

        let mut batch = self.pending.batch();
        self.update_epoch(&element, &mut batch)?;

        if self.should_cut_new_blob(&element) {
            self.cut_and_reset_blob(&mut batch, element_index).await?;
        }

        self.handle_event_blob_certification(&element, element_index, &mut batch)?;

        batch.write()?;

        self.attest_next_blob().await?;
        self.metrics
            .latest_processed_event_index
            .set(element_index as i64);

        Ok(())
    }

    fn current_blob_path(&self) -> PathBuf {
        self.blob_dir().join("current_blob")
    }

    /// Returns the current size of the blob being written.
    fn current_blob_size(&self) -> usize {
        self.buf_offset
    }

    /// Determines if a new blob should be cut based on the current event.
    ///
    /// This method checks if the current event is an end-of-checkpoint marker and
    /// if either the checkpoint count or blob size threshold has been reached.
    fn should_cut_new_blob(&self, element: &PositionedStreamEvent) -> bool {
        element.is_end_of_checkpoint_marker()
            && (self.current_blob_size() > MAX_BLOB_SIZE
                || (element.checkpoint_event_position.checkpoint_sequence_number + 1
                    >= self.start.unwrap_or(0) + self.num_checkpoints_per_blob() as u64))
    }

    /// Cuts the current blob and resets for a new one.
    ///
    /// This method finalizes the current blob, stores its metadata, and prepares
    /// for writing a new blob.
    async fn cut_and_reset_blob(&mut self, batch: &mut DBBatch, element_index: u64) -> Result<()> {
        self.cut().await?;
        let blob_metadata = PendingEventBlobMetadata::new(
            self.start.unwrap(),
            self.end.unwrap(),
            self.event_cursor,
            self.current_epoch,
        );
        batch.insert_batch(
            &self.pending,
            std::iter::once((element_index, blob_metadata)),
        )?;
        self.reset()
    }

    /// Handles the certification of event blobs.
    ///
    /// This method processes blob certification events, updates the database state,
    /// and manages file cleanup for certified blobs.
    fn handle_event_blob_certification(
        &mut self,
        element: &PositionedStreamEvent,
        element_index: u64,
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
        self.node
            .storage()
            .update_blob_info_with_metadata(&blob_id)
            .context("unable to update metadata")?;

        self.metrics
            .latest_certified_event_index
            .set(element_index as i64);
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
        self.prev_certified_event_id = metadata.event_cursor.event_id;
        Ok(())
    }

    /// Updates the current epoch based on epoch change events.
    ///
    /// This method checks for epoch change events and updates the current epoch
    /// and blob states accordingly.
    fn update_epoch(
        &mut self,
        indexed_stream_element: &PositionedStreamEvent,
        batch: &mut DBBatch,
    ) -> Result<()> {
        let EventStreamElement::ContractEvent(ContractEvent::EpochChangeEvent(
            EpochChangeEvent::EpochChangeStart(new_epoch),
        )) = &indexed_stream_element.element
        else {
            return Ok(());
        };
        self.current_epoch = new_epoch.epoch;
        self.move_attested_blob_to_pending(batch)?;
        Ok(())
    }

    /// Moves an attested blob to the pending state during epoch changes.
    ///
    /// This method updates the database state to move attested blobs back to
    /// pending status when an epoch change occurs.
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

    /// Checks if a blob event indicates a certified blob.
    ///
    /// This method examines a BlobEvent to determine if it represents a
    /// certified blob, returning the blob ID if so.
    fn match_event_blob_is_certified(&self, blob_event: &BlobEvent) -> Option<BlobId> {
        match blob_event {
            BlobEvent::Certified(blob_certified) => Some(blob_certified.blob_id),
            _ => None,
        }
    }

    /// Updates the sequence range for the current blob.
    ///
    /// This method updates the start and end checkpoint sequence numbers
    /// based on the provided event.
    fn update_sequence_range(&mut self, element: &PositionedStreamEvent) {
        if self.start.is_none() {
            self.start = Some(element.checkpoint_event_position.checkpoint_sequence_number);
        }
        self.end = Some(element.checkpoint_event_position.checkpoint_sequence_number);
    }

    /// Writes an event to the internal buffer.
    ///
    /// This method encodes the provided event and writes it to the internal
    /// buffer, updating the buffer offset.
    fn write_event_to_buffer(
        &mut self,
        event: PositionedStreamEvent,
        element_index: u64,
    ) -> Result<()> {
        let event_with_cursor = IndexedStreamEvent::new(event, element_index);
        let entry = BlobEntry::encode(&event_with_cursor, EntryEncoding::Bcs)?;
        self.buf_offset += entry.write(&mut self.wbuf)?;
        Ok(())
    }

    /// Returns the current event cursor.
    pub fn event_cursor(&self) -> EventStreamCursor {
        self.event_cursor
    }

    /// Returns the number of checkpoints per blob.
    pub fn num_checkpoints_per_blob(&self) -> u32 {
        self.num_checkpoints_per_blob
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs::File,
        io::Read,
        path::{Path, PathBuf},
    };

    use anyhow::Result;
    use prometheus::Registry;
    use typed_store::Map;
    use walrus_core::{BlobId, ShardIndex};
    use walrus_sui::{
        test_utils::EventForTesting,
        types::{BlobCertified, ContractEvent},
    };

    use crate::{
        node::events::{
            event_blob::EventBlob,
            event_blob_writer::{EventBlobWriter, EventBlobWriterFactory},
            CheckpointEventPosition,
            PositionedStreamEvent,
        },
        test_utils::StorageNodeHandle,
    };

    #[tokio::test]
    async fn test_blob_writer_all_events_exist() -> Result<()> {
        const NUM_BLOBS: u64 = 10;
        const NUM_EVENTS_PER_CHECKPOINT: u64 = 2;

        let dir: PathBuf = tempfile::tempdir()?.into_path();
        let registry = Registry::new();
        let node = create_test_node().await?;
        let blob_writer_factory = EventBlobWriterFactory::new(
            &dir,
            node.storage_node.inner().clone(),
            &registry,
            Some(10),
        )?;
        let mut blob_writer = blob_writer_factory.create().await?;
        let num_checkpoints: u64 = NUM_BLOBS * blob_writer.num_checkpoints_per_blob() as u64;

        generate_and_write_events(&mut blob_writer, num_checkpoints, NUM_EVENTS_PER_CHECKPOINT)
            .await?;

        let pending_blobs = blob_writer.pending.unbounded_iter().collect::<Vec<_>>();
        assert_eq!(pending_blobs.len() as u64, NUM_BLOBS - 1);

        let mut prev_blob_id = BlobId([0; 32]);
        while !blob_writer.attested.is_empty() {
            let attested_blob = blob_writer
                .attested
                .get(&())?
                .expect("Attested blob should exist");
            let attested_blob_id = attested_blob.blob_id;
            let f = File::open(
                dir.join("event_blob_writer")
                    .join("blobs")
                    .join(attested_blob.event_cursor.element_index.to_string()),
            )?;
            let mut buf = Vec::new();
            let mut buf_reader = std::io::BufReader::new(f);
            buf_reader.read_to_end(&mut buf)?;
            let blob = EventBlob::new(&buf)?;
            assert_eq!(blob.prev_blob_id(), prev_blob_id);
            prev_blob_id = attested_blob_id;
            for entry in blob {
                tracing::info!("{:?}", entry);
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
        let registry = Registry::new();
        let blob_writer_factory = EventBlobWriterFactory::new(
            &dir,
            node.storage_node.inner().clone(),
            &registry,
            Some(10),
        )?;
        let mut blob_writer = blob_writer_factory.create().await?;
        let num_checkpoints: u64 = NUM_BLOBS * blob_writer.num_checkpoints_per_blob() as u64;

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
            first_pending_blob_event_index + 1,
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
                    PositionedStreamEvent::new_checkpoint_boundary(i, num_events_per_checkpoint - 1)
                } else {
                    PositionedStreamEvent::new(
                        ContractEvent::BlobEvent(
                            BlobCertified::for_testing(BlobId([7; 32])).into(),
                        ),
                        CheckpointEventPosition::new(i, j),
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
        let certified_event = PositionedStreamEvent::new(
            ContractEvent::BlobEvent(BlobCertified::for_testing(attested_blob_id).into()),
            CheckpointEventPosition::new(num_checkpoints, 0),
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
            .join("event_blob_writer")
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

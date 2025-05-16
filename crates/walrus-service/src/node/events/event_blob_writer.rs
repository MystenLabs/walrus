// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Event blob writer.

use std::{
    ffi::OsStr,
    fs::{self, File, OpenOptions},
    io::{self, BufWriter, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result};
use byteorder::{BigEndian, WriteBytesExt};
use futures_util::future::try_join_all;
use prometheus::{IntGauge, register_int_gauge_with_registry};
use rand::{Rng, thread_rng};
use rocksdb::Options;
use serde::{Deserialize, Serialize};
use sui_types::{event::EventID, messages_checkpoint::CheckpointSequenceNumber};
use typed_store::{
    Map,
    rocks,
    rocks::{DBBatch, DBMap, MetricConf, ReadWriteOptions, errors::typed_store_err_from_rocks_err},
};
use walrus_core::{
    BlobId,
    DEFAULT_ENCODING,
    Epoch,
    Sliver,
    encoding::EncodingConfigTrait as _,
    ensure,
    metadata::VerifiedBlobMetadataWithId,
};
use walrus_sui::{
    client::SuiClientError,
    types::{
        BlobEvent,
        ContractEvent,
        EpochChangeEvent,
        move_errors::{MoveExecutionError, SystemStateInnerError},
        move_structs::EventBlob as SuiEventBlob,
    },
};
use walrus_utils::metrics::Registry;

use crate::node::{
    DatabaseConfig,
    StorageNodeInner,
    errors::StoreSliverError,
    events::{
        EventStreamCursor,
        EventStreamElement,
        IndexedStreamEvent,
        InitState,
        PositionedStreamEvent,
        event_blob::{BlobEntry, EntryEncoding, EventBlob, SerializedEventID},
    },
};

/// The column family name for certified event blobs.
const CERTIFIED: &str = "certified_blob_store";
/// The column family name for attested event blobs.
const ATTESTED: &str = "attested_blob_store";
/// The column family name for pending event blobs.
const PENDING: &str = "pending_blob_store";
/// The column family name for failed to attest event blobs.
const FAILED_TO_ATTEST: &str = "failed_to_attest_blob_store";
/// The file name for the current blob.
const CURRENT_BLOB: &str = "current_blob";
/// The maximum size of a blob file.
const MAX_BLOB_SIZE: usize = 100 * 1024 * 1024;
/// The number of checkpoints per blob.
pub(crate) const NUM_CHECKPOINTS_PER_BLOB: u32 = 216_000;
/// The default number of unattested blobs threshold.
const DEFAULT_NUM_UNATTESTED_BLOBS_THRESHOLD: u32 = 3;

pub(crate) fn certified_cf_name() -> &'static str {
    CERTIFIED
}

pub(crate) fn attested_cf_name() -> &'static str {
    ATTESTED
}

pub(crate) fn pending_cf_name() -> &'static str {
    PENDING
}

pub(crate) fn failed_to_attest_cf_name() -> &'static str {
    FAILED_TO_ATTEST
}

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
pub(crate) type PendingEventBlobMetadata = EventBlobMetadata<CheckpointSequenceNumber, ()>;

/// Metadata for a blob that failed to attest.
pub(crate) type FailedToAttestEventBlobMetadata =
    EventBlobMetadata<CheckpointSequenceNumber, BlobId>;

/// Metadata for a blob that is last attested.
pub(crate) type AttestedEventBlobMetadata = EventBlobMetadata<CheckpointSequenceNumber, BlobId>;

/// Metadata for a blob that is last certified.
pub(crate) type CertifiedEventBlobMetadata = EventBlobMetadata<(), BlobId>;

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

    fn to_failed_to_attest(&self, blob_id: BlobId) -> FailedToAttestEventBlobMetadata {
        FailedToAttestEventBlobMetadata {
            start: self.start,
            end: self.end,
            event_cursor: self.event_cursor,
            epoch: self.epoch,
            blob_id,
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
    /// The latest checkpoint sequence number attested in an event blob.
    pub latest_attested_checkpoint_sequence_number: IntGauge,
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
            latest_attested_checkpoint_sequence_number: register_int_gauge_with_registry!(
                "event_blob_writer_latest_attested_checkpoint_sequence_number",
                "Latest attested checkpoint sequence number",
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
    /// Failed to attest blobs metadata.
    failed_to_attest: DBMap<(), FailedToAttestEventBlobMetadata>,
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

    /// Cleans up orphaned blob files from the filesystem that don't exist in any database.
    fn cleanup_orphaned_blobs(
        blobs_path: &Path,
        pending: &DBMap<u64, PendingEventBlobMetadata>,
        attested: &DBMap<(), AttestedEventBlobMetadata>,
        failed_to_attest: &DBMap<(), FailedToAttestEventBlobMetadata>,
        certified: &DBMap<(), CertifiedEventBlobMetadata>,
    ) -> Result<()> {
        if !blobs_path.exists() {
            return Ok(());
        }
        let blobs = fs::read_dir(blobs_path)?;
        for blob in blobs {
            let blob_path = blob?.path();
            if !blob_path.is_file() {
                continue;
            }
            if blob_path.file_name() == Some(OsStr::new(CURRENT_BLOB)) {
                continue;
            }
            let event_index = blob_path
                .file_name()
                .and_then(|name| name.to_str())
                .and_then(|name| name.parse::<u64>().ok());
            if let Some(event_index) = event_index {
                // Check if blob exists in any database
                let exists_in_db = pending.safe_iter().any(|result| {
                    result
                        .map(|(_, metadata)| metadata.event_cursor.element_index == event_index)
                        .unwrap_or(false)
                }) || attested
                    .get(&())
                    .ok()
                    .flatten()
                    .map(|metadata| metadata.event_cursor.element_index == event_index)
                    .unwrap_or(false)
                    || failed_to_attest
                        .get(&())
                        .ok()
                        .flatten()
                        .map(|metadata| metadata.event_cursor.element_index == event_index)
                        .unwrap_or(false)
                    || certified
                        .get(&())
                        .ok()
                        .flatten()
                        .map(|metadata| metadata.event_cursor.element_index == event_index)
                        .unwrap_or(false);

                if !exists_in_db {
                    if let Err(e) = fs::remove_file(&blob_path) {
                        tracing::warn!(?blob_path, ?e, "Failed to remove orphaned blob from disk");
                    } else {
                        tracing::info!(?blob_path, "Removed orphaned event blob from disk");
                    }
                }
            } else {
                tracing::warn!(?blob_path, "Orphaned event blob with no event index found");
            }
        }
        Ok(())
    }

    /// Create a new event blob writer factory.
    pub fn new(
        root_dir_path: &Path,
        db_config: &DatabaseConfig,
        node: Arc<StorageNodeInner>,
        registry: &Registry,
        num_checkpoints_per_blob: Option<u32>,
        last_certified_event_blob: Option<SuiEventBlob>,
        num_uncertified_blob_threshold: Option<u32>,
    ) -> Result<EventBlobWriterFactory> {
        let db_path = Self::db_path(root_dir_path);
        fs::create_dir_all(db_path.as_path())?;

        let mut db_opts = Options::from(&db_config.global());
        let metric_conf = MetricConf::new("event_blob_writer");
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);
        let database = rocks::open_cf_opts(
            db_path,
            Some(db_opts),
            metric_conf,
            &[
                (PENDING, db_config.pending().to_options()),
                (ATTESTED, db_config.attested().to_options()),
                (CERTIFIED, db_config.certified().to_options()),
                (FAILED_TO_ATTEST, db_config.failed_to_attest().to_options()),
            ],
        )?;
        if database.cf_handle(CERTIFIED).is_none() {
            database
                .create_cf(CERTIFIED, &db_config.certified().to_options())
                .map_err(typed_store_err_from_rocks_err)?;
        }
        if database.cf_handle(ATTESTED).is_none() {
            database
                .create_cf(ATTESTED, &db_config.attested().to_options())
                .map_err(typed_store_err_from_rocks_err)?;
        }
        if database.cf_handle(PENDING).is_none() {
            database
                .create_cf(PENDING, &db_config.pending().to_options())
                .map_err(typed_store_err_from_rocks_err)?;
        }
        if database.cf_handle(FAILED_TO_ATTEST).is_none() {
            database
                .create_cf(FAILED_TO_ATTEST, &db_config.failed_to_attest().to_options())
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
        let failed_to_attest: DBMap<(), FailedToAttestEventBlobMetadata> = DBMap::reopen(
            &database,
            Some(FAILED_TO_ATTEST),
            &ReadWriteOptions::default(),
            false,
        )?;

        let blobs_path = Self::blobs_path(root_dir_path);
        Self::cleanup_orphaned_blobs(
            &blobs_path,
            &pending,
            &attested,
            &failed_to_attest,
            &certified,
        )?;

        Self::reset_uncertified_blobs(
            &pending,
            &attested,
            &failed_to_attest,
            num_uncertified_blob_threshold,
            last_certified_event_blob,
            &blobs_path,
        )?;

        let event_cursor = pending
            .safe_iter()
            .last()
            .map(|result| result.map(|(_, metadata)| metadata.event_cursor))
            .transpose()?
            .or_else(|| {
                failed_to_attest
                    .get(&())
                    .ok()
                    .flatten()
                    .map(|metadata| metadata.event_cursor)
            })
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
            .safe_iter()
            .last()
            .map(|result| result.map(|(_, metadata)| metadata.epoch))
            .transpose()?
            .or_else(|| {
                failed_to_attest
                    .get(&())
                    .ok()
                    .flatten()
                    .map(|metadata| metadata.epoch)
            })
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
            failed_to_attest,
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
            failed_to_attest: self.failed_to_attest.clone(),
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

    /// Resets the state of unattested blobs when the system detects potential network
    /// synchronization issues.
    ///
    /// This function helps recover from network-wide synchronization issues by resetting
    /// uncertified blobs when their count exceeds a threshold. It's specifically designed
    /// to handle cases where:
    /// - The entire network is out of sync for an extended period
    /// - The cluster is unable to achieve quorum for certifying new blobs
    /// - There's a need to reset currently formed blobsand retry blob attestation
    ///
    /// # Note
    /// This is a network-level recovery mechanism and would not be helpful with handling local
    /// node corruption.
    ///
    /// # Process
    /// 1. Gets the last certified blob in system object as an argument
    /// 2. Counts local uncertified blobs (pending, attested, failed) after the last certified
    ///    checkpoint
    /// 3. if there are more total blobs than after the last certified checkpoint, it means that
    ///    the node is still processing blobs before the last certified checkpoint, and hence there
    ///    is no need to reset.
    /// 4. If that count exceeds the threshold, resets all local uncertified blobs
    fn reset_uncertified_blobs(
        pending_db: &DBMap<u64, PendingEventBlobMetadata>,
        attested_db: &DBMap<(), AttestedEventBlobMetadata>,
        failed_to_attest_db: &DBMap<(), FailedToAttestEventBlobMetadata>,
        num_uncertified_blob_threshold: Option<u32>,
        last_certified_event_blob: Option<SuiEventBlob>,
        blobs_path: &Path,
    ) -> Result<()> {
        let Some(last_certified_event_blob) = last_certified_event_blob else {
            return Ok(());
        };
        let num_uncertified_blob_threshold =
            num_uncertified_blob_threshold.unwrap_or(DEFAULT_NUM_UNATTESTED_BLOBS_THRESHOLD);

        let pending = pending_db
            .safe_iter()
            .filter(|result| match result {
                Ok((_, metadata)) => {
                    metadata.end > last_certified_event_blob.ending_checkpoint_sequence_number
                }
                Err(_) => true,
            })
            .collect::<Result<Vec<_>, _>>()?;
        let attested = attested_db
            .safe_iter()
            .filter(|result| match result {
                Ok((_, metadata)) => {
                    metadata.end > last_certified_event_blob.ending_checkpoint_sequence_number
                }
                Err(_) => true,
            })
            .collect::<Result<Vec<_>, _>>()?;
        let failed_to_attest = failed_to_attest_db
            .safe_iter()
            .filter(|result| match result {
                Ok((_, metadata)) => {
                    metadata.end > last_certified_event_blob.ending_checkpoint_sequence_number
                }
                Err(_) => true,
            })
            .collect::<Result<Vec<_>, _>>()?;
        let total_uncertified_blobs = pending_db.safe_iter().count()
            + failed_to_attest_db.safe_iter().count()
            + attested_db.safe_iter().count();
        let uncertified_blobs_after_last_certified =
            pending.len() + failed_to_attest.len() + attested.len();

        let consecutive_uncertified = uncertified_blobs_after_last_certified as u32;

        // We reset the node pending blobs if the local node has already certified the last
        // certified blob on chain, and have pending blob greater than
        // num_uncertified_blob_threshold to recover the possible local node event blob corruption.
        // Node that we need to check either the local last certified blob is the same as the chain
        // last certified blob or the local last certified blob is one checkpoint behind the chain
        // last certified blob, to account for the situation where node is crashed after sending
        // out the last certified blob and before receiving the certification event.
        let should_reset = consecutive_uncertified >= num_uncertified_blob_threshold
            && (total_uncertified_blobs == consecutive_uncertified as usize
                || total_uncertified_blobs == consecutive_uncertified as usize + 1);

        if !should_reset {
            tracing::info!(
                "Skipping reset: uncertified blobs ({}/{}) below threshold {}. Last certified at \
                checkpoint {}",
                consecutive_uncertified,
                total_uncertified_blobs,
                num_uncertified_blob_threshold,
                last_certified_event_blob.ending_checkpoint_sequence_number
            );
            return Ok(());
        }

        tracing::info!(
            "Resetting event blob writer: uncertified blobs ({}) exceeded threshold ({})",
            consecutive_uncertified,
            num_uncertified_blob_threshold
        );

        let mut blobs_to_delete = Vec::new();
        let mut wb = pending_db.batch();

        for (k, metadata) in pending {
            blobs_to_delete.push(metadata.event_cursor.element_index);
            wb.delete_batch(pending_db, std::iter::once(k))?;
        }

        for (_, metadata) in attested {
            blobs_to_delete.push(metadata.event_cursor.element_index);
            wb.delete_batch(attested_db, std::iter::once(()))?;
        }

        for (_, metadata) in failed_to_attest {
            blobs_to_delete.push(metadata.event_cursor.element_index);
            wb.delete_batch(failed_to_attest_db, std::iter::once(()))?;
        }

        wb.write()?;

        for blob_index in blobs_to_delete {
            let blob_path = blobs_path.join(blob_index.to_string());
            if blob_path.exists() {
                if let Err(e) = fs::remove_file(&blob_path) {
                    tracing::warn!(?blob_path, ?e, "Failed to remove blob file");
                } else {
                    tracing::info!(?blob_path, "Removed uncertified blob file");
                }
            }
        }

        Ok(())
    }
}

/// EventBlobWriter manages the creation, storage, and certification of event blobs.
///
/// ```text
///  +-------------------+
///  |  EventBlobWriter  |
///  +-------------------+
///           |
///           v
///   +------------+    +-------------+    +-------------+    +---------------+
///   |Current Blob|--->| Pending Blob|--->|Attested Blob|--->|System Contract|
///   +------------+    +-------------+    +-------------+    +---------------+
///         |                 |                  ^                   |
///         |                 |                  |                   |
///         |                 v                  |                   |
///   +-----------------+    +------------------+                    |
///   |Filesystem (tmp) |    | Failed to Attest |                    |
///   +-----------------+    +------------------+                    |
///                                                                  v
///                  +------------------+                     +--------------+
///                  | Database Storage |<------------------- |Certified Blob|
///                  +------------------+                     +--------------+
///                  (Blob removed from
///                   filesystem after
///                   certification)
///
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
/// d. Failed to Attest:
/// If the node fails to attest a blob, it moves the blob to the failed to attest database.
/// The node will attempt to attest this blob again with a backoff logic.
///
/// e. Certified:
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
    /// Failed to attest blobs metadata.
    failed_to_attest: DBMap<(), FailedToAttestEventBlobMetadata>,
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
    /// Whether to pause blob attestations.
    pause_attestations: bool,
    /// Backoff logic for event writing.
    backoff: EventBackoff,
}

/// Struct to group database-related parameters.
#[derive(Debug)]
pub struct EventBlobDatabases {
    certified: DBMap<(), CertifiedEventBlobMetadata>,
    attested: DBMap<(), AttestedEventBlobMetadata>,
    failed_to_attest: DBMap<(), FailedToAttestEventBlobMetadata>,
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

/// Struct to manage event-based backoff logic.
#[derive(Debug)]
struct EventBackoff {
    /// Base number of events to wait before retry
    base_events: u64,
    /// Current number of events to wait
    current_wait: u64,
    /// Number of events processed since last attempt
    events_since_attempt: u64,
    /// Maximum number of retry attempts after which
    /// the backoff will stop increasing
    max_increase_backoff_for_attempts: u32,
    /// Current number of attempts
    attempts: u32,
}

impl EventBackoff {
    /// Creates a new EventBackoff instance with base event count and max attempts.
    fn new(base_events: u64, max_increase_backoff_for_attempts: u32) -> Self {
        Self {
            base_events,
            current_wait: base_events,
            events_since_attempt: 0,
            max_increase_backoff_for_attempts,
            attempts: 0,
        }
    }

    /// Returns the next wait count. Once max attempts is reached,
    /// keeps returning the current wait count without doubling.
    fn update_next_attempt(&mut self) {
        if self.attempts < self.max_increase_backoff_for_attempts {
            self.current_wait *= 2
        }
        self.attempts += 1;
        // Add random jitter between -10% and +10%
        let jitter = thread_rng().gen_range(-0.1..0.1);
        self.current_wait = (self.current_wait as f64 * (1.0 + jitter)) as u64;
    }

    /// Records an event.
    fn tick(&mut self) {
        self.events_since_attempt += 1;
    }

    /// Checks if enough events have passed to retry.
    fn can_retry(&self) -> bool {
        self.events_since_attempt >= self.current_wait
    }

    /// Resets the backoff to initial state.
    fn reset(&mut self) {
        self.current_wait = self.base_events;
        self.events_since_attempt = 0;
        self.attempts = 0;
    }
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
            failed_to_attest: databases.failed_to_attest,
            node,
            current_epoch: config.current_epoch,
            event_cursor: config.event_stream_cursor,
            prev_certified_blob_id: config.prev_certified_blob_id,
            prev_certified_event_id: config.event_stream_cursor.event_id,
            metrics: config.metrics,
            num_checkpoints_per_blob,
            pause_attestations: false,
            backoff: EventBackoff::new(5, 5),
        };

        // Upon receiving a blob_certified event for an event blob, we may have crashed after making
        // the db changes but before attesting the next pending blob (since those two events are
        // not atomic).This invocation is to account for that particular scenario.
        blob_writer.try_attest_next_blob().await;
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
        let next_file_path = dir_path.join(CURRENT_BLOB);
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
            .get_for_type(DEFAULT_ENCODING)
            .encode_with_metadata(&content)?;
        self.node
            .storage()
            .put_verified_metadata_without_blob_info(&blob_metadata)
            .context("unable to store metadata")?;
        // TODO: Once shard assignment per storage node will be read from walrus
        // system object at the beginning of the walrus epoch, we can only store the blob for
        // shards that are locally assigned to this node. (#682)
        let blob_metadata_clone = blob_metadata.clone();

        try_join_all(sliver_pairs.iter().map(|sliver_pair| async {
            self.node
                .store_sliver_unchecked(
                    blob_metadata_clone.clone(),
                    sliver_pair.index(),
                    Sliver::Primary(sliver_pair.primary.clone()),
                )
                .await
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

        let blob_metadata_clone = blob_metadata.clone();

        try_join_all(sliver_pairs.iter().map(|sliver_pair| async {
            self.node
                .store_sliver_unchecked(
                    blob_metadata_clone.clone(),
                    sliver_pair.index(),
                    Sliver::Secondary(sliver_pair.secondary.clone()),
                )
                .await
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
        &mut self,
        metadata: &VerifiedBlobMetadataWithId,
        checkpoint_sequence_number: CheckpointSequenceNumber,
    ) -> Result<()> {
        let blob_id = *metadata.blob_id();

        if self.attestations_paused() {
            tracing::debug!("attestations are paused, skipping blob: {}", blob_id);
            return Ok(());
        }
        tracing::info!(
            blob_id = %blob_id,
            epoch = self.current_epoch,
            checkpoint = checkpoint_sequence_number,
            "attesting event blob"
        );

        self.metrics
            .latest_attested_checkpoint_sequence_number
            .set(checkpoint_sequence_number as i64);

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
            Ok(()) => {
                tracing::info!(
                    blob_id = %blob_id,
                    epoch = self.current_epoch,
                    checkpoint = checkpoint_sequence_number,
                    "attesting event blob successfully"
                );
                Ok(())
            }
            Err(err) => {
                let result = match err {
                    SuiClientError::TransactionExecutionError(
                        MoveExecutionError::SystemStateInner(
                            SystemStateInnerError::EInvalidIdEpoch(_)
                            | SystemStateInnerError::ENotCommitteeMember(_),
                        ),
                    ) => {
                        self.pause_attestations();
                        Ok(())
                    }

                    e @ SuiClientError::TransactionExecutionError(
                        MoveExecutionError::SystemStateInner(
                            SystemStateInnerError::EIncorrectAttestation(_)
                            | SystemStateInnerError::ERepeatedAttestation(_),
                        ),
                    ) => {
                        tracing::error!(
                            walrus.epoch = self.current_epoch,
                            error = ?e,
                            blob_id = ?blob_id,
                            "Unexpected non-retriable event blob certification error \
                            while attesting event blob"
                        );
                        Ok(())
                    }
                    SuiClientError::TransactionExecutionError(MoveExecutionError::NotParsable(
                        _,
                    )) => {
                        tracing::error!(blob_id = ?blob_id,
                                "Unexpected unknown transaction execution error while \
                                attesting event blob, retrying");
                        Err(err)
                    }
                    ref e @ SuiClientError::TransactionExecutionError(_) => {
                        tracing::warn!(error = ?e, blob_id = ?blob_id,
                                "Unexpected move execution error while attesting event blob");
                        Err(err)
                    }
                    SuiClientError::SharedObjectCongestion(_) => {
                        tracing::debug!(blob_id = ?blob_id,
                                "Shared object congestion error while attesting event blob, \
                                retrying");
                        Err(err)
                    }
                    ref e => {
                        tracing::error!(error = ?e, blob_id = ?blob_id,
                                "Unexpected event blob certification error while attesting \
                                event blob");
                        Err(err)
                    }
                };
                result?;
                Ok(())
            }
        }
    }

    /// Attests the next pending blob.
    ///
    /// This method processes the next pending blob by storing its slivers,
    /// attesting it, and updating the database state. Returns the blob id if it is attested.
    async fn attest_pending_blob(&mut self) -> Result<Option<BlobId>> {
        let Some(result) = self.pending.safe_iter().next() else {
            return Ok(None);
        };
        let (event_index, metadata) = result?;

        self.update_blob_header(
            metadata.event_cursor.element_index,
            &self.prev_certified_blob_id,
            &self.prev_certified_event_id,
        )?;

        let blob_metadata = self.store_slivers(&metadata).await?;
        let blob_id = *blob_metadata.blob_id();

        let mut batch = self.pending.batch();
        match self.attest_blob(&blob_metadata, metadata.end).await {
            Ok(_) => {
                let attested_metadata = metadata.to_attested(blob_metadata.clone());
                batch.insert_batch(&self.attested, std::iter::once(((), attested_metadata)))?;
                batch.delete_batch(&self.pending, std::iter::once(event_index))?;
                batch.write()?;
                Ok(Some(blob_id))
            }
            Err(e) => {
                batch.insert_batch(
                    &self.failed_to_attest,
                    std::iter::once(((), metadata.to_failed_to_attest(blob_id))),
                )?;
                batch.delete_batch(&self.pending, std::iter::once(event_index))?;
                batch.write()?;
                self.backoff.reset();
                Err(e.context(blob_id.to_string()))
            }
        }
    }

    /// Attempts to attest the blob which previously failed to attest if the backoff logic allows it
    /// and if there is a blob to attest.
    ///
    /// This method processes a failed to attest blob by storing its slivers,
    /// attesting it, and updating the database state.
    async fn attest_failed_to_attest_blob(&mut self) -> Result<Option<BlobId>> {
        let Some(metadata) = self.failed_to_attest.get(&())? else {
            return Ok(None);
        };
        if !self.backoff.can_retry() {
            return Err(anyhow::anyhow!("backoff cannot retry"));
        } else {
            self.backoff.update_next_attempt();
        }
        let blob_metadata = self.store_slivers(&metadata.to_pending()).await?;
        let blob_id = *blob_metadata.blob_id();

        match self.attest_blob(&blob_metadata, metadata.end).await {
            Ok(_) => {
                let mut batch = self.failed_to_attest.batch();
                batch.insert_batch(&self.attested, std::iter::once(((), metadata)))?;
                batch.delete_batch(&self.failed_to_attest, std::iter::once(()))?;
                batch.write()?;
                Ok(Some(blob_id))
            }
            Err(e) => Err(e.context(blob_id.to_string())),
        }
    }

    /// Attempts to attest the next blob, catching and logging any errors.
    async fn attest_next_blob(&mut self) -> Result<Option<BlobId>> {
        self.backoff.tick();

        if !self.attested.is_empty() {
            return Ok(None);
        }

        match self.attest_failed_to_attest_blob().await {
            Ok(Some(blob_id)) => Ok(Some(blob_id)),
            Ok(None) => self.attest_pending_blob().await,
            Err(e) => Err(e),
        }
    }

    /// Attempts to attest the next blob, catching and logging any errors.
    /// This is a wrapper around `attest_next_blob` that ensures execution continues
    /// even if attestation fails.
    async fn try_attest_next_blob(&mut self) {
        match self.attest_next_blob().await {
            Ok(Some(blob_id)) => {
                tracing::info!("attested event blob with id: {}", blob_id);
            }
            Ok(None) => {}
            Err(e) => {
                tracing::debug!(
                    error = ?e,
                    "failed to attest event blob, will retry later"
                );
            }
        }
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

        self.metrics
            .latest_processed_event_index
            .set(element_index as i64);

        self.try_attest_next_blob().await;

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

        let attested = self.attested.clone();
        let failed_to_attest = self.failed_to_attest.clone();
        let metadata = self
            .handle_blob_certification(blob_id, batch, &attested)?
            .or_else(|| {
                self.handle_blob_certification(blob_id, batch, &failed_to_attest)
                    .ok()
                    .flatten()
            });

        let Some(metadata) = metadata else {
            return Ok(());
        };

        self.node
            .storage()
            .update_blob_info_with_metadata(&blob_id)
            .context("unable to update metadata")?;

        self.metrics
            .latest_certified_event_index
            .set(element_index as i64);

        batch.insert_batch(&self.certified, std::iter::once(((), metadata.clone())))?;

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

    /// Handles the certification of a blob from either attested or failed to attest state.
    ///
    /// This method checks if the blob exists in either the attested or failed to attest map,
    /// and if so, deletes it from the corresponding map and returns the certified metadata.
    fn handle_blob_certification(
        &mut self,
        blob_id: BlobId,
        batch: &mut DBBatch,
        db_map: &DBMap<(), EventBlobMetadata<CheckpointSequenceNumber, BlobId>>,
    ) -> Result<Option<CertifiedEventBlobMetadata>> {
        let Some(metadata) = db_map.get(&())? else {
            return Ok(None);
        };

        if metadata.blob_id != blob_id {
            return Ok(None);
        }

        batch.delete_batch(db_map, std::iter::once(()))?;
        Ok(Some(metadata.to_certified()))
    }

    /// Pauses attestations until next epoch, logging the state change.
    fn pause_attestations(&mut self) {
        if !self.pause_attestations {
            tracing::info!(
                "pausing attestations until next epoch {}",
                self.current_epoch + 1
            );
            self.pause_attestations = true;
        }
    }

    /// Resumes attestations if they were paused, logging the state change.
    fn resume_attestations(&mut self) {
        if self.pause_attestations {
            tracing::info!("resuming attestations in new epoch {}", self.current_epoch);
            self.pause_attestations = false;
        }
    }

    /// Returns true if attestations are currently paused
    fn attestations_paused(&self) -> bool {
        self.pause_attestations
    }

    /// Updates the current epoch based on epoch change events.
    ///
    /// This method checks for epoch change events and updates the current epoch
    /// and blob states accordingly.
    fn update_epoch(&mut self, event: &PositionedStreamEvent, batch: &mut DBBatch) -> Result<()> {
        let EventStreamElement::ContractEvent(ContractEvent::EpochChangeEvent(
            EpochChangeEvent::EpochChangeStart(new_epoch),
        )) = &event.element
        else {
            return Ok(());
        };
        self.current_epoch = new_epoch.epoch;
        self.resume_attestations();
        self.move_attested_blob_to_pending(batch)?;
        Ok(())
    }

    /// Moves an attested blob to the pending state during epoch changes.
    ///
    /// This method updates the database state to move attested blobs back to
    /// pending status when an epoch change occurs.
    fn move_attested_blob_to_pending(&mut self, batch: &mut DBBatch) -> Result<()> {
        let Some(result) = self.attested.safe_iter().next() else {
            return Ok(());
        };
        let (_, metadata) = result?;

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
    use sui_types::{digests::TransactionDigest, event::EventID};
    use typed_store::Map;
    use walrus_core::{BlobId, ShardIndex};
    use walrus_sui::{
        test_utils::EventForTesting,
        types::{BlobCertified, ContractEvent, EpochChangeEvent, EpochChangeStart},
    };
    use walrus_utils::metrics::Registry;

    use crate::{
        node::{
            DatabaseConfig,
            events::{
                CheckpointEventPosition,
                PositionedStreamEvent,
                event_blob::EventBlob,
                event_blob_writer::{EventBlobWriter, EventBlobWriterFactory},
            },
        },
        test_utils::StorageNodeHandle,
    };

    #[tokio::test]
    async fn test_blob_writer_all_events_exist() -> Result<()> {
        const NUM_BLOBS: u64 = 10;
        const NUM_EVENTS_PER_CHECKPOINT: u64 = 2;

        let dir: PathBuf = tempfile::tempdir()?.keep();
        let registry = Registry::default();
        let node = create_test_node().await?;
        let blob_writer_factory = EventBlobWriterFactory::new(
            &dir,
            &DatabaseConfig::default(),
            node.storage_node.inner().clone(),
            &registry,
            Some(10),
            None,
            None,
        )?;
        let mut blob_writer = blob_writer_factory.create().await?;
        let num_checkpoints: u64 = NUM_BLOBS * blob_writer.num_checkpoints_per_blob() as u64;

        generate_and_write_events(&mut blob_writer, num_checkpoints, NUM_EVENTS_PER_CHECKPOINT)
            .await?;

        let pending_blobs = blob_writer.pending.safe_iter().collect::<Vec<_>>();
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
    async fn test_blob_writer_with_paused_attestations() -> Result<()> {
        const NUM_BLOBS: u64 = 10;
        const NUM_EVENTS_PER_CHECKPOINT: u64 = 1;

        let dir: PathBuf = tempfile::tempdir()?.keep();
        let node = create_test_node().await?;
        let registry = Registry::default();

        let blob_writer_factory = EventBlobWriterFactory::new(
            &dir,
            &DatabaseConfig::default(),
            node.storage_node.inner().clone(),
            &registry,
            Some(10),
            None,
            None,
        )?;
        let mut blob_writer = blob_writer_factory.create().await?;
        let num_checkpoints: u64 = NUM_BLOBS * blob_writer.num_checkpoints_per_blob() as u64;

        // Verify attestations are not paused
        assert!(!blob_writer.attestations_paused());

        // Simulate error and pause attestations
        blob_writer.pause_attestations();
        assert!(blob_writer.attestations_paused());

        // Generate and write events
        generate_and_write_events(&mut blob_writer, num_checkpoints, NUM_EVENTS_PER_CHECKPOINT)
            .await?;

        // Simulate epoch change
        let epoch_change_event = PositionedStreamEvent::new(
            ContractEvent::EpochChangeEvent(EpochChangeEvent::EpochChangeStart(EpochChangeStart {
                epoch: 1,
                event_id: EventID {
                    tx_digest: TransactionDigest::default(),
                    event_seq: 0,
                },
            })),
            CheckpointEventPosition::new(0, 0),
        );

        blob_writer
            .write(epoch_change_event, blob_writer.event_cursor.element_index)
            .await?;

        // Verify attestations are resumed
        assert!(!blob_writer.attestations_paused());

        // There should be one attested blob
        let attested_blob = blob_writer
            .attested
            .get(&())?
            .expect("Attested blob should exist");
        let attested_blob_id = attested_blob.blob_id;

        let pending_blobs = blob_writer
            .pending
            .safe_iter()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(pending_blobs.len() as u64, NUM_BLOBS - 1);
        let first_pending_blob_event_index = pending_blobs[0].0;

        // Certify blob and verify state
        certify_attested_blob(&mut blob_writer, attested_blob_id, num_checkpoints).await?;

        verify_certified_blob(&blob_writer, attested_blob_id)?;
        verify_next_attested_blob(
            &blob_writer,
            &dir,
            first_pending_blob_event_index + 1,
            attested_blob_id,
        )?;

        assert_eq!(
            blob_writer.pending.safe_iter().count() as u64,
            NUM_BLOBS - 2
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_blob_writer_e2e() -> Result<()> {
        const NUM_BLOBS: u64 = 10;
        const NUM_EVENTS_PER_CHECKPOINT: u64 = 1;

        let dir: PathBuf = tempfile::tempdir()?.keep();
        let node = create_test_node().await?;
        let registry = Registry::default();
        let blob_writer_factory = EventBlobWriterFactory::new(
            &dir,
            &DatabaseConfig::default(),
            node.storage_node.inner().clone(),
            &registry,
            Some(10),
            None,
            None,
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

        let pending_blobs = blob_writer
            .pending
            .safe_iter()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(pending_blobs.len() as u64, NUM_BLOBS - 1);
        let first_pending_blob_event_index = pending_blobs[0].0;

        // Certify blob and verify state
        certify_attested_blob(&mut blob_writer, attested_blob_id, num_checkpoints).await?;

        verify_certified_blob(&blob_writer, attested_blob_id)?;
        verify_next_attested_blob(
            &blob_writer,
            &dir,
            first_pending_blob_event_index + 1,
            attested_blob_id,
        )?;

        assert_eq!(
            blob_writer.pending.safe_iter().count() as u64,
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

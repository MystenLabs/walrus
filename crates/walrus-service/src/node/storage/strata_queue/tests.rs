// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{path::Path, time::Duration};

use strata::queue::{
    BlobEdit,
    BlobOperand,
    BlobOperation,
    EpochBarrier,
    Error,
    PENDING_BLOBS_CF,
    PendingBatch,
    PendingBlobOps,
    Result,
    ShardGeneration,
};
use strata_index::port::{IndexDb, TypedMap, codec::encode_key};
use sui_types::digests::TransactionDigest;
use tempfile::TempDir;
use typed_store::{
    Map,
    rocks::{DBMap, MetricConf, ReadWriteOptions},
};
use walrus_test_utils::Result as TestResult;
use walrus_utils::metrics::Registry;

use super::*;
use crate::node::storage::{DatabaseConfig, Storage, constants};

fn open(path: &Path, optimistic: bool) -> anyhow::Result<Storage> {
    // The metrics accessor initializes a shared registry; serialize its first use in these tests.
    static METRICS: std::sync::Once = std::sync::Once::new();
    METRICS.call_once(|| {
        typed_store::DBMetrics::get();
    });
    let mut config = DatabaseConfig::default_for_test();
    config.global.use_optimistic_transaction_db = optimistic;
    Storage::open(path, config, MetricConf::default(), Registry::default())
}

fn progress(
    storage: &Storage,
) -> std::result::Result<DBMap<(), u64>, typed_store::TypedStoreError> {
    DBMap::reopen(
        &storage.database,
        Some(constants::event_index_cf_name()),
        &ReadWriteOptions::default(),
        false,
    )
}

fn database(storage: &Storage) -> Arc<dyn IndexDb> {
    Arc::new(database::Database(Arc::clone(&storage.database)))
}

fn blobs(storage: &Storage) -> Result<TypedMap<Vec<u8>, PendingBlobOps>> {
    Ok(TypedMap::new(database(storage), PENDING_BLOBS_CF))
}

fn stage_progress(batch: &mut PendingBatch, value: u64) -> Result<()> {
    // Walrus metadata keeps its existing BCS encoding; the shared batch carries raw bytes.
    batch.metadata().put(
        constants::event_index_cf_name(),
        &encode_key(&())?,
        &bcs::to_bytes(&value)
            .map_err(|error| strata_index::Error::Serialization(error.to_string()))?,
    )?;
    Ok(())
}

fn source(event_index: u64) -> Vec<u8> {
    bcs::to_bytes(&StrataSourceEvent {
        event_index,
        event_id: EventID {
            tx_digest: TransactionDigest::new([7; 32]),
            event_seq: 3,
        },
    })
    .expect("event serialization succeeds")
}

fn delete(cancellable: bool) -> BlobOperation {
    BlobOperation::Delete {
        shards: vec![ShardGeneration {
            shard: 2,
            generation: 3,
        }],
        cancellable,
    }
}

fn aborted() -> Error {
    Error::InvalidPendingOperation("abort".into())
}

#[tokio::test]
async fn batches_commit_metadata_blob_commands_and_barriers_together() -> TestResult {
    for optimistic in [false, true] {
        let dir = TempDir::new()?;
        let storage = open(dir.path(), optimistic)?;
        let progress = progress(&storage)?;
        let queue = &storage.strata_queue;
        queue.write_batch(|write| {
            write.register(b"a", 42, 15, source(42))?;
            write.register(b"b", 42, 15, source(42))?;
            write.advance_epoch(43, 10, source(43))?;
            stage_progress(write, 43)
        })?;
        assert_eq!(progress.get(&())?, Some(43));
        let snapshot = queue.durable_snapshot()?;
        let rows = queue
            .blobs(snapshot.as_ref())?
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(rows.len(), 2);
        for (_, row) in rows {
            assert_eq!(row.commands()[0].event_index, 42);
            assert_eq!(row.commands()[0].source, source(42));
        }
        assert_eq!(
            queue
                .barriers(snapshot.as_ref())?
                .collect::<Result<Vec<_>>>()?,
            vec![(
                43,
                EpochBarrier::V1 {
                    epoch: 10,
                    source: source(43)
                }
            )]
        );
    }
    Ok(())
}

#[tokio::test]
async fn aborted_batch_discards_metadata_and_pending_events() -> TestResult {
    for optimistic in [false, true] {
        let dir = TempDir::new()?;
        let storage = open(dir.path(), optimistic)?;
        let progress = progress(&storage)?;
        let queue = &storage.strata_queue;
        let result: Result<()> = queue.write_batch(|write| {
            stage_progress(write, 1)?;
            write.register(b"a", 0, 15, source(0))?;
            write.advance_epoch(1, 10, source(1))?;
            Err(aborted())
        });
        assert!(result.is_err());
        assert_eq!(progress.get(&())?, None);
        let snapshot = queue.durable_snapshot()?;
        assert!(
            queue
                .blobs(snapshot.as_ref())?
                .collect::<Result<Vec<_>>>()?
                .is_empty()
        );
        assert!(
            queue
                .barriers(snapshot.as_ref())?
                .collect::<Result<Vec<_>>>()?
                .is_empty()
        );
        queue.write_batch(|w| {
            w.register(b"a", 0, 15, source(0))?;
            stage_progress(w, 0)
        })?;
        assert_eq!(progress.get(&())?, Some(0));
        let snapshot = queue.durable_snapshot()?;
        assert_eq!(
            queue
                .blobs(snapshot.as_ref())?
                .next()
                .expect("row exists")?
                .1
                .commands()[0]
                .event_index,
            0
        );
    }
    Ok(())
}

#[tokio::test]
async fn registration_cancels_deletes_without_scanning_and_ack_preserves_newer_work() -> TestResult
{
    for optimistic in [false, true] {
        let dir = TempDir::new()?;
        let storage = open(dir.path(), optimistic)?;
        let queue = &storage.strata_queue;
        queue.write_batch(|w| {
            w.append(b"a", 10, delete(true), source(10))?;
            w.append(b"a", 20, delete(false), source(20))?;
            w.register(b"a", 30, 15, source(30))
        })?;
        let snapshot = queue.durable_snapshot()?;
        let rows = queue
            .blobs(snapshot.as_ref())?
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(
            rows[0]
                .1
                .commands()
                .iter()
                .map(|c| c.event_index)
                .collect::<Vec<_>>(),
            vec![20, 30]
        );
        // Worker captured through event 30. Event 40 arrives before its acknowledgement.
        queue.write_batch(|w| {
            w.append(
                b"a",
                40,
                BlobOperation::SetLifetime { end_epoch: 20 },
                source(40),
            )
        })?;
        let map = blobs(&storage)?;
        let mut cleanup = map.batch();
        let operand = BlobOperand::V1(BlobEdit::Acknowledge {
            through_event_index: 30,
        })
        .encode()?;
        cleanup.partial_merge_batch(&map, [(b"a".to_vec(), operand)])?;
        cleanup.write_with_sync(true)?;
        let snapshot = queue.durable_snapshot()?;
        let rows = queue
            .blobs(snapshot.as_ref())?
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(rows[0].1.commands().len(), 1);
        assert_eq!(rows[0].1.commands()[0].event_index, 40);
        assert_eq!(
            rows[0].1.commands()[0].operation,
            BlobOperation::SetLifetime { end_epoch: 20 }
        );
    }
    Ok(())
}

#[tokio::test]
async fn durable_snapshot_streams_stable_rows_and_epoch_barriers() -> TestResult {
    for optimistic in [false, true] {
        let dir = TempDir::new()?;
        let storage = open(dir.path(), optimistic)?;
        let queue = &storage.strata_queue;
        queue.write_batch(|w| {
            w.register(b"a", 254, 15, source(254))?;
            w.advance_epoch(255, 10, source(255))?;
            w.advance_epoch(256, 11, source(256))?;
            w.register(b"b", 257, 20, source(257))
        })?;
        let snapshot = queue.durable_snapshot()?;
        let mut rows = queue.blobs(snapshot.as_ref())?;
        // The barrier iterator is created later but must use the same captured view.
        queue.write_batch(|w| {
            w.append(b"a", 258, delete(true), source(258))?;
            w.register(b"c", 259, 25, source(259))?;
            w.advance_epoch(260, 12, source(260))
        })?;
        let a = rows.next().expect("first row exists")?;
        assert_eq!(a.0, b"a");
        assert_eq!(a.1.commands().len(), 1);
        assert_eq!(rows.next().expect("second row exists")?.0, b"b");
        assert!(rows.next().is_none());
        assert_eq!(
            queue
                .barriers(snapshot.as_ref())?
                .map(|row| row.map(|(event_index, _)| event_index))
                .collect::<Result<Vec<_>>>()?,
            vec![255, 256]
        );
        let latest = queue.durable_snapshot()?;
        assert_eq!(
            queue
                .blobs(latest.as_ref())?
                .collect::<Result<Vec<_>>>()?
                .len(),
            3
        );
        assert_eq!(
            queue
                .barriers(latest.as_ref())?
                .collect::<Result<Vec<_>>>()?
                .len(),
            3
        );
    }
    Ok(())
}

#[tokio::test]
async fn concurrent_blobs_preserve_caller_event_order() -> TestResult {
    let dir = TempDir::new()?;
    let storage = open(dir.path(), true)?;
    std::thread::scope(|scope| {
        let mut threads = Vec::new();
        for blob in 0u8..4 {
            let queue = storage.strata_queue.clone();
            threads.push(scope.spawn(move || -> Result<()> {
                // Each blob receives the same event stream; scheduling across blobs is independent.
                for event_index in 0..16 {
                    queue.write_batch(|w| {
                        w.append(
                            &[blob],
                            event_index,
                            BlobOperation::SetLifetime { end_epoch: 20 },
                            source(event_index),
                        )
                    })?;
                }
                Ok(())
            }));
        }
        for thread in threads {
            thread.join().expect("producer must not panic")?;
        }
        Ok::<_, Error>(())
    })?;
    let queue = &storage.strata_queue;
    let snapshot = queue.durable_snapshot()?;
    let rows = queue
        .blobs(snapshot.as_ref())?
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(rows.len(), 4);
    for (_, row) in rows {
        assert_eq!(
            row.commands()
                .iter()
                .map(|c| c.event_index)
                .collect::<Vec<_>>(),
            (0..16).collect::<Vec<_>>()
        );
    }
    Ok(())
}

#[tokio::test]
async fn caller_event_indexes_survive_reopen_and_row_cleanup() -> TestResult {
    for optimistic in [false, true] {
        let dir = TempDir::new()?;
        let storage = open(dir.path(), optimistic)?;
        storage.strata_queue.write_batch(|w| {
            w.register(b"a", 0, 15, source(0))?;
            w.append(b"a", 7, delete(true), source(7))?;
            w.register(b"a", 42, 20, source(42))?;
            w.register(b"b", 42, 20, source(42))?;
            w.advance_epoch(100, 10, source(100))
        })?;
        // Model safe row cleanup after completion. Subsequent IDs still come from Walrus.
        let map = blobs(&storage)?;
        let mut cleanup = map.batch();
        cleanup.delete_batch(&map, [b"b".to_vec()])?;
        cleanup.write()?;
        drop(map);
        drop(storage.strata_queue.durable_snapshot()?);
        let database = Arc::downgrade(&storage.database);
        drop(storage);
        tokio::time::timeout(Duration::from_secs(5), async {
            while database.strong_count() > 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;
        let storage = open(dir.path(), optimistic)?;
        let queue = &storage.strata_queue;
        let snapshot = queue.durable_snapshot()?;
        let rows = queue
            .blobs(snapshot.as_ref())?
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0]
                .1
                .commands()
                .iter()
                .map(|c| c.event_index)
                .collect::<Vec<_>>(),
            vec![0, 42]
        );
        assert_eq!(
            queue
                .barriers(snapshot.as_ref())?
                .collect::<Result<Vec<_>>>()?[0]
                .0,
            100
        );
        queue.write_batch(|w| w.register(b"b", 200, 25, source(200)))?;
        let latest = queue.durable_snapshot()?;
        let rows = queue.blobs(latest.as_ref())?.collect::<Result<Vec<_>>>()?;
        assert_eq!(rows[1].1.commands()[0].event_index, 200);
    }
    Ok(())
}

#[tokio::test]
async fn persisted_event_progress_filters_retries_even_after_acknowledgement() -> TestResult {
    for optimistic in [false, true] {
        let dir = TempDir::new()?;
        let storage = open(dir.path(), optimistic)?;
        let progress = progress(&storage)?;
        let queue = &storage.strata_queue;
        // Model Walrus's ordered event handler, including the replay check before enqueueing.
        let enqueue = |event_index| -> TestResult {
            if progress
                .get(&())?
                .is_some_and(|handled| handled >= event_index)
            {
                return Ok(());
            }
            queue.write_batch(|w| {
                w.register(b"a", event_index, 15, source(event_index))?;
                stage_progress(w, event_index)
            })?;
            Ok(())
        };
        enqueue(0)?;
        assert_eq!(progress.get(&())?, Some(0));
        let map = blobs(&storage)?;
        let mut cleanup = map.batch();
        let operand = BlobOperand::V1(BlobEdit::Acknowledge {
            through_event_index: 0,
        })
        .encode()?;
        cleanup.partial_merge_batch(&map, [(b"a".to_vec(), operand)])?;
        cleanup.write_with_sync(true)?;
        enqueue(0)?;
        let snapshot = queue.durable_snapshot()?;
        let rows = queue
            .blobs(snapshot.as_ref())?
            .collect::<Result<Vec<_>>>()?;
        assert!(rows[0].1.commands().is_empty());
        enqueue(u64::MAX)?;
        assert_eq!(progress.get(&())?, Some(u64::MAX));
        let latest = queue.durable_snapshot()?;
        let rows = queue.blobs(latest.as_ref())?.collect::<Result<Vec<_>>>()?;
        assert_eq!(rows[0].1.commands()[0].event_index, u64::MAX);
    }
    Ok(())
}

#[tokio::test]
async fn corrupt_merge_data_is_an_error_instead_of_an_empty_queue() -> TestResult {
    for optimistic in [false, true] {
        let dir = TempDir::new()?;
        let storage = open(dir.path(), optimistic)?;
        let map = blobs(&storage)?;
        let mut batch = map.batch();
        batch.partial_merge_batch(&map, [(b"a".to_vec(), vec![255])])?;
        batch.write()?;
        let queue = &storage.strata_queue;
        let snapshot = queue.durable_snapshot()?;
        assert!(
            queue
                .blobs(snapshot.as_ref())?
                .collect::<Result<Vec<_>>>()
                .is_err()
        );
    }
    Ok(())
}

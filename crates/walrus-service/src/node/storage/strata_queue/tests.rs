// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{path::Path, time::Duration};

use ::strata_queue::{BlobEdit, BlobOperand, BlobOperation, ShardGeneration};
use sui_types::digests::TransactionDigest;
use tempfile::TempDir;
use typed_store::rocks::MetricConf;
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

fn progress(storage: &Storage) -> Result<DBMap<(), u64>> {
    Ok(DBMap::reopen(
        &storage.database,
        Some(constants::event_index_cf_name()),
        &ReadWriteOptions::default(),
        false,
    )?)
}

fn last_revision(storage: &Storage) -> Result<DBMap<(), Revision>> {
    Ok(DBMap::reopen(
        &storage.database,
        Some(LAST_REVISION_CF),
        &ReadWriteOptions::default(),
        false,
    )?)
}

fn blobs(storage: &Storage) -> Result<DBMap<Vec<u8>, PendingBlobOps>> {
    Ok(DBMap::reopen(
        &storage.database,
        Some(PENDING_BLOBS_CF),
        &ReadWriteOptions::default(),
        false,
    )?)
}

fn source() -> Vec<u8> {
    bcs::to_bytes(&StrataSourceEvent {
        event_index: 42,
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

fn aborted() -> QueueError {
    QueueError::Storage(TypedStoreError::TaskError("abort".into()))
}

#[tokio::test]
async fn batches_commit_metadata_blob_commands_and_barriers_together() -> TestResult {
    for optimistic in [false, true] {
        let dir = TempDir::new()?;
        let storage = open(dir.path(), optimistic)?;
        let progress = progress(&storage)?;
        let queue = &storage.strata_queue;
        let registration = queue.write_batch(|write| {
            let registration = write.register(b"a", 15, source())?;
            write.advance_epoch(10, source())?;
            write.metadata().insert_batch(&progress, [((), 42)])?;
            Ok(registration)
        })?;
        assert_eq!(registration, Revision(1));
        assert_eq!(progress.get(&())?, Some(42));
        assert_eq!(last_revision(&storage)?.get(&())?, Some(Revision(2)));
        let snapshot = queue.durable_snapshot()?;
        let rows = snapshot.blobs(None, 10)?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1.commands()[0].revision, registration);
        assert_eq!(rows[0].1.commands()[0].source, source());
        assert_eq!(
            snapshot.barriers(None, 10)?,
            vec![(
                Revision(2),
                EpochBarrier::V1 {
                    epoch: 10,
                    source: source()
                }
            )]
        );
    }
    Ok(())
}

#[tokio::test]
async fn aborted_batch_discards_all_tables_and_revision_allocation() -> TestResult {
    for optimistic in [false, true] {
        let dir = TempDir::new()?;
        let storage = open(dir.path(), optimistic)?;
        let progress = progress(&storage)?;
        let queue = &storage.strata_queue;
        let result: Result<()> = queue.write_batch(|write| {
            write.metadata().insert_batch(&progress, [((), 42)])?;
            write.register(b"a", 15, source())?;
            write.advance_epoch(10, source())?;
            Err(aborted())
        });
        assert!(result.is_err());
        assert_eq!(progress.get(&())?, None);
        assert_eq!(last_revision(&storage)?.get(&())?, None);
        let snapshot = queue.durable_snapshot()?;
        assert!(snapshot.blobs(None, 10)?.is_empty());
        assert!(snapshot.barriers(None, 10)?.is_empty());
        assert_eq!(
            queue.write_batch(|w| w.register(b"a", 15, source()))?,
            Revision(1)
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
            w.append(b"a", delete(true), vec![])?;
            w.append(b"a", delete(false), vec![])?;
            w.register(b"a", 15, source())
        })?;
        let rows = queue.durable_snapshot()?.blobs(None, 10)?;
        assert_eq!(
            rows[0]
                .1
                .commands()
                .iter()
                .map(|c| c.revision)
                .collect::<Vec<_>>(),
            vec![Revision(2), Revision(3)]
        );
        // Worker captured through revision 3. A new command arrives before its acknowledgement.
        queue.write_batch(|w| {
            w.append(b"a", BlobOperation::SetLifetime { end_epoch: 20 }, source())
        })?;
        let map = blobs(&storage)?;
        let mut cleanup = map.batch();
        let operand = BlobOperand::V1(BlobEdit::Acknowledge {
            through: Revision(3),
        })
        .encode()?;
        cleanup.partial_merge_batch(&map, [(b"a".to_vec(), operand)])?;
        cleanup.write_with_sync(true)?;
        let rows = queue.durable_snapshot()?.blobs(None, 10)?;
        assert_eq!(rows[0].1.commands().len(), 1);
        assert_eq!(rows[0].1.commands()[0].revision, Revision(4));
        assert_eq!(
            rows[0].1.commands()[0].operation,
            BlobOperation::SetLifetime { end_epoch: 20 }
        );
    }
    Ok(())
}

#[tokio::test]
async fn durable_snapshot_is_stable_and_pages_blobs_and_epoch_barriers() -> TestResult {
    for optimistic in [false, true] {
        let dir = TempDir::new()?;
        let storage = open(dir.path(), optimistic)?;
        let queue = &storage.strata_queue;
        last_revision(&storage)?.insert(&(), &Revision(253))?;
        queue.write_batch(|w| {
            w.register(b"a", 15, source())?;
            w.advance_epoch(10, source())?;
            w.advance_epoch(11, source())?;
            w.register(b"b", 20, source())
        })?;
        let snapshot = queue.durable_snapshot()?;
        queue.write_batch(|w| {
            w.append(b"a", delete(true), vec![])?;
            w.register(b"c", 25, source())?;
            w.advance_epoch(12, source())
        })?;
        let a = snapshot.blobs(None, 1)?;
        assert_eq!(a.len(), 1);
        assert_eq!(a[0].0, b"a");
        assert_eq!(a[0].1.commands().len(), 1);
        let b = snapshot.blobs(Some(&a[0].0), 10)?;
        assert_eq!(b.len(), 1);
        assert_eq!(b[0].0, b"b");
        assert!(snapshot.blobs(Some(&b[0].0), 1)?.is_empty());
        assert!(snapshot.blobs(None, 0)?.is_empty());
        let barriers = snapshot.barriers(None, 1)?;
        assert_eq!(barriers[0].0, Revision(255));
        assert_eq!(
            snapshot
                .barriers(Some(Revision(255)), 10)?
                .iter()
                .map(|(r, _)| *r)
                .collect::<Vec<_>>(),
            vec![Revision(256)]
        );
        assert!(snapshot.barriers(Some(Revision(256)), 10)?.is_empty());
        assert!(snapshot.barriers(None, 0)?.is_empty());
        assert_eq!(queue.durable_snapshot()?.blobs(None, 10)?.len(), 3);
    }
    Ok(())
}

#[tokio::test]
async fn concurrent_producers_append_without_lost_operations() -> TestResult {
    let dir = TempDir::new()?;
    let storage = open(dir.path(), true)?;
    std::thread::scope(|scope| {
        let mut threads = Vec::new();
        for _ in 0..4 {
            let queue = storage.strata_queue.clone();
            threads.push(scope.spawn(move || -> Result<()> {
                for _ in 0..16 {
                    queue.write_batch(|w| {
                        w.append(b"a", BlobOperation::SetLifetime { end_epoch: 20 }, vec![])
                    })?;
                }
                Ok(())
            }));
        }
        for thread in threads {
            thread.join().expect("producer must not panic")?;
        }
        Ok::<_, QueueError>(())
    })?;
    let rows = storage.strata_queue.durable_snapshot()?.blobs(None, 10)?;
    assert_eq!(
        rows[0]
            .1
            .commands()
            .iter()
            .map(|c| c.revision.0)
            .collect::<Vec<_>>(),
        (1..=64).collect::<Vec<_>>()
    );
    Ok(())
}

#[tokio::test]
async fn synced_queue_reopens_and_revisions_survive_row_cleanup() -> TestResult {
    for optimistic in [false, true] {
        let dir = TempDir::new()?;
        let storage = open(dir.path(), optimistic)?;
        storage.strata_queue.write_batch(|w| {
            w.register(b"a", 15, source())?;
            w.append(b"a", delete(true), vec![])?;
            w.register(b"a", 20, source())?;
            w.register(b"b", 20, source())?;
            w.advance_epoch(10, source())
        })?;
        // Model safe row cleanup after completion. The revision allocator is never deleted.
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
        let snapshot = storage.strata_queue.durable_snapshot()?;
        let rows = snapshot.blobs(None, 10)?;
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0]
                .1
                .commands()
                .iter()
                .map(|c| c.revision)
                .collect::<Vec<_>>(),
            vec![Revision(1), Revision(3)]
        );
        assert_eq!(snapshot.barriers(None, 10)?[0].0, Revision(5));
        assert_eq!(
            storage
                .strata_queue
                .write_batch(|w| w.register(b"b", 25, source()))?,
            Revision(6)
        );
    }
    Ok(())
}

#[tokio::test]
async fn revision_exhaustion_does_not_commit_metadata() -> TestResult {
    let dir = TempDir::new()?;
    let storage = open(dir.path(), true)?;
    let progress = progress(&storage)?;
    last_revision(&storage)?.insert(&(), &Revision(u64::MAX))?;
    assert!(
        storage
            .strata_queue
            .write_batch(|w| {
                w.metadata().insert_batch(&progress, [((), 42)])?;
                w.register(b"a", 15, source())
            })
            .is_err()
    );
    assert_eq!(progress.get(&())?, None);
    assert!(
        storage
            .strata_queue
            .durable_snapshot()?
            .blobs(None, 10)?
            .is_empty()
    );
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
        let snapshot = storage.strata_queue.durable_snapshot()?;
        assert!(snapshot.blobs(None, 10).is_err());
    }
    Ok(())
}

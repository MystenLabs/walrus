// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{path::Path, time::Duration};

use strata_index::{
    Error,
    Result,
    port::{IndexDb, TypedMap, codec::encode_key},
    queue::{
        BlobEdit,
        BlobOperand,
        BlobOperation,
        EpochBarrier,
        LAST_REVISION_CF,
        PENDING_BLOBS_CF,
        PendingBatch,
        PendingBlobOps,
        ShardGeneration,
    },
};
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

fn last_revision(storage: &Storage) -> Result<TypedMap<(), Revision>> {
    Ok(TypedMap::new(database(storage), LAST_REVISION_CF))
}

fn blobs(storage: &Storage) -> Result<TypedMap<Vec<u8>, PendingBlobOps>> {
    Ok(TypedMap::new(database(storage), PENDING_BLOBS_CF))
}

fn stage_progress(batch: &mut PendingBatch, value: u64) -> Result<()> {
    // Walrus metadata keeps its existing BCS encoding; the shared batch carries raw bytes.
    batch.metadata().put(
        constants::event_index_cf_name(),
        &encode_key(&())?,
        &bcs::to_bytes(&value).map_err(|error| Error::Serialization(error.to_string()))?,
    )
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
        let registration = queue.write_batch(|write| {
            let registration = write.register(b"a", 15, source())?;
            write.advance_epoch(10, source())?;
            stage_progress(write, 42)?;
            Ok(registration)
        })?;
        assert_eq!(registration, Revision(1));
        assert_eq!(progress.get(&())?, Some(42));
        assert_eq!(last_revision(&storage)?.get(&())?, Some(Revision(2)));
        let snapshot = queue.durable_snapshot()?;
        let rows = queue
            .blobs(snapshot.as_ref())?
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1.commands()[0].revision, registration);
        assert_eq!(rows[0].1.commands()[0].source, source());
        assert_eq!(
            queue
                .barriers(snapshot.as_ref())?
                .collect::<Result<Vec<_>>>()?,
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
            stage_progress(write, 42)?;
            write.register(b"a", 15, source())?;
            write.advance_epoch(10, source())?;
            Err(aborted())
        });
        assert!(result.is_err());
        assert_eq!(progress.get(&())?, None);
        assert_eq!(last_revision(&storage)?.get(&())?, None);
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
        let snapshot = queue.durable_snapshot()?;
        let rows = queue
            .blobs(snapshot.as_ref())?
            .collect::<Result<Vec<_>>>()?;
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
        let snapshot = queue.durable_snapshot()?;
        let rows = queue
            .blobs(snapshot.as_ref())?
            .collect::<Result<Vec<_>>>()?;
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
async fn durable_snapshot_streams_stable_rows_and_epoch_barriers() -> TestResult {
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
        let mut rows = queue.blobs(snapshot.as_ref())?;
        // The barrier iterator is created later but must use the same captured view.
        queue.write_batch(|w| {
            w.append(b"a", delete(true), vec![])?;
            w.register(b"c", 25, source())?;
            w.advance_epoch(12, source())
        })?;
        let a = rows.next().expect("first row exists")?;
        assert_eq!(a.0, b"a");
        assert_eq!(a.1.commands().len(), 1);
        assert_eq!(rows.next().expect("second row exists")?.0, b"b");
        assert!(rows.next().is_none());
        assert_eq!(
            queue
                .barriers(snapshot.as_ref())?
                .map(|row| row.map(|(revision, _)| revision))
                .collect::<Result<Vec<_>>>()?,
            vec![Revision(255), Revision(256)]
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
        Ok::<_, Error>(())
    })?;
    let queue = &storage.strata_queue;
    let snapshot = queue.durable_snapshot()?;
    let rows = queue
        .blobs(snapshot.as_ref())?
        .collect::<Result<Vec<_>>>()?;
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
                .map(|c| c.revision)
                .collect::<Vec<_>>(),
            vec![Revision(1), Revision(3)]
        );
        assert_eq!(
            queue
                .barriers(snapshot.as_ref())?
                .collect::<Result<Vec<_>>>()?[0]
                .0,
            Revision(5)
        );
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
                stage_progress(w, 42)?;
                w.register(b"a", 15, source())
            })
            .is_err()
    );
    assert_eq!(progress.get(&())?, None);
    let queue = &storage.strata_queue;
    let snapshot = queue.durable_snapshot()?;
    assert!(
        queue
            .blobs(snapshot.as_ref())?
            .collect::<Result<Vec<_>>>()?
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

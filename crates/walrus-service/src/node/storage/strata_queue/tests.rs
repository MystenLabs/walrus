// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{path::Path, time::Duration};

use sui_types::digests::TransactionDigest;
use tempfile::TempDir;
use typed_store::rocks::MetricConf;
use walrus_test_utils::Result as TestResult;
use walrus_utils::metrics::Registry;

use super::*;
use crate::node::storage::{DatabaseConfig, Storage};

fn open(path: &Path, optimistic: bool) -> anyhow::Result<Storage> {
    let mut config = DatabaseConfig::default_for_test();
    config.global.use_optimistic_transaction_db = optimistic;
    Storage::open(path, config, MetricConf::default(), Registry::default())
}

fn event_progress(storage: &Storage) -> Result<DBMap<(), u64>, TypedStoreError> {
    DBMap::reopen(
        &storage.database,
        Some(constants::event_index_cf_name()),
        &ReadWriteOptions::default(),
        false,
    )
}

fn registration() -> StrataQueueEntry {
    StrataQueueEntry::V1(StrataQueueEntryV1 {
        source_event: Some(StrataSourceEvent {
            event_index: 42,
            event_id: EventID {
                tx_digest: TransactionDigest::new([7; 32]),
                event_seq: 3,
            },
        }),
        operation: StrataOperation::RegisterBlob {
            blob_id: BlobId([9; 32]),
            end_epoch: 12,
        },
    })
}

fn advance_epoch(epoch: Epoch) -> StrataQueueEntry {
    StrataQueueEntry::V1(StrataQueueEntryV1 {
        source_event: None,
        operation: StrataOperation::AdvanceEpoch { epoch },
    })
}

fn aborted() -> TypedStoreError {
    TypedStoreError::TaskError("abort before commit".to_owned())
}

fn stage_progress(
    write: &StrataQueueTransaction<'_>,
    progress: &DBMap<(), u64>,
    value: u64,
) -> Result<(), TypedStoreError> {
    write
        .metadata()
        .put_cf(
            &progress.cf()?,
            be_fix_int_ser(&())?,
            bcs::to_bytes(&value).map_err(typed_store_err_from_bcs_err)?,
        )
        .map_err(typed_store_err_from_rocks_err)
}

#[tokio::test]
async fn batch_commits_metadata_and_commands_together_on_both_engines() -> TestResult {
    for optimistic in [false, true] {
        let directory = TempDir::new()?;
        let storage = open(directory.path(), optimistic)?;
        let progress = event_progress(&storage)?;
        let queue = &storage.strata_queue;
        assert_eq!(queue.last_committed_sequence()?, StrataQueueSequence(0));

        let dependency = queue.write_batch(|write| {
            let sequence = write.enqueue(&registration())?;
            write.enqueue(&advance_epoch(10))?;
            write.metadata().insert_batch(&progress, [((), 42)])?;
            Ok(sequence)
        })?;

        assert_eq!(dependency, StrataQueueSequence(1));
        assert_eq!(progress.get(&())?, Some(42));
        assert_eq!(queue.last_committed_sequence()?, StrataQueueSequence(2));
        assert_eq!(
            queue.scan(None, StrataQueueSequence(2), 10)?,
            vec![
                (StrataQueueSequence(1), registration()),
                (StrataQueueSequence(2), advance_epoch(10)),
            ]
        );
    }
    Ok(())
}

#[tokio::test]
async fn aborted_batch_discards_metadata_commands_and_sequence_allocation() -> TestResult {
    for optimistic in [false, true] {
        let directory = TempDir::new()?;
        let storage = open(directory.path(), optimistic)?;
        let progress = event_progress(&storage)?;
        let queue = &storage.strata_queue;

        let result: Result<(), _> = queue.write_batch(|write| {
            write.metadata().insert_batch(&progress, [((), 42)])?;
            write.enqueue(&registration())?;
            Err(aborted())
        });
        assert_eq!(result, Err(aborted()));
        assert_eq!(progress.get(&())?, None);
        assert_eq!(queue.last_committed_sequence()?, StrataQueueSequence(0));
        assert!(queue.scan(None, StrataQueueSequence(10), 10)?.is_empty());
        assert_eq!(
            queue.write_batch(|write| write.enqueue(&registration()))?,
            StrataQueueSequence(1)
        );
    }
    Ok(())
}

#[tokio::test]
async fn transaction_abort_and_conflict_do_not_leave_queue_records() -> TestResult {
    let directory = TempDir::new()?;
    let storage = open(directory.path(), true)?;
    let progress = event_progress(&storage)?;
    let queue = &storage.strata_queue;

    let result: Result<(), _> = queue.write_transaction(|write| {
        stage_progress(write, &progress, 42)?;
        write.enqueue(&registration())?;
        Err(aborted())
    });
    assert_eq!(result, Err(aborted()));
    assert_eq!(progress.get(&())?, None);
    assert_eq!(queue.last_committed_sequence()?, StrataQueueSequence(0));
    assert!(queue.scan(None, StrataQueueSequence(10), 10)?.is_empty());

    progress.insert(&(), &10)?;
    let result = queue.write_transaction(|write| {
        write
            .metadata()
            .get_for_update_cf(&progress.cf()?, be_fix_int_ser(&())?, false)
            .map_err(typed_store_err_from_rocks_err)?;
        stage_progress(write, &progress, 42)?;
        let sequence = write.enqueue(&registration())?;
        // An independent metadata writer wins after the transaction's read.
        progress.insert(&(), &11)?;
        Ok(sequence)
    });
    assert_eq!(result, Err(TypedStoreError::RetryableTransactionError));
    assert_eq!(progress.get(&())?, Some(11));
    assert_eq!(queue.last_committed_sequence()?, StrataQueueSequence(0));
    assert!(queue.scan(None, StrataQueueSequence(10), 10)?.is_empty());

    let sequence = queue.write_transaction(|write| {
        stage_progress(write, &progress, 42)?;
        write.enqueue(&registration())
    })?;
    assert_eq!(sequence, StrataQueueSequence(1));
    assert_eq!(progress.get(&())?, Some(42));
    assert_eq!(
        queue.scan(None, sequence, 10)?,
        vec![(sequence, registration())]
    );
    Ok(())
}

#[tokio::test]
async fn concurrent_batch_and_transaction_producers_share_one_sequence() -> TestResult {
    let directory = TempDir::new()?;
    let storage = open(directory.path(), true)?;
    let progress = event_progress(&storage)?;

    std::thread::scope(|scope| {
        let mut threads = Vec::new();
        for producer in 0..4 {
            let queue = storage.strata_queue.clone();
            let progress = progress.clone();
            threads.push(scope.spawn(move || -> Result<(), TypedStoreError> {
                for epoch in 1..=16 {
                    if producer % 2 == 0 {
                        queue.write_batch(|write| {
                            let sequence = write.enqueue(&advance_epoch(epoch))?;
                            write
                                .metadata()
                                .insert_batch(&progress, [((), sequence.0)])?;
                            Ok(())
                        })?;
                    } else {
                        queue.write_transaction(|write| {
                            let sequence = write.enqueue(&advance_epoch(epoch))?;
                            stage_progress(write, &progress, sequence.0)
                        })?;
                    }
                }
                Ok(())
            }));
        }
        for thread in threads {
            thread.join().expect("producer must not panic")?;
        }
        Ok::<_, TypedStoreError>(())
    })?;

    let tail = storage.strata_queue.last_committed_sequence()?;
    assert_eq!(tail, StrataQueueSequence(64));
    assert_eq!(progress.get(&())?, Some(64));
    assert_eq!(
        storage
            .strata_queue
            .scan(None, tail, 100)?
            .into_iter()
            .map(|(sequence, _)| sequence.0)
            .collect::<Vec<_>>(),
        (1..=64).collect::<Vec<_>>()
    );
    Ok(())
}

#[tokio::test]
async fn scans_are_numeric_bounded_and_exclude_newer_commits() -> TestResult {
    let directory = TempDir::new()?;
    let storage = open(directory.path(), true)?;
    let queue = &storage.strata_queue;
    queue.last_sequence.insert(&(), &StrataQueueSequence(254))?;
    queue.write_batch(|write| {
        write.enqueue(&advance_epoch(1))?;
        write.enqueue(&advance_epoch(2))
    })?;
    let captured = queue.last_committed_sequence()?;
    queue.write_transaction(|write| write.enqueue(&advance_epoch(3)))?;
    let first = queue.scan(None, captured, 1)?;
    assert_eq!(first, vec![(StrataQueueSequence(255), advance_epoch(1))]);
    assert_eq!(
        queue.scan(Some(first[0].0), captured, 10)?,
        vec![(StrataQueueSequence(256), advance_epoch(2))]
    );
    assert!(queue.scan(Some(captured), captured, 10)?.is_empty());
    assert!(queue.scan(None, captured, 0)?.is_empty());
    Ok(())
}

#[tokio::test]
async fn reopen_preserves_commands_and_never_reuses_cleaned_up_sequences() -> TestResult {
    for optimistic in [false, true] {
        let directory = TempDir::new()?;
        let storage = open(directory.path(), optimistic)?;
        let progress = event_progress(&storage)?;
        storage.strata_queue.write_batch(|write| {
            write.enqueue(&registration())?;
            write.enqueue(&advance_epoch(10))?;
            write.metadata().insert_batch(&progress, [((), 42)])?;
            Ok(())
        })?;
        // Model future worker cleanup. The allocation high-water mark must outlive queue rows.
        let mut cleanup = storage.strata_queue.entries.batch();
        cleanup.delete_batch(&storage.strata_queue.entries, [StrataQueueSequence(1)])?;
        cleanup.write_with_sync(true)?;
        drop(progress);
        let database = Arc::downgrade(&storage.database);
        drop(storage);
        tokio::time::timeout(Duration::from_secs(5), async {
            while database.strong_count() > 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;

        let storage = open(directory.path(), optimistic)?;
        assert_eq!(event_progress(&storage)?.get(&())?, Some(42));
        let queue = &storage.strata_queue;
        assert_eq!(queue.last_committed_sequence()?, StrataQueueSequence(2));
        assert_eq!(
            queue.scan(None, StrataQueueSequence(2), 10)?,
            vec![(StrataQueueSequence(2), advance_epoch(10))]
        );
        assert_eq!(
            queue.write_batch(|write| write.enqueue(&registration()))?,
            StrataQueueSequence(3)
        );
    }
    Ok(())
}

#[tokio::test]
async fn sequence_exhaustion_does_not_commit_metadata() -> TestResult {
    let directory = TempDir::new()?;
    let storage = open(directory.path(), true)?;
    let progress = event_progress(&storage)?;
    let queue = &storage.strata_queue;
    queue
        .last_sequence
        .insert(&(), &StrataQueueSequence(u64::MAX))?;
    assert!(
        queue
            .write_batch(|write| {
                write.metadata().insert_batch(&progress, [((), 42)])?;
                write.enqueue(&registration())
            })
            .is_err()
    );
    assert!(
        queue
            .write_transaction(|write| {
                stage_progress(write, &progress, 42)?;
                write.enqueue(&registration())
            })
            .is_err()
    );
    assert_eq!(progress.get(&())?, None);
    assert!(
        queue
            .scan(None, StrataQueueSequence(u64::MAX), 10)?
            .is_empty()
    );
    Ok(())
}

// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::path::Path;

use sui_types::base_types::ObjectID;
use tempfile::TempDir;
use typed_store::rocks::MetricConf;
use walrus_sui::{
    test_utils::EventForTesting,
    types::{BlobCertified, BlobRegistered, PooledBlobRegistered, StoragePoolEvent},
};

use super::{super::SliverStoreBackend, *};
use crate::node::storage::{DatabaseConfig, SliverStoreBackendKind, Storage, tests::get_sliver};

type TestResult = anyhow::Result<()>;
const X: BlobId = BlobId([71; 32]);
const Y: BlobId = BlobId([72; 32]);
const SHARD: ShardIndex = ShardIndex(7);
const POOL: ObjectID = ObjectID::from_single_byte(91);

fn open(path: &Path, optimistic: bool) -> anyhow::Result<Storage> {
    typed_store::DBMetrics::get();
    let mut config = DatabaseConfig::default_for_test();
    config.global.use_optimistic_transaction_db = optimistic;
    let storage = Storage::open_with_sliver_backend(
        path,
        config,
        MetricConf::default(),
        Registry::default(),
        SliverStoreBackendKind::Strata,
    )?;
    storage.set_node_status(crate::node::storage::NodeStatus::Active)?;
    Ok(storage)
}

fn strata(storage: &Storage) -> &StrataSliverStore {
    let SliverStoreBackend::Strata(store) = storage.sliver_store.backend.as_ref() else {
        panic!("Strata backend expected");
    };
    store
}

async fn run_until(storage: &Storage, epoch: Epoch, delete_data: bool) -> TestResult {
    tokio::select! {
        result = storage.run_strata_reconciliation(delete_data) => result,
        result = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if matches!(strata(storage).lifecycle.progress.get(&())?,
                    Some(EpochProgress::Complete(done)) if done >= epoch) {
                    // Complete is visible just before the final clock transition releases
                    // its guard. Wait for that transition before cancelling the worker.
                    let _guard = strata(storage).lifecycle.lock_lifecycle().await?;
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }) => result?,
    }
}

async fn reconcile(
    storage: &Storage,
    epoch: Epoch,
    current: Epoch,
    delete_data: bool,
) -> TestResult {
    storage
        .request_strata_reconciliation(epoch, current)
        .await?;
    if epoch == current {
        // These tests drive passes directly; worker tests use run_until once per opened store.
        strata(storage).reconcile_epoch(epoch, delete_data).await?;
    }
    Ok(())
}

async fn close(storage: Storage) -> TestResult {
    let weak = Arc::downgrade(&storage.database);
    drop(storage);
    tokio::time::timeout(Duration::from_secs(5), async {
        while weak.strong_count() > 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    Ok(())
}

fn registration(id: BlobId, end_epoch: Epoch) -> BlobRegistered {
    BlobRegistered {
        end_epoch,
        deletable: true,
        ..BlobRegistered::for_testing_with_random_object_id(id)
    }
}

async fn put(storage: &Storage, id: BlobId) -> TestResult {
    let shard = storage
        .shard_storage(SHARD)
        .await
        .expect("test shard exists");
    for axis in [SliverType::Primary, SliverType::Secondary] {
        assert!(
            shard
                .put_registered_sliver(id, get_sliver(axis, 2), 1)
                .await?
        );
    }
    Ok(())
}

fn stored(storage: &Storage, id: BlobId) -> anyhow::Result<bool> {
    strata(storage)
        .contains_sliver_pairs_in_all(id, &[SHARD])
        .map_err(Into::into)
}

#[tokio::test]
async fn explicit_delete_is_deferred_and_survives_restart() -> TestResult {
    for optimistic in [false, true] {
        let dir = TempDir::new()?;
        let storage = open(dir.path(), optimistic)?;
        storage
            .create_storage_for_shards_for_testing(&[SHARD])
            .await?;
        let registered = registration(X, 20);
        storage
            .update_blob_info_coordinated(0, &registered.clone().into())
            .await?;
        put(&storage, X).await?;
        storage
            .update_blob_info_coordinated(
                1,
                &registered
                    .into_corresponding_deleted_event_for_testing(false)
                    .into(),
            )
            .await?;
        assert!(stored(&storage, X)?);
        assert!(strata(&storage).lifecycle.blobs.get(&X)?.unwrap().delete);
        // The event can complete before a worker runs; the durable request must survive restart.
        storage.request_strata_reconciliation(2, 2).await?;
        close(storage).await?;
        let storage = open(dir.path(), optimistic)?;
        run_until(&storage, 2, true).await?;
        assert!(!stored(&storage, X)?);
        assert!(strata(&storage).lifecycle.blobs.get(&X)?.is_none());
        assert!(
            strata(&storage)
                .store
                .index()
                .submitted_batch_lsns()
                .is_empty()?
        );
        close(storage).await?;
    }
    Ok(())
}

#[tokio::test]
async fn put_cancels_delete_but_preserves_lifetime_reconciliation() -> TestResult {
    for optimistic in [false, true] {
        let dir = TempDir::new()?;
        let storage = open(dir.path(), optimistic)?;
        storage
            .create_storage_for_shards_for_testing(&[SHARD])
            .await?;
        let long = registration(X, 80);
        let short = registration(X, 30);
        storage
            .update_blob_info_coordinated(0, &long.clone().into())
            .await?;
        storage
            .update_blob_info_coordinated(1, &short.into())
            .await?;
        put(&storage, X).await?;
        storage
            .update_blob_info_coordinated(
                2,
                &long
                    .into_corresponding_deleted_event_for_testing(false)
                    .into(),
            )
            .await?;
        assert!(strata(&storage).lifecycle.blobs.get(&X)?.unwrap().delete);
        put(&storage, X).await?;
        let pending = strata(&storage).lifecycle.blobs.get(&X)?.unwrap();
        assert!(!pending.delete);
        assert_eq!(pending.event_index, 2);
        reconcile(&storage, 2, 2, true).await?;
        assert_eq!(strata(&storage).lifecycle.lifetimes.get(&X)?, Some(30));
        assert!(stored(&storage, X)?);
        reconcile(&storage, 30, 30, true).await?;
        assert!(!stored(&storage, X)?);
        close(storage).await?;
    }
    Ok(())
}

#[tokio::test]
async fn stale_snapshot_cannot_delete_a_new_registration_and_put() -> TestResult {
    for optimistic in [false, true] {
        let dir = TempDir::new()?;
        let storage = open(dir.path(), optimistic)?;
        storage
            .create_storage_for_shards_for_testing(&[SHARD])
            .await?;
        let old = registration(X, 20);
        storage
            .update_blob_info_coordinated(0, &old.clone().into())
            .await?;
        put(&storage, X).await?;
        storage
            .update_blob_info_coordinated(
                1,
                &old.into_corresponding_deleted_event_for_testing(false)
                    .into(),
            )
            .await?;
        let snapshot = storage.blob_info.reconciliation_candidates()?.blobs;
        // Registration and payload both happen after the worker's snapshot.
        storage
            .update_blob_info_coordinated(2, &registration(X, 40).into())
            .await?;
        put(&storage, X).await?;
        strata(&storage).reconcile_blobs(2, &snapshot, true).await?;
        assert!(stored(&storage, X)?);
        assert_eq!(
            strata(&storage)
                .lifecycle
                .blobs
                .get(&X)?
                .unwrap()
                .event_index,
            2
        );
        reconcile(&storage, 2, 2, true).await?;
        assert!(stored(&storage, X)?);
        close(storage).await?;
    }
    Ok(())
}

#[tokio::test]
async fn stale_put_cannot_recreate_data_without_a_live_reference() -> TestResult {
    let dir = TempDir::new()?;
    let storage = open(dir.path(), true)?;
    storage
        .create_storage_for_shards_for_testing(&[SHARD])
        .await?;
    let registered = registration(X, 20);
    storage
        .update_blob_info_coordinated(0, &registered.clone().into())
        .await?;
    let guard = strata(&storage).lifecycle.lock_blobs(&[&X.0]).await?;
    let shard = storage.shard_storage(SHARD).await.unwrap();
    let writer = tokio::spawn({
        let shard = shard.clone();
        async move {
            shard
                .put_registered_sliver(X, get_sliver(SliverType::Primary, 2), 1)
                .await
        }
    });
    // Already hold the coordination lock; use the synchronous metadata helper.
    storage.update_blob_info(
        1,
        &registered
            .into_corresponding_deleted_event_for_testing(false)
            .into(),
    )?;
    drop(guard);
    assert!(!writer.await??);
    drop(shard);
    close(storage).await?;
    Ok(())
}

#[tokio::test]
async fn a_blocked_blob_does_not_block_an_unrelated_put() -> TestResult {
    let dir = TempDir::new()?;
    let storage = open(dir.path(), true)?;
    storage
        .create_storage_for_shards_for_testing(&[SHARD])
        .await?;
    storage
        .update_blob_info_coordinated(0, &registration(X, 20).into())
        .await?;
    storage
        .update_blob_info_coordinated(1, &registration(Y, 30).into())
        .await?;
    let snapshot = storage.blob_info.reconciliation_candidates()?.blobs;
    let only_x: Vec<_> = snapshot.into_iter().filter(|blob| blob.id == X).collect();
    let guard = strata(&storage).lifecycle.lock_blobs(&[&X.0]).await?;
    let worker = tokio::spawn({
        let backend = strata(&storage).clone();
        async move { backend.reconcile_blobs(2, &only_x, true).await }
    });
    tokio::time::timeout(Duration::from_secs(5), put(&storage, Y)).await??;
    assert!(!worker.is_finished());
    drop(guard);
    worker.await??;
    close(storage).await?;
    Ok(())
}

#[tokio::test]
async fn pool_updates_coalesce_and_other_references_keep_their_lifetime() -> TestResult {
    for optimistic in [false, true] {
        let dir = TempDir::new()?;
        let storage = open(dir.path(), optimistic)?;
        storage
            .create_storage_for_shards_for_testing(&[SHARD])
            .await?;
        storage
            .update_storage_pool_info_coordinated(
                0,
                &StoragePoolEvent::created_for_testing(POOL, 1, 20),
            )
            .await?;
        let member = PooledBlobRegistered {
            storage_pool_id: POOL,
            ..PooledBlobRegistered::for_testing_with_random_object_id(X)
        };
        storage
            .update_blob_info_coordinated(
                1,
                &walrus_sui::types::BlobEvent::PooledBlobRegistered(member),
            )
            .await?;
        storage
            .update_blob_info_coordinated(2, &registration(X, 80).into())
            .await?;
        put(&storage, X).await?;
        reconcile(&storage, 2, 2, true).await?;
        storage
            .update_storage_pool_info_coordinated(
                3,
                &StoragePoolEvent::extended_for_testing(POOL, 30),
            )
            .await?;
        storage
            .update_storage_pool_info_coordinated(
                4,
                &StoragePoolEvent::extended_for_testing(POOL, 50),
            )
            .await?;
        assert_eq!(strata(&storage).lifecycle.pools.safe_iter()?.count(), 1);
        assert!(strata(&storage).lifecycle.blobs.is_empty()); // No foreground member fan-out.
        reconcile(&storage, 20, 20, true).await?;
        assert_eq!(strata(&storage).lifecycle.lifetimes.get(&X)?, Some(80));
        assert!(stored(&storage, X)?);
        assert!(strata(&storage).lifecycle.pools.is_empty());
        reconcile(&storage, 80, 80, true).await?;
        assert!(!stored(&storage, X)?);
        close(storage).await?;
    }
    Ok(())
}

#[tokio::test]
async fn extension_is_applied_before_the_old_expiry_and_restart() -> TestResult {
    for optimistic in [false, true] {
        let dir = TempDir::new()?;
        let storage = open(dir.path(), optimistic)?;
        storage
            .create_storage_for_shards_for_testing(&[SHARD])
            .await?;
        let registered = registration(X, 3);
        storage
            .update_blob_info_coordinated(0, &registered.clone().into())
            .await?;
        storage
            .update_blob_info_coordinated(
                1,
                &registered
                    .clone()
                    .into_corresponding_certified_event_for_testing()
                    .into(),
            )
            .await?;
        put(&storage, X).await?;
        let extension = BlobCertified {
            is_extension: true,
            end_epoch: 20,
            ..registered.into_corresponding_certified_event_for_testing()
        };
        storage
            .update_blob_info_coordinated(2, &extension.into())
            .await?;
        strata(&storage).lifecycle.db.flush_wal(true)?;
        close(storage).await?;
        let storage = open(dir.path(), optimistic)?;
        reconcile(&storage, 3, 3, true).await?;
        assert!(stored(&storage, X)?);
        reconcile(&storage, 20, 20, true).await?;
        assert!(!stored(&storage, X)?);
        close(storage).await?;
    }
    Ok(())
}

#[tokio::test]
async fn restart_after_durable_delete_does_not_repeat_it_after_a_new_put() -> TestResult {
    let dir = TempDir::new()?;
    let storage = open(dir.path(), true)?;
    storage
        .create_storage_for_shards_for_testing(&[SHARD])
        .await?;
    let old = registration(X, 20);
    storage
        .update_blob_info_coordinated(0, &old.clone().into())
        .await?;
    put(&storage, X).await?;
    storage
        .update_blob_info_coordinated(
            1,
            &old.into_corresponding_deleted_event_for_testing(false)
                .into(),
        )
        .await?;
    let pending = strata(&storage).lifecycle.blobs.get(&X)?.unwrap();
    let mut start = strata(&storage).lifecycle.progress.batch();
    start.insert_batch(
        &strata(&storage).lifecycle.progress,
        [((), EpochProgress::Applying(2))],
    )?;
    start.write_with_sync(true)?;
    let snapshot = storage.blob_info.reconciliation_candidates()?.blobs;
    strata(&storage).reconcile_blobs(2, &snapshot, true).await?;
    assert!(!stored(&storage, X)?);
    // Model a crash after Strata sync but before dirty-marker acknowledgement.
    let mut restore = strata(&storage).lifecycle.blobs.batch();
    restore.insert_batch(&strata(&storage).lifecycle.blobs, [(X, pending)])?;
    restore.write_with_sync(true)?;
    close(storage).await?;
    let storage = open(dir.path(), true)?;
    storage
        .update_blob_info_coordinated(2, &registration(X, 40).into())
        .await?;
    put(&storage, X).await?;
    run_until(&storage, 2, true).await?;
    assert!(stored(&storage, X)?);
    assert_eq!(strata(&storage).store.current_epoch()?, 2);
    assert!(
        strata(&storage)
            .store
            .index()
            .submitted_batch_lsns()
            .is_empty()?
    );
    close(storage).await?;
    Ok(())
}

#[tokio::test]
async fn restart_after_epoch_sync_does_not_advance_twice() -> TestResult {
    let dir = TempDir::new()?;
    let storage = open(dir.path(), true)?;
    let mut progress = strata(&storage).lifecycle.progress.batch();
    progress.insert_batch(
        &strata(&storage).lifecycle.progress,
        [((), EpochProgress::Advancing(11))],
    )?;
    progress.write_with_sync(true)?;
    let mut batch = strata(&storage).store.batch();
    batch.advance_epoch_to(11);
    batch.write()?;
    strata(&storage).store.sync()?;
    close(storage).await?;
    let storage = open(dir.path(), true)?;
    storage.set_node_status(crate::node::storage::NodeStatus::RecoveryCatchUp)?;
    // A new request must not replace the interrupted pass. The worker finishes it first.
    storage.request_strata_reconciliation(12, 12).await?;
    assert_eq!(strata(&storage).store.current_epoch()?, 11);
    assert_eq!(
        strata(&storage).lifecycle.progress.get(&())?,
        Some(EpochProgress::Advancing(11))
    );
    run_until(&storage, 12, true).await?;
    assert_eq!(strata(&storage).store.current_epoch()?, 12);
    assert_eq!(
        strata(&storage).lifecycle.progress.get(&())?,
        Some(EpochProgress::Complete(12))
    );
    close(storage).await?;
    Ok(())
}

#[tokio::test]
async fn disabled_data_deletion_preserves_pending_deletes_and_expiry() -> TestResult {
    let dir = TempDir::new()?;
    let storage = open(dir.path(), true)?;
    storage
        .create_storage_for_shards_for_testing(&[SHARD])
        .await?;
    let registered = registration(X, 3);
    storage
        .update_blob_info_coordinated(0, &registered.clone().into())
        .await?;
    put(&storage, X).await?;
    storage
        .update_blob_info_coordinated(
            1,
            &registered
                .into_corresponding_deleted_event_for_testing(false)
                .into(),
        )
        .await?;
    reconcile(&storage, 3, 3, false).await?;
    assert!(stored(&storage, X)?);
    assert!(strata(&storage).lifecycle.blobs.get(&X)?.unwrap().delete);
    reconcile(&storage, 4, 4, true).await?;
    assert!(!stored(&storage, X)?);
    close(storage).await?;
    Ok(())
}

#[tokio::test]
async fn put_after_restart_cannot_restore_a_stale_pool_lifetime() -> TestResult {
    let dir = TempDir::new()?;
    let storage = open(dir.path(), true)?;
    storage
        .create_storage_for_shards_for_testing(&[SHARD])
        .await?;
    storage
        .update_storage_pool_info_coordinated(0, &StoragePoolEvent::created_for_testing(POOL, 1, 3))
        .await?;
    let member = PooledBlobRegistered {
        storage_pool_id: POOL,
        ..PooledBlobRegistered::for_testing_with_random_object_id(X)
    };
    storage
        .update_blob_info_coordinated(
            1,
            &walrus_sui::types::BlobEvent::PooledBlobRegistered(member),
        )
        .await?;
    put(&storage, X).await?;
    reconcile(&storage, 2, 2, true).await?;
    storage
        .update_storage_pool_info_coordinated(2, &StoragePoolEvent::extended_for_testing(POOL, 20))
        .await?;
    let mut start = strata(&storage).lifecycle.progress.batch();
    start.insert_batch(
        &strata(&storage).lifecycle.progress,
        [((), EpochProgress::Applying(3))],
    )?;
    start.write_with_sync(true)?;
    let snapshot = storage.blob_info.reconciliation_candidates()?.blobs;
    strata(&storage).reconcile_blobs(3, &snapshot, true).await?;
    // The pool remains dirty, so recovery will discover the same extension and its saved LSN.
    assert_eq!(strata(&storage).lifecycle.lifetimes.get(&X)?, Some(20));
    close(storage).await?;
    let storage = open(dir.path(), true)?;
    put(&storage, X).await?;
    run_until(&storage, 3, true).await?;
    assert!(stored(&storage, X)?);
    reconcile(&storage, 20, 20, true).await?;
    assert!(!stored(&storage, X)?);
    close(storage).await?;
    Ok(())
}

#[tokio::test]
async fn catch_up_allows_recovery_puts_before_reconciliation() -> TestResult {
    use crate::node::storage::NodeStatus;

    for optimistic in [false, true] {
        let dir = TempDir::new()?;
        let storage = open(dir.path(), optimistic)?;
        storage.set_node_status(NodeStatus::RecoveryCatchUp)?;
        storage
            .update_storage_pool_info_coordinated(
                0,
                &StoragePoolEvent::created_for_testing(POOL, 1, 20),
            )
            .await?;
        storage
            .update_blob_info_coordinated(
                1,
                &walrus_sui::types::BlobEvent::PooledBlobRegistered(PooledBlobRegistered {
                    storage_pool_id: POOL,
                    ..PooledBlobRegistered::for_testing_with_random_object_id(X)
                }),
            )
            .await?;
        reconcile(&storage, 10, 100, true).await?;
        storage
            .update_storage_pool_info_coordinated(
                2,
                &StoragePoolEvent::extended_for_testing(POOL, 60),
            )
            .await?;
        reconcile(&storage, 20, 100, true).await?;
        storage
            .update_storage_pool_info_coordinated(
                3,
                &StoragePoolEvent::extended_for_testing(POOL, 120),
            )
            .await?;
        let retired = registration(Y, 120);
        storage
            .update_blob_info_coordinated(4, &retired.clone().into())
            .await?;
        storage
            .update_blob_info_coordinated(
                5,
                &retired
                    .into_corresponding_deleted_event_for_testing(false)
                    .into(),
            )
            .await?;
        reconcile(&storage, 60, 100, true).await?;
        assert_eq!(strata(&storage).store.current_epoch()?, 0);
        assert_eq!(strata(&storage).lifecycle.lifetimes.get(&X)?, Some(20));
        assert!(strata(&storage).lifecycle.blobs.get(&Y)?.unwrap().delete);
        assert!(strata(&storage).lifecycle.progress.get(&())?.is_none());

        // The boundary only requests work. Recovery can create shards and write slivers
        // before the worker runs, even though the cached pool lifetime is still historical.
        storage.request_strata_reconciliation(100, 100).await?;
        storage.set_node_status(NodeStatus::RecoveryInProgress(100))?;
        storage
            .create_storage_for_shards_for_testing(&[SHARD])
            .await?;
        assert!(!stored(&storage, X)?);
        let shard = storage.shard_storage(SHARD).await.unwrap();
        for axis in [SliverType::Primary, SliverType::Secondary] {
            assert!(
                shard
                    .put_registered_sliver(X, get_sliver(axis, 2), 100)
                    .await?
            );
        }
        drop(shard);
        assert!(stored(&storage, X)?);
        assert_eq!(strata(&storage).store.current_epoch()?, 0);
        assert_eq!(strata(&storage).lifecycle.lifetimes.get(&X)?, Some(120));
        run_until(&storage, 100, true).await?;
        assert_eq!(strata(&storage).store.current_epoch()?, 100);
        assert!(stored(&storage, X)?);
        assert!(strata(&storage).lifecycle.lifetimes.get(&Y)?.is_none());
        assert!(strata(&storage).lifecycle.blobs.is_empty());
        assert!(strata(&storage).lifecycle.pools.is_empty());

        // Reference state keeps advancing while either kind of payload recovery is running.
        for (epoch, status) in [
            (101, NodeStatus::RecoveryInProgress(101)),
            (102, NodeStatus::RecoverMetadata),
        ] {
            storage.set_node_status(status)?;
            reconcile(&storage, epoch, epoch, true).await?;
            assert_eq!(strata(&storage).store.current_epoch()?, u64::from(epoch));
            assert!(stored(&storage, X)?);
        }
        reconcile(&storage, 120, 120, true).await?;
        assert!(!stored(&storage, X)?);
        close(storage).await?;
    }
    Ok(())
}

#[tokio::test]
async fn expiry_keeps_dirty_ids_after_reference_rows_are_removed() -> TestResult {
    for optimistic in [false, true] {
        let dir = TempDir::new()?;
        let storage = open(dir.path(), optimistic)?;
        storage
            .create_storage_for_shards_for_testing(&[SHARD])
            .await?;
        storage
            .update_blob_info_coordinated(0, &registration(X, 3).into())
            .await?;
        storage
            .update_storage_pool_info_coordinated(
                1,
                &StoragePoolEvent::created_for_testing(POOL, 1, 3),
            )
            .await?;
        storage
            .update_blob_info_coordinated(
                2,
                &walrus_sui::types::BlobEvent::PooledBlobRegistered(PooledBlobRegistered {
                    storage_pool_id: POOL,
                    ..PooledBlobRegistered::for_testing_with_random_object_id(Y)
                }),
            )
            .await?;
        put(&storage, X).await?;
        put(&storage, Y).await?;
        reconcile(&storage, 2, 2, true).await?;
        assert!(strata(&storage).lifecycle.blobs.is_empty());
        let metrics = crate::node::metrics::NodeMetricSet::new(&Registry::default());
        storage
            .process_expired_storage_pools(3, &metrics, 100)
            .await?;
        storage
            .process_expired_blob_objects(3, &metrics, 100)
            .await?;
        let snapshot = storage.blob_info.reconciliation_candidates()?;
        assert_eq!(snapshot.blobs.len(), 2);
        assert!(snapshot.blobs.iter().all(|blob| blob.end_epoch == 0));
        assert!(strata(&storage).lifecycle.pool_references.is_empty());
        strata(&storage).lifecycle.db.flush_wal(true)?;
        close(storage).await?;
        let storage = open(dir.path(), optimistic)?;
        reconcile(&storage, 3, 3, true).await?;
        for id in [X, Y] {
            assert!(!stored(&storage, id)?);
            assert!(strata(&storage).lifecycle.blobs.get(&id)?.is_none());
            assert!(strata(&storage).lifecycle.lifetimes.get(&id)?.is_none());
        }
        close(storage).await?;
    }
    Ok(())
}

#[tokio::test]
async fn blocked_worker_does_not_block_requests_events_or_unrelated_puts() -> TestResult {
    for optimistic in [false, true] {
        let dir = TempDir::new()?;
        let storage = open(dir.path(), optimistic)?;
        storage
            .create_storage_for_shards_for_testing(&[SHARD])
            .await?;
        storage
            .update_blob_info_coordinated(0, &registration(X, 200).into())
            .await?;
        let guard = strata(&storage).lifecycle.lock_blobs(&[&X.0]).await?;
        storage.request_strata_reconciliation(100, 100).await?;
        let worker_storage = storage.clone();
        let worker = tokio::spawn(async move { run_until(&worker_storage, 102, true).await });
        tokio::time::timeout(Duration::from_secs(5), async {
            while strata(&storage).lifecycle.progress.get(&())?
                != Some(EpochProgress::Applying(100))
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await??;
        tokio::time::timeout(Duration::from_secs(5), async {
            storage.request_strata_reconciliation(101, 101).await?;
            storage.request_strata_reconciliation(102, 102).await?;
            storage
                .update_blob_info_coordinated(1, &registration(Y, 300).into())
                .await?;
            put(&storage, Y).await?;
            Ok::<_, anyhow::Error>(())
        })
        .await??;
        assert_eq!(strata(&storage).store.current_epoch()?, 0);
        assert_eq!(
            strata(&storage).lifecycle.progress.get(&())?,
            Some(EpochProgress::Applying(100))
        );
        assert_eq!(
            strata(&storage).lifecycle.requested_epoch.get(&())?,
            Some(102)
        );
        assert!(stored(&storage, Y)?);
        drop(guard);
        worker.await??;
        assert_eq!(strata(&storage).store.current_epoch()?, 102);
        assert!(stored(&storage, Y)?);
        close(storage).await?;
    }
    Ok(())
}

#[tokio::test]
async fn only_one_worker_can_start_until_the_store_is_reopened() -> TestResult {
    for optimistic in [false, true] {
        let dir = TempDir::new()?;
        let storage = open(dir.path(), optimistic)?;
        storage
            .update_blob_info_coordinated(0, &registration(X, 200).into())
            .await?;
        let guard = strata(&storage).lifecycle.lock_blobs(&[&X.0]).await?;
        storage.request_strata_reconciliation(100, 100).await?;
        let worker_storage = storage.clone();
        let worker =
            tokio::spawn(async move { worker_storage.run_strata_reconciliation(true).await });
        tokio::time::timeout(Duration::from_secs(5), async {
            while strata(&storage).lifecycle.progress.get(&())?
                != Some(EpochProgress::Applying(100))
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await??;

        let duplicate = tokio::time::timeout(
            Duration::from_secs(1),
            storage.run_strata_reconciliation(true),
        )
        .await?
        .expect_err("a second worker must be rejected, not wait for the first");
        assert!(duplicate.to_string().contains("worker already started"));

        // Cancelling the worker leaves its pass blocked on X. A replacement must not start.
        worker.abort();
        assert!(
            worker
                .await
                .expect_err("worker should be cancelled")
                .is_cancelled()
        );
        let duplicate = tokio::time::timeout(
            Duration::from_secs(1),
            storage.run_strata_reconciliation(true),
        )
        .await?
        .expect_err("cancellation must not release the worker's claim");
        assert!(duplicate.to_string().contains("worker already started"));
        drop(guard);

        // The detached pass can still finish safely after its worker is cancelled.
        tokio::time::timeout(Duration::from_secs(5), async {
            while strata(&storage).lifecycle.progress.get(&())?
                != Some(EpochProgress::Complete(100))
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let _guard = strata(&storage).lifecycle.lock_lifecycle().await?;
            Ok::<_, anyhow::Error>(())
        })
        .await??;
        close(storage).await?;

        let storage = open(dir.path(), optimistic)?;
        storage.request_strata_reconciliation(101, 101).await?;
        run_until(&storage, 101, true).await?;
        assert_eq!(strata(&storage).store.current_epoch()?, 101);
        close(storage).await?;
    }
    Ok(())
}

#[tokio::test]
async fn put_and_stale_scan_resolve_a_pending_pool_extension() -> TestResult {
    for optimistic in [false, true] {
        let dir = TempDir::new()?;
        let storage = open(dir.path(), optimistic)?;
        storage
            .create_storage_for_shards_for_testing(&[SHARD])
            .await?;
        storage
            .update_storage_pool_info_coordinated(
                0,
                &StoragePoolEvent::created_for_testing(POOL, 1, 3),
            )
            .await?;
        let registered = PooledBlobRegistered {
            storage_pool_id: POOL,
            ..PooledBlobRegistered::for_testing_with_random_object_id(X)
        };
        storage
            .update_blob_info_coordinated(
                1,
                &walrus_sui::types::BlobEvent::PooledBlobRegistered(registered.clone()),
            )
            .await?;
        let old_snapshot = storage.blob_info.reconciliation_candidates()?.blobs;
        storage
            .update_storage_pool_info_coordinated(
                2,
                &StoragePoolEvent::extended_for_testing(POOL, 20),
            )
            .await?;
        assert_eq!(strata(&storage).lifecycle.lifetimes.get(&X)?, Some(3));
        // Puts must not wait for the worker to expand this pool's membership.
        put(&storage, X).await?;
        assert_eq!(strata(&storage).lifecycle.lifetimes.get(&X)?, Some(20));
        strata(&storage)
            .reconcile_blobs(3, &old_snapshot, true)
            .await?;
        assert_eq!(strata(&storage).lifecycle.lifetimes.get(&X)?, Some(20));
        assert_eq!(strata(&storage).lifecycle.pools.get(&POOL)?, Some(2));
        reconcile(&storage, 3, 3, true).await?;
        assert!(stored(&storage, X)?);
        // Simulate opening an older store without the lookup. Initialization backfills it.
        let lifecycle = &strata(&storage).lifecycle;
        let mut batch = lifecycle.pool_references.batch();
        batch.delete_batch(&lifecycle.pool_references, [(X, registered.object_id)])?;
        batch.delete_batch(&lifecycle.requested_epoch, [()])?;
        batch.write_with_sync(true)?;
        close(storage).await?;
        let storage = open(dir.path(), optimistic)?;
        assert_eq!(
            strata(&storage)
                .lifecycle
                .pool_references
                .get(&(X, registered.object_id))?,
            Some(POOL)
        );
        storage
            .update_blob_info_coordinated(
                3,
                &walrus_sui::types::BlobEvent::PooledBlobDeleted(
                    walrus_sui::types::PooledBlobDeleted {
                        epoch: 3,
                        blob_id: X,
                        object_id: registered.object_id,
                        storage_pool_id: POOL,
                        was_certified: false,
                        event_id: registered.event_id,
                    },
                ),
            )
            .await?;
        assert!(
            strata(&storage)
                .lifecycle
                .pool_references
                .get(&(X, registered.object_id))?
                .is_none()
        );
        close(storage).await?;
    }
    Ok(())
}

#[tokio::test]
async fn later_delete_in_the_same_pass_does_not_reuse_an_old_binding() -> TestResult {
    let dir = TempDir::new()?;
    let storage = open(dir.path(), true)?;
    storage
        .create_storage_for_shards_for_testing(&[SHARD])
        .await?;
    let lifecycle = &strata(&storage).lifecycle;
    let mut start = lifecycle.progress.batch();
    start.insert_batch(&lifecycle.progress, [((), EpochProgress::Applying(2))])?;
    start.write_with_sync(true)?;
    for event_index in [0, 2] {
        let registered = registration(X, 20);
        storage
            .update_blob_info_coordinated(event_index, &registered.clone().into())
            .await?;
        put(&storage, X).await?;
        storage
            .update_blob_info_coordinated(
                event_index + 1,
                &registered
                    .into_corresponding_deleted_event_for_testing(false)
                    .into(),
            )
            .await?;
        let snapshot = storage.blob_info.reconciliation_candidates()?.blobs;
        strata(&storage).reconcile_blobs(2, &snapshot, true).await?;
        assert!(!stored(&storage, X)?);
    }
    assert_eq!(
        strata(&storage)
            .store
            .index()
            .submitted_batch_lsns()
            .safe_iter()?
            .count(),
        2
    );
    run_until(&storage, 2, true).await?;
    close(storage).await?;
    Ok(())
}

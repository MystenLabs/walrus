// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{path::Path, sync::Arc, time::Duration};

use strata::queue::{BlobOperation, ShardGeneration};
use strata_index::StrataIndex;
use tempfile::TempDir;
use typed_store::{Map, rocks::MetricConf};
use walrus_core::{BlobId, ShardIndex, SliverType};
use walrus_sui::{
    test_utils::EventForTesting,
    types::{
        BlobEvent,
        BlobRegistered,
        PooledBlobRegistered,
        StoragePoolCreatedEvent,
        StoragePoolEvent,
    },
};
use walrus_test_utils::Result as TestResult;
use walrus_utils::metrics::Registry;

use super::*;
use crate::node::storage::{DatabaseConfig, SliverStoreBackendKind, Storage, tests::get_sliver};

#[path = "extension_tests.rs"]
mod extensions;

#[path = "pool_extension_tests.rs"]
mod pool_extensions;

const BLOB: BlobId = BlobId([71; 32]);
const SHARD: ShardIndex = ShardIndex(7);

fn open(path: &Path, optimistic: bool) -> anyhow::Result<Storage> {
    typed_store::DBMetrics::get();
    let mut config = DatabaseConfig::default_for_test();
    config.global.use_optimistic_transaction_db = optimistic;
    Storage::open_with_sliver_backend(
        path,
        config,
        MetricConf::default(),
        Registry::default(),
        SliverStoreBackendKind::Strata,
    )
}

async fn close(storage: Storage) -> anyhow::Result<()> {
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

fn registration(end_epoch: Epoch) -> BlobRegistered {
    BlobRegistered {
        end_epoch,
        deletable: true,
        ..BlobRegistered::for_testing_with_random_object_id(BLOB)
    }
}

fn delete() -> BlobOperation {
    BlobOperation::Delete {
        shards: vec![ShardGeneration {
            shard: 7,
            generation: 0,
        }],
        cancellable: true,
    }
}

#[tokio::test]
async fn registration_cancels_delete_atomically_and_put_waits_for_both_sliver_lifetimes()
-> TestResult {
    for optimistic in [false, true] {
        let dir = TempDir::new()?;
        let storage = open(dir.path(), optimistic)?;
        storage
            .create_storage_for_shards_for_testing(&[SHARD])
            .await?;
        let first = registration(80);
        storage
            .update_blob_info_coordinated(0, &first.into())
            .await?;
        let worker = storage.sliver_store.strata_worker().unwrap();
        assert_eq!(worker.process_batch().await?.acknowledged_blobs, 1);
        storage
            .strata_queue
            .write_batch(|b| b.append(&BLOB.0, 1, delete(), vec![]))?;
        let next = registration(50);
        storage
            .update_blob_info_coordinated(2, &next.clone().into())
            .await?;
        storage
            .update_blob_info_coordinated(2, &next.into())
            .await?; // Replay cannot enqueue twice.
        let record = storage.blob_info.strata_registration(&BLOB)?.unwrap();
        assert_eq!(record.event_index, 2);
        assert_eq!(record.end_epoch, 80); // A shorter second reference cannot shorten the first.
        {
            let guard = storage.strata_queue.lock_blobs(&[&BLOB.0]).await?;
            let pending = storage.strata_queue.pending_blob(&guard, &BLOB.0)?;
            assert_eq!(pending.commands().len(), 1);
            assert_eq!(
                pending.commands()[0].operation,
                BlobOperation::SetLifetime { end_epoch: 80 }
            );
        }
        let shard = storage.shard_storage(SHARD).await.unwrap();
        for axis in [SliverType::Primary, SliverType::Secondary] {
            assert!(
                shard
                    .put_registered_sliver(BLOB, get_sliver(axis, 2), 1)
                    .await?
            );
        }
        assert!(worker.process_batch().await?.is_idle());
        storage
            .strata_queue
            .write_batch(|b| b.advance_epoch(3, 60, vec![]))?;
        assert_eq!(worker.process_batch().await?.acknowledged_epochs, 1);
        assert!(shard.is_sliver_pair_stored(&BLOB)?);
        let index = StrataIndex::from_db(database(&storage.database), "strata/slivers")?;
        assert!(index.submitted_batch_lsns().is_empty()?);
        assert!(!dir.path().join("slivers/index").exists());
        drop(index);
        drop(shard);
        drop(worker);
        close(storage).await?;
        let reopened = open(dir.path(), optimistic)?;
        assert!(
            reopened
                .shard_storage(SHARD)
                .await
                .unwrap()
                .is_sliver_pair_stored(&BLOB)?
        );
        close(reopened).await?;
    }
    Ok(())
}

#[tokio::test]
async fn stale_put_is_rechecked_after_its_last_reference_is_deleted() -> TestResult {
    for optimistic in [false, true] {
        let dir = TempDir::new()?;
        let storage = open(dir.path(), optimistic)?;
        storage
            .create_storage_for_shards_for_testing(&[SHARD])
            .await?;
        let registration = registration(80);
        storage
            .update_blob_info_coordinated(0, &registration.clone().into())
            .await?;
        let worker = storage.sliver_store.strata_worker().unwrap();
        worker.process_batch().await?;
        let shard = storage.shard_storage(SHARD).await.unwrap();
        let guard = storage.strata_queue.lock_blobs(&[&BLOB.0]).await?;
        let put = tokio::spawn({
            let shard = shard.clone();
            async move {
                shard
                    .put_registered_sliver(BLOB, get_sliver(SliverType::Primary, 2), 1)
                    .await
            }
        });
        tokio::task::yield_now().await;
        // This helper is synchronous because the test already owns the shared guard.
        storage.update_blob_info(
            1,
            &registration
                .into_corresponding_deleted_event_for_testing(false)
                .into(),
        )?;
        storage
            .strata_queue
            .write_batch(|b| b.append(&BLOB.0, 1, delete(), vec![]))?;
        drop(guard);
        assert!(!put.await??);
        assert!(shard.get_sliver(&BLOB, SliverType::Primary)?.is_none());
        assert!(worker.process_batch().await?.is_idle());
        drop(shard);
        drop(worker);
        close(storage).await?;
    }
    Ok(())
}

#[tokio::test]
async fn pooled_registration_uses_pool_lifetime_and_missing_pool_does_not_commit() -> TestResult {
    let dir = TempDir::new()?;
    let storage = open(dir.path(), true)?;
    let pooled = PooledBlobRegistered::for_testing(BLOB);
    // Exercise the atomic staging helper directly so an expected validation error doesn't halt
    // the production coordinator used for uncertain commit errors.
    assert!(
        storage
            .update_blob_info(1, &BlobEvent::PooledBlobRegistered(pooled.clone()))
            .is_err()
    );
    assert!(storage.blob_info.get(&BLOB)?.is_none());
    assert!(storage.blob_info.strata_registration(&BLOB)?.is_none());
    let created = StoragePoolCreatedEvent {
        epoch: 1,
        storage_pool_id: pooled.storage_pool_id,
        reserved_encoded_capacity_bytes: 1000,
        start_epoch: 1,
        end_epoch: 65,
        event_id: pooled.event_id,
    };
    storage
        .blob_info
        .update_storage_pool_info(0, &StoragePoolEvent::StoragePoolCreated(created))?;
    storage
        .update_blob_info_coordinated(1, &BlobEvent::PooledBlobRegistered(pooled))
        .await?;
    assert_eq!(
        storage
            .blob_info
            .strata_registration(&BLOB)?
            .unwrap()
            .end_epoch,
        65
    );
    close(storage).await?;
    Ok(())
}

#[tokio::test]
async fn shard_sync_skips_retired_blobs_and_commits_control_state_after_data_is_durable()
-> TestResult {
    let dir = TempDir::new()?;
    let storage = open(dir.path(), true)?;
    storage
        .create_storage_for_shards_for_testing(&[SHARD])
        .await?;
    storage
        .update_blob_info_coordinated(0, &registration(80).into())
        .await?;
    let shard = storage.shard_storage(SHARD).await.unwrap();
    let slivers = shard.sliver_store();
    let mut batch = slivers.sync_batch(storage.garbage_collector_table.batch(), 1);
    batch.control().insert_batch(
        &storage.garbage_collector_table,
        [("milestone5".to_owned(), 9)],
    )?;
    let unregistered = BlobId([72; 32]);
    slivers.insert_in_batch(&mut batch, &BLOB, &get_sliver(SliverType::Primary, 2))?;
    slivers.insert_in_batch(
        &mut batch,
        &unregistered,
        &get_sliver(SliverType::Primary, 3),
    )?;
    assert_eq!(
        storage
            .garbage_collector_table
            .get(&"milestone5".to_owned())?,
        None
    );
    batch.write().await?;
    assert!(shard.get_sliver(&BLOB, SliverType::Primary)?.is_some());
    assert!(
        shard
            .get_sliver(&unregistered, SliverType::Primary)?
            .is_none()
    );
    assert_eq!(
        storage
            .garbage_collector_table
            .get(&"milestone5".to_owned())?,
        Some(9)
    );
    let index = StrataIndex::from_db(database(&storage.database), "strata/slivers")?;
    assert_eq!(index.get_committed_lsn()? + 1, index.get_next_lsn()?);
    drop(index);
    drop(slivers);
    drop(shard);
    close(storage).await?;
    Ok(())
}

#[tokio::test]
async fn pending_registration_is_applied_after_restart_and_event_puts_need_no_registration()
-> TestResult {
    let dir = TempDir::new()?;
    let storage = open(dir.path(), true)?;
    storage
        .create_storage_for_shards_for_testing(&[SHARD])
        .await?;
    storage
        .update_blob_info_coordinated(0, &registration(80).into())
        .await?;
    drop(storage.strata_queue.durable_snapshot()?);
    close(storage).await?;
    let storage = open(dir.path(), true)?;
    let shard = storage.shard_storage(SHARD).await.unwrap();
    assert!(
        shard
            .put_registered_sliver(BLOB, get_sliver(SliverType::Primary, 2), 1)
            .await?
    );
    let event_blob = BlobId([73; 32]);
    assert!(
        !shard
            .put_registered_sliver(event_blob, get_sliver(SliverType::Primary, 3), 1)
            .await?
    );
    shard
        .put_sliver(event_blob, get_sliver(SliverType::Primary, 3))
        .await?;
    drop(shard);
    close(storage).await?;
    let storage = open(dir.path(), true)?;
    let shard = storage.shard_storage(SHARD).await.unwrap();
    assert!(
        shard
            .get_sliver(&event_blob, SliverType::Primary)?
            .is_some()
    );
    assert!(shard.get_sliver(&BLOB, SliverType::Primary)?.is_some());
    drop(shard);
    close(storage).await?;
    Ok(())
}

#[tokio::test]
async fn cancelled_put_finishes_before_reference_updates_resume() -> TestResult {
    let dir = TempDir::new()?;
    let storage = open(dir.path(), true)?;
    storage
        .create_storage_for_shards_for_testing(&[SHARD])
        .await?;
    storage
        .update_blob_info_coordinated(0, &registration(80).into())
        .await?;
    let shard = storage.shard_storage(SHARD).await.unwrap();
    let guard = storage.strata_queue.lock_blobs(&[&BLOB.0]).await?;
    let mut put =
        Box::pin(shard.put_registered_sliver(BLOB, get_sliver(SliverType::Primary, 2), 1));
    // Polling starts the owned write task. Cancel its caller while it is waiting for admission.
    assert!(futures::poll!(&mut put).is_pending());
    drop(put);
    drop(guard);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let guard = storage.strata_queue.lock_blobs(&[&BLOB.0]).await?;
            if shard.get_sliver(&BLOB, SliverType::Primary)?.is_some() {
                // Acquiring the guard after observing the put also waits for its durability.
                let index = StrataIndex::from_db(database(&storage.database), "strata/slivers")?;
                assert_eq!(index.get_committed_lsn()? + 1, index.get_next_lsn()?);
                break Ok::<(), anyhow::Error>(());
            }
            drop(guard);
            tokio::task::yield_now().await;
        }
    })
    .await??;
    drop(shard);
    close(storage).await?;
    Ok(())
}

#[tokio::test]
async fn retired_shard_handles_cannot_write_or_delete_replacement_data() -> TestResult {
    let dir = TempDir::new()?;
    let storage = open(dir.path(), true)?;
    storage
        .create_storage_for_shards_for_testing(&[SHARD])
        .await?;
    let retired = storage.shard_storage(SHARD).await.unwrap();
    storage.remove_storage_for_shards(&[SHARD]).await?;
    storage
        .create_storage_for_shards_for_testing(&[SHARD])
        .await?;
    let current = storage.shard_storage(SHARD).await.unwrap();
    let data = get_sliver(SliverType::Primary, 2);
    current.put_sliver(BLOB, data.clone()).await?;
    assert!(
        retired
            .put_sliver(BLOB, get_sliver(SliverType::Primary, 3))
            .await
            .is_err()
    );
    assert!(retired.sliver_store().delete_pair(BLOB).await.is_err());
    assert_eq!(current.get_sliver(&BLOB, SliverType::Primary)?, Some(data));
    drop(current);
    drop(retired);
    close(storage).await?;
    Ok(())
}

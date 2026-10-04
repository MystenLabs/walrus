// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use sui_types::base_types::ObjectID;
use walrus_sui::types::PooledBlobDeleted;

use super::*;
use crate::node::storage::blob_info::STRATA_POOL_EXTENSION_BATCH_SIZE;

const POOL: ObjectID = ObjectID::from_single_byte(91);
const OTHER_POOL: ObjectID = ObjectID::from_single_byte(92);

fn member(blob_id: BlobId, pool_id: ObjectID, ordinal: u32) -> PooledBlobRegistered {
    let mut bytes = [0; 32];
    bytes[28..].copy_from_slice(&ordinal.to_be_bytes());
    PooledBlobRegistered {
        storage_pool_id: pool_id,
        ..PooledBlobRegistered::for_testing_with_object_id(blob_id, ObjectID::new(bytes))
    }
}

async fn create_pool(storage: &Storage, index: u64, pool: ObjectID, end: Epoch) -> TestResult {
    storage
        .update_storage_pool_info_coordinated(
            index,
            &StoragePoolEvent::created_for_testing(pool, 1, end),
        )
        .await?;
    Ok(())
}

async fn register_member(
    storage: &Storage,
    index: u64,
    member: PooledBlobRegistered,
) -> TestResult {
    storage
        .update_blob_info_coordinated(index, &BlobEvent::PooledBlobRegistered(member))
        .await?;
    Ok(())
}

#[tokio::test]
async fn pool_extension_deduplicates_members_and_preserves_other_references_and_deletes()
-> TestResult {
    for optimistic in [false, true] {
        let dir = TempDir::new()?;
        let storage = open(dir.path(), optimistic)?;
        create_pool(&storage, 0, POOL, 20).await?;
        create_pool(&storage, 1, OTHER_POOL, 80).await?;
        let longer_pool = BlobId([72; 32]);
        let longer_regular = BlobId([73; 32]);
        let deleted = BlobId([74; 32]);
        let unrelated = BlobId([75; 32]);
        register_member(&storage, 2, member(BLOB, POOL, 1)).await?;
        register_member(&storage, 3, member(BLOB, POOL, 2)).await?;
        register_member(&storage, 4, member(longer_pool, POOL, 3)).await?;
        register_member(&storage, 5, member(longer_pool, OTHER_POOL, 4)).await?;
        register_member(&storage, 6, member(longer_regular, POOL, 5)).await?;
        storage
            .update_blob_info_coordinated(
                7,
                &BlobRegistered {
                    blob_id: longer_regular,
                    ..registration(100)
                }
                .into(),
            )
            .await?;
        let removed = member(deleted, POOL, 6);
        register_member(&storage, 8, removed.clone()).await?;
        storage
            .update_blob_info_coordinated(
                9,
                &BlobEvent::PooledBlobDeleted(PooledBlobDeleted {
                    blob_id: deleted,
                    storage_pool_id: POOL,
                    object_id: removed.object_id,
                    was_certified: false,
                    ..PooledBlobDeleted::for_testing(deleted)
                }),
            )
            .await?;
        register_member(&storage, 10, member(unrelated, OTHER_POOL, 7)).await?;
        let worker = storage.sliver_store.strata_worker().unwrap();
        while !worker.process_batch().await?.is_idle() {}
        storage
            .strata_queue
            .write_batch(|b| b.append(&BLOB.0, 11, delete(), vec![]))?;
        let event = StoragePoolEvent::extended_for_testing(POOL, 50);
        storage
            .update_storage_pool_info_coordinated(12, &event)
            .await?;
        assert_eq!(
            storage
                .blob_info
                .strata_registration(&BLOB)?
                .unwrap()
                .end_epoch,
            20
        );
        assert!(
            StrataRegistrations::reopen(&storage.database)?
                .pools
                .get(&12)?
                .is_some()
        );
        storage.finish_strata_pool_extensions().await?;
        storage
            .update_storage_pool_info_coordinated(12, &event)
            .await?;
        assert_eq!(
            storage.get_storage_pool_info(&POOL)?.unwrap().end_epoch(),
            50
        );
        assert_eq!(storage.get_latest_handled_event_index()?, 12);
        let expected = [
            (BLOB, 50),
            (longer_pool, 80),
            (longer_regular, 100),
            (deleted, 20),
            (unrelated, 80),
        ];
        for (blob, end) in expected {
            assert_eq!(
                storage
                    .blob_info
                    .strata_registration(&blob)?
                    .unwrap()
                    .end_epoch,
                end
            );
            let guard = storage.strata_queue.lock_blobs(&[&blob.0]).await?;
            let pending = storage.strata_queue.pending_blob(&guard, &blob.0)?;
            if blob == BLOB {
                assert_eq!(pending.commands().len(), 2);
                assert_eq!(pending.commands()[0].operation, delete());
                assert_eq!(pending.commands()[1].event_index, 12);
                assert_eq!(
                    pending.commands()[1].operation,
                    BlobOperation::SetLifetime { end_epoch: 50 }
                );
                assert_eq!(
                    bcs::from_bytes::<StrataSourceEvent>(&pending.commands()[1].source)?,
                    StrataSourceEvent {
                        event_index: 12,
                        event_id: event.event_id()
                    }
                );
            } else {
                assert!(pending.commands().is_empty());
            }
        }
        assert_eq!(
            storage
                .blob_info
                .strata_registration(&BLOB)?
                .unwrap()
                .event_index,
            3
        );
        // A non-increasing pool extension is a metadata-only no-op, and later members inherit
        // the committed pool expiry without relying on a previous membership scan.
        storage
            .update_storage_pool_info_coordinated(
                13,
                &StoragePoolEvent::extended_for_testing(POOL, 40),
            )
            .await?;
        let later = BlobId([76; 32]);
        register_member(&storage, 14, member(later, POOL, 8)).await?;
        assert_eq!(
            storage
                .blob_info
                .strata_registration(&later)?
                .unwrap()
                .end_epoch,
            50
        );
        drop(worker);
        close(storage).await?;
    }
    Ok(())
}

#[tokio::test]
async fn pool_fanout_deduplicates_across_batch_boundaries() -> TestResult {
    for optimistic in [false, true] {
        let dir = TempDir::new()?;
        let storage = open(dir.path(), optimistic)?;
        create_pool(&storage, 0, POOL, 20).await?;
        let count = STRATA_POOL_EXTENSION_BATCH_SIZE + 3;
        let mut blobs = std::collections::HashSet::new();
        for i in 0..count {
            let ordinal = u32::try_from(i)?;
            let blob = if i == 0 || i == 1 || i == STRATA_POOL_EXTENSION_BATCH_SIZE {
                BLOB
            } else {
                let mut bytes = [0; 32];
                bytes[28..].copy_from_slice(&ordinal.to_be_bytes());
                BlobId(bytes)
            };
            blobs.insert(blob);
            register_member(
                &storage,
                u64::from(ordinal) + 1,
                member(blob, POOL, ordinal),
            )
            .await?;
        }
        let event_index = count as u64 + 1;
        storage
            .update_storage_pool_info_coordinated(
                event_index,
                &StoragePoolEvent::extended_for_testing(POOL, 50),
            )
            .await?;
        assert!(
            storage
                .blob_info
                .process_strata_pool_extension_batch(&storage.strata_queue)
                .await?
        );
        let job = StrataRegistrations::reopen(&storage.database)?
            .pools
            .get(&event_index)?
            .unwrap();
        assert_eq!(
            job.after_object,
            Some(
                member(
                    BLOB,
                    POOL,
                    u32::try_from(STRATA_POOL_EXTENSION_BATCH_SIZE - 1)?
                )
                .object_id
            )
        );
        drop(storage.strata_queue.durable_snapshot()?);
        close(storage).await?;
        let storage = open(dir.path(), optimistic)?;
        storage.finish_strata_pool_extensions().await?;
        {
            let snapshot = storage.strata_queue.durable_snapshot()?;
            let rows = storage
                .strata_queue
                .blobs(snapshot.as_ref())?
                .collect::<Result<Vec<_>, _>>()?;
            assert_eq!(rows.len(), blobs.len());
            for (_, pending) in rows {
                let updates: Vec<_> = pending
                    .commands()
                    .iter()
                    .filter(|c| c.event_index == event_index)
                    .collect();
                assert_eq!(updates.len(), 1);
                assert_eq!(
                    updates[0].operation,
                    BlobOperation::SetLifetime { end_epoch: 50 }
                );
            }
        }
        assert_eq!(storage.get_latest_handled_event_index()?, event_index);
        close(storage).await?;
    }
    Ok(())
}

#[tokio::test]
async fn restart_resumes_partial_pool_fanout_before_epoch_advancement() -> TestResult {
    for optimistic in [false, true] {
        for acknowledged_prefix in [false, true] {
            let dir = TempDir::new()?;
            let storage = open(dir.path(), optimistic)?;
            storage
                .create_storage_for_shards_for_testing(&[SHARD])
                .await?;
            create_pool(&storage, 0, POOL, 20).await?;
            let other = BlobId([72; 32]);
            register_member(&storage, 1, member(BLOB, POOL, 1)).await?;
            register_member(&storage, 2, member(other, POOL, 2)).await?;
            let shard = storage.shard_storage(SHARD).await.unwrap();
            for blob in [BLOB, other] {
                for axis in [SliverType::Primary, SliverType::Secondary] {
                    assert!(
                        shard
                            .put_registered_sliver(blob, get_sliver(axis, 2), 1)
                            .await?
                    );
                }
            }
            let event = StoragePoolEvent::extended_for_testing(POOL, 50);
            storage
                .update_storage_pool_info_coordinated(3, &event)
                .await?;
            {
                let _guard = storage.strata_queue.lock_lifecycle().await?;
                // Persist a completed prefix and its cursor, as one background batch would.
                // An already-running blob pass can acknowledge it before the remaining scan.
                let registrations = StrataRegistrations::reopen(&storage.database)?;
                let mut batch = storage.metadata.batch();
                registrations.extend(
                    &mut batch,
                    BLOB,
                    StrataSourceEvent {
                        event_index: 3,
                        event_id: event.event_id(),
                    },
                    50,
                )?;
                let mut job = registrations.pools.get(&3)?.unwrap();
                job.after_object = Some(member(BLOB, POOL, 1).object_id);
                batch.insert_batch(&registrations.pools, [(3, job)])?;
                batch.write()?;
            }
            drop(storage.strata_queue.durable_snapshot()?);
            if acknowledged_prefix {
                // Model a generic pass already in flight while the adapter is still expanding.
                assert_eq!(
                    storage
                        .sliver_store
                        .strata_worker()
                        .unwrap()
                        .worker
                        .process_batch()
                        .await?
                        .acknowledged_blobs,
                    1
                );
            }
            assert_eq!(
                storage.get_storage_pool_info(&POOL)?.unwrap().end_epoch(),
                50
            );
            assert_eq!(storage.get_latest_handled_event_index()?, 3);
            drop(shard);
            close(storage).await?;

            let storage = open(dir.path(), optimistic)?;
            // The producer watermark filters replay without dropping the unfinished job. The
            // adapter must expand it before letting the generic worker reach the epoch barrier.
            storage.update_storage_pool_info(3, &event)?;
            assert!(
                StrataRegistrations::reopen(&storage.database)?
                    .pools
                    .get(&3)?
                    .is_some()
            );
            storage
                .strata_queue
                .write_batch(|b| b.advance_epoch(4, 30, vec![]))?;
            let worker = storage.sliver_store.strata_worker().unwrap();
            let expected = if acknowledged_prefix { 1 } else { 2 };
            assert_eq!(worker.process_batch().await?.acknowledged_blobs, expected);
            assert!(
                StrataRegistrations::reopen(&storage.database)?
                    .pools
                    .get(&3)?
                    .is_none()
            );
            assert_eq!(worker.process_batch().await?.acknowledged_epochs, 1);
            let shard = storage.shard_storage(SHARD).await.unwrap();
            assert!(shard.is_sliver_pair_stored(&BLOB)?);
            assert!(shard.is_sliver_pair_stored(&other)?);
            storage
                .strata_queue
                .write_batch(|b| b.advance_epoch(5, 50, vec![]))?;
            assert_eq!(worker.process_batch().await?.acknowledged_epochs, 1);
            assert!(!shard.is_sliver_pair_stored(&BLOB)?);
            assert!(!shard.is_sliver_pair_stored(&other)?);
            drop(shard);
            drop(worker);
            close(storage).await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn missing_pool_or_member_state_preserves_pending_work_and_closes_admission() -> TestResult {
    for optimistic in [false, true] {
        for missing_pool in [false, true] {
            let dir = TempDir::new()?;
            let storage = open(dir.path(), optimistic)?;
            if !missing_pool {
                create_pool(&storage, 0, POOL, 20).await?;
                register_member(&storage, 1, member(BLOB, POOL, 1)).await?;
                // Simulate a missing prerequisite; fan-out must fail instead of silently skipping
                // a live member and publishing the pool expiry/watermark anyway.
                StrataRegistrations::reopen(&storage.database)?
                    .records
                    .remove(&BLOB)?;
            }
            let result = storage
                .update_storage_pool_info_coordinated(
                    2,
                    &StoragePoolEvent::extended_for_testing(POOL, 50),
                )
                .await;
            if missing_pool {
                assert!(result.is_err());
            } else {
                result?;
                assert!(
                    storage
                        .sliver_store
                        .strata_worker()
                        .unwrap()
                        .process_batch()
                        .await
                        .is_err()
                );
                assert!(
                    StrataRegistrations::reopen(&storage.database)?
                        .pools
                        .get(&2)?
                        .is_some()
                );
            }
            assert_eq!(
                storage.get_storage_pool_info(&POOL)?.map(|p| p.end_epoch()),
                if missing_pool { None } else { Some(50) }
            );
            assert_eq!(
                storage.get_latest_handled_event_index()?,
                if missing_pool { 0 } else { 2 }
            );
            assert!(storage.strata_queue.lock_blobs(&[&BLOB.0]).await.is_err());
            close(storage).await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn empty_pool_extension_and_caller_cancellation_finish_without_strata_writes() -> TestResult {
    for optimistic in [false, true] {
        let dir = TempDir::new()?;
        let storage = open(dir.path(), optimistic)?;
        create_pool(&storage, 0, POOL, 20).await?;
        let index = StrataIndex::from_db(database(&storage.database), "strata/slivers")?;
        let next_lsn = index.get_next_lsn()?;
        let guard = storage.strata_queue.lock_blobs(&[&BLOB.0]).await?;
        let event = StoragePoolEvent::extended_for_testing(POOL, 50);
        let mut update = Box::pin(storage.update_storage_pool_info_coordinated(1, &event));
        assert!(futures::poll!(&mut update).is_pending());
        drop(update);
        assert_eq!(
            storage.get_storage_pool_info(&POOL)?.unwrap().end_epoch(),
            20
        );
        drop(guard);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if storage.get_storage_pool_info(&POOL)?.unwrap().end_epoch() == 50 {
                    break Ok::<(), TypedStoreError>(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await??;
        assert_eq!(storage.get_latest_handled_event_index()?, 1);
        assert!(
            storage
                .sliver_store
                .strata_worker()
                .unwrap()
                .process_batch()
                .await?
                .is_idle()
        );
        assert_eq!(index.get_next_lsn()?, next_lsn);
        drop(index);
        close(storage).await?;
    }
    Ok(())
}

#[tokio::test]
async fn background_pool_expansion_precedes_registration_and_allows_unrelated_puts() -> TestResult {
    for optimistic in [false, true] {
        let dir = TempDir::new()?;
        let storage = open(dir.path(), optimistic)?;
        storage
            .create_storage_for_shards_for_testing(&[SHARD])
            .await?;
        create_pool(&storage, 0, POOL, 20).await?;
        register_member(&storage, 1, member(BLOB, POOL, 1)).await?;
        let unrelated = BlobId([77; 32]);
        storage
            .update_blob_info_coordinated(
                2,
                &BlobRegistered {
                    blob_id: unrelated,
                    ..registration(50)
                }
                .into(),
            )
            .await?;
        let worker = storage.sliver_store.strata_worker().unwrap();
        while !worker.process_batch().await?.is_idle() {}
        storage
            .update_storage_pool_info_coordinated(
                100,
                &StoragePoolEvent::extended_for_testing(POOL, 30),
            )
            .await?;
        assert_eq!(
            storage.get_storage_pool_info(&POOL)?.unwrap().end_epoch(),
            30
        );
        assert_eq!(
            storage
                .blob_info
                .strata_registration(&BLOB)?
                .unwrap()
                .end_epoch,
            20
        );

        // Hold X while the background scan is trying to enqueue event 100. Event 101 must
        // await that scan, but a put to an unrelated, already-registered blob can finish.
        let guard = storage.strata_queue.lock_blobs(&[&BLOB.0]).await?;
        let scan_storage = storage.clone();
        let scan = tokio::spawn(async move { scan_storage.finish_strata_pool_extensions().await });
        let later = registration(25);
        let event = BlobEvent::Registered(later.clone());
        let mut register = Box::pin(storage.update_blob_info_coordinated(101, &event));
        assert!(futures::poll!(&mut register).is_pending());
        let shard = storage.shard_storage(SHARD).await.unwrap();
        assert!(
            tokio::time::timeout(
                Duration::from_secs(5),
                shard.put_registered_sliver(unrelated, get_sliver(SliverType::Primary, 2), 1)
            )
            .await??
        );
        assert_eq!(storage.get_latest_handled_event_index()?, 100);
        assert!(storage.get_per_object_info(&later.object_id)?.is_none());
        drop(guard);
        scan.await??;
        register.await?;
        let record = storage.blob_info.strata_registration(&BLOB)?.unwrap();
        assert_eq!(record.event_index, 101);
        assert_eq!(record.end_epoch, 30);
        {
            let guard = storage.strata_queue.lock_blobs(&[&BLOB.0]).await?;
            let pending = storage.strata_queue.pending_blob(&guard, &BLOB.0)?;
            assert_eq!(
                pending
                    .commands()
                    .iter()
                    .map(|c| c.event_index)
                    .collect::<Vec<_>>(),
                [100, 101]
            );
            assert!(
                pending
                    .commands()
                    .iter()
                    .all(|c| c.operation == BlobOperation::SetLifetime { end_epoch: 30 })
            );
        }
        drop(shard);
        drop(worker);
        close(storage).await?;
    }
    Ok(())
}

#[tokio::test]
async fn supervised_worker_applies_pool_extension_without_later_events() -> TestResult {
    for optimistic in [false, true] {
        let dir = TempDir::new()?;
        let storage = open(dir.path(), optimistic)?;
        storage
            .create_storage_for_shards_for_testing(&[SHARD])
            .await?;
        create_pool(&storage, 0, POOL, 20).await?;
        register_member(&storage, 1, member(BLOB, POOL, 1)).await?;
        let shard = storage.shard_storage(SHARD).await.unwrap();
        for axis in [SliverType::Primary, SliverType::Secondary] {
            assert!(
                shard
                    .put_registered_sliver(BLOB, get_sliver(axis, 2), 1)
                    .await?
            );
        }
        storage
            .update_storage_pool_info_coordinated(
                2,
                &StoragePoolEvent::extended_for_testing(POOL, 30),
            )
            .await?;
        let worker = storage.sliver_store.strata_worker().unwrap();
        let (shutdown, receiver) = tokio::sync::watch::channel(false);
        let background = worker.clone();
        let task = tokio::spawn(async move { background.run(receiver).await });
        tokio::time::timeout(Duration::from_secs(5), async {
            while StrataRegistrations::reopen(&storage.database)?
                .pools
                .get(&2)?
                .is_some()
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Ok::<_, TypedStoreError>(())
        })
        .await??;
        shutdown.send(true)?;
        task.await??; // Graceful stop waits for the in-flight Strata durability/acknowledgement.
        storage
            .strata_queue
            .write_batch(|b| b.advance_epoch(3, 20, vec![]))?;
        assert_eq!(worker.process_batch().await?.acknowledged_epochs, 1);
        assert!(shard.is_sliver_pair_stored(&BLOB)?);
        drop(shard);
        drop(worker);
        close(storage).await?;
    }
    Ok(())
}

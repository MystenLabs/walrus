// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use walrus_sui::types::BlobCertified;

use super::*;
use crate::node::storage::{NodeStatus, blob_info::CertifiedBlobInfoApi};

fn extension(registered: &BlobRegistered, end_epoch: Epoch) -> BlobCertified {
    BlobCertified {
        end_epoch,
        is_extension: true,
        ..registered
            .clone()
            .into_corresponding_certified_event_for_testing()
    }
}

#[tokio::test]
async fn queued_extensions_survive_restart_and_preserve_both_slivers() -> TestResult {
    for optimistic in [false, true] {
        for deletable in [false, true] {
            let dir = TempDir::new()?;
            let storage = open(dir.path(), optimistic)?;
            storage
                .create_storage_for_shards_for_testing(&[SHARD])
                .await?;
            let registered = BlobRegistered {
                deletable,
                ..registration(20)
            };
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
            let worker = storage.sliver_store.strata_worker().unwrap();
            assert_eq!(worker.process_batch().await?.acknowledged_blobs, 1);
            // Ordinary certification must not enqueue a second lifetime update.
            assert!(worker.process_batch().await?.is_idle());
            let shard = storage.shard_storage(SHARD).await.unwrap();
            for axis in [SliverType::Primary, SliverType::Secondary] {
                assert!(
                    shard
                        .put_registered_sliver(BLOB, get_sliver(axis, 2), 1)
                        .await?
                );
            }
            let first = extension(&registered, 50);
            let second = extension(&registered, 80);
            storage
                .update_blob_info_coordinated(2, &first.clone().into())
                .await?;
            storage
                .update_blob_info_coordinated(3, &second.clone().into())
                .await?;
            storage
                .update_blob_info_coordinated(2, &first.into())
                .await?;
            storage
                .update_blob_info_coordinated(3, &second.clone().into())
                .await?;
            let record = storage.blob_info.strata_registration(&BLOB)?.unwrap();
            assert_eq!(record.event_index, 0);
            assert_eq!(record.end_epoch, 80);
            let object = storage
                .blob_info
                .get_per_object_info(&registered.object_id)?
                .unwrap();
            assert!(object.is_certified(79));
            assert!(!object.is_certified(80));
            assert_eq!(storage.get_latest_handled_event_index()?, 3);
            {
                let guard = storage.strata_queue.lock_blobs(&[&BLOB.0]).await?;
                let pending = storage.strata_queue.pending_blob(&guard, &BLOB.0)?;
                assert_eq!(
                    pending
                        .commands()
                        .iter()
                        .map(|c| c.event_index)
                        .collect::<Vec<_>>(),
                    [2, 3]
                );
                assert_eq!(
                    pending.commands()[1].operation,
                    BlobOperation::SetLifetime { end_epoch: 80 }
                );
                assert_eq!(
                    bcs::from_bytes::<StrataSourceEvent>(&pending.commands()[1].source)?,
                    StrataSourceEvent {
                        event_index: 3,
                        event_id: second.event_id
                    }
                );
            }
            // Persist the input intents, then restart before either is submitted to Strata.
            drop(storage.strata_queue.durable_snapshot()?);
            drop(shard);
            drop(worker);
            close(storage).await?;
            let storage = open(dir.path(), optimistic)?;
            storage
                .update_blob_info_coordinated(3, &second.into())
                .await?;
            storage
                .strata_queue
                .write_batch(|b| b.advance_epoch(4, 70, vec![]))?;
            let worker = storage.sliver_store.strata_worker().unwrap();
            assert_eq!(worker.process_batch().await?.acknowledged_blobs, 1);
            assert_eq!(worker.process_batch().await?.acknowledged_blobs, 1);
            assert_eq!(worker.process_batch().await?.acknowledged_epochs, 1);
            let shard = storage.shard_storage(SHARD).await.unwrap();
            assert!(shard.is_sliver_pair_stored(&BLOB)?);
            // A later, shorter reference must retain the lifetime established by extension.
            storage
                .update_blob_info_coordinated(5, &registration(75).into())
                .await?;
            assert_eq!(
                storage
                    .blob_info
                    .strata_registration(&BLOB)?
                    .unwrap()
                    .end_epoch,
                80
            );
            assert_eq!(worker.process_batch().await?.acknowledged_blobs, 1);
            storage
                .strata_queue
                .write_batch(|b| b.advance_epoch(6, 80, vec![]))?;
            assert_eq!(worker.process_batch().await?.acknowledged_epochs, 1);
            assert!(!shard.is_sliver_pair_stored(&BLOB)?);
            assert!(worker.process_batch().await?.is_idle());
            drop(shard);
            drop(worker);
            close(storage).await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn extending_a_shorter_reference_preserves_longer_lifetime_and_pending_deletes() -> TestResult
{
    for optimistic in [false, true] {
        let dir = TempDir::new()?;
        let storage = open(dir.path(), optimistic)?;
        let shorter = registration(20);
        storage
            .update_blob_info_coordinated(0, &shorter.clone().into())
            .await?;
        storage
            .update_blob_info_coordinated(
                1,
                &shorter
                    .clone()
                    .into_corresponding_certified_event_for_testing()
                    .into(),
            )
            .await?;
        storage
            .update_blob_info_coordinated(2, &registration(80).into())
            .await?;
        let worker = storage.sliver_store.strata_worker().unwrap();
        assert_eq!(worker.process_batch().await?.acknowledged_blobs, 1);
        assert_eq!(worker.process_batch().await?.acknowledged_blobs, 1);
        storage.strata_queue.write_batch(|batch| {
            batch.append(&BLOB.0, 3, delete(), vec![])?;
            batch.append(
                &BLOB.0,
                4,
                BlobOperation::Delete {
                    shards: vec![],
                    cancellable: false,
                },
                vec![],
            )
        })?;
        storage
            .update_blob_info_coordinated(5, &extension(&shorter, 50).into())
            .await?;
        let record = storage.blob_info.strata_registration(&BLOB)?.unwrap();
        assert_eq!(record.event_index, 2);
        assert_eq!(record.end_epoch, 80);
        let object = storage
            .blob_info
            .get_per_object_info(&shorter.object_id)?
            .unwrap();
        assert!(object.is_certified(49));
        assert!(!object.is_certified(50));
        {
            let guard = storage.strata_queue.lock_blobs(&[&BLOB.0]).await?;
            let pending = storage.strata_queue.pending_blob(&guard, &BLOB.0)?;
            assert_eq!(
                pending
                    .commands()
                    .iter()
                    .map(|c| c.event_index)
                    .collect::<Vec<_>>(),
                [3, 4, 5]
            );
            assert_eq!(pending.commands()[0].operation, delete());
            assert!(matches!(
                pending.commands()[1].operation,
                BlobOperation::Delete {
                    cancellable: false,
                    ..
                }
            ));
            assert_eq!(
                pending.commands()[2].operation,
                BlobOperation::SetLifetime { end_epoch: 80 }
            );
        }
        drop(worker);
        close(storage).await?;
    }
    Ok(())
}

#[tokio::test]
async fn missing_registration_rejects_extension_without_committing_metadata_or_progress()
-> TestResult {
    for optimistic in [false, true] {
        let dir = TempDir::new()?;
        let storage = open(dir.path(), optimistic)?;
        let registered = registration(20);
        let event = extension(&registered, 50);
        // Use the staging entry point: this expected validation failure is not a storage failure.
        let error = storage
            .update_blob_info(8, &event.clone().into())
            .unwrap_err();
        assert!(error.to_string().contains("no registration identity"));
        assert!(storage.blob_info.get(&BLOB)?.is_none());
        assert!(
            storage
                .blob_info
                .get_per_object_info(&registered.object_id)?
                .is_none()
        );
        assert!(storage.blob_info.strata_registration(&BLOB)?.is_none());
        assert_eq!(storage.get_latest_handled_event_index()?, 0);
        {
            let guard = storage.strata_queue.lock_blobs(&[&BLOB.0]).await?;
            assert!(
                storage
                    .strata_queue
                    .pending_blob(&guard, &BLOB.0)?
                    .commands()
                    .is_empty()
            );
        }
        // Failed event 8 must not advance the watermark and filter out these earlier events.
        storage
            .update_blob_info_coordinated(6, &registered.clone().into())
            .await?;
        storage
            .update_blob_info_coordinated(
                7,
                &registered
                    .into_corresponding_certified_event_for_testing()
                    .into(),
            )
            .await?;
        storage
            .update_blob_info_coordinated(8, &event.into())
            .await?;
        assert_eq!(
            storage
                .blob_info
                .strata_registration(&BLOB)?
                .unwrap()
                .end_epoch,
            50
        );
        close(storage).await?;
    }
    Ok(())
}

#[tokio::test]
async fn incomplete_history_extension_reconstructs_registration_before_puts() -> TestResult {
    for optimistic in [false, true] {
        let dir = TempDir::new()?;
        let storage = open(dir.path(), optimistic)?;
        storage
            .create_storage_for_shards_for_testing(&[SHARD])
            .await?;
        storage.set_node_status(NodeStatus::RecoveryCatchUpWithIncompleteHistory {
            first_complete_epoch: 5,
            epoch_at_start: 10,
        })?;
        let expired_registration = registration(5);
        storage
            .strata_queue
            .write_batch(|b| b.append(&BLOB.0, 1, delete(), vec![]))?;
        let event = extension(&expired_registration, 50);
        storage
            .update_blob_info_coordinated(2, &event.clone().into())
            .await?;
        storage
            .update_blob_info_coordinated(2, &event.into())
            .await?;
        let record = storage.blob_info.strata_registration(&BLOB)?.unwrap();
        assert_eq!(record.event_index, 2);
        assert_eq!(record.end_epoch, 50);
        {
            let guard = storage.strata_queue.lock_blobs(&[&BLOB.0]).await?;
            let pending = storage.strata_queue.pending_blob(&guard, &BLOB.0)?;
            assert_eq!(pending.commands().len(), 1);
            assert_eq!(pending.commands()[0].event_index, 2);
            assert_eq!(
                pending.commands()[0].operation,
                BlobOperation::SetLifetime { end_epoch: 50 }
            );
        }
        let shard = storage.shard_storage(SHARD).await.unwrap();
        assert!(
            shard
                .put_registered_sliver(BLOB, get_sliver(SliverType::Primary, 2), 10)
                .await?
        );
        // Once reconstructed, further extensions follow the ordinary path and keep its identity.
        storage
            .update_blob_info_coordinated(3, &extension(&expired_registration, 80).into())
            .await?;
        assert_eq!(
            storage
                .blob_info
                .strata_registration(&BLOB)?
                .unwrap()
                .event_index,
            2
        );
        assert_eq!(
            storage
                .sliver_store
                .strata_worker()
                .unwrap()
                .process_batch()
                .await?
                .acknowledged_blobs,
            1
        );
        drop(shard);
        close(storage).await?;
    }
    Ok(())
}

#[tokio::test]
async fn waiting_put_applies_extension_before_writing() -> TestResult {
    for optimistic in [false, true] {
        let dir = TempDir::new()?;
        let storage = open(dir.path(), optimistic)?;
        storage
            .create_storage_for_shards_for_testing(&[SHARD])
            .await?;
        let registered = registration(20);
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
        let worker = storage.sliver_store.strata_worker().unwrap();
        worker.process_batch().await?;
        let shard = storage.shard_storage(SHARD).await.unwrap();
        let guard = storage.strata_queue.lock_blobs(&[&BLOB.0]).await?;
        let event = extension(&registered, 50).into();
        let mut put =
            Box::pin(shard.put_registered_sliver(BLOB, get_sliver(SliverType::Primary, 2), 1));
        assert!(futures::poll!(&mut put).is_pending());
        assert_eq!(
            storage
                .blob_info
                .strata_registration(&BLOB)?
                .unwrap()
                .end_epoch,
            20
        );
        // The extension writer already owns the shared blob guard. Commit its metadata and
        // queue entry while the put waits, without relying on task scheduling to order them.
        storage.update_blob_info(2, &event)?;
        drop(guard);
        assert!(put.await?);
        assert!(worker.process_batch().await?.is_idle());
        storage
            .strata_queue
            .write_batch(|b| b.advance_epoch(3, 30, vec![]))?;
        assert_eq!(worker.process_batch().await?.acknowledged_epochs, 1);
        assert!(shard.get_sliver(&BLOB, SliverType::Primary)?.is_some());
        drop(shard);
        drop(worker);
        close(storage).await?;
    }
    Ok(())
}

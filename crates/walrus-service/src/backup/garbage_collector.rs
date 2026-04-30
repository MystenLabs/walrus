// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{panic::Location, time::Duration};

use anyhow::Result;
use diesel::{QueryableByName, sql_types::Bytea};
use diesel_async::{RunQueryDsl, scoped_futures::ScopedFutureExt};
use object_store::{ObjectStore, gcp::GoogleCloudStorageBuilder, local::LocalFileSystem};
use walrus_core::BlobId;

/// Maximum number of pool refs drained per pool-expiry GC tick. Bounds the per-tick write
/// cost when a large pool finally expires.
const POOL_EXPIRY_BATCH_SIZE: i64 = 200;

use super::{
    BACKUP_BLOB_ARCHIVE_SUBDIR,
    config::BackupConfig,
    metrics::BackupGarbageCollectorMetricSet,
    service::{establish_connection_async, retry_serializable_query},
};
use crate::common::utils::{self, MetricsAndLoggingRuntime};

/// Starts a new backup node runtime.
pub async fn start_backup_garbage_collector(
    version: &'static str,
    config: BackupConfig,
    metrics_runtime: &MetricsAndLoggingRuntime,
) -> Result<()> {
    tracing::info!(?config, "starting backup node");

    let registry_clone = metrics_runtime.registry.clone();
    tokio::spawn(async move {
        registry_clone
            .register(mysten_metrics::uptime_metric(
                "walrus_backup_garbage_collector",
                version,
                "walrus",
            ))
            .expect("metrics defined at compile time must be valid");
    });

    tracing::info!(version, "Walrus backup binary version");
    tracing::info!(
        metrics_address = %config.metrics_address, "started Prometheus HTTP endpoint",
    );

    utils::export_build_info(&metrics_runtime.registry, version);

    let backup_garbage_collector_metric_set =
        BackupGarbageCollectorMetricSet::new(&metrics_runtime.registry);

    collect_garbage(&config, &backup_garbage_collector_metric_set).await
}

#[derive(Debug, QueryableByName)]
struct GarbageBlob {
    #[diesel(sql_type = diesel::sql_types::Bytea)]
    blob_id: Vec<u8>,
}

async fn collect_garbage(
    config: &BackupConfig,
    metric_set: &BackupGarbageCollectorMetricSet,
) -> Result<()> {
    let database_url = config.db_config.database_url.clone();

    let mut conn = establish_connection_async(&database_url, "db connect for db metrics polling")
        .await
        .expect("failed to connect to postgres for collect_garbage");

    // GC eligibility for a blob is a single per-row predicate:
    //   regular source dead — `end_epoch IS NULL OR end_epoch + offset <= current_epoch`
    //   AND no live pool refs — `pool_ref_count = 0`
    //
    // The `blob_state_gc_eligible` partial index materializes exactly this row set
    // (state='archived' AND pool_ref_count = 0, ordered by end_epoch NULLS FIRST), so
    // the GC scan touches only blobs that are already candidates. Live pool-only
    // blobs have `pool_ref_count > 0` and never enter the index — no daily re-check.
    //
    // `pool_ref_count` is decremented exclusively by the orchestrator's pool-related
    // event handlers and the pool-expiry GC below, both of which mutate it
    // transactionally with the corresponding `pooled_blob_ref` row change.
    //
    // We push `initiate_gc_after` forward by 1 day for every blob we pick. If the
    // subsequent object-storage delete succeeds, the blob's state moves to 'deleted'
    // and `initiate_gc_after` becomes irrelevant. If the delete fails, the deferral
    // throttles retries so a persistent storage outage doesn't hot-loop.
    let garbage_query = format!(
        "
            WITH expired_blob_ids AS (
                SELECT blob_id FROM blob_state
                WHERE
                    state = 'archived'
                    AND pool_ref_count = 0
                    AND (
                        end_epoch IS NULL
                        OR end_epoch + {offset}
                            <= COALESCE((SELECT MAX(epoch) FROM epoch_change_start_event), 0)
                    )
                    AND (initiate_gc_after IS NULL OR initiate_gc_after < NOW())
                ORDER BY end_epoch NULLS FIRST
                LIMIT 15
            ),
            _updated_count AS (
                UPDATE blob_state
                SET initiate_gc_after = NOW() + INTERVAL '1 day'
                WHERE blob_id IN (SELECT blob_id FROM expired_blob_ids)
            )
            SELECT blob_id FROM expired_blob_ids
        ",
        offset = config.garbage_collection_epoch_offset,
    );
    let garbage_query = garbage_query.as_str();

    // Start an infinite loop polling the database for blob state statistics and updating the
    // metrics.
    loop {
        // Drain a batch of refs from any expired pool. This decrements the affected blobs'
        // pool_ref_count atomically with the ref deletion. Once a blob's last live source
        // (regular or pool) is gone, it becomes eligible in the blob-GC scan below.
        //
        // Crash-safety: each batch is one transaction. On restart the same expired pool is
        // still in `storage_pool_state` (we only delete the pool row once it has no refs
        // left), so we resume draining without missing or double-decrementing.
        let pool_refs_drained = drain_expired_pool_refs(&mut conn, config, metric_set)
            .await
            .unwrap_or(0);

        let garbage_blobs: Vec<GarbageBlob> = retry_serializable_query(
            &mut conn,
            Location::caller(),
            &config.db_config,
            &metric_set
                .db_serializability_retries
                .with_label_values(&["collect_garbage"]),
            &metric_set.db_reconnects,
            |conn| {
                {
                    async move {
                        diesel::sql_query(garbage_query)
                            .get_results(conn)
                            .await
                            .inspect_err(|error| {
                                tracing::warn!(
                                    ?error,
                                    "failed to query blob_state table for expired blobs"
                                );
                            })
                            .inspect(|blobs| {
                                tracing::info!(?blobs, "fetched expired blobs");
                            })
                    }
                }
                .scope_boxed()
            },
        )
        .await
        .unwrap_or_default();

        if garbage_blobs.is_empty() && pool_refs_drained == 0 {
            metric_set.idle_state.set(1.0);
            tracing::info!("no garbage blobs found, sleeping");
            tokio::time::sleep(config.idle_garbage_collector_sleep_time).await;
            metric_set.idle_state.set(0.0);
        } else {
            for GarbageBlob { blob_id } in garbage_blobs.into_iter() {
                let blob_id = BlobId::try_from(blob_id.as_slice())
                    .expect("db has a check constraint on blob_id length");
                // Note that there is a potential synchronization issue here where the blob could
                // be deleted from storage but not from the database. This is acceptable as we'll
                // just try again later.
                if delete_blob_from_storage(blob_id, config).await.is_ok() {
                    let _ = retry_serializable_query(
                        &mut conn,
                        Location::caller(),
                        &config.db_config,
                        &metric_set
                            .db_serializability_retries
                            .with_label_values(&["delete_blob"]),
                        &metric_set.db_reconnects,
                        |conn| {
                            tracing::info!(?blob_id, "deleting blob from database");
                            async move {
                                diesel::sql_query(
                                    "
                                        UPDATE blob_state
                                        SET state = 'deleted', backup_url = NULL, retry_count = NULL
                                        WHERE
                                            blob_id = $1 AND
                                            state = 'archived'
                                    ",
                                )
                                .bind::<Bytea, _>(blob_id.0)
                                .execute(conn)
                                .await
                                .inspect(|&count| {
                                    if count != 0 {
                                        tracing::info!(?blob_id, "deleted blob from database");
                                    } else {
                                        tracing::info!(
                                            ?blob_id,
                                            "blob was not deleted from database"
                                        );
                                    }
                                    metric_set.blobs_deleted.inc_by(count as u64);
                                })
                            }
                            .scope_boxed()
                        },
                    )
                    .await;
                }
            }
            // Throttle these calls for now.
            tokio::time::sleep(Duration::from_secs_f64(0.25)).await;
        }
    }
}

/// Drains a bounded batch of `pooled_blob_ref` rows belonging to expired pools.
///
/// Each batch atomically:
///   1. picks an expired pool (`end_epoch + offset <= current_epoch`),
///   2. deletes up to `POOL_EXPIRY_BATCH_SIZE` of its refs,
///   3. decrements `pool_ref_count` on the affected blobs by exactly the number of
///      refs deleted, and
///   4. if the pool has no remaining refs, removes its `storage_pool_state` row.
///
/// All four steps happen inside a single serializable transaction. A crash mid-batch
/// rolls back; a crash between batches leaves a consistent state where some refs of
/// the same pool remain to be drained in the next tick. Either way, the counter is
/// always in sync with the actual ref-table contents.
///
/// Returns the number of refs drained in this tick. Zero means there's nothing to do.
pub(crate) async fn drain_expired_pool_refs(
    conn: &mut diesel_async::AsyncPgConnection,
    config: &BackupConfig,
    metric_set: &BackupGarbageCollectorMetricSet,
) -> Result<usize, diesel::result::Error> {
    let offset = config.garbage_collection_epoch_offset;

    let drained = retry_serializable_query(
        conn,
        Location::caller(),
        &config.db_config,
        &metric_set
            .db_serializability_retries
            .with_label_values(&["pool_expiry_drain"]),
        &metric_set.db_reconnects,
        |conn| async move { drain_expired_pool_refs_step(conn, offset).await }.scope_boxed(),
    )
    .await?;

    if drained > 0 {
        tracing::info!(drained, "drained pool refs from expired pools");
    }

    // Once a pool has no remaining refs, drop its `storage_pool_state` row so it stops
    // showing up as a candidate. This is its own statement so it can find pools that
    // had their last ref removed in *any* previous drain batch (including this one).
    retry_serializable_query(
        conn,
        Location::caller(),
        &config.db_config,
        &metric_set
            .db_serializability_retries
            .with_label_values(&["pool_expiry_cleanup"]),
        &metric_set.db_reconnects,
        |conn| async move { cleanup_drained_pools(conn, offset).await }.scope_boxed(),
    )
    .await?;

    Ok(drained)
}

/// Atomically drains up to `POOL_EXPIRY_BATCH_SIZE` refs from a single expired pool and
/// decrements the affected blobs' `pool_ref_count`. Returns the number of refs drained
/// in this call (zero means there's nothing to drain right now). Exposed for tests.
pub(crate) async fn drain_expired_pool_refs_step(
    conn: &mut diesel_async::AsyncPgConnection,
    offset: u64,
) -> Result<usize, diesel::result::Error> {
    let query = format!(
        "
            WITH selected_pool AS (
                SELECT storage_pool_id FROM storage_pool_state
                WHERE end_epoch + {offset}
                    <= COALESCE((SELECT MAX(epoch) FROM epoch_change_start_event), 0)
                ORDER BY end_epoch
                LIMIT 1
            ),
            ref_batch AS (
                SELECT storage_pool_id, blob_id FROM pooled_blob_ref
                WHERE storage_pool_id = (SELECT storage_pool_id FROM selected_pool)
                LIMIT {batch}
            ),
            deleted AS (
                DELETE FROM pooled_blob_ref pbr
                USING ref_batch rb
                WHERE pbr.storage_pool_id = rb.storage_pool_id AND pbr.blob_id = rb.blob_id
                RETURNING pbr.blob_id
            ),
            decremented AS (
                UPDATE blob_state bs
                SET pool_ref_count = bs.pool_ref_count - 1
                FROM deleted d
                WHERE bs.blob_id = d.blob_id
                RETURNING bs.blob_id
            )
            SELECT COUNT(*)::bigint AS value FROM decremented
        ",
        offset = offset,
        batch = POOL_EXPIRY_BATCH_SIZE,
    );

    #[derive(QueryableByName)]
    struct CountRow {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        value: i64,
    }

    let drained = diesel::sql_query(query.as_str())
        .get_result::<CountRow>(conn)
        .await
        .map(|row| row.value)?;

    Ok(usize::try_from(drained).unwrap_or(0))
}

/// Deletes `storage_pool_state` rows for pools that are expired AND have no remaining
/// `pooled_blob_ref` rows referencing them. Idempotent. Exposed for tests.
pub(crate) async fn cleanup_drained_pools(
    conn: &mut diesel_async::AsyncPgConnection,
    offset: u64,
) -> Result<usize, diesel::result::Error> {
    let affected = diesel::sql_query(format!(
        "
            DELETE FROM storage_pool_state sps
            WHERE sps.end_epoch + {offset}
                    <= COALESCE((SELECT MAX(epoch) FROM epoch_change_start_event), 0)
                AND NOT EXISTS (
                    SELECT 1 FROM pooled_blob_ref pbr
                    WHERE pbr.storage_pool_id = sps.storage_pool_id
                )
        ",
        offset = offset,
    ))
    .execute(conn)
    .await?;
    Ok(affected)
}

/// Deletes a blob from storage. Returns whether the blob should be set to 'deleted' in the
/// database.
#[tracing::instrument(skip_all)]
async fn delete_blob_from_storage(blob_id: BlobId, backup_config: &BackupConfig) -> Result<bool> {
    let store: Box<dyn ObjectStore> =
        if let Some(backup_bucket) = backup_config.backup_bucket.as_deref() {
            Box::new(
                GoogleCloudStorageBuilder::from_env()
                    .with_client_options(
                        object_store::ClientOptions::default()
                            .with_timeout(backup_config.blob_upload_timeout),
                    )
                    .with_bucket_name(backup_bucket.to_string())
                    .build()?,
            )
        } else {
            Box::new(LocalFileSystem::new_with_prefix(
                backup_config
                    .backup_storage_path
                    .join(BACKUP_BLOB_ARCHIVE_SUBDIR),
            )?)
        };

    // Delete them
    tracing::info!(?blob_id, "deleting blob from storage");
    match store.delete(&blob_id.to_string().into()).await {
        Ok(()) => {
            tracing::info!(?blob_id, "successfully deleted blob from storage");
            Ok(true)
        }
        Err(error @ object_store::Error::NotFound { .. }) => {
            tracing::warn!(?blob_id, ?error, "blob not found in storage");
            Ok(true)
        }
        Err(error) => {
            tracing::warn!(?error, "failed to delete blob from storage");
            Ok(false)
        }
    }
}

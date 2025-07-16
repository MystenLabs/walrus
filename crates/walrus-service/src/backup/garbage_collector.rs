// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{panic::Location, time::Duration};

use anyhow::Result;
use diesel::{QueryableByName, sql_types::Bytea};
use diesel_async::{RunQueryDsl, scoped_futures::ScopedFutureExt};
use object_store::{ObjectStore, gcp::GoogleCloudStorageBuilder, local::LocalFileSystem};
use walrus_core::BlobId;

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

    // Start an infinite loop polling the database for blob state statistics and updating the
    // metrics.
    loop {
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
                        diesel::sql_query(
                            "
                            WITH expired_blob_ids AS (
                                SELECT blob_id
                                FROM blob_state
                                WHERE
                                    state = 'archived' AND
                                    (initiate_gc_after IS NULL OR initiate_gc_after < NOW()) AND
                                    COALESCE(
                                        end_epoch + 2 <= (
                                            SELECT MAX(epoch) FROM epoch_change_start_event),
                                        FALSE)
                                LIMIT 15
                            ),
                            _updated_count AS (
                                UPDATE blob_state
                                SET initiate_gc_after = NOW() + INTERVAL '1 day'
                                WHERE blob_id IN (SELECT blob_id FROM expired_blob_ids)
                            )
                            SELECT blob_id from expired_blob_ids
                            ",
                        )
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

        if garbage_blobs.is_empty() {
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

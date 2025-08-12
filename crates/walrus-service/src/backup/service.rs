// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Backup service implementation.
use std::{panic::Location, pin::Pin, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
use diesel::{
    Connection as _,
    QueryableByName,
    result::{DatabaseErrorKind, Error},
    sql_types::{Bytea, Int4, Int8, Text},
};
use diesel_async::{
    AsyncConnection as _,
    AsyncPgConnection,
    RunQueryDsl as _,
    scoped_futures::ScopedFutureExt,
};
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};
use futures::{StreamExt, stream};
use object_store::{ObjectStore, gcp::GoogleCloudStorageBuilder, local::LocalFileSystem};
use prometheus::core::{AtomicU64, GenericCounter};
use sha2::Digest;
use sui_types::event::EventID;
use tokio_util::sync::CancellationToken;
use walrus_core::{BlobId, encoding::Primary};
use walrus_sdk::{
    client::Client,
    config::{ClientConfig, combine_rpc_urls},
};
use walrus_sui::{
    client::{SuiReadClient, retry_client::RetriableSuiClient},
    types::{BlobEvent, ContractEvent, EpochChangeEvent, EpochChangeStart},
};
use walrus_utils::metrics::Registry;

use super::{
    BACKUP_BLOB_ARCHIVE_SUBDIR,
    config::{BackupConfig, BackupDbConfig},
    models::{self, BlobIdRow, StreamEvent},
    schema,
};
use crate::{
    backup::metrics::{BackupDbMetricSet, BackupFetcherMetricSet, BackupOrchestratorMetricSet},
    common::utils::{self, MetricsAndLoggingRuntime},
    event::{
        event_processor::{processor::EventProcessor, runtime::EventProcessorRuntime},
        events::{CheckpointEventPosition, EventStreamElement, PositionedStreamEvent},
    },
    node::{DatabaseConfig, metrics::TelemetryLabel as _, system_events::SystemEventProvider as _},
};

const FETCHER_ERROR_BACKOFF: Duration = Duration::from_secs(1);

pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

async fn stream_events(
    version: &'static str,
    event_processor: Arc<EventProcessor>,
    _metrics_registry: Registry,
    db_config: &BackupDbConfig,
    backup_orchestrator_metric_set: BackupOrchestratorMetricSet,
) -> Result<()> {
    let mut pg_connection =
        establish_connection_async(&db_config.database_url, "db connect for stream_events").await?;

    let event_cursor = models::get_backup_node_cursor(&mut pg_connection).await?;
    tracing::info!(?event_cursor, "[stream_events] starting");
    let event_stream = Pin::from(event_processor.events(event_cursor).await?);
    let next_event_index = event_cursor.element_index;
    let index_stream = stream::iter(next_event_index..);
    let mut indexed_element_stream = index_stream.zip(event_stream);
    let counter: &GenericCounter<AtomicU64> = &backup_orchestrator_metric_set
        .db_serializability_retries
        .with_label_values(&["record_event"]);
    while let Some((
        element_index,
        PositionedStreamEvent {
            element,
            checkpoint_event_position,
        },
    )) = indexed_element_stream.next().await
    {
        backup_orchestrator_metric_set.sui_events_seen.inc();
        match &element {
            EventStreamElement::ContractEvent(contract_event) => {
                record_event(
                    version,
                    &mut pg_connection,
                    &element,
                    checkpoint_event_position,
                    element_index,
                    contract_event,
                    db_config,
                    counter,
                    &backup_orchestrator_metric_set.db_reconnects,
                )
                .await?;
                backup_orchestrator_metric_set.events_recorded.inc();
            }
            EventStreamElement::CheckpointBoundary => {
                // Skip checkpoint boundaries as they are not relevant for the backup node.
                continue;
            }
        }
    }

    bail!("event stream for blob events stopped")
}

#[allow(clippy::too_many_arguments)]
async fn record_event(
    version: &'static str,
    pg_connection: &mut AsyncPgConnection,
    element: &EventStreamElement,
    checkpoint_event_position: CheckpointEventPosition,
    element_index: u64,
    contract_event: &ContractEvent,
    db_config: &BackupDbConfig,
    retry_counter: &GenericCounter<AtomicU64>,
    db_reconnects: &GenericCounter<AtomicU64>,
) -> Result<(), Error> {
    let event_id: EventID = contract_event.event_id();
    retry_serializable_query(
        pg_connection,
        Location::caller(),
        db_config,
        retry_counter,
        db_reconnects,
        |conn| {
            async move {
                diesel::insert_into(schema::stream_event::dsl::stream_event)
                    .values(&StreamEvent::new(
                        checkpoint_event_position,
                        event_id.tx_digest.into_inner(),
                        event_id.event_seq,
                        element_index,
                        element,
                    ))
                    .execute(conn)
                    .await?;

                dispatch_contract_event(version, contract_event, conn).await
            }
            .scope_boxed()
        },
    )
    .await?;
    Ok(())
}

async fn dispatch_contract_event(
    version: &'static str,
    contract_event: &ContractEvent,
    conn: &mut AsyncPgConnection,
) -> Result<(), Error> {
    match contract_event {
        // Note that certifying the same blob twice might result in resetting the retry_count. This
        // automatically gives the re-certified blob another chance to be backed up without human
        // intervention.
        ContractEvent::BlobEvent(BlobEvent::Certified(blob_certified)) => {
            tracing::info!(
                blob_id = ?blob_certified.blob_id,
                incoming_end_epoch = blob_certified.end_epoch,
                "writing blob state to database");
            diesel::dsl::sql_query(
                "
                INSERT INTO blob_state (
                    blob_id,
                    state,
                    end_epoch,
                    orchestrator_version,
                    retry_count,
                    initiate_fetch_after
                )
                VALUES ($1, 'waiting', $2, $3, 1, NOW())
                ON CONFLICT (blob_id)
                DO UPDATE SET
                    end_epoch = GREATEST(EXCLUDED.end_epoch, blob_state.end_epoch),
                    retry_count = CASE WHEN
                        blob_state.state = 'archived' THEN
                            NULL
                        ELSE
                            1
                        END,
                    state = CASE WHEN
                        blob_state.state = 'archived' THEN
                            blob_state.state
                        ELSE
                            'waiting'
                        END,
                    backup_url = CASE WHEN
                        blob_state.state = 'archived' THEN
                            blob_state.backup_url
                        ELSE
                            NULL
                        END,
                    initiate_fetch_after = CASE WHEN
                        blob_state.state = 'archived' THEN
                            NULL
                        ELSE
                            NOW()
                        END,
                    initiate_gc_after = NULL,
                    orchestrator_version = $3",
            )
            .bind::<Bytea, _>(blob_certified.blob_id.0.to_vec())
            .bind::<Int8, _>(i64::from(blob_certified.end_epoch))
            .bind::<Text, _>(version)
            .execute(conn)
            .await?;
            tracing::info!(
                blob_id = %blob_certified.blob_id,
                incoming_end_epoch = blob_certified.end_epoch,
                "upserted blob into blob_state table"
            );
        }
        ContractEvent::EpochChangeEvent(EpochChangeEvent::EpochChangeStart(EpochChangeStart {
            epoch,
            ..
        })) => {
            diesel::dsl::sql_query(
                "
                INSERT INTO epoch_change_start_event (epoch) VALUES ($1)
                ON CONFLICT DO NOTHING",
            )
            .bind::<Int8, _>(i64::from(*epoch))
            .execute(conn)
            .await?;
            tracing::info!(epoch, "a new walrus epoch has begun");
        }
        event => {
            tracing::info!(?event, "ignoring event");
        }
    }
    Ok(())
}

fn establish_connection(database_url: &str, context: &str) -> Result<diesel::PgConnection> {
    tracing::info!(context, "attempting to connect to postgres");
    match diesel::PgConnection::establish(database_url) {
        Err(e) => {
            tracing::error!(?e, context, "failed to connect to postgres");
            Err(e.into())
        }
        Ok(conn) => {
            tracing::info!(context, "connected to postgres");
            Ok(conn)
        }
    }
}
pub async fn establish_connection_async(
    database_url: &str,
    context: &str,
) -> Result<AsyncPgConnection> {
    tracing::info!(context, "attempting to connect to postgres");
    match AsyncPgConnection::establish(database_url).await {
        Err(e) => {
            tracing::error!(?e, context, "failed to connect to postgres");
            Err(e.into())
        }
        Ok(conn) => {
            tracing::info!(context, "connected to postgres");
            Ok(conn)
        }
    }
}

/// Run the database migrations for the backup node.
pub fn run_backup_database_migrations(config: &BackupConfig) {
    let mut connection = match establish_connection(
        &config.db_config.database_url,
        "run_backup_database_migrations",
    ) {
        Ok(connection) => connection,
        Err(error) => {
            tracing::error!(
                ?error,
                "failed to connect to postgres for database migration"
            );
            std::process::exit(1);
        }
    };

    tracing::info!("running pending migrations");
    match connection.run_pending_migrations(MIGRATIONS) {
        Ok(versions) => {
            tracing::info!(?versions, "migrations ran successfully");
        }
        Err(error) => {
            tracing::error!(?error, "failed to run pending migrations");
            std::process::exit(1);
        }
    }
}

/// Starts a new backup node runtime.
pub async fn start_backup_orchestrator(
    version: &'static str,
    config: BackupConfig,
    metrics_runtime: &MetricsAndLoggingRuntime,
) -> Result<()> {
    tracing::info!(?config, version, "starting backup node");

    let registry_clone = metrics_runtime.registry.clone();
    tokio::spawn(async move {
        registry_clone
            .register(mysten_metrics::uptime_metric(
                "walrus_backup_orchestrator",
                version,
                "walrus",
            ))
            .expect("metrics defined at compile time must be valid");
    });

    tracing::info!(
        metrics_address = %config.metrics_address, "started Prometheus HTTP endpoint",
    );

    utils::export_build_info(&metrics_runtime.registry, version);

    let cancel_token = CancellationToken::new();

    start_db_metrics_loop(metrics_runtime, &config);

    let event_processor = EventProcessorRuntime::start_async(
        config.sui.clone(),
        config.event_processor_config.clone(),
        &config.backup_storage_path,
        &metrics_runtime.registry,
        cancel_token.child_token(),
        &DatabaseConfig::default(),
    )
    .await?;

    let metrics_registry = metrics_runtime.registry.clone();

    let backup_orchestrator_metric_set =
        BackupOrchestratorMetricSet::new(&metrics_runtime.registry);
    // Connect to the database.
    // Stream events from Sui and pull them into our main business logic workflow.
    stream_events(
        version,
        event_processor,
        metrics_registry,
        &config.db_config,
        backup_orchestrator_metric_set,
    )
    .await
}

#[derive(Debug, QueryableByName)]
struct DbStatistic {
    #[diesel(sql_type = diesel::sql_types::Text)]
    name: String,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    value: i64,
}

fn start_db_metrics_loop(metrics_runtime: &MetricsAndLoggingRuntime, config: &BackupConfig) {
    // Start an infinite loop polling the database for blob state statistics and updating the
    // metrics.
    let backup_db_metric_set = BackupDbMetricSet::new(&metrics_runtime.registry);
    let database_url = config.db_config.database_url.clone();

    tokio::spawn(async move {
        let mut conn =
            establish_connection_async(&database_url, "db connect for db metrics polling")
                .await
                .expect("failed to connect to postgres for db metrics polling");

        loop {
            // TODO: add documentation for the stats fetched here.
            let stats: Vec<DbStatistic> = diesel::sql_query(
                "
                    SELECT
                        state AS name,
                        COUNT(*)::bigint AS value
                    FROM blob_state
                    WHERE
                        state = 'archived' AND
                        COALESCE(end_epoch > (SELECT MAX(epoch) FROM epoch_change_start_event),
                                    FALSE)
                    GROUP BY 1
                    UNION
                    SELECT
                        state AS name,
                        COUNT(*)::bigint AS value
                    FROM blob_state
                    WHERE
                        state = 'deleted' OR
                        (state = 'waiting' AND
                            COALESCE(end_epoch > (SELECT MAX(epoch) FROM epoch_change_start_event),
                                    TRUE))

                    GROUP BY 1
                    UNION
                    SELECT
                        'garbage' AS name,
                        COUNT(*)::bigint AS value
                    FROM blob_state
                    WHERE
                        state = 'archived' AND
                        COALESCE(end_epoch <= (SELECT MAX(epoch) FROM epoch_change_start_event),
                                    TRUE)
                    UNION
                    SELECT
                        'total_bytes_archived' AS name,
                        COALESCE(SUM(size), 0)::bigint AS value
                    FROM blob_state
                    WHERE state = 'archived';
                    ",
            )
            .get_results(&mut conn)
            .await
            .inspect_err(|error: &Error| {
                tracing::error!(
                    ?error,
                    "encountered an error querying for blob state statistics"
                );
            })
            .unwrap_or_default();

            tracing::info!(?stats, "fetched blob state statistics");
            for stat in stats {
                // Note that the above query is a bit overloaded in order to reduce the number of
                // roundtrips to the db.
                if stat.name == "total_bytes_archived" {
                    backup_db_metric_set
                        .total_bytes_archived
                        .set(stat.value as f64);
                } else {
                    backup_db_metric_set
                        .blob_states
                        .with_label_values(&[&stat.name])
                        .set(stat.value as f64);
                }
            }
            tokio::time::sleep(Duration::from_secs(30)).await;
        }
    });
}

/// Starts a new backup node runtime.
pub async fn start_backup_fetcher(
    version: &'static str,
    config: BackupConfig,
    metrics_runtime: &MetricsAndLoggingRuntime,
) -> Result<()> {
    tracing::info!(?config, version, "starting backup node");
    if config.backup_bucket.is_none() {
        // If the backup bucket is not set, we need to create the backup archive storage dir.
        let backup_path_dir = config.backup_storage_path.join(BACKUP_BLOB_ARCHIVE_SUBDIR);
        if tokio::fs::metadata(&backup_path_dir).await.is_err() {
            tracing::info!(?backup_path_dir, "creating backup storage directory");
            tokio::fs::create_dir_all(&backup_path_dir).await?;
        } else {
            tracing::info!(?backup_path_dir, "backup storage directory already exists");
        }
    }
    let registry_clone = metrics_runtime.registry.clone();
    tokio::spawn(async move {
        registry_clone
            .register(mysten_metrics::uptime_metric(
                "walrus_backup_fetcher",
                version,
                "walrus",
            ))
            .expect("metrics defined at compile time must be valid");
    });

    tracing::info!(
        metrics_address = %config.metrics_address, "started Prometheus HTTP endpoint",
    );

    utils::export_build_info(&metrics_runtime.registry, version);

    let backup_fetcher_metric_set = BackupFetcherMetricSet::new(&metrics_runtime.registry);
    backup_fetcher(version, config, backup_fetcher_metric_set).await
}

/// Read the oldest un-fetched blob states from the database and return their BlobIds.
///
/// Note that this function mutates the database when it "takes" a blob_state job from the database
/// by pushing the `initiate_fetch_after` timestamp forward into the future by some configured
/// amount (to prevent other workers from also claiming this task.)
async fn backup_take_tasks(
    conn: &mut AsyncPgConnection,
    backup_fetcher_metric_set: &BackupFetcherMetricSet,
    retry_fetch_after_interval: Duration,
    max_retries_per_blob: u32,
    blob_job_chunk_size: u32,
    db_config: &BackupDbConfig,
) -> Vec<BlobId> {
    let max_retries_per_blob =
        i32::try_from(max_retries_per_blob).expect("max_retries_per_blob config overflow");
    let blob_job_chunk_size =
        i32::try_from(blob_job_chunk_size).expect("blob_job_chunk_size config overflow");
    let retry_fetch_after_interval_seconds = i32::try_from(retry_fetch_after_interval.as_secs())
        .expect("retry_fetch_after_interval_seconds config overflow");
    // Poll the db for a new work item.
    let blob_id_rows: Option<Vec<BlobIdRow>> = retry_serializable_query(
        conn,
        Location::caller(),
        db_config,
        &backup_fetcher_metric_set
            .db_serializability_retries
            .with_label_values(&["take_task"]),
        &backup_fetcher_metric_set.db_reconnects,
        |conn| {
            async {
                // This query will fetch the next blobs that are in the waiting state and are ready
                // to be fetched. It will also update their initiate_fetch_after timestamp to give
                // this backup_fetcher worker time to conduct the fetches, and the pushes to GCS.
                //
                // The backoff interval is calculated to be an exponential function of the retry
                // count, with a maximum of 24 hours. The exponential base is 1.5, which means that
                // the backoff interval will increase by 50% with each retry.
                //
                // This query will also ensure that the blob is still unexpired by checking the
                // end_epoch of the blob against the latest epoch_change_start_event.
                diesel::sql_query(
                    "WITH ready_blob_ids AS (
                            SELECT blob_id FROM blob_state
                            WHERE
                                state = 'waiting'
                                AND blob_state.initiate_fetch_after < NOW()
                                AND blob_state.retry_count < $1
                                AND COALESCE(
                                    blob_state.end_epoch > (SELECT MAX(epoch)
                                                            FROM epoch_change_start_event),
                                    TRUE)
                            ORDER BY blob_state.initiate_fetch_after ASC
                            LIMIT $2
                        ),
                        _updated_count AS (
                            UPDATE blob_state
                            SET
                                initiate_fetch_after =
                                    NOW()
                                    + LEAST(86400, ($3 / 1.5) * POW(1.5, retry_count))
                                        * INTERVAL '1 second',
                                retry_count = retry_count + 1
                            WHERE blob_id IN (SELECT blob_id FROM ready_blob_ids)
                        )
                        SELECT blob_id FROM ready_blob_ids",
                )
                .bind::<Int4, _>(max_retries_per_blob)
                .bind::<Int4, _>(blob_job_chunk_size)
                .bind::<Int4, _>(retry_fetch_after_interval_seconds)
                .get_results(conn)
                .await
            }
            .scope_boxed()
        },
    )
    .await
    .inspect_err(|error: &Error| {
        tracing::error!(?error, "encountered an error querying for ready blob_ids");
    })
    .ok();
    let Some(blob_id_rows) = blob_id_rows else {
        // Something went wrong, or we encountered an error, which we should have logged. So let's
        // just return an empty list indicating there is no work to do.
        return Vec::new();
    };
    tracing::debug!(
        count = blob_id_rows.len(),
        "[backup_delegator] found blobs in waiting state",
    );
    blob_id_rows
        .into_iter()
        .map(|row| BlobId::try_from(row.blob_id.as_slice()).expect("bad blob_id found in db!"))
        .collect()
}

async fn backup_fetcher(
    version: &'static str,
    backup_config: BackupConfig,
    backup_metric_set: BackupFetcherMetricSet,
) -> Result<()> {
    tracing::info!("[backup_fetcher] starting worker");
    let mut conn =
        establish_connection_async(&backup_config.db_config.database_url, "backup_fetcher")
            .await
            .context("[backup_fetcher] connecting to postgres")?;
    let sui_read_client = SuiReadClient::new(
        RetriableSuiClient::new_for_rpc_urls(
            &combine_rpc_urls(
                &backup_config.sui.rpc,
                &backup_config.sui.additional_rpc_endpoints,
            ),
            backup_config.sui.backoff_config.clone(),
            None,
        )
        .await
        .context("[backup_fetcher] cannot create RetriableSuiClient")?,
        &backup_config.sui.contract_config,
    )
    .await
    .context("[backup_fetcher] cannot create SuiReadClient")?;

    let walrus_client_config =
        ClientConfig::new_from_contract_config(backup_config.sui.contract_config.clone());

    let read_client =
        Client::new_read_client_with_refresher(walrus_client_config, sui_read_client.clone())
            .await?;

    let mut consecutive_fetch_errors = 0;
    loop {
        // Fetch the next several blobs to work on.
        let blob_ids = backup_take_tasks(
            &mut conn,
            &backup_metric_set,
            backup_config.retry_fetch_after_interval,
            backup_config.max_retries_per_blob,
            backup_config.blob_job_chunk_size,
            &backup_config.db_config,
        )
        .await;
        if !blob_ids.is_empty() {
            for blob_id in blob_ids {
                match backup_fetch_inner_core(
                    version,
                    &mut conn,
                    &backup_config,
                    &backup_metric_set,
                    &read_client,
                    blob_id,
                )
                .await
                {
                    Ok(()) => {
                        consecutive_fetch_errors = 0;
                    }
                    Err(error) => {
                        // Handle the error, report it, and continue polling for work to do.
                        consecutive_fetch_errors += 1;
                        tracing::error!(?error, consecutive_fetch_errors, "[backup_fetcher] error");
                        tokio::time::sleep(FETCHER_ERROR_BACKOFF).await;
                    }
                }
                backup_metric_set
                    .consecutive_blob_fetch_errors
                    .set(f64::from(consecutive_fetch_errors));
            }
        } else {
            // Nothing to fetch. We are idle. Let's rest a bit.
            backup_metric_set.idle_state.set(1.0);
            tokio::time::sleep(backup_config.idle_fetcher_sleep_time).await;
            backup_metric_set.idle_state.set(0.0);
        }
    }
}

#[tracing::instrument(skip_all)]
async fn backup_fetch_inner_core(
    version: &'static str,
    conn: &mut AsyncPgConnection,
    backup_config: &BackupConfig,
    backup_metric_set: &BackupFetcherMetricSet,
    read_client: &WalrusNodeClient<SuiReadClient>,
    blob_id: BlobId,
) -> Result<()> {
    tracing::info!(blob_id = %blob_id, "[backup_fetcher] received work item");

    // Fetch the blob from Walrus network.
    let timer_guard = backup_metric_set.blob_fetch_duration.start_timer();
    let blob: Vec<u8> = match read_client.read_blob::<Primary>(&blob_id).await {
        Ok(blob) => {
            let fetch_time = Duration::from_secs_f64(timer_guard.stop_and_record());
            backup_metric_set.blobs_fetched.inc();
            backup_metric_set
                .blob_bytes_fetched
                .inc_by(blob.len() as u64);
            tracing::info!(blob_id = %blob_id, ?fetch_time,
                "[blob_fetcher] fetched blob from network");
            blob
        }
        Err(error) => {
            timer_guard.stop_and_discard();
            backup_metric_set
                .blob_fetch_errors
                .with_label_values(&[error.kind().label()])
                .inc();
            tracing::error!(?error, %blob_id, "[backup_fetcher] error reading blob");
            return Err(error.into());
        }
    };

    // Store the blob in the backup storage (Google Cloud Storage or fallback to filesystem).
    let timer_guard = backup_metric_set.blob_upload_duration.start_timer();
    let blob_len = blob.len();
    let md5 = md5::compute(&blob).0;
    let sha256 = sha2::Sha256::digest(&blob).to_vec();
    match upload_blob_to_storage(blob_id, blob, backup_config).await {
        Ok(backup_url) => {
            backup_metric_set.blob_bytes_uploaded.inc_by(
                blob_len
                    .try_into()
                    .expect("blob_len is guaranteed tofit into a u64"),
            );
            let upload_time = Duration::from_secs_f64(timer_guard.stop_and_record());
            let affected_rows: usize = retry_serializable_query(
                conn,
                Location::caller(),
                &backup_config.db_config,
                &backup_metric_set
                    .db_serializability_retries
                    .with_label_values(&["uploaded_blob"]),
                &backup_metric_set.db_reconnects,
                |conn| {
                    async {
                        diesel::sql_query(
                            "UPDATE blob_state
                                SET state = 'archived',
                                    backup_url = $1,
                                    initiate_fetch_after = NULL,
                                    retry_count = NULL,
                                    last_error = NULL,
                                    size = $2,
                                    md5 = $3,
                                    sha256 = $4,
                                    fetcher_version = $5
                                WHERE
                                    blob_id = $6
                                    AND backup_url IS NULL
                                    AND state = 'waiting'",
                        )
                        .bind::<Text, _>(&backup_url)
                        .bind::<Int8, _>(
                            i64::try_from(blob_len)
                                .expect("blob_len is guaranteed to fit into a i64"),
                        )
                        .bind::<Bytea, _>(md5)
                        .bind::<Bytea, _>(&sha256)
                        .bind::<Text, _>(version)
                        .bind::<Bytea, _>(blob_id.as_ref().to_vec())
                        .execute(conn)
                        .await
                    }
                    .scope_boxed()
                },
            )
            .await?;
            tracing::info!(
                affected_rows,
                blob_id = %blob_id,
                ?upload_time,
                "[backup_fetcher] attempted update to blob_state"
            );
            backup_metric_set.blobs_uploaded.inc();
            Ok(())
        }
        Err(error) => {
            timer_guard.stop_and_discard();
            tracing::error!(?error, %blob_id, "error uploading blob to storage");

            // Update the database to indicate what went wrong and enable faster debugging. Ignore
            // errors here as this is just a nice-to-have.
            let _ = diesel::sql_query(
                "UPDATE blob_state
                    SET last_error = $1,
                        fetcher_version = $2
                    WHERE blob_id = $3",
            )
            .bind::<Text, _>(error.to_string())
            .bind::<Text, _>(version)
            .bind::<Bytea, _>(blob_id.as_ref().to_vec())
            .execute(conn)
            .await;
            return Err(error);
        }
    }
}

#[tracing::instrument(skip_all)]
async fn upload_blob_to_storage(
    blob_id: BlobId,
    blob: Vec<u8>,
    backup_config: &BackupConfig,
) -> Result<String> {
    let blob_size = blob.len();
    let (store, blob_url): (Box<dyn ObjectStore>, String) =
        if let Some(backup_bucket) = backup_config.backup_bucket.as_deref() {
            (
                Box::new(
                    GoogleCloudStorageBuilder::from_env()
                        .with_client_options(
                            object_store::ClientOptions::default()
                                .with_timeout(backup_config.blob_upload_timeout),
                        )
                        .with_bucket_name(backup_bucket.to_string())
                        .build()?,
                ),
                format!("gs://{backup_bucket}/{blob_id}"),
            )
        } else {
            (
                Box::new(LocalFileSystem::new_with_prefix(
                    backup_config
                        .backup_storage_path
                        .join(BACKUP_BLOB_ARCHIVE_SUBDIR),
                )?),
                format!(
                    "file://{}",
                    backup_config
                        .backup_storage_path
                        .join(BACKUP_BLOB_ARCHIVE_SUBDIR)
                        .join(blob_id.to_string())
                        .display()
                ),
            )
        };
    let put_result: object_store::PutResult =
        store.put(&blob_id.to_string().into(), blob.into()).await?;
    tracing::info!(
        blob_id = %blob_id,
        blob_size,
        ?put_result,
        "[upload_blob_to_storage] uploaded blob",
    );
    Ok(blob_url)
}

pub(crate) async fn retry_serializable_query<'a, 'b, T, F>(
    conn: &mut AsyncPgConnection,
    callsite: &'static Location<'static>,
    db_config: &BackupDbConfig,
    retry_counter: &GenericCounter<AtomicU64>,
    db_reconnects: &GenericCounter<AtomicU64>,
    f: F,
) -> std::result::Result<T, Error>
where
    F: for<'r> FnMut(
            &'r mut AsyncPgConnection,
        ) -> scoped_futures::ScopedBoxFuture<'b, 'r, Result<T, Error>>
        + Send
        + Clone
        + 'a,
    T: 'b,
{
    let starting_retry_count = retry_counter.get();
    loop {
        match conn
            .build_transaction()
            .serializable()
            .run::<_, Error, _>(f.clone())
            .await
        {
            Ok(value) => break Ok(value),
            Err(Error::DatabaseError(DatabaseErrorKind::SerializationFailure, detail)) => {
                tracing::warn!(
                    ?detail,
                    ?callsite,
                    retry_count = retry_counter.get() - starting_retry_count,
                    "encountered a SerializationFailure, retrying"
                );
                retry_counter.inc();
                // Retry after a short delay.
                tokio::time::sleep(db_config.db_serializability_retry_time).await;
                continue;
            }
            Err(error) if retry_counter.get() - starting_retry_count < 3 => {
                // This is a bit of a broken situation, since we don't understand the failure. but
                // we can try to reconnect to the database a few times before giving up.
                tracing::error!(
                    ?error,
                    ?callsite,
                    retry_count = retry_counter.get() - starting_retry_count,
                    ?db_config.db_reconnect_wait_time,
                    "unexpected error within retry_serializable_query. attempting to reconnect"
                );
                retry_counter.inc();
                tokio::time::sleep(db_config.db_reconnect_wait_time).await;

                // Implicitly drop the prior connection and create a new one.
                *conn =
                    establish_connection_async(&db_config.database_url, "retry_serializable_query")
                        .await
                        .expect("attempt to reconnect failed");
                db_reconnects.inc();
            }
            Err(error) => {
                panic!("final error within retry_serializable_query: {error:?}");
            }
        }
    }
}

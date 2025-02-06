// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Backup service implementation.

use std::{pin::Pin, sync::Arc, time::Duration};

use anyhow::{bail, Context, Result};
use diesel::Connection as _;
use diesel_async::{
    scoped_futures::ScopedFutureExt,
    AsyncConnection as _,
    AsyncPgConnection,
    RunQueryDsl as _,
};
use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};
use futures::{stream, StreamExt};
use object_store::{gcp::GoogleCloudStorageBuilder, local::LocalFileSystem, ObjectStore};
use prometheus::Registry;
use sui_types::event::EventID;
use tokio_util::sync::CancellationToken;
use walrus_core::{encoding::Primary, BlobId};
use walrus_sui::{
    client::{retry_client::RetriableSuiClient, SuiReadClient},
    types::{BlobEvent, ContractEvent, EpochChangeEvent, EpochChangeStart},
};

use super::{
    config::BackupConfig,
    models::{self, BlobIdRow, StreamEvent},
    schema,
    BACKUP_BLOB_ARCHIVE_SUBDIR,
};
use crate::{
    client::{
        config::{ClientCommunicationConfig, Config as ClientConfig},
        Client,
    },
    common::utils::{self, version, MetricsAndLoggingRuntime},
    node::{
        events::{
            event_processor::EventProcessor,
            event_processor_runtime::EventProcessorRuntime,
            CheckpointEventPosition,
            EventStreamElement,
            PositionedStreamEvent,
        },
        system_events::SystemEventProvider as _,
    },
};

/// The version of the Walrus backup service.
pub const VERSION: &str = version!();
const FETCHER_ERROR_BACKOFF: Duration = Duration::from_secs(1);

pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

async fn stream_events(
    event_processor: Arc<EventProcessor>,
    _metrics_registry: Registry,
    mut pg_connection: AsyncPgConnection,
) -> Result<()> {
    let event_cursor = models::get_backup_node_cursor(&mut pg_connection).await?;
    tracing::info!(?event_cursor, "[stream_events] starting");
    let event_stream = Pin::from(event_processor.events(event_cursor).await?);
    let next_event_index = event_cursor.element_index;
    let index_stream = stream::iter(next_event_index..);
    let mut indexed_element_stream = index_stream.zip(event_stream);
    while let Some((
        element_index,
        PositionedStreamEvent {
            element,
            checkpoint_event_position,
        },
    )) = indexed_element_stream.next().await
    {
        match &element {
            EventStreamElement::ContractEvent(ref contract_event) => {
                record_event(
                    &mut pg_connection,
                    &element,
                    checkpoint_event_position,
                    element_index,
                    contract_event,
                )
                .await?;
            }
            EventStreamElement::CheckpointBoundary => {
                // Skip checkpoint boundaries as they are not relevant for the backup node.
                continue;
            }
        }
    }

    bail!("event stream for blob events stopped")
}

async fn record_event(
    pg_connection: &mut AsyncPgConnection,
    element: &EventStreamElement,
    checkpoint_event_position: CheckpointEventPosition,
    element_index: u64,
    contract_event: &ContractEvent,
) -> Result<(), anyhow::Error> {
    let event_id: EventID = element.event_id().unwrap();
    pg_connection
        .build_transaction()
        .serializable()
        .run::<_, anyhow::Error, _>(|conn| {
            async move {
                diesel::insert_into(schema::stream_event::dsl::stream_event)
                    .values(&StreamEvent::new(
                        checkpoint_event_position,
                        event_id.tx_digest.into_inner(),
                        event_id.event_seq,
                        element_index,
                        element,
                    )?)
                    .execute(conn)
                    .await?;

                dispatch_contract_event(contract_event, conn).await
            }
            .scope_boxed()
        })
        .await?;
    Ok(())
}

async fn dispatch_contract_event(
    contract_event: &ContractEvent,
    conn: &mut AsyncPgConnection,
) -> Result<()> {
    match contract_event {
        // Note that certifying the same blob twice might result in resetting the retry_count. This
        // automatically gives the re-certified blob another chance to be backed up without human
        // intervention.
        ContractEvent::BlobEvent(BlobEvent::Certified(blob_certified)) => {
            diesel::dsl::sql_query(
                "
                INSERT INTO blob_state (
                    blob_id,
                    state,
                    end_epoch,
                    orchestrator_version,
                    retry_count
                )
                VALUES ($1, 'waiting', $2, $3, 1)
                ON CONFLICT (blob_id)
                DO UPDATE SET
                    end_epoch = GREATEST(EXCLUDED.end_epoch, blob_state.end_epoch),
                    state = CASE WHEN
                        blob_state.state = 'archived' THEN
                            blob_state.state
                        ELSE
                            'waiting'
                        END,
                    retry_count = CASE WHEN
                        blob_state.state = 'waiting' THEN
                            1
                        ELSE
                            NULL
                        END,
                    orchestrator_version = $3",
            )
            .bind::<diesel::sql_types::Bytea, _>(blob_certified.blob_id.0.to_vec())
            .bind::<diesel::sql_types::Int8, _>(i64::from(blob_certified.end_epoch))
            .bind::<diesel::sql_types::Text, _>(VERSION)
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
            .bind::<diesel::sql_types::Int8, _>(i64::from(*epoch))
            .execute(conn)
            .await?;
            tracing::info!(epoch, "a new walrus epoch has begun");
        }
        _ => {}
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
async fn establish_connection_async(
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
    let mut connection =
        establish_connection(&config.database_url, "run_backup_database_migrations")
            .inspect_err(|error| {
                tracing::error!(
                    ?error,
                    "failed to connect to postgres for database migration"
                );
                std::process::exit(1);
            })
            .unwrap();
    tracing::info!("running pending migrations");
    let versions = connection
        .run_pending_migrations(MIGRATIONS)
        .inspect_err(|error| {
            tracing::error!(?error, "failed to run pending migrations");
            std::process::exit(1);
        })
        .unwrap();
    tracing::info!(?versions, "migrations ran successfully");
}

/// Starts a new backup node runtime.
pub async fn start_backup_orchestrator(
    config: BackupConfig,
    metrics_runtime: &MetricsAndLoggingRuntime,
) -> Result<()> {
    tracing::info!(?config, "starting backup node");

    let registry_clone = metrics_runtime.registry.clone();
    tokio::spawn(async move {
        registry_clone
            .register(mysten_metrics::uptime_metric(
                "walrus_backup_orchestrator",
                VERSION,
                "walrus",
            ))
            .unwrap();
    });

    tracing::info!(version = VERSION, "Walrus backup binary version");
    tracing::info!(
        metrics_address = %config.metrics_address, "started Prometheus HTTP endpoint",
    );

    utils::export_build_info(&metrics_runtime.registry, VERSION);

    let cancel_token = CancellationToken::new();

    let event_processor = EventProcessorRuntime::start_async(
        config.sui.clone(),
        config.event_processor_config.clone(),
        &config.backup_storage_path,
        &metrics_runtime.registry,
        cancel_token.child_token(),
    )
    .await?;

    let metrics_registry = metrics_runtime.registry.clone();

    // Connect to the database.
    let pg_connection =
        establish_connection_async(&config.database_url, "start_backup_node").await?;

    // Stream events from Sui and pull them into our main business logic workflow.
    stream_events(event_processor, metrics_registry, pg_connection).await
}

/// Starts a new backup node runtime.
pub async fn start_backup_fetcher(
    config: BackupConfig,
    metrics_runtime: &MetricsAndLoggingRuntime,
) -> Result<()> {
    tracing::info!(?config, "starting backup node");
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
                VERSION,
                "walrus",
            ))
            .unwrap();
    });

    tracing::info!(version = VERSION, "Walrus backup binary version");
    tracing::info!(
        metrics_address = %config.metrics_address, "started Prometheus HTTP endpoint",
    );

    utils::export_build_info(&metrics_runtime.registry, VERSION);

    backup_fetcher(config).await
}

/// Read the oldest un-fetched blob state from the database and return its BlobId.
///
/// Note that this function mutates the database when it "takes" a blob_state job from the database
/// by pushing the `initiate_fetch_after` timestamp forward into the future by some configured
/// amount (to prevent other workers from also claiming this task.)
async fn backup_take_task(
    conn: &mut AsyncPgConnection,
    retry_fetch_after_interval: Duration,
    max_retries_per_blob: u32,
) -> Option<BlobId> {
    let max_retries_per_blob =
        i32::try_from(max_retries_per_blob).expect("max_retries_per_blob config overflow");
    let retry_fetch_after_interval_seconds = i32::try_from(retry_fetch_after_interval.as_secs())
        .expect("retry_fetch_after_interval_seconds config overflow");
    // Poll the db for a new work item.
    let blob_id_rows: Vec<BlobIdRow> = conn
        .build_transaction()
        .serializable()
        .run::<_, anyhow::Error, _>(|conn| {
            async move {
                // This query will fetch the next blob that is in the waiting state and is ready to
                // be fetched. It will also update its initiate_fetch_after timestamp to give this
                // backup_fetcher worker time to conduct the fetch, and the push to GCS.
                Ok(diesel::sql_query(
                    "WITH ready_blob_ids AS (
                        SELECT blob_id FROM blob_state
                        WHERE
                            state = 'waiting'
                            AND blob_state.initiate_fetch_after < NOW()
                            AND blob_state.retry_count < $1
                            AND LENGTH(blob_state.blob_id) = 32
                        ORDER BY blob_state.initiate_fetch_after ASC
                        LIMIT 1
                    ),
                    _updated_count AS (
                        UPDATE blob_state
                        SET
                            initiate_fetch_after = NOW() + $2 * INTERVAL '1 second',
                            retry_count = retry_count + 1
                        WHERE blob_id IN (SELECT blob_id FROM ready_blob_ids)
                    )
                    SELECT blob_id FROM ready_blob_ids",
                )
                .bind::<diesel::sql_types::Int4, _>(max_retries_per_blob)
                .bind::<diesel::sql_types::Int4, _>(retry_fetch_after_interval_seconds)
                .get_results(conn)
                .await?)
            }
            .scope_boxed()
        })
        .await
        .inspect_err(|error| {
            tracing::error!(?error, "encountered an error querying for ready blob_ids");
        })
        .ok()?;

    tracing::debug!(
        count = blob_id_rows.len(),
        "[backup_delegator] found blobs in waiting state",
    );
    blob_id_rows.into_iter().next().map(|row| {
        row.blob_id
            .as_slice()
            .try_into()
            .expect("bad blob_id found in db!")
    })
}

async fn backup_fetcher(backup_config: BackupConfig) -> Result<()> {
    tracing::info!("[backup_fetcher] starting worker");
    let mut conn = establish_connection_async(&backup_config.database_url, "backup_fetcher")
        .await
        .context("[backup_fetcher] connecting to postgres")?;
    let sui_read_client = SuiReadClient::new(
        RetriableSuiClient::new_for_rpc(
            &backup_config.sui.rpc,
            backup_config.sui.backoff_config.clone(),
        )
        .await
        .context("[backup_fetcher] cannot create RetriableSuiClient")?,
        &backup_config.sui.contract_config,
    )
    .await
    .context("[backup_fetcher] cannot create SuiReadClient")?;

    let walrus_client_config = ClientConfig {
        contract_config: backup_config.sui.contract_config.clone(),
        exchange_objects: vec![],
        wallet_config: None,
        communication_config: ClientCommunicationConfig::default(),
        refresh_config: Default::default(),
    };

    let read_client =
        Client::new_read_client_with_refresher(walrus_client_config, sui_read_client.clone())
            .await?;

    let mut consecutive_fetch_errors = 0;
    loop {
        if let Some(blob_id) = backup_take_task(
            &mut conn,
            backup_config.retry_fetch_after_interval,
            backup_config.max_retries_per_blob,
        )
        .await
        {
            match backup_fetch_inner_core(&mut conn, &backup_config, &read_client, blob_id).await {
                Ok(()) => {
                    consecutive_fetch_errors = 0;
                }
                Err(error) => {
                    // Handle the error, report it, and continue polling for work to do.
                    consecutive_fetch_errors += 1;
                    tracing::error!(consecutive_fetch_errors, ?error, "[backup_fetcher] error");
                    tokio::time::sleep(FETCHER_ERROR_BACKOFF).await;
                }
            }
        } else {
            // Nothing to fetch. We are idle. Let's rest a bit.
            tokio::time::sleep(backup_config.idle_fetcher_sleep_time).await;
        }
    }
}

#[tracing::instrument(skip_all)]
async fn backup_fetch_inner_core(
    conn: &mut AsyncPgConnection,
    backup_config: &BackupConfig,
    read_client: &Client<SuiReadClient>,
    blob_id: BlobId,
) -> Result<()> {
    tracing::info!(blob_id = %blob_id, "[backup_fetcher] received work item");
    // Fetch the blob from Walrus network.
    let blob: Vec<u8> = read_client
        .read_blob::<Primary>(&blob_id)
        .await
        .inspect_err(|error| {
            tracing::error!(?error, %blob_id, "[backup_fetcher] error reading blob");
        })?;
    tracing::info!(blob_id = %blob_id, "[blob_fetcher] fetched blob from network");
    // Store the blob in the backup storage (Google Cloud Storage or fallback to filesystem).
    match upload_blob_to_storage(blob_id, blob, backup_config).await {
        Ok(backup_url) => {
            let affected_rows = conn
                .build_transaction()
                .serializable()
                .run::<_, anyhow::Error, _>(|conn| {
                    async move {
                        Ok(diesel::sql_query(
                            "UPDATE blob_state
                                SET state = 'archived',
                                    backup_url = $1,
                                    initiate_fetch_after = NULL,
                                    retry_count = NULL,
                                    last_error = NULL,
                                    fetcher_version = $2
                                WHERE
                                    blob_id = $3
                                    AND backup_url IS NULL
                                    AND state = 'waiting'",
                        )
                        .bind::<diesel::sql_types::Text, _>(backup_url)
                        .bind::<diesel::sql_types::Text, _>(VERSION)
                        .bind::<diesel::sql_types::Bytea, _>(blob_id.as_ref().to_vec())
                        .execute(conn)
                        .await?)
                    }
                    .scope_boxed()
                })
                .await?;
            tracing::info!(
                affected_rows,
                blob_id = %blob_id,
                "[backup_fetcher] attempted update to blob_state"
            );
            Ok(())
        }
        Err(error) => {
            tracing::error!(?error, %blob_id, "error uploading blob to storage");

            // Update the database to indicate what went wrong and enable faster debugging. Ignore
            // errors here as this is just a nice-to-have.
            let _ = diesel::sql_query(
                "UPDATE blob_state
                    SET last_error = $1,
                        fetcher_version = $2
                    WHERE blob_id = $3",
            )
            .bind::<diesel::sql_types::Text, _>(error.to_string())
            .bind::<diesel::sql_types::Text, _>(VERSION)
            .bind::<diesel::sql_types::Bytea, _>(blob_id.as_ref().to_vec())
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
    let (store, blob_url): (Box<dyn ObjectStore>, String) =
        if let Some(backup_bucket) = backup_config.backup_bucket.as_deref() {
            (
                Box::new(
                    GoogleCloudStorageBuilder::from_env()
                        .with_bucket_name(backup_bucket.to_string())
                        .build()?,
                ),
                format!("gs://{}/{}", backup_bucket, blob_id),
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
        ?put_result,
        "[upload_blob_to_storage] uploaded blob",
    );
    Ok(blob_url)
}

// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Walrus Backup node.
///
/// Walrus Backup Node.
///
use std::{net::SocketAddr, path::PathBuf, pin::Pin, sync::Arc};

use anyhow::{bail, Context, Result};
use diesel_async::{
    scoped_futures::ScopedFutureExt,
    AsyncConnection as _,
    AsyncPgConnection,
    RunQueryDsl as _,
};
use futures::{stream, StreamExt};
use prometheus::Registry;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use sui_types::event::EventID;
use walrus_core::{encoding::Primary, BlobId};
use walrus_sui::{
    client::{retry_client::RetriableSuiClient, SuiReadClient},
    types::{BlobEvent, ContractEvent, EpochChangeEvent, EpochChangeStart},
};

use self::models::{get_backup_node_cursor, StreamEvent};
use crate::{
    client::Client,
    common::{config::SuiReaderConfig, utils},
    node::{
        events::{
            event_processor::EventProcessor,
            CheckpointEventPosition,
            EventProcessorConfig,
            EventStreamElement,
            PositionedStreamEvent,
        },
        system_events::SystemEventProvider as _,
    },
};

mod models;
mod schema;

const WORKER_COUNT: usize = 4;

/// Configuration of a Walrus backup node.
#[serde_as]
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BackupNodeConfig {
    /// Directory in which to persist the database
    #[serde(deserialize_with = "utils::resolve_home_dir")]
    pub backup_storage_path: PathBuf,
    /// Socket address on which the Prometheus server should export its metrics.
    #[serde(default = "defaults::metrics_address")]
    pub metrics_address: SocketAddr,
    /// Sui config for the node
    pub sui: SuiReaderConfig,
    /// Configuration for the event processor.
    ///
    /// This is ignored if `use_legacy_event_provider` is set to `true`.
    #[serde(default, skip_serializing_if = "defaults::is_default")]
    pub event_processor_config: EventProcessorConfig,
    /// Database URL.
    ///
    /// URL of the PostgreSQL database used to manage blob backup state and event stream progress.
    #[serde(default, skip_serializing_if = "defaults::is_default")]
    pub database_url: String,
    /// Size of the message queue.
    ///
    /// This is the maximum number of messages that can be stored in the queue before incoming work
    /// items will be pushed into the future.
    #[serde(default = "defaults::message_queue_size")]
    pub message_queue_size: u32,
    /// Maximum number of fetch attempts allowed per blob.
    ///
    /// This is the maximum number of times that a backup_delegator will enqueue a particular blob
    /// for fetching. (This is a safety measure to prevent resource starvation for the other blobs'
    /// backup lifecycles.)
    #[serde(default = "defaults::max_fetch_attempts_per_blob")]
    pub max_fetch_attempts_per_blob: u32,
}

async fn stream_events(
    event_processor: Arc<EventProcessor>,
    _metrics_registry: Registry,
    mut pg_connection: AsyncPgConnection,
) -> Result<()> {
    let event_cursor = get_backup_node_cursor(&mut pg_connection).await?;
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
    use self::schema::stream_event::dsl::stream_event;
    let event_id: EventID = element.event_id().unwrap();
    pg_connection
        .build_transaction()
        .serializable()
        .run::<_, anyhow::Error, _>(|conn| {
            async move {
                diesel::insert_into(stream_event)
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
        ContractEvent::BlobEvent(BlobEvent::Registered(_)) => {}
        ContractEvent::BlobEvent(BlobEvent::InvalidBlobID(_)) => {}
        ContractEvent::BlobEvent(BlobEvent::Deleted(_)) => {}
        ContractEvent::BlobEvent(BlobEvent::DenyListBlobDeleted(_)) => {}
        ContractEvent::BlobEvent(BlobEvent::Certified(blob_certified)) => {
            diesel::dsl::sql_query(
                "INSERT INTO blob_state (blob_id, state, end_epoch, fetch_attempts)
                 VALUES ($1, 'waiting', $2, 0)
                 ON CONFLICT (blob_id)
                 DO UPDATE SET
                     end_epoch = GREATEST(EXCLUDED.end_epoch, blob_state.end_epoch),
                     state = CASE WHEN
                         blob_state.state = 'archived' THEN
                             blob_state.state
                         ELSE
                             'waiting'
                         END,
                     fetch_attempts = CASE WHEN
                         blob_state.state = 'waiting' THEN
                             0
                         ELSE
                             NULL
                         END",
            )
            .bind::<diesel::sql_types::Bytea, _>(blob_certified.blob_id.0.to_vec())
            .bind::<diesel::sql_types::Int8, _>(i64::from(blob_certified.end_epoch))
            .execute(conn)
            .await?;
        }
        ContractEvent::EpochChangeEvent(EpochChangeEvent::EpochChangeStart(EpochChangeStart {
            epoch,
            ..
        })) => {
            diesel::dsl::sql_query(
                "INSERT INTO epoch_change_start_event (epoch) VALUES ($1)
                 ON CONFLICT DO NOTHING",
            )
            .bind::<diesel::sql_types::Int8, _>(i64::from(*epoch))
            .execute(conn)
            .await?;
        }
        ContractEvent::EpochChangeEvent(_) => {}
        ContractEvent::PackageEvent(_package_event) => {}
        ContractEvent::DenyListEvent(_deny_list_event) => {}
    }
    Ok(())
}

async fn establish_connection(database_url: &str) -> Result<AsyncPgConnection> {
    AsyncPgConnection::establish(database_url)
        .await
        .context("error connecting to database")
}

/// Work item for the backup fetcher.
struct BackupWorkItem {
    /// The blob to be fetched and archived.
    blob_id: BlobId,
}

/// Starts a new backup node runtime.
pub async fn start_backup_node(
    metrics_registry: Registry,
    event_processor: Arc<EventProcessor>,
    config: BackupNodeConfig,
) -> Result<()> {
    // Connect to the database, if we are configured to do so.
    let pg_connection = establish_connection(&config.database_url).await?;

    let (tx, rx) =
        async_channel::bounded::<BackupWorkItem>(usize::try_from(config.message_queue_size)?);

    // TODO: In the future we can run these workers on distinct machines and perhaps even in
    // different regions, assuming we have a cross-region queue (0-1 guaranteed delivery). Google
    // Cloud Pub/Sub is a viable choice here. (See WAL-533.)
    for worker_id in 0..WORKER_COUNT {
        let rx = rx.clone();
        let config = config.clone();
        tokio::spawn(async move {
            backup_fetcher(worker_id, config, rx)
                .await
                .with_context(|| format!("backup fetcher thread {worker_id} failed"))
        });
    }

    tokio::spawn(backup_delegator(config.clone(), tx));

    // Stream events from Sui and pull them into our main business logic workflow.
    stream_events(event_processor, metrics_registry, pg_connection)
        .await
        .map_err(|error| {
            tracing::error!(?error, "backup node exited with an error");
            error
        })
}

/// Backup delegator's job is to batch read un-fetched blob states from the database and
/// push them into the work queue. It should be a singleton.
async fn backup_delegator(
    backup_config: BackupNodeConfig,
    tx: async_channel::Sender<BackupWorkItem>,
) -> Result<()> {
    use self::models::BlobIdRow;

    tracing::info!("[backup_delegator] Starting worker with config: {backup_config:?}");
    let mut conn = AsyncPgConnection::establish(&backup_config.database_url).await?;
    let max_fetch_attempts_per_blob = i32::try_from(backup_config.max_fetch_attempts_per_blob)
        .expect("max_fetch_attempts_per_blob config overflow");
    let message_queue_size = i32::try_from(backup_config.message_queue_size)
        .expect("message_queue_size config overflow");
    loop {
        // Poll the db for new work items. Use the message_queue_size as an upper bound.
        let blob_id_rows: Vec<BlobIdRow> = conn
            .build_transaction()
            .serializable()
            .run::<_, anyhow::Error, _>(|conn| {
                async move {
                    // This query will fetch the next `message_queue_size` blobs that are in the
                    // waiting state and are ready to be fetched. It will also update their
                    // initiate_fetch_after timestamp to give a backup_fetcher worker time to
                    // conduct the fetch, and the push to GCS.
                    //
                    // Explanation of the 45 minute interval:
                    //    15 GB at 15 MBps = ~16.7 minutes. Double that to add time to send to GCS.
                    //    Add some extra buffer time to make it 45 minutes.
                    // TODO: Make this configurable. (See WAL-550.)
                    Ok(diesel::sql_query(
                        "WITH ready_blob_states AS (
                            SELECT blob_id FROM blob_state
                            WHERE
                                state = 'waiting'
                                AND blob_state.initiate_fetch_after < NOW()
                                AND blob_state.fetch_attempts < $1
                            LIMIT $2
                        ),
                        _updated_count AS (
                            UPDATE blob_state
                            SET
                                initiate_fetch_after = NOW() + INTERVAL '45 minute',
                                fetch_attempts = fetch_attempts + 1
                            WHERE blob_id IN (SELECT blob_id FROM ready_blob_states)
                        )
                        SELECT blob_id FROM ready_blob_states",
                    )
                    .bind::<diesel::sql_types::Int4, _>(max_fetch_attempts_per_blob)
                    .bind::<diesel::sql_types::Int4, _>(message_queue_size)
                    .get_results(conn)
                    .await?)
                }
                .scope_boxed()
            })
            .await?;

        tracing::info!(
            "[backup_delegator] found {count} blobs in waiting state",
            count = blob_id_rows.len()
        );
        for BlobIdRow { blob_id } in blob_id_rows {
            tx.send(BackupWorkItem {
                blob_id: BlobId::try_from(blob_id.as_slice())?,
            })
            .await?;
        }
        // Give the system a break.
        // TODO: Make this configurable. (See WAL-550.)
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    }
}

async fn backup_fetcher(
    worker_id: usize,
    backup_config: BackupNodeConfig,
    rx: async_channel::Receiver<BackupWorkItem>,
) -> Result<()> {
    let result = backup_fetcher_core(worker_id, backup_config, rx).await;
    panic!("[backup_fetcher:{worker_id}] exited prematurely {result:?}")
}

async fn backup_fetcher_core(
    worker_id: usize,
    backup_config: BackupNodeConfig,
    rx: async_channel::Receiver<BackupWorkItem>,
) -> Result<()> {
    tracing::info!("[backup_fetcher:{worker_id}] starting worker");
    let mut conn = AsyncPgConnection::establish(&backup_config.database_url)
        .await
        .with_context(|| format!("[backup_fetcher:{worker_id}] connecting to postgres"))?;
    let rpc_url = backup_config.sui.rpc;
    let sui_read_client = SuiReadClient::new(
        RetriableSuiClient::new_for_rpc(&rpc_url, backup_config.sui.backoff_config.clone())
            .await
            .with_context(|| {
                format!("[backup_fetcher:{worker_id}] cannot create RetriableSuiClient")
            })?,
        &backup_config.sui.contract_config,
    )
    .await
    .with_context(|| format!("[backup_fetcher:{worker_id}] cannot create SuiReadClient"))?;
    use crate::client::config::{ClientCommunicationConfig, Config as ClientConfig};

    let walrus_client_config = ClientConfig {
        contract_config: backup_config.sui.contract_config.clone(),
        exchange_objects: vec![],
        wallet_config: None,
        communication_config: ClientCommunicationConfig::default(),
    };

    let read_client =
        Client::new_read_client(walrus_client_config, sui_read_client.clone()).await?;

    loop {
        // Pull a work item from the queue.
        let item = rx.recv().await.expect("channel closed!");
        tracing::info!(
            "[backup_fetcher:{worker_id}] processing blob {blob_id:?}",
            blob_id = item.blob_id
        );
        // Fetch the blob from Walrus network.
        let _blob: Vec<u8> = read_client
            .read_blob::<Primary>(&item.blob_id)
            .await
            .context(format!(
                "[backup_fetcher:{worker_id}] error reading blob {blob_id:?}",
                blob_id = item.blob_id
            ))?;
        tracing::info!(
            "[blob_fetcher:{worker_id}] fetched blob {blob_id:?}",
            blob_id = item.blob_id
        );
        // TODO: store the blob in the backup storage (Google Cloud Storage). (See WAL-550.)
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
                             fetch_attempts = NULL
                         WHERE
                             blob_id = $2
                             AND backup_url IS NULL
                             AND state = 'waiting'
                        ",
                    )
                    // TODO: use the real GCS URL. (See WAL-550.)
                    .bind::<diesel::sql_types::Text, _>(format!(
                        "gs://backup/{blob_id}",
                        blob_id = item.blob_id
                    ))
                    .bind::<diesel::sql_types::Bytea, _>(item.blob_id.as_ref().to_vec())
                    .execute(conn)
                    .await?)
                }
                .scope_boxed()
            })
            .await?;
        tracing::info!("[backup_fetcher:{worker_id}] updated {affected_rows} rows");
    }
}

/// Backup-related default values.
pub mod defaults {
    use std::net::{Ipv4Addr, SocketAddr};

    /// Default backup node metrics port.
    pub const METRICS_PORT: u16 = 10184;

    /// Returns the default metrics address.
    pub fn metrics_address() -> SocketAddr {
        (Ipv4Addr::LOCALHOST, METRICS_PORT).into()
    }

    /// Returns true iff the value is the default and we don't run in test mode.
    pub fn is_default<T: PartialEq + Default>(t: &T) -> bool {
        // The `cfg!(test)` check is there to allow serializing the full configuration, specifically
        // to generate the example configuration files.
        !cfg!(test) && t == &T::default()
    }
    /// Default backup work item queue size.
    pub fn message_queue_size() -> u32 {
        100
    }
    /// Default backup max_fetch_attempts_per_blob.
    pub fn max_fetch_attempts_per_blob() -> u32 {
        25
    }
}

// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Walrus Backup node.
///
/// Walrus Backup Node.
///
use std::{net::SocketAddr, path::PathBuf, pin::Pin, sync::Arc};

use anyhow::bail;
use futures::{stream, StreamExt};
use prometheus::Registry;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use tokio::{
    runtime::{self, Runtime},
    select,
    sync::oneshot,
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::{
    common::{
        config::SuiReaderConfig,
        utils::{self, LoadConfig},
    },
    node::{
        events::{EventProcessorConfig, EventStreamCursor},
        system_events::EventManager,
    },
};

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
    /// Use the legacy event provider.
    ///
    /// This is deprecated and will be removed in the future.
    #[serde(default, skip_serializing_if = "defaults::is_default")]
    pub use_legacy_event_provider: bool,
}

impl LoadConfig for BackupNodeConfig {}

/// The Backup node.
#[derive(Debug)]
pub struct BackupNode {
    #[allow(dead_code)]
    metrics_registry: Registry,
    event_manager: Box<dyn EventManager>,
}

impl BackupNode {
    /// Creates a new [`BackupNode`].
    pub fn new(event_manager: Box<dyn EventManager>, metrics_registry: Registry) -> Self {
        Self {
            metrics_registry,
            event_manager,
        }
    }
    /// Runs the backup node.
    pub async fn run(&self, cancel_token: CancellationToken) -> anyhow::Result<()> {
        select! {
            result = self.process_events() => match result {
                Ok(()) => unreachable!("process_events should never return successfully"),
                Err(err) => return Err(err),
            },
            _ = cancel_token.cancelled() => {
            },
        }
        Ok(())
    }

    fn get_storage_node_cursor(&self) -> anyhow::Result<EventStreamCursor> {
        // TODO: restart from a saved cursor. Tracked in WAL-532.
        tracing::warn!("cursor state not yet implemented, starting from the beginning");
        Ok(EventStreamCursor::new(None, 0))
    }

    async fn process_events(&self) -> anyhow::Result<()> {
        let event_cursor = self.get_storage_node_cursor()?;
        let event_stream = Pin::from(self.event_manager.events(event_cursor).await?);
        let next_event_index = event_cursor.element_index;
        let index_stream = stream::iter(next_event_index..);
        let mut indexed_element_stream = index_stream.zip(event_stream);
        while let Some((_element_index, stream_element)) = indexed_element_stream.next().await {
            println!("{}", serde_json::to_string(&stream_element).unwrap());
        }

        bail!("event stream for blob events stopped")
    }
}

/// Backup node runtime lifecycle object.
#[derive(Debug)]
pub struct BackupNodeRuntime {
    backup_node_handle: JoinHandle<anyhow::Result<()>>,
    // INV: Runtime must be dropped last
    runtime: Runtime,
}

impl BackupNodeRuntime {
    /// Starts a new backup node runtime.
    pub fn start(
        metrics_registry: Registry,
        exit_notifier: oneshot::Sender<()>,
        event_manager: Box<dyn EventManager>,
        cancel_token: CancellationToken,
    ) -> anyhow::Result<Self> {
        let runtime = runtime::Builder::new_multi_thread()
            .thread_name("walrus-backup-runtime")
            .enable_all()
            .build()
            .expect("walrus-node runtime creation must succeed");
        let _guard = runtime.enter();

        let backup_node = Arc::new(BackupNode::new(event_manager, metrics_registry));

        let backup_node_cancel_token = cancel_token.child_token();
        let backup_node_handle = tokio::spawn(async move {
            let cancel_token = backup_node_cancel_token.clone();

            // Actually run the backup node!
            let result = backup_node.run(backup_node_cancel_token).await;

            if exit_notifier.send(()).is_err() && !cancel_token.is_cancelled() {
                tracing::warn!(
                    "unable to notify that the backup node has exited, \
                        but shutdown is not in progress?"
                )
            }
            if let Err(ref error) = result {
                tracing::error!(?error, "backup node exited with an error");
            }

            result
        });

        Ok(Self {
            runtime,
            backup_node_handle,
        })
    }

    /// Waits for the backup node to shutdown.
    pub fn join(&mut self) -> Result<(), anyhow::Error> {
        self.runtime.block_on(&mut self.backup_node_handle)?
    }
}

mod defaults {
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
}

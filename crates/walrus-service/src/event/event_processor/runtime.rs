// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Walrus event processor runtime.

use std::{path::Path, sync::Arc};

use anyhow::Context;
use tokio::{
    runtime::{self, Runtime},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use walrus_sdk::config::combine_rpc_urls;
use walrus_utils::metrics::Registry;

use crate::{
    common::config::SuiReaderConfig,
    event::event_processor::{
        config::{EventProcessorConfig, EventProcessorRuntimeConfig, SystemConfig},
        processor::EventProcessor,
    },
    node::{
        DatabaseConfig,
        system_events::{EventManager, SuiSystemEventProvider},
    },
};

/// Event processor runtime.
#[derive(Debug)]
pub struct EventProcessorRuntime {
    event_processor_handle: JoinHandle<anyhow::Result<()>>,
    // INV: Runtime must be dropped last.
    runtime: Runtime,
}

impl EventProcessorRuntime {
    async fn build_event_processor(
        sui_reader_config: &SuiReaderConfig,
        event_processor_config: &EventProcessorConfig,
        db_path: &Path,
        metrics_registry: &Registry,
        db_config: &DatabaseConfig,
    ) -> anyhow::Result<Arc<EventProcessor>> {
        let runtime_config = EventProcessorRuntimeConfig {
            rpc_addresses: combine_rpc_urls(
                &sui_reader_config.rpc,
                &sui_reader_config.additional_rpc_endpoints,
            ),
            event_polling_interval: sui_reader_config.event_polling_interval,
            db_path: db_path.join("events"),
            rpc_fallback_config: sui_reader_config.rpc_fallback_config.clone(),
            db_config: db_config.clone(),
        };
        let system_config = SystemConfig {
            system_pkg_id: sui_reader_config
                .new_read_client()
                .await?
                .get_system_package_id(),
            system_object_id: sui_reader_config.contract_config.system_object,
            staking_object_id: sui_reader_config.contract_config.staking_object,
        };
        Ok(Arc::new(
            EventProcessor::new(
                event_processor_config,
                runtime_config,
                system_config,
                metrics_registry,
            )
            .await?,
        ))
    }

    /// Starts the event processor runtime.
    pub fn start(
        sui_config: SuiReaderConfig,
        event_processor_config: EventProcessorConfig,
        use_legacy_event_provider: bool,
        db_path: &Path,
        metrics_registry: &Registry,
        cancel_token: CancellationToken,
        db_config: &DatabaseConfig,
    ) -> anyhow::Result<(Box<dyn EventManager>, Self)> {
        tracing::info!(
            use_legacy_event_provider = use_legacy_event_provider,
            use_field_masking = event_processor_config.use_field_masking,
            adaptive_use_field_masking = event_processor_config.adaptive_downloader_config.use_field_masking,
            "event_processor runtime: starting with config"
        );

        let runtime = runtime::Builder::new_multi_thread()
            .thread_name("event-manager-runtime")
            .worker_threads(2)
            .enable_all()
            .build()
            .context("event manager runtime creation failed")?;
        let _guard = runtime.enter();

        let (event_manager, event_processor_handle): (Box<dyn EventManager>, _) =
            if use_legacy_event_provider {
                let read_client = runtime.block_on(async { sui_config.new_read_client().await })?;
                (
                    Box::new(SuiSystemEventProvider::new(
                        read_client,
                        sui_config.event_polling_interval,
                    )),
                    tokio::spawn(async { std::future::pending().await }),
                )
            } else {
                let event_processor = runtime.block_on(async {
                    Self::build_event_processor(
                        &sui_config,
                        &event_processor_config,
                        db_path,
                        metrics_registry,
                        db_config,
                    )
                    .await
                })?;
                let cloned_event_processor = event_processor.clone();
                let event_processor_handle = tokio::spawn(async move {
                    let result = cloned_event_processor.start(cancel_token).await;
                    if let Err(ref error) = result {
                        tracing::error!(?error, "event manager exited with an error");
                    }
                    result
                });
                (Box::new(event_processor), event_processor_handle)
            };

        Ok((
            event_manager,
            Self {
                runtime,
                event_processor_handle,
            },
        ))
    }

    /// Starts the event processor within the context of a pre-existing async runtime.
    pub async fn start_async(
        sui_config: SuiReaderConfig,
        event_processor_config: EventProcessorConfig,
        db_path: &Path,
        metrics_registry: &Registry,
        cancel_token: CancellationToken,
        db_config: &DatabaseConfig,
    ) -> anyhow::Result<Arc<EventProcessor>> {
        tracing::info!(?db_path, "[start_async] running");
        let event_processor = Self::build_event_processor(
            &sui_config,
            &event_processor_config,
            db_path,
            metrics_registry,
            db_config,
        )
        .await?;
        let event_processor_clone = event_processor.clone();
        tokio::spawn(async move {
            let result = event_processor_clone.start(cancel_token).await;
            if let Err(ref error) = result {
                panic!("event manager exited with an error: {error:?}");
            }
        });
        Ok(event_processor)
    }

    /// Waits for the event processor to shutdown.
    pub fn join(&mut self) -> Result<(), anyhow::Error> {
        tracing::debug!("waiting for the event processor to shutdown...");
        self.runtime.block_on(&mut self.event_processor_handle)?
    }
}

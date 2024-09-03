// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Helper struct to run the Walrus client binary commands.

use std::{io::Write, num::NonZeroU16, path::PathBuf, time::Duration};

use anyhow::Result;
use prometheus::Registry;
use sui_sdk::wallet_context::WalletContext;
use tokio::runtime::{self, Runtime};
use walrus_core::{
    encoding::{encoded_blob_length_for_n_shards, EncodingConfig, Primary},
    BlobId,
};
use walrus_service::{
    client::{
        cli_utils::{
            get_contract_client,
            get_read_client,
            get_sui_read_client_from_rpc_node_or_wallet,
            load_configuration,
            load_wallet_context,
            read_blob_from_file,
            BlobIdDecimal,
            CliOutput,
        },
        responses::{
            BlobIdConversionOutput,
            BlobIdOutput,
            BlobStatusOutput,
            DryRunOutput,
            InfoOutput,
            ReadOutput,
        },
        Client,
        ClientDaemon,
        Config,
    },
    utils::MetricsAndLoggingRuntime,
};
use walrus_sui::{
    client::{ContractClient, ReadClient, SuiContractClient},
    utils::price_for_encoded_length,
};

use super::args::{DaemonArgs, FileOrBlobId, PublisherArgs};

/// Converts the _inner function to the public function by running it in a new runtime.
macro_rules! command_from_inner {
    ($self:ident, $outer:ident, $inner:ident, $($arg_name:ident: $arg_type:ty),*) => {
        pub(crate) fn $outer(
            self,
            $($arg_name: $arg_type),*
        ) -> Result<()> {
            Self::runtime().block_on(self.$inner($($arg_name),*))
        }
    };
}

/// Converts the _inner function to the public function by running it in a new runtime, and starts a
/// separate runtime for metrics and logging.
macro_rules! command_with_metrics {
    ($self:ident, $outer:ident, $inner:ident, $($arg_name:ident: $arg_type:ty),*) => {
        pub(crate) fn $outer(
            self,
            $($arg_name: $arg_type),*
        ) -> Result<()> {
            let registry = self.metrics_runtime.registry();
            Self::runtime().block_on(self.$inner(&registry, $($arg_name),*))
        }
    };
}

/// Marker trait to extract a registry from a type.
pub(crate) trait WithRegistry {
    fn registry(&self) -> Registry;
}

impl WithRegistry for MetricsAndLoggingRuntime {
    fn registry(&self) -> Registry {
        self.registry.clone()
    }
}

pub(crate) struct ClientCommandRunner<R = ()> {
    /// The wallet path.
    wallet_path: Option<PathBuf>,
    /// The Sui wallet for the client.
    wallet: Result<WalletContext>,
    /// The config for the client.
    config: Result<Config>,
    /// Whether to output JSON.
    json: bool,
    /// The gas budget for the client commands.
    gas_budget: u64,
    /// The optional address for the metrics server.
    metrics_runtime: R,
}

impl ClientCommandRunner {
    /// Creates a new client runner, loading the configuration and wallet context.
    pub(crate) fn new(
        config: &Option<PathBuf>,
        wallet: &Option<PathBuf>,
        gas_budget: u64,
        json: bool,
    ) -> Self {
        let config = load_configuration(config);
        let wallet_path = wallet.clone().or(config
            .as_ref()
            .ok()
            .and_then(|conf| conf.wallet_config.clone()));
        let wallet = load_wallet_context(&wallet_path);

        Self {
            wallet_path,
            wallet,
            config,
            gas_budget,
            json,
            metrics_runtime: (),
        }
    }

    /// Sets the metrics and logging runtime for the client runner.
    pub(crate) fn with_metrics_runtime(
        self,
        metrics_runtime: MetricsAndLoggingRuntime,
    ) -> ClientCommandRunner<MetricsAndLoggingRuntime> {
        let Self {
            wallet_path,
            wallet,
            config,
            json,
            gas_budget,
            ..
        } = self;
        ClientCommandRunner {
            wallet_path,
            wallet,
            config,
            json,
            gas_budget,
            metrics_runtime,
        }
    }
}

impl<R> ClientCommandRunner<R> {
    fn runtime() -> Runtime {
        runtime::Builder::new_multi_thread()
            .thread_name("client-runtime")
            .enable_all()
            .build()
            .expect("runtime creation must succeed")
    }

    // Implementations of client commands.

    async fn read_inner(
        self,
        blob_id: BlobId,
        out: Option<PathBuf>,
        rpc_url: Option<String>,
    ) -> Result<()> {
        let client = get_read_client(
            self.config?,
            rpc_url,
            self.wallet,
            self.wallet_path.is_none(),
            &None,
        )
        .await?;
        let blob = client.read_blob::<Primary>(&blob_id).await?;
        match out.as_ref() {
            Some(path) => std::fs::write(path, &blob)?,
            None => {
                if !self.json {
                    std::io::stdout().write_all(&blob)?
                }
            }
        }
        ReadOutput::new(out, blob_id, blob).print_output(self.json)
    }

    async fn store_inner(
        self,
        file: PathBuf,
        epochs: u64,
        force: bool,
        dry_run: bool,
    ) -> Result<()> {
        let client = get_contract_client(self.config?, self.wallet, self.gas_budget, &None).await?;

        if dry_run {
            tracing::info!("Performing dry-run store for file {}", file.display());
            let encoding_config = client.encoding_config();
            tracing::debug!(n_shards = encoding_config.n_shards(), "encoding the blob");
            let metadata = encoding_config
                .get_blob_encoder(&read_blob_from_file(&file)?)?
                .compute_metadata();
            let unencoded_size = metadata.metadata().unencoded_length;
            let encoded_size =
                encoded_blob_length_for_n_shards(encoding_config.n_shards(), unencoded_size)
                    .expect("must be valid as the encoding succeeded");
            let price_per_unit_size = client
                .sui_client()
                .read_client()
                .price_per_unit_size()
                .await?;
            let storage_cost = price_for_encoded_length(encoded_size, price_per_unit_size, epochs);
            DryRunOutput {
                blob_id: *metadata.blob_id(),
                unencoded_size,
                encoded_size,
                storage_cost,
            }
            .print_output(self.json)
        } else {
            tracing::info!("Storing file {} as blob on Walrus", file.display());
            let result = client
                .reserve_and_store_blob(&read_blob_from_file(&file)?, epochs, force)
                .await?;
            result.print_output(self.json)
        }
    }

    async fn blob_status_inner(
        self,
        file_or_blob_id: FileOrBlobId,
        timeout: Duration,
        rpc_url: Option<String>,
    ) -> Result<()> {
        tracing::debug!(?file_or_blob_id, "getting blob status");
        let config = self.config?;
        let sui_read_client = get_sui_read_client_from_rpc_node_or_wallet(
            &config,
            rpc_url,
            self.wallet,
            self.wallet_path.is_none(),
        )
        .await?;
        let client = Client::new_read_client(config, &sui_read_client).await?;
        let file = file_or_blob_id.file.clone();
        let blob_id = file_or_blob_id.get_or_compute_blob_id(client.encoding_config())?;

        let status = client
            .get_verified_blob_status(&blob_id, &sui_read_client, timeout)
            .await?;
        BlobStatusOutput {
            blob_id,
            file,
            status,
        }
        .print_output(self.json)
    }

    async fn info_inner(self, rpc_url: Option<String>, dev: bool) -> Result<()> {
        let config = self.config?;
        let sui_read_client = get_sui_read_client_from_rpc_node_or_wallet(
            &config,
            rpc_url,
            self.wallet,
            self.wallet_path.is_none(),
        )
        .await?;
        InfoOutput::get_system_info(&sui_read_client, dev)
            .await?
            .print_output(self.json)
    }

    async fn blob_id_inner(
        self,
        file: PathBuf,
        n_shards: Option<NonZeroU16>,
        rpc_url: Option<String>,
    ) -> Result<()> {
        let n_shards = if let Some(n) = n_shards {
            n
        } else {
            let config = self.config?;
            tracing::debug!("reading `n_shards` from chain");
            let sui_read_client = get_sui_read_client_from_rpc_node_or_wallet(
                &config,
                rpc_url,
                self.wallet,
                self.wallet_path.is_none(),
            )
            .await?;
            sui_read_client.current_committee().await?.n_shards()
        };

        tracing::debug!(%n_shards, "encoding the blob");
        let metadata = EncodingConfig::new(n_shards)
            .get_blob_encoder(&read_blob_from_file(&file)?)?
            .compute_metadata();

        BlobIdOutput::new(&file, &metadata).print_output(self.json)
    }

    async fn list_blobs_inner(self, include_expired: bool) -> Result<()> {
        let contract_client =
            SuiContractClient::new(self.wallet?, self.config?.system_object, self.gas_budget)
                .await?;
        let blobs = contract_client.owned_blobs(include_expired).await?;
        blobs.print_output(self.json)
    }

    async fn publisher_inner(self, registry: &Registry, args: PublisherArgs) -> Result<()> {
        args.print_debug_message("attempting to run the Walrus publisher");
        let client = get_contract_client(
            self.config?,
            self.wallet,
            self.gas_budget,
            &args.daemon_args.blocklist,
        )
        .await?;
        ClientDaemon::new_publisher(
            client,
            args.daemon_args.bind_address,
            args.max_body_size(),
            registry,
        )
        .run()
        .await?;
        Ok(())
    }

    async fn aggregator_inner(
        self,
        registry: &Registry,
        rpc_url: Option<String>,
        daemon_args: DaemonArgs,
    ) -> Result<()> {
        tracing::debug!(?rpc_url, "attempting to run the Walrus aggregator");
        let client = get_read_client(
            self.config?,
            rpc_url,
            self.wallet,
            self.wallet_path.is_none(),
            &daemon_args.blocklist,
        )
        .await?;
        ClientDaemon::new_aggregator(client, daemon_args.bind_address, registry)
            .run()
            .await?;
        Ok(())
    }

    async fn daemon_inner(self, registry: &Registry, args: PublisherArgs) -> Result<()> {
        args.print_debug_message("attempting to run the Walrus daemon");
        let client = get_contract_client(
            self.config?,
            self.wallet,
            self.gas_budget,
            &args.daemon_args.blocklist,
        )
        .await?;
        ClientDaemon::new_daemon(
            client,
            args.daemon_args.bind_address,
            args.max_body_size(),
            registry,
        )
        .run()
        .await?;
        Ok(())
    }

    // Public implementations.

    command_from_inner!(
        self, read, read_inner, blob_id: BlobId, out: Option<PathBuf>, rpc_url: Option<String>
    );
    command_from_inner!(
        self, store, store_inner, file: PathBuf, epochs: u64, force: bool, dry_run: bool
    );
    command_from_inner!(
        self,
        blob_status,
        blob_status_inner,
        file_or_blob_id: FileOrBlobId,
        timeout: Duration,
        rpc_url: Option<String>
    );
    command_from_inner!(self, info, info_inner, rpc_url: Option<String>, dev: bool);
    command_from_inner!(
        self,
        blob_id,
        blob_id_inner,
        file: PathBuf,
        n_shards: Option<NonZeroU16>,
        rpc_url: Option<String>
    );
    command_from_inner!(self, list_blobs, list_blobs_inner, include_expired: bool);

    pub(crate) fn convert_blob_id(self, blob_id_decimal: BlobIdDecimal) -> Result<()> {
        BlobIdConversionOutput::from(blob_id_decimal).print_output(self.json)
    }
}

impl<R: WithRegistry> ClientCommandRunner<R> {
    command_with_metrics!(self, publisher, publisher_inner, args: PublisherArgs);
    command_with_metrics!(
        self,
        aggregator,
        aggregator_inner,
        rpc_url: Option<String>,
        daemon_args: DaemonArgs
    );
    command_with_metrics!(self, daemon, daemon_inner, args: PublisherArgs);
}

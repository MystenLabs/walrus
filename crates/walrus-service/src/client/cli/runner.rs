// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Helper struct to run the Walrus client binary commands.

use std::{io::Write, num::NonZeroU16, path::PathBuf, time::Duration};

use anyhow::Result;
use prometheus::Registry;
use sui_sdk::wallet_context::WalletContext;
use walrus_core::{
    encoding::{encoded_blob_length_for_n_shards, EncodingConfig, Primary},
    BlobId,
    EpochCount,
};
use walrus_sui::{
    client::{ContractClient, ReadClient, SuiContractClient},
    utils::storage_price_for_encoded_length,
};

use super::args::{
    CliCommands,
    DaemonArgs,
    DaemonCommands,
    FileOrBlobId,
    FileOrBlobIdOrObjectId,
    PublisherArgs,
    RpcArg,
};
use crate::{
    client::{
        cli::{
            get_contract_client,
            get_read_client,
            get_sui_read_client_from_rpc_node_or_wallet,
            load_configuration,
            load_wallet_context,
            read_blob_from_file,
            success,
            BlobIdDecimal,
            CliOutput,
        },
        responses::{
            BlobIdConversionOutput,
            BlobIdOutput,
            BlobStatusOutput,
            DeleteOutput,
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

/// A helper struct to run commands for the Walrus client.
#[allow(missing_debug_implementations)]
pub struct ClientCommandRunner {
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
}

impl ClientCommandRunner {
    /// Creates a new client runner, loading the configuration and wallet context.
    pub fn new(
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
        }
    }

    /// Runs the binary commands in "cli" mode (i.e., without running a server).
    ///
    /// Consumes `self`.
    #[tokio::main]
    pub async fn run_cli_app(self, command: CliCommands) -> Result<()> {
        match command {
            CliCommands::Read {
                blob_id,
                out,
                rpc_arg: RpcArg { rpc_url },
            } => self.read(blob_id, out, rpc_url).await,

            CliCommands::Store {
                file,
                epochs,
                dry_run,
                force,
                deletable,
            } => self.store(file, epochs, dry_run, force, deletable).await,

            CliCommands::BlobStatus {
                file_or_blob_id,
                timeout,
                rpc_arg: RpcArg { rpc_url },
            } => self.blob_status(file_or_blob_id, timeout, rpc_url).await,

            CliCommands::Info {
                rpc_arg: RpcArg { rpc_url },
                dev,
            } => self.info(rpc_url, dev).await,

            CliCommands::BlobId {
                file,
                n_shards,
                rpc_arg: RpcArg { rpc_url },
            } => self.blob_id(file, n_shards, rpc_url).await,

            CliCommands::ConvertBlobId { blob_id_decimal } => self.convert_blob_id(blob_id_decimal),

            CliCommands::ListBlobs { include_expired } => self.list_blobs(include_expired).await,

            CliCommands::Delete { target } => self.delete(target).await,
        }
    }

    /// Runs the binary commands in "daemon" mode (i.e., running a server).
    ///
    /// Consumes `self`.
    #[tokio::main]
    pub async fn run_daemon_app(
        self,
        command: DaemonCommands,
        metrics_runtime: MetricsAndLoggingRuntime,
    ) -> Result<()> {
        match command {
            DaemonCommands::Publisher { args } => {
                self.publisher(&metrics_runtime.registry, args).await
            }

            DaemonCommands::Aggregator {
                rpc_arg: RpcArg { rpc_url },
                daemon_args,
            } => {
                self.aggregator(&metrics_runtime.registry, rpc_url, daemon_args)
                    .await
            }

            DaemonCommands::Daemon { args } => self.daemon(&metrics_runtime.registry, args).await,
        }
    }

    // Implementations of client commands.

    pub(crate) async fn read(
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

    pub(crate) async fn store(
        self,
        file: PathBuf,
        epochs: EpochCount,
        force: bool,
        dry_run: bool,
        deletable: bool,
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
                .storage_price_per_unit_size()
                .await?;
            let storage_cost =
                storage_price_for_encoded_length(encoded_size, price_per_unit_size, epochs);
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
                .reserve_and_store_blob(&read_blob_from_file(&file)?, epochs, force, deletable)
                .await?;
            result.print_output(self.json)
        }
    }

    pub(crate) async fn blob_status(
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

    pub(crate) async fn info(self, rpc_url: Option<String>, dev: bool) -> Result<()> {
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

    pub(crate) async fn blob_id(
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

    pub(crate) async fn list_blobs(self, include_expired: bool) -> Result<()> {
        let config = self.config?;
        let contract_client = SuiContractClient::new(
            self.wallet?,
            config.system_object,
            config.staking_object,
            self.gas_budget,
        )
        .await?;
        let blobs = contract_client.owned_blobs(include_expired).await?;
        blobs.print_output(self.json)
    }

    pub(crate) async fn publisher(self, registry: &Registry, args: PublisherArgs) -> Result<()> {
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

    pub(crate) async fn aggregator(
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

    pub(crate) async fn daemon(self, registry: &Registry, args: PublisherArgs) -> Result<()> {
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

    pub(crate) fn convert_blob_id(self, blob_id_decimal: BlobIdDecimal) -> Result<()> {
        BlobIdConversionOutput::from(blob_id_decimal).print_output(self.json)
    }

    pub(crate) async fn delete(self, target: FileOrBlobIdOrObjectId) -> Result<()> {
        // Check that the target is valid.
        target.exactly_one_is_some()?;

        let client = get_contract_client(self.config?, self.wallet, self.gas_budget, &None).await?;
        let file = target.file.clone();
        let object_id = target.object_id;

        let (blob_id, deleted_blobs) =
            if let Some(blob_id) = target.get_or_compute_blob_id(client.encoding_config())? {
                let to_delete = client
                    .deletable_blobs_by_id(&blob_id)
                    .await?
                    .collect::<Vec<_>>();

                if !self.json {
                    println!(
                        "The following blobs with blob ID {} are deletable:",
                        blob_id
                    );
                    to_delete.print_output(self.json)?;
                    if !ask_for_confirmation()? {
                        println!("{} Aborting. No blobs were deleted.", success());
                        return Ok(());
                    }
                    println!("Deleting blobs...");
                }

                for blob in to_delete.iter() {
                    client.delete_owned_blob_by_object(blob.id).await?;
                }

                (Some(blob_id), to_delete)
            } else if let Some(object_id) = object_id {
                if let Some(to_delete) = client
                    .sui_client()
                    .owned_blobs(false)
                    .await?
                    .into_iter()
                    .find(|blob| blob.id == object_id)
                {
                    client.delete_owned_blob_by_object(object_id).await?;
                    (Some(to_delete.blob_id), vec![to_delete])
                } else {
                    (None, vec![])
                }
            } else {
                unreachable!("we checked that either file, blob ID or object ID are be provided");
            };

        DeleteOutput {
            blob_id,
            file,
            object_id,
            deleted_blobs,
        }
        .print_output(self.json)
    }
}

pub fn ask_for_confirmation() -> Result<bool> {
    println!("Do you want to proceed? [y/N]");
    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
    Ok(input.trim().to_lowercase().starts_with('y'))
}

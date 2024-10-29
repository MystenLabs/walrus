// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Helper struct to run the Walrus client binary commands.

use std::{
    io::Write,
    num::NonZeroU16,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result};
use prometheus::Registry;
use rand::seq::SliceRandom;
use sui_sdk::wallet_context::WalletContext;
use sui_types::base_types::ObjectID;
use walrus_core::{
    encoding::{encoded_blob_length_for_n_shards, EncodingConfig, Primary},
    BlobId,
    EpochCount,
};
use walrus_sui::{
    client::{BlobPersistence, ContractClient, ReadClient},
    utils::{price_for_encoded_length, SuiNetwork},
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
            read_blob_from_file,
            success,
            BlobIdDecimal,
            CliOutput,
            HumanReadableMist,
        },
        config::ExchangeObjectConfig,
        responses::{
            BlobIdConversionOutput,
            BlobIdOutput,
            BlobStatusOutput,
            DeleteOutput,
            DryRunOutput,
            ExchangeOutput,
            InfoOutput,
            ReadOutput,
            StakeOutput,
            WalletOutput,
        },
        Client,
        ClientDaemon,
        Config,
        StoreWhen,
    },
    utils::{self, generate_sui_wallet, MetricsAndLoggingRuntime},
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
        let wallet = crate::utils::load_wallet_context(&wallet_path);

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
            } => {
                self.store(
                    file,
                    epochs,
                    dry_run,
                    StoreWhen::always(force),
                    BlobPersistence::from_deletable(deletable),
                )
                .await
            }

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

            CliCommands::Stake { node_id, amount } => {
                self.stake_with_node_pool(node_id, amount).await
            }

            CliCommands::GenerateSuiWallet {
                path,
                sui_network,
                faucet_timeout,
            } => {
                self.generate_sui_wallet(&path, sui_network, faucet_timeout)
                    .await
            }

            CliCommands::GetWal {
                exchange_id,
                amount,
            } => self.exchange_sui_for_wal(exchange_id, amount).await,
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
        self.maybe_export_contract_info(&metrics_runtime.registry);

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

    fn maybe_export_contract_info(&self, registry: &Registry) {
        let Ok(config) = self.config.as_ref() else {
            return;
        };
        utils::export_contract_info(
            registry,
            &config.system_object,
            &config.staking_object,
            utils::load_wallet_context(&self.wallet_path)
                .and_then(|mut wallet| wallet.active_address())
                .ok(),
        );
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

        let start_timer = std::time::Instant::now();
        let blob = client.read_blob::<Primary>(&blob_id).await?;
        let blob_size = blob.len();
        let elapsed = humantime::Duration::from(start_timer.elapsed());

        tracing::info!(?blob_id, %elapsed, blob_size, "read blob");

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
        dry_run: bool,
        store_when: StoreWhen,
        persistence: BlobPersistence,
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
            let price_per_unit_size = client.sui_client().storage_price_per_unit_size().await?;
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
                .reserve_and_store_blob_retry_epoch(
                    &read_blob_from_file(&file)?,
                    epochs,
                    store_when,
                    persistence,
                )
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
        let client = Client::new(config, &sui_read_client).await?;
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
        let contract_client = config
            .new_contract_client(self.wallet?, self.gas_budget)
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
                    if to_delete.is_empty() {
                        println!("No owned deletable blobs found for blob ID {blob_id}.");
                        return Ok(());
                    }
                    println!("The following blobs with blob ID {blob_id} are deletable:");
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

    pub(crate) async fn stake_with_node_pool(self, node_id: ObjectID, amount: u64) -> Result<()> {
        let client = get_contract_client(self.config?, self.wallet, self.gas_budget, &None).await?;
        let staked_wal = client.stake_with_node_pool(node_id, amount).await?;
        StakeOutput { staked_wal }.print_output(self.json)
    }

    pub(crate) async fn generate_sui_wallet(
        self,
        path: &Path,
        sui_network: SuiNetwork,
        faucet_timeout: Duration,
    ) -> Result<()> {
        let wallet_address = generate_sui_wallet(sui_network, path, faucet_timeout).await?;
        WalletOutput { wallet_address }.print_output(self.json)
    }

    pub(crate) async fn exchange_sui_for_wal(
        self,
        exchange_id: Option<ObjectID>,
        amount: u64,
    ) -> Result<()> {
        let config = self.config?;
        let exchange_id = match (exchange_id, &config.exchange_object) {
            (Some(exchange_id), _) => Some(exchange_id),
            (None, None) => None,
            (None, Some(ExchangeObjectConfig::One(exchange_id))) => Some(*exchange_id),
            (None, Some(ExchangeObjectConfig::Multiple(exchange_ids))) => {
                exchange_ids.choose(&mut rand::thread_rng()).copied()
            }
        }
        .context(
            "Object ID of exchange object must be specified either in the config file or as a \
            command-line argument.",
        )?;
        let client = get_contract_client(config, self.wallet, self.gas_budget, &None).await?;
        tracing::info!(
            "exchanging {} for WAL using exchange object {exchange_id}",
            HumanReadableMist::from(amount)
        );
        client.exchange_sui_for_wal(exchange_id, amount).await?;
        ExchangeOutput { amount_sui: amount }.print_output(self.json)
    }
}

pub fn ask_for_confirmation() -> Result<bool> {
    println!("Do you want to proceed? [y/N]");
    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
    Ok(input.trim().to_lowercase().starts_with('y'))
}

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
use itertools::Itertools as _;
use prometheus::Registry;
use rand::seq::SliceRandom;
use sui_config::{sui_config_dir, SUI_CLIENT_CONFIG};
use sui_sdk::wallet_context::WalletContext;
use sui_types::base_types::ObjectID;
use walrus_core::{
    encoding::{encoded_blob_length_for_n_shards, EncodingConfig, Primary},
    ensure,
    metadata::BlobMetadataApi as _,
    BlobId,
    EpochCount,
};
use walrus_sui::{
    client::{
        BlobPersistence,
        ExpirySelectionPolicy,
        PostStoreAction,
        ReadClient,
        SuiContractClient,
    },
    utils::SuiNetwork,
};

use super::args::{
    BurnSelection,
    CliCommands,
    DaemonArgs,
    DaemonCommands,
    FileOrBlobId,
    FileOrBlobIdOrObjectId,
    PublisherArgs,
    RpcArg,
    UserConfirmation,
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
            warning,
            BlobIdDecimal,
            CliOutput,
            HumanReadableMist,
        },
        multiplexer::ClientMultiplexer,
        responses::{
            BlobIdConversionOutput,
            BlobIdOutput,
            BlobStatusOutput,
            DeleteOutput,
            DryRunOutput,
            ExchangeOutput,
            ExtendBlobOutput,
            FundSharedBlobOutput,
            InfoOutput,
            ReadOutput,
            ShareBlobOutput,
            StakeOutput,
            WalletOutput,
        },
        styled_spinner,
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
                files,
                epochs,
                dry_run,
                force,
                deletable,
                share,
            } => {
                self.store(
                    files,
                    epochs,
                    dry_run,
                    StoreWhen::always(force),
                    BlobPersistence::from_deletable(deletable),
                    PostStoreAction::from_share(share),
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

            CliCommands::Delete {
                target,
                yes,
                no_status_check,
            } => self.delete(target, yes.into(), no_status_check).await,

            CliCommands::Stake { node_id, amount } => {
                self.stake_with_node_pool(node_id, amount).await
            }

            CliCommands::GenerateSuiWallet {
                path,
                sui_network,
                use_faucet,
                faucet_timeout,
            } => {
                let wallet_path = if let Some(path) = path {
                    path
                } else {
                    // Check if the Sui configuration directory exists.
                    let config_dir = sui_config_dir()?;
                    if config_dir.exists() {
                        anyhow::bail!(
                            "Sui configuration directory already exists; please specify a \
                            different path using `--path` or manage the wallet using the Sui CLI."
                        );
                    } else {
                        tracing::debug!(
                            config_dir = ?config_dir.display(),
                            "creating the Sui configuration directory"
                        );
                        std::fs::create_dir_all(&config_dir)?;
                        config_dir.join(SUI_CLIENT_CONFIG)
                    }
                };

                self.generate_sui_wallet(&wallet_path, sui_network, use_faucet, faucet_timeout)
                    .await
            }

            CliCommands::GetWal {
                exchange_id,
                amount,
            } => self.exchange_sui_for_wal(exchange_id, amount).await,

            CliCommands::BurnBlobs {
                burn_selection,
                yes,
            } => self.burn_blobs(burn_selection, yes.into()).await,

            CliCommands::FundSharedBlob {
                shared_blob_obj_id,
                amount,
            } => {
                let sui_client = self
                    .config?
                    .new_contract_client(self.wallet?, self.gas_budget)
                    .await?;
                let spinner = styled_spinner();
                spinner.set_message("funding blob...");

                sui_client
                    .fund_shared_blob(shared_blob_obj_id, amount)
                    .await?;

                spinner.finish_with_message("done");
                FundSharedBlobOutput { amount }.print_output(self.json)
            }

            CliCommands::Extend {
                shared_blob_obj_id,
                epochs_ahead,
            } => {
                let sui_client = self
                    .config?
                    .new_contract_client(self.wallet?, self.gas_budget)
                    .await?;
                let spinner = styled_spinner();
                spinner.set_message("extending blob...");

                sui_client
                    .extend_shared_blob(shared_blob_obj_id, epochs_ahead)
                    .await?;

                spinner.finish_with_message("done");
                ExtendBlobOutput { epochs_ahead }.print_output(self.json)
            }

            CliCommands::Share {
                blob_obj_id,
                amount,
            } => {
                let sui_client = self
                    .config?
                    .new_contract_client(self.wallet?, self.gas_budget)
                    .await?;
                let spinner = styled_spinner();
                spinner.set_message("sharing blob...");

                let shared_blob_object_id = sui_client
                    .share_and_maybe_fund_blob(blob_obj_id, amount)
                    .await?;

                spinner.finish_with_message("done");
                ShareBlobOutput {
                    shared_blob_object_id,
                    amount,
                }
                .print_output(self.json)
            }
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
            &config.contract_config.system_object,
            &config.contract_config.staking_object,
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
        let elapsed = start_timer.elapsed();

        tracing::info!(%blob_id, ?elapsed, blob_size, "finished reading blob");

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
        files: Vec<PathBuf>,
        epochs: EpochCount,
        dry_run: bool,
        store_when: StoreWhen,
        persistence: BlobPersistence,
        post_store: PostStoreAction,
    ) -> Result<()> {
        let client = get_contract_client(self.config?, self.wallet, self.gas_budget, &None).await?;

        // Check that the number of epochs is lower than the number of epochs the blob can be stored
        // for.
        let fixed_params = client.sui_client().fixed_system_parameters().await?;

        ensure!(
            epochs <= fixed_params.max_epochs_ahead,
            "blobs can only be stored for up to {} epochs ahead; {} epochs were requested",
            fixed_params.max_epochs_ahead,
            epochs
        );

        if persistence.is_deletable() && post_store == PostStoreAction::Share {
            anyhow::bail!("deletable blobs cannot be shared");
        }

        if dry_run {
            return Self::store_dry_run(client, files, epochs, self.json).await;
        }

        tracing::info!("storing {} files as blobs on Walrus", files.len());
        let start_timer = std::time::Instant::now();
        let blobs = files
            .into_iter()
            .map(|file| read_blob_from_file(&file).map(|blob| (file, blob)))
            .collect::<Result<Vec<(PathBuf, Vec<u8>)>>>()?;
        let results = client
            .reserve_and_store_blobs_retry_epoch_with_path(
                &blobs,
                epochs,
                store_when,
                persistence,
                post_store,
            )
            .await?;
        let blobs_len = blobs.len();
        if results.len() != blobs_len {
            let original_paths: Vec<_> = blobs.into_iter().map(|(path, _)| path).collect();
            let not_stored = results
                .iter()
                .filter(|blob| !original_paths.contains(&blob.path))
                .map(|blob| blob.blob_store_result.blob_id())
                .collect::<Vec<_>>();
            tracing::warn!(
                "some blobs ({}) are not stored",
                not_stored.into_iter().join(", ")
            );
        }
        tracing::info!(
            duration = ?start_timer.elapsed(),
            "{} out of {} blobs stored",
            results.len(),
            blobs_len
        );
        results.print_output(self.json)
    }

    async fn store_dry_run(
        client: Client<SuiContractClient>,
        files: Vec<PathBuf>,
        epochs_ahead: EpochCount,
        json: bool,
    ) -> Result<()> {
        tracing::info!("performing dry-run store for {} files", files.len());
        let encoding_config = client.encoding_config();
        let mut outputs = Vec::with_capacity(files.len());

        for file in files {
            let blob = read_blob_from_file(&file)?;
            let (_, metadata) = client.encode_pairs_and_metadata(&blob).await?;
            let unencoded_size = metadata.metadata().unencoded_length();
            let encoded_size =
                encoded_blob_length_for_n_shards(encoding_config.n_shards(), unencoded_size)
                    .expect("must be valid as the encoding succeeded");
            let storage_cost = client.price_computation.operation_cost(
                &crate::client::resource::RegisterBlobOp::RegisterFromScratch {
                    encoded_length: encoded_size,
                    epochs_ahead,
                },
            );

            outputs.push(DryRunOutput {
                path: file,
                blob_id: *metadata.blob_id(),
                unencoded_size,
                encoded_size,
                storage_cost,
            });
        }
        outputs.print_output(json)
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
        let spinner = styled_spinner();
        spinner.set_message("computing the blob ID");
        let metadata = EncodingConfig::new(n_shards)
            .get_blob_encoder(&read_blob_from_file(&file)?)?
            .compute_metadata();
        spinner.finish_with_message(format!("blob ID computed: {}", metadata.blob_id()));

        BlobIdOutput::new(&file, &metadata).print_output(self.json)
    }

    pub(crate) async fn list_blobs(self, include_expired: bool) -> Result<()> {
        let config = self.config?;
        let contract_client = config
            .new_contract_client(self.wallet?, self.gas_budget)
            .await?;
        let blobs = contract_client
            .owned_blobs(
                None,
                ExpirySelectionPolicy::from_include_expired_flag(include_expired),
            )
            .await?;
        blobs.print_output(self.json)
    }

    pub(crate) async fn publisher(self, registry: &Registry, args: PublisherArgs) -> Result<()> {
        args.print_debug_message("attempting to run the Walrus publisher");
        let client = ClientMultiplexer::new(
            self.wallet?,
            &self.config?,
            self.gas_budget,
            registry,
            &args,
        )
        .await?;

        ClientDaemon::new_publisher(
            client,
            args.daemon_args.bind_address,
            args.max_body_size(),
            registry,
            args.max_request_buffer_size,
            args.max_concurrent_requests,
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
            args.max_request_buffer_size,
            args.max_concurrent_requests,
        )
        .run()
        .await?;
        Ok(())
    }

    pub(crate) fn convert_blob_id(self, blob_id_decimal: BlobIdDecimal) -> Result<()> {
        BlobIdConversionOutput::from(blob_id_decimal).print_output(self.json)
    }

    pub(crate) async fn delete(
        self,
        target: FileOrBlobIdOrObjectId,
        confirmation: UserConfirmation,
        no_status_check: bool,
    ) -> Result<()> {
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
                    if confirmation.is_required() {
                        println!("The following blobs with blob ID {blob_id} are deletable:");
                        to_delete.print_output(self.json)?;
                        if !ask_for_confirmation()? {
                            println!("{} Aborting. No blobs were deleted.", success());
                            return Ok(());
                        }
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
                    .owned_blobs(None, ExpirySelectionPolicy::Valid)
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

        let post_deletion_status = match (no_status_check, blob_id) {
            (false, Some(deleted_blob_id)) => {
                // Wait to ensure that the deletion information is propagated.
                tokio::time::sleep(Duration::from_secs(1)).await;
                Some(
                    client
                        .get_blob_status_with_retries(&deleted_blob_id, client.sui_client())
                        .await?,
                )
            }
            _ => None,
        };

        DeleteOutput {
            blob_id,
            file,
            object_id,
            deleted_blobs,
            post_deletion_status,
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
        use_faucet: bool,
        faucet_timeout: Duration,
    ) -> Result<()> {
        let wallet_address =
            generate_sui_wallet(sui_network, path, use_faucet, faucet_timeout).await?;
        WalletOutput { wallet_address }.print_output(self.json)
    }

    pub(crate) async fn exchange_sui_for_wal(
        self,
        exchange_id: Option<ObjectID>,
        amount: u64,
    ) -> Result<()> {
        let config = self.config?;
        let exchange_id = exchange_id
            .or_else(|| {
                config
                    .exchange_objects
                    .choose(&mut rand::thread_rng())
                    .copied()
            })
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

    pub(crate) async fn burn_blobs(
        self,
        burn_selection: BurnSelection,
        confirmation: UserConfirmation,
    ) -> Result<()> {
        let sui_client = self
            .config?
            .new_contract_client(self.wallet?, self.gas_budget)
            .await?;
        let object_ids = burn_selection.get_object_ids(&sui_client).await?;

        if object_ids.is_empty() {
            println!(
                "The wallet does not own any {}blob objects.",
                if burn_selection.is_all_expired() {
                    "expired "
                } else {
                    ""
                }
            );
            return Ok(());
        }

        if confirmation.is_required() {
            let object_list = object_ids.iter().map(|id| id.to_string()).join("\n");
            println!(
                "{} You are about to burn the following blob object(s):\n{}\n({} total). \
                \nIf unsure, please enter `No` and check the `--help` manual.",
                warning(),
                object_list,
                object_ids.len()
            );
            if !ask_for_confirmation()? {
                println!("{} Aborting. No blobs were burned.", success());
                return Ok(());
            }
        }

        let spinner = styled_spinner();
        spinner.set_message("burning blobs...");
        sui_client.burn_blobs(&object_ids).await?;
        spinner.finish_with_message("done");

        println!("{} The specified blob objects have been burned", success());
        Ok(())
    }
}

pub fn ask_for_confirmation() -> Result<bool> {
    println!("Do you want to proceed? [y/N]");
    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
    Ok(input.trim().to_lowercase().starts_with('y'))
}

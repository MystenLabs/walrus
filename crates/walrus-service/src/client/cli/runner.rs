// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Helper struct to run the Walrus client binary commands.

use std::{
    io::Write,
    iter,
    num::NonZeroU16,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use indicatif::MultiProgress;
use itertools::Itertools as _;
use prometheus::Registry;
use rand::seq::SliceRandom;
use sui_config::{sui_config_dir, SUI_CLIENT_CONFIG};
use sui_sdk::wallet_context::WalletContext;
use sui_types::base_types::ObjectID;
use walrus_core::{
    encoding::{
        encoded_blob_length_for_n_shards,
        EncodingConfig,
        EncodingConfigTrait as _,
        Primary,
    },
    ensure,
    metadata::BlobMetadataApi as _,
    BlobId,
    EncodingType,
    EpochCount,
    SUPPORTED_ENCODING_TYPES,
};
use walrus_sdk::api::BlobStatus;
use walrus_sui::{
    client::{
        BlobPersistence,
        ExpirySelectionPolicy,
        PostStoreAction,
        ReadClient,
        SuiContractClient,
    },
    types::move_structs::{BlobAttribute, EpochState},
    utils::SuiNetwork,
};

use super::args::{
    AggregatorArgs,
    BurnSelection,
    CliCommands,
    DaemonArgs,
    DaemonCommands,
    EpochArg,
    FileOrBlobId,
    FileOrBlobIdOrObjectId,
    HealthSortBy,
    InfoCommands,
    NodeSelection,
    PublisherArgs,
    RpcArg,
    SortBy,
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
        error::ClientErrorKind,
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
            GetBlobAttributeOutput,
            InfoBftOutput,
            InfoCommitteeOutput,
            InfoEpochOutput,
            InfoOutput,
            InfoPriceOutput,
            InfoSizeOutput,
            InfoStorageOutput,
            ReadOutput,
            ServiceHealthInfoOutput,
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
    gas_budget: Option<u64>,
}

impl ClientCommandRunner {
    /// Creates a new client runner, loading the configuration and wallet context.
    pub fn new(
        config: &Option<PathBuf>,
        wallet: &Option<PathBuf>,
        gas_budget: Option<u64>,
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
                epoch_arg,
                dry_run,
                force,
                ignore_resources,
                deletable,
                share,
                encoding_type,
            } => {
                self.store(
                    files,
                    epoch_arg,
                    dry_run,
                    StoreWhen::from_flags(force, ignore_resources),
                    BlobPersistence::from_deletable(deletable),
                    PostStoreAction::from_share(share),
                    encoding_type,
                )
                .await
            }

            CliCommands::BlobStatus {
                file_or_blob_id,
                timeout,
                rpc_arg: RpcArg { rpc_url },
                encoding_type,
            } => {
                self.blob_status(file_or_blob_id, timeout, rpc_url, encoding_type)
                    .await
            }

            CliCommands::Info {
                rpc_arg: RpcArg { rpc_url },
                command,
            } => self.info(rpc_url, command).await,

            CliCommands::Health {
                node_selection,
                detail,
                sort,
                rpc_arg: RpcArg { rpc_url },
            } => self.health(rpc_url, node_selection, detail, sort).await,

            CliCommands::BlobId {
                file,
                n_shards,
                encoding_type,
                rpc_arg: RpcArg { rpc_url },
            } => self.blob_id(file, n_shards, rpc_url, encoding_type).await,

            CliCommands::ConvertBlobId { blob_id_decimal } => self.convert_blob_id(blob_id_decimal),

            CliCommands::ListBlobs { include_expired } => self.list_blobs(include_expired).await,

            CliCommands::Delete {
                target,
                yes,
                no_status_check,
                encoding_type,
            } => {
                self.delete(target, yes.into(), no_status_check, encoding_type)
                    .await
            }

            CliCommands::Stake { node_ids, amounts } => {
                self.stake_with_node_pools(node_ids, amounts).await
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
                blob_obj_id,
                shared,
                epochs_extended,
            } => {
                let sui_client = self
                    .config?
                    .new_contract_client(self.wallet?, self.gas_budget)
                    .await?;
                let spinner = styled_spinner();
                spinner.set_message("extending blob...");

                if shared {
                    sui_client
                        .extend_shared_blob(blob_obj_id, epochs_extended)
                        .await?;
                } else {
                    sui_client.extend_blob(blob_obj_id, epochs_extended).await?;
                }

                spinner.finish_with_message("done");
                ExtendBlobOutput { epochs_extended }.print_output(self.json)
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

            CliCommands::GetBlobAttribute { blob_obj_id } => {
                let sui_read_client = get_sui_read_client_from_rpc_node_or_wallet(
                    &self.config?,
                    None,
                    self.wallet,
                    self.wallet_path.is_none(),
                )
                .await?;
                let attribute = sui_read_client.get_blob_attribute(&blob_obj_id).await?;
                GetBlobAttributeOutput { attribute }.print_output(self.json)
            }

            CliCommands::SetBlobAttribute {
                blob_obj_id,
                attributes,
            } => {
                let pairs: Vec<(String, String)> = attributes
                    .chunks_exact(2)
                    .map(|chunk| (chunk[0].clone(), chunk[1].clone()))
                    .collect();
                let mut sui_client = self
                    .config?
                    .new_contract_client(self.wallet?, self.gas_budget)
                    .await?;
                let attribute = BlobAttribute::from(pairs);
                sui_client
                    .insert_or_update_blob_attribute_pairs(blob_obj_id, attribute.iter(), true)
                    .await?;
                if !self.json {
                    println!(
                        "{} Successfully added attribute for blob object {}",
                        success(),
                        blob_obj_id
                    );
                }
                Ok(())
            }

            CliCommands::RemoveBlobAttributeFields { blob_obj_id, keys } => {
                let mut sui_client = self
                    .config?
                    .new_contract_client(self.wallet?, self.gas_budget)
                    .await?;
                sui_client
                    .remove_blob_attribute_pairs(blob_obj_id, keys)
                    .await?;
                if !self.json {
                    println!(
                        "{} Successfully removed attribute for blob object {}",
                        success(),
                        blob_obj_id
                    );
                }
                Ok(())
            }

            CliCommands::RemoveBlobAttribute { blob_obj_id } => {
                let mut sui_client = self
                    .config?
                    .new_contract_client(self.wallet?, self.gas_budget)
                    .await?;
                sui_client.remove_blob_attribute(blob_obj_id).await?;
                if !self.json {
                    println!(
                        "{} Successfully removed attribute for blob object {}",
                        success(),
                        blob_obj_id
                    );
                }
                Ok(())
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
                aggregator_args,
            } => {
                self.aggregator(
                    &metrics_runtime.registry,
                    rpc_url,
                    daemon_args,
                    aggregator_args,
                )
                .await
            }

            DaemonCommands::Daemon {
                args,
                aggregator_args,
            } => {
                self.daemon(&metrics_runtime.registry, args, aggregator_args)
                    .await
            }
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

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn store(
        self,
        files: Vec<PathBuf>,
        epoch_arg: EpochArg,
        dry_run: bool,
        store_when: StoreWhen,
        persistence: BlobPersistence,
        post_store: PostStoreAction,
        encoding_type: Option<EncodingType>,
    ) -> Result<()> {
        epoch_arg.exactly_one_is_some()?;
        if encoding_type.is_some_and(|encoding| !encoding.is_supported()) {
            anyhow::bail!(ClientErrorKind::UnsupportedEncodingType(
                encoding_type.expect("just checked that option is Some")
            ));
        }

        let client = get_contract_client(self.config?, self.wallet, self.gas_budget, &None).await?;

        let system_object = client.sui_client().read_client.get_system_object().await?;
        let epochs_ahead =
            get_epochs_ahead(epoch_arg, system_object.max_epochs_ahead(), &client).await?;

        if persistence.is_deletable() && post_store == PostStoreAction::Share {
            anyhow::bail!("deletable blobs cannot be shared");
        }

        let encoding_type =
            encoding_type_or_default_for_version(encoding_type, system_object.version);

        if dry_run {
            return Self::store_dry_run(client, files, encoding_type, epochs_ahead, self.json)
                .await;
        }

        tracing::info!("storing {} files as blobs on Walrus", files.len());
        let start_timer = std::time::Instant::now();
        let blobs = files
            .into_iter()
            .map(|file| read_blob_from_file(&file).map(|blob| (file, blob)))
            .collect::<Result<Vec<(PathBuf, Vec<u8>)>>>()?;
        let results = client
            .reserve_and_store_blobs_retry_committees_with_path(
                &blobs,
                encoding_type,
                epochs_ahead,
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
        encoding_type: EncodingType,
        epochs_ahead: EpochCount,
        json: bool,
    ) -> Result<()> {
        tracing::info!("performing dry-run store for {} files", files.len());
        let encoding_config = client.encoding_config();
        let mut outputs = Vec::with_capacity(files.len());

        for file in files {
            let blob = read_blob_from_file(&file)?;
            let (_, metadata) = client
                .encode_pairs_and_metadata(&blob, encoding_type, &MultiProgress::new())
                .await?;
            let unencoded_size = metadata.metadata().unencoded_length();
            let encoded_size = encoded_blob_length_for_n_shards(
                encoding_config.n_shards(),
                unencoded_size,
                encoding_type,
            )
            .expect("must be valid as the encoding succeeded");
            let storage_cost = client.get_price_computation().await?.operation_cost(
                &crate::client::resource::RegisterBlobOp::RegisterFromScratch {
                    encoded_length: encoded_size,
                    epochs_ahead,
                },
            );

            outputs.push(DryRunOutput {
                path: file,
                blob_id: *metadata.blob_id(),
                encoding_type: metadata.metadata().encoding_type(),
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
        encoding_type: Option<EncodingType>,
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

        let encoding_type = encoding_type_or_default_for_version(
            encoding_type,
            sui_read_client.system_object_version().await?,
        );

        let refresher_handle = config
            .refresh_config
            .build_refresher_and_run(sui_read_client.clone())
            .await?;
        let client = Client::new(config, refresher_handle).await?;

        let file = file_or_blob_id.file.clone();
        let blob_id =
            file_or_blob_id.get_or_compute_blob_id(client.encoding_config(), encoding_type)?;

        let status = client
            .get_verified_blob_status(&blob_id, &sui_read_client, timeout)
            .await?;

        // Compute estimated blob expiry in DateTime if it is a permanent blob.
        let estimated_expiry_timestamp = if let BlobStatus::Permanent { end_epoch, .. } = status {
            let staking_object = sui_read_client.get_staking_object().await?;
            let epoch_duration = Duration::from_millis(staking_object.epoch_duration());
            let epoch_state = staking_object.epoch_state();
            let current_epoch = staking_object.epoch();

            let estimated_start_of_current_epoch = match epoch_state {
                EpochState::EpochChangeDone(epoch_start)
                | EpochState::NextParamsSelected(epoch_start) => *epoch_start,
                EpochState::EpochChangeSync(_) => Utc::now(),
            };
            ensure!(
                end_epoch > current_epoch,
                "end_epoch must be greater than the current epoch"
            );
            Some(estimated_start_of_current_epoch + epoch_duration * (end_epoch - current_epoch))
        } else {
            None
        };
        BlobStatusOutput {
            blob_id,
            file,
            status,
            estimated_expiry_timestamp,
        }
        .print_output(self.json)
    }

    pub(crate) async fn info(
        self,
        rpc_url: Option<String>,
        command: Option<InfoCommands>,
    ) -> Result<()> {
        let config = self.config?;
        let sui_read_client = get_sui_read_client_from_rpc_node_or_wallet(
            &config,
            rpc_url,
            self.wallet,
            self.wallet_path.is_none(),
        )
        .await?;

        match command {
            None => InfoOutput::get_system_info(
                &sui_read_client,
                false,
                SortBy::default(),
                SUPPORTED_ENCODING_TYPES,
            )
            .await?
            .print_output(self.json),
            Some(InfoCommands::All { sort }) => {
                InfoOutput::get_system_info(&sui_read_client, true, sort, SUPPORTED_ENCODING_TYPES)
                    .await?
                    .print_output(self.json)
            }
            Some(InfoCommands::Epoch) => InfoEpochOutput::get_epoch_info(&sui_read_client)
                .await?
                .print_output(self.json),
            Some(InfoCommands::Storage) => InfoStorageOutput::get_storage_info(&sui_read_client)
                .await?
                .print_output(self.json),
            Some(InfoCommands::Size) => InfoSizeOutput::get_size_info(&sui_read_client)
                .await?
                .print_output(self.json),
            Some(InfoCommands::Price) => {
                InfoPriceOutput::get_price_info(&sui_read_client, SUPPORTED_ENCODING_TYPES)
                    .await?
                    .print_output(self.json)
            }
            Some(InfoCommands::Committee { sort }) => {
                InfoCommitteeOutput::get_committee_info(&sui_read_client, sort)
                    .await?
                    .print_output(self.json)
            }
            Some(InfoCommands::Bft) => InfoBftOutput::get_bft_info(&sui_read_client)
                .await?
                .print_output(self.json),
        }
    }

    pub(crate) async fn health(
        self,
        rpc_url: Option<String>,
        node_selection: NodeSelection,
        detail: bool,
        sort: SortBy<HealthSortBy>,
    ) -> Result<()> {
        node_selection.exactly_one_is_set()?;
        let config = self.config?;
        let sui_read_client = get_sui_read_client_from_rpc_node_or_wallet(
            &config,
            rpc_url,
            self.wallet,
            self.wallet_path.is_none(),
        )
        .await?;

        ServiceHealthInfoOutput::new_for_nodes(
            node_selection.get_nodes(&sui_read_client).await?,
            detail,
            sort,
        )
        .await?
        .print_output(self.json)
    }

    pub(crate) async fn blob_id(
        self,
        file: PathBuf,
        n_shards: Option<NonZeroU16>,
        rpc_url: Option<String>,
        encoding_type: Option<EncodingType>,
    ) -> Result<()> {
        let (n_shards, encoding_type) =
            if let (Some(n_shards), Some(encoding_type)) = (n_shards, encoding_type) {
                (n_shards, encoding_type)
            } else {
                let config = self.config?;
                let sui_read_client = get_sui_read_client_from_rpc_node_or_wallet(
                    &config,
                    rpc_url,
                    self.wallet,
                    self.wallet_path.is_none(),
                )
                .await?;
                let n_shards = if let Some(n_shards) = n_shards {
                    n_shards
                } else {
                    tracing::debug!("reading `n_shards` from chain");
                    sui_read_client.current_committee().await?.n_shards()
                };
                let encoding_type = encoding_type_or_default_for_version(
                    encoding_type,
                    sui_read_client.system_object_version().await?,
                );
                (n_shards, encoding_type)
            };

        tracing::debug!(%n_shards, "encoding the blob");
        let spinner = styled_spinner();
        spinner.set_message("computing the blob ID");
        let metadata = EncodingConfig::new(n_shards)
            .get_for_type(encoding_type)
            .compute_metadata(&read_blob_from_file(&file)?)?;
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
        let auth_config = args.generate_auth_config()?;

        ClientDaemon::new_publisher(
            client,
            auth_config,
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
        aggregator_args: AggregatorArgs,
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
        ClientDaemon::new_aggregator(
            client,
            daemon_args.bind_address,
            registry,
            aggregator_args.allowed_headers,
        )
        .run()
        .await?;
        Ok(())
    }

    pub(crate) async fn daemon(
        self,
        registry: &Registry,
        args: PublisherArgs,
        aggregator_args: AggregatorArgs,
    ) -> Result<()> {
        args.print_debug_message("attempting to run the Walrus daemon");
        let auth_config = args.generate_auth_config()?;

        let client = get_contract_client(
            self.config?,
            self.wallet,
            self.gas_budget,
            &args.daemon_args.blocklist,
        )
        .await?;
        ClientDaemon::new_daemon(client, auth_config, registry, &args, &aggregator_args)
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
        encoding_type: Option<EncodingType>,
    ) -> Result<()> {
        // Check that the target is valid.
        target.exactly_one_is_some()?;

        let client = get_contract_client(self.config?, self.wallet, self.gas_budget, &None).await?;
        let file = target.file.clone();
        let object_id = target.object_id;
        let encoding_type = encoding_type_or_default_for_version(
            encoding_type,
            client.sui_client().system_object_version().await?,
        );

        let (blob_id, deleted_blobs) = if let Some(blob_id) =
            target.get_or_compute_blob_id(client.encoding_config(), encoding_type)?
        {
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

    pub(crate) async fn stake_with_node_pools(
        self,
        node_ids: Vec<ObjectID>,
        amounts: Vec<u64>,
    ) -> Result<()> {
        let n_nodes = node_ids.len();
        if amounts.len() != n_nodes && amounts.len() != 1 {
            anyhow::bail!(
                "the number of amounts must be either 1 or equal to the number of node IDs"
            );
        }
        let node_ids_with_amounts = if amounts.len() == 1 && n_nodes > 1 {
            node_ids
                .into_iter()
                .zip(iter::repeat(amounts[0]))
                .collect::<Vec<_>>()
        } else {
            node_ids
                .into_iter()
                .zip(amounts.into_iter())
                .collect::<Vec<_>>()
        };
        let client = get_contract_client(self.config?, self.wallet, self.gas_budget, &None).await?;
        let staked_wal = client.stake_with_node_pools(&node_ids_with_amounts).await?;
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
                "The object ID of an exchange object must be specified either in the config file \
                or as a command-line argument.\n\
                Note that this command is only available on Testnet.",
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

async fn get_epochs_ahead(
    epoch_arg: EpochArg,
    max_epochs_ahead: EpochCount,
    client: &Client<SuiContractClient>,
) -> Result<u32, anyhow::Error> {
    let epochs_ahead = match epoch_arg {
        EpochArg {
            epochs: Some(epochs),
            ..
        } => epochs.try_into_epoch_count(max_epochs_ahead)?,
        EpochArg {
            earliest_expiry_time: Some(earliest_expiry_time),
            ..
        } => {
            let staking_object = client.sui_client().read_client.get_staking_object().await?;
            let epoch_state = staking_object.epoch_state();
            let estimated_start_of_current_epoch = match epoch_state {
                EpochState::EpochChangeDone(epoch_start)
                | EpochState::NextParamsSelected(epoch_start) => *epoch_start,
                EpochState::EpochChangeSync(_) => Utc::now(),
            };
            let earliest_expiry_ts = DateTime::from(earliest_expiry_time);
            ensure!(
                earliest_expiry_ts > estimated_start_of_current_epoch
                    && earliest_expiry_ts > Utc::now(),
                "earliest_expiry_time must be greater than the current epoch start time
                and the current time"
            );
            let delta =
                (earliest_expiry_ts - estimated_start_of_current_epoch).num_milliseconds() as u64;
            (delta / staking_object.epoch_duration() + 1) as u32
        }
        EpochArg {
            end_epoch: Some(end_epoch),
            ..
        } => {
            let current_epoch = client.sui_client().current_epoch().await?;
            ensure!(
                end_epoch > current_epoch,
                "end_epoch must be greater than the current epoch"
            );
            end_epoch - current_epoch
        }
        _ => {
            anyhow::bail!("either epochs or earliest_expiry_time or end_epoch must be provided")
        }
    };

    // Check that the number of epochs is lower than the number of epochs the blob can be stored
    // for.
    ensure!(
        epochs_ahead <= max_epochs_ahead,
        "blobs can only be stored for up to {} epochs ahead; {} epochs were requested",
        max_epochs_ahead,
        epochs_ahead
    );

    Ok(epochs_ahead)
}

pub fn ask_for_confirmation() -> Result<bool> {
    println!("Do you want to proceed? [y/N]");
    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
    Ok(input.trim().to_lowercase().starts_with('y'))
}

// TODO(WAL-647): Remove for mainnet
fn encoding_type_or_default_for_version(
    encoding_type: Option<EncodingType>,
    system_version: u64,
) -> EncodingType {
    if let Some(encoding_type) = encoding_type {
        encoding_type
    } else {
        let encoding_type = if system_version >= 2 {
            EncodingType::RS2
        } else {
            EncodingType::RedStuffRaptorQ
        };
        tracing::debug!(
            system_version,
            ?encoding_type,
            "choosing default encoding based on system version"
        );
        encoding_type
    }
}

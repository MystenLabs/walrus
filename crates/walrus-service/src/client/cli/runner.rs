// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Helper struct to run the Walrus client binary commands.

use std::{
    io::Write,
    iter,
    num::NonZeroU16,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use fastcrypto::encoding::Encoding;
use indicatif::MultiProgress;
use itertools::Itertools as _;
use rand::seq::SliceRandom;
use reqwest::Url;
use sui_config::{SUI_CLIENT_CONFIG, sui_config_dir};
use sui_types::base_types::ObjectID;
use walrus_core::{
    BlobId,
    DEFAULT_ENCODING,
    EncodingType,
    EpochCount,
    SUPPORTED_ENCODING_TYPES,
    encoding::{
        EncodingConfig,
        EncodingFactory as _,
        Primary,
        encoded_blob_length_for_n_shards,
        quilt_encoding::{QuiltApi, QuiltStoreBlob, QuiltVersionV1},
    },
    ensure,
    metadata::{BlobMetadataApi as _, QuiltIndex},
};
use walrus_sdk::{
    SuiReadClient,
    client::{
        NodeCommunicationFactory,
        StoreArgs,
        WalrusNodeClient,
        quilt_client::{
            assign_identifiers_with_paths,
            generate_identifier_from_path,
            read_blobs_from_paths,
        },
        resource::RegisterBlobOp,
        upload_relay_client::UploadRelayClient,
    },
    config::{UploadMode, load_configuration},
    error::ClientErrorKind,
    store_optimizations::StoreOptimizations,
    sui::{
        client::{
            BlobPersistence,
            ExpirySelectionPolicy,
            PostStoreAction,
            ReadClient,
            SuiContractClient,
        },
        config::WalletConfig,
        types::move_structs::{Authorized, BlobAttribute, EpochState},
        utils::SuiNetwork,
    },
    utils::styled_spinner,
};
use walrus_storage_node_client::api::BlobStatus;
use walrus_sui::{client::rpc_client, wallet::Wallet};
use walrus_utils::{metrics::Registry, read_blob_from_file};

use super::{
    args::{
        AggregatorArgs,
        BlobIdentifiers,
        BlobIdentity,
        BurnSelection,
        CliCommands,
        DaemonArgs,
        DaemonCommands,
        EpochArg,
        FileOrBlobId,
        HealthSortBy,
        InfoCommands,
        NodeAdminCommands,
        NodeSelection,
        PublisherArgs,
        RpcArg,
        SortBy,
        UserConfirmation,
    },
    backfill::{pull_archive_blobs, run_blob_backfill},
};
use crate::{
    client::{
        ClientConfig,
        ClientDaemon,
        cli::{
            BlobIdDecimal,
            CliOutput,
            HumanReadableFrost,
            HumanReadableMist,
            QuiltBlobInput,
            QuiltPatchByIdentifier,
            QuiltPatchByPatchId,
            QuiltPatchByTag,
            QuiltPatchSelector,
            args::TraceExporter,
            get_contract_client,
            get_read_client,
            get_sui_read_client_from_rpc_node_or_wallet,
            success,
            warning,
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
            GetBlobAttributeOutput,
            InfoBftOutput,
            InfoCommitteeOutput,
            InfoEpochOutput,
            InfoOutput,
            InfoPriceOutput,
            InfoSizeOutput,
            InfoStorageOutput,
            ReadOutput,
            ReadQuiltOutput,
            ServiceHealthInfoOutput,
            ShareBlobOutput,
            StakeOutput,
            StoreQuiltDryRunOutput,
            WalletOutput,
        },
    },
    common::telemetry::TracingSubscriberBuilder,
    utils::{self, MetricsAndLoggingRuntime, generate_sui_wallet},
};

fn apply_upload_mode_to_config(
    mut config: walrus_sdk::config::ClientConfig,
    upload_mode: UploadMode,
) -> walrus_sdk::config::ClientConfig {
    config.communication_config = upload_mode.apply_to(config.communication_config.clone());
    config
}

/// A helper struct to run commands for the Walrus client.
#[allow(missing_debug_implementations)]
pub struct ClientCommandRunner {
    /// The Sui wallet for the client.
    wallet: Result<Wallet>,
    /// The config for the client.
    config: Result<ClientConfig>,
    /// Whether to output JSON.
    json: bool,
    /// The gas budget for the client commands.
    gas_budget: Option<u64>,
}

impl ClientCommandRunner {
    /// Creates a new client runner, loading the configuration and wallet context.
    pub fn new(
        config_path: &Option<PathBuf>,
        context: Option<&str>,
        wallet_override: &Option<PathBuf>,
        gas_budget: Option<u64>,
        json: bool,
    ) -> Self {
        let config = load_configuration(config_path.as_ref(), context);
        let wallet_config = wallet_override
            .as_ref()
            .map(WalletConfig::from_path)
            .or(config
                .as_ref()
                .ok()
                .and_then(|config: &ClientConfig| config.wallet_config.clone()));
        let wallet = WalletConfig::load_wallet(
            wallet_config.as_ref(),
            config
                .as_ref()
                .ok()
                .and_then(|config| config.communication_config.sui_client_request_timeout),
        );

        Self {
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
    pub async fn run_cli_app(
        self,
        command: CliCommands,
        trace_cli: Option<TraceExporter>,
    ) -> Result<()> {
        let result = {
            let mut subscriber_builder = TracingSubscriberBuilder::default();
            match trace_cli {
                Some(TraceExporter::Otlp) => {
                    subscriber_builder.with_otlp_trace_exporter();
                }
                Some(TraceExporter::File(path)) => {
                    subscriber_builder.with_file_trace_exporter(path);
                }
                None => (),
            }
            // Since we may export OTLP, this needs to be initialised in an async context.
            let _guard = subscriber_builder.init()?;

            self.run_cli_app_inner(command).await
        };

        opentelemetry::global::shutdown_tracer_provider();
        result
    }

    #[tracing::instrument(name="run", skip_all, fields(command = command.as_str()))]
    async fn run_cli_app_inner(self, command: CliCommands) -> Result<()> {
        match command {
            CliCommands::Read {
                blob_id,
                out,
                rpc_arg: RpcArg { rpc_url },
            } => self.read(blob_id, out, rpc_url).await,

            CliCommands::ReadQuilt {
                quilt_patch_query,
                out,
                rpc_arg: RpcArg { rpc_url },
            } => {
                self.read_quilt(quilt_patch_query.into_selector()?, out, rpc_url)
                    .await
            }

            CliCommands::ListPatchesInQuilt {
                quilt_id,
                rpc_arg: RpcArg { rpc_url },
            } => self.list_patches_in_quilt(quilt_id, rpc_url).await,

            CliCommands::Store {
                files,
                common_options,
            } => {
                self.store(
                    files,
                    common_options.epoch_arg,
                    common_options.dry_run,
                    StoreOptimizations::from_force_and_ignore_resources_flags(
                        common_options.force,
                        common_options.ignore_resources,
                    ),
                    BlobPersistence::from_deletable_and_permanent(
                        common_options.deletable,
                        common_options.permanent,
                    )?,
                    PostStoreAction::from_share(common_options.share),
                    common_options.encoding_type,
                    common_options.upload_relay,
                    common_options.skip_tip_confirmation.into(),
                    common_options.upload_mode.map(Into::into),
                )
                .await
            }

            CliCommands::StoreQuilt {
                paths,
                blobs,
                common_options,
            } => {
                self.store_quilt(
                    paths,
                    blobs,
                    common_options.epoch_arg,
                    common_options.dry_run,
                    StoreOptimizations::from_force_and_ignore_resources_flags(
                        common_options.force,
                        common_options.ignore_resources,
                    ),
                    BlobPersistence::from_deletable_and_permanent(
                        common_options.deletable,
                        common_options.permanent,
                    )?,
                    PostStoreAction::from_share(common_options.share),
                    common_options.encoding_type,
                    common_options.upload_relay,
                    common_options.skip_tip_confirmation.into(),
                    common_options.upload_mode.map(Into::into),
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
                concurrent_requests,
            } => {
                self.health(rpc_url, node_selection, detail, sort, concurrent_requests)
                    .await
            }

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
                    // This automatically creates the Sui configuration directory if it doesn't
                    // exist.
                    let config_dir = sui_config_dir()?;
                    anyhow::ensure!(
                        config_dir.read_dir()?.next().is_none(),
                        "The Sui configuration directory {} is not empty; please specify a \
                        different wallet path using `--path` or manage the wallet using the Sui \
                        CLI.",
                        config_dir.display()
                    );
                    config_dir.join(SUI_CLIENT_CONFIG)
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
                let sui_read_client =
                    get_sui_read_client_from_rpc_node_or_wallet(&self.config?, None, self.wallet)
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

            CliCommands::NodeAdmin { command } => self.run_admin_command(command).await,
            CliCommands::PullArchiveBlobs {
                gcs_bucket,
                prefix,
                backfill_dir,
                pulled_state,
            } => pull_archive_blobs(gcs_bucket, prefix, backfill_dir, pulled_state).await,
            CliCommands::BlobBackfill {
                backfill_dir,
                node_ids,
                pushed_state,
            } => {
                let result = run_blob_backfill(backfill_dir, node_ids, pushed_state).await;
                tracing::info!("blob backfill exited with: {:?}", result);
                result
            }
        }
    }

    /// Runs the binary commands in "daemon" mode (i.e., running a server).
    ///
    /// Consumes `self`.
    #[tokio::main]
    pub async fn run_daemon_app(
        mut self,
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

    fn maybe_export_contract_info(&mut self, registry: &Registry) {
        let Ok(config) = self.config.as_ref() else {
            return;
        };
        utils::export_contract_info(
            registry,
            &config.contract_config.system_object,
            &config.contract_config.staking_object,
            match &mut self.wallet {
                Ok(wallet_context) => wallet_context.active_address().ok(),
                Err(_) => None,
            },
        );
    }

    // Implementations of client commands.

    pub(crate) async fn read(
        self,
        blob_id: BlobId,
        out: Option<PathBuf>,
        rpc_url: Option<String>,
    ) -> Result<()> {
        let client = get_read_client(self.config?, rpc_url, self.wallet, &None, None).await?;

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

    pub(crate) async fn read_quilt(
        self,
        selector: QuiltPatchSelector,
        out: Option<PathBuf>,
        rpc_url: Option<String>,
    ) -> Result<()> {
        let config = self.config?;
        let sui_read_client =
            get_sui_read_client_from_rpc_node_or_wallet(&config, rpc_url, self.wallet).await?;
        let read_client =
            WalrusNodeClient::new_read_client_with_refresher(config, sui_read_client).await?;

        let quilt_read_client = read_client.quilt_client();

        let mut retrieved_blobs = match selector {
            QuiltPatchSelector::ByIdentifier(QuiltPatchByIdentifier {
                quilt_id,
                identifiers,
            }) => {
                let identifiers: Vec<&str> = identifiers.iter().map(|s| s.as_str()).collect();
                quilt_read_client
                    .get_blobs_by_identifiers(&quilt_id, &identifiers)
                    .await?
            }
            QuiltPatchSelector::ByTag(QuiltPatchByTag {
                quilt_id,
                tag,
                value,
            }) => {
                quilt_read_client
                    .get_blobs_by_tag(&quilt_id, &tag, &value)
                    .await?
            }
            QuiltPatchSelector::ByPatchId(QuiltPatchByPatchId { quilt_patch_ids }) => {
                quilt_read_client.get_blobs_by_ids(&quilt_patch_ids).await?
            }
            QuiltPatchSelector::All(quilt_id) => quilt_read_client.get_all_blobs(&quilt_id).await?,
        };

        tracing::info!("retrieved {} blobs from quilt", retrieved_blobs.len());

        if let Some(out) = out.as_ref() {
            Self::write_blobs_dedup(&mut retrieved_blobs, out).await?;
        }

        ReadQuiltOutput::new(out.clone(), retrieved_blobs).print_output(self.json)
    }

    pub(crate) async fn list_patches_in_quilt(
        self,
        quilt_id: BlobId,
        rpc_url: Option<String>,
    ) -> Result<()> {
        let config = self.config?;
        let sui_read_client =
            get_sui_read_client_from_rpc_node_or_wallet(&config, rpc_url, self.wallet).await?;
        let read_client =
            WalrusNodeClient::new_read_client_with_refresher(config, sui_read_client).await?;

        let quilt_read_client = read_client.quilt_client();
        let quilt_metadata = quilt_read_client.get_quilt_metadata(&quilt_id).await?;

        quilt_metadata.print_output(self.json)?;

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn store(
        self,
        files: Vec<PathBuf>,
        epoch_arg: EpochArg,
        dry_run: bool,
        store_optimizations: StoreOptimizations,
        persistence: BlobPersistence,
        post_store: PostStoreAction,
        encoding_type: Option<EncodingType>,
        upload_relay: Option<Url>,
        confirmation: UserConfirmation,
        upload_mode: Option<UploadMode>,
    ) -> Result<()> {
        epoch_arg.exactly_one_is_some()?;
        if encoding_type.is_some_and(|encoding| !encoding.is_supported()) {
            anyhow::bail!(ClientErrorKind::UnsupportedEncodingType(
                encoding_type.expect("just checked that option is Some")
            ));
        }

        // Apply CLI upload preset to the in-memory config before building the client, if provided.
        let mut config = self.config?;
        config = apply_upload_mode_to_config(config, upload_mode.unwrap_or(UploadMode::Balanced));
        let client = get_contract_client(config, self.wallet, self.gas_budget, &None).await?;

        let system_object = client.sui_client().read_client.get_system_object().await?;
        let epochs_ahead =
            get_epochs_ahead(epoch_arg, system_object.max_epochs_ahead(), &client).await?;

        if persistence.is_deletable() && post_store == PostStoreAction::Share {
            anyhow::bail!("deletable blobs cannot be shared");
        }

        let encoding_type = encoding_type.unwrap_or(DEFAULT_ENCODING);

        if dry_run {
            return Self::store_dry_run(client, files, encoding_type, epochs_ahead, self.json)
                .await;
        }

        tracing::info!("storing {} files as blobs on Walrus", files.len());
        let start_timer = std::time::Instant::now();

        let blobs = tracing::info_span!("read_blobs").in_scope(|| {
            files
                .into_iter()
                .map(|file| read_blob_from_file(&file).map(|blob| (file, blob)))
                .collect::<Result<Vec<(PathBuf, Vec<u8>)>>>()
        })?;

        let mut store_args = StoreArgs::new(
            encoding_type,
            epochs_ahead,
            store_optimizations,
            persistence,
            post_store,
        );

        if let Some(upload_relay) = upload_relay {
            let upload_relay_client = UploadRelayClient::new(
                client.sui_client().address(),
                client.encoding_config().n_shards(),
                upload_relay,
                self.gas_budget,
                client.config().backoff_config().clone(),
            )
            .await?;
            // Store operations will use the upload relay.
            store_args = store_args.with_upload_relay_client(upload_relay_client);

            let total_tip = store_args.compute_total_tip_amount(
                client.encoding_config().n_shards(),
                &blobs
                    .iter()
                    .map(|blob| blob.1.len().try_into().expect("32 or 64-bit arch"))
                    .collect::<Vec<_>>(),
            )?;

            if confirmation.is_required() {
                ask_for_tip_confirmation(total_tip)?;
            }
        }

        let results = client
            .reserve_and_store_blobs_retry_committees_with_path(&blobs, &store_args)
            .await?;
        let blobs_len = blobs.len();
        if results.len() != blobs_len {
            let not_stored = results
                .iter()
                .filter(|blob| blob.blob_store_result.is_not_stored())
                .map(|blob| blob.path.clone())
                .collect::<Vec<_>>();
            tracing::warn!(
                "some blobs ({}) are not stored",
                not_stored
                    .into_iter()
                    .map(|path| path.display().to_string())
                    .join(", ")
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

    #[tracing::instrument(skip_all)]
    async fn store_dry_run(
        client: WalrusNodeClient<SuiContractClient>,
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
            let (_, metadata) =
                client.encode_pairs_and_metadata(&blob, encoding_type, &MultiProgress::new())?;
            let unencoded_size = metadata.metadata().unencoded_length();
            let encoded_size = encoded_blob_length_for_n_shards(
                encoding_config.n_shards(),
                unencoded_size,
                encoding_type,
            )
            .expect("must be valid as the encoding succeeded");
            let storage_cost = client.get_price_computation().await?.operation_cost(
                &RegisterBlobOp::RegisterFromScratch {
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

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn store_quilt(
        self,
        paths: Vec<PathBuf>,
        blobs: Vec<QuiltBlobInput>,
        epoch_arg: EpochArg,
        dry_run: bool,
        store_optimizations: StoreOptimizations,
        persistence: BlobPersistence,
        post_store: PostStoreAction,
        encoding_type: Option<EncodingType>,
        upload_relay: Option<Url>,
        confirmation: UserConfirmation,
        upload_mode: Option<UploadMode>,
    ) -> Result<()> {
        epoch_arg.exactly_one_is_some()?;
        if encoding_type.is_some_and(|encoding| !encoding.is_supported()) {
            anyhow::bail!(ClientErrorKind::UnsupportedEncodingType(
                encoding_type.expect("just checked that option is Some")
            ));
        }
        if persistence.is_deletable() && post_store == PostStoreAction::Share {
            anyhow::bail!("deletable blobs cannot be shared");
        }

        let encoding_type = encoding_type.unwrap_or(DEFAULT_ENCODING);
        // Apply CLI upload preset to the in-memory config before building the client, if provided.
        let mut config = self.config?;
        config = apply_upload_mode_to_config(config, upload_mode.unwrap_or(UploadMode::Balanced));
        let client = get_contract_client(config, self.wallet, self.gas_budget, &None).await?;

        let system_object = client.sui_client().read_client.get_system_object().await?;
        let epochs_ahead =
            get_epochs_ahead(epoch_arg, system_object.max_epochs_ahead(), &client).await?;

        let quilt_store_blobs = Self::load_blobs_for_quilt(&paths, blobs).await?;

        if dry_run {
            return Self::store_quilt_dry_run(
                client,
                &quilt_store_blobs,
                encoding_type,
                epochs_ahead,
                self.json,
            )
            .await;
        }

        let start_timer = std::time::Instant::now();
        let quilt_write_client = client.quilt_client();
        let quilt = quilt_write_client
            .construct_quilt::<QuiltVersionV1>(&quilt_store_blobs, encoding_type)
            .await?;
        let mut store_args = StoreArgs::new(
            encoding_type,
            epochs_ahead,
            store_optimizations,
            persistence,
            post_store,
        );

        if let Some(upload_relay) = upload_relay {
            let upload_relay_client = UploadRelayClient::new(
                client.sui_client().address(),
                client.encoding_config().n_shards(),
                upload_relay,
                self.gas_budget,
                client.config().backoff_config().clone(),
            )
            .await?;
            // Store operations will use the upload relay.
            store_args = store_args.with_upload_relay_client(upload_relay_client);

            let total_tip = store_args.compute_total_tip_amount(
                client.encoding_config().n_shards(),
                &quilt_store_blobs
                    .iter()
                    .map(|blob| blob.unencoded_length())
                    .collect::<Vec<_>>(),
            )?;

            if confirmation.is_required() {
                ask_for_tip_confirmation(total_tip)?;
            }
        }

        let result = quilt_write_client
            .reserve_and_store_quilt::<QuiltVersionV1>(&quilt, &store_args)
            .await?;

        tracing::info!(
            duration = ?start_timer.elapsed(),
            "{} blobs stored in quilt",
            result.stored_quilt_blobs.len(),
        );

        result.print_output(self.json)
    }

    async fn load_blobs_for_quilt(
        paths: &[PathBuf],
        blob_inputs: Vec<QuiltBlobInput>,
    ) -> Result<Vec<QuiltStoreBlob<'static>>> {
        if !paths.is_empty() && !blob_inputs.is_empty() {
            anyhow::bail!("cannot provide both paths and blob_inputs");
        } else if !paths.is_empty() {
            let blobs = read_blobs_from_paths(paths)?;
            Ok(assign_identifiers_with_paths(blobs)?)
        } else if !blob_inputs.is_empty() {
            let paths = blob_inputs
                .iter()
                .map(|input| input.path.clone())
                .collect::<Vec<_>>();
            let mut blobs = read_blobs_from_paths(&paths)?;
            let quilt_store_blobs = blob_inputs
                .into_iter()
                .enumerate()
                .map(|(i, input)| {
                    let (path, blob) = blobs
                        .remove_entry(&input.path)
                        .expect("blob should have been read");
                    QuiltStoreBlob::new_owned(
                        blob,
                        input
                            .identifier
                            .clone()
                            .unwrap_or_else(|| generate_identifier_from_path(&path, i)),
                    )
                    .map(|blob| blob.with_tags(input.tags.clone()))
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(quilt_store_blobs)
        } else {
            anyhow::bail!("either paths or blob_inputs must be provided");
        }
    }

    /// Performs a dry run of storing a quilt.
    async fn store_quilt_dry_run(
        client: WalrusNodeClient<SuiContractClient>,
        blobs: &[QuiltStoreBlob<'static>],
        encoding_type: EncodingType,
        epochs_ahead: EpochCount,
        json: bool,
    ) -> Result<()> {
        tracing::info!("performing dry-run for quilt from {} blobs", blobs.len());

        let quilt_client = client.quilt_client();
        let quilt = quilt_client
            .construct_quilt::<QuiltVersionV1>(blobs, encoding_type)
            .await?;
        let (_, metadata) =
            client.encode_pairs_and_metadata(quilt.data(), encoding_type, &MultiProgress::new())?;
        let unencoded_size = metadata.metadata().unencoded_length();
        let encoded_size = encoded_blob_length_for_n_shards(
            client.encoding_config().n_shards(),
            unencoded_size,
            encoding_type,
        )
        .expect("must be valid as the encoding succeeded");

        let storage_cost = client.get_price_computation().await?.operation_cost(
            &RegisterBlobOp::RegisterFromScratch {
                encoded_length: encoded_size,
                epochs_ahead,
            },
        );

        let output = StoreQuiltDryRunOutput {
            quilt_blob_output: DryRunOutput {
                path: PathBuf::from("n/a"),
                blob_id: *metadata.blob_id(),
                encoding_type,
                unencoded_size,
                encoded_size,
                storage_cost,
            },
            quilt_index: QuiltIndex::V1(quilt.quilt_index()?.clone()),
        };

        output.print_output(json)
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
        let sui_read_client =
            get_sui_read_client_from_rpc_node_or_wallet(&config, rpc_url, self.wallet).await?;

        let encoding_type = encoding_type.unwrap_or(DEFAULT_ENCODING);

        let refresher_handle = config
            .refresh_config
            .build_refresher_and_run(sui_read_client.clone())
            .await?;
        let client = WalrusNodeClient::new(config, refresher_handle).await?;

        let file = file_or_blob_id.file.clone();
        let blob_id =
            file_or_blob_id.get_or_compute_blob_id(client.encoding_config(), encoding_type)?;

        let status = client
            .get_verified_blob_status(&blob_id, &sui_read_client, timeout)
            .await?;

        // Compute estimated blob expiry in DateTime if it is a permanent blob.
        let estimated_expiry_timestamp = if let BlobStatus::Permanent { end_epoch, .. } = status {
            let staking_object = sui_read_client.get_staking_object().await?;
            let epoch_duration = staking_object.epoch_duration();
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
        let sui_read_client =
            get_sui_read_client_from_rpc_node_or_wallet(&config, rpc_url, self.wallet).await?;

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
        concurrent_requests: usize,
    ) -> Result<()> {
        node_selection.exactly_one_is_set()?;

        let latest_seq =
            get_latest_checkpoint_sequence_number(rpc_url.as_ref(), &self.wallet).await;

        let config = self.config?;
        let sui_read_client =
            get_sui_read_client_from_rpc_node_or_wallet(&config, rpc_url.clone(), self.wallet)
                .await?;
        let communication_factory = NodeCommunicationFactory::new(
            config.communication_config.clone(),
            Arc::new(EncodingConfig::new(
                sui_read_client.current_committee().await?.n_shards(),
            )),
            None,
        )?;

        ServiceHealthInfoOutput::get_for_nodes(
            node_selection.get_nodes(&sui_read_client).await?,
            &communication_factory,
            latest_seq,
            detail,
            sort,
            concurrent_requests,
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
        let n_shards = if let Some(n_shards) = n_shards {
            n_shards
        } else {
            let config = self.config?;
            let sui_read_client =
                get_sui_read_client_from_rpc_node_or_wallet(&config, rpc_url, self.wallet).await?;
            tracing::debug!("reading `n_shards` from chain");
            sui_read_client.current_committee().await?.n_shards()
        };

        let encoding_type = encoding_type.unwrap_or(DEFAULT_ENCODING);

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

    #[tracing::instrument(skip_all)]
    async fn init_publisher(
        self,
        registry: &Registry,
        args: PublisherArgs,
    ) -> Result<ClientDaemon<ClientMultiplexer>> {
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

        Ok(ClientDaemon::new_publisher(
            client,
            auth_config,
            &args,
            registry,
        ))
    }

    pub(crate) async fn publisher(self, registry: &Registry, args: PublisherArgs) -> Result<()> {
        let publisher = self.init_publisher(registry, args).await?;
        publisher.run().await?;
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn init_aggregator(
        self,
        registry: &Registry,
        rpc_url: Option<String>,
        daemon_args: DaemonArgs,
        aggregator_args: AggregatorArgs,
    ) -> Result<ClientDaemon<WalrusNodeClient<SuiReadClient>>> {
        tracing::debug!(?rpc_url, "attempting to run the Walrus aggregator");
        let client = get_read_client(
            self.config?,
            rpc_url,
            self.wallet,
            &daemon_args.blocklist,
            aggregator_args.max_blob_size,
        )
        .await?;
        Ok(ClientDaemon::new_aggregator(
            client,
            daemon_args.bind_address,
            registry,
            aggregator_args.allowed_headers,
            aggregator_args.allow_quilt_patch_tags_in_response,
        ))
    }

    pub(crate) async fn aggregator(
        self,
        registry: &Registry,
        rpc_url: Option<String>,
        daemon_args: DaemonArgs,
        aggregator_args: AggregatorArgs,
    ) -> Result<()> {
        let aggregator = self
            .init_aggregator(registry, rpc_url, daemon_args, aggregator_args)
            .await?;
        aggregator.run().await?;
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn init_daemon(
        self,
        registry: &Registry,
        args: PublisherArgs,
        aggregator_args: AggregatorArgs,
    ) -> Result<ClientDaemon<ClientMultiplexer>> {
        args.print_debug_message("attempting to run the Walrus daemon");
        let client = ClientMultiplexer::new(
            self.wallet?,
            &self.config?,
            self.gas_budget,
            registry,
            &args,
        )
        .await?;
        let auth_config = args.generate_auth_config()?;

        Ok(ClientDaemon::new_daemon(
            client,
            auth_config,
            registry,
            &args,
            &aggregator_args,
        ))
    }

    pub(crate) async fn daemon(
        self,
        registry: &Registry,
        args: PublisherArgs,
        aggregator_args: AggregatorArgs,
    ) -> Result<()> {
        let daemon = self.init_daemon(registry, args, aggregator_args).await?;
        daemon.run().await?;
        Ok(())
    }

    pub(crate) fn convert_blob_id(self, blob_id_decimal: BlobIdDecimal) -> Result<()> {
        BlobIdConversionOutput::from(blob_id_decimal).print_output(self.json)
    }

    pub(crate) async fn delete(
        self,
        target: BlobIdentifiers,
        confirmation: UserConfirmation,
        no_status_check: bool,
        encoding_type: Option<EncodingType>,
    ) -> Result<()> {
        // Create client once to be reused
        let client =
            match get_contract_client(self.config?, self.wallet, self.gas_budget, &None).await {
                Ok(client) => client,
                Err(e) => {
                    if !self.json {
                        eprintln!("Error connecting to client: {e}");
                    }
                    return Err(e);
                }
            };

        let mut delete_outputs = Vec::new();

        let encoding_type = encoding_type.unwrap_or(DEFAULT_ENCODING);
        let blobs = target.get_blob_identities(client.encoding_config(), encoding_type)?;

        // Process each target
        for blob in blobs {
            let output = delete_blob(
                &client,
                blob,
                confirmation.clone(),
                no_status_check,
                self.json,
            )
            .await;
            delete_outputs.push(output);
        }

        // Check if any operations were performed
        if delete_outputs.is_empty() {
            if !self.json {
                println!("No operations were performed.");
            }
            return Ok(());
        }

        // Print results
        if self.json {
            println!("{}", serde_json::to_string(&delete_outputs)?);
        } else {
            // In CLI mode, print each result individually
            for output in &delete_outputs {
                output.print_cli_output();
            }

            // Print summary
            let success_count = delete_outputs
                .iter()
                .filter(|output| output.error.is_none() && !output.aborted && !output.no_blob_found)
                .count();
            let total_count = delete_outputs.len();

            if success_count == total_count {
                println!(
                    "\n{} All {} deletion operations completed successfully.",
                    success(),
                    total_count
                );
            } else {
                println!(
                    "\n{} {}/{} deletion operations completed successfully.",
                    warning(),
                    success_count,
                    total_count
                );
            }
        }

        Ok(())
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

    pub(crate) async fn run_admin_command(self, command: NodeAdminCommands) -> Result<()> {
        let sui_client = self
            .config?
            .new_contract_client(self.wallet?, self.gas_budget)
            .await?;
        match command {
            NodeAdminCommands::VoteForUpgrade {
                node_id,
                upgrade_manager_object_id,
                package_path,
            } => {
                let digest = sui_client
                    .vote_for_upgrade(upgrade_manager_object_id, node_id, package_path)
                    .await?;
                println!(
                    "{} Voted for package upgrade with digest 0x{}",
                    success(),
                    digest.iter().map(|b| format!("{b:02x}")).join("")
                );
            }
            NodeAdminCommands::SetCommissionAuthorized {
                node_id,
                object_or_address,
            } => {
                let authorized: Authorized = object_or_address.try_into()?;
                sui_client
                    .set_commission_receiver(node_id, authorized.clone())
                    .await?;
                println!(
                    "{} Commission receiver for node id {} has been set to {}",
                    success(),
                    node_id,
                    authorized
                );
            }
            NodeAdminCommands::SetGovernanceAuthorized {
                node_id,
                object_or_address,
            } => {
                let authorized: Authorized = object_or_address.try_into()?;
                sui_client
                    .set_governance_authorized(node_id, authorized.clone())
                    .await?;
                println!(
                    "{} Governance authorization for node id {} has been set to {}",
                    success(),
                    node_id,
                    authorized
                );
            }
            NodeAdminCommands::CollectCommission { node_id } => {
                let amount =
                    HumanReadableFrost::from(sui_client.collect_commission(node_id).await?);
                println!("{} Collected {} as commission", success(), amount);
            }
            NodeAdminCommands::PackageDigest { package_path } => {
                let digest = sui_client
                    .read_client()
                    .compute_package_digest(package_path.clone())
                    .await?;
                println!(
                    "{} Digest for package '{}':\n  Hex: 0x{}\n  Base64: {}",
                    success(),
                    package_path.display(),
                    digest.iter().map(|b| format!("{b:02x}")).join(""),
                    fastcrypto::encoding::Base64::encode(digest)
                );
            }
        }
        Ok(())
    }

    async fn write_blobs_dedup(
        blobs: &mut [QuiltStoreBlob<'static>],
        out_dir: &Path,
    ) -> Result<()> {
        let mut filename_counters = std::collections::HashMap::new();

        for blob in &mut *blobs {
            let original_filename = blob.identifier().to_owned();
            let counter = filename_counters
                .entry(original_filename.clone())
                .or_insert(0);
            *counter += 1;

            let output_file_path = if *counter == 1 {
                out_dir.join(&original_filename)
            } else {
                let (stem, extension) = match original_filename.rsplit_once('.') {
                    Some((s, e)) => (s, Some(e)),
                    None => (original_filename.as_str(), None),
                };

                let new_filename = if let Some(ext) = extension {
                    format!("{stem}_{counter}.{ext}")
                } else {
                    format!("{stem}_{counter}")
                };

                out_dir.join(new_filename)
            };

            std::fs::write(&output_file_path, blob.take_blob())?;
        }

        Ok(())
    }
}

async fn delete_blob(
    client: &WalrusNodeClient<SuiContractClient>,
    target: BlobIdentity,
    confirmation: UserConfirmation,
    no_status_check: bool,
    json: bool,
) -> DeleteOutput {
    let mut result = DeleteOutput {
        blob_identity: target.clone(),
        ..Default::default()
    };

    if let Some(blob_id) = target.blob_id {
        let to_delete = match client.deletable_blobs_by_id(&blob_id).await {
            Ok(blobs) => blobs.collect::<Vec<_>>(),
            Err(e) => {
                result.error = Some(e.to_string());
                return result;
            }
        };

        if to_delete.is_empty() {
            result.no_blob_found = true;
            return result;
        }

        if confirmation.is_required() && !json {
            println!(
                "The following blobs with blob ID {blob_id} are deletable:\n{}",
                to_delete.iter().map(|blob| blob.id.to_string()).join("\n")
            );
            match ask_for_confirmation() {
                Ok(confirmed) => {
                    if !confirmed {
                        println!("{} Aborting. No blobs were deleted.", success());
                        result.aborted = true;
                        return result;
                    }
                }
                Err(e) => {
                    result.error = Some(e.to_string());
                    return result;
                }
            }
        }

        if !json {
            println!("Deleting blobs...");
        }

        for blob in to_delete.iter() {
            if let Err(e) = client.delete_owned_blob_by_object(blob.id).await {
                result.error = Some(e.to_string());
                return result;
            }
            result.deleted_blobs.push(blob.clone());
        }

        if !no_status_check {
            // Wait to ensure that the deletion information is propagated.
            tokio::time::sleep(Duration::from_secs(1)).await;
            result.post_deletion_status = match client
                .get_blob_status_with_retries(&blob_id, client.sui_client())
                .await
            {
                Ok(status) => Some(status),
                Err(e) => {
                    result.error = Some(format!("Failed to get post-deletion status: {e}"));
                    None
                }
            };
        }
    } else if let Some(object_id) = target.object_id {
        let to_delete = match client
            .sui_client()
            .owned_blobs(None, ExpirySelectionPolicy::Valid)
            .await
        {
            Ok(blobs) => blobs.into_iter().find(|blob| blob.id == object_id),
            Err(e) => {
                result.error = Some(e.to_string());
                return result;
            }
        };

        if let Some(blob) = to_delete {
            if let Err(e) = client.delete_owned_blob_by_object(object_id).await {
                result.error = Some(e.to_string());
                return result;
            }
            result.deleted_blobs = vec![blob];
        } else {
            result.no_blob_found = true;
        }
    } else {
        result.error = Some("No valid target provided".to_string());
    }

    result
}

#[tracing::instrument(skip_all)]
async fn get_epochs_ahead(
    epoch_arg: EpochArg,
    max_epochs_ahead: EpochCount,
    client: &WalrusNodeClient<SuiContractClient>,
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
                "earliest_expiry_time must be greater than the current epoch start time \
                and the current time"
            );
            let delta =
                (earliest_expiry_ts - estimated_start_of_current_epoch).num_milliseconds() as u64;
            (delta / staking_object.epoch_duration_millis() + 1)
                .try_into()
                .map_err(|_| anyhow::anyhow!("expiry time is too far in the future"))?
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

pub fn ask_for_tip_confirmation(total_tip: Option<u64>) -> Result<bool> {
    if let Some(total_tip) = total_tip {
        println!(
            "You are about to store the blobs using the provided upload relay;\n \
            on top of the Walrus base costs, the tip for the upload relay will \
            amount to {} (+ gas fees).",
            HumanReadableMist::from(total_tip)
        );

        if !ask_for_confirmation()? {
            anyhow::bail!("operation cancelled by user");
        }
    }

    // If no tip is required, we proceed with the upload.
    Ok(true)
}

/// Get the latest checkpoint sequence number from the Sui RPC node.
async fn get_latest_checkpoint_sequence_number(
    rpc_url: Option<&String>,
    wallet: &Result<Wallet, anyhow::Error>,
) -> Option<u64> {
    // Early return if no URL is available
    let url = if let Some(url) = rpc_url {
        url.clone()
    } else if let Ok(wallet) = wallet {
        match wallet.get_rpc_url() {
            Ok(rpc) => rpc,
            Err(error) => {
                eprintln!("Failed to get full node RPC URL. (error: {error})");
                return None;
            }
        }
    } else {
        println!("Failed to get full node RPC URL.");
        return None;
    };

    // Now url is a String, not an Option<String>
    let rpc_client_result = rpc_client::create_sui_rpc_client(&url);
    if let Ok(rpc_client) = rpc_client_result {
        match rpc_client.get_latest_checkpoint().await {
            Ok(checkpoint) => Some(checkpoint.sequence_number),
            Err(e) => {
                eprintln!("Failed to get latest checkpoint: {e}");
                None
            }
        }
    } else {
        println!("Failed to create RPC client.");
        None
    }
}

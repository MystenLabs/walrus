// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Helper struct to run the Walrus client binary commands.

use std::{
    io::Write,
    iter,
    num::NonZeroU16,
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use fastcrypto::encoding::Encoding;
use itertools::Itertools as _;
use rand::seq::SliceRandom;
use reqwest::Url;
use serde::Deserialize;
use serde_json;
use sui_config::{SUI_CLIENT_CONFIG, sui_config_dir};
use sui_types::base_types::ObjectID;
use tokio::sync::Mutex as TokioMutex;
use walrus_core::{
    BlobId,
    DEFAULT_ENCODING,
    EncodingType,
    EpochCount,
    SUPPORTED_ENCODING_TYPES,
    encoding::{
        ConsistencyCheckType,
        EncodingConfig,
        EncodingFactory as _,
        Primary,
        encoded_blob_length_for_n_shards,
        quilt_encoding::{
            QuiltApi,
            QuiltConfigApi,
            QuiltEncoderApi,
            QuiltStoreBlob,
            QuiltV1,
            QuiltVersion,
            QuiltVersionV1,
        },
    },
    ensure,
    metadata::{BlobMetadataApi as _, QuiltIndex},
};
use walrus_sdk::{
    SuiReadClient,
    client::{
        NodeCommunicationFactory,
        StoreArgs,
        StoreBlobsApi as _,
        WalrusNodeClient,
        WalrusNodeClientCreatedInBackground,
        quilt_client::{
            QuiltClient,
            assign_identifiers_with_paths,
            generate_identifier_from_path,
            read_blobs_from_paths,
        },
        resource::RegisterBlobOp,
        responses as sdk_responses,
        upload_relay_client::UploadRelayClient,
    },
    config::load_configuration,
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
    uploader::TailHandling,
    utils::styled_spinner,
};
use walrus_storage_node_client::api::BlobStatus;
use walrus_sui::{client::rpc_client, wallet::Wallet};
use walrus_utils::{load_from_yaml_str, metrics::Registry, read_blob_from_file};

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
            LocalFileQuiltQuery,
            QuiltBlobInput,
            QuiltPatchByIdentifier,
            QuiltPatchByPatchId,
            QuiltPatchByTag,
            QuiltPatchSelector,
            args::{CommonStoreOptions, TraceExporter},
            get_contract_client,
            get_read_client,
            get_sui_read_client_from_rpc_node_or_wallet,
            internal_run::{
                ChildUploaderEvent,
                InternalRunContext,
                emit_child_event,
                emit_v1_certified_event,
                maybe_spawn_child_upload_process,
                quilt_blob_input_to_cli_arg,
            },
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
            StoreQuiltToFileOutput,
            WalletOutput,
        },
    },
    common::telemetry::TracingSubscriberBuilder,
    utils::{self, MetricsAndLoggingRuntime, generate_sui_wallet},
};

const MAINNET_CLIENT_CONFIG_YAML: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../setup/client_config_mainnet.yaml"
));
const TESTNET_CLIENT_CONFIG_YAML: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../setup/client_config_testnet.yaml"
));

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct ContractIds {
    system_object: ObjectID,
    staking_object: ObjectID,
}

#[derive(Debug)]
struct KnownNetworkIds {
    mainnet: Option<ContractIds>,
    testnet: Option<ContractIds>,
}

fn known_network_ids() -> &'static KnownNetworkIds {
    static KNOWN: OnceLock<KnownNetworkIds> = OnceLock::new();
    KNOWN.get_or_init(|| KnownNetworkIds {
        mainnet: load_from_yaml_str(
            "mainnet client config",
            MAINNET_CLIENT_CONFIG_YAML,
            "contract ids",
        ),
        testnet: load_from_yaml_str(
            "testnet client config",
            TESTNET_CLIENT_CONFIG_YAML,
            "contract ids",
        ),
    })
}

fn default_child_process_uploads(config: &ClientConfig) -> bool {
    let config_ids = ContractIds {
        system_object: config.contract_config.system_object,
        staking_object: config.contract_config.staking_object,
    };
    let known = known_network_ids();
    if known.mainnet.as_ref() == Some(&config_ids) {
        return false;
    }
    known.testnet.as_ref() == Some(&config_ids)
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
            let _guard = subscriber_builder.init_scoped()?;

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
                strict_consistency_check,
                skip_consistency_check,
            } => {
                let consistency_check = crate::client::utils::consistency_check_type_from_flags(
                    strict_consistency_check,
                    skip_consistency_check,
                )?;
                self.read(blob_id, out, rpc_url, consistency_check).await
            }

            CliCommands::ReadQuilt {
                quilt_patch_query,
                out,
                rpc_arg: RpcArg { rpc_url },
            } => {
                if let Some(file_path) = quilt_patch_query.file.clone() {
                    // Read from local file
                    let n_shards = quilt_patch_query.n_shards;
                    let query = quilt_patch_query.into_local_file_query()?;
                    self.read_quilt_from_file(file_path, n_shards, query, out, rpc_url)
                        .await
                } else {
                    // Fetch from network
                    self.read_quilt(quilt_patch_query.into_selector()?, out, rpc_url)
                        .await
                }
            }

            CliCommands::ListPatchesInQuilt {
                quilt_id,
                rpc_arg: RpcArg { rpc_url },
            } => self.list_patches_in_quilt(quilt_id, rpc_url).await,

            CliCommands::Store {
                files,
                common_options,
            } => self.store(files, common_options.try_into()?).await,

            CliCommands::StoreQuilt {
                paths,
                blobs,
                file,
                n_shards,
                rpc_arg: RpcArg { rpc_url },
                common_options,
            } => {
                if let Some(file_path) = file {
                    self.store_quilt_to_file(
                        paths,
                        blobs,
                        file_path,
                        n_shards,
                        common_options.encoding_type,
                        rpc_url,
                    )
                    .await
                } else {
                    self.store_quilt(paths, blobs, common_options.try_into()?)
                        .await
                }
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
            self.wallet
                .as_ref()
                .map(|wallet| wallet.active_address())
                .ok(),
        );
    }

    // Implementations of client commands.

    pub(crate) async fn read(
        self,
        blob_id: BlobId,
        out: Option<PathBuf>,
        rpc_url: Option<String>,
        consistency_check: ConsistencyCheckType,
    ) -> Result<()> {
        let client = get_read_client(self.config?, rpc_url, self.wallet, &None, None).await?;

        let start_timer = std::time::Instant::now();
        let blob = client
            .read_blob_with_consistency_check_type::<Primary>(&blob_id, consistency_check)
            .await?;
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
            Self::write_blobs_dedup(&mut retrieved_blobs, out)?;
        }

        ReadQuiltOutput::new(out.clone(), retrieved_blobs).print_output(self.json)
    }

    /// Reads quilt patches from a local file instead of fetching from the network.
    pub(crate) async fn read_quilt_from_file(
        self,
        file: PathBuf,
        n_shards: Option<NonZeroU16>,
        query: LocalFileQuiltQuery,
        out: Option<PathBuf>,
        rpc_url: Option<String>,
    ) -> Result<()> {
        // Get n_shards either from the argument or from the chain
        let n_shards = match n_shards {
            Some(n) => n,
            None => {
                let config = match self.config.as_ref() {
                    Ok(config) => config,
                    Err(e) => bail!("failed to load client config: {e}"),
                };
                let sui_read_client =
                    get_sui_read_client_from_rpc_node_or_wallet(config, rpc_url, self.wallet)
                        .await?;
                tracing::debug!("reading `n_shards` from chain");
                sui_read_client.current_committee().await?.n_shards()
            }
        };

        // Read the quilt blob from the file
        let quilt_blob = read_blob_from_file(&file)?;
        tracing::info!(
            "read quilt blob from file: {} ({} bytes)",
            file.display(),
            quilt_blob.len()
        );

        // Create the encoding config and parse the quilt
        let encoding_config = EncodingConfig::new(n_shards).get_for_type(DEFAULT_ENCODING);
        let quilt =
            QuiltV1::new_from_quilt_blob(quilt_blob, encoding_config).with_context(|| {
                format!(
                    "failed to parse quilt from file: {}. \
                    Make sure the file contains valid quilt data and n_shards ({}) is correct.",
                    file.display(),
                    n_shards
                )
            })?;

        // Query the blobs based on the query type
        let mut retrieved_blobs = match query {
            LocalFileQuiltQuery::ByIdentifiers(identifiers) => {
                let ids: Vec<&str> = identifiers.iter().map(|s| s.as_str()).collect();
                quilt.get_blobs_by_identifiers(&ids)?
            }
            LocalFileQuiltQuery::ByTag { tag, value } => quilt.get_blobs_by_tag(&tag, &value)?,
            LocalFileQuiltQuery::ByPatchIds(patch_ids) => patch_ids
                .iter()
                .map(|id| quilt.get_blob_by_patch_internal_id(id))
                .collect::<Result<Vec<_>, _>>()?,
            LocalFileQuiltQuery::All => quilt.get_all_blobs()?,
        };

        tracing::info!("retrieved {} blobs from quilt file", retrieved_blobs.len());

        if let Some(out) = out.as_ref() {
            Self::write_blobs_dedup(&mut retrieved_blobs, out)?;
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

    // TODO(WAL-1098): Refactor this function and reduce duplication with `store_quilt`.
    async fn store(
        self,
        files: Vec<PathBuf>,
        StoreOptions {
            epoch_arg,
            dry_run,
            store_optimizations,
            persistence,
            post_store,
            encoding_type,
            upload_relay,
            confirmation,
            child_process_uploads,
            internal_run,
        }: StoreOptions,
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

        let config = self.config?;
        let child_process_uploads = child_process_uploads
            .unwrap_or_else(|| default_child_process_uploads(&config))
            && !internal_run;
        let wallet = self.wallet?;
        let active_address = wallet.active_address();
        let mut client_created_in_bg = WalrusNodeClientCreatedInBackground::new(
            get_contract_client(config.clone(), wallet, self.gas_budget),
            config.contract_config.n_shards,
        );
        let epochs_ahead = get_epochs_ahead(
            &epoch_arg,
            config.contract_config.max_epochs_ahead,
            &mut client_created_in_bg,
        )
        .await?;

        if dry_run {
            return Self::store_dry_run(
                client_created_in_bg.into_client().await?,
                files,
                encoding_type,
                epochs_ahead,
                self.json,
            )
            .await;
        }

        let file_count = files.len();
        tracing::info!(
            "storing {} file{} on Walrus",
            file_count,
            if file_count == 1 { "" } else { "s" }
        );
        let start_timer = std::time::Instant::now();

        let blobs = tracing::info_span!("read_blobs").in_scope(|| {
            files
                .into_iter()
                .map(|file| read_blob_from_file(&file).map(|blob| (file, blob)))
                .collect::<Result<Vec<(PathBuf, Vec<u8>)>>>()
        })?;

        if blobs.len() > 1 {
            let max_total_blob_size = config.communication_config.max_total_blob_size;
            let total_blob_size = blobs.iter().map(|(_, blob)| blob.len()).sum::<usize>();
            if total_blob_size > max_total_blob_size {
                anyhow::bail!(
                    "total size of all blobs ({total_blob_size}) exceeds the maximum limit of \
                    {max_total_blob_size}; you can change this limit in the client config"
                );
            }
        }

        if let Some(()) = maybe_spawn_child_upload_process(
            &config,
            child_process_uploads,
            &epoch_arg,
            dry_run,
            &store_optimizations,
            persistence,
            post_store,
            upload_relay.as_ref(),
            internal_run,
            "store",
            |cmd| {
                for (path, _) in &blobs {
                    cmd.arg(path);
                }
            },
            blobs.len(),
        )
        .await?
        {
            return Ok(());
        }

        let base_store_args = StoreArgs::new(
            encoding_type,
            epochs_ahead,
            store_optimizations,
            persistence,
            post_store,
        );

        let mut internal_run_ctx = InternalRunContext::new(internal_run, &config, blobs.len());
        let mut store_args = internal_run_ctx.configure_store_args(internal_run, base_store_args);
        let mut tail_handle_collector: Option<Arc<TokioMutex<Vec<tokio::task::JoinHandle<()>>>>> =
            None;
        if !internal_run
            && matches!(
                config.communication_config.tail_handling,
                TailHandling::Detached
            )
        {
            if !child_process_uploads {
                tracing::warn!(
                    "tail uploads will run in the current process; \
                    rerun with --child-process-uploads to offload them"
                );
            }
            let collector = Arc::new(TokioMutex::new(Vec::new()));
            store_args = store_args
                .with_tail_handling(TailHandling::Detached)
                .with_tail_handle_collector(collector.clone());
            tail_handle_collector = Some(collector);
        }

        if let Some(upload_relay) = upload_relay {
            let n_shards = client_created_in_bg.encoding_config().await?.n_shards();
            let upload_relay_client = UploadRelayClient::new(
                active_address,
                n_shards,
                upload_relay,
                self.gas_budget,
                config.backoff_config().clone(),
            )
            .await?;
            // Store operations will use the upload relay.
            store_args = store_args.with_upload_relay_client(upload_relay_client);

            let total_tip = store_args.compute_total_tip_amount(
                n_shards,
                &blobs
                    .iter()
                    .map(|blob| blob.1.len().try_into().expect("32 or 64-bit arch"))
                    .collect::<Vec<_>>(),
            )?;

            if confirmation.is_required() {
                ask_for_tip_confirmation(total_tip)?;
            }
        }

        let blobs_len = blobs.len();
        let results = client_created_in_bg
            .reserve_and_store_blobs_retry_committees_with_path(blobs, &store_args)
            .await?;

        internal_run_ctx.finalize_after_store(&mut store_args).await;

        if let Some(collector) = tail_handle_collector {
            let mut handles_guard = collector.lock().await;
            let mut handles = Vec::new();
            std::mem::swap(&mut *handles_guard, &mut handles);
            drop(handles_guard);
            for handle in handles {
                if let Err(err) = handle.await {
                    tracing::warn!(?err, "tail upload task failed");
                }
            }
        }

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

        if internal_run {
            for result in &results {
                if let Err(err) = emit_v1_certified_event(&result.blob_store_result) {
                    tracing::warn!(%err, "failed to emit certification event");
                }

                match &result.blob_store_result {
                    sdk_responses::BlobStoreResult::NewlyCreated {
                        blob_object,
                        resource_operation,
                        cost,
                        shared_blob_object,
                    } => {
                        let operation_note = match resource_operation {
                            RegisterBlobOp::RegisterFromScratch { .. } => {
                                "(storage was purchased, and a new blob object was registered)"
                                    .to_string()
                            }
                            RegisterBlobOp::ReuseStorage { .. } => concat!(
                                "(already-owned storage was reused, and a ",
                                "new blob object was registered)"
                            )
                            .to_string(),
                            RegisterBlobOp::ReuseAndExtend { .. } => {
                                "(previous storage was extended)".to_string()
                            }
                            RegisterBlobOp::ReuseAndExtendNonCertified { .. } => {
                                "(non-certified storage was extended)".to_string()
                            }
                            RegisterBlobOp::ReuseRegistration { .. } => {
                                "(existing registration reused)".to_string()
                            }
                        };

                        let event = ChildUploaderEvent::StoreDetailNewlyCreated {
                            path: result.path.display().to_string(),
                            blob_id: blob_object.blob_id.to_string(),
                            object_id: blob_object.id.to_string(),
                            deletable: blob_object.deletable,
                            unencoded_size: blob_object.size,
                            encoded_size: resource_operation.encoded_length(),
                            cost: *cost,
                            end_epoch: u64::from(blob_object.storage.end_epoch),
                            shared_blob_object_id: shared_blob_object
                                .as_ref()
                                .map(|id| id.to_string()),
                            encoding_type: blob_object.encoding_type.to_string(),
                            operation_note,
                        };
                        if let Err(err) = emit_child_event(&event) {
                            tracing::warn!(%err, "failed to emit store detail (new)");
                        }
                    }
                    sdk_responses::BlobStoreResult::AlreadyCertified {
                        blob_id,
                        event_or_object,
                        end_epoch,
                    } => {
                        let event = ChildUploaderEvent::StoreDetailAlreadyCertified {
                            path: result.path.display().to_string(),
                            blob_id: blob_id.to_string(),
                            end_epoch: u64::from(*end_epoch),
                            event_or_object: event_or_object.to_string(),
                        };
                        if let Err(err) = emit_child_event(&event) {
                            tracing::warn!(%err, "failed to emit store detail (already)");
                        }
                    }
                    _ => {}
                }
            }

            let mut total_encoded_size: u64 = 0;
            let mut total_cost: u64 = 0;
            let mut reuse_and_extend_count: u64 = 0;
            let mut newly_certified: u64 = 0;
            for res in &results {
                if let sdk_responses::BlobStoreResult::NewlyCreated {
                    resource_operation,
                    cost,
                    ..
                } = &res.blob_store_result
                {
                    total_encoded_size += resource_operation.encoded_length();
                    total_cost += *cost;
                    match resource_operation {
                        RegisterBlobOp::ReuseAndExtend { .. } => reuse_and_extend_count += 1,
                        RegisterBlobOp::RegisterFromScratch { .. }
                        | RegisterBlobOp::ReuseAndExtendNonCertified { .. }
                        | RegisterBlobOp::ReuseStorage { .. }
                        | RegisterBlobOp::ReuseRegistration { .. } => newly_certified += 1,
                    }
                }
            }

            if let Err(err) = emit_child_event(&ChildUploaderEvent::Done {
                ok: true,
                error: None,
                newly_certified,
                reuse_and_extend_count,
                total_encoded_size,
                total_cost,
            }) {
                tracing::warn!(%err, "failed to emit completion event");
            }
            internal_run_ctx.await_tail_handles().await;
        } else {
            tracing::info!(
                duration = ?start_timer.elapsed(),
                "finished storing blobs ({}/{})",
                results.len(),
                blobs_len
            );
        }

        results.print_output(self.json)
    }

    #[tracing::instrument(skip_all)]
    async fn store_dry_run(
        client: WalrusNodeClient<impl ReadClient>,
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
            let metadata = encoding_config
                .get_for_type(encoding_type)
                .compute_metadata(&blob)?;
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

    // TODO(WAL-1098): Refactor this function and reduce duplication with `store`.
    async fn store_quilt(
        self,
        paths: Vec<PathBuf>,
        blobs: Vec<QuiltBlobInput>,
        StoreOptions {
            epoch_arg,
            dry_run,
            store_optimizations,
            persistence,
            post_store,
            encoding_type,
            upload_relay,
            confirmation,
            child_process_uploads,
            internal_run,
        }: StoreOptions,
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
        let config = self.config?;
        let child_process_uploads = child_process_uploads
            .unwrap_or_else(|| default_child_process_uploads(&config))
            && !internal_run;
        let wallet = self.wallet?;
        let active_address = wallet.active_address();
        let mut client_created_in_bg = WalrusNodeClientCreatedInBackground::new(
            get_contract_client(config.clone(), wallet, self.gas_budget),
            config.contract_config.n_shards,
        );

        let epochs_ahead = get_epochs_ahead(
            &epoch_arg,
            config.contract_config.max_epochs_ahead,
            &mut client_created_in_bg,
        )
        .await?;

        let path_args_for_child = if paths.is_empty() {
            None
        } else {
            Some(paths.clone())
        };
        let blob_cli_args = if blobs.is_empty() {
            None
        } else {
            Some(
                blobs
                    .iter()
                    .map(quilt_blob_input_to_cli_arg)
                    .collect::<Result<Vec<_>>>()?,
            )
        };

        let quilt_store_blobs = Self::load_blobs_for_quilt(&paths, blobs)?;

        if dry_run {
            return Self::store_quilt_dry_run(
                client_created_in_bg.into_client().await?,
                &quilt_store_blobs,
                encoding_type,
                epochs_ahead,
                self.json,
            )
            .await;
        }

        if let Some(()) = maybe_spawn_child_upload_process(
            &config,
            child_process_uploads,
            &epoch_arg,
            dry_run,
            &store_optimizations,
            persistence,
            post_store,
            upload_relay.as_ref(),
            internal_run,
            "store-quilt",
            move |cmd| {
                if let Some(path_args) = path_args_for_child.as_ref() {
                    cmd.arg("--paths");
                    for path in path_args {
                        cmd.arg(path);
                    }
                } else if let Some(blob_args) = blob_cli_args.as_ref() {
                    cmd.arg("--blobs");
                    for arg in blob_args {
                        cmd.arg(arg);
                    }
                }
            },
            1,
        )
        .await?
        {
            return Ok(());
        }

        let start_timer = std::time::Instant::now();
        let quilt_write_client =
            QuiltClient::new(&client_created_in_bg, config.quilt_client_config.clone());
        let quilt = quilt_write_client
            .construct_quilt::<QuiltVersionV1>(&quilt_store_blobs, encoding_type)
            .await?;
        let base_store_args = StoreArgs::new(
            encoding_type,
            epochs_ahead,
            store_optimizations,
            persistence,
            post_store,
        );

        let mut internal_run_ctx = InternalRunContext::new(internal_run, &config, 1);
        let mut store_args = internal_run_ctx.configure_store_args(internal_run, base_store_args);
        let mut tail_handle_collector: Option<Arc<TokioMutex<Vec<tokio::task::JoinHandle<()>>>>> =
            None;
        if !internal_run
            && matches!(
                config.communication_config.tail_handling,
                TailHandling::Detached
            )
        {
            if !child_process_uploads {
                tracing::warn!(
                    "tail uploads will run in the current process; \
                    rerun with --child-process-uploads to offload them"
                );
            }
            let collector = Arc::new(TokioMutex::new(Vec::new()));
            store_args = store_args
                .with_tail_handling(TailHandling::Detached)
                .with_tail_handle_collector(collector.clone());
            tail_handle_collector = Some(collector);
        }

        if let Some(upload_relay) = upload_relay {
            let n_shards = client_created_in_bg.encoding_config().await?.n_shards();
            let upload_relay_client = UploadRelayClient::new(
                active_address,
                n_shards,
                upload_relay,
                self.gas_budget,
                config.backoff_config().clone(),
            )
            .await?;
            // Store operations will use the upload relay.
            store_args = store_args.with_upload_relay_client(upload_relay_client);

            let total_tip = store_args.compute_total_tip_amount(
                n_shards,
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
            .reserve_and_store_quilt::<QuiltVersionV1>(quilt, &store_args)
            .await?;

        internal_run_ctx.finalize_after_store(&mut store_args).await;

        if let Some(collector) = tail_handle_collector {
            let mut handles_guard = collector.lock().await;
            let mut handles = Vec::new();
            std::mem::swap(&mut *handles_guard, &mut handles);
            drop(handles_guard);
            for handle in handles {
                if let Err(err) = handle.await {
                    tracing::warn!(?err, "tail upload task failed");
                }
            }
        }

        if !internal_run {
            tracing::info!(
                duration = ?start_timer.elapsed(),
                "{} blobs stored in quilt",
                result.stored_quilt_blobs.len(),
            );
        }

        if internal_run {
            if let Err(err) = emit_v1_certified_event(&result.blob_store_result) {
                tracing::warn!(%err, "failed to emit certification event");
            }

            match &result.blob_store_result {
                sdk_responses::BlobStoreResult::NewlyCreated {
                    blob_object,
                    resource_operation,
                    cost,
                    shared_blob_object,
                } => {
                    let operation_note = match resource_operation {
                        RegisterBlobOp::RegisterFromScratch { .. } => {
                            "(storage was purchased, and a new blob object was registered)"
                                .to_string()
                        }
                        RegisterBlobOp::ReuseStorage { .. } => concat!(
                            "(already-owned storage was reused, and a new blob ",
                            "object was registered)"
                        )
                        .to_string(),
                        RegisterBlobOp::ReuseAndExtend { .. } => {
                            "(the blob was extended in lifetime)".to_string()
                        }
                        RegisterBlobOp::ReuseAndExtendNonCertified { .. } => {
                            "(non-certified storage was extended)".to_string()
                        }
                        RegisterBlobOp::ReuseRegistration { .. } => {
                            "(existing registration reused)".to_string()
                        }
                    };

                    let event = ChildUploaderEvent::StoreDetailNewlyCreated {
                        path: "<quilt>".to_string(),
                        blob_id: blob_object.blob_id.to_string(),
                        object_id: blob_object.id.to_string(),
                        deletable: blob_object.deletable,
                        unencoded_size: blob_object.size,
                        encoded_size: resource_operation.encoded_length(),
                        cost: *cost,
                        end_epoch: u64::from(blob_object.storage.end_epoch),
                        shared_blob_object_id: shared_blob_object.as_ref().map(|id| id.to_string()),
                        encoding_type: blob_object.encoding_type.to_string(),
                        operation_note,
                    };
                    if let Err(err) = emit_child_event(&event) {
                        tracing::warn!(%err, "failed to emit store detail (new)");
                    }
                }
                sdk_responses::BlobStoreResult::AlreadyCertified {
                    blob_id,
                    event_or_object,
                    end_epoch,
                } => {
                    let event = ChildUploaderEvent::StoreDetailAlreadyCertified {
                        path: "<quilt>".to_string(),
                        blob_id: blob_id.to_string(),
                        end_epoch: u64::from(*end_epoch),
                        event_or_object: event_or_object.to_string(),
                    };
                    if let Err(err) = emit_child_event(&event) {
                        tracing::warn!(%err, "failed to emit store detail (already)");
                    }
                }
                _ => {}
            }

            let (total_encoded_size, total_cost, newly_certified, reuse_and_extend_count) =
                match &result.blob_store_result {
                    sdk_responses::BlobStoreResult::NewlyCreated {
                        resource_operation,
                        cost,
                        ..
                    } => {
                        let encoded = resource_operation.encoded_length();
                        let cost = *cost;
                        let (newly_certified, reuse_and_extend_count) = match resource_operation {
                            RegisterBlobOp::ReuseAndExtend { .. } => (0, 1),
                            RegisterBlobOp::RegisterFromScratch { .. }
                            | RegisterBlobOp::ReuseAndExtendNonCertified { .. }
                            | RegisterBlobOp::ReuseStorage { .. }
                            | RegisterBlobOp::ReuseRegistration { .. } => (1, 0),
                        };
                        (encoded, cost, newly_certified, reuse_and_extend_count)
                    }
                    _ => (0, 0, 0, 0),
                };

            if let Err(err) = emit_child_event(&ChildUploaderEvent::Done {
                ok: true,
                error: None,
                newly_certified,
                reuse_and_extend_count,
                total_encoded_size,
                total_cost,
            }) {
                tracing::warn!(%err, "failed to emit completion event");
            }

            internal_run_ctx.await_tail_handles().await;
        }

        result.print_output(self.json)
    }

    fn load_blobs_for_quilt(
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

    /// Writes a quilt to a local file instead of uploading to Walrus.
    async fn store_quilt_to_file(
        self,
        paths: Vec<PathBuf>,
        blobs: Vec<QuiltBlobInput>,
        file: PathBuf,
        n_shards: Option<NonZeroU16>,
        encoding_type: Option<EncodingType>,
        rpc_url: Option<String>,
    ) -> Result<()> {
        // Get n_shards either from the argument or from the chain
        let n_shards = match n_shards {
            Some(n) => n,
            None => {
                let config = match self.config.as_ref() {
                    Ok(config) => config,
                    Err(e) => bail!("failed to load client config: {e}"),
                };
                let sui_read_client =
                    get_sui_read_client_from_rpc_node_or_wallet(config, rpc_url, self.wallet)
                        .await?;
                tracing::debug!("reading `n_shards` from chain");
                sui_read_client.current_committee().await?.n_shards()
            }
        };

        // Load blobs from paths/blob inputs
        let quilt_store_blobs = Self::load_blobs_for_quilt(&paths, blobs)?;

        // Create encoding config and construct quilt
        let encoding_type = encoding_type.unwrap_or(DEFAULT_ENCODING);
        let encoding_config = EncodingConfig::new(n_shards).get_for_type(encoding_type);

        // Construct the quilt using the encoder directly
        let encoder = <QuiltVersionV1 as QuiltVersion>::QuiltConfig::get_encoder(
            encoding_config,
            &quilt_store_blobs,
        );
        let quilt = encoder.construct_quilt()?;

        // Compute metadata to get blob ID
        let encoding_config = EncodingConfig::new(n_shards).get_for_type(encoding_type);
        let metadata = encoding_config.compute_metadata(quilt.data())?;
        let blob_id = *metadata.blob_id();
        let unencoded_size = metadata.metadata().unencoded_length();
        let quilt_index = quilt.quilt_index()?.clone();

        // Write quilt data to file
        std::fs::write(&file, quilt.data())
            .with_context(|| format!("failed to write quilt to file: {}", file.display()))?;

        tracing::info!(
            %blob_id,
            unencoded_size,
            "quilt written to file: {}",
            file.display()
        );

        let output = StoreQuiltToFileOutput {
            file,
            blob_id,
            unencoded_size,
            quilt_index: QuiltIndex::V1(quilt_index),
        };

        output.print_output(self.json)
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
        let metadata = client
            .encoding_config()
            .get_for_type(encoding_type)
            .compute_metadata(quilt.data())?;
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
            .build_refresher_and_run(sui_read_client.clone())
            .await?;
        let client = WalrusNodeClient::new(config, refresher_handle)?;

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

            let estimated_start_of_current_epoch = epoch_state.earliest_start_of_current_epoch();

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
            &aggregator_args,
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
        let client = match get_contract_client(self.config?, self.wallet?, self.gas_budget).await {
            Ok(client) => client,
            Err(e) => {
                if !self.json {
                    eprintln!("Error connecting to client: {e}");
                }
                anyhow::bail!(e);
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
        let client = get_contract_client(self.config?, self.wallet?, self.gas_budget).await?;
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
        let client = get_contract_client(config, self.wallet?, self.gas_budget).await?;
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

    fn write_blobs_dedup(blobs: &mut [QuiltStoreBlob<'static>], out_dir: &Path) -> Result<()> {
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

struct StoreOptions {
    epoch_arg: EpochArg,
    dry_run: bool,
    store_optimizations: StoreOptimizations,
    persistence: BlobPersistence,
    post_store: PostStoreAction,
    encoding_type: Option<EncodingType>,
    upload_relay: Option<Url>,
    confirmation: UserConfirmation,
    child_process_uploads: Option<bool>,
    internal_run: bool,
}

impl TryFrom<CommonStoreOptions> for StoreOptions {
    type Error = anyhow::Error;

    fn try_from(
        CommonStoreOptions {
            epoch_arg,
            dry_run,
            force,
            ignore_resources,
            deletable,
            permanent,
            share,
            encoding_type,
            upload_relay,
            skip_tip_confirmation,
            child_process_uploads,
            internal_run,
        }: CommonStoreOptions,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            epoch_arg,
            dry_run,
            store_optimizations: StoreOptimizations::from_force_and_ignore_resources_flags(
                force,
                ignore_resources,
            ),
            persistence: BlobPersistence::from_deletable_and_permanent(deletable, permanent)?,
            post_store: PostStoreAction::from_share(share),
            encoding_type,
            upload_relay,
            confirmation: skip_tip_confirmation.into(),
            child_process_uploads,
            internal_run,
        })
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
async fn get_epochs_ahead<C>(
    epoch_arg: &EpochArg,
    max_epochs_ahead: Option<EpochCount>,
    client_created_in_bg: &mut WalrusNodeClientCreatedInBackground<C>,
) -> anyhow::Result<EpochCount>
where
    C: std::fmt::Debug + ReadClient + 'static,
{
    let max_epochs_ahead = if let Some(max_epochs_ahead) = max_epochs_ahead {
        max_epochs_ahead
    } else {
        client_created_in_bg
            .client()
            .await?
            .sui_client()
            .read_client()
            .get_system_object()
            .await?
            .max_epochs_ahead()
    };
    let epochs_ahead = match epoch_arg {
        EpochArg {
            epochs: Some(epochs),
            ..
        } => epochs.clone().try_into_epoch_count(max_epochs_ahead)?,
        EpochArg {
            earliest_expiry_time: Some(earliest_expiry_time),
            ..
        } => {
            let staking_object = client_created_in_bg
                .client()
                .await?
                .sui_client()
                .read_client()
                .get_staking_object()
                .await?;
            let epoch_state = staking_object.epoch_state();
            let estimated_start_of_current_epoch = match epoch_state {
                EpochState::EpochChangeDone(epoch_start)
                | EpochState::NextParamsSelected(epoch_start) => *epoch_start,
                EpochState::EpochChangeSync(_) => Utc::now(),
            };
            let earliest_expiry_ts: DateTime<Utc> = (*earliest_expiry_time).into();
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
            let current_epoch = client_created_in_bg
                .client()
                .await?
                .sui_client()
                .current_epoch()
                .await?;
            ensure!(
                *end_epoch > current_epoch,
                "end_epoch must be greater than the current epoch"
            );
            *end_epoch - current_epoch
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
        wallet.get_rpc_url().to_string()
    } else {
        println!("Failed to get full node RPC URL.");
        return None;
    };

    // Now url is a String, not an Option<String>
    let rpc_client_result = rpc_client::create_sui_rpc_client(&url);
    if let Ok(mut rpc_client) = rpc_client_result {
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

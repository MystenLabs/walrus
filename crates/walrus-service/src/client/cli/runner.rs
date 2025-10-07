// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Helper struct to run the Walrus client binary commands.

use std::{
    collections::HashMap,
    env,
    fs as stdfs,
    io::Write,
    iter,
    num::NonZeroU16,
    path::{Path, PathBuf},
    process::Stdio,
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use fastcrypto::encoding::Encoding;
use indicatif::{MultiProgress, ProgressBar};
use itertools::Itertools as _;
use rand::seq::SliceRandom;
use reqwest::Url;
use sui_config::{SUI_CLIENT_CONFIG, sui_config_dir};
use sui_types::base_types::ObjectID;
use tokio::{io::AsyncBufReadExt, process::Command as TokioCommand};
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
        responses as sdk_responses,
        upload_relay_client::UploadRelayClient,
    },
    config::{UploadMode, UploadPreset, load_configuration},
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
    uploader::{TailHandling, UploaderEvent},
    utils::{styled_progress_bar_with_disabled_steady_tick, styled_spinner},
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
            HumanReadableBytes,
            HumanReadableFrost,
            HumanReadableMist,
            QuiltBlobInput,
            QuiltPatchByIdentifier,
            QuiltPatchByPatchId,
            QuiltPatchByTag,
            QuiltPatchSelector,
            WalrusColors,
            args::{CommonStoreOptions, TraceExporter},
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
// Colorize is imported locally where needed to avoid unused warning.

fn apply_upload_mode_to_config(
    mut config: walrus_sdk::config::ClientConfig,
    upload_mode: UploadMode,
) -> walrus_sdk::config::ClientConfig {
    // Use UploadPreset to apply tuning to the communication config
    let preset = match upload_mode {
        UploadMode::Conservative => UploadPreset::Conservative,
        UploadMode::Balanced => UploadPreset::Balanced,
        UploadMode::Aggressive => UploadPreset::Aggressive,
    };
    config.communication_config = preset.apply_to(config.communication_config.clone());
    config
}

/// Deserializable representation of JSON events from the walrus-uploader-child stdout.
#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ChildUploaderEvent {
    SliverProgress {
        blob_id: String,
        completed_weight: u64,
        total_weight: u64,
    },
    QuorumReached {
        blob_id: String,
        #[serde(default)]
        #[allow(dead_code)]
        elapsed_ms: u64,
        extra_ms: u64,
    },
    V1Certified {
        blob_id: String,
        object_id: String,
        #[serde(default)]
        end_epoch: Option<u64>,
        #[serde(default)]
        shared_object_id: Option<String>,
    },
    StoreDetailNewlyCreated {
        path: String,
        blob_id: String,
        object_id: String,
        deletable: bool,
        unencoded_size: u64,
        encoded_size: u64,
        cost: u64,
        end_epoch: u64,
        #[serde(default)]
        shared_blob_object_id: Option<String>,
        encoding_type: String,
        operation_note: String,
    },
    StoreDetailAlreadyCertified {
        path: String,
        blob_id: String,
        end_epoch: u64,
        event_or_object: String,
    },
    Done {
        #[allow(dead_code)]
        ok: bool,
        #[allow(dead_code)]
        error: Option<String>,
        #[serde(default)]
        newly_certified: u64,
        #[serde(default)]
        reuse_and_extend_count: u64,
        #[serde(default)]
        total_encoded_size: u64,
        #[serde(default)]
        total_cost: u64,
    },
}

// Spawn a background task to forward child's stderr to parent's stderr line-by-line.
fn spawn_child_stderr_to_stderr(stderr: tokio::process::ChildStderr) {
    tokio::spawn(async move {
        let reader = tokio::io::BufReader::new(stderr);
        let mut lines = reader.lines();
        while let Ok(Some(line)) = lines.next_line().await {
            eprintln!("[child] {line}");
            tracing::debug!(target = "walrus.child", line = %line, "child stderr");
        }
    });
}

// Process child's stdout JSON event stream; invoke `on_quorum` when quorum is reached, then return.
async fn process_child_stdout_events<F, Fut>(
    stdout: tokio::process::ChildStdout,
    on_quorum: F,
    expected_blobs: usize,
    progress_bar: Option<ProgressBar>,
) where
    F: Fn(BlobId, std::time::Duration) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    process_child_stdout_reader(
        tokio::io::BufReader::new(stdout),
        on_quorum,
        expected_blobs,
        progress_bar,
    )
    .await;
}

async fn process_child_stdout_reader<R, F, Fut>(
    reader: R,
    on_quorum: F,
    expected_blobs: usize,
    mut progress_bar: Option<ProgressBar>,
) where
    R: tokio::io::AsyncBufRead + Unpin,
    F: Fn(BlobId, std::time::Duration) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let mut lines = reader.lines();
    let mut certified_count = 0;
    let multi = MultiProgress::new();
    let mut per_blob_bars: HashMap<String, ProgressBar> = HashMap::new();

    while let Ok(Some(line)) = lines.next_line().await {
        match serde_json::from_str::<ChildUploaderEvent>(&line) {
            Ok(event) => match event {
                ChildUploaderEvent::SliverProgress {
                    blob_id,
                    completed_weight,
                    total_weight,
                } => {
                    tracing::debug!(
                        completed_weight,
                        total_weight,
                        blob_id,
                        "child: sliver progress"
                    );
                    let bar = per_blob_bars.entry(blob_id.clone()).or_insert_with(|| {
                        let pb = styled_progress_bar_with_disabled_steady_tick(total_weight);
                        pb.disable_steady_tick();
                        multi.add(pb)
                    });
                    bar.set_length(total_weight);
                    bar.set_position(std::cmp::min(completed_weight, total_weight));
                }
                ChildUploaderEvent::QuorumReached {
                    blob_id,
                    extra_ms,
                    elapsed_ms,
                } => {
                    tracing::debug!(elapsed_ms, "child: quorum elapsed");
                    if let Some(pb) = per_blob_bars.get(&blob_id) {
                        pb.finish_with_message(format!("slivers sent blob ({})", blob_id));
                    }
                    if let Ok(blob_id) = BlobId::from_str(&blob_id) {
                        tracing::info!(
                            %blob_id, extra_ms, "child: quorum reached; sending deferral notice");
                        on_quorum(blob_id, std::time::Duration::from_millis(extra_ms)).await;
                    } else {
                        tracing::warn!(blob_id, "child: failed to parse blob_id");
                    }
                }
                ChildUploaderEvent::V1Certified {
                    blob_id,
                    object_id,
                    end_epoch,
                    shared_object_id,
                } => {
                    tracing::info!(
                        blob_id,
                        object_id,
                        ?end_epoch,
                        ?shared_object_id,
                        "certified blob on Sui"
                    );
                    certified_count += 1;
                    if certified_count >= expected_blobs {
                        tracing::info!(
                            certified_count,
                            expected_blobs,
                            "child: all blobs certified; parent can exit"
                        );
                    }
                }
                ChildUploaderEvent::StoreDetailNewlyCreated {
                    path,
                    blob_id,
                    object_id,
                    deletable,
                    unencoded_size,
                    encoded_size,
                    cost,
                    end_epoch,
                    shared_blob_object_id,
                    encoding_type,
                    operation_note,
                } => {
                    println!(
                        "{} {} blob stored successfully.
                        \nPath: {}
                        \nBlob ID: {}
                        \nSui object ID: {}
                        \nUnencoded size: {}
                        \nEncoded size (including replicated metadata): {}
                        \nCost (excluding gas): {} {}
                        \nExpiry epoch (exclusive): {}{}
                        \nEncoding type: {}",
                        crate::client::cli::success(),
                        if deletable { "Deletable" } else { "Permanent" },
                        path,
                        blob_id,
                        object_id,
                        HumanReadableBytes(unencoded_size),
                        HumanReadableBytes(encoded_size),
                        HumanReadableFrost::from(cost),
                        operation_note,
                        end_epoch,
                        shared_blob_object_id.map_or_else(String::new, |id| format!(
                            "\nShared blob object ID: {id}"
                        )),
                        encoding_type,
                    );
                }
                ChildUploaderEvent::StoreDetailAlreadyCertified {
                    path,
                    blob_id,
                    end_epoch,
                    event_or_object,
                } => {
                    println!(
                        "{} Blob was already available and certified within Walrus,
                        for a sufficient number of epochs.
                        \nPath: {}
                        \nBlob ID: {}
                        \n{}
                        \nExpiry epoch (exclusive): {}
                        \n",
                        crate::client::cli::success(),
                        path,
                        blob_id,
                        event_or_object,
                        end_epoch,
                    );
                }
                ChildUploaderEvent::Done {
                    ok,
                    error,
                    newly_certified,
                    reuse_and_extend_count,
                    total_encoded_size,
                    total_cost,
                } => {
                    tracing::debug!(ok, ?error, "child: done");
                    if !ok {
                        tracing::error!(?error, "child process finished with an error");
                        if let Some(pb) = progress_bar.take() {
                            pb.finish_with_message("Child upload encountered an error");
                        }
                        break;
                    }
                    // Child reported successful completion; clear progress bar
                    // and print summary similar to CLI output.
                    if let Some(pb) = progress_bar.take() {
                        pb.finish_and_clear();
                    }

                    if newly_certified > 0 || reuse_and_extend_count > 0 {
                        let mut parts = Vec::new();
                        if newly_certified > 0 {
                            parts.push(format!("{} newly certified", newly_certified));
                        }
                        if reuse_and_extend_count > 0 {
                            parts.push(format!("{} extended", reuse_and_extend_count));
                        }

                        use colored::Colorize as _;
                        println!(
                            "{} ({})",
                            "Summary for Modified or Created Blobs"
                                .bold()
                                .walrus_purple(),
                            parts.join(", ")
                        );
                        println!(
                            "Total encoded size: {}",
                            HumanReadableBytes(total_encoded_size)
                        );
                        println!("Total cost: {}", HumanReadableFrost::from(total_cost));
                    } else {
                        use colored::Colorize as _;
                        println!(
                            "{}",
                            "No blobs were modified or created".bold().walrus_purple()
                        );
                    }

                    return;
                }
            },
            Err(e) => {
                tracing::debug!(%e, line = line, "child: failed to parse JSON line");
            }
        }
    }

    for (_, pb) in per_blob_bars.into_iter() {
        pb.finish_and_clear();
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        io::Cursor,
        str::FromStr,
        sync::{Arc, Mutex},
    };

    use tokio::io::BufReader;

    use super::*;

    #[tokio::test]
    async fn process_child_events_handles_progress_quorum_and_done() -> Result<()> {
        let blob_id = "4BKcDC0Ih5RJ8R0tFMz3MZVNZV8b2goT6_JiEEwNHQo";
        let started = serde_json::json!({
            "type": "started",
            "blob_id": blob_id,
        });
        let progress = serde_json::json!({
            "type": "sliver_progress",
            "blob_id": blob_id,
            "completed_weight": 2,
            "total_weight": 6,
        });
        let quorum = serde_json::json!({
            "type": "quorum_reached",
            "blob_id": blob_id,
            "elapsed_ms": 5,
            "extra_ms": 10,
        });
        let cert = serde_json::json!({
            "type": "v1_certified",
            "blob_id": blob_id,
            "object_id": "0x1234",
            "end_epoch": 123,
            "shared_object_id": null,
        });
        let done = serde_json::json!({
            "type": "done",
            "ok": true,
            "error": null,
        });
        let json = vec![started, progress, quorum, cert, done]
            .into_iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join("\n");

        let seen: Arc<Mutex<VecDeque<(BlobId, u64)>>> = Arc::new(Mutex::new(VecDeque::new()));
        let reader = BufReader::new(Cursor::new(json));

        process_child_stdout_reader(
            reader,
            {
                let seen = seen.clone();
                move |blob, extra| {
                    let seen = seen.clone();
                    async move {
                        seen.lock()
                            .unwrap()
                            .push_back((blob, extra.as_millis() as u64));
                    }
                }
            },
            1,
            None,
        )
        .await;

        let guard = seen.lock().unwrap();
        assert_eq!(guard.len(), 1);
        let (observed_blob, extra_ms) = guard.front().unwrap();
        assert_eq!(observed_blob, &BlobId::from_str(blob_id)?);
        assert_eq!(*extra_ms, 10);

        Ok(())
    }
}

fn emit_child_event(event: &ChildUploaderEvent) -> Result<()> {
    tracing::debug!(?event, "child: emitting event to stdout");
    println!("{}", serde_json::to_string(event)?);
    Ok(())
}

fn emit_v1_certified_event(result: &sdk_responses::BlobStoreResultWithPath) -> Result<()> {
    match &result.blob_store_result {
        sdk_responses::BlobStoreResult::NewlyCreated {
            blob_object,
            shared_blob_object,
            ..
        } => {
            tracing::debug!(blob_id = %blob_object.blob_id, "child: emitting V1Certified (new)");
            let event = ChildUploaderEvent::V1Certified {
                blob_id: blob_object.blob_id.to_string(),
                object_id: blob_object.id.to_string(),
                end_epoch: Some(u64::from(blob_object.storage.end_epoch)),
                shared_object_id: shared_blob_object.as_ref().map(|id| id.to_string()),
            };
            emit_child_event(&event)
        }
        sdk_responses::BlobStoreResult::AlreadyCertified {
            blob_id,
            event_or_object,
            end_epoch,
        } => {
            let object_id = match event_or_object {
                sdk_responses::EventOrObjectId::Object(id) => id.to_string(),
                other => other.to_string(),
            };
            tracing::debug!(blob_id = %blob_id, "child: emitting V1Certified (already)");
            let event = ChildUploaderEvent::V1Certified {
                blob_id: blob_id.to_string(),
                object_id,
                end_epoch: Some(u64::from(*end_epoch)),
                shared_object_id: None,
            };
            emit_child_event(&event)
        }
        _ => Ok(()),
    }
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
                strict_integrity_check: _,
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
            } => self.store(files, common_options.try_into()?).await,

            CliCommands::StoreQuilt {
                paths,
                blobs,
                common_options,
            } => {
                self.store_quilt(paths, blobs, common_options.try_into()?)
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
            upload_mode,
            internal_run,
        }: StoreOptions,
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
            get_epochs_ahead(epoch_arg.clone(), system_object.max_epochs_ahead(), &client).await?;

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

        let child_uploads_enabled = client.config().communication_config.child_uploads_enabled;

        if child_uploads_enabled && upload_relay.is_none() && !internal_run {
            tracing::info!("Spawning child process for uploads");

            let tmp_dir = env::temp_dir();
            let config_filename = format!(
                "walrus_child_config_{}_{}.yaml",
                std::process::id(),
                chrono::Utc::now().timestamp_millis()
            );
            let config_yaml_path = tmp_dir.join(config_filename);

            if let Err(e) = stdfs::write(
                &config_yaml_path,
                serde_yaml::to_string(client.config()).context("serialize ClientConfig")?,
            ) {
                tracing::warn!(
                    error = %e, "failed to write temp client config;
                    falling back to single-process mode");
            } else {
                let exe = env::current_exe().unwrap_or_else(|_| PathBuf::from("walrus"));
                let mut cmd = TokioCommand::new(exe);
                // Mark child process execution via environment for downstream consumers
                cmd.env("INTERNAL_RUN", "true");

                cmd.arg("store")
                    .arg("--internal-run")
                    .arg("--config")
                    .arg(&config_yaml_path);

                if let Some(ref epochs) = epoch_arg.epochs {
                    match epochs {
                        super::args::EpochCountOrMax::Max => {
                            cmd.arg("--epochs").arg("max");
                        }
                        super::args::EpochCountOrMax::Epochs(count) => {
                            cmd.arg("--epochs").arg(count.to_string());
                        }
                    }
                }
                if let Some(time) = &epoch_arg.earliest_expiry_time {
                    let datetime: DateTime<Utc> = (*time).into();
                    cmd.arg("--earliest-expiry-time").arg(datetime.to_rfc3339());
                }
                if let Some(end_epoch) = &epoch_arg.end_epoch {
                    cmd.arg("--end-epoch").arg(end_epoch.to_string());
                }
                if dry_run {
                    cmd.arg("--dry-run");
                }
                if !store_optimizations.check_status {
                    cmd.arg("--force");
                }
                if !store_optimizations.reuse_resources {
                    cmd.arg("--ignore-resources");
                }
                match persistence {
                    BlobPersistence::Deletable => {
                        cmd.arg("--deletable");
                    }
                    BlobPersistence::Permanent => {
                        cmd.arg("--permanent");
                    }
                }
                if post_store == PostStoreAction::Share {
                    cmd.arg("--share");
                }
                if let Some(mode) = upload_mode {
                    cmd.arg("--upload-mode").arg(match mode {
                        walrus_sdk::config::UploadMode::Conservative => "conservative",
                        walrus_sdk::config::UploadMode::Balanced => "balanced",
                        walrus_sdk::config::UploadMode::Aggressive => "aggressive",
                    });
                }

                for (path, _) in &blobs {
                    cmd.arg(path);
                }

                cmd.kill_on_drop(false);

                match cmd.stdout(Stdio::piped()).stderr(Stdio::piped()).spawn() {
                    Ok(mut child) => {
                        tracing::info!("Child process spawned successfully");

                        if let Some(stderr) = child.stderr.take() {
                            spawn_child_stderr_to_stderr(stderr);
                        }

                        if let Some(stdout) = child.stdout.take() {
                            let num_blobs = blobs.len();
                            let progress_bar = styled_progress_bar_with_disabled_steady_tick(
                                walrus_core::bft::min_n_correct(
                                    client.encoding_config().n_shards(),
                                )
                                .get()
                                .into(),
                            );
                            process_child_stdout_events(
                                stdout,
                                |blob_id, extra| async move {
                                    tracing::info!(%blob_id, extra_ms = extra.as_millis(),
                                    "child quorum reached; tail uploads continuing");
                                },
                                num_blobs,
                                Some(progress_bar.clone()),
                            )
                            .await;

                            tracing::info!(
                                "All blobs are now certified and the
                                parent process is exiting, the child continues tail uploads"
                            );
                            return Ok(());
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            "failed to spawn child process; falling back to single-process mode");
                        if let Err(e) = stdfs::remove_file(&config_yaml_path) {
                            tracing::warn!(error = %e, "failed to remove temp client config");
                        }
                    }
                }
            }
        }

        let mut store_args = StoreArgs::new(
            encoding_type,
            epochs_ahead,
            store_optimizations,
            persistence,
            post_store,
        );

        let mut tail_handle_collector: Option<
            Arc<tokio::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>>,
        > = None;
        let mut uploader_event_tx: Option<tokio::sync::mpsc::Sender<UploaderEvent>> = None;
        let mut event_task: Option<tokio::task::JoinHandle<()>> = None;

        if internal_run {
            use tokio::sync::{Mutex as TokioMutex, mpsc::channel as mpsc_channel};

            let collector = Arc::new(TokioMutex::new(Vec::new()));
            let (tx, rx) = mpsc_channel(blobs.len().max(1));

            let communication_config = client.config().communication_config.clone();
            event_task = Some(tokio::spawn(async move {
                let mut rx = rx;
                while let Some(event) = rx.recv().await {
                    match event {
                        UploaderEvent::BlobProgress {
                            blob_id,
                            completed_weight,
                            required_weight,
                        } => {
                            tracing::debug!(%blob_id, completed_weight, required_weight,
                                "child: forwarding progress event to parent");
                            if let Err(err) =
                                emit_child_event(&ChildUploaderEvent::SliverProgress {
                                    blob_id: blob_id.to_string(),
                                    completed_weight: completed_weight as u64,
                                    total_weight: required_weight as u64,
                                })
                            {
                                tracing::warn!(%err, "failed to emit progress event");
                            }
                        }
                        UploaderEvent::BlobQuorumReached { blob_id, elapsed } => {
                            let extra_duration = communication_config
                                .sliver_write_extra_time
                                .extra_time(elapsed);
                            let elapsed_ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);
                            let extra_ms =
                                u64::try_from(extra_duration.as_millis()).unwrap_or(u64::MAX);
                            tracing::debug!(%blob_id, elapsed_ms, extra_ms,
                                "child: forwarding quorum event to parent");
                            if let Err(err) = emit_child_event(&ChildUploaderEvent::QuorumReached {
                                blob_id: blob_id.to_string(),
                                elapsed_ms,
                                extra_ms,
                            }) {
                                tracing::warn!(%err, "failed to emit quorum event");
                            }
                        }
                    }
                }
                tracing::debug!("child: quorum forwarding task completed");
            }));

            uploader_event_tx = Some(tx.clone());
            tail_handle_collector = Some(collector.clone());

            store_args = store_args
                .with_tail_handle_collector(collector)
                .with_tail_handling(TailHandling::Detached)
                .with_quorum_event_tx(tx);
        }

        if let Some(upload_relay) = upload_relay {
            let upload_relay_client = UploadRelayClient::new(
                client.sui_client().address(),
                client.encoding_config().n_shards(),
                upload_relay,
                self.gas_budget,
                client.config().backoff_config().clone(),
            )
            .await?;
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

        if let Some(tx) = uploader_event_tx.take() {
            tracing::debug!("child: closing uploader event channel");
            drop(tx);
        }

        store_args.quorum_event_tx = None;
        if let Some(task) = event_task.take() {
            tracing::debug!("child: awaiting quorum forwarding task");
            if let Err(err) = task.await {
                tracing::warn!(?err, "uploader event task terminated with error");
            }
        }

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

        if !internal_run {
            tracing::info!(
                duration = ?start_timer.elapsed(),
                "{} out of {} blobs stored",
                results.len(),
                blobs_len
            );
        }

        if internal_run {
            for result in &results {
                if let Err(err) = emit_v1_certified_event(result) {
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
                            RegisterBlobOp::RegisterFromScratch { .. } => "(storage was purchased,
                                and a new blob object was registered)"
                                .to_string(),
                            RegisterBlobOp::ReuseStorage { .. } => {
                                "(already-owned storage was reused,
                                and a new blob object was registered)"
                                    .to_string()
                            }
                            RegisterBlobOp::ReuseRegistration { .. } => {
                                "(an existing registration was reused)".to_string()
                            }
                            RegisterBlobOp::ReuseAndExtend { .. } => {
                                "(the blob was extended in lifetime)".to_string()
                            }
                            RegisterBlobOp::ReuseAndExtendNonCertified { .. } => {
                                "(an existing registration was reused and extended)".to_string()
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
                        RegisterBlobOp::ReuseAndExtend { .. } => {
                            reuse_and_extend_count += 1;
                        }
                        RegisterBlobOp::RegisterFromScratch { .. }
                        | RegisterBlobOp::ReuseAndExtendNonCertified { .. }
                        | RegisterBlobOp::ReuseStorage { .. }
                        | RegisterBlobOp::ReuseRegistration { .. } => {
                            newly_certified += 1;
                        }
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

            if let Some(collector) = tail_handle_collector.as_ref() {
                let mut handles = collector.lock().await;
                while let Some(handle) = handles.pop() {
                    tracing::debug!("child: awaiting detached tail handle");
                    if let Err(err) = handle.await {
                        tracing::warn!(?err, "tail upload task failed");
                    }
                }
            }
        }
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
            upload_mode,
            internal_run: _,
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

struct StoreOptions {
    epoch_arg: EpochArg,
    dry_run: bool,
    store_optimizations: StoreOptimizations,
    persistence: BlobPersistence,
    post_store: PostStoreAction,
    encoding_type: Option<EncodingType>,
    upload_relay: Option<Url>,
    confirmation: UserConfirmation,
    upload_mode: Option<UploadMode>,
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
            upload_mode,
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
            upload_mode: upload_mode.map(Into::into),
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

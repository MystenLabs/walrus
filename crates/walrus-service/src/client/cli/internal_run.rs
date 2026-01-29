// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Internal-run related functionality for the Walrus client.

use std::{
    collections::HashMap,
    env,
    fs as stdfs,
    path::PathBuf,
    process::Stdio,
    str::FromStr,
    sync::Arc,
};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use colored::Colorize as _;
use indicatif::{MultiProgress, ProgressBar};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use serde_json;
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, BufReader},
    process::{Child as TokioChild, ChildStderr, ChildStdout, Command as TokioCommand},
    sync::{Mutex as TokioMutex, mpsc::channel as mpsc_channel, watch},
};
use walrus_core::BlobId;
use walrus_sdk::{
    client::{EncodingProgressEvent, StoreArgs, responses as sdk_responses},
    config::ClientConfig,
    store_optimizations::StoreOptimizations,
    sui::client::{BlobPersistence, PostStoreAction},
    uploader::{TailHandling, UploaderEvent},
    utils::{styled_progress_bar_with_disabled_steady_tick, styled_spinner},
};

use crate::client::cli::{
    HumanReadableBytes,
    HumanReadableFrost,
    WalrusColors,
    args::{EpochArg, QuiltBlobInput},
};

#[derive(Clone)]
struct ChildProcessHandle {
    pid: u32,
    inner: Arc<TokioMutex<Option<TokioChild>>>,
}

impl ChildProcessHandle {
    fn new(mut child: TokioChild) -> (Self, Option<ChildStdout>, Option<ChildStderr>) {
        let pid = child.id().expect("spawned child process must have a pid");
        let stdout = child.stdout.take();
        let stderr = child.stderr.take();
        let handle = Self {
            pid,
            inner: Arc::new(TokioMutex::new(Some(child))),
        };
        (handle, stdout, stderr)
    }

    fn pid(&self) -> u32 {
        self.pid
    }

    async fn take_child(&self) -> Option<TokioChild> {
        self.inner.lock().await.take()
    }
}

/// Deserializable representation of JSON events from the walrus-uploader-child stdout.
/// These events are emitted by the child process and sent to the parent process.
#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum ChildUploaderEvent {
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
    EncodingStarted {
        total: u64,
    },
    EncodingProgress {
        completed: u64,
        total: u64,
    },
    EncodingFinished,
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
    /// The final JSON output for the command that the child process is running.
    ///
    /// This is used to ensure `--json` output stays clean (only JSON on stdout) when the parent
    /// process is delegating work to a child process.
    CommandOutput {
        output: serde_json::Value,
    },
}

/// Emits an event to the stdout of the child process.
pub(crate) fn emit_child_event(event: &ChildUploaderEvent) -> Result<()> {
    tracing::debug!(?event, "child: emitting event to stdout");
    println!("{}", serde_json::to_string(event)?);
    Ok(())
}

/// Emits a V1Certified event to the stdout of the child process.
pub(crate) fn emit_v1_certified_event(result: &sdk_responses::BlobStoreResult) -> Result<()> {
    match result {
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

/// Converts a quilt blob input to a CLI argument.
pub(crate) fn quilt_blob_input_to_cli_arg(input: &QuiltBlobInput) -> Result<String> {
    let mut map = serde_json::Map::new();
    map.insert(
        "path".to_string(),
        serde_json::Value::String(input.path.to_string_lossy().into_owned()),
    );

    if let Some(identifier) = &input.identifier {
        map.insert(
            "identifier".to_string(),
            serde_json::Value::String(identifier.clone()),
        );
    }

    if !input.tags.is_empty() {
        map.insert(
            "tags".to_string(),
            serde_json::to_value(&input.tags).context("serialize quilt blob tags")?,
        );
    }

    Ok(serde_json::Value::Object(map).to_string())
}

/// Spawns a task to print the stderr of the child process to the stderr of the parent process.
fn spawn_child_stderr_to_stderr(stderr: ChildStderr) {
    tokio::spawn(async move {
        let reader = BufReader::new(stderr);
        let mut lines = reader.lines();
        while let Ok(Some(line)) = lines.next_line().await {
            eprintln!("[child] {line}");
            tracing::debug!(target = "walrus.child", line = %line, "child stderr");
        }
    });
}

/// Processes the stdout events of the child process.
pub(crate) async fn process_child_stdout_events<F, Fut>(
    stdout: ChildStdout,
    on_quorum: F,
    expected_blobs: usize,
    json_output: bool,
) -> Option<serde_json::Value>
where
    F: Fn(BlobId, std::time::Duration) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    process_child_stdout_reader(
        BufReader::new(stdout),
        on_quorum,
        expected_blobs,
        json_output,
    )
    .await
}

/// Processes the stdout events of the child process.
async fn process_child_stdout_reader<R, F, Fut>(
    reader: R,
    on_quorum: F,
    expected_blobs: usize,
    json_output: bool,
) -> Option<serde_json::Value>
where
    R: AsyncBufRead + Unpin,
    F: Fn(BlobId, std::time::Duration) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let mut lines = reader.lines();
    let mut certified_count = 0;
    let mut command_output: Option<serde_json::Value> = None;
    let multi = MultiProgress::new();
    let mut per_blob_bars: HashMap<String, ProgressBar> = HashMap::new();
    let mut encoding_spinner: Option<ProgressBar> = None;

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
                ChildUploaderEvent::EncodingStarted { total } => {
                    if total == 0 {
                        continue;
                    }
                    let spinner =
                        encoding_spinner.get_or_insert_with(|| multi.add(styled_spinner()));
                    spinner.set_message(format!("encoded 0/{total} blobs"));
                }
                ChildUploaderEvent::EncodingProgress { completed, total } => {
                    let spinner =
                        encoding_spinner.get_or_insert_with(|| multi.add(styled_spinner()));
                    spinner.set_message(format!("encoded {completed}/{total} blobs"));
                    if completed >= total && total > 0 {
                        spinner.finish_with_message("encoding complete");
                        encoding_spinner = None;
                    }
                }
                ChildUploaderEvent::EncodingFinished => {
                    if let Some(spinner) = encoding_spinner.take() {
                        spinner.finish_with_message("encoding complete");
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
                    if json_output {
                        continue;
                    }
                    println!(
                        "{} {} blob stored successfully.\n\
                        \nPath: {}\n\
                        Blob ID: {}\n\
                        Object ID: {}\n\
                        Deletable: {}\n\
                        Unencoded Size: {}\n\
                        Encoded Size: {}\n\
                        Cost: {} {}\n\
                        End Epoch: {}\n\
                        Shared Blob Object ID: {}\n\
                        Encoding Type: {}\n",
                        "Store Detail".bold().walrus_purple(),
                        if deletable {
                            "(deletable)"
                        } else {
                            "(permanent)"
                        },
                        path,
                        blob_id,
                        object_id,
                        deletable,
                        HumanReadableBytes(unencoded_size),
                        HumanReadableBytes(encoded_size),
                        HumanReadableFrost::from(cost),
                        operation_note,
                        end_epoch,
                        shared_blob_object_id.unwrap_or_else(|| "<none>".to_string()),
                        encoding_type,
                    );
                }
                ChildUploaderEvent::StoreDetailAlreadyCertified {
                    path,
                    blob_id,
                    end_epoch,
                    event_or_object,
                } => {
                    if json_output {
                        continue;
                    }
                    println!(
                        "{} Blob already certified.\n\
                        \nPath: {}\n\
                        Blob ID: {}\n\
                        End Epoch: {}\n\
                        Event/Object: {}",
                        "Store Detail".bold().walrus_purple(),
                        path,
                        blob_id,
                        end_epoch,
                        event_or_object,
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
                    tracing::debug!(?ok, ?error, "child: done event received");

                    if json_output {
                        return command_output;
                    }

                    if newly_certified > 0 || reuse_and_extend_count > 0 {
                        let mut parts = Vec::new();
                        if newly_certified > 0 {
                            parts.push(format!("{} newly certified", newly_certified));
                        }
                        if reuse_and_extend_count > 0 {
                            parts.push(format!("{} extended", reuse_and_extend_count));
                        }

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
                        println!(
                            "{}",
                            "No blobs were modified or created".bold().walrus_purple()
                        );
                    }

                    return command_output;
                }
                ChildUploaderEvent::CommandOutput { output } => {
                    command_output = Some(output);
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

    command_output
}

/// Applies the common store child arguments to the command.
/// This is used to configure the child process for the store command.
fn apply_common_store_child_args(
    cmd: &mut TokioCommand,
    epoch_arg: &EpochArg,
    dry_run: bool,
    store_optimizations: &StoreOptimizations,
    persistence: BlobPersistence,
    post_store: PostStoreAction,
) {
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
        BlobPersistence::Deletable => cmd.arg("--deletable"),
        BlobPersistence::Permanent => cmd.arg("--permanent"),
    };

    if post_store == PostStoreAction::Share {
        cmd.arg("--share");
    }
}

/// Spawns a task to forward signals to the child uploader process.
/// This is used to forward the signals from the parent process to the child process.
#[cfg(unix)]
fn spawn_signal_forwarders(handle: ChildProcessHandle, cancel_rx: watch::Receiver<bool>) {
    use tokio::signal::unix::{SignalKind, signal};

    let signals = [
        (SignalKind::terminate(), libc::SIGTERM, "SIGTERM"),
        (SignalKind::interrupt(), libc::SIGINT, "SIGINT"),
    ];

    for (kind, signo, name) in signals {
        let handle_clone = handle.clone();
        let mut cancel_rx = cancel_rx.clone();
        match signal(kind) {
            Ok(stream) => {
                let mut stream = stream;
                tokio::spawn(async move {
                    tokio::select! {
                        _ = stream.recv() => {
                            forward_unix_signal_to_child(handle_clone, signo, name);
                            // When not catching the signal, the process will exit with
                            // 128 + signo by default. But when catching the signal, we need
                            // to do it ourself.
                            let exit_code = 128 + signo;
                            std::process::exit(exit_code);
                        }
                        _ = cancel_rx.changed() => {
                        }
                    }
                });
            }
            Err(error) => {
                tracing::warn!(%error, signal = name,
                    "failed to register signal forwarder for child uploader process");
            }
        }
    }
}

/// Forwards a Unix signal to the child uploader process.
/// This is used to forward the Unix signals from the parent process to the child process.
#[cfg(unix)]
fn forward_unix_signal_to_child(
    handle: ChildProcessHandle,
    signo: libc::c_int,
    signal_name: &'static str,
) {
    let pid = i32::try_from(handle.pid()).expect("pid should be in range");
    let result = unsafe { libc::kill(pid, signo) };
    if result != 0 {
        let error = std::io::Error::last_os_error();
        tracing::warn!(pid, signal = signal_name, %error,
            "failed to forward signal to child uploader process");
    }
    tracing::debug!(
        pid,
        signal = signal_name,
        "forwarded signal to child uploader process"
    );
}

#[cfg(not(unix))]
fn spawn_signal_forwarders(handle: ChildProcessHandle, mut cancel_rx: watch::Receiver<bool>) {
    tokio::spawn(async move {
        tokio::select! {
            res = tokio::signal::ctrl_c() => {
                match res {
                    Ok(()) => {
                        tracing::debug!("forwarding ctrl_c to child uploader process");
                        if let Some(mut child) = handle.take_child().await {
                            if let Err(error) = child.start_kill() {
                                tracing::warn!(%error,
                                    "failed to terminate child uploader process after ctrl_c");
                            }
                        }
                        std::process::exit(130);
                    }
                    Err(error) => {
                        tracing::warn!(%error,
                            "failed to listen for ctrl_c to forward to child uploader process");
                    }
                }
            }
            _ = cancel_rx.changed() => {
                return;
            }
        }
    });
}

/// Spawns a child process to handle the upload of blobs.
/// This is used to handle the upload of blobs in a separate child process.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn maybe_spawn_child_upload_process<F>(
    client_config: &ClientConfig,
    child_process_uploads: bool,
    epoch_arg: &EpochArg,
    dry_run: bool,
    store_optimizations: &StoreOptimizations,
    persistence: BlobPersistence,
    post_store: PostStoreAction,
    upload_relay: Option<&Url>,
    internal_run: bool,
    json_output: bool,
    command_name: &str,
    add_command_args: F,
    num_blobs: usize,
) -> Result<Option<()>>
where
    F: FnOnce(&mut TokioCommand),
{
    let tail_handling = client_config.communication_config.tail_handling;
    if !(child_process_uploads
        && matches!(tail_handling, TailHandling::Detached)
        && upload_relay.is_none()
        && !internal_run)
    {
        return Ok(None);
    }

    tracing::info!("Spawning child process for uploads");

    let tmp_dir = env::temp_dir();
    let config_filename = format!(
        "walrus_child_config_{}_{}.yaml",
        std::process::id(),
        Utc::now().timestamp_millis()
    );
    let config_yaml_path = tmp_dir.join(config_filename);

    if let Err(e) = stdfs::write(
        &config_yaml_path,
        serde_yaml::to_string(client_config).context("serialize ClientConfig")?,
    ) {
        tracing::warn!(
            error = %e,
            "failed to write temp client config; falling back to single-process mode"
        );
        return Ok(None);
    }

    let exe = env::current_exe().unwrap_or_else(|_| PathBuf::from("walrus"));
    let mut cmd = TokioCommand::new(exe);
    cmd.env("INTERNAL_RUN", "true");
    cmd.arg(command_name)
        .arg("--internal-run")
        .arg("--config")
        .arg(&config_yaml_path);

    apply_common_store_child_args(
        &mut cmd,
        epoch_arg,
        dry_run,
        store_optimizations,
        persistence,
        post_store,
    );

    add_command_args(&mut cmd);
    cmd.kill_on_drop(false);

    match cmd.stdout(Stdio::piped()).stderr(Stdio::piped()).spawn() {
        Ok(child) => {
            tracing::info!("Child process spawned successfully");

            let (child_handle, child_stdout, child_stderr) = ChildProcessHandle::new(child);
            let (cancel_tx, cancel_rx) = watch::channel(false);
            spawn_signal_forwarders(child_handle.clone(), cancel_rx);
            tokio::spawn({
                let handle_for_wait = child_handle.clone();
                let cancel_tx = cancel_tx.clone();
                async move {
                    if let Some(mut child) = handle_for_wait.take_child().await
                        && let Err(error) = child.wait().await
                    {
                        tracing::warn!(%error,
                                "failed to wait for child process to exit");
                    }
                    if let Err(error) = cancel_tx.send(true) {
                        tracing::warn!(%error,
                            "failed to send cancellation signal to child process");
                    }
                }
            });

            if let Some(stderr) = child_stderr {
                spawn_child_stderr_to_stderr(stderr);
            }

            if let Some(stdout) = child_stdout {
                let output = process_child_stdout_events(
                    stdout,
                    |blob_id, extra| async move {
                        tracing::info!(%blob_id, extra_ms = extra.as_millis(),
                            "child quorum reached; tail uploads continuing");
                    },
                    num_blobs,
                    json_output,
                )
                .await;

                if json_output {
                    let output = output.context("child process did not emit command output")?;
                    serde_json::to_writer_pretty(std::io::stdout(), &output)
                        .context("write JSON output from child process")?;
                }

                tracing::info!(concat!(
                    "All blobs are now certified and the parent process is exiting, ",
                    "the child continues tail uploads"
                ));
                return Ok(Some(()));
            }
        }
        Err(e) => {
            tracing::warn!(
                error = %e,
                "failed to spawn child process; falling back to single-process mode"
            );
        }
    }

    if let Err(e) = stdfs::remove_file(&config_yaml_path) {
        tracing::warn!(error = %e, "failed to remove temp client config");
    }

    Ok(None)
}

/// A context for the internal run.
/// This is used to handle the upload of blobs in a separate child process.
#[derive(Default)]
pub(crate) struct InternalRunContext {
    /// A collector for the tail handles.
    /// This is used to collect the tail handles from the child process.
    tail_handle_collector: Option<Arc<TokioMutex<Vec<tokio::task::JoinHandle<()>>>>>,
    /// A sender for the uploader events.
    /// This is used to send the uploader events to the child process.
    uploader_event_tx: Option<tokio::sync::mpsc::Sender<UploaderEvent>>,
    /// A sender for encoding progress events.
    encoding_event_tx: Option<tokio::sync::mpsc::UnboundedSender<EncodingProgressEvent>>,
    /// A task for the event forwarding.
    /// This is used to forward the events from the child process to the parent process.
    event_task: Option<tokio::task::JoinHandle<()>>,
    /// A task for forwarding encoding events.
    encoding_event_task: Option<tokio::task::JoinHandle<()>>,
}

impl InternalRunContext {
    pub(crate) fn new(internal_run: bool, client_config: &ClientConfig, num_items: usize) -> Self {
        if !internal_run {
            return Self::default();
        }

        let collector = Arc::new(TokioMutex::new(Vec::new()));
        let (tx, rx) = mpsc_channel(num_items.max(1));
        let (encoding_tx, mut encoding_rx) = tokio::sync::mpsc::unbounded_channel();

        let communication_config = client_config.communication_config.clone();
        let event_task = tokio::spawn(async move {
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
                        if let Err(err) = emit_child_event(&ChildUploaderEvent::SliverProgress {
                            blob_id: blob_id.to_string(),
                            completed_weight: completed_weight as u64,
                            total_weight: required_weight as u64,
                        }) {
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
        });

        let encoding_event_task = tokio::spawn(async move {
            while let Some(event) = encoding_rx.recv().await {
                match event {
                    EncodingProgressEvent::Started { total } => {
                        if let Err(err) = emit_child_event(&ChildUploaderEvent::EncodingStarted {
                            total: total as u64,
                        }) {
                            tracing::warn!(%err, "failed to emit encoding start event");
                        }
                    }
                    EncodingProgressEvent::BlobCompleted { completed, total } => {
                        if let Err(err) = emit_child_event(&ChildUploaderEvent::EncodingProgress {
                            completed: completed as u64,
                            total: total as u64,
                        }) {
                            tracing::warn!(%err, "failed to emit encoding progress event");
                        }
                    }
                    EncodingProgressEvent::Finished => {
                        if let Err(err) = emit_child_event(&ChildUploaderEvent::EncodingFinished) {
                            tracing::warn!(%err, "failed to emit encoding finish event");
                        }
                    }
                }
            }
            tracing::debug!("child: encoding forwarding task completed");
        });

        InternalRunContext {
            tail_handle_collector: Some(collector),
            uploader_event_tx: Some(tx),
            encoding_event_tx: Some(encoding_tx),
            event_task: Some(event_task),
            encoding_event_task: Some(encoding_event_task),
        }
    }

    /// Configures the store arguments for the internal run.
    /// This is used to configure the store arguments for the internal run.
    pub(crate) fn configure_store_args(
        &self,
        internal_run: bool,
        mut store_args: StoreArgs,
    ) -> StoreArgs {
        if let Some(collector) = self.tail_handle_collector.as_ref() {
            store_args = store_args.with_tail_handle_collector(collector.clone());
        }
        if let Some(tx) = self.uploader_event_tx.as_ref() {
            store_args = store_args.with_quorum_event_tx(tx.clone());
        }
        if let Some(tx) = self.encoding_event_tx.as_ref() {
            store_args = store_args.with_encoding_event_tx(tx.clone());
        }
        if internal_run {
            store_args = store_args.with_tail_handling(TailHandling::Detached);
        }
        store_args
    }

    /// Finalizes the store arguments for the internal run.
    pub(crate) async fn finalize_after_store(&mut self, store_args: &mut StoreArgs) {
        if let Some(tx) = self.uploader_event_tx.take() {
            tracing::debug!("child: closing uploader event channel");
            drop(tx);
        }
        if let Some(tx) = self.encoding_event_tx.take() {
            tracing::debug!("child: closing encoding event channel");
            drop(tx);
        }

        store_args.quorum_event_tx = None;
        store_args.tail_handle_collector = None;
        store_args.encoding_event_tx = None;

        if let Some(task) = self.event_task.take() {
            tracing::debug!("child: awaiting quorum forwarding task");
            if let Err(err) = task.await {
                tracing::warn!(?err, "uploader event task terminated with error");
            }
        }
        if let Some(task) = self.encoding_event_task.take() {
            tracing::debug!("child: awaiting encoding forwarding task");
            if let Err(err) = task.await {
                tracing::warn!(?err, "encoding event task terminated with error");
            }
        }
    }

    /// Awaits the tail handles for the internal run.
    /// This is used to await the tail handles for the internal run.
    pub(crate) async fn await_tail_handles(&mut self) {
        if let Some(collector) = self.tail_handle_collector.take() {
            let mut handles = collector.lock().await;
            while let Some(handle) = handles.pop() {
                tracing::debug!("child: awaiting detached tail handle");
                if let Err(err) = handle.await {
                    tracing::warn!(?err, "tail upload task failed");
                }
            }
        }
    }
}

#[cfg(all(test, not(msim)))]
mod tests {
    use std::{
        collections::VecDeque,
        io::Cursor,
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
                        seen.lock().unwrap().push_back((
                            blob,
                            u64::try_from(extra.as_millis()).expect("extra is not a millisecond"),
                        ));
                    }
                }
            },
            1,
            false,
        )
        .await;

        let guard = seen.lock().unwrap();
        assert_eq!(guard.len(), 1);
        let (observed_blob, extra_ms) = guard.front().unwrap();
        assert_eq!(observed_blob, &BlobId::from_str(blob_id)?);
        assert_eq!(*extra_ms, 10);

        Ok(())
    }

    #[tokio::test]
    async fn process_child_events_returns_command_output_in_json_mode() -> Result<()> {
        let output = serde_json::json!([
            {
                "blobStoreResult": {
                    "alreadyCertified": {
                        "blobId": "XbN7UoXgqlvlfUNwQ1I-iR5T87tfjIBSZ0FL9MPgu2k",
                        "object": "0x23b47c2f56a8262d4692287f487d2f8b916623618d135e09fcf08a239123",
                        "endEpoch": 303
                    }
                },
                "path": "random_file.bin"
            }
        ]);
        let command_output = serde_json::json!({
            "type": "command_output",
            "output": output.clone(),
        });
        let done = serde_json::json!({
            "type": "done",
            "ok": true,
            "error": null,
        });

        let json = vec![command_output, done]
            .into_iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join("\n");

        let reader = BufReader::new(Cursor::new(json));
        let observed = process_child_stdout_reader(reader, |_blob, _extra| async {}, 1, true).await;

        assert_eq!(observed, Some(output));
        Ok(())
    }
}

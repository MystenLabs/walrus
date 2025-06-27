// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Utility functions for the Walrus service.

use std::{
    collections::{HashMap, HashSet},
    env,
    fmt::Debug,
    future::Future,
    mem,
    net::SocketAddr,
    path::Path,
    pin::Pin,
    str::FromStr,
    sync::Arc,
    task::{Context, Poll, ready},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context as _, Result, anyhow};
use fastcrypto::{
    encoding::Base64,
    secp256r1::Secp256r1KeyPair,
    traits::{EncodeDecodeBase64, RecoverableSigner},
};
use futures::future::FusedFuture;
use pin_project::pin_project;
use prometheus::{Encoder, HistogramVec};
use serde::{Deserialize, Deserializer, Serialize, de::Error};
use serde_json;
use sui_types::base_types::{ObjectID, SuiAddress};
use telemetry_subscribers::{TelemetryGuards, TracingHandle};
use tokio::{
    runtime::{self, Runtime},
    sync::oneshot,
    task::{JoinError, JoinHandle},
    time::Instant,
};
use tokio_util::sync::CancellationToken;
use tracing::subscriber::DefaultGuard;
use tracing_subscriber::{
    EnvFilter,
    Layer,
    filter::Filtered,
    layer::{Layered, SubscriberExt as _},
    util::SubscriberInitExt,
};
use typed_store::DBMetrics;
use uuid::Uuid;
use walrus_core::{PublicKey, ShardIndex};
use walrus_sdk::active_committees::ActiveCommittees;
use walrus_sui::{
    client::{contract_config::ContractConfig, retry_client::RetriableSuiClient},
    utils::SuiNetwork,
};
use walrus_utils::metrics::{Registry, monitored_scope};

use crate::node::config::MetricsPushConfig;

/// The maximum length of the storage node name. Keep in sync with `MAX_NODE_NAME_LENGTH` in
/// `contracts/walrus/sources/staking/staking_pool.move`.
pub const MAX_NODE_NAME_LENGTH: usize = 100;

/// Helper functions applied to futures.
pub(crate) trait FutureHelpers: Future {
    /// Reports metrics for the future.
    fn observe_future<F, const N: usize>(
        self,
        histograms: HistogramVec,
        get_labels: F,
    ) -> Observe<Self, F, N>
    where
        Self: Sized,
        F: FnOnce(Option<&<Self as Future>::Output>) -> [&'static str; N],
    {
        Observe::new(self, histograms, get_labels)
    }
}

impl<T: Future> FutureHelpers for T {}

/// Structure that can wrap a future to report metrics for that future.
#[pin_project(PinnedDrop)]
#[derive(Debug)]
pub(crate) struct Observe<Fut, F, const N: usize>
where
    Fut: Future,
    F: FnOnce(Option<&Fut::Output>) -> [&'static str; N],
{
    #[pin]
    inner: Fut,
    start: Instant,
    histograms: HistogramVec,
    // INV: Is Some until the future has completed.
    get_labels: Option<F>,
}

impl<Fut, F, const N: usize> Observe<Fut, F, N>
where
    Fut: Future,
    F: FnOnce(Option<&Fut::Output>) -> [&'static str; N],
{
    pub fn new(inner: Fut, histograms: HistogramVec, get_labels: F) -> Self {
        Self {
            inner,
            histograms,
            start: Instant::now(),
            get_labels: Some(get_labels),
        }
    }

    fn observe(self: Pin<&mut Self>, output: Option<&<Self as Future>::Output>) {
        let this = self.project();

        let Some(get_labels) = this.get_labels.take() else {
            panic!("must not be called if terminated");
        };

        this.histograms
            .with_label_values(&get_labels(output))
            .observe(this.start.elapsed().as_secs_f64());
    }
}

impl<Fut, F, const N: usize> FusedFuture for Observe<Fut, F, N>
where
    Fut: Future,
    F: FnOnce(Option<&Fut::Output>) -> [&'static str; N],
{
    fn is_terminated(&self) -> bool {
        self.get_labels.is_none()
    }
}

impl<Fut, F, const N: usize> Future for Observe<Fut, F, N>
where
    Fut: Future,
    F: FnOnce(Option<&Fut::Output>) -> [&'static str; N],
{
    type Output = Fut::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.is_terminated() {
            panic!("observe must not be called after it is terminated");
        }

        let this = self.as_mut().project();
        let output = ready!(this.inner.poll(cx));

        self.observe(Some(&output));

        Poll::Ready(output)
    }
}

#[pin_project::pinned_drop]
impl<Fut, F, const N: usize> PinnedDrop for Observe<Fut, F, N>
where
    Fut: Future,
    F: FnOnce(Option<&Fut::Output>) -> [&'static str; N],
{
    fn drop(self: Pin<&mut Self>) {
        if !self.is_terminated() {
            self.observe(None);
        }
    }
}

/// Deserializes the storage node name and checks that it does not exceed the maximum length
/// `MAX_NODE_NAME_LENGTH`.
pub fn deserialize_node_name<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let name: String = Deserialize::deserialize(deserializer)?;
    if name.len() > MAX_NODE_NAME_LENGTH {
        return Err(D::Error::custom(format!(
            "Node name must not exceed {MAX_NODE_NAME_LENGTH} characters"
        )));
    }
    Ok(name)
}

/// A runtime for metrics and logging.
#[allow(missing_debug_implementations)]
pub struct MetricsAndLoggingRuntime {
    /// The Prometheus registry.
    pub registry: Registry,
    _telemetry_guards: TelemetryGuards,
    /// The tracing handle.
    pub tracing_handle: Arc<TracingHandle>,
    /// The runtime for metrics and logging.
    // INV: Runtime must be dropped last.
    pub runtime: Option<Runtime>,
}

impl MetricsAndLoggingRuntime {
    /// Start metrics and log collection in a new runtime
    pub fn start(metrics_address: SocketAddr) -> anyhow::Result<Self> {
        let runtime = runtime::Builder::new_multi_thread()
            .thread_name("metrics-runtime")
            .worker_threads(2)
            .enable_all()
            .build()
            .context("metrics runtime creation failed")?;
        let _guard = runtime.enter();

        Self::new(metrics_address, Some(runtime))
    }

    /// Create a new runtime for metrics and logging.
    pub fn new(metrics_address: SocketAddr, runtime: Option<Runtime>) -> anyhow::Result<Self> {
        let registry_service = mysten_metrics::start_prometheus_server(metrics_address);
        let walrus_registry = registry_service.default_registry();

        monitored_scope::init_monitored_scope_metrics(&walrus_registry);

        // Initialize logging subscriber
        let (telemetry_guards, tracing_handle) = telemetry_subscribers::TelemetryConfig::new()
            .with_env()
            .with_prom_registry(&walrus_registry)
            .with_json()
            .init();

        // Initialize metrics to track db usage before we create any db instances.
        DBMetrics::init(&walrus_registry);

        Ok(Self {
            runtime,
            registry: Registry::new(walrus_registry),
            _telemetry_guards: telemetry_guards,
            tracing_handle: Arc::new(tracing_handle),
        })
    }
}

/// A config struct to initialize the push metrics. Some binaries that depend on
/// MetricPushRuntime do not need nor is it appropriate to have push metrics.
#[derive(Debug)]
pub struct EnableMetricsPush {
    /// token that is used to gracefully shut down the metrics push process
    pub cancel: CancellationToken,
    /// the network keys we use to identify the client using this push config
    pub network_key_pair: Arc<Secp256r1KeyPair>,
    /// the url, timeouts, etc used to push the metrics
    pub config: MetricsPushConfig,
}

/// MetricPushRuntime to manage the metric push task.
/// We run this in a dedicated runtime to avoid being blocked by others.
#[allow(missing_debug_implementations)]
pub struct MetricPushRuntime {
    metric_push_handle: JoinHandle<anyhow::Result<()>>,
    // INV: Runtime must be dropped last.
    runtime: Runtime,
}

impl MetricPushRuntime {
    /// Starts a task to periodically push metrics to a configured endpoint
    /// if a metrics push endpoint is configured.
    pub fn start(registry: Registry, mp_config: EnableMetricsPush) -> anyhow::Result<Self> {
        let runtime = runtime::Builder::new_multi_thread()
            .thread_name("metric-push-runtime")
            .worker_threads(1)
            .enable_all()
            .build()
            .context("metric push runtime creation failed")?;
        let _guard = runtime.enter();

        let metric_push_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(mp_config.config.push_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut client = create_push_client();
            tracing::info!("starting metrics push to '{}'", &mp_config.config.push_url);
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Err(error) = push_metrics(
                            mp_config.network_key_pair.as_ref(),
                            &client,
                            &mp_config.config.push_url,
                            &registry,
                            // clone because we serialize this with our metrics
                            mp_config.config.labels.clone(),
                        ).await {
                            tracing::warn!(?error, "unable to push metrics");
                            client = create_push_client();
                        }
                    }
                    _ = mp_config.cancel.cancelled() => {
                        tracing::info!("received cancellation request, shutting down metrics push");
                        return Ok(());
                    }
                }
            }
        });

        Ok(Self {
            runtime,
            metric_push_handle,
        })
    }

    /// join handle for the task
    pub fn join(&mut self) -> Result<(), anyhow::Error> {
        tracing::debug!("waiting for the metric push to shutdown...");
        self.runtime.block_on(&mut self.metric_push_handle)?
    }
}

/// Create a request client builder that is used to push metrics to mimir.
fn create_push_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("unable to build client")
}

#[derive(Debug, Deserialize, Serialize)]
/// MetricPayload holds static labels and metric data
/// the static labels are always sent and will be merged within the proxy
pub struct MetricPayload {
    #[serde(skip_serializing_if = "Option::is_none")]
    /// static labels defined in config, eg host, network, etc
    pub labels: Option<HashMap<String, String>>,
    /// protobuf encoded metric families. these must be decoded on the proxy side
    pub buf: Vec<u8>,
}

/// Responsible for sending data to walrus-proxy, used within the async scope of
/// MetricPushRuntime::start.
async fn push_metrics(
    network_key_pair: &Secp256r1KeyPair,
    client: &reqwest::Client,
    push_url: &str,
    registry: &Registry,
    labels: Option<HashMap<String, String>>,
) -> Result<(), anyhow::Error> {
    tracing::debug!(push_url, "pushing metrics to remote");

    // now represents a collection timestamp for all of the metrics we send to the proxy.
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("current time is definitely after the UNIX epoch")
        .as_millis()
        .try_into()
        .expect("timestamp must fit into an i64");

    let mut metric_families = registry.gather();
    for mf in metric_families.iter_mut() {
        for m in mf.mut_metric() {
            m.set_timestamp_ms(now);
        }
    }

    let mut buf: Vec<u8> = vec![];
    let encoder = prometheus::ProtobufEncoder::new();
    encoder.encode(&metric_families, &mut buf)?;

    // serialize the MetricPayload to JSON using serde_json and then compress the entire thing
    let serialized = serde_json::to_vec(&MetricPayload { labels, buf }).inspect_err(|error| {
        tracing::warn!(?error, "unable to serialize MetricPayload to JSON");
    })?;

    let mut s = snap::raw::Encoder::new();
    let compressed = s.compress_vec(&serialized).inspect_err(|error| {
        tracing::warn!(?error, "unable to snappy encode metrics");
    })?;

    let uid = Uuid::now_v7();
    let uids = uid.simple().to_string();
    let signature = network_key_pair.sign_recoverable(uid.as_bytes());
    let auth = serde_json::json!({"signature":signature.encode_base64(), "message":uids});
    let auth_encoded_with_scheme = format!(
        "Secp256k1-recoverable: {}",
        Base64::from_bytes(auth.to_string().as_bytes()).encoded()
    );
    let response = client
        .post(push_url)
        .header(reqwest::header::AUTHORIZATION, auth_encoded_with_scheme)
        .header(reqwest::header::CONTENT_ENCODING, "snappy")
        .body(compressed)
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = match response.text().await {
            Ok(body) => body,
            Err(error) => format!("couldn't decode response body; {error}"),
        };
        return Err(anyhow::anyhow!(
            "metrics push failed: [{}]:{}",
            status,
            body
        ));
    }
    tracing::debug!("successfully pushed metrics to {push_url}");
    Ok(())
}

/// For a storage node, given the shard allocation in previous and current epoch, and currently
/// existing shards, calculate the shards movement in respect to previous epoch (for shard sync)
/// as well as to local existing shards (for shard removal and recovery).
#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub(crate) struct ShardDiffCalculator {
    /// The list of shards that are assigned to the node in current epoch.
    all_owned_shards: Vec<ShardIndex>,
    /// The list of shards that the node gained from the previous epoch.
    gained_shards_from_prev_epoch: Vec<ShardIndex>,
    /// The list of shards that the node no longer needs and can be removed.
    shards_to_remove: Vec<ShardIndex>,
    /// The list of shards that the node should lock.
    shards_to_lock: Vec<ShardIndex>,
}

impl ShardDiffCalculator {
    // TODO(WAL-657): Use `node_id` instead of `public_key`.
    //
    /// Create a new `ShardDiffCalculator` for a storage node.
    pub fn new(committees: &ActiveCommittees, id: &PublicKey, shards_exist: &[ShardIndex]) -> Self {
        let shards_assigned_prev_epoch = committees
            .previous_committee()
            .map(|committee| committee.shards_for_node_public_key(id).to_vec())
            .unwrap_or_default();
        let shards_assigned_current_epoch = committees
            .current_committee()
            .shards_for_node_public_key(id)
            .to_vec();

        let gained_shards_from_prev_epoch = {
            let current: HashSet<_> = shards_assigned_current_epoch.iter().copied().collect();
            let prev: HashSet<_> = shards_assigned_prev_epoch.iter().copied().collect();
            current.difference(&prev).copied().collect()
        };

        let shards_to_remove = {
            let all: HashSet<_> = shards_exist.iter().copied().collect();
            let assigned: HashSet<_> = shards_assigned_prev_epoch
                .iter()
                .chain(shards_assigned_current_epoch.iter())
                .copied()
                .collect();
            all.difference(&assigned).copied().collect()
        };

        let shards_to_lock = {
            let prev: HashSet<_> = shards_assigned_prev_epoch.iter().copied().collect();
            let current: HashSet<_> = shards_assigned_current_epoch.iter().copied().collect();
            prev.difference(&current).copied().collect()
        };

        Self {
            all_owned_shards: shards_assigned_current_epoch,
            gained_shards_from_prev_epoch,
            shards_to_remove,
            shards_to_lock,
        }
    }

    /// Returns the list of shards that are assigned to the node in current epoch serving data.
    pub fn all_owned_shards(&self) -> &[ShardIndex] {
        &self.all_owned_shards
    }

    /// These are the shards that are gained from the previous epoch, and can be synced using
    /// shard sync.
    pub fn gained_shards_from_prev_epoch(&self) -> &[ShardIndex] {
        &self.gained_shards_from_prev_epoch
    }

    /// These are the shards that are no longer needed and can be removed. Note that these shards
    /// do not include the ones that are just moved out. We still need to keep them for shard
    /// sync. These are the shards that was owned by the node at least two epochs ago.
    pub fn shards_to_remove(&self) -> &[ShardIndex] {
        &self.shards_to_remove
    }

    /// These are the shards that are just moved out, and therefore should be locked. They
    /// should not be removed yet given that they need to be served as the source of shard
    /// sync for the new shard owner.
    pub fn shards_to_lock(&self) -> &[ShardIndex] {
        &self.shards_to_lock
    }
}

/// Generates a new Sui wallet for the specified network at the specified path and attempts to fund
/// it through the faucet.
pub async fn generate_sui_wallet(
    sui_network: SuiNetwork,
    path: &Path,
    use_faucet: bool,
    faucet_timeout: Duration,
) -> Result<SuiAddress> {
    tracing::info!(
        "generating Sui wallet for {sui_network} at '{}'",
        path.display()
    );
    let mut wallet = walrus_sui::utils::create_wallet(path, sui_network.env(), None, None)?;
    let rpc_urls = &[wallet.get_rpc_url()?];
    let client = RetriableSuiClient::new_for_rpc_urls(rpc_urls, Default::default(), None).await?;

    let wallet_address = wallet.active_address()?;
    tracing::info!("generated a new Sui wallet; address: {wallet_address}");

    if use_faucet {
        tracing::info!("attempting to get SUI from faucet...");
        match tokio::time::timeout(
            faucet_timeout,
            walrus_sui::utils::request_sui_from_faucet(wallet_address, &sui_network, &client),
        )
        .await
        {
            Err(_) => tracing::warn!("reached timeout while waiting to get SUI from the faucet"),
            Ok(Err(error)) => {
                tracing::warn!(
                    ?error,
                    "an error occurred when trying to get SUI from the faucet"
                )
            }
            Ok(Ok(_)) => tracing::info!("successfully obtained SUI from the faucet"),
        }
    }

    Ok(wallet_address)
}

/// Provides approximate parsing of human-friendly byte values.
///
/// Values are calculated as floating points and the resulting number of bytes is rounded down.
#[derive(Debug, Default, Copy, Clone, PartialEq, Eq)]
pub struct ByteCount(pub u64);

impl ByteCount {
    /// Returns the number of bytes as a `u64`.
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl FromStr for ByteCount {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let original = s;
        let s = s.strip_suffix("B").unwrap_or(s);

        let suffixes = [
            ("K", 1e3),
            ("M", 1e6),
            ("G", 1e9),
            ("T", 1e12),
            ("P", 1e15),
            ("Ki", (1u64 << 10) as f64),
            ("Mi", (1u64 << 20) as f64),
            ("Gi", (1u64 << 30) as f64),
            ("Ti", (1u64 << 40) as f64),
            ("Pi", (1u64 << 50) as f64),
        ];
        let error_context = || format!("invalid byte-count string: {original:?}");

        if let Some((value_str, scale)) = suffixes
            .into_iter()
            .find_map(|(suffix, scale)| Some((s.strip_suffix(suffix)?, scale)))
        {
            let value = f64::from_str(value_str.trim()).with_context(error_context)?;
            let byte_count = (value * scale).floor();
            if byte_count > u64::MAX as f64 {
                anyhow::bail!("provided byte-count is too large: {original:?}");
            } else {
                #[allow(clippy::cast_possible_truncation)] // truncation is fine here
                Ok(ByteCount(byte_count as u64))
            }
        } else {
            // Otherwise, assume unitless.
            // Bytes cannot have fractional components.
            u64::from_str(s.trim())
                .map(ByteCount)
                .with_context(error_context)
        }
    }
}

/// Export the walrus binary version.
// TODO(jsmith): Once the cli logic is moved within the package, this should be crate-visible
pub fn export_build_info(registry: &Registry, version: &'static str) {
    let opts = prometheus::opts!("walrus_build_info", "Walrus binary info");
    let metric = prometheus::register_int_gauge_vec_with_registry!(opts, &["version"], registry)
        .expect("static metric is valid");
    metric
        .get_metric_with_label_values(&[version])
        .expect("metric exists")
        .set(1);
}

/// Export information about the contract to which the storage nodes are communicating.
// TODO(jsmith): Once the cli logic is moved within the package, this should be crate-visible
pub fn export_contract_info(
    registry: &Registry,
    system_object: &ObjectID,
    staking_object: &ObjectID,
    active_address: Option<SuiAddress>,
) {
    let opts = prometheus::opts!("walrus_contract_info", "Walrus smart-contract information");
    let metric = prometheus::register_int_gauge_vec_with_registry!(
        opts,
        &["system_object", "staking_object", "active_address"],
        registry
    )
    .expect("static metric is valid");

    metric
        .get_metric_with_label_values(&[
            &system_object.to_hex_uncompressed(),
            &staking_object.to_hex_uncompressed(),
            &active_address
                .map(|addr| addr.to_string())
                .unwrap_or_default(),
        ])
        .expect("metric exists")
        .set(1);
}

type TracingSubscriberConfiguration = Layered<
    Filtered<
        Box<dyn Layer<tracing_subscriber::Registry> + Send + Sync>,
        EnvFilter,
        tracing_subscriber::Registry,
    >,
    tracing_subscriber::Registry,
>;

/// Prepare the tracing subscriber based on the environment variables.
fn prepare_subscriber(default_log_format: Option<&str>) -> Result<TracingSubscriberConfiguration> {
    // Use INFO level by default.
    let directive = format!(
        "info,{}",
        env::var(EnvFilter::DEFAULT_ENV).unwrap_or_default()
    );
    let layer = tracing_subscriber::fmt::layer().with_writer(std::io::stderr);

    // Control output format based on `LOG_FORMAT` env variable.
    let format = env::var("LOG_FORMAT")
        .ok()
        .or_else(|| default_log_format.map(|fmt| fmt.to_string()));
    let layer = if let Some(format) = &format {
        match format.to_lowercase().as_str() {
            "default" => layer.boxed(),
            "compact" => layer.compact().boxed(),
            "pretty" => layer.pretty().boxed(),
            "json" => layer.json().boxed(),
            s => Err(anyhow!("LOG_FORMAT '{}' is not supported", s))?,
        }
    } else {
        layer.boxed()
    };

    Ok(tracing_subscriber::registry().with(layer.with_filter(EnvFilter::new(directive.clone()))))
}

/// Initializes the logger and tracing subscriber as the global subscriber, requiring a preference
/// for the log format.
pub fn init_tracing_subscriber_with(default_log_format: &str) -> Result<()> {
    prepare_subscriber(Some(default_log_format))?.init();
    tracing::debug!("initialized global tracing subscriber");
    Ok(())
}

/// Initializes the logger and tracing subscriber as the global subscriber. This routine expresses
/// no preference for the log format.
pub fn init_tracing_subscriber() -> Result<()> {
    prepare_subscriber(None)?.init();
    tracing::debug!("initialized global tracing subscriber");
    Ok(())
}

/// Initializes the logger and tracing subscriber as the subscriber for the current scope.
pub fn init_scoped_tracing_subscriber() -> Result<DefaultGuard> {
    let guard = prepare_subscriber(None)?.set_default();
    tracing::debug!("initialized scoped tracing subscriber");
    Ok(guard)
}

/// Returns whether a node cursor should be repositioned.
///
/// The node cursor should be repositioned if it is behind the actual event index and it is at
/// the beginning of the event stream.
///
/// - `event_index`: The index of the next event the node needs to process.
/// - `actual_event_index`: The index of the first event available in the current event stream.
///
/// Usually, these indices match up, meaning the node picks up processing right where it left
/// off. However, there's a special case during node bootstrapping (when starting with no
/// previous state) and event processor bootstrapping itself from event blobs:
///
/// When a node starts up for the first time, older event blobs might have expired due to the
/// MAX_EPOCHS_AHEAD limit. Let's say the earliest available event starts at index N ( where N >
/// 0).
///
/// In this case, the event stream can only provide events starting from index N. But the node,
/// being brand new, wants to start processing from index 0. In this scenario, we actually want
/// to reposition the node's cursor to index N. This is safe because those events are no longer
/// available in the system anyway (they've expired), and marking them as completed prevents the
/// event tracking system from flagging this natural gap as an error.
pub fn should_reposition_cursor(event_index: u64, actual_event_index: u64) -> bool {
    let is_behind = event_index < actual_event_index;
    let is_at_beginning = event_index == 0;
    is_behind && is_at_beginning
}

/// Wait for SIGINT and SIGTERM (unix only).
#[tracing::instrument(skip_all)]
pub async fn wait_until_terminated(mut exit_listener: oneshot::Receiver<()>) {
    #[cfg(not(unix))]
    async fn wait_for_other_signals() {
        // Disables this branch in the select statement.
        std::future::pending().await
    }

    #[cfg(unix)]
    async fn wait_for_other_signals() {
        use tokio::signal::unix;

        unix::signal(unix::SignalKind::terminate())
            .expect("unable to register for SIGTERM signals")
            .recv()
            .await;
        tracing::info!("received SIGTERM")
    }

    tokio::select! {
        biased;
        _ = wait_for_other_signals() => (),
        _ = tokio::signal::ctrl_c() => tracing::info!("received SIGINT"),
        exit_or_dropped = &mut exit_listener => match exit_or_dropped {
            Err(_) => tracing::info!("exit notification sender was dropped"),
            Ok(_) => tracing::info!("exit notification received"),
        }
    }
}

/// Clone a service and return the original instance.
///
/// When cloning a service the clone may not yet be ready. For example, consider cloning a service
/// that holds a permit for a semaphore. The clone is (typically) returned without the permit
/// which is left on the original service. This means that the clone is likely not ready ready to
/// be `call`-ed.
///
/// Therefore, the correct way to clone a ready service, where you would like to the clone to have
/// the readiness, is to `clone()` then call [`std::mem::replace`] to swap the ready, original
/// instance with the clone.
///
/// This function is a convenience function for that pattern.
///
/// For more information see the [`tower::Service`] documentation.
pub(crate) fn clone_ready_service<T, R>(svc: &mut T) -> T
where
    T: tower::Service<R> + Clone,
{
    let unready_clone = svc.clone();
    mem::replace(svc, unready_clone)
}

/// Unwraps the return value from a call to [`tokio::task::spawn_blocking`],
/// or resumes a panic if the function had panicked.
pub(crate) fn unwrap_or_resume_unwind<T, E: std::convert::From<tokio::task::JoinError>>(
    result: Result<Result<T, E>, JoinError>,
) -> Result<T, E> {
    match result {
        Ok(value) => value,
        Err(error) => {
            if error.is_panic() {
                std::panic::resume_unwind(error.into_panic())
            } else {
                Err(error.into())
            }
        }
    }
}

/// Creates a new Walrus client with a refresher using the provided configuration
/// and Sui read client.
pub async fn create_walrus_client_with_refresher(
    contract_config: ContractConfig,
    sui_read_client: walrus_sui::client::SuiReadClient,
) -> anyhow::Result<walrus_sdk::client::Client<walrus_sui::client::SuiReadClient>> {
    let client_config = crate::client::ClientConfig::new_from_contract_config(contract_config);
    walrus_sdk::client::Client::new_read_client_with_refresher(client_config, sui_read_client)
        .await
        .map_err(anyhow::Error::from)
}

#[cfg(test)]
mod tests {
    // Allowing casts in tests.
    #![allow(clippy::cast_lossless, clippy::cast_possible_truncation)]

    use std::num::NonZeroU16;

    use walrus_sui::{test_utils, types::Committee};
    use walrus_test_utils::{assert_unordered_eq, param_test};

    use super::*;

    mod byte_count {
        use super::*;

        param_test! {
            parse: [
                byte: ("1240B", 1240),
                byte_with_space: ("72 B", 72),
                unitless: ("7240", 7240),
                zero: ("0", 0),
            ]
        }
        fn parse(input: &str, expected: u64) {
            assert_eq!(ByteCount::from_str(input).unwrap(), ByteCount(expected));
        }

        macro_rules! test_parse_various {
            ($case_name:ident, $suffix:literal, $scale:expr) => {
                mod $case_name {
                    use super::*;

                    param_test! {
                        parse: [
                            int: (concat!("420", $suffix), 420 * ($scale) as u64),
                            int_b_suffix: (concat!("420", $suffix, "B"), 420 * ($scale) as u64),
                            float: (concat!("1.97", $suffix), (1.97 * ($scale) as f64) as u64),
                            float_b_suffix: (
                                concat!("1.97", $suffix, "B"), (1.97 * ($scale) as f64) as u64
                            ),
                            with_space: (concat!("72", " ", $suffix), 72 * ($scale) as u64),
                            with_space_b_suffix: (
                                concat!("4.2", " ", $suffix, "B"), (4.2 * ($scale) as f64) as u64
                            ),
                        ]
                    }
                }
            };
        }

        test_parse_various!(kilo, "K", 1000);
        test_parse_various!(kibi, "Ki", 1024);
        test_parse_various!(mega, "M", 1e6);
        test_parse_various!(mebi, "Mi", 1024 * 1024);
        test_parse_various!(giga, "G", 1e9);
        test_parse_various!(gibi, "Gi", 1024 * 1024 * 1024);
        test_parse_various!(tera, "T", 1e12);
        test_parse_various!(tebi, "Ti", 1024 * 1024 * 1024 * 1024u64);
        test_parse_various!(peta, "P", 1e15);
        test_parse_various!(pebi, "Pi", 1024 * 1024 * 1024 * 1024 * 1024u64);
    }

    /// Input for the shard diff calculator tests.
    struct ShardDiffCalculatorInput {
        /// The list of shard indexes that are assigned to each node in previous epoch.
        prev_epoch_shards: Vec<Vec<u16>>,
        /// The list of shard indexes that are assigned to each node in current epoch.
        current_epoch_shards: Vec<Vec<u16>>,
        /// The list of shard indexes that the node 0 currently holds.
        existing_shards_all: Vec<u16>,
    }

    /// The expected results from the shard diff calculator.
    struct ExpectedResult {
        /// The list of shard indexes that the node 0 currently serves.
        all_owned_shards: Vec<u16>,
        /// The list of shard indexes that the node 0 gained from the previous epoch.
        gained_shards_from_prev_epoch: Vec<u16>,
        /// The list of shard indexes that the node 0 no longer needs and can be removed.
        shards_to_remove: Vec<u16>,
        /// The list of shard indexes that the node 0 should lock.
        shards_to_lock: Vec<u16>,
    }

    param_test! {
        test_shard_diff_calculator: [
            only_gain: (
                ShardDiffCalculatorInput {
                    prev_epoch_shards: vec![vec![0, 2], vec![1, 3, 4, 5]],
                    current_epoch_shards: vec![vec![0, 1, 2, 3], vec![4, 5]],
                    existing_shards_all: vec![0, 2],
                },
                ExpectedResult {
                    all_owned_shards: vec![0, 1, 2, 3],
                    gained_shards_from_prev_epoch: vec![1, 3],
                    shards_to_remove: vec![],
                    shards_to_lock: vec![],
                },
            ),
            only_lost: (
                ShardDiffCalculatorInput {
                    prev_epoch_shards: vec![vec![2, 3, 4, 5], vec![0, 1]],
                    current_epoch_shards: vec![vec![2, 5], vec![3, 4, 0, 1]],
                    existing_shards_all: vec![2, 3, 4, 5],
                },
                ExpectedResult {
                    all_owned_shards: vec![2, 5],
                    gained_shards_from_prev_epoch: vec![],
                    shards_to_remove: vec![],
                    shards_to_lock: vec![3, 4],
                },
            ),
            gain_and_lost_and_removed: (
                ShardDiffCalculatorInput {
                    prev_epoch_shards: vec![vec![1, 2, 3], vec![0, 4, 5]],
                    current_epoch_shards: vec![vec![0, 1, 2], vec![3, 4, 5]],
                    existing_shards_all: vec![0, 1, 2, 3, 4],
                },
                ExpectedResult {
                    all_owned_shards: vec![0, 1, 2],
                    gained_shards_from_prev_epoch: vec![0],
                    shards_to_remove: vec![4],
                    shards_to_lock: vec![3],
                },
            ),
            // This simulates the case for a node recovery, where the local shards completely
            // mismatches the assignment in the previous and current epochs.
            existing_shards_all_mismatch_assignment: (
                ShardDiffCalculatorInput {
                    prev_epoch_shards: vec![vec![1, 2, 3], vec![0, 4, 5]],
                    current_epoch_shards: vec![vec![0, 1, 2], vec![3, 4, 5]],
                    existing_shards_all: vec![3, 4, 5],
                },
                ExpectedResult {
                    all_owned_shards: vec![0, 1, 2],
                    gained_shards_from_prev_epoch: vec![0],
                    shards_to_remove: vec![4, 5],
                    shards_to_lock: vec![3],
                },
            ),
            // This simulates the case for a shard, 4, that is moved out and back in in the next
            // epoch.
            removed_and_gained: (
                ShardDiffCalculatorInput {
                    prev_epoch_shards: vec![vec![1, 2, 3], vec![0, 4, 5]],
                    current_epoch_shards: vec![vec![1, 2, 3, 4], vec![0, 5]],
                    existing_shards_all: vec![1, 2, 3, 4],
                },
                ExpectedResult {
                    all_owned_shards: vec![1, 2, 3, 4],
                    gained_shards_from_prev_epoch: vec![4],
                    shards_to_remove: vec![],
                    shards_to_lock: vec![],
                },
            ),
        ]
    }
    fn test_shard_diff_calculator(
        input: ShardDiffCalculatorInput,
        expected_result: ExpectedResult,
    ) {
        let prev_epoch = 5;
        let current_epoch = 6;

        // Build active committees based on the input.
        let mut node_0 = test_utils::new_move_storage_node_for_testing();
        node_0.shard_ids = input.prev_epoch_shards[0]
            .iter()
            .map(|s| ShardIndex(*s))
            .collect();
        let mut node_1 = test_utils::new_move_storage_node_for_testing();
        node_1.shard_ids = input.prev_epoch_shards[1]
            .iter()
            .map(|s| ShardIndex(*s))
            .collect();
        let prev_committees = Committee::new(
            vec![node_0.clone(), node_1.clone()],
            prev_epoch,
            NonZeroU16::new(
                input
                    .prev_epoch_shards
                    .iter()
                    .map(|s| s.len())
                    .sum::<usize>() as u16,
            )
            .unwrap(),
        )
        .expect("failed to create committees");

        node_0.shard_ids = input.current_epoch_shards[0]
            .iter()
            .map(|s| ShardIndex(*s))
            .collect();
        node_1.shard_ids = input.current_epoch_shards[1]
            .iter()
            .map(|s| ShardIndex(*s))
            .collect();
        let current_committees = Committee::new(
            vec![node_0.clone(), node_1.clone()],
            current_epoch,
            NonZeroU16::new(
                input
                    .current_epoch_shards
                    .iter()
                    .map(|s| s.len())
                    .sum::<usize>() as u16,
            )
            .unwrap(),
        )
        .expect("failed to create committees");

        let active_committees = ActiveCommittees::new(current_committees, Some(prev_committees));

        let shard_diff_calculator = ShardDiffCalculator::new(
            &active_committees,
            &node_0.public_key,
            &input
                .existing_shards_all
                .iter()
                .map(|s| ShardIndex(*s))
                .collect::<Vec<_>>(),
        );

        assert_unordered_eq!(
            shard_diff_calculator.all_owned_shards().iter().copied(),
            expected_result
                .all_owned_shards
                .iter()
                .map(|s| ShardIndex(*s))
        );
        assert_unordered_eq!(
            shard_diff_calculator
                .gained_shards_from_prev_epoch()
                .iter()
                .copied(),
            expected_result
                .gained_shards_from_prev_epoch
                .iter()
                .map(|s| ShardIndex(*s))
        );
        assert_unordered_eq!(
            shard_diff_calculator.shards_to_remove().iter().copied(),
            expected_result
                .shards_to_remove
                .iter()
                .map(|s| ShardIndex(*s))
        );
        assert_unordered_eq!(
            shard_diff_calculator.shards_to_lock().iter().copied(),
            expected_result
                .shards_to_lock
                .iter()
                .map(|s| ShardIndex(*s))
        );
    }
}

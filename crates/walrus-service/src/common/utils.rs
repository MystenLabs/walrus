// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Utility functions for the Walrus service.

use std::{
    collections::HashSet,
    env,
    fmt::Debug,
    future::Future,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    pin::Pin,
    str::FromStr,
    sync::Arc,
    task::{ready, Context, Poll},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, Context as _, Result};
use fastcrypto::{
    encoding::Base64,
    secp256r1::Secp256r1KeyPair,
    traits::{EncodeDecodeBase64, RecoverableSigner},
};
use futures::future::FusedFuture;
use pin_project::pin_project;
use prometheus::{Encoder, HistogramVec, Registry};
use serde::{
    de::{DeserializeOwned, Error},
    Deserialize,
    Deserializer,
};
use serde_json;
use sui_sdk::wallet_context::WalletContext;
use sui_types::base_types::{ObjectID, SuiAddress};
use telemetry_subscribers::{TelemetryGuards, TracingHandle};
use tokio::{
    runtime::{self, Runtime},
    sync::Semaphore,
    task::JoinHandle,
    time::Instant,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, subscriber::DefaultGuard};
use tracing_subscriber::{
    filter::Filtered,
    layer::{Layered, SubscriberExt as _},
    util::SubscriberInitExt,
    EnvFilter,
    Layer,
};
use uuid::Uuid;
use walrus_core::{PublicKey, ShardIndex};
use walrus_sui::utils::SuiNetwork;

use super::active_committees::ActiveCommittees;
use crate::node::config::MetricsConfig;

/// Defines a constant containing the version consisting of the package version and git revision.
///
/// We are using a macro as placing this logic into a library can result in unnecessary builds.
#[macro_export]
macro_rules! version {
    () => {{
        /// The Git revision obtained through `git describe` at compile time.
        const GIT_REVISION: &str = {
            if let Some(revision) = option_env!("GIT_REVISION") {
                revision
            } else {
                let version = git_version::git_version!(
                    args = ["--always", "--abbrev=12", "--dirty", "--exclude", "*"],
                    fallback = ""
                );
                if version.is_empty() {
                    panic!("unable to query git revision");
                }
                version
            }
        };

        // The version consisting of the package version and Git revision.
        walrus_core::concat_const_str!(env!("CARGO_PKG_VERSION"), "-", GIT_REVISION)
    }};
}
pub use version;

/// Trait for loading configuration from a YAML file.
pub trait LoadConfig: DeserializeOwned {
    /// Load the configuration from a YAML file located at the provided path.
    fn load<P: AsRef<Path>>(path: P) -> Result<Self, anyhow::Error> {
        let path = path.as_ref();
        tracing::debug!(path = %path.display(), "reading config from file");

        let reader = std::fs::File::open(path)
            .with_context(|| format!("Unable to load config from {}", path.display()))?;

        Ok(serde_yaml::from_reader(reader)?)
    }
}

/// Helper functions applied to futures.
pub(crate) trait FutureHelpers: Future {
    /// Limits the number of simultaneously executing futures.
    #[allow(dead_code)]
    async fn batch_limit(self, permits: Arc<Semaphore>) -> Self::Output
    where
        Self: Future,
        Self: Sized,
    {
        let _permit = permits
            .acquire_owned()
            .await
            .expect("semaphore never closed");
        self.await
    }

    /// Reports metrics for the future.
    fn observe<F, const N: usize>(
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
    fn new(inner: Fut, histograms: HistogramVec, get_labels: F) -> Self {
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

/// Can be used to deserialize optional paths such that the `~` is resolved to the user's home
/// directory.
pub fn resolve_home_dir<'de, D>(deserializer: D) -> Result<PathBuf, D::Error>
where
    D: Deserializer<'de>,
{
    let path: PathBuf = Deserialize::deserialize(deserializer)?;
    path_with_resolved_home_dir(path).map_err(D::Error::custom)
}

/// Can be used to deserialize optional paths such that the `~` is resolved to the user's home
/// directory.
pub fn resolve_home_dir_option<'de, D>(deserializer: D) -> Result<Option<PathBuf>, D::Error>
where
    D: Deserializer<'de>,
{
    let path: Option<PathBuf> = Deserialize::deserialize(deserializer)?;
    let Some(path) = path else { return Ok(None) };
    Ok(Some(
        path_with_resolved_home_dir(path).map_err(D::Error::custom)?,
    ))
}

fn path_with_resolved_home_dir(path: PathBuf) -> Result<PathBuf> {
    if path.starts_with("~/") {
        let home = home::home_dir().context("unable to resolve home directory")?;
        return Ok(home.join(
            path.strip_prefix("~")
                .expect("we just checked for this prefix"),
        ));
    } else {
        Ok(path)
    }
}

/// A runtime for metrics and logging.
#[allow(missing_debug_implementations)]
pub struct MetricsAndLoggingRuntime {
    /// The Prometheus registry.
    pub registry: Registry,
    _telemetry_guards: TelemetryGuards,
    _tracing_handle: TracingHandle,
    /// The runtime for metrics and logging.
    // INV: Runtime must be dropped last.
    pub runtime: Runtime,
}

impl MetricsAndLoggingRuntime {
    /// Start metrics and log collection in a new runtime
    pub fn start(mut metrics_address: SocketAddr) -> anyhow::Result<Self> {
        let runtime = runtime::Builder::new_multi_thread()
            .thread_name("metrics-runtime")
            .worker_threads(2)
            .enable_all()
            .build()
            .context("metrics runtime creation failed")?;
        let _guard = runtime.enter();

        metrics_address.set_ip(IpAddr::V4(Ipv4Addr::UNSPECIFIED));
        let registry_service = mysten_metrics::start_prometheus_server(metrics_address);
        let walrus_registry = registry_service.default_registry();

        // Initialize logging subscriber
        let (telemetry_guards, tracing_handle) = telemetry_subscribers::TelemetryConfig::new()
            .with_env()
            .with_prom_registry(&walrus_registry)
            .with_json()
            .init();

        Ok(Self {
            runtime,
            registry: walrus_registry,
            _telemetry_guards: telemetry_guards,
            _tracing_handle: tracing_handle,
        })
    }
}

/// A config struct to initialize the push metrics. Some binaries that depend on
/// MetricsAndLoggingRuntime do not need nor is it appropriate to have push metrics.
#[derive(Debug)]
pub struct EnableMetricsPush {
    /// token that is used to gracefully shut down the metrics push process
    pub cancel: CancellationToken,
    /// the network keys we use to identify the client using this push config
    pub network_key_pair: Arc<Secp256r1KeyPair>,
    /// the url, timeouts, etc used to push the metrics
    pub config: MetricsConfig,
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
            .context("meteric push runtime creation failed")?;
        let _guard = runtime.enter();

        // associate a default tls provider for this runtime
        let tls_provider = rustls::crypto::ring::default_provider();
        tls_provider
            .install_default()
            .expect("unable to install default tls provider for rustls in MetricPushRuntime");

        let metric_push_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(mp_config.config.push_interval_seconds);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut client = create_push_client();
            let push_url = mp_config.config.push_url;
            info!("starting metrics push to {}", &push_url);
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Err(e) = push_metrics(
                            mp_config.network_key_pair.clone(),
                            &client,
                            &push_url,
                            &registry,
                        ).await {
                            error!("unable to push metrics: {e}");
                            client = create_push_client();
                        }
                    }
                    _ = mp_config.cancel.cancelled() => {
                        info!("received cancellation request, shutting down metrics push");
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

/// Responsible for sending data to walrus-proxy, used within the async scope of
/// MetricPushRuntime::start.
async fn push_metrics(
    network_key_pair: Arc<Secp256r1KeyPair>,
    client: &reqwest::Client,
    push_url: &str,
    registry: &Registry,
) -> Result<(), anyhow::Error> {
    info!(push_url =% push_url, "pushing metrics to remote");

    // now represents a collection timestamp for all of the metrics we send to the proxy.
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    let mut metric_families = registry.gather();
    for mf in metric_families.iter_mut() {
        for m in mf.mut_metric() {
            m.set_timestamp_ms(now);
        }
    }

    let mut buf: Vec<u8> = vec![];
    let encoder = prometheus::ProtobufEncoder::new();
    encoder.encode(&metric_families, &mut buf)?;

    let mut s = snap::raw::Encoder::new();
    let compressed = s.compress_vec(&buf).map_err(|err| {
        error!("unable to snappy encode; {err}");
        err
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
        .header(reqwest::header::CONTENT_TYPE, prometheus::PROTOBUF_FORMAT)
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
    debug!("successfully pushed metrics to {push_url}");
    Ok(())
}

/// The difference between shard allocations in different epochs.
#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub(crate) struct ShardDiff {
    /// Shards lost from the assignment.
    pub lost: Vec<ShardIndex>,
    /// Shards gained in the assignment.
    pub gained: Vec<ShardIndex>,
    /// Shards which are common to both assignments.
    pub unchanged: Vec<ShardIndex>,
    /// Shards that can be removed from this node.
    pub removed: Vec<ShardIndex>,
}

impl ShardDiff {
    /// Returns a new `ShardDiff` when moving from the allocation in
    /// `committees.previous_committee()` to `committees.current_committee()` for the node
    /// identified by the provided public key.
    /// `exist` is the list of shards that the node currently holds, which is used to find out
    /// the shards that are no longer needed in the node and can be removed.
    pub fn diff_previous(
        committees: &ActiveCommittees,
        exist: &[ShardIndex],
        id: &PublicKey,
    ) -> ShardDiff {
        let from: &[ShardIndex] = committees
            .previous_committee()
            .map_or(&[], |committee| committee.shards_for_node_public_key(id));
        let to = committees
            .current_committee()
            .shards_for_node_public_key(id);
        Self::diff(from, to, exist)
    }

    /// Returns a new `ShardDiff` when moving from the allocation in `from` to `to`.
    pub fn diff(from: &[ShardIndex], to: &[ShardIndex], exist: &[ShardIndex]) -> ShardDiff {
        let from: HashSet<ShardIndex> = from.iter().copied().collect();
        let to: HashSet<ShardIndex> = to.iter().copied().collect();
        let exist: HashSet<ShardIndex> = exist.iter().copied().collect();

        ShardDiff {
            unchanged: exist.intersection(&to).copied().collect(),
            lost: from.difference(&to).copied().collect(),
            gained: to.difference(&exist).copied().collect(),
            removed: exist
                .difference(&from.union(&to).copied().collect())
                .copied()
                .collect(),
        }
    }
}

/// Returns the path if it is `Some` or any of the default paths if they exist (attempt in order).
pub fn path_or_defaults_if_exist(path: &Option<PathBuf>, defaults: &[PathBuf]) -> Option<PathBuf> {
    tracing::debug!(?path, ?defaults, "looking for configuration file");
    let mut path = path.clone();
    for default in defaults {
        if path.is_some() {
            break;
        }
        path = default.exists().then_some(default.clone());
    }
    path
}

/// Loads the wallet context from the given path.
///
/// If no path is provided, tries to load the configuration first from the local folder, and then
/// from the standard Sui configuration directory.
// NB: When making changes to the logic, make sure to update the argument docs in
// `crates/walrus-service/bin/client.rs`.
pub fn load_wallet_context(path: &Option<PathBuf>) -> Result<WalletContext> {
    let mut default_paths = vec!["./sui_config.yaml".into()];
    if let Some(home_dir) = home::home_dir() {
        default_paths.push(home_dir.join(".sui").join("sui_config").join("client.yaml"))
    }
    let path = path_or_defaults_if_exist(path, &default_paths)
        .ok_or(anyhow!("could not find a valid wallet config file"))?;
    tracing::info!("using Sui wallet configuration from '{}'", path.display());
    WalletContext::new(&path, None, None)
}

/// Generates a new Sui wallet for the specified network at the specified path and attempts to fund
/// it through the faucet.
pub async fn generate_sui_wallet(
    sui_network: SuiNetwork,
    path: &Path,
    faucet_timeout: Duration,
) -> Result<SuiAddress> {
    tracing::info!(
        "generating Sui wallet for {sui_network} at '{}'",
        path.display()
    );
    let mut wallet = walrus_sui::utils::create_wallet(path, sui_network.env(), None)?;
    let wallet_address = wallet.active_address()?;
    tracing::info!("generated a new Sui wallet; address: {wallet_address}");

    tracing::info!("attempting to get SUI from faucet...");
    match tokio::time::timeout(
        faucet_timeout,
        walrus_sui::utils::request_sui_from_faucet(
            wallet_address,
            &sui_network,
            &wallet.get_client().await?,
        ),
    )
    .await
    {
        Err(_) => tracing::warn!("reached timeout while waiting to get SUI from the faucet"),
        Ok(Err(e)) => {
            tracing::warn!("an error occurred when trying to get SUI from the faucet: {e}")
        }
        Ok(Ok(_)) => tracing::info!("successfully obtained SUI from the faucet"),
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
            f64::from_str(value_str.trim())
                .map(|value| ByteCount((value * scale).floor() as u64))
                .with_context(error_context)
        } else {
            // Otherwise, assume unittless.
            // Bytes cannot have fractional components
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

type PrepareResult = Result<
    Layered<
        Filtered<
            Box<dyn Layer<tracing_subscriber::Registry> + Send + Sync>,
            EnvFilter,
            tracing_subscriber::Registry,
        >,
        tracing_subscriber::Registry,
    >,
>;

/// Prepare the tracing subscriber based on the environment variables.
fn prepare_subscriber() -> PrepareResult {
    // Use INFO level by default.
    let directive = format!(
        "info,{}",
        env::var(EnvFilter::DEFAULT_ENV).unwrap_or_default()
    );
    let layer = tracing_subscriber::fmt::layer().with_writer(std::io::stderr);

    // Control output format based on `LOG_FORMAT` env variable.
    let format = env::var("LOG_FORMAT").ok();
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

/// Initializes the logger and tracing subscriber as the global subscriber.
pub fn init_tracing_subscriber() -> Result<()> {
    prepare_subscriber()?.init();
    tracing::debug!("initialized global tracing subscriber");
    Ok(())
}

/// Initializes the logger and tracing subscriber as the subscriber for the current scope.
pub fn init_scoped_tracing_subscriber() -> Result<DefaultGuard> {
    let guard = prepare_subscriber()?.set_default();
    tracing::debug!("initialized scoped tracing subscriber");
    Ok(guard)
}

#[cfg(test)]
mod tests {

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

    param_test! {
        test_shard_diff: [
            only_gain: (
                ShardDiff::diff(
                    &[],
                    &[ShardIndex(1), ShardIndex(2), ShardIndex(3), ShardIndex(4)],
                    &[],
                ),
                ShardDiff {
                    gained: vec![ShardIndex(1), ShardIndex(2), ShardIndex(3), ShardIndex(4)],
                    ..Default::default()
                },
            ),
            gained_and_lost: (
                ShardDiff::diff(
                    &[ShardIndex(1), ShardIndex(2), ShardIndex(3), ShardIndex(4)],
                    &[ShardIndex(2), ShardIndex(3), ShardIndex(4), ShardIndex(5)],
                    &[ShardIndex(1), ShardIndex(2), ShardIndex(3), ShardIndex(4)],
                ),
                ShardDiff {
                    lost: vec![ShardIndex(1)],
                    gained: vec![ShardIndex(5)],
                    unchanged: vec![ShardIndex(2), ShardIndex(3), ShardIndex(4)],
                    removed: vec![],
                },
            ),
            gained_lost_and_removed: (
                ShardDiff::diff(
                    &[ShardIndex(3), ShardIndex(4), ShardIndex(5)],
                    &[ShardIndex(6), ShardIndex(7), ShardIndex(4)],
                    &[ShardIndex(1), ShardIndex(2), ShardIndex(4)],
                ),
                ShardDiff {
                    lost: vec![ShardIndex(3), ShardIndex(5)],
                    gained: vec![ShardIndex(6), ShardIndex(7)],
                    unchanged: vec![ShardIndex(4)],
                    removed: vec![ShardIndex(1), ShardIndex(2)],
                },
            ),
            gained_from_old_epochs: (
                ShardDiff::diff(
                    &[ShardIndex(3), ShardIndex(4), ShardIndex(5)],
                    &[ShardIndex(6), ShardIndex(3), ShardIndex(4)],
                    &[ShardIndex(1), ShardIndex(2), ShardIndex(4)],
                ),
                ShardDiff {
                    lost: vec![ShardIndex(5)],
                    gained: vec![ShardIndex(3), ShardIndex(6)],
                    unchanged: vec![ShardIndex(4)],
                    removed: vec![ShardIndex(1), ShardIndex(2)],
                },
            ),
        ]
    }
    fn test_shard_diff(computed_diff: ShardDiff, expected_result: ShardDiff) {
        assert_unordered_eq!(computed_diff.lost, expected_result.lost);
        assert_unordered_eq!(computed_diff.gained, expected_result.gained);
        assert_unordered_eq!(computed_diff.unchanged, expected_result.unchanged);
        assert_unordered_eq!(computed_diff.removed, expected_result.removed);
    }
}

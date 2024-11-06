// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Push metrics implementation

use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context as _, Error, Result};
use fastcrypto::{
    encoding::Base64,
    secp256r1::Secp256r1KeyPair,
    traits::{EncodeDecodeBase64, RecoverableSigner},
};
use prometheus::{Encoder, Registry};
use serde_json;
use tokio::{
    runtime::{Builder, Runtime},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};
use uuid::Uuid;

use super::config::MetricsConfig;

/// MetricPushRuntime to manage the metric push task.
#[allow(missing_debug_implementations)]
pub struct MetricPushRuntime {
    metric_push_handle: JoinHandle<anyhow::Result<()>>,
    // INV: Runtime must be dropped last.
    runtime: Runtime,
}

impl MetricPushRuntime {
    /// Starts a task to periodically push metrics to a configured
    /// endpoint if a metrics push endpoint is configured.
    pub fn start(
        cancel: CancellationToken,
        network_key_pair: Arc<Secp256r1KeyPair>,
        config: MetricsConfig,
        registry: Registry,
    ) -> anyhow::Result<Self> {
        let runtime = Builder::new_multi_thread()
            .thread_name("metrics-push-runtime")
            .worker_threads(2)
            .enable_all()
            .build()
            .context("metrics push runtime creation failed")?;
        let _guard = runtime.enter();

        // associate a default tls provider for this runtime
        let tls_provider = rustls::crypto::ring::default_provider();
        tls_provider
            .install_default()
            .expect("unable to install default tls provider for rustls in MetricPushRuntime");

        let metric_push_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(config.push_interval_seconds);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut client = create_push_client();
            info!("starting metrics push to {}", &config.push_url);
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Err(e) = push_metrics(
                            network_key_pair.clone(),
                            &client, &config.push_url, &registry
                        ).await {
                            error!("unable to push metrics: {e}");
                            client = create_push_client();
                        }
                    }
                    _ = cancel.cancelled() => {
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

    /// join handle for the task.
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

/// Responsible for sending data to walrus-proxy, used within the async
/// scope of MetricPushRuntime::start.
async fn push_metrics(
    network_key_pair: Arc<Secp256r1KeyPair>,
    client: &reqwest::Client,
    push_url: &str,
    registry: &Registry,
) -> Result<(), Error> {
    info!(push_url =% push_url, "pushing metrics to remote");

    // now represents a collection timestamp for all of the metrics we send to the
    // proxy.
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

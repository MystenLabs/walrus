// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Push metrics implementation

use super::config::MetricsConfig;
use anyhow::{Error, Result};
use fastcrypto::secp256r1::Secp256r1KeyPair;
use fastcrypto::traits::RecoverableSigner;
use prometheus::Encoder;
use prometheus::Registry;
use serde_json;
use uuid::Uuid;
use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

/// Starts a task to periodically push metrics to a configured endpoint if a metrics push endpoint
/// is configured.
pub fn start_metrics_push_task(
    cancel: CancellationToken,
    network_key_pair: Arc<Secp256r1KeyPair>,
    config: MetricsConfig,
    registry: Registry,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(
            config.push_interval_seconds.unwrap_or(60),
        ));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut client = create_push_client();
        let push_url = config.push_url.expect("missing push for metrics url!");
        info!("starting metrics push to {}", &push_url);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(e) = push_metrics(network_key_pair.clone(), &client, &push_url, &registry).await {
                        error!("unable to push metrics: {e}; a new push client will be created");
                        client = create_push_client();
                    }
                }
                _ = cancel.cancelled() => {
                    info!("received cancellation request, shutting down metrics push");
                    return;
                }
            }
        }
    });
}

fn create_push_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("unable to build client")
}

async fn push_metrics(
    network_key_pair: Arc<Secp256r1KeyPair>,
    client: &reqwest::Client,
    push_url: &str,
    registry: &Registry,
) -> Result<(), Error> {
    info!(push_url =% push_url, "pushing metrics to remote");

    // now represents a collection timestamp for all of the metrics we send to the proxy
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
    let auth = serde_json::json!({"signature":network_key_pair.sign_recoverable(uid.as_bytes()), "message":uids});

    let response = client
        .post(push_url)
        .header(reqwest::header::AUTHORIZATION, auth.to_string())
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

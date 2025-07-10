// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Walrus Upload Relay entry point.

use std::{env, net::SocketAddr, path::PathBuf};

use anyhow::Result;
use clap::Parser;
use controller::{DEFAULT_SERVER_ADDRESS, run_upload_relay};
use walrus_sdk::core_utils::{
    bin_version,
    metrics::{Registry, monitored_scope},
};

mod controller;
mod error;
mod metrics;
mod tip;
mod utils;

bin_version!();

#[derive(Parser, Debug, Clone)]
#[command(
    author,
    about = "The Walrus Upload Relay",
    long_about = None,
    name = env!("CARGO_BIN_NAME"),
    version = VERSION,
rename_all = "kebab-case",
)]
struct Args {
    #[arg(
        long,
        help = "Override the metrics address to use",
        default_value = "127.0.0.1:9184"
    )]
    metrics_address: std::net::SocketAddr,
    /// The configuration context to use for the client, if omitted the default_context is used.
    #[arg(long)]
    context: Option<String>,
    /// The file path to the Walrus read client configuration.
    #[arg(long)]
    walrus_config: PathBuf,
    /// The address to listen on. Defaults to 0.0.0.0:57391.
    #[arg(long, default_value=DEFAULT_SERVER_ADDRESS)]
    server_address: SocketAddr,
    /// The file path to the configuration of the Walrus Upload Relay.
    #[arg(long)]
    relay_config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let registry_service = mysten_metrics::start_prometheus_server(args.metrics_address);
    let walrus_registry = registry_service.default_registry();

    monitored_scope::init_monitored_scope_metrics(&walrus_registry);

    // Initialize logging subscriber
    let (_telemetry_guards, _tracing_handle) = telemetry_subscribers::TelemetryConfig::new()
        .with_env()
        .with_prom_registry(&walrus_registry)
        .with_json()
        .init();
    let registry = Registry::new(walrus_registry);

    run_upload_relay(
        args.context,
        args.walrus_config,
        args.server_address,
        args.relay_config,
        registry,
    )
    .await
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use axum::{extract::Query, http::Uri};
    use reqwest::Url;
    use sui_types::digests::TransactionDigest;
    use walrus_sdk::{ObjectID, core::BlobId};

    use crate::{
        params::{DIGEST_LEN, Params},
        shared::blob_upload_relay_url,
    };

    #[test]
    fn test_upload_relay_parse_query() {
        let blob_id =
            BlobId::from_str("efshm0WcBczCA_GVtB0itHbbSXLT5VMeQDl0A1b2_0Y").expect("valid blob id");
        let tx_id = TransactionDigest::new([13; DIGEST_LEN]);
        let nonce = [23; DIGEST_LEN];
        let params = Params {
            blob_id,
            nonce: Some(nonce),
            deletable_blob_object: Some(ObjectID::from_single_byte(42)),
            tx_id: Some(tx_id),
            encoding_type: None,
        };

        let url =
            blob_upload_relay_url(&Url::parse("http://localhost").expect("valid url"), &params)
                .expect("valid parameters");

        let uri = Uri::from_str(url.as_ref()).expect("valid conversion");
        let result = Query::<Params>::try_from_uri(&uri).expect("parsing the uri works");

        assert_eq!(params.blob_id, result.blob_id);
        assert_eq!(params.nonce, result.nonce);
        assert_eq!(params.tx_id, result.tx_id);
        assert_eq!(params.deletable_blob_object, result.deletable_blob_object);
    }
}

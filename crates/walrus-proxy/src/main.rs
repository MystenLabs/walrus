// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
//! - walrus-proxy service
//!
//! the walrus-proxy service acts as a relay for nodes to push metrics to and we
//! in turn push them to a mimir cluster.

use std::env;

use anyhow::Result;
use clap::Parser;
use tracing::info;
use tracing_subscriber;
use walrus_proxy::{
    admin,
    config::{load, ProxyConfig},
    consumer::Label,
    histogram_relay,
    metrics,
    providers,
};

// Define the `GIT_REVISION` and `VERSION` consts
walrus_proxy::bin_version!();

/// user agent we use when posting to mimir
static APP_USER_AGENT: &str = const_str::concat!(
    env!("CARGO_BIN_NAME"),
    "/",
    env!("CARGO_PKG_VERSION"),
    "/",
    VERSION
);

#[derive(Parser, Debug)]
#[clap(rename_all = "kebab-case")]
#[clap(name = env!("CARGO_BIN_NAME"))]
#[clap(version = VERSION)]
struct Args {
    #[clap(
        long,
        short,
        default_value = "./walrus-proxy.yaml",
        help = "Specify the config file path to use"
    )]
    config: String,
}

/// main fn for walrus proxy
#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    let config: ProxyConfig = load(args.config)?;

    info!(
        "listen on {:?} send to {:?}",
        config.listen_address, config.remote_write.url
    );

    let listener = std::net::TcpListener::bind(config.listen_address).unwrap();
    let histogram_listener = std::net::TcpListener::bind(config.histogram_address).unwrap();
    let metrics_listener = std::net::TcpListener::bind(config.metrics_address).unwrap();

    let remote_write_client = admin::make_reqwest_client(config.remote_write, APP_USER_AGENT);
    let histogram_relay = histogram_relay::start_prometheus_server(histogram_listener);
    let _registry = metrics::start_prometheus_server(metrics_listener);

    // setup committee provider
    let walrus_node_provider = providers::WalrusNodeProvider::new(
        &config.dynamic_peers.url,
        &config.dynamic_peers.interval,
        &config.dynamic_peers.staking_object_id,
    );
    // begin polling
    walrus_node_provider.poll_peer_list();

    let app = admin::app(
        vec![Label {
            name: "network".into(),
            value: config.network,
        }],
        remote_write_client,
        histogram_relay,
        Some(walrus_node_provider),
    );

    admin::server(listener, app, config.self_signd_tls)
        .await
        .unwrap();
    Ok(())
}

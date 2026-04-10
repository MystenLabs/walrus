// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Benchmark to find the maximum committee size the Walrus Move contracts can support.
//!
//! Performs a binary search over committee sizes, testing worst-case dynamic field accesses
//! during `voting_end` and `initiate_epoch_change` by forcing a complete committee replacement.
//!
//! Note that to use this benchmark, the `TEMP_ACTIVE_SET_SIZE_LIMIT` in
//! `contracts/walrus/sources/staking/staking_inner.move` needs to be increased to at least `high`.
//!
//! Usage:
//!   cargo bench -p walrus-sui --features test-utils --bench committee_size_bench
//!
//! Options:
//!   --low <N>        Lower bound for binary search (default: 100)
//!   --high <N>       Upper bound for binary search (default: 1000)
//!   --precision <N>  Stop when range is within N (default: 1)

use std::{num::NonZeroU16, sync::Arc, time::Duration};

use anyhow::Result;
use clap::Parser;
use tokio::sync::Mutex;
use walrus_core::keys::{NetworkKeyPair, ProtocolKeyPair};
use walrus_sui::{
    client::{ReadClient, SuiContractClient},
    test_utils::{
        TestClusterHandle,
        new_wallet_on_sui_test_cluster,
        system_setup::{
            create_and_init_system_for_test,
            end_epoch_zero,
            register_committee_and_stake,
        },
    },
    types::{NodeRegistrationParams, StorageNodeCap},
};

const ONE_WAL: u64 = 1_000_000_000;
const N_SHARDS: u16 = 1000;

#[derive(Parser, Debug)]
#[command(about = "Find the maximum committee size the Walrus contracts can support")]
struct Args {
    /// Lower bound for binary search.
    #[arg(long, default_value_t = 100)]
    low: usize,
    /// Upper bound for binary search.
    #[arg(long, default_value_t = 1000)]
    high: usize,
    /// Stop when the search range is within this precision.
    #[arg(long, default_value_t = 1)]
    precision: usize,
    /// Fraction of the committee that changes between epochs (0.0 to 1.0).
    ///
    /// Controls how many replacement nodes are registered with higher stake to displace
    /// existing committee members. For example, 0.33 replaces roughly a third of the committee.
    /// Default 1.0 means the entire committee changes (worst case).
    #[arg(long, default_value_t = 1.0)]
    churn_fraction: f64,
    /// Argument passed by `cargo bench`, ignored.
    #[arg(long, default_value_t = true)]
    bench: bool,
}

fn generate_node_params(n: usize) -> (Vec<NodeRegistrationParams>, Vec<ProtocolKeyPair>) {
    let bls_keys: Vec<_> = (0..n).map(|_| ProtocolKeyPair::generate()).collect();
    let params: Vec<_> = bls_keys
        .iter()
        .map(|protocol_keypair| {
            let network_key_pair = NetworkKeyPair::generate();
            NodeRegistrationParams::new_for_test(
                protocol_keypair.public(),
                network_key_pair.public(),
            )
        })
        .collect();
    (params, bls_keys)
}

async fn register_nodes(
    admin_client: &SuiContractClient,
    node_params: &[NodeRegistrationParams],
    bls_keys: &[ProtocolKeyPair],
    stake_per_node: u64,
) -> Result<Vec<StorageNodeCap>> {
    let n = node_params.len();
    let client_refs: Vec<_> = (0..n).map(|_| admin_client).collect();
    let amounts: Vec<_> = (0..n).map(|_| stake_per_node).collect();

    register_committee_and_stake(
        admin_client,
        node_params,
        bls_keys,
        &client_refs,
        &amounts,
        Some(1),
    )
    .await
}

/// Calls `epoch_sync_done` for enough nodes to reach quorum (2f+1 shards).
async fn complete_epoch_sync(
    client: &SuiContractClient,
    epoch: u32,
    caps: &[StorageNodeCap],
    n_shards: u16,
) -> Result<()> {
    let n_nodes = caps.len() as u64;
    let shards_needed = (2 * u64::from(n_shards)) / 3 + 1;
    let shards_per_node = u64::from(n_shards) / n_nodes;
    let nodes_needed = if shards_per_node == 0 {
        n_nodes
    } else {
        shards_needed.div_ceil(shards_per_node)
    };
    let nodes_to_sync =
        usize::try_from(nodes_needed.min(n_nodes)).expect("node count fits in usize");

    tracing::info!(nodes_to_sync, "completing epoch sync for quorum");

    for cap in caps.iter().take(nodes_to_sync) {
        client.epoch_sync_done(epoch, cap.id).await?;
    }
    Ok(())
}

/// Attempts an epoch change with `n_nodes` nodes, replacing a `churn_fraction` of the committee.
///
/// With `churn_fraction = 1.0`, registers N replacement nodes with higher stake to fully displace
/// the original committee (worst case). With smaller fractions, only that portion is replaced.
///
/// Returns `Ok(())` if both `voting_end` and `initiate_epoch_change` succeed.
async fn try_committee_size(
    sui_cluster: Arc<Mutex<TestClusterHandle>>,
    n_nodes: usize,
    churn_fraction: f64,
) -> Result<()> {
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss
    )]
    let n_replacements = (n_nodes as f64 * churn_fraction).ceil() as usize;
    tracing::info!(
        n_nodes,
        n_replacements,
        churn_fraction,
        "=== testing committee size ==="
    );

    let admin_wallet = new_wallet_on_sui_test_cluster(sui_cluster).await?;

    let result = admin_wallet
        .and_then_async(async |wallet| {
            create_and_init_system_for_test(
                wallet,
                NonZeroU16::new(N_SHARDS).expect("number of shards is non-zero"),
                Duration::from_secs(0),
                Duration::from_secs(0),
                None,
                false,
                None,
                None,
            )
            .await
        })
        .await?;
    let (_system_context, contract_client) = result.inner;

    tracing::info!(n_nodes, "system initialized, registering set A");

    let (params_a, keys_a) = generate_node_params(n_nodes);
    let caps_a = register_nodes(&contract_client, &params_a, &keys_a, ONE_WAL).await?;

    tracing::info!(n_nodes, "set A registered, ending epoch 0");
    end_epoch_zero(&contract_client).await?;

    tracing::info!(n_nodes, "epoch 0 ended, completing epoch sync");
    complete_epoch_sync(&contract_client, 1, &caps_a, N_SHARDS).await?;

    tracing::info!(
        n_nodes,
        n_replacements,
        "epoch sync complete, registering replacement nodes"
    );

    // Register replacement nodes with much higher stake so they displace existing members
    // in the apportionment.
    let (params_b, keys_b) = generate_node_params(n_replacements);
    let _caps_b = register_nodes(&contract_client, &params_b, &keys_b, 100 * ONE_WAL).await?;

    tracing::info!(
        n_nodes,
        n_replacements,
        "replacement nodes registered, calling voting_end"
    );
    contract_client.voting_end().await?;

    tracing::info!(
        n_nodes,
        "voting_end succeeded, calling initiate_epoch_change"
    );
    contract_client.initiate_epoch_change().await?;

    let epoch = contract_client.current_epoch().await?;
    assert_eq!(epoch, 2, "expected epoch 2 after two epoch transitions");

    tracing::info!(
        n_nodes,
        n_replacements,
        "committee size {n_nodes} with churn {churn_fraction} passed"
    );
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    walrus_test_utils::init_tracing();

    let args = Args::parse();
    let sui_cluster = walrus_sui::test_utils::using_tokio::global_sui_test_cluster();

    let mut low = args.low;
    let mut high = args.high;
    let precision = args.precision;
    let churn = args.churn_fraction;

    println!(
        "binary search for max committee size in [{low}, {high}] (precision: {}, churn: {churn})",
        args.precision
    );

    // Verify lower bound.
    println!("verifying lower bound ({low})...");
    try_committee_size(sui_cluster.clone(), low, churn).await?;
    println!("lower bound {low} OK");

    while high - low > precision {
        let mid = (low + high) / 2;
        println!("trying {mid} (range: [{low}, {high}])...");

        match try_committee_size(sui_cluster.clone(), mid, churn).await {
            Ok(()) => {
                println!("  {mid} succeeded");
                low = mid;
            }
            Err(e) => {
                println!("  {mid} failed: {e}");
                high = mid;
            }
        }
    }

    if precision == 1 {
        println!("\nresult: max committee size is {low}");
    } else {
        println!("\nresult: max committee size is in range [{low}, {high})");
    }
    Ok(())
}

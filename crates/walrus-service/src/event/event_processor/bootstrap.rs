// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Bootstrap module for getting the initial committee and checkpoint information.
use anyhow::{Context as _, Result};
use sui_sdk::rpc_types::SuiTransactionBlockResponseOptions;
use sui_types::{
    base_types::ObjectID,
    committee::Committee,
    messages_checkpoint::VerifiedCheckpoint,
    sui_serde::BigInt,
};
use walrus_sui::client::retry_client::{RetriableRpcClient, RetriableSuiClient};

/// Gets the initial committee and checkpoint information by:
/// 1. Fetching the system package object
/// 2. Getting its previous transaction
/// 3. Using that transaction's checkpoint to get the committee and checkpoint data
///
/// Returns a tuple containing:
/// - The committee for the current or next epoch
/// - The verified checkpoint containing the system package deployment
pub async fn get_bootstrap_committee_and_checkpoint(
    sui_client: RetriableSuiClient,
    rpc_client: RetriableRpcClient,
    system_pkg_id: ObjectID,
) -> Result<(Committee, VerifiedCheckpoint)> {
    let txn_digest = sui_client.get_previous_transaction(system_pkg_id).await?;
    let txn_options = SuiTransactionBlockResponseOptions::new();
    let txn = sui_client
        .get_transaction_with_options(txn_digest, txn_options)
        .await?;
    let checkpoint_data = rpc_client
        .get_full_checkpoint(txn.checkpoint.context("No checkpoint data")?)
        .await?;
    let epoch = checkpoint_data.checkpoint_summary.epoch;
    let checkpoint_summary = checkpoint_data.checkpoint_summary.clone();
    let committee = if let Some(end_of_epoch_data) = &checkpoint_summary.end_of_epoch_data {
        let next_committee = end_of_epoch_data
            .next_epoch_committee
            .iter()
            .cloned()
            .collect();
        Committee::new(epoch + 1, next_committee)
    } else {
        let committee_info = sui_client
            .get_committee_info(Some(BigInt::from(epoch)))
            .await?;
        Committee::new(
            committee_info.epoch,
            committee_info.validators.into_iter().collect(),
        )
    };
    let verified_checkpoint = VerifiedCheckpoint::new_unchecked(checkpoint_summary);
    Ok((committee, verified_checkpoint))
}

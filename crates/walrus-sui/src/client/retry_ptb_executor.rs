// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! PTB executor for retrying RPC calls with backoff, in case there are retriable errors.

use rand::{
    rngs::{StdRng, ThreadRng},
    Rng,
};
#[cfg(msim)]
use sui_macros::fail_point_if;
use sui_sdk::{rpc_types::SuiTransactionBlockResponse, wallet_context::WalletContext};
#[cfg(msim)]
use sui_types::transaction::{Transaction, TransactionDataAPI};
use sui_types::{
    base_types::{ObjectRef, SuiAddress},
    transaction::{ProgrammableTransaction, TransactionData},
};
use walrus_utils::backoff::{ExponentialBackoff, ExponentialBackoffConfig};

use super::retry_client::retry_rpc_errors;

/// PTB executor that executes and retries upon receiving a retriable error.
#[derive(Debug)]
pub struct RetryPtbExecutor {
    /// The gas budget for the PTB.
    gas_budget: u64,
    /// The backoff configuration.
    backoff_config: ExponentialBackoffConfig,
}

impl RetryPtbExecutor {
    /// Creates a new `RetryPtbExecutor`.
    pub fn new(gas_budget: u64, backoff_config: ExponentialBackoffConfig) -> Self {
        Self {
            gas_budget,
            backoff_config,
        }
    }

    /// Gets a backoff strategy, seeded from the internal RNG.
    fn get_strategy(&self) -> ExponentialBackoff<StdRng> {
        self.backoff_config.get_strategy(ThreadRng::default().gen())
    }

    /// Sign and send a PTB.
    pub async fn sign_and_send_ptb(
        &self,
        sender: SuiAddress,
        wallet: &WalletContext,
        programmable_transaction: ProgrammableTransaction,
        gas_coins: Vec<ObjectRef>,
    ) -> anyhow::Result<SuiTransactionBlockResponse> {
        let gas_price = wallet.get_reference_gas_price().await?;

        let transaction = TransactionData::new_programmable(
            sender,
            gas_coins,
            programmable_transaction,
            self.gas_budget,
            gas_price,
        );

        let transaction = wallet.sign_transaction(&transaction);

        // Retry here must use the exact same transaction to avoid locked objects.
        retry_rpc_errors(self.get_strategy(), || async {
            #[cfg(msim)]
            {
                maybe_return_injected_error_in_stake_pool_transaction(&transaction)?;
            }
            wallet
                .execute_transaction_may_fail(transaction.clone())
                .await
        })
        .await
    }
}

/// Injects a simulated error for testing retry behavior executing sui transactions.
/// We use stake_with_pool as an example here to incorporate with the test logic in
/// `test_ptb_executor_retriable_error` in `test_client.rs`.
#[cfg(msim)]
fn maybe_return_injected_error_in_stake_pool_transaction(
    transaction: &Transaction,
) -> anyhow::Result<()> {
    // Check if this transaction contains a stake_with_pool operation
    let is_stake_pool_tx = transaction
        .transaction_data()
        .move_calls()
        .iter()
        .any(|(_, _, function_name)| *function_name == "stake_with_pool");

    // Early return if this isn't a stake pool transaction
    if !is_stake_pool_tx {
        return Ok(());
    }

    // Check if we should inject an error via the fail point
    let mut should_inject_error = false;
    fail_point_if!("ptb_executor_stake_pool_retriable_error", || {
        should_inject_error = true;
    });

    if should_inject_error {
        tracing::warn!("Injecting a retriable RPC error for stake pool transaction");

        // Simulate a retriable RPC error (502 Bad Gateway)
        Err(sui_sdk::error::Error::RpcError(
            anyhow::anyhow!("Server returned an error status code: 502").into(),
        ))?;
    }

    Ok(())
}

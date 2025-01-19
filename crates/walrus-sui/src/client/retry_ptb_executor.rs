// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! PTB executor for retrying RPC calls with backoff, in case there are retriable errors.

use rand::{
    rngs::{StdRng, ThreadRng},
    Rng,
};
use sui_sdk::{rpc_types::SuiTransactionBlockResponse, wallet_context::WalletContext};
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
            wallet
                .execute_transaction_may_fail(transaction.clone())
                .await
        })
        .await
    }
}

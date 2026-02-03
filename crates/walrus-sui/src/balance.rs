// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! A module defining the Balance struct.

use anyhow::Context;

use crate::coin::Coin;

/// An error type for balance retrieval errors.
#[derive(Debug, thiserror::Error)]
#[error("error retrieving balance information: {0}")]
pub struct BalanceRetrievalError(anyhow::Error);

impl From<anyhow::Error> for BalanceRetrievalError {
    fn from(err: anyhow::Error) -> Self {
        BalanceRetrievalError(err)
    }
}

/// A struct representing the balance of a specific coin type.
#[derive(Debug, Clone)]
pub struct Balance {
    /// The total balance across all coin objects.
    total_balance: u64,
    /// The number of coin objects or the actual coin objects.
    coin_balance: CoinBalance,
}

/// An enum representing either the count of coin objects or the actual coin objects.
#[derive(Debug, Clone)]
pub enum CoinBalance {
    /// The count of coin objects.
    Count(usize),
    /// The actual coin objects.
    Coins(Vec<Coin>),
}

impl Balance {
    /// Creates a new Balance from a coin type and a list of coins.
    pub fn try_from_coins(coins: Vec<Coin>) -> Result<Self, BalanceRetrievalError> {
        let total_balance: u128 = coins.iter().map(|coin| u128::from(coin.balance)).sum();
        Ok(Self {
            total_balance: u64::try_from(total_balance).context("total balance exceeds u64")?,
            coin_balance: CoinBalance::Coins(coins),
        })
    }

    /// Returns the number of coin objects.
    pub fn coin_object_count(&self) -> usize {
        match self.coin_balance {
            CoinBalance::Count(count) => count,
            CoinBalance::Coins(ref coins) => coins.len(),
        }
    }

    /// Returns the total balance.
    pub fn total_balance(&self) -> u64 {
        self.total_balance
    }

    /// Take the coins if they are available.
    pub fn coins(self) -> Option<Vec<Coin>> {
        match self.coin_balance {
            CoinBalance::Count(_) => None,
            CoinBalance::Coins(coins) => Some(coins),
        }
    }
}

impl TryFrom<sui_sdk::rpc_types::Balance> for Balance {
    type Error = BalanceRetrievalError;

    fn try_from(balance: sui_sdk::rpc_types::Balance) -> Result<Self, BalanceRetrievalError> {
        Ok(Self {
            coin_balance: CoinBalance::Count(balance.coin_object_count),
            total_balance: u64::try_from(balance.total_balance)
                .context("total balance exceeds u64")?,
        })
    }
}

// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/// A utility module which implements an `ExchangeRate` struct and its methods.
/// It stores a fixed point exchange rate between the Wal token and pool token.
module walrus::pool_exchange_rate;

// Error codes
// Error types in `walrus-sui/types/move_errors.rs` are auto-generated from the Move error codes.
const EInvalidRate: u64 = 0;

/// Represents the exchange rate for the staking pool.
public enum PoolExchangeRate has copy, drop, store {
    Flat, // one to one exchange rate
    Variable {
        /// Amount of staked WAL tokens + rewards.
        wal_amount: u128,
        /// Amount of total tokens in the pool (<= wal_amount).
        pool_token_amount: u128,
    },
}

/// Create an empty exchange rate.
public(package) fun flat(): PoolExchangeRate {
    PoolExchangeRate::Flat
}

/// Create a new exchange rate with the given amounts.
public(package) fun new(wal_amount: u64, pool_token_amount: u64): PoolExchangeRate {
    assert!(pool_token_amount <= wal_amount, EInvalidRate);
    match (wal_amount) {
        0 => PoolExchangeRate::Flat,
        _ => {
            PoolExchangeRate::Variable {
                wal_amount: (wal_amount as u128),
                pool_token_amount: (pool_token_amount as u128),
            }
        },
    }
}

/// Assumptions:
/// - amount is at most the amount of pool tokens in the pool
public(package) fun convert_to_wal_amount(exchange_rate: &PoolExchangeRate, amount: u64): u64 {
    match (exchange_rate) {
        PoolExchangeRate::Flat => amount,
        PoolExchangeRate::Variable { wal_amount, pool_token_amount } => {
            let amount = (amount as u128);
            let res = (amount * *wal_amount) / *pool_token_amount;
            res as u64
        },
    }
}

/// Assumptions:
/// - amount is at most the amount of WAL in the pool
public(package) fun convert_to_token_amount(exchange_rate: &PoolExchangeRate, amount: u64): u64 {
    match (exchange_rate) {
        PoolExchangeRate::Flat => amount,
        PoolExchangeRate::Variable { wal_amount, pool_token_amount } => {
            let amount = (amount as u128);
            let res = (amount * *pool_token_amount) / *wal_amount;
            res as u64
        },
    }
}

// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/// Coin stash implementation for BlobManager.
/// Allows anyone to deposit SUI and WAL coins that can be used by anyone to extend or buy storage.
/// The coin stash exists independently and can survive BlobManager destruction.
module walrus::coin_stash;

use sui::{balance::{Self, Balance}, coin::{Self, Coin}, sui::SUI};
use wal::wal::WAL;

// === Error Codes ===

/// Insufficient WAL balance in the stash.
const EInsufficientWalBalance: u64 = 1;
/// Insufficient SUI balance in the stash.
const EInsufficientSuiBalance: u64 = 2;
/// Invalid withdrawal amount (zero or negative).
const EInvalidWithdrawalAmount: u64 = 3;

// === Main Struct ===

/// Coin stash embedded within BlobManager for community funding.
/// Simplified design without key ability since BlobManager is already shared.
public struct BlobManagerCoinStash has store {
    /// WAL token balance for storage payments.
    wal_balance: Balance<WAL>,
    /// SUI token balance for gas payments.
    sui_balance: Balance<SUI>,
}

// === Constructor ===

/// Creates a new coin stash for a BlobManager.
/// Returns the coin stash to be embedded in BlobManager.
/// TODO(heliu): do we need SUI? Discuss with the product team.
public fun new(): BlobManagerCoinStash {
    BlobManagerCoinStash {
        wal_balance: balance::zero<WAL>(),
        sui_balance: balance::zero<SUI>(),
    }
}

// === Public Deposit Functions (Anyone Can Deposit) ===

/// Deposits WAL coins to the stash.
/// Anyone can call this to contribute funds.
public fun deposit_wal(self: &mut BlobManagerCoinStash, payment: Coin<WAL>) {
    let amount = payment.value();
    if (amount == 0) {
        // Destroy empty coin and return.
        coin::destroy_zero(payment);
        return
    };

    // Add to balance.
    self.wal_balance.join(payment.into_balance());
}

/// Deposits SUI coins to the stash.
/// Anyone can call this to contribute funds.
public fun deposit_sui(self: &mut BlobManagerCoinStash, payment: Coin<SUI>) {
    let amount = payment.value();
    if (amount == 0) {
        // Destroy empty coin and return.
        coin::destroy_zero(payment);
        return
    };

    // Add to balance.
    self.sui_balance.join(payment.into_balance());
}

// === Public Withdrawal Functions (For Storage Operations) ===

/// Withdraws WAL for storage operations.
/// Anyone can use these funds for valid storage operations.
public fun withdraw_wal_for_storage(
    self: &mut BlobManagerCoinStash,
    amount: u64,
    ctx: &mut TxContext,
): Coin<WAL> {
    assert!(amount > 0, EInvalidWithdrawalAmount);
    assert!(self.wal_balance.value() >= amount, EInsufficientWalBalance);

    coin::from_balance(self.wal_balance.split(amount), ctx)
}

/// Withdraws SUI for gas operations.
/// Anyone can use these funds for gas operations.
public fun withdraw_sui_for_gas(
    self: &mut BlobManagerCoinStash,
    amount: u64,
    ctx: &mut TxContext,
): Coin<SUI> {
    assert!(amount > 0, EInvalidWithdrawalAmount);
    assert!(self.sui_balance.value() >= amount, EInsufficientSuiBalance);

    coin::from_balance(self.sui_balance.split(amount), ctx)
}

// === Fund Manager Functions ===

/// Withdraws all WAL funds from the stash.
/// Only callable through BlobManager with fund_manager permission.
public fun withdraw_all_wal(self: &mut BlobManagerCoinStash, ctx: &mut TxContext): Coin<WAL> {
    let amount = self.wal_balance.value();
    if (amount == 0) {
        coin::zero<WAL>(ctx)
    } else {
        coin::from_balance(self.wal_balance.split(amount), ctx)
    }
}

/// Withdraws all SUI funds from the stash.
/// Only callable through BlobManager with fund_manager permission.
public fun withdraw_all_sui(self: &mut BlobManagerCoinStash, ctx: &mut TxContext): Coin<SUI> {
    let amount = self.sui_balance.value();
    if (amount == 0) {
        coin::zero<SUI>(ctx)
    } else {
        coin::from_balance(self.sui_balance.split(amount), ctx)
    }
}

// === Query Functions ===

/// Checks if there is sufficient WAL balance.
public fun has_sufficient_wal(self: &BlobManagerCoinStash, amount: u64): bool {
    self.wal_balance.value() >= amount
}

/// Checks if there is sufficient SUI balance.
public fun has_sufficient_sui(self: &BlobManagerCoinStash, amount: u64): bool {
    self.sui_balance.value() >= amount
}

/// Gets the WAL balance.
public fun wal_balance(self: &BlobManagerCoinStash): u64 {
    self.wal_balance.value()
}

/// Gets the SUI balance.
public fun sui_balance(self: &BlobManagerCoinStash): u64 {
    self.sui_balance.value()
}

/// Gets both balances at once.
public fun balances(self: &BlobManagerCoinStash): (u64, u64) {
    (self.wal_balance.value(), self.sui_balance.value())
}

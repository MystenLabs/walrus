// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/// Module: exchange
module wal_exchange::exchange;

use sui::{balance::{Self, Balance}, coin::Coin, sui::SUI};
use wal::wal::WAL;

#[error]
const EInsufficientFundsInExchange: vector<u8> =
    b"The exchange has insufficient funds to perform the exchange";

#[error]
const EInsufficientInputBalance: vector<u8> =
    b"The provided coin has insufficient balance for the exchange";

/// A public exchange that allows exchanging SUI for WAL at a fixed exchange rate.
public struct Exchange has key, store {
    id: UID,
    wal: Balance<WAL>,
    sui: Balance<SUI>,
    wal_per_sui: u64,
}

/// Creates a new shared exchange with the given exchange rate.
public fun new(wal_per_sui: u64, ctx: &mut TxContext) {
    transfer::share_object(Exchange {
        id: object::new(ctx),
        wal: balance::zero(),
        sui: balance::zero(),
        wal_per_sui,
    });
}

/// Adds WAL to the balance stored in the exchange.
public fun add_wal(self: &mut Exchange, wal: Coin<WAL>) {
    self.wal.join(wal.into_balance());
}

/// Adds SUI to the balance stored in the exchange.
public fun add_sui(self: &mut Exchange, sui: Coin<SUI>) {
    self.sui.join(sui.into_balance());
}

/// Exchanges the provided SUI coin for WAL at the exchange's rate.
public fun exchange_all_for_wal(
    self: &mut Exchange,
    sui: Coin<SUI>,
    ctx: &mut TxContext,
): Coin<WAL> {
    let value_wal = sui.value() * self.wal_per_sui;
    assert!(self.wal.value() >= value_wal, EInsufficientFundsInExchange);
    self.sui.join(sui.into_balance());
    self.wal.split(value_wal).into_coin(ctx)
}

/// Exchanges `amount_sui` out of the provided SUI coin for WAL at the exchange's rate.
public fun exchange_for_wal(
    self: &mut Exchange,
    sui: &mut Coin<SUI>,
    amount_sui: u64,
    ctx: &mut TxContext,
): Coin<WAL> {
    assert!(sui.value() >= amount_sui, EInsufficientInputBalance);
    self.exchange_all_for_wal(sui.split(amount_sui, ctx), ctx)
}

/// Exchanges the provided WAL coin for SUI at the exchange's rate.
public fun exchange_all_for_sui(
    self: &mut Exchange,
    wal: Coin<WAL>,
    ctx: &mut TxContext,
): Coin<SUI> {
    let value_sui = wal.value() / self.wal_per_sui;
    assert!(self.sui.value() >= value_sui, EInsufficientFundsInExchange);
    self.wal.join(wal.into_balance());
    self.sui.split(value_sui).into_coin(ctx)
}

/// Exchanges `amount_wal` out of the provided WAL coin for SUI at the exchange's rate.
public fun exchange_for_sui(
    self: &mut Exchange,
    wal: &mut Coin<WAL>,
    amount_wal: u64,
    ctx: &mut TxContext,
): Coin<SUI> {
    assert!(wal.value() >= amount_wal, EInsufficientInputBalance);
    self.exchange_all_for_sui(wal.split(amount_wal, ctx), ctx)
}

// === Tests ===

#[test_only]
use sui::coin;
#[test_only]
use sui::test_utils::destroy;

#[test_only]
fun new_for_testing(wal_per_sui: u64, ctx: &mut TxContext): Exchange {
    Exchange {
        id: object::new(ctx),
        wal: balance::zero(),
        sui: balance::zero(),
        wal_per_sui,
    }
}

#[test]
fun test_standard_flow() {
    let ctx = &mut tx_context::dummy();
    let mut exchange = new_for_testing(2, ctx);

    exchange.add_wal(coin::mint_for_testing(1_000_000, ctx));
    exchange.add_sui(coin::mint_for_testing(1_000_000, ctx));

    let mut wal_coin = exchange.exchange_all_for_wal(coin::mint_for_testing(42, ctx), ctx);
    assert!(wal_coin.value() == 84);
    assert!(exchange.sui.value() == 1_000_042);
    assert!(exchange.wal.value() == 999_916);

    let mut sui_coin = exchange.exchange_for_sui(&mut wal_coin, 9, ctx);
    assert!(sui_coin.value() == 4);
    assert!(wal_coin.value() == 75);

    let wal_coin_2 = exchange.exchange_for_wal(&mut sui_coin, 2, ctx);
    assert!(wal_coin_2.value() == 4);
    assert!(sui_coin.value() == 2);

    destroy(wal_coin);
    destroy(wal_coin_2);
    destroy(sui_coin);
    destroy(exchange);
}

#[test]
#[expected_failure(abort_code = EInsufficientFundsInExchange)]
fun test_insufficient_funds_in_exchange() {
    let ctx = &mut tx_context::dummy();
    let mut exchange = new_for_testing(2, ctx);

    exchange.add_sui(coin::mint_for_testing(1_000_000, ctx));
    let wal_coin = exchange.exchange_all_for_wal(coin::mint_for_testing(1, ctx), ctx);

    destroy(wal_coin);
    destroy(exchange);
}

#[test]
#[expected_failure(abort_code = EInsufficientInputBalance)]
fun test_insufficient_coin() {
    let ctx = &mut tx_context::dummy();
    let mut exchange = new_for_testing(2, ctx);

    exchange.add_sui(coin::mint_for_testing(1_000_000, ctx));
    let mut sui_coin = coin::mint_for_testing(1, ctx);
    let wal_coin = exchange.exchange_for_wal(&mut sui_coin, 2, ctx);

    destroy(sui_coin);
    destroy(wal_coin);
    destroy(exchange);
}

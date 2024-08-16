// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/// Common test utilities for the tests.
module walrus::test_utils;

use sui::{
    balance::{Self, Balance},
    bls12381::{Self, bls12381_min_pk_verify},
    coin::{Self, Coin},
    sui::SUI
};
use walrus::{staking_pool::{Self, StakingPool}, walrus_context::{Self, WalrusContext}};

// === Coins and Context ===

public fun wctx(epoch: u64, committee_selected: bool): WalrusContext {
    walrus_context::new(epoch, committee_selected)
}

public fun mint(amount: u64, ctx: &mut TxContext): Coin<SUI> {
    coin::mint_for_testing(amount, ctx)
}

public fun mint_balance(amount: u64): Balance<SUI> {
    balance::create_for_testing(amount)
}

// === Pool Builder ===

/// Struct to support building a staking pool in tests with variable parameters.
public struct PoolBuilder has copy, drop {
    commission_rate: Option<u64>,
    storage_price: Option<u64>,
    write_price: Option<u64>,
    node_capacity: Option<u64>,
}

/// Test Utility: Creates a new `PoolBuilder` with default values.
///
/// ```rust
/// // Example usage:
/// let pool_a = pool().commission_rate(1000).build(&wctx, ctx);
/// let pool_b = pool().write_price(1000).storage_price(1000).build(&wctx, ctx);
/// let pool_c = pool()
///     .commission_rate(1000)
///     .storage_price(1000)
///     .write_price(1000)
///     .node_capacity(1000)
///     .build(&wctx, ctx);
/// ```
public fun pool(): PoolBuilder {
    PoolBuilder {
        commission_rate: option::none(),
        storage_price: option::none(),
        write_price: option::none(),
        node_capacity: option::none(),
    }
}

public fun commission_rate(mut self: PoolBuilder, commission_rate: u64): PoolBuilder {
    self.commission_rate.fill(commission_rate);
    self
}

public fun storage_price(mut self: PoolBuilder, storage_price: u64): PoolBuilder {
    self.storage_price.fill(storage_price);
    self
}

public fun write_price(mut self: PoolBuilder, write_price: u64): PoolBuilder {
    self.write_price.fill(write_price);
    self
}

public fun node_capacity(mut self: PoolBuilder, node_capacity: u64): PoolBuilder {
    self.node_capacity.fill(node_capacity);
    self
}

public fun build(self: PoolBuilder, wctx: &WalrusContext, ctx: &mut TxContext): StakingPool {
    let PoolBuilder {
        commission_rate,
        storage_price,
        write_price,
        node_capacity,
    } = self;

    staking_pool::new(
        commission_rate.destroy_with_default(1000),
        storage_price.destroy_with_default(1000),
        write_price.destroy_with_default(1000),
        node_capacity.destroy_with_default(1000),
        wctx,
        ctx,
    )
}

// == BLS Helpers ==

public fun bls_min_pk_sign(msg: &vector<u8>, sk: &vector<u8>): vector<u8> {
    let sk_element = bls12381::scalar_from_bytes(sk);
    let hashed_msg = bls12381::hash_to_g2(msg);
    let sig = bls12381::g2_mul(&sk_element, &hashed_msg);
    *sig.bytes()
}

public fun bls_min_pk_from_sk(sk: &vector<u8>): vector<u8> {
    let sk_element = bls12381::scalar_from_bytes(sk);
    let g1 = bls12381::g1_generator();
    let pk = bls12381::g1_mul(&sk_element, &g1);
    *pk.bytes()
}

// == Unit Tests ==

#[test]
fun test_bls_pk() {
    let sk = x"0000000000000000000000000000000000000000000000000000000000000075";
    let pub_key_bytes = x"95eacc3adc09c827593f581e8e2de068bf4cf5d0c0eb29e5372f0d23364788ee0f9beb112c8a7e9c2f0c720433705cf0"; // editorconfig-checker-disable-line
    assert!(bls_min_pk_from_sk(&sk) == pub_key_bytes)
}

#[test]
fun test_bls_sign() {
    let sk = x"0000000000000000000000000000000000000000000000000000000000000075";
    let pub_key_bytes = bls_min_pk_from_sk(&sk);
    let msg = x"deadbeef";
    let sig = bls_min_pk_sign(&msg, &sk);

    assert!(
        bls12381_min_pk_verify(
            &sig,
            &pub_key_bytes,
            &msg,
        ),
    );
}

// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
// editorconfig-checker-disable-file

/// Common test utilities for the tests.
module walrus::test_utils;

use std::string::String;
use sui::{
    balance::{Self, Balance},
    bls12381::{Self, bls12381_min_pk_verify},
    coin::{Self, Coin},
    sui::SUI,
    vec_map
};
use walrus::{
    staking_inner::StakingInnerV1,
    staking_pool::{Self, StakingPool},
    walrus_context::{Self, WalrusContext},
    bls_aggregate,
};

// === Coins and Context ===

public fun wctx(epoch: u32, committee_selected: bool): WalrusContext {
    walrus_context::new(epoch, committee_selected, vec_map::empty())
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
    name: Option<String>,
    network_address: Option<String>,
    public_key: Option<vector<u8>>,
    network_public_key: Option<vector<u8>>,
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
///     .name(b"my node".to_string())
///     .network_address(b"0.0.0.0".to_string())
///     .public_key(x"a60e75190e62b6a54142d147289a735c4ce11a9d997543da539a3db57def5ed83ba40b74e55065f02b35aa1d504c404b")
///     .network_public_key(x"820e2b273530a00de66c9727c40f48be985da684286983f398ef7695b8a44677ab")
///     .commission_rate(1000)
///     .storage_price(1000)
///     .write_price(1000)
///     .node_capacity(1000)
///     .build(&wctx, ctx);
/// ```
public fun pool(): PoolBuilder {
    PoolBuilder {
        name: option::none(),
        network_address: option::none(),
        public_key: option::none(),
        network_public_key: option::none(),
        commission_rate: option::none(),
        storage_price: option::none(),
        write_price: option::none(),
        node_capacity: option::none(),
    }
}

/// Sets the commission rate for the pool.
public fun commission_rate(mut self: PoolBuilder, commission_rate: u64): PoolBuilder {
    self.commission_rate.fill(commission_rate);
    self
}

/// Sets the storage price for the pool.
public fun storage_price(mut self: PoolBuilder, storage_price: u64): PoolBuilder {
    self.storage_price.fill(storage_price);
    self
}

/// Sets the write price for the pool.
public fun write_price(mut self: PoolBuilder, write_price: u64): PoolBuilder {
    self.write_price.fill(write_price);
    self
}

/// Sets the node capacity for the pool.
public fun node_capacity(mut self: PoolBuilder, node_capacity: u64): PoolBuilder {
    self.node_capacity.fill(node_capacity);
    self
}

/// Sets the name for the pool.
public fun name(mut self: PoolBuilder, name: String): PoolBuilder {
    self.name.fill(name);
    self
}

/// Sets the network address for the pool.
public fun network_address(mut self: PoolBuilder, network_address: String): PoolBuilder {
    self.network_address.fill(network_address);
    self
}

/// Sets the public key for the pool.
public fun public_key(mut self: PoolBuilder, public_key: vector<u8>): PoolBuilder {
    self.public_key.fill(public_key);
    self
}

/// Sets the network public key for the pool.
public fun network_public_key(mut self: PoolBuilder, network_public_key: vector<u8>): PoolBuilder {
    self.network_public_key.fill(network_public_key);
    self
}

/// Builds a staking pool with the parameters set in the builder.
public fun build(self: PoolBuilder, wctx: &WalrusContext, ctx: &mut TxContext): StakingPool {
    let PoolBuilder {
        name,
        network_address,
        public_key,
        network_public_key,
        commission_rate,
        storage_price,
        write_price,
        node_capacity,
    } = self;

    staking_pool::new(
        name.destroy_with_default(b"pool".to_string()),
        network_address.destroy_with_default(b"127.0.0.1".to_string()),
        public_key.destroy_with_default(
            x"a60e75190e62b6a54142d147289a735c4ce11a9d997543da539a3db57def5ed83ba40b74e55065f02b35aa1d504c404b",
        ),
        network_public_key.destroy_with_default(
            x"820e2b273530a00de66c9727c40f48be985da684286983f398ef7695b8a44677ab",
        ),
        commission_rate.destroy_with_default(1000),
        storage_price.destroy_with_default(1000),
        write_price.destroy_with_default(1000),
        node_capacity.destroy_with_default(1000),
        wctx,
        ctx,
    )
}

/// Similar to `build` but registers the pool with the staking inner, using the same set of
/// parameters.
public fun register(self: PoolBuilder, inner: &mut StakingInnerV1, ctx: &mut TxContext): ID {
    let PoolBuilder {
        name,
        network_address,
        public_key,
        network_public_key,
        commission_rate,
        storage_price,
        write_price,
        node_capacity,
    } = self;

    inner.create_pool(
        name.destroy_with_default(b"pool".to_string()),
        network_address.destroy_with_default(b"127.0.0.1".to_string()),
        public_key.destroy_with_default(
            x"a60e75190e62b6a54142d147289a735c4ce11a9d997543da539a3db57def5ed83ba40b74e55065f02b35aa1d504c404b",
        ),
        network_public_key.destroy_with_default(
            x"820e2b273530a00de66c9727c40f48be985da684286983f398ef7695b8a44677ab",
        ),
        commission_rate.destroy_with_default(1000),
        storage_price.destroy_with_default(1000),
        write_price.destroy_with_default(1000),
        node_capacity.destroy_with_default(1000),
        ctx,
    )
}

// === BLS Helpers ===

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

// Prepends the key with zeros to get 32 bytes.
public fun pad_bls_sk(sk: &vector<u8>): vector<u8> {
    let mut sk = *sk;
    if (sk.length() < 32) {
        // Prepend with zeros to get 32 bytes.
        sk.reverse();
        (32 - sk.length()).do!(|_| sk.push_back(0));
        sk.reverse();
    };
    sk
}

/// Returns the secret key scalar 117
public fun bls_sk_for_testing(): vector<u8> {
    pad_bls_sk(&x"75")
}

/// Returns 10 bls secret keys.
public fun bls_secret_keys_for_testing(): vector<vector<u8>> {
    vector[
        x"DEADBEEF",
        x"BEEF",
        x"CAFE",
        x"C0DE",
        x"C0FFEE",
        x"DEAD",
        x"DECADE",
        x"DEC0DE",
        x"FACE",
        x"F00D",
    ].map!(|key| pad_bls_sk(&key))
}

/// Aggregates the given signatures into one signature.
public fun bls_aggregate_sigs(signatures: &vector<vector<u8>>): vector<u8> {
    let mut aggregate = bls12381::g2_identity();
    signatures.do_ref!(
        |sig| aggregate = bls12381::g2_add(&aggregate, &bls12381::g2_from_bytes(sig)),
    );
    *aggregate.bytes()
}

/// Test committee with one committee member and 100 shards, using
/// `test_utils::bls_sk_for_testing()` as secret key.
public fun new_bls_committee_for_testing(epoch: u32): bls_aggregate::BlsCommittee {
    let node_id = tx_context::dummy().fresh_object_address().to_id();
    let sk = bls_sk_for_testing();
    let pub_key = bls12381::g1_from_bytes(&bls_min_pk_from_sk(&sk));
    let member = bls_aggregate::new_bls_committee_member(pub_key, 100, node_id);
    bls_aggregate::new_bls_committee(epoch, vector[member])
}

// === Unit Tests ===

#[test]
fun test_bls_pk() {
    let sk = bls_sk_for_testing();
    let pub_key_bytes =
        x"95eacc3adc09c827593f581e8e2de068bf4cf5d0c0eb29e5372f0d23364788ee0f9beb112c8a7e9c2f0c720433705cf0";
    assert!(bls_min_pk_from_sk(&sk) == pub_key_bytes)
}

#[test]
fun test_bls_sign() {
    let sk = bls_sk_for_testing();
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

// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/// Common test utilities for the tests.
module walrus::test_utils;

use sui::{coin::{Self, Coin}, sui::SUI, balance::{Self, Balance}};
use walrus::walrus_context::{Self, WalrusContext};

public fun wctx(epoch: u64, committee_selected: bool): WalrusContext {
    walrus_context::new(epoch, committee_selected)
}

public fun mint(amount: u64, ctx: &mut TxContext): Coin<SUI> {
    coin::mint_for_testing(amount, ctx)
}

public fun mint_balance(amount: u64): Balance<SUI> {
    balance::create_for_testing(amount)
}

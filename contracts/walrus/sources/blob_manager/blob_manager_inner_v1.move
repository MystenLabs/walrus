// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/// BlobManager's inner state, backed by `StoragePool`.
module walrus::blob_manager_inner_v1;

use sui::{balance::{Self, Balance}, coin::{Self, Coin}, sui::SUI};
use wal::wal::WAL;
use walrus::{storage_pool::StoragePool, system::System};

/// The storage pool backing this blob manager has expired.
const EBlobManagerStorageExpired: u64 = 0;
/// The blob manager coin stash does not have enough WAL for the requested operation.
const EInsufficientWalBalance: u64 = 1;
/// The blob manager coin stash does not have enough SUI for the requested operation.
const EInsufficientSuiBalance: u64 = 2;
/// The blob already exists with a different deletable flag.
const EBlobPermanencyConflict: u64 = 3;
/// Withdrawal amounts must be positive.
const EInvalidWithdrawalAmount: u64 = 4;

public struct BlobManagerInnerV1 has store {
    storage_pool: StoragePool,
    coin_stash: BlobManagerCoinStash,
}

public struct BlobManagerCoinStash has store {
    wal_balance: Balance<WAL>,
    sui_balance: Balance<SUI>,
}

public(package) fun new(
    storage_pool: StoragePool,
    initial_wal: Coin<WAL>,
    _ctx: &mut TxContext,
): BlobManagerInnerV1 {
    let mut inner = BlobManagerInnerV1 {
        storage_pool,
        coin_stash: new_coin_stash(),
    };
    coin_stash_deposit_wal(&mut inner.coin_stash, initial_wal);
    inner
}

public(package) fun register_blob(
    self: &mut BlobManagerInnerV1,
    system: &mut System,
    blob_id: u256,
    root_hash: u256,
    unencoded_size: u64,
    encoding_type: u8,
    deletable: bool,
    ctx: &mut TxContext,
) {
    verify_pool_active(self, system);

    if (self.storage_pool.contains_blob(blob_id)) {
        assert!(
            self.storage_pool.borrow_blob(blob_id).is_deletable() == deletable,
            EBlobPermanencyConflict,
        );
        return
    };

    let mut payment = withdraw_all_wal(&mut self.coin_stash, ctx);
    system.register_pooled_blob(
        &mut self.storage_pool,
        blob_id,
        root_hash,
        unencoded_size,
        encoding_type,
        deletable,
        &mut payment,
        ctx,
    );
    coin_stash_deposit_wal(&mut self.coin_stash, payment);
}

public(package) fun certify_blob(
    self: &mut BlobManagerInnerV1,
    system: &System,
    blob_id: u256,
    signature: vector<u8>,
    signers_bitmap: vector<u8>,
    message: vector<u8>,
) {
    verify_pool_active(self, system);

    if (self.storage_pool.borrow_blob(blob_id).is_certified()) {
        return
    };

    system.certify_pooled_blob(
        &mut self.storage_pool,
        blob_id,
        signature,
        signers_bitmap,
        message,
    );
}

public(package) fun delete_blob(self: &mut BlobManagerInnerV1, system: &System, blob_id: u256) {
    verify_pool_active(self, system);
    system.delete_pooled_blob(&mut self.storage_pool, blob_id);
}

public(package) fun deposit_wal_to_coin_stash(self: &mut BlobManagerInnerV1, payment: Coin<WAL>) {
    coin_stash_deposit_wal(&mut self.coin_stash, payment);
}

public(package) fun deposit_sui_to_coin_stash(self: &mut BlobManagerInnerV1, payment: Coin<SUI>) {
    coin_stash_deposit_sui(&mut self.coin_stash, payment);
}

public(package) fun withdraw_wal(
    self: &mut BlobManagerInnerV1,
    amount: u64,
    ctx: &mut TxContext,
): Coin<WAL> {
    coin_stash_withdraw_wal(&mut self.coin_stash, amount, ctx)
}

public(package) fun withdraw_sui(
    self: &mut BlobManagerInnerV1,
    amount: u64,
    ctx: &mut TxContext,
): Coin<SUI> {
    coin_stash_withdraw_sui(&mut self.coin_stash, amount, ctx)
}

public(package) fun has_blob(self: &BlobManagerInnerV1, blob_id: u256): bool {
    self.storage_pool.contains_blob(blob_id)
}

public(package) fun get_blob_object_id(self: &BlobManagerInnerV1, blob_id: u256): ID {
    self.storage_pool.blob_object_id(blob_id)
}

public(package) fun end_epoch(self: &BlobManagerInnerV1): u32 {
    self.storage_pool.end_epoch()
}

public(package) fun reserved_encoded_capacity_bytes(self: &BlobManagerInnerV1): u64 {
    self.storage_pool.reserved_encoded_capacity_bytes()
}

public(package) fun used_encoded_bytes(self: &BlobManagerInnerV1): u64 {
    self.storage_pool.used_encoded_bytes()
}

public(package) fun available_encoded_bytes(self: &BlobManagerInnerV1): u64 {
    self.storage_pool.available_encoded_bytes()
}

public(package) fun blob_count(self: &BlobManagerInnerV1): u64 {
    self.storage_pool.blob_count()
}

public(package) fun wal_balance(self: &BlobManagerInnerV1): u64 {
    coin_stash_wal_balance(&self.coin_stash)
}

public(package) fun sui_balance(self: &BlobManagerInnerV1): u64 {
    coin_stash_sui_balance(&self.coin_stash)
}

public(package) fun storage_pool_id(self: &BlobManagerInnerV1): ID {
    self.storage_pool.object_id()
}

fun verify_pool_active(self: &BlobManagerInnerV1, system: &System) {
    assert!(self.storage_pool.end_epoch() > system.epoch(), EBlobManagerStorageExpired);
}

fun new_coin_stash(): BlobManagerCoinStash {
    BlobManagerCoinStash {
        wal_balance: balance::zero(),
        sui_balance: balance::zero(),
    }
}

fun coin_stash_deposit_wal(self: &mut BlobManagerCoinStash, payment: Coin<WAL>) {
    if (payment.value() == 0) {
        coin::destroy_zero(payment);
        return
    };
    self.wal_balance.join(payment.into_balance());
}

fun coin_stash_deposit_sui(self: &mut BlobManagerCoinStash, payment: Coin<SUI>) {
    if (payment.value() == 0) {
        coin::destroy_zero(payment);
        return
    };
    self.sui_balance.join(payment.into_balance());
}

fun coin_stash_withdraw_wal(
    self: &mut BlobManagerCoinStash,
    amount: u64,
    ctx: &mut TxContext,
): Coin<WAL> {
    assert!(amount > 0, EInvalidWithdrawalAmount);
    assert!(self.wal_balance.value() >= amount, EInsufficientWalBalance);
    coin::from_balance(self.wal_balance.split(amount), ctx)
}

fun coin_stash_withdraw_sui(
    self: &mut BlobManagerCoinStash,
    amount: u64,
    ctx: &mut TxContext,
): Coin<SUI> {
    assert!(amount > 0, EInvalidWithdrawalAmount);
    assert!(self.sui_balance.value() >= amount, EInsufficientSuiBalance);
    coin::from_balance(self.sui_balance.split(amount), ctx)
}

fun coin_stash_wal_balance(self: &BlobManagerCoinStash): u64 {
    self.wal_balance.value()
}

fun coin_stash_sui_balance(self: &BlobManagerCoinStash): u64 {
    self.sui_balance.value()
}

fun withdraw_all_wal(self: &mut BlobManagerCoinStash, ctx: &mut TxContext): Coin<WAL> {
    let amount = coin_stash_wal_balance(self);
    assert!(amount > 0, EInsufficientWalBalance);
    coin_stash_withdraw_wal(self, amount, ctx)
}

#[test_only]
public fun destroy_for_testing(self: BlobManagerInnerV1) {
    std::unit_test::destroy(self);
}

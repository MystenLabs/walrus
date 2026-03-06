// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/// BlobManager built on top of `StoragePool`.
module walrus::blob_manager;

use sui::{coin::Coin, dynamic_field, sui::SUI};
use wal::wal::WAL;
use walrus::{
    blob_manager_inner_v1::{Self, BlobManagerInnerV1},
    storage_pool::StoragePool,
    system::System
};

const VERSION: u64 = 1;

/// The provided capability does not belong to this blob manager.
const EInvalidBlobManagerCap: u64 = 0;
/// The blob manager object version does not match the package version.
const EWrongVersion: u64 = 1;

public struct BlobManager has key, store {
    id: UID,
    version: u64,
}

public struct BlobManagerCap has key, store {
    id: UID,
    manager_id: ID,
}

public fun new(
    storage_pool: StoragePool,
    initial_wal: Coin<WAL>,
    ctx: &mut TxContext,
): (BlobManager, BlobManagerCap) {
    let mut manager = BlobManager {
        id: object::new(ctx),
        version: VERSION,
    };
    let inner = blob_manager_inner_v1::new(storage_pool, initial_wal, ctx);
    dynamic_field::add(&mut manager.id, VERSION, inner);

    let cap = BlobManagerCap {
        id: object::new(ctx),
        manager_id: object::id(&manager),
    };

    (manager, cap)
}

public fun share(self: BlobManager) {
    transfer::share_object(self);
}

public fun register_blob(
    self: &mut BlobManager,
    cap: &BlobManagerCap,
    system: &mut System,
    blob_id: u256,
    root_hash: u256,
    unencoded_size: u64,
    encoding_type: u8,
    deletable: bool,
    ctx: &mut TxContext,
) {
    check_cap(self, cap);
    self
        .inner_mut()
        .register_blob(
            system,
            blob_id,
            root_hash,
            unencoded_size,
            encoding_type,
            deletable,
            ctx,
        );
}

public fun certify_blob(
    self: &mut BlobManager,
    system: &System,
    blob_id: u256,
    signature: vector<u8>,
    signers_bitmap: vector<u8>,
    message: vector<u8>,
) {
    self.inner_mut().certify_blob(system, blob_id, signature, signers_bitmap, message);
}

public fun delete_blob(
    self: &mut BlobManager,
    cap: &BlobManagerCap,
    system: &System,
    blob_id: u256,
) {
    check_cap(self, cap);
    self.inner_mut().delete_blob(system, blob_id);
}

public fun deposit_wal_to_coin_stash(self: &mut BlobManager, payment: Coin<WAL>) {
    self.inner_mut().deposit_wal_to_coin_stash(payment);
}

public fun deposit_sui_to_coin_stash(self: &mut BlobManager, payment: Coin<SUI>) {
    self.inner_mut().deposit_sui_to_coin_stash(payment);
}

public fun withdraw_wal(
    self: &mut BlobManager,
    cap: &BlobManagerCap,
    amount: u64,
    ctx: &mut TxContext,
): Coin<WAL> {
    check_cap(self, cap);
    self.inner_mut().withdraw_wal(amount, ctx)
}

public fun withdraw_sui(
    self: &mut BlobManager,
    cap: &BlobManagerCap,
    amount: u64,
    ctx: &mut TxContext,
): Coin<SUI> {
    check_cap(self, cap);
    self.inner_mut().withdraw_sui(amount, ctx)
}

public fun version(self: &BlobManager): u64 {
    self.version
}

public fun manager_id(self: &BlobManager): ID {
    object::id(self)
}

public fun has_blob(self: &BlobManager, blob_id: u256): bool {
    self.inner().has_blob(blob_id)
}

public fun get_blob_object_id(self: &BlobManager, blob_id: u256): ID {
    self.inner().get_blob_object_id(blob_id)
}

public fun storage_pool_id(self: &BlobManager): ID {
    self.inner().storage_pool_id()
}

public fun end_epoch(self: &BlobManager): u32 {
    self.inner().end_epoch()
}

public fun reserved_encoded_capacity_bytes(self: &BlobManager): u64 {
    self.inner().reserved_encoded_capacity_bytes()
}

public fun used_encoded_bytes(self: &BlobManager): u64 {
    self.inner().used_encoded_bytes()
}

public fun available_encoded_bytes(self: &BlobManager): u64 {
    self.inner().available_encoded_bytes()
}

public fun blob_count(self: &BlobManager): u64 {
    self.inner().blob_count()
}

public fun wal_balance(self: &BlobManager): u64 {
    self.inner().wal_balance()
}

public fun sui_balance(self: &BlobManager): u64 {
    self.inner().sui_balance()
}

fun check_cap(self: &BlobManager, cap: &BlobManagerCap) {
    assert!(object::id(self) == cap.manager_id, EInvalidBlobManagerCap);
}

fun inner(self: &BlobManager): &BlobManagerInnerV1 {
    assert!(self.version == VERSION, EWrongVersion);
    dynamic_field::borrow(&self.id, VERSION)
}

fun inner_mut(self: &mut BlobManager): &mut BlobManagerInnerV1 {
    assert!(self.version == VERSION, EWrongVersion);
    dynamic_field::borrow_mut(&mut self.id, VERSION)
}

#[test_only]
public fun destroy_for_testing(self: BlobManager): BlobManagerInnerV1 {
    let mut manager = self;
    let inner = dynamic_field::remove(&mut manager.id, VERSION);
    let BlobManager { id, version: _ } = manager;
    id.delete();
    inner
}

#[test_only]
public fun destroy_cap_for_testing(self: BlobManagerCap) {
    std::unit_test::destroy(self);
}

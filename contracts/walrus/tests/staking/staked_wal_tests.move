// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

module walrus::staked_wal_tests;
use walrus::staked_wal;

#[test]
fun test_staked_wal_flow() {
    let ctx = &mut tx_context::dummy();
    let pool_id = ctx.fresh_object_address().to_id();
    let staked_wal = staked_wal::mint(pool_id, 100, 1, ctx);

    assert!(staked_wal.pool_id() == pool_id);
    assert!(staked_wal.value() == 100);
    assert!(staked_wal.activation_epoch() == 1);

    let principal = staked_wal.burn();

    assert!(principal == 100);
}

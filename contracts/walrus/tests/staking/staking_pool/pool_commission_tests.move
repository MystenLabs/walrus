// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

module walrus::pool_commission_tests;

use walrus::{auth, test_utils::{mint_wal_balance, frost_per_wal, pool, context_runner, assert_eq}};

#[test]
// Scenario:
// 0. Pool has initial commission rate of 10%
// 1. E0: Alice stakes
// 2. E1: Alice requests withdrawal
// 2. E2: Pool receives 10_000 rewards, Alice withdraws her stake
fun collect_commission_with_rewards() {
    let mut test = context_runner();
    let (wctx, ctx) = test.current();
    let mut pool = pool().commission_rate(10_00).build(&wctx, ctx);

    // Alice stakes before committee selection, stake applied E+1
    // And she performs the withdrawal right away
    let mut sw1 = pool.stake(mint_wal_balance(1000), &wctx, ctx);

    let (wctx, _) = test.next_epoch();
    pool.advance_epoch(mint_wal_balance(0), &wctx);
    pool.request_withdraw_stake(&mut sw1, true, false, &wctx);

    let (wctx, ctx) = test.next_epoch();
    pool.advance_epoch(mint_wal_balance(10_000), &wctx);

    // Alice's stake: 1000 + 9000 (90%) rewards
    assert_eq!(
        pool.withdraw_stake(sw1, true, false, &wctx).destroy_for_testing(),
        10_000 * frost_per_wal(),
    );
    assert_eq!(pool.commission_amount(), 1000 * frost_per_wal());

    // Commission is blocked right after advance_epoch; collecting returns 0.
    let auth = auth::authenticate_sender(ctx);
    pool.collect_commission(auth).destroy_zero();

    // After clearing blocked commission (simulating voting_end), full amount is available.
    pool.clear_blocked_commission();
    let auth = auth::authenticate_sender(ctx);
    let commission = pool.collect_commission(auth);
    assert_eq!(commission.destroy_for_testing(), 1000 * frost_per_wal());

    pool.destroy_empty();
}

public struct TestObject has key { id: UID }

#[test]
fun change_commission_receiver() {
    let mut test = context_runner();
    let (wctx, ctx) = test.current();
    let mut pool = pool().commission_rate(10_00).build(&wctx, ctx);

    // by default sender is the receiver
    let auth = auth::authenticate_sender(ctx);
    let cap = TestObject { id: object::new(ctx) };
    let new_receiver = auth::authorized_object(object::id(&cap));

    // make sure the initial setting is correct
    assert!(pool.commission_receiver() == &auth::authorized_address(ctx.sender()));

    // update the receiver
    pool.set_commission_receiver(auth, new_receiver);

    // check the new receiver
    assert!(pool.commission_receiver() == &new_receiver);

    // try claiming the commission with the new receiver
    let auth = auth::authenticate_with_object(&cap);
    pool.collect_commission(auth).destroy_zero();

    // change it back
    let auth = auth::authenticate_with_object(&cap);
    let new_receiver = auth::authorized_address(ctx.sender());
    pool.set_commission_receiver(auth, new_receiver);

    // check the new receiver
    assert!(pool.commission_receiver() == &new_receiver);

    // try claiming the commission with the new receiver
    let auth = auth::authenticate_sender(ctx);
    pool.collect_commission(auth).destroy_zero();

    let TestObject { id } = cap;
    id.delete();
    pool.destroy_empty();
}

#[test, expected_failure(abort_code = ::walrus::staking_pool::EAuthorizationFailure)]
fun change_commission_receiver_fail_incorrect_auth() {
    let mut test = context_runner();
    let (wctx, ctx) = test.current();
    let mut pool = pool().commission_rate(10_00).build(&wctx, ctx);

    // by default sender is the receiver
    let cap = TestObject { id: object::new(ctx) };
    let auth = auth::authenticate_with_object(&cap);
    let new_receiver = auth::authorized_object(object::id(&cap));

    // failure!
    pool.set_commission_receiver(auth, new_receiver);

    abort
}

#[test, expected_failure(abort_code = ::walrus::staking_pool::EAuthorizationFailure)]
fun collect_commission_receiver_fail_incorrect_auth() {
    let mut test = context_runner();
    let (wctx, ctx) = test.current();
    let mut pool = pool().commission_rate(10_00).build(&wctx, ctx);

    // by default sender is the receiver
    let cap = TestObject { id: object::new(ctx) };
    let auth = auth::authenticate_with_object(&cap);

    // failure!
    pool.collect_commission(auth).destroy_zero();

    abort
}

#[test]
fun commission_setting_at_different_epochs() {
    let mut test = context_runner();
    let (wctx, ctx) = test.current();
    let mut pool = pool().commission_rate(0).build(&wctx, ctx);

    assert_eq!(pool.commission_rate(), 0);
    pool.set_next_commission(10_00, &wctx); // applied E+2
    assert_eq!(pool.commission_rate(), 0);

    let (wctx, _) = test.next_epoch(); // E+1
    pool.advance_epoch(mint_wal_balance(0), &wctx);

    assert_eq!(pool.commission_rate(), 0);
    pool.set_next_commission(20_00, &wctx); // set E+3
    pool.set_next_commission(30_00, &wctx); // override E+3

    let (wctx, _) = test.next_epoch(); // E+2
    pool.advance_epoch(mint_wal_balance(0), &wctx);
    assert_eq!(pool.commission_rate(), 10_00);
    pool.set_next_commission(40_00, &wctx); // set E+4

    let (wctx, _) = test.next_epoch(); // E+3
    pool.advance_epoch(mint_wal_balance(0), &wctx);
    assert_eq!(pool.commission_rate(), 30_00);

    let (wctx, _) = test.next_epoch(); // E+4
    pool.advance_epoch(mint_wal_balance(0), &wctx);
    assert_eq!(pool.commission_rate(), 40_00);

    pool.destroy_empty();
}

#[test, expected_failure(abort_code = ::walrus::staking_pool::EIncorrectCommissionRate)]
fun set_incorrect_commission_rate_fail() {
    let mut test = context_runner();
    let (wctx, ctx) = test.current();
    let mut pool = pool().commission_rate(0).build(&wctx, ctx);

    pool.set_next_commission(100_01, &wctx);

    abort
}

// === Commission Blocking Tests ===
//
// Commission earned via advance_epoch is blocked until clear_blocked_commission is called
// (which happens at voting_end). This prevents operators from withdrawing commission
// before the voting period ends. Only the unblocked portion is collectable.

#[test]
/// Two-epoch cycle: E2 commission (500) is blocked then cleared, E3 commission (1000) is
/// blocked. Collecting returns only the previously-cleared 500. A second collect returns
/// zero since the remaining 1000 is still blocked. After clearing again, the 1000 becomes
/// collectable.
fun commission_blocking_across_epochs() {
    let mut test = context_runner();
    let (wctx, ctx) = test.current();
    let mut pool = pool().commission_rate(10_00).build(&wctx, ctx);

    // E0: Alice stakes 1000 WAL
    let mut sw = pool.stake(mint_wal_balance(1000), &wctx, ctx);

    // E1: Advance with 0 rewards (activates stake)
    let (wctx, _) = test.next_epoch();
    pool.advance_epoch(mint_wal_balance(0), &wctx);

    // E2: Pool receives 5,000 rewards -> 500 commission
    let (wctx, _) = test.next_epoch();
    pool.advance_epoch(mint_wal_balance(5_000), &wctx);

    // All 500 is blocked; collecting returns zero.
    assert_eq!(pool.commission_amount(), 500 * frost_per_wal());
    assert_eq!(pool.blocked_commission_amount(), 500 * frost_per_wal());

    // Clear blocked commission (simulating voting_end).
    pool.clear_blocked_commission();
    assert_eq!(pool.blocked_commission_amount(), 0);

    // E3: Pool receives 10,000 more rewards -> 1000 more commission
    let (wctx, ctx) = test.next_epoch();
    pool.advance_epoch(mint_wal_balance(10_000), &wctx);

    // Total: 1500. Blocked: 1000. Collectable: 500.
    assert_eq!(pool.commission_amount(), 1500 * frost_per_wal());
    assert_eq!(pool.blocked_commission_amount(), 1000 * frost_per_wal());

    let auth = auth::authenticate_sender(ctx);
    let collected = pool.collect_commission(auth);
    assert_eq!(collected.destroy_for_testing(), 500 * frost_per_wal());

    // Remaining 1000 is still blocked; second collect returns zero.
    assert_eq!(pool.commission_amount(), 1000 * frost_per_wal());
    let auth = auth::authenticate_sender(ctx);
    pool.collect_commission(auth).destroy_zero();

    // Clear blocked and collect the rest.
    pool.clear_blocked_commission();
    let auth = auth::authenticate_sender(ctx);
    let collected = pool.collect_commission(auth);
    assert_eq!(collected.destroy_for_testing(), 1000 * frost_per_wal());

    // Cleanup: withdraw stake
    pool.request_withdraw_stake(&mut sw, true, false, &wctx);
    let (wctx, _) = test.next_epoch();
    pool.advance_epoch(mint_wal_balance(0), &wctx);
    pool.withdraw_stake(sw, true, false, &wctx).destroy_for_testing();
    let (wctx, _) = test.next_epoch();
    pool.advance_epoch(mint_wal_balance(0), &wctx);
    pool.destroy_empty();
}

#[test]
/// Edge case: zero rewards produce zero commission and zero blocked amount.
fun zero_commission_not_blocked() {
    let mut test = context_runner();
    let (wctx, ctx) = test.current();
    let mut pool = pool().commission_rate(10_00).build(&wctx, ctx);

    let (wctx, ctx) = test.next_epoch();
    pool.advance_epoch(mint_wal_balance(0), &wctx);

    assert_eq!(pool.commission_amount(), 0);
    assert_eq!(pool.blocked_commission_amount(), 0);

    let auth = auth::authenticate_sender(ctx);
    pool.collect_commission(auth).destroy_zero();

    pool.destroy_empty();
}

#[test]
/// Slashing via extract_commission_to_burn removes all commission and resets the blocked
/// amount to zero.
fun extract_commission_to_burn_clears_blocked() {
    let mut test = context_runner();
    let (wctx, ctx) = test.current();
    let mut pool = pool().commission_rate(10_00).build(&wctx, ctx);

    // E0: Alice stakes 1000 WAL
    let mut sw = pool.stake(mint_wal_balance(1000), &wctx, ctx);

    // E1: Advance with 0 rewards (activates stake)
    let (wctx, _) = test.next_epoch();
    pool.advance_epoch(mint_wal_balance(0), &wctx);
    pool.request_withdraw_stake(&mut sw, true, false, &wctx);

    // E2: Pool receives 10,000 rewards -> 1000 commission
    let (wctx, _) = test.next_epoch();
    pool.advance_epoch(mint_wal_balance(10_000), &wctx);

    assert_eq!(pool.blocked_commission_amount(), 1000 * frost_per_wal());

    // Burn all commission (slashing). This should also clear blocked.
    let burned = pool.extract_commission_to_burn();
    assert_eq!(burned.destroy_for_testing(), 1000 * frost_per_wal());
    assert_eq!(pool.blocked_commission_amount(), 0);
    assert_eq!(pool.commission_amount(), 0);

    // Cleanup
    pool.withdraw_stake(sw, true, false, &wctx).destroy_for_testing();
    pool.destroy_empty();
}

#[test]
/// add_commission with block=true accumulates the blocked amount (100 + 200 = 300 blocked).
/// add_commission with block=false is immediately collectable (50 WAL). Collecting returns
/// only the unblocked portion; the blocked portion requires clear_blocked_commission first.
fun add_commission_blocking() {
    let mut test = context_runner();
    let (wctx, ctx) = test.current();
    let mut pool = pool().commission_rate(0).build(&wctx, ctx);

    // Add blocked commission: 100 + 200 = 300 blocked.
    pool.add_commission(mint_wal_balance(100), true);
    pool.add_commission(mint_wal_balance(200), true);
    assert_eq!(pool.commission_amount(), 300 * frost_per_wal());
    assert_eq!(pool.blocked_commission_amount(), 300 * frost_per_wal());

    // Add 50 WAL unblocked. Total: 350. Blocked: 300. Collectable: 50.
    pool.add_commission(mint_wal_balance(50), false);
    assert_eq!(pool.commission_amount(), 350 * frost_per_wal());
    assert_eq!(pool.blocked_commission_amount(), 300 * frost_per_wal());

    let auth = auth::authenticate_sender(ctx);
    let collected = pool.collect_commission(auth);
    assert_eq!(collected.destroy_for_testing(), 50 * frost_per_wal());

    // Clear and collect the blocked 300.
    pool.clear_blocked_commission();
    let auth = auth::authenticate_sender(ctx);
    let collected = pool.collect_commission(auth);
    assert_eq!(collected.destroy_for_testing(), 300 * frost_per_wal());

    pool.destroy_empty();
}

#[test]
/// Edge case: pool can be destroyed even when advance_epoch has set the blocked commission
/// key (with value 0). Ensures the dynamic field doesn't prevent cleanup.
fun destroy_pool_with_zero_blocked_commission() {
    let mut test = context_runner();
    let (wctx, ctx) = test.current();
    let mut pool = pool().commission_rate(10_00).build(&wctx, ctx);

    let (wctx, _) = test.next_epoch();
    pool.advance_epoch(mint_wal_balance(0), &wctx);

    assert_eq!(pool.blocked_commission_amount(), 0);
    pool.destroy_empty();
}

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

#[test]
/// Commission from advance_epoch is blocked. Collecting before clearing returns zero.
fun commission_blocked_after_advance_epoch() {
    let mut test = context_runner();
    let (wctx, ctx) = test.current();
    let mut pool = pool().commission_rate(10_00).build(&wctx, ctx);

    // E0: Alice stakes 1000 WAL
    let mut sw = pool.stake(mint_wal_balance(1000), &wctx, ctx);

    // E1: Advance with 0 rewards (activates stake)
    let (wctx, _) = test.next_epoch();
    pool.advance_epoch(mint_wal_balance(0), &wctx);
    pool.request_withdraw_stake(&mut sw, true, false, &wctx);

    // E2: Pool receives 10,000 rewards -> 1000 commission (10%)
    let (wctx, ctx) = test.next_epoch();
    pool.advance_epoch(mint_wal_balance(10_000), &wctx);

    // Total commission is 1000 WAL, but all of it is blocked.
    assert_eq!(pool.commission_amount(), 1000 * frost_per_wal());
    assert_eq!(pool.blocked_commission_amount(), 1000 * frost_per_wal());

    // Collecting returns zero balance.
    let auth = auth::authenticate_sender(ctx);
    pool.collect_commission(auth).destroy_zero();

    // After clearing, full amount is available.
    pool.clear_blocked_commission();
    let auth = auth::authenticate_sender(ctx);
    let commission = pool.collect_commission(auth);
    assert_eq!(commission.destroy_for_testing(), 1000 * frost_per_wal());

    // Withdraw stake and cleanup.
    pool.withdraw_stake(sw, true, false, &wctx).destroy_for_testing();
    pool.destroy_empty();
}

#[test]
/// Previously accumulated (unblocked) commission is collectable even when new commission
/// is blocked.
fun collect_unblocked_commission_while_new_is_blocked() {
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

    // Blocked: 500. Total: 500. Collectable: 0.
    assert_eq!(pool.commission_amount(), 500 * frost_per_wal());
    assert_eq!(pool.blocked_commission_amount(), 500 * frost_per_wal());

    // Clear blocked commission (simulating voting_end).
    pool.clear_blocked_commission();
    assert_eq!(pool.blocked_commission_amount(), 0);

    // E3: Pool receives 10,000 more rewards -> 1000 more commission
    let (wctx, ctx) = test.next_epoch();
    pool.advance_epoch(mint_wal_balance(10_000), &wctx);

    // Total commission: 500 + 1000 = 1500. Blocked: 1000. Collectable: 500.
    assert_eq!(pool.commission_amount(), 1500 * frost_per_wal());
    assert_eq!(pool.blocked_commission_amount(), 1000 * frost_per_wal());

    let auth = auth::authenticate_sender(ctx);
    let collected = pool.collect_commission(auth);
    assert_eq!(collected.destroy_for_testing(), 500 * frost_per_wal());

    // Remaining commission is 1000 (still blocked).
    assert_eq!(pool.commission_amount(), 1000 * frost_per_wal());

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
/// Zero rewards produce zero blocked commission; collecting still works.
fun zero_commission_not_blocked() {
    let mut test = context_runner();
    let (wctx, ctx) = test.current();
    let mut pool = pool().commission_rate(10_00).build(&wctx, ctx);

    // E1: Advance with 0 rewards (no stake, so wal_balance check passes with 0 rewards)
    let (wctx, ctx) = test.next_epoch();
    pool.advance_epoch(mint_wal_balance(0), &wctx);

    // Total and blocked are both 0.
    assert_eq!(pool.commission_amount(), 0);
    assert_eq!(pool.blocked_commission_amount(), 0);

    // Collecting returns zero balance.
    let auth = auth::authenticate_sender(ctx);
    pool.collect_commission(auth).destroy_zero();

    pool.destroy_empty();
}

#[test]
/// Full epoch cycle: commission is blocked after advance_epoch, cleared after
/// clear_blocked_commission, then new epoch's commission is blocked again.
fun full_epoch_cycle_commission_blocking() {
    let mut test = context_runner();
    let (wctx, ctx) = test.current();
    let mut pool = pool().commission_rate(20_00).build(&wctx, ctx);

    // E0: Alice stakes 1000 WAL
    let mut sw = pool.stake(mint_wal_balance(1000), &wctx, ctx);

    // E1: Advance with 0 rewards (activates stake)
    let (wctx, _) = test.next_epoch();
    pool.advance_epoch(mint_wal_balance(0), &wctx);

    // E2: Pool receives 1000 rewards -> 200 commission (20%)
    let (wctx, ctx) = test.next_epoch();
    pool.advance_epoch(mint_wal_balance(1000), &wctx);

    // Commission: 200, all blocked.
    assert_eq!(pool.commission_amount(), 200 * frost_per_wal());
    assert_eq!(pool.blocked_commission_amount(), 200 * frost_per_wal());
    let auth = auth::authenticate_sender(ctx);
    pool.collect_commission(auth).destroy_zero();

    // Simulate voting_end: clear blocked.
    pool.clear_blocked_commission();
    assert_eq!(pool.blocked_commission_amount(), 0);

    // Now collect the 200.
    let auth = auth::authenticate_sender(ctx);
    let collected = pool.collect_commission(auth);
    assert_eq!(collected.destroy_for_testing(), 200 * frost_per_wal());

    // E3: Pool receives 2000 rewards -> 400 commission
    let (wctx, ctx) = test.next_epoch();
    pool.advance_epoch(mint_wal_balance(2000), &wctx);

    // Commission: 400, all blocked (previous was fully collected).
    assert_eq!(pool.commission_amount(), 400 * frost_per_wal());
    assert_eq!(pool.blocked_commission_amount(), 400 * frost_per_wal());
    let auth = auth::authenticate_sender(ctx);
    pool.collect_commission(auth).destroy_zero();

    // Simulate voting_end: clear blocked.
    pool.clear_blocked_commission();

    // Collect the 400.
    let auth = auth::authenticate_sender(ctx);
    let collected = pool.collect_commission(auth);
    assert_eq!(collected.destroy_for_testing(), 400 * frost_per_wal());

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
/// extract_commission_to_burn clears the blocked commission.
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
/// add_commission does not affect the blocked amount; only advance_epoch does.
fun add_commission_is_not_blocked() {
    let mut test = context_runner();
    let (wctx, ctx) = test.current();
    let mut pool = pool().commission_rate(10_00).build(&wctx, ctx);

    // E0: Alice stakes 1000 WAL
    let mut sw = pool.stake(mint_wal_balance(1000), &wctx, ctx);

    // E1: Advance with 0 rewards (activates stake)
    let (wctx, _) = test.next_epoch();
    pool.advance_epoch(mint_wal_balance(0), &wctx);

    // E2: Pool receives 1000 rewards -> 100 commission (10%)
    let (wctx, ctx) = test.next_epoch();
    pool.advance_epoch(mint_wal_balance(1000), &wctx);

    // Blocked: 100.
    assert_eq!(pool.blocked_commission_amount(), 100 * frost_per_wal());

    // Add 200 WAL commission externally (not blocked).
    pool.add_commission(mint_wal_balance(200));

    // Total: 300. Blocked: 100. Collectable: 200.
    assert_eq!(pool.commission_amount(), 300 * frost_per_wal());
    assert_eq!(pool.blocked_commission_amount(), 100 * frost_per_wal());

    let auth = auth::authenticate_sender(ctx);
    let collected = pool.collect_commission(auth);
    assert_eq!(collected.destroy_for_testing(), 200 * frost_per_wal());

    // Clear blocked and collect rest
    pool.clear_blocked_commission();
    let auth = auth::authenticate_sender(ctx);
    pool.collect_commission(auth).destroy_for_testing();

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
/// Multiple collects: first collect gets unblocked portion, second gets nothing.
fun multiple_collects_with_blocked_commission() {
    let mut test = context_runner();
    let (wctx, ctx) = test.current();
    let mut pool = pool().commission_rate(10_00).build(&wctx, ctx);

    // E0: Alice stakes 1000 WAL
    let mut sw = pool.stake(mint_wal_balance(1000), &wctx, ctx);

    // E1: Advance with 0 rewards (activates stake)
    let (wctx, _) = test.next_epoch();
    pool.advance_epoch(mint_wal_balance(0), &wctx);

    // E2: Pool receives 5000 rewards -> 500 commission
    let (wctx, _) = test.next_epoch();
    pool.advance_epoch(mint_wal_balance(5_000), &wctx);

    // Clear blocked, making all 500 available.
    pool.clear_blocked_commission();

    // E3: Pool receives 10,000 rewards -> 1000 commission
    let (wctx, ctx) = test.next_epoch();
    pool.advance_epoch(mint_wal_balance(10_000), &wctx);

    // Total: 500 + 1000 = 1500. Blocked: 1000. Collectable: 500.
    assert_eq!(pool.commission_amount(), 1500 * frost_per_wal());

    // First collect: gets 500.
    let auth = auth::authenticate_sender(ctx);
    let collected = pool.collect_commission(auth);
    assert_eq!(collected.destroy_for_testing(), 500 * frost_per_wal());

    // Second collect: gets 0 (remaining 1000 is all blocked).
    let auth = auth::authenticate_sender(ctx);
    pool.collect_commission(auth).destroy_zero();

    // Clear and collect the blocked 1000.
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
/// Pool can be destroyed even if it has a blocked commission key with value 0.
fun destroy_pool_with_zero_blocked_commission() {
    let mut test = context_runner();
    let (wctx, ctx) = test.current();
    let mut pool = pool().commission_rate(10_00).build(&wctx, ctx);

    // E1: Advance with 0 rewards (sets blocked key with value 0).
    let (wctx, _) = test.next_epoch();
    pool.advance_epoch(mint_wal_balance(0), &wctx);

    assert_eq!(pool.blocked_commission_amount(), 0);

    // Pool should be destroyable even with the blocked key present.
    pool.destroy_empty();
}

#[test]
/// Collecting commission when total equals blocked returns zero safely.
fun collect_when_total_equals_blocked() {
    let mut test = context_runner();
    let (wctx, ctx) = test.current();
    let mut pool = pool().commission_rate(50_00).build(&wctx, ctx);

    // E0: Alice stakes 1000 WAL
    let mut sw = pool.stake(mint_wal_balance(1000), &wctx, ctx);

    // E1: Advance with 0 rewards (activates stake)
    let (wctx, _) = test.next_epoch();
    pool.advance_epoch(mint_wal_balance(0), &wctx);
    pool.request_withdraw_stake(&mut sw, true, false, &wctx);

    // E2: Pool receives 100 rewards -> 50 commission (50%)
    let (wctx, ctx) = test.next_epoch();
    pool.advance_epoch(mint_wal_balance(100), &wctx);

    // Total == blocked.
    assert_eq!(pool.commission_amount(), 50 * frost_per_wal());
    assert_eq!(pool.blocked_commission_amount(), 50 * frost_per_wal());

    // Collecting returns zero.
    let auth = auth::authenticate_sender(ctx);
    pool.collect_commission(auth).destroy_zero();

    // Clear and collect.
    pool.clear_blocked_commission();
    let auth = auth::authenticate_sender(ctx);
    pool.collect_commission(auth).destroy_for_testing();
    pool.withdraw_stake(sw, true, false, &wctx).destroy_for_testing();
    pool.destroy_empty();
}

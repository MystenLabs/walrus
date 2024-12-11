// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/// Module: `staked_wal`
///
/// Implements the `StakedWal` functionality - a staked WAL is an object that
/// represents a staked amount of WALs in a staking pool. It is created in the
/// `staking_pool` on staking and can be split, joined, and burned. The burning
/// is performed via the `withdraw_stake` method in the `staking_pool`.
module walrus::staked_wal;

use sui::balance::Balance;
use wal::wal::WAL;
use walrus::walrus_context::WalrusContext;

// Error codes
// Error types in `walrus-sui/types/move_errors.rs` are auto-generated from the Move error codes.
const ENotWithdrawing: u64 = 0;
const EMetadataMismatch: u64 = 1;
const EInvalidAmount: u64 = 2;
const ENonZeroPrincipal: u64 = 3;
/// Trying to mark stake as withdrawing when it is already marked as withdrawing.
const EAlreadyWithdrawing: u64 = 6;

/// The state of the staked WAL. It can be either `Staked` or `Withdrawing`.
/// The `Withdrawing` state contains the epoch when the staked WAL can be
///
public enum StakedWalState has copy, drop, store {
    // Default state of the staked WAL - it is staked in the staking pool.
    Staked,
    // The staked WAL is in the process of withdrawing. The value inside the
    // variant is the epoch when the staked WAL can be withdrawn.
    Withdrawing { withdraw_epoch: u32, pool_token_amount: Option<u64> },
}

/// Represents a staked WAL, does not store the `Balance` inside, but uses
/// `u64` to represent the staked amount. Behaves similarly to `Balance` and
/// `Coin` providing methods to `split` and `join`.
public struct StakedWal has key, store {
    id: UID,
    /// Whether the staked WAL is active or withdrawing.
    state: StakedWalState,
    /// ID of the staking pool.
    node_id: ID,
    /// The staked amount.
    principal: Balance<WAL>,
    /// The Walrus epoch when the staked WAL was activated.
    activation_epoch: u32,
}

/// Protected method to create a new staked WAL.
public(package) fun mint(
    node_id: ID,
    principal: Balance<WAL>,
    activation_epoch: u32,
    ctx: &mut TxContext,
): StakedWal {
    StakedWal {
        id: object::new(ctx),
        state: StakedWalState::Staked,
        node_id,
        principal,
        activation_epoch,
    }
}

/// Burns the staked WAL and returns the `principal`.
public(package) fun into_balance(sw: StakedWal): Balance<WAL> {
    let StakedWal { id, principal, .. } = sw;
    id.delete();
    principal
}

/// Sets the staked WAL state to `Withdrawing`
public(package) fun set_withdrawing(
    sw: &mut StakedWal,
    withdraw_epoch: u32,
    pool_token_amount: Option<u64>,
) {
    assert!(sw.is_staked(), EAlreadyWithdrawing);
    sw.state = StakedWalState::Withdrawing { withdraw_epoch, pool_token_amount };
}

/// Checks if the staked WAL can be withdrawn directly.
///
/// The staked WAL can be withdrawn early if:
/// - activation epoch is current epoch + 2
/// - activation epoch is current epoch + 1 and committee hasn't been selected
public(package) fun can_withdraw_early(sw: &StakedWal, wctx: &WalrusContext): bool {
    let activation_epoch = sw.activation_epoch;
    let current_epoch = wctx.epoch();
    let is_withdrawing = sw.is_withdrawing();

    // early return if stake is already active
    if (activation_epoch <= current_epoch || is_withdrawing) {
        return false
    };

    // if stake is to be applied in 2 epochs
    activation_epoch == current_epoch + 2 ||
    (sw.activation_epoch == current_epoch + 1 && !wctx.committee_selected())
}

// === Accessors ===

/// Returns the `node_id` of the staked WAL.
public fun node_id(sw: &StakedWal): ID { sw.node_id }

/// Returns the `principal` of the staked WAL. Called `value` to be consistent
/// with `Coin`.
public fun value(sw: &StakedWal): u64 { sw.principal.value() }

/// Returns the `activation_epoch` of the staked WAL.
public fun activation_epoch(sw: &StakedWal): u32 { sw.activation_epoch }

/// Returns true if the staked WAL is in the `Staked` state.
public fun is_staked(sw: &StakedWal): bool { sw.state == StakedWalState::Staked }

/// Checks whether the staked WAL is in the `Withdrawing` state.
public fun is_withdrawing(sw: &StakedWal): bool {
    match (sw.state) {
        StakedWalState::Withdrawing { .. } => true,
        _ => false,
    }
}

/// Returns the `withdraw_epoch` of the staked WAL if it is in the `Withdrawing`.
/// Aborts otherwise.
public fun withdraw_epoch(sw: &StakedWal): u32 {
    match (sw.state) {
        StakedWalState::Withdrawing { withdraw_epoch, .. } => withdraw_epoch,
        _ => abort ENotWithdrawing,
    }
}

/// Return the `withdraw_amount` of the staked WAL if it is in the `Withdrawing`.
/// Aborts otherwise.
public fun pool_token_amount(sw: &StakedWal): Option<u64> {
    match (sw.state) {
        StakedWalState::Withdrawing { pool_token_amount, .. } => pool_token_amount,
        _ => abort ENotWithdrawing,
    }
}

// === Public APIs ===

/// Joins the staked WAL with another staked WAL, adding the `principal` of the
/// `other` staked WAL to the current staked WAL.
///
/// Aborts if the `node_id` or `activation_epoch` of the staked WALs do not match.
public fun join(sw: &mut StakedWal, other: StakedWal) {
    assert!(sw.node_id == other.node_id, EMetadataMismatch);

    // Simple scenario - staked wal is in `Staked` state. We guarantee that the
    // metadata is identical: same activation epoch and both are in the same state.
    if (sw.is_staked()) {
        assert!(other.is_staked(), EMetadataMismatch);
        assert!(sw.activation_epoch == other.activation_epoch, EMetadataMismatch);

        let StakedWal { id, principal, .. } = other;
        sw.principal.join(principal);
        id.delete();
        return
    };

    // Withdrawing scenario - we no longer check that the activation epoch is
    // the same, as the staked WAL is in the process of withdrawing. Instead,
    // we make sure that the withdraw epoch is the same.
    assert!(sw.is_withdrawing() && other.is_withdrawing(), EMetadataMismatch);
    assert!(sw.withdraw_epoch() == other.withdraw_epoch(), EMetadataMismatch);

    let pool_token_amount = other.pool_token_amount();
    let StakedWal { id, principal, .. } = other;
    sw.principal.join(principal);
    id.delete();

    // Both either need to be set or unset.
    assert!(pool_token_amount.is_some() == sw.pool_token_amount().is_some(), EMetadataMismatch);

    pool_token_amount.do!(|amount| {
        match (&mut sw.state) {
            StakedWalState::Withdrawing { pool_token_amount: current_pool_token_amount, .. } => {
                current_pool_token_amount.do_mut!(|current| *current = *current + amount);
            },
            _ => abort, // unreachable
        }
    });
}

/// Splits the staked WAL into two parts, one with the `amount` and the other
/// with the remaining `principal`. The `node_id`, `activation_epoch` are the
/// same for both the staked WALs.
///
/// Aborts if the `amount` is greater than the `principal` of the staked WAL.
/// Aborts if the `amount` is zero.
public fun split(sw: &mut StakedWal, amount: u64, ctx: &mut TxContext): StakedWal {
    assert!(sw.principal.value() > amount, EInvalidAmount);
    assert!(amount > 0, EInvalidAmount);

    match (&mut sw.state) {
        // If the staked WAL is staked, we can simply split the principal.
        StakedWalState::Staked => {
            StakedWal {
                id: object::new(ctx),
                state: sw.state, // state is preserved
                node_id: sw.node_id,
                principal: sw.principal.split(amount),
                activation_epoch: sw.activation_epoch,
            }
        },
        // If the staked WAL is withdrawing, we need to perform pool token amount
        // calculation based on the amount being split. Needn't worry about the
        // rounding errors as the value is always subtracted from the principal.
        StakedWalState::Withdrawing { withdraw_epoch, pool_token_amount } => {
            // reclaculate the pool token amount if it is set, otherwise ignore
            let new_pool_token_amount = if (pool_token_amount.is_some()) {
                let pool_token_amount = pool_token_amount.borrow_mut();
                let new_pool_token_amount = (*pool_token_amount * amount) / sw.principal.value();
                *pool_token_amount = *pool_token_amount - new_pool_token_amount;
                option::some(new_pool_token_amount)
            } else {
                option::none()
            };

            StakedWal {
                id: object::new(ctx),
                state: StakedWalState::Withdrawing {
                    withdraw_epoch: *withdraw_epoch,
                    pool_token_amount: new_pool_token_amount,
                },
                node_id: sw.node_id,
                principal: sw.principal.split(amount),
                activation_epoch: sw.activation_epoch,
            }
        },
    }
}

/// Destroys the staked WAL if the `principal` is zero. Ignores the `node_id`
/// and `activation_epoch` of the staked WAL given that it is zero.
public fun destroy_zero(sw: StakedWal) {
    assert!(sw.principal.value() == 0, ENonZeroPrincipal);
    let StakedWal { id, principal, .. } = sw;
    principal.destroy_zero();
    id.delete();
}

#[test_only]
public fun destroy_for_testing(sw: StakedWal) {
    let StakedWal { id, principal, .. } = sw;
    principal.destroy_for_testing();
    id.delete();
}

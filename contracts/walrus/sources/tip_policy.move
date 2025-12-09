// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/// Tip policy module for determining tip amounts for transaction senders.
/// This module defines policies for rewarding users who help execute storage operations.
module walrus::tip_policy;

// === Error Codes ===

/// Invalid tip amount (must be non-zero).
const EInvalidTipAmount: u64 = 1;

// === Structs ===

/// Policy for determining tip amounts.
/// Currently supports fixed amount tips, but can be extended for other strategies.
public struct TipPolicy has copy, drop, store {
    /// Fixed tip amount in SUI (in MIST units).
    tip_amount: u64,
}

// === Constructor ===

/// Creates a new tip policy with a fixed tip amount.
public fun new_fixed_amount(tip_amount: u64): TipPolicy {
    assert!(tip_amount > 0, EInvalidTipAmount);
    TipPolicy { tip_amount }
}

// === Accessor Functions ===

/// Gets the tip amount based on the policy.
/// For now, returns a fixed amount regardless of the operation.
/// In the future, this could consider factors like:
/// - Operation type
/// - Storage size
/// - Network conditions
/// - User reputation
public fun get_tip_amount(self: &TipPolicy): u64 {
    self.tip_amount
}

/// Checks if tipping is enabled.
public fun is_enabled(self: &TipPolicy): bool {
    self.tip_amount > 0
}

/// Updates the tip amount in the policy.
public fun set_tip_amount(self: &mut TipPolicy, new_amount: u64) {
    self.tip_amount = new_amount;
}

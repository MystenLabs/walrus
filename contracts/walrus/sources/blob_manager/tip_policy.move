// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/// Tip policy for BlobManager storage extensions.
/// Defines how tips are calculated to incentivize community members to extend storage.
///
/// Public API uses WAL for constructor parameters (human-readable values).
/// Internal storage and return values use FROST (smallest unit).
module walrus::tip_policy;

// === Constants ===

/// 1 MB in bytes.
const MB: u64 = 1_000_000;

/// 1 TB in MB.
const TB_IN_MB: u64 = 1_000_000;

/// Conversion factor: 1 WAL = 1_000_000_000 FROST.
const WAL_TO_FROST: u64 = 1_000_000_000;

/// Scale factor for multiplier calculations (1.0x = 1_000_000).
const MULTIPLIER_SCALE: u64 = 1_000_000;

// Default Adaptive policy values (in WAL units for constructor).

/// Default base tip: 1 WAL.
const DEFAULT_BASE_TIP_WAL: u64 = 1;

/// Default max tip: 2000 WAL.
const DEFAULT_MAX_TIP_WAL: u64 = 2000;

/// Default tip per TB: 10 WAL per TB.
const DEFAULT_TIP_PER_TB_WAL: u64 = 10;

// === Main Structure ===

/// Tip policy defining how tips are calculated for storage extensions.
/// All tip amounts are stored in FROST (smallest WAL unit) internally.
public enum TipPolicy has copy, drop, store {
    /// Fixed tip amount regardless of capacity or time.
    Fixed { tip_amount_frost: u64 },
    /// Adaptive tip that scales with used capacity and time urgency.
    Adaptive {
        /// Minimum base tip in FROST.
        base_tip_frost: u64,
        /// Maximum tip cap in FROST.
        max_tip_frost: u64,
        /// Additional tip per TB of used capacity in FROST.
        tip_per_tb_frost: u64,
    },
}

// === Constructors ===

/// Creates a fixed tip policy.
/// `tip_wal`: Tip amount in WAL (e.g., 10 = 10 WAL).
public(package) fun fixed(tip_wal: u64): TipPolicy {
    TipPolicy::Fixed { tip_amount_frost: tip_wal * WAL_TO_FROST }
}

/// Creates an adaptive tip policy.
/// All parameters in WAL for readability:
/// - `base_tip`: Base tip (e.g., 1 = 1 WAL)
/// - `max_tip`: Maximum tip cap (e.g., 2000 = 2000 WAL)
/// - `tip_per_tb`: Tip per TB of used capacity (e.g., 10 = 10 WAL/TB)
public(package) fun adaptive(base_tip: u64, max_tip: u64, tip_per_tb: u64): TipPolicy {
    TipPolicy::Adaptive {
        base_tip_frost: base_tip * WAL_TO_FROST,
        max_tip_frost: max_tip * WAL_TO_FROST,
        tip_per_tb_frost: tip_per_tb * WAL_TO_FROST,
    }
}

/// Creates a default adaptive tip policy.
/// Default: base=1 WAL, max=2000 WAL, per_tb=10 WAL.
public(package) fun default_adaptive(): TipPolicy {
    adaptive(DEFAULT_BASE_TIP_WAL, DEFAULT_MAX_TIP_WAL, DEFAULT_TIP_PER_TB_WAL)
}

// === Tip Calculation ===

/// Calculates the tip amount in FROST based on the policy.
/// Uses current epoch info to determine time multiplier (last epoch only).
///
/// Parameters:
/// - `used_bytes`: Total used storage in bytes
/// - `current_timestamp_ms`: Current time from Clock
/// - `current_epoch`: Current Walrus epoch
/// - `current_epoch_start_ms`: When the current epoch started (from Staking epoch_state)
/// - `epoch_duration_ms`: Duration of an epoch in ms
/// - `storage_end_epoch`: When storage expires
///
/// Returns: Tip amount in FROST
public(package) fun calculate_tip(
    policy: &TipPolicy,
    used_bytes: u64,
    current_timestamp_ms: u64,
    current_epoch: u32,
    current_epoch_start_ms: u64,
    epoch_duration_ms: u64,
    storage_end_epoch: u32,
): u64 {
    // Convert bytes to MB for smaller numbers in intermediate calculations.
    let used_mb = used_bytes / MB;

    match (policy) {
        TipPolicy::Fixed { tip_amount_frost } => *tip_amount_frost,
        TipPolicy::Adaptive { base_tip_frost, max_tip_frost, tip_per_tb_frost } => {
            // Capacity-based tip: base + (used_mb / TB_IN_MB) * tip_per_tb.
            // All values in FROST.
            let tb_count = used_mb / TB_IN_MB;
            let capacity_tip_frost = *base_tip_frost + tb_count * *tip_per_tb_frost;

            // Time multiplier (only active in the last epoch before expiry).
            let multiplier = calculate_last_epoch_multiplier(
                current_timestamp_ms,
                current_epoch,
                current_epoch_start_ms,
                epoch_duration_ms,
                storage_end_epoch,
            );

            // Apply multiplier using u128 to prevent overflow.
            let tip_frost =
                (capacity_tip_frost as u128) * (multiplier as u128)
                / (MULTIPLIER_SCALE as u128);
            let tip_frost = tip_frost as u64;

            // Cap to max tip.
            if (tip_frost > *max_tip_frost) {
                *max_tip_frost
            } else {
                tip_frost
            }
        },
    }
}

/// Calculates the tip amount without time multiplier.
/// Convenience function that doesn't require epoch timing parameters.
/// Returns: Tip amount in FROST
public(package) fun calculate_tip_simple(policy: &TipPolicy, used_bytes: u64): u64 {
    let used_mb = used_bytes / MB;

    match (policy) {
        TipPolicy::Fixed { tip_amount_frost } => *tip_amount_frost,
        TipPolicy::Adaptive { base_tip_frost, max_tip_frost, tip_per_tb_frost } => {
            let tb_count = used_mb / TB_IN_MB;
            let tip_frost = *base_tip_frost + tb_count * *tip_per_tb_frost;
            if (tip_frost > *max_tip_frost) {
                *max_tip_frost
            } else {
                tip_frost
            }
        },
    }
}

// === Time Multiplier ===

/// Returns the time multiplier in MULTIPLIER_SCALE (1_000_000 = 1.0x, 2_000_000 = 2.0x).
/// The multiplier is only active when we're in the last epoch before storage expiry.
///
/// Uses a hyperbolic curve: multiplier = 2 * half / (delta + half)
/// where delta = distance from midpoint (absolute value).
///
/// This creates a curve that:
/// - Starts at 1.0x at epoch start
/// - Rises sharply toward 2.0x at midpoint (peak)
/// - Falls back to 1.0x at epoch end
///
/// The curve is flat near the edges and spikes at the midpoint, incentivizing
/// extensions in the middle of the last epoch.
fun calculate_last_epoch_multiplier(
    current_timestamp_ms: u64,
    current_epoch: u32,
    current_epoch_start_ms: u64,
    epoch_duration_ms: u64,
    storage_end_epoch: u32,
): u64 {
    // Handle edge case where storage_end_epoch is 0 or 1.
    if (storage_end_epoch <= 1) {
        return MULTIPLIER_SCALE
    };

    // The "last epoch" is the one just before storage expires.
    let last_epoch = storage_end_epoch - 1;

    // Only apply multiplier if we're currently in the last epoch.
    if (current_epoch != last_epoch) {
        return MULTIPLIER_SCALE
    };

    // We're in the last epoch. Calculate position within the epoch.
    let elapsed = if (current_timestamp_ms > current_epoch_start_ms) {
        current_timestamp_ms - current_epoch_start_ms
    } else {
        0
    };

    // Cap elapsed time to epoch duration.
    if (elapsed >= epoch_duration_ms) {
        return MULTIPLIER_SCALE
    };

    let half_duration = epoch_duration_ms / 2;

    // Calculate delta = distance from midpoint.
    let delta = if (elapsed <= half_duration) {
        half_duration - elapsed
    } else {
        elapsed - half_duration
    };

    // Hyperbolic formula: multiplier = 2 * half / (delta + half)
    // At delta=0 (midpoint): 2 * half / half = 2.0
    // At delta=half (start/end): 2 * half / (2*half) = 1.0
    // Scale by MULTIPLIER_SCALE for integer math.
    2 * MULTIPLIER_SCALE * half_duration / (delta + half_duration)
}

// === Tests ===

#[test]
fun test_fixed_policy() {
    let policy = fixed(10); // 10 WAL fixed tip.
    // Fixed policy always returns the same amount.
    let tip = calculate_tip_simple(&policy, 0);
    assert!(tip == 10 * WAL_TO_FROST); // 10 WAL.
    let tip = calculate_tip_simple(&policy, 100 * TB_IN_MB * MB); // 100 TB.
    assert!(tip == 10 * WAL_TO_FROST); // Still 10 WAL.
}

#[test]
fun test_adaptive_policy_capacity_scaling() {
    let policy = adaptive(1, 2000, 10); // base=1 WAL, max=2000 WAL, 10 WAL/TB.

    // 0 TB: base tip only = 1 WAL.
    let tip = calculate_tip_simple(&policy, 0);
    assert!(tip == 1 * WAL_TO_FROST);

    // 1 TB: base + 10 WAL = 11 WAL.
    let tip = calculate_tip_simple(&policy, TB_IN_MB * MB);
    assert!(tip == 11 * WAL_TO_FROST);

    // 50 TB: base + 500 WAL = 501 WAL.
    let tip = calculate_tip_simple(&policy, 50 * TB_IN_MB * MB);
    assert!(tip == 501 * WAL_TO_FROST);

    // 100 TB: base + 1000 WAL = 1001 WAL.
    let tip = calculate_tip_simple(&policy, 100 * TB_IN_MB * MB);
    assert!(tip == 1001 * WAL_TO_FROST);

    // 500 TB: would be 5001 WAL but capped to 2000 WAL.
    let tip = calculate_tip_simple(&policy, 500 * TB_IN_MB * MB);
    assert!(tip == 2000 * WAL_TO_FROST);
}

#[test]
fun test_time_multiplier_not_in_last_epoch() {
    // storage_end_epoch=100, last_epoch=99.
    // We're in epoch 98 (not last epoch).
    let mult = calculate_last_epoch_multiplier(
        5000, // current_timestamp_ms
        98, // current_epoch (not the last one)
        0, // current_epoch_start_ms
        10000, // epoch_duration_ms
        100, // storage_end_epoch
    );
    assert!(mult == MULTIPLIER_SCALE); // 1.0x.
}

#[test]
fun test_time_multiplier_in_last_epoch() {
    // storage_end_epoch=100, last_epoch=99.
    // We're in epoch 99 (the last epoch).
    // epoch_duration=10000ms, half_duration=5000, epoch_start=0.
    //
    // Hyperbolic formula: mult = 2 * half / (delta + half)
    // where delta = |elapsed - half|

    // At epoch start (elapsed=0, delta=5000): 2*5000/(5000+5000) = 1.0x.
    let mult = calculate_last_epoch_multiplier(0, 99, 0, 10000, 100);
    assert!(mult == MULTIPLIER_SCALE);

    // At 1/4 through (elapsed=2500, delta=2500): 2*5000/(2500+5000) = 10000/7500 = 1.33x.
    let mult = calculate_last_epoch_multiplier(2500, 99, 0, 10000, 100);
    assert!(mult == 1_333_333);

    // At midpoint (elapsed=5000, delta=0): 2*5000/(0+5000) = 2.0x.
    let mult = calculate_last_epoch_multiplier(5000, 99, 0, 10000, 100);
    assert!(mult == 2 * MULTIPLIER_SCALE);

    // At 3/4 through (elapsed=7500, delta=2500): same as 1/4, 1.33x.
    let mult = calculate_last_epoch_multiplier(7500, 99, 0, 10000, 100);
    assert!(mult == 1_333_333);

    // At epoch end (elapsed=10000): back to 1.0x.
    let mult = calculate_last_epoch_multiplier(10000, 99, 0, 10000, 100);
    assert!(mult == MULTIPLIER_SCALE);
}

#[test]
fun test_adaptive_with_time_multiplier() {
    let policy = adaptive(1, 2000, 10); // base=1 WAL, max=2000 WAL, 10 WAL/TB.

    // At 10 TB, not in last epoch: 1 + 100 = 101 WAL.
    let tip = calculate_tip(
        &policy,
        10 * TB_IN_MB * MB, // 10 TB
        5000, // current_timestamp_ms
        98, // current_epoch (not last)
        0, // current_epoch_start_ms
        10000, // epoch_duration_ms
        100, // storage_end_epoch
    );
    assert!(tip == 101 * WAL_TO_FROST);

    // At 10 TB, at midpoint of last epoch: 101 WAL * 2 = 202 WAL.
    let tip = calculate_tip(
        &policy,
        10 * TB_IN_MB * MB, // 10 TB
        5000, // current_timestamp_ms (at midpoint)
        99, // current_epoch (last epoch)
        0, // current_epoch_start_ms
        10000, // epoch_duration_ms
        100, // storage_end_epoch
    );
    assert!(tip == 202 * WAL_TO_FROST);
}

#[test]
fun test_default_adaptive() {
    let policy = default_adaptive();
    let tip = calculate_tip_simple(&policy, 0);
    assert!(tip == DEFAULT_BASE_TIP_WAL * WAL_TO_FROST);
}

#[test]
fun test_edge_cases() {
    // storage_end_epoch = 0.
    let mult = calculate_last_epoch_multiplier(1000, 0, 0, 1000, 0);
    assert!(mult == MULTIPLIER_SCALE);

    // storage_end_epoch = 1.
    let mult = calculate_last_epoch_multiplier(1000, 0, 0, 1000, 1);
    assert!(mult == MULTIPLIER_SCALE);
}

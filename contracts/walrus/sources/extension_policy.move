// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/// Extension policy for BlobManager storage extensions.
/// Controls when and how storage can be extended by anyone using the coin stash.
module walrus::extension_policy;

use walrus::tip_policy::{Self, TipPolicy};

// === Error Codes ===

/// Extension is not allowed yet - storage hasn't reached the expiry threshold.
const EExtensionTooEarly: u64 = 0;

// === Main Structure ===

/// Extension policy controlling when and how storage can be extended.
/// To disable extensions, set all values to 0 (expiry_threshold_epochs=0, max_extension_epochs=0).
public struct ExtensionPolicy has copy, drop, store {
    /// Extension only allowed when current epoch >= end_epoch - expiry_threshold_epochs.
    expiry_threshold_epochs: u32,
    /// Maximum epochs that can be extended in a single call. Set to 0 to disable extensions.
    max_extension_epochs: u32,
    /// Tip policy for rewarding users who execute extensions. Tips are paid in WAL.
    tip_policy: TipPolicy,
}

// === Constructors ===

/// Creates a new extension policy with a fixed tip amount (in DWAL = 0.1 WAL units).
/// To disable extensions, set max_extension_epochs to 0.
public(package) fun new(
    expiry_threshold_epochs: u32,
    max_extension_epochs: u32,
    tip_amount_dwal: u64,
): ExtensionPolicy {
    ExtensionPolicy {
        expiry_threshold_epochs,
        max_extension_epochs,
        tip_policy: tip_policy::fixed(tip_amount_dwal),
    }
}

/// Creates a new extension policy with a custom tip policy.
public(package) fun new_with_tip_policy(
    expiry_threshold_epochs: u32,
    max_extension_epochs: u32,
    tip_policy: TipPolicy,
): ExtensionPolicy {
    ExtensionPolicy {
        expiry_threshold_epochs,
        max_extension_epochs,
        tip_policy,
    }
}

/// Creates a policy with default thresholds and adaptive tip policy.
/// Default: expiry_threshold=2, max_extension=5, adaptive tip.
public(package) fun default(): ExtensionPolicy {
    ExtensionPolicy {
        expiry_threshold_epochs: 2,
        max_extension_epochs: 5,
        tip_policy: tip_policy::default_adaptive(),
    }
}

/// Creates a policy with fixed tip for backward compatibility.
/// Default: expiry_threshold=2, max_extension=5, tip=10 DWAL (1 WAL).
public(package) fun default_fixed(): ExtensionPolicy {
    ExtensionPolicy {
        expiry_threshold_epochs: 2,
        max_extension_epochs: 5,
        tip_policy: tip_policy::fixed(10), // 1 WAL = 10 DWAL.
    }
}

// === Validation Functions ===

/// Validates extension request and computes the new end epoch.
/// All extensions must follow policy constraints.
///
/// Aborts if extension is too early (before expiry threshold).
///
/// Returns the new end_epoch (capped by both policy and system limits).
/// If max_extension_epochs is 0, the storage_end_epoch is returned unchanged.
public(package) fun validate_and_compute_end_epoch(
    policy: &ExtensionPolicy,
    current_epoch: u32,
    storage_end_epoch: u32,
    requested_extension_epochs: u32,
    system_max_epochs_ahead: u32,
): u32 {
    // Check time threshold.
    let threshold_epoch = if (storage_end_epoch > policy.expiry_threshold_epochs) {
        storage_end_epoch - policy.expiry_threshold_epochs
    } else {
        0
    };
    assert!(current_epoch >= threshold_epoch, EExtensionTooEarly);

    // Calculate new end epoch with all caps applied.
    // Cap 1: Policy max extension (if 0, no extension allowed).
    let capped_extension = if (requested_extension_epochs > policy.max_extension_epochs) {
        policy.max_extension_epochs
    } else {
        requested_extension_epochs
    };

    // New end epoch based on capped extension.
    let new_end_epoch = storage_end_epoch + capped_extension;

    // Cap 2: System max (cannot exceed current_epoch + system_max_epochs_ahead).
    let system_max_end_epoch = current_epoch + system_max_epochs_ahead;
    if (new_end_epoch > system_max_end_epoch) {
        system_max_end_epoch
    } else {
        new_end_epoch
    }
}

// === Accessors ===

/// Gets the tip policy.
public fun tip_policy(policy: &ExtensionPolicy): &TipPolicy {
    &policy.tip_policy
}

/// Gets the expiry threshold epochs.
public fun expiry_threshold_epochs(policy: &ExtensionPolicy): u32 {
    policy.expiry_threshold_epochs
}

/// Gets the max extension epochs.
public fun max_extension_epochs(policy: &ExtensionPolicy): u32 {
    policy.max_extension_epochs
}

/// Calculates the tip amount in FROST based on policy, capacity, and timing.
/// This is the full calculation with time multiplier.
public fun calculate_tip(
    policy: &ExtensionPolicy,
    used_bytes: u64,
    current_timestamp_ms: u64,
    current_epoch: u32,
    current_epoch_start_ms: u64,
    epoch_duration_ms: u64,
    storage_end_epoch: u32,
): u64 {
    tip_policy::calculate_tip(
        &policy.tip_policy,
        used_bytes,
        current_timestamp_ms,
        current_epoch,
        current_epoch_start_ms,
        epoch_duration_ms,
        storage_end_epoch,
    )
}

/// Calculates the tip amount without time multiplier (simpler version).
public fun calculate_tip_simple(policy: &ExtensionPolicy, used_bytes: u64): u64 {
    tip_policy::calculate_tip_simple(&policy.tip_policy, used_bytes)
}

// === Tests ===

/// DWAL to FROST conversion for test assertions.
const DWAL_TO_FROST: u64 = 100_000_000;

#[test]
fun test_policy_creation() {
    // new() creates a fixed tip policy with tip in DWAL units.
    let policy = new(1, 5, 10); // 10 DWAL = 1 WAL.
    // Policy created successfully. Tip is 10 DWAL = 1 WAL = 1_000_000_000 FROST.
    assert!(calculate_tip_simple(&policy, 0) == 10 * DWAL_TO_FROST);
}

#[test]
fun test_disabled_policy_no_extension() {
    // When max_extension_epochs is 0, no extension happens.
    // expiry_threshold=0 means extension only allowed when current_epoch >= storage_end_epoch.
    let policy = new(0, 0, 0);
    // current=100, end=100, requested=5, but max=0 so capped to 0.
    // new_end = 100 + 0 = 100 (unchanged).
    let result = validate_and_compute_end_epoch(&policy, 100, 100, 5, 52);
    assert!(result == 100);
}

#[test, expected_failure(abort_code = EExtensionTooEarly)]
fun test_too_early_aborts() {
    let policy = new(1, 5, 10);
    // Threshold is 99, current is 98 - too early.
    validate_and_compute_end_epoch(&policy, 98, 100, 5, 52);
}

#[test]
fun test_at_threshold() {
    let policy = new(1, 5, 10);
    // Threshold is 99, current is 99 - allowed.
    // end=100, extension=5, new_end=105.
    let result = validate_and_compute_end_epoch(&policy, 99, 100, 5, 52);
    assert!(result == 105);
}

#[test]
fun test_caps_to_policy_max() {
    let policy = new(1, 5, 10);
    // Requesting 10, but policy max is 5.
    // end=100, capped_extension=5, new_end=105.
    let result = validate_and_compute_end_epoch(&policy, 99, 100, 10, 52);
    assert!(result == 105);
}

#[test]
fun test_caps_to_system_max() {
    // Policy: extend only within 1 epoch of expiry, max 100 epochs.
    // Using high policy max to test system cap.
    let policy = new(1, 100, 10);

    // current=99, end=100, threshold=99, within window (99 >= 99).
    // requested=100, policy_capped=100, new_end=200.
    // system_max_end=99+52=151, so capped to 151.
    let result = validate_and_compute_end_epoch(&policy, 99, 100, 100, 52);
    assert!(result == 151);

    // current=150, end=151, threshold=150, within window.
    // requested=100, policy_capped=100, new_end=251.
    // system_max_end=150+52=202, so capped to 202.
    let result2 = validate_and_compute_end_epoch(&policy, 150, 151, 100, 52);
    assert!(result2 == 202);

    // Test with smaller policy max.
    let policy2 = new(1, 10, 10);
    // current=99, end=100, threshold=99, within window.
    // requested=100, policy_capped=10, new_end=110.
    // system_max_end=151, so new_end=110 (no system cap needed).
    let result3 = validate_and_compute_end_epoch(&policy2, 99, 100, 100, 52);
    assert!(result3 == 110);
}

#[test]
fun test_tip_calculation() {
    // Fixed tip policy: 20 DWAL = 2 WAL.
    let policy = new(1, 5, 20);
    assert!(calculate_tip_simple(&policy, 0) == 20 * DWAL_TO_FROST);

    // Zero tip policy.
    let zero_tip_policy = new(1, 5, 0);
    assert!(calculate_tip_simple(&zero_tip_policy, 0) == 0);

    // Default policy uses adaptive tip.
    let default_policy = default();
    // Adaptive base tip is 1 DWAL = 0.1 WAL.
    assert!(calculate_tip_simple(&default_policy, 0) == 1 * DWAL_TO_FROST);
}

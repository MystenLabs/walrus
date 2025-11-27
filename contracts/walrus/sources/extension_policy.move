// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/// Extension policy for BlobManager storage extensions.
/// Controls when and how storage can be extended by anyone using the coin stash.
module walrus::extension_policy;

// === Error Codes ===

/// Extension is not allowed yet - storage hasn't reached the expiry threshold.
const EExtensionTooEarly: u64 = 0;
/// Extension is disabled - no one can extend until policy is changed.
const EExtensionDisabled: u64 = 2;

// === Main Structure ===

/// Extension policy controlling when and how storage can be extended.
public enum ExtensionPolicy has copy, drop, store {
    /// Extension completely disabled - no one can extend until policy is changed.
    Disabled,
    /// Only fund_manager can extend (no public extension).
    FundManagerOnly,
    /// Allow extension within constraints by anyone.
    Constrained {
        /// Extension only allowed when current epoch >= end_epoch - expiry_threshold_epochs.
        expiry_threshold_epochs: u32,
        /// Maximum epochs that can be extended in a single call.
        max_extension_epochs: u32,
    },
}

// === Constructors ===

/// Creates a disabled policy (no one can extend).
public fun disabled(): ExtensionPolicy {
    ExtensionPolicy::Disabled
}

/// Creates a fund_manager-only policy (only fund_manager can extend).
public fun fund_manager_only(): ExtensionPolicy {
    ExtensionPolicy::FundManagerOnly
}

/// Creates a constrained policy with the given thresholds.
public fun constrained(expiry_threshold_epochs: u32, max_extension_epochs: u32): ExtensionPolicy {
    ExtensionPolicy::Constrained {
        expiry_threshold_epochs,
        max_extension_epochs,
    }
}

// === Validation Functions ===

/// Validates extension for fund_manager and returns the effective extension epochs.
/// Fund manager bypasses policy time/amount constraints but still respects system max.
/// Caller is responsible for verifying the cap has fund_manager permission.
///
/// Aborts if policy is Disabled (no one can extend).
///
/// Returns the capped extension epochs (may be less than requested due to system limits).
public fun validate_and_cap_extension_fund_manager(
    policy: &ExtensionPolicy,
    current_epoch: u32,
    storage_end_epoch: u32,
    requested_extension_epochs: u32,
    system_max_epochs_ahead: u32,
): u32 {
    // Disabled policy blocks everyone, even fund_manager.
    assert!(!is_disabled(policy), EExtensionDisabled);

    // Fund manager bypasses time/amount constraints, but still respect system max.
    cap_to_system_max(
        current_epoch,
        storage_end_epoch,
        requested_extension_epochs,
        system_max_epochs_ahead,
    )
}

/// Validates public extension (no cap) and returns the effective extension epochs.
/// Public extensions must follow policy constraints.
///
/// Aborts if:
/// - Policy is Disabled (no one can extend).
/// - Policy is FundManagerOnly (need fund_manager cap).
/// - Policy is Constrained and extension is too early.
///
/// Returns the capped extension epochs (may be less than requested due to policy/system limits).
public fun validate_and_cap_extension(
    policy: &ExtensionPolicy,
    current_epoch: u32,
    storage_end_epoch: u32,
    requested_extension_epochs: u32,
    system_max_epochs_ahead: u32,
): u32 {
    match (policy) {
        ExtensionPolicy::Disabled => {
            abort EExtensionDisabled
        },
        ExtensionPolicy::FundManagerOnly => {
            // Public extension not allowed.
            abort EExtensionDisabled
        },
        ExtensionPolicy::Constrained { expiry_threshold_epochs, max_extension_epochs } => {
            // Check time threshold.
            let threshold_epoch = if (storage_end_epoch > *expiry_threshold_epochs) {
                storage_end_epoch - *expiry_threshold_epochs
            } else {
                0
            };
            assert!(current_epoch >= threshold_epoch, EExtensionTooEarly);

            // Cap to both policy max and system max.
            let system_capped = cap_to_system_max(
                current_epoch,
                storage_end_epoch,
                requested_extension_epochs,
                system_max_epochs_ahead,
            );

            // Also cap to policy's max_extension_epochs.
            if (system_capped > *max_extension_epochs) {
                *max_extension_epochs
            } else {
                system_capped
            }
        },
    }
}

/// Helper function to cap extension to system maximum.
/// Returns 0 if no extension is possible.
fun cap_to_system_max(
    current_epoch: u32,
    storage_end_epoch: u32,
    requested_extension_epochs: u32,
    system_max_epochs_ahead: u32,
): u32 {
    // Calculate the maximum allowed end epoch based on system limits.
    let max_allowed_end_epoch = current_epoch + system_max_epochs_ahead;

    // If storage_end_epoch already at or past max, no extension possible.
    if (storage_end_epoch >= max_allowed_end_epoch) {
        return 0
    };

    // Calculate the new end epoch after extension.
    let new_end_epoch = storage_end_epoch + requested_extension_epochs;

    // Cap the extension to not exceed max_allowed_end_epoch.
    if (new_end_epoch > max_allowed_end_epoch) {
        max_allowed_end_epoch - storage_end_epoch
    } else {
        requested_extension_epochs
    }
}

// === Accessors ===

/// Returns true if the policy is disabled.
public fun is_disabled(policy: &ExtensionPolicy): bool {
    match (policy) {
        ExtensionPolicy::Disabled => true,
        ExtensionPolicy::FundManagerOnly => false,
        ExtensionPolicy::Constrained { .. } => false,
    }
}

// === Tests ===

#[test]
fun test_disabled_policy() {
    let policy = disabled();
    assert!(is_disabled(&policy));
}

#[test]
fun test_fund_manager_only_policy() {
    let policy = fund_manager_only();
    assert!(!is_disabled(&policy));
}

#[test]
fun test_constrained_policy_creation() {
    let policy = constrained(1, 5);
    assert!(!is_disabled(&policy));
}

#[test]
fun test_cap_to_system_max() {
    let system_max = 52u32;

    // Normal case: requested is within limits.
    assert!(cap_to_system_max(100, 110, 5, system_max) == 5);

    // Requested exceeds system max.
    // current=100, end=150, max_allowed=152, so max extension = 2.
    assert!(cap_to_system_max(100, 150, 10, system_max) == 2);

    // Storage already at max.
    // current=100, end=152, max_allowed=152, no extension possible.
    assert!(cap_to_system_max(100, 152, 10, system_max) == 0);

    // Storage past max.
    assert!(cap_to_system_max(100, 160, 10, system_max) == 0);
}

#[test, expected_failure(abort_code = EExtensionDisabled)]
fun test_validate_disabled_aborts() {
    let policy = disabled();
    validate_and_cap_extension(&policy, 99, 100, 5, 52);
}

#[test, expected_failure(abort_code = EExtensionDisabled)]
fun test_validate_disabled_aborts_fund_manager() {
    let policy = disabled();
    validate_and_cap_extension_fund_manager(&policy, 99, 100, 5, 52);
}

#[test, expected_failure(abort_code = EExtensionDisabled)]
fun test_validate_fund_manager_only_without_cap_aborts() {
    let policy = fund_manager_only();
    validate_and_cap_extension(&policy, 99, 100, 5, 52);
}

#[test]
fun test_validate_fund_manager_only_with_fund_manager() {
    let policy = fund_manager_only();
    let result = validate_and_cap_extension_fund_manager(&policy, 99, 100, 5, 52);
    assert!(result == 5);
}

#[test, expected_failure(abort_code = EExtensionTooEarly)]
fun test_validate_constrained_too_early_aborts() {
    let policy = constrained(1, 5);
    // Threshold is 99, current is 98 - too early.
    validate_and_cap_extension(&policy, 98, 100, 5, 52);
}

#[test]
fun test_validate_constrained_at_threshold() {
    let policy = constrained(1, 5);
    // Threshold is 99, current is 99 - allowed.
    let result = validate_and_cap_extension(&policy, 99, 100, 5, 52);
    assert!(result == 5);
}

#[test]
fun test_validate_constrained_caps_to_policy_max() {
    let policy = constrained(1, 5);
    // Requesting 10, but policy max is 5.
    let result = validate_and_cap_extension(&policy, 99, 100, 10, 52);
    assert!(result == 5);
}

#[test]
fun test_validate_constrained_caps_to_system_max() {
    // Policy: extend only within 1 epoch of expiry, max 100 epochs.
    // Using high policy max to test system cap.
    let policy = constrained(1, 100);

    // current=99, end=100, threshold=99, within window (99 >= 99).
    // max_allowed=99+52=151, requested 100, new_end=200 > 151.
    // system cap = 151-100 = 51, < policy max 100.
    let result = validate_and_cap_extension(&policy, 99, 100, 100, 52);
    assert!(result == 51);

    // No extension possible: end >= max_allowed.
    // current=99, end=151, threshold=150, within window (99 >= 150? No!).
    // Need current=150.
    // current=150, end=151, threshold=150, within window.
    // max_allowed=150+52=202, system cap = 202-151 = 51.
    let result2 = validate_and_cap_extension(&policy, 150, 151, 100, 52);
    assert!(result2 == 51);

    // Test with smaller policy max.
    let policy2 = constrained(1, 10);
    // current=99, end=100, threshold=99, within window.
    // max_allowed=99+52=151, system cap = 51, but policy max = 10.
    let result3 = validate_and_cap_extension(&policy2, 99, 100, 100, 52);
    assert!(result3 == 10);
}

#[test]
fun test_validate_fund_manager_bypasses_threshold() {
    let policy = constrained(1, 5);
    // Threshold is 99, current is 50 - would be too early for public.
    // But fund_manager bypasses the threshold check.
    // system max = 50 + 52 = 102, so 10 epochs is within limit.
    let result = validate_and_cap_extension_fund_manager(&policy, 50, 100, 10, 52);
    assert!(result == 2); // Capped by system: 102 - 100 = 2.
}

#[test]
fun test_validate_fund_manager_still_capped_by_system() {
    let policy = constrained(1, 5);
    // current=100, end=150, max_allowed=152.
    // Fund manager can bypass policy max (5), but system max is 2.
    let result = validate_and_cap_extension_fund_manager(&policy, 100, 150, 10, 52);
    assert!(result == 2);
}

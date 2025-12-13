// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/// Extension policy for BlobManager storage extensions.
/// Controls when and how storage can be extended by anyone using the coin stash.
module walrus::extension_policy;

// === Error Codes ===

/// Extension is not allowed yet - storage hasn't reached the expiry threshold.
const EExtensionTooEarly: u64 = 0;
/// Extension is disabled.
const EExtensionDisabled: u64 = 1;

// === Main Structure ===

/// Extension policy controlling when and how storage can be extended.
public enum ExtensionPolicy has copy, drop, store {
    /// Extension is disabled - no one can extend.
    Disabled,
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
public(package) fun disabled(): ExtensionPolicy {
    ExtensionPolicy::Disabled
}

/// Creates a constrained policy with the given thresholds.
public(package) fun constrained(
    expiry_threshold_epochs: u32,
    max_extension_epochs: u32,
): ExtensionPolicy {
    ExtensionPolicy::Constrained {
        expiry_threshold_epochs,
        max_extension_epochs,
    }
}

/// Creates a constrained policy with the given thresholds.
public(package) fun default_constrained(): ExtensionPolicy {
    ExtensionPolicy::Constrained {
        expiry_threshold_epochs: 2,
        max_extension_epochs: 5,
    }
}

// === Validation Functions ===

/// Validates extension request and computes the new end epoch.
/// All extensions must follow policy constraints.
///
/// Aborts if:
/// - Policy is Disabled (no one can extend).
/// - Policy is Constrained and extension is too early.
///
/// Returns the new end_epoch (capped by both policy and system limits).
public(package) fun validate_and_compute_end_epoch(
    policy: &ExtensionPolicy,
    current_epoch: u32,
    storage_end_epoch: u32,
    requested_extension_epochs: u32,
    system_max_epochs_ahead: u32,
): u32 {
    match (policy) {
        ExtensionPolicy::Disabled => {
            // Extension is disabled.
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

            // Calculate new end epoch with all caps applied.
            // Cap 1: Policy max extension.
            let capped_extension = if (requested_extension_epochs > *max_extension_epochs) {
                *max_extension_epochs
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
        },
    }
}

// === Tests ===

#[test]
fun test_disabled_policy_creation() {
    let _policy = disabled();
    // Policy created successfully.
}

#[test]
fun test_constrained_policy_creation() {
    let _policy = constrained(1, 5);
    // Policy created successfully.
}

#[test, expected_failure(abort_code = EExtensionDisabled)]
fun test_disabled_policy_aborts() {
    let policy = disabled();
    validate_and_compute_end_epoch(&policy, 99, 100, 5, 52);
}

#[test, expected_failure(abort_code = EExtensionTooEarly)]
fun test_constrained_too_early_aborts() {
    let policy = constrained(1, 5);
    // Threshold is 99, current is 98 - too early.
    validate_and_compute_end_epoch(&policy, 98, 100, 5, 52);
}

#[test]
fun test_constrained_at_threshold() {
    let policy = constrained(1, 5);
    // Threshold is 99, current is 99 - allowed.
    // end=100, extension=5, new_end=105.
    let result = validate_and_compute_end_epoch(&policy, 99, 100, 5, 52);
    assert!(result == 105);
}

#[test]
fun test_constrained_caps_to_policy_max() {
    let policy = constrained(1, 5);
    // Requesting 10, but policy max is 5.
    // end=100, capped_extension=5, new_end=105.
    let result = validate_and_compute_end_epoch(&policy, 99, 100, 10, 52);
    assert!(result == 105);
}

#[test]
fun test_constrained_caps_to_system_max() {
    // Policy: extend only within 1 epoch of expiry, max 100 epochs.
    // Using high policy max to test system cap.
    let policy = constrained(1, 100);

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
    let policy2 = constrained(1, 10);
    // current=99, end=100, threshold=99, within window.
    // requested=100, policy_capped=10, new_end=110.
    // system_max_end=151, so new_end=110 (no system cap needed).
    let result3 = validate_and_compute_end_epoch(&policy2, 99, 100, 100, 52);
    assert!(result3 == 110);
}

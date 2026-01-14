// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/// Storage purchase policy controls both capacity purchases and time extensions
/// for BlobManager storage. This prevents abuse and incentivizes community participation.
module blob_manager::storage_purchase_policy;

// === Error Codes ===

/// Error when available storage exceeds threshold for conditional purchase.
const EStorageAvailableExceedsThreshold: u64 = 1;

/// Extension is not allowed yet - storage hasn't reached the expiry threshold.
const EExtensionTooEarly: u64 = 2;

// === Constants ===

/// Default threshold for constrained purchase: 15GB in bytes.
const DEFAULT_CONSTRAINED_THRESHOLD_BYTES: u64 = 15_000_000_000;

/// Default max purchase for constrained purchase: 15GB in bytes.
const DEFAULT_CONSTRAINED_MAX_PURCHASE_BYTES: u64 = 15_000_000_000;

/// Conversion factor: 1 WAL = 1_000_000_000 FROST.
const WAL_TO_FROST: u64 = 1_000_000_000;

// === Tip Policy Structure ===

/// Tip policy for rewarding users who execute storage extensions.
/// Simple fixed tip with optional last-epoch multiplier.
public struct TipPolicy has copy, drop, store {
    /// Base tip amount in FROST. E.g., 10_000_000_000 = 10 WAL.
    tip_amount: u64,
    /// Multiplier for last epoch as literal value (e.g., 2 = 2x, 1 = 1x to disable).
    /// Applied when current_epoch == storage_end_epoch - 1.
    last_epoch_multiplier: u64,
}

// === Main Structure ===

/// Policy for controlling storage purchases and extensions.
public struct StoragePurchasePolicy has copy, drop, store {
    /// Policy for capacity purchases.
    capacity_policy: CapacityPolicy,
    /// Extension only allowed when current epoch >= end_epoch - expiry_threshold_epochs.
    expiry_threshold_epochs: u32,
    /// Maximum epochs that can be extended in a single call. Set to 0 to disable extensions.
    max_extension_epochs: u32,
    /// Tip policy for rewarding users who execute extensions. Tips are paid in WAL.
    tip_policy: TipPolicy,
}

/// Policy for capacity purchase controls.
public enum CapacityPolicy has copy, drop, store {
    /// No limit on storage purchases.
    Unlimited,
    /// Constrained purchase: only allow purchases when available storage < threshold.
    /// Max purchase amount is capped at max_purchase_bytes.
    Constrained {
        /// Threshold for available storage.
        /// Purchases only allowed when available < threshold_bytes.
        threshold_bytes: u64,
        /// Maximum storage size in bytes that can be purchased in a single call.
        max_purchase_bytes: u64,
    },
}

// === Constructors ===

/// Creates the default storage purchase policy.
/// - Capacity: constrained with 15GB threshold and max
/// - Extensions: allowed within 2 epochs of expiry, max 5 epochs
/// - Tip: 10 WAL base with 2x multiplier in last epoch
public(package) fun default(): StoragePurchasePolicy {
    StoragePurchasePolicy {
        capacity_policy: CapacityPolicy::Constrained {
            threshold_bytes: DEFAULT_CONSTRAINED_THRESHOLD_BYTES,
            max_purchase_bytes: DEFAULT_CONSTRAINED_MAX_PURCHASE_BYTES,
        },
        expiry_threshold_epochs: 2,
        max_extension_epochs: 5,
        tip_policy: TipPolicy {
            tip_amount: 10_000_000_000, // 10 WAL base tip
            last_epoch_multiplier: 2, // 2x in last epoch
        },
    }
}

/// Creates a simple storage purchase policy.
/// - Capacity: unlimited
/// - Extensions: allowed within 2 epochs of expiry, max 5 epochs
/// - Tip: fixed amount with multiplier
public(package) fun simple(
    tip_amount_frost: u64,
    last_epoch_multiplier: u64,
): StoragePurchasePolicy {
    StoragePurchasePolicy {
        capacity_policy: CapacityPolicy::Unlimited,
        expiry_threshold_epochs: 2,
        max_extension_epochs: 5,
        tip_policy: TipPolicy {
            tip_amount: tip_amount_frost,
            last_epoch_multiplier,
        },
    }
}

/// Creates a fully customized storage purchase policy.
public(package) fun custom(
    capacity_policy: CapacityPolicy,
    expiry_threshold_epochs: u32,
    max_extension_epochs: u32,
    tip_amount_frost: u64,
    last_epoch_multiplier: u64,
): StoragePurchasePolicy {
    StoragePurchasePolicy {
        capacity_policy,
        expiry_threshold_epochs,
        max_extension_epochs,
        tip_policy: TipPolicy {
            tip_amount: tip_amount_frost,
            last_epoch_multiplier,
        },
    }
}

// === Capacity Purchase Functions ===

/// Validates a storage capacity purchase request and returns the allowed amount.
/// - For Unlimited: returns the requested amount as-is.
/// - For Constrained: checks that available_storage < threshold, then caps at max_purchase_bytes.
///
/// Aborts with EStorageAvailableExceedsThreshold if Constrained threshold is exceeded.
public(package) fun validate_purchase(
    self: &StoragePurchasePolicy,
    requested_amount: u64,
    available_storage: u64,
): u64 {
    match (&self.capacity_policy) {
        CapacityPolicy::Unlimited => requested_amount,
        CapacityPolicy::Constrained { threshold_bytes, max_purchase_bytes } => {
            // Only allow purchase when available storage is below threshold.
            assert!(available_storage < *threshold_bytes, EStorageAvailableExceedsThreshold);
            // Cap at max_purchase_bytes.
            if (requested_amount > *max_purchase_bytes) {
                *max_purchase_bytes
            } else {
                requested_amount
            }
        },
    }
}

// === Extension Functions ===

/// Validates extension request and computes the new end epoch.
/// All extensions must follow policy constraints.
///
/// Aborts if extension is too early (before expiry threshold).
///
/// Returns the new end_epoch (capped by both policy and system limits).
/// If max_extension_epochs is 0, the storage_end_epoch is returned unchanged.
public(package) fun validate_and_compute_end_epoch(
    policy: &StoragePurchasePolicy,
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

/// Calculates the tip amount in FROST based on the policy.
/// Applies last epoch multiplier if currently in the last epoch before expiry.
public fun calculate_tip(
    policy: &StoragePurchasePolicy,
    used_bytes: u64,
    current_timestamp_ms: u64,
    current_epoch: u32,
    current_epoch_start_ms: u64,
    epoch_duration_ms: u64,
    storage_end_epoch: u32,
): u64 {
    // Unused parameters in simplified model
    let _ = used_bytes;
    let _ = current_timestamp_ms;
    let _ = current_epoch_start_ms;
    let _ = epoch_duration_ms;

    let tip = &policy.tip_policy;

    // Check if we're in the last epoch
    let is_last_epoch = storage_end_epoch > 0 && current_epoch == storage_end_epoch - 1;

    if (is_last_epoch && tip.last_epoch_multiplier > 1) {
        // Apply multiplier for last epoch
        tip.tip_amount * tip.last_epoch_multiplier
    } else {
        tip.tip_amount
    }
}

// === Accessors ===

/// Gets the tip policy.
public fun tip_policy(policy: &StoragePurchasePolicy): &TipPolicy {
    &policy.tip_policy
}

/// Gets the expiry threshold epochs.
public fun expiry_threshold_epochs(policy: &StoragePurchasePolicy): u32 {
    policy.expiry_threshold_epochs
}

/// Gets the max extension epochs.
public fun max_extension_epochs(policy: &StoragePurchasePolicy): u32 {
    policy.max_extension_epochs
}

// === Setters (for admin configuration) ===

/// Sets unlimited capacity policy (no purchase limits).
public(package) fun set_capacity_unlimited(self: &mut StoragePurchasePolicy) {
    self.capacity_policy = CapacityPolicy::Unlimited;
}

/// Sets constrained capacity policy.
public(package) fun set_capacity_constrained(
    self: &mut StoragePurchasePolicy,
    threshold_bytes: u64,
    max_purchase_bytes: u64,
) {
    self.capacity_policy = CapacityPolicy::Constrained { threshold_bytes, max_purchase_bytes };
}

/// Sets extension parameters.
public(package) fun set_extension_params(
    self: &mut StoragePurchasePolicy,
    expiry_threshold_epochs: u32,
    max_extension_epochs: u32,
    tip_amount_frost: u64,
    last_epoch_multiplier: u64,
) {
    self.expiry_threshold_epochs = expiry_threshold_epochs;
    self.max_extension_epochs = max_extension_epochs;
    self.tip_policy =
        TipPolicy {
            tip_amount: tip_amount_frost,
            last_epoch_multiplier,
        };
}

// === Tests ===

#[test]
fun test_unlimited_capacity() {
    let policy = simple(10 * WAL_TO_FROST, 2);

    // Any size should be allowed, regardless of available storage.
    assert!(policy.validate_purchase(1000, 0) == 1000);
    assert!(policy.validate_purchase(1_000_000_000, 0) == 1_000_000_000);
    assert!(policy.validate_purchase(1_000_000_000_000, 999_999_999_999) == 1_000_000_000_000);
}

#[test]
fun test_constrained_capacity() {
    let policy = default();

    // Default is 15GB threshold and 15GB max purchase.
    let default_value = 15_000_000_000;

    // Should work when available < threshold.
    assert!(policy.validate_purchase(1000, 0) == 1000);
    assert!(policy.validate_purchase(default_value, 0) == default_value);

    // Should cap at max_purchase.
    assert!(policy.validate_purchase(default_value * 2, 0) == default_value);
}

#[test]
#[expected_failure(abort_code = EStorageAvailableExceedsThreshold)]
fun test_constrained_blocks_when_threshold_exceeded() {
    let policy = default();
    let threshold = 15_000_000_000;

    // Should abort when available storage >= threshold.
    policy.validate_purchase(1000, threshold);
}

#[test]
fun test_extension_validation() {
    let policy = default();

    // Default: expiry_threshold=2, max_extension=5

    // Not in threshold window (current=97, end=100, threshold=98).
    // This should fail in real test but we're just testing the computation here.

    // In threshold window (current=98, end=100).
    // Extension of 5 epochs: new_end = 100 + 5 = 105.
    let result = validate_and_compute_end_epoch(&policy, 98, 100, 5, 52);
    assert!(result == 105);

    // Request 10 epochs but capped at 5: new_end = 100 + 5 = 105.
    let result = validate_and_compute_end_epoch(&policy, 98, 100, 10, 52);
    assert!(result == 105);
}

#[test]
fun test_tip_calculation() {
    let policy = default();

    // Default: 10 WAL base with 2x in last epoch

    // Not in last epoch (current=50, end=100).
    assert!(calculate_tip(&policy, 0, 0, 50, 0, 10000, 100) == 10 * WAL_TO_FROST);

    // In last epoch (current=99, end=100).
    assert!(calculate_tip(&policy, 0, 0, 99, 0, 10000, 100) == 20 * WAL_TO_FROST);
}

#[test]
#[expected_failure(abort_code = EExtensionTooEarly)]
fun test_extension_too_early() {
    let policy = default();

    // Current=96, end=100, threshold=98 - too early.
    validate_and_compute_end_epoch(&policy, 96, 100, 5, 52);
}

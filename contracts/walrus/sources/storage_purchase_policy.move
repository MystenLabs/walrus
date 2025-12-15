// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/// Storage purchase policy controls the amount of storage that can be purchased
/// from the BlobManager's coin stash. This prevents abuse by limiting how much
/// storage a malicious writer could purchase to drain the funds.
module walrus::storage_purchase_policy;

// === Error Codes ===

/// Error when available storage exceeds threshold for conditional purchase.
const EStorageAvailableExceedsThreshold: u64 = 1;

// === Constants ===

/// Default threshold for constrained purchase: 15GB in bytes.
const DEFAULT_CONSTRAINED_THRESHOLD_BYTES: u64 = 15_000_000_000;

/// Default max purchase for constrained purchase: 15GB in bytes.
const DEFAULT_CONSTRAINED_MAX_PURCHASE_BYTES: u64 = 15_000_000_000;

// === Structs ===

/// Policy for controlling storage purchases.
public enum StoragePurchasePolicy has copy, drop, store {
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

// === Constructor ===

/// Create an unlimited storage purchase policy.
public(package) fun unlimited(): StoragePurchasePolicy {
    StoragePurchasePolicy::Unlimited
}

/// Create a constrained storage purchase policy.
/// Only allows purchases when available storage < threshold_bytes.
/// Maximum purchase is capped at max_purchase_bytes.
public(package) fun constrained(
    threshold_bytes: u64,
    max_purchase_bytes: u64,
): StoragePurchasePolicy {
    StoragePurchasePolicy::Constrained { threshold_bytes, max_purchase_bytes }
}

/// Creates a storage purchase policy with the default constrained parameters (15GB threshold, 15GB
/// max). Only allows purchases when available storage < 15GB, max purchase is 15GB.
public(package) fun default_constrained(): StoragePurchasePolicy {
    StoragePurchasePolicy::Constrained {
        threshold_bytes: DEFAULT_CONSTRAINED_THRESHOLD_BYTES,
        max_purchase_bytes: DEFAULT_CONSTRAINED_MAX_PURCHASE_BYTES,
    }
}

// === Core Functions ===

/// Validates a storage purchase request and returns the allowed amount.
/// - For Unlimited: returns the requested amount as-is.
/// - For Constrained: checks that available_storage < threshold, then caps at max_purchase_bytes.
///
/// Aborts with EStorageAvailableExceedsThreshold if Constrained threshold is exceeded.
public(package) fun validate_purchase(
    self: &StoragePurchasePolicy,
    requested_amount: u64,
    available_storage: u64,
): u64 {
    match (self) {
        StoragePurchasePolicy::Unlimited => requested_amount,
        StoragePurchasePolicy::Constrained { threshold_bytes, max_purchase_bytes } => {
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

#[test]
fun test_unlimited_policy() {
    let policy = unlimited();

    // Any size should be allowed, regardless of available storage.
    assert!(policy.validate_purchase(1000, 0) == 1000);
    assert!(policy.validate_purchase(1_000_000_000, 0) == 1_000_000_000);
    assert!(policy.validate_purchase(1_000_000_000_000, 999_999_999_999) == 1_000_000_000_000);
}

#[test]
fun test_constrained_policy() {
    let threshold = 15_000_000_000; // 15GB
    let max_purchase = 10_000_000_000; // 10GB
    let policy = constrained(threshold, max_purchase);

    // Under max_purchase should work when available < threshold.
    assert!(policy.validate_purchase(1000, 0) == 1000);
    assert!(policy.validate_purchase(max_purchase, 0) == max_purchase);

    // Over max_purchase should be capped.
    assert!(policy.validate_purchase(max_purchase + 1, 0) == max_purchase);
    assert!(policy.validate_purchase(max_purchase * 2, 0) == max_purchase);

    // Should work when available is just under threshold.
    assert!(policy.validate_purchase(1000, threshold - 1) == 1000);
}

#[test]
fun test_default_constrained_policy() {
    let policy = default_constrained();

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
fun test_constrained_blocks_when_available_equals_threshold() {
    let threshold = 15_000_000_000; // 15GB
    let max_purchase = 10_000_000_000; // 10GB
    let policy = constrained(threshold, max_purchase);

    // Should abort when available storage == threshold.
    policy.validate_purchase(1000, threshold);
}

#[test]
#[expected_failure(abort_code = EStorageAvailableExceedsThreshold)]
fun test_constrained_blocks_when_available_exceeds_threshold() {
    let threshold = 15_000_000_000; // 15GB
    let max_purchase = 10_000_000_000; // 10GB
    let policy = constrained(threshold, max_purchase);

    // Should abort when available storage > threshold.
    policy.validate_purchase(1000, threshold + 1);
}

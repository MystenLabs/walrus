// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/// Storage purchase policy controls the amount of storage that can be purchased
/// from the BlobManager's coin stash. This prevents abuse by limiting how much
/// storage a malicious writer could purchase to drain the funds.
module walrus::storage_purchase_policy;

// === Error Codes ===

/// Error when trying to purchase more than the allowed maximum.
const EStoragePurchaseExceedsLimit: u64 = 0;
/// Error when available storage exceeds threshold for conditional purchase.
const EStorageAvailableExceedsThreshold: u64 = 1;

// === Constants ===

/// Default threshold for conditional purchase: 15GB in bytes.
const DEFAULT_CONDITIONAL_PURCHASE_THRESHOLD_BYTES: u64 = 15_000_000_000;

// === Structs ===

/// Policy for controlling storage purchases.
public enum StoragePurchasePolicy has copy, drop, store {
    /// No limit on storage purchases.
    Unlimited,
    /// Fixed cap on the total amount of storage that can be purchased.
    FixedCap {
        /// Maximum storage size in bytes that can be purchased.
        max_storage_bytes: u64,
    },
    /// Conditional purchase: only allow purchases when available storage < threshold.
    /// Max purchase amount is capped at threshold.
    ConditionalPurchase {
        /// Threshold for available storage (e.g., 15GB).
        /// Purchases only allowed when available < threshold.
        threshold_bytes: u64,
    },
}

// === Constructor ===

/// Create an unlimited storage purchase policy.
public(package) fun unlimited(): StoragePurchasePolicy {
    StoragePurchasePolicy::Unlimited
}

/// Create a fixed cap storage purchase policy.
public(package) fun fixed_cap(max_storage_bytes: u64): StoragePurchasePolicy {
    StoragePurchasePolicy::FixedCap { max_storage_bytes }
}

/// Create a conditional purchase policy.
/// Only allows purchases when available storage < threshold_bytes.
/// Maximum purchase is capped at threshold_bytes.
public(package) fun conditional_purchase(threshold_bytes: u64): StoragePurchasePolicy {
    StoragePurchasePolicy::ConditionalPurchase { threshold_bytes }
}

/// Creates a storage purchase policy with the default conditional purchase threshold (15GB).
/// Only allows purchases when available storage < 15GB, max purchase is 15GB.
public(package) fun default_conditional_purchase(): StoragePurchasePolicy {
    StoragePurchasePolicy::ConditionalPurchase {
        threshold_bytes: DEFAULT_CONDITIONAL_PURCHASE_THRESHOLD_BYTES,
    }
}

// === Core Functions ===

/// Validate that a storage purchase request is within policy limits.
/// Returns the allowed storage size (which may be capped).
public(package) fun validate_and_cap_purchase(
    policy: &StoragePurchasePolicy,
    requested_storage_bytes: u64,
): u64 {
    match (policy) {
        StoragePurchasePolicy::Unlimited => requested_storage_bytes,
        StoragePurchasePolicy::FixedCap { max_storage_bytes } => {
            // Cap the purchase to the maximum allowed.
            if (requested_storage_bytes > *max_storage_bytes) {
                *max_storage_bytes
            } else {
                requested_storage_bytes
            }
        },
        StoragePurchasePolicy::ConditionalPurchase { threshold_bytes } => {
            // For conditional purchase, cap at the threshold.
            // The availability check happens separately in validate_conditional_purchase.
            if (requested_storage_bytes > *threshold_bytes) {
                *threshold_bytes
            } else {
                requested_storage_bytes
            }
        },
    }
}

/// Check if a storage purchase would exceed the policy limit.
/// Aborts if the purchase exceeds the limit.
public(package) fun enforce_limit(policy: &StoragePurchasePolicy, requested_storage_bytes: u64) {
    match (policy) {
        StoragePurchasePolicy::Unlimited => {},
        StoragePurchasePolicy::FixedCap { max_storage_bytes } => {
            assert!(requested_storage_bytes <= *max_storage_bytes, EStoragePurchaseExceedsLimit);
        },
        StoragePurchasePolicy::ConditionalPurchase { threshold_bytes } => {
            assert!(requested_storage_bytes <= *threshold_bytes, EStoragePurchaseExceedsLimit);
        },
    }
}

/// Validate that a conditional purchase is allowed based on available storage.
/// This function should be called before purchasing storage with ConditionalPurchase policy.
/// Aborts if available storage is >= threshold.
public(package) fun validate_conditional_purchase(
    policy: &StoragePurchasePolicy,
    available_storage: u64,
) {
    match (policy) {
        StoragePurchasePolicy::Unlimited => {},
        StoragePurchasePolicy::FixedCap { max_storage_bytes: _ } => {},
        StoragePurchasePolicy::ConditionalPurchase { threshold_bytes } => {
            assert!(available_storage < *threshold_bytes, EStorageAvailableExceedsThreshold);
        },
    }
}

#[test]
fun test_unlimited_policy() {
    let policy = unlimited();

    // Any size should be allowed.
    assert!(validate_and_cap_purchase(&policy, 1000) == 1000);
    assert!(validate_and_cap_purchase(&policy, 1_000_000_000) == 1_000_000_000);

    // Enforcement should always pass.
    enforce_limit(&policy, 1_000_000_000_000);
}

#[test]
fun test_fixed_cap_policy() {
    let max_bytes = 15_000_000_000; // 15GB
    let policy = fixed_cap(max_bytes);

    // Under limit should work.
    assert!(validate_and_cap_purchase(&policy, 1000) == 1000);
    assert!(validate_and_cap_purchase(&policy, max_bytes) == max_bytes);

    // Over limit should be capped.
    assert!(validate_and_cap_purchase(&policy, max_bytes + 1) == max_bytes);
    assert!(validate_and_cap_purchase(&policy, max_bytes * 2) == max_bytes);

    // Enforcement should pass under limit.
    enforce_limit(&policy, max_bytes);
}

#[test]
#[expected_failure(abort_code = EStoragePurchaseExceedsLimit)]
fun test_fixed_cap_enforcement_fails() {
    let max_bytes = 15_000_000_000; // 15GB
    let policy = fixed_cap(max_bytes);

    // Should abort when over limit.
    enforce_limit(&policy, max_bytes + 1);
}

#[test]
fun test_conditional_purchase_policy() {
    let threshold = 15_000_000_000; // 15GB
    let policy = conditional_purchase(threshold);

    // Test validate_and_cap_purchase - should cap at threshold.
    assert!(validate_and_cap_purchase(&policy, 1000) == 1000);
    assert!(validate_and_cap_purchase(&policy, threshold) == threshold);
    assert!(validate_and_cap_purchase(&policy, threshold * 2) == threshold);

    // Test validate_conditional_purchase - should pass when available < threshold.
    validate_conditional_purchase(&policy, 0);
    validate_conditional_purchase(&policy, threshold - 1);

    // Test enforcement - should pass when under threshold.
    enforce_limit(&policy, threshold);
}

#[test]
#[expected_failure(abort_code = EStorageAvailableExceedsThreshold)]
fun test_conditional_purchase_blocks_when_available_high() {
    let threshold = 15_000_000_000; // 15GB
    let policy = conditional_purchase(threshold);

    // Should abort when available storage >= threshold.
    validate_conditional_purchase(&policy, threshold);
}

#[test]
#[expected_failure(abort_code = EStorageAvailableExceedsThreshold)]
fun test_conditional_purchase_blocks_when_available_exceeds() {
    let threshold = 15_000_000_000; // 15GB
    let policy = conditional_purchase(threshold);

    // Should abort when available storage > threshold.
    validate_conditional_purchase(&policy, threshold + 1);
}

#[test]
#[expected_failure(abort_code = EStoragePurchaseExceedsLimit)]
fun test_conditional_purchase_enforcement_fails() {
    let threshold = 15_000_000_000; // 15GB
    let policy = conditional_purchase(threshold);

    // Should abort when requested amount > threshold.
    enforce_limit(&policy, threshold + 1);
}

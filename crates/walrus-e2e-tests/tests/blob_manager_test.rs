// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

// Allowing `unwrap`s in tests.
#![allow(clippy::unwrap_used)]
// This avoids the following error when running simtests:
// error[E0275]: overflow evaluating the requirement `{coroutine ...}: std::marker::Send`
#![recursion_limit = "256"]

//! Contains end-to-end tests for mixed regular and managed blob storage.
//!
//! These tests validate the interaction between regular blob storage and BlobManager-managed blobs,
//! including:
//! - Storing the same blob content across multiple storage contexts.
//! - Verifying blob status aggregation from storage nodes.
//! - Testing deletion scenarios and their effect on blob availability.
//! - Verifying combined `DeletableCounts` updates and storage release via `get_storage_info()`.

use std::{sync::Arc, time::Duration};

use sui_types::base_types::ObjectID;
use walrus_core::{BlobId, DEFAULT_ENCODING, Epoch, encoding::Primary};
use walrus_proc_macros::walrus_simtest;
use walrus_sdk::{
    client::{
        StoreArgs,
        WalrusNodeClient,
        client_types::{
            BlobWithStatus,
            WalrusStoreBlob,
            WalrusStoreBlobMaybeFinished,
            WalrusStoreBlobState,
            WalrusStoreBlobUnfinished,
            WalrusStoreEncodedBlobApi,
            partition_unfinished_finished,
        },
        responses::BlobStoreResult,
    },
    error::ClientErrorKind,
};
use walrus_service::test_utils::{TestNodesConfig, test_cluster};
use walrus_storage_node_client::api::{BlobStatus, DeletableCounts};
use walrus_sui::{
    client::{BlobPersistence, SuiContractClient},
    test_utils,
    types::move_structs::BlobAttribute,
    wallet::Wallet,
};
use walrus_test_utils::{Result as TestResult, WithTempDir};

// =============================================================================
// Data Structures for Tracking Test State
// =============================================================================

// These data structures are designed for future randomized test cases.
// They provide a framework for tracking blob instances across multiple storage contexts.
#[allow(dead_code)]
/// The storage context for a blob instance.
#[derive(Debug, Clone)]
enum StorageContext {
    /// Regular blob storage (user's wallet).
    Regular {
        object_id: ObjectID,
        end_epoch: Epoch,
    },

    /// Managed by BlobManager B.
    BlobManager { manager: Arc<TestBlobManager> },
}

#[allow(dead_code)]
#[derive(Debug)]
struct TestBlobManager {
    id: ObjectID,
    end_epoch: Epoch,
}

/// Tracks a single blob instance in a storage context.
#[allow(dead_code)]
#[derive(Debug, Clone)]
struct TestBlobInstance {
    /// The storage context for this instance.
    context: StorageContext,
    /// Whether this instance is deletable.
    deletable: bool,
    /// Whether this instance has been deleted.
    deleted: bool,
}

impl TestBlobInstance {
    fn end_epoch(&self) -> Epoch {
        match &self.context {
            StorageContext::Regular { end_epoch, .. } => *end_epoch,
            StorageContext::BlobManager { manager } => manager.end_epoch,
        }
    }
}

/// Aggregated info for a single blob_id across all storage contexts.
#[allow(dead_code)]
#[derive(Debug)]
struct TestAggregatedBlobInfo {
    /// The blob ID (content hash).
    blob_id: BlobId,
    /// Original test data.
    test_data: Vec<u8>,
    /// All instances of this blob.
    instances: Vec<TestBlobInstance>,
}

#[allow(dead_code)]
impl TestAggregatedBlobInfo {
    /// Creates a new TestAggregatedBlobInfo with no instances.
    fn new(blob_id: BlobId, test_data: Vec<u8>) -> Self {
        Self {
            blob_id,
            test_data,
            instances: Vec::new(),
        }
    }

    /// Adds an instance to the aggregated info.
    fn add_instance(&mut self, context: StorageContext, deletable: bool) {
        self.instances.push(TestBlobInstance {
            context,
            deletable,
            deleted: false,
        });
    }

    /// Marks a managed blob instance as deleted for the given manager.
    /// Only marks deletable instances. Returns true if an instance was marked.
    fn mark_deleted_for_manager(&mut self, manager: &Arc<TestBlobManager>) -> bool {
        for instance in &mut self.instances {
            if let StorageContext::BlobManager { manager: mgr } = &instance.context
                && Arc::ptr_eq(mgr, manager)
                && instance.deletable
                && !instance.deleted
            {
                instance.deleted = true;
                return true;
            }
        }
        false
    }

    /// Returns the expected max end_epoch from all non-deleted instances.
    fn expected_end_epoch(&self) -> Option<Epoch> {
        self.instances
            .iter()
            .filter(|i| !i.deleted)
            .map(|i| i.end_epoch())
            .max()
    }

    /// Returns true if there are any non-deleted permanent instances (regular or managed).
    fn has_permanent(&self) -> bool {
        self.instances.iter().any(|i| !i.deleted && !i.deletable)
    }

    /// Returns true if all non-deleted instances are managed (no regular blobs).
    fn is_managed_only(&self) -> bool {
        let non_deleted: Vec<_> = self.instances.iter().filter(|i| !i.deleted).collect();
        !non_deleted.is_empty()
            && non_deleted
                .iter()
                .all(|i| matches!(i.context, StorageContext::BlobManager { .. }))
    }

    /// Returns count of non-deleted managed instances (total registered).
    #[allow(clippy::cast_possible_truncation)]
    fn expected_managed_registered_total(&self) -> u32 {
        self.instances
            .iter()
            .filter(|i| !i.deleted && matches!(i.context, StorageContext::BlobManager { .. }))
            .count() as u32
    }

    /// Returns count of non-deleted deletable managed instances.
    #[allow(clippy::cast_possible_truncation)]
    fn expected_managed_registered_deletable(&self) -> u32 {
        self.instances
            .iter()
            .filter(|i| {
                !i.deleted && i.deletable && matches!(i.context, StorageContext::BlobManager { .. })
            })
            .count() as u32
    }

    /// Returns count of non-deleted deletable regular instances.
    #[allow(clippy::cast_possible_truncation)]
    fn expected_deletable_total(&self) -> u32 {
        self.instances
            .iter()
            .filter(|i| {
                !i.deleted && i.deletable && matches!(i.context, StorageContext::Regular { .. })
            })
            .count() as u32
    }

    /// Returns true if there are no non-deleted instances.
    fn is_nonexistent(&self) -> bool {
        self.instances.iter().all(|i| i.deleted)
    }

    /// Returns the expected end_epoch for permanent instances only (regular permanent blobs).
    /// BlobStatus::Permanent's end_epoch only tracks permanent blob end_epoch, not managed blobs.
    fn expected_permanent_end_epoch(&self) -> Option<Epoch> {
        self.instances
            .iter()
            .filter(|i| !i.deleted && !i.deletable)
            .filter(|i| matches!(i.context, StorageContext::Regular { .. }))
            .map(|i| i.end_epoch())
            .max()
    }

    /// Returns the expected end_epoch for permanent managed blobs.
    /// Permanent managed blobs contribute to `Permanent` status end_epoch.
    fn expected_managed_permanent_end_epoch(&self) -> Option<Epoch> {
        self.instances
            .iter()
            .filter(|i| !i.deleted && !i.deletable)
            .filter(|i| matches!(i.context, StorageContext::BlobManager { .. }))
            .map(|i| i.end_epoch())
            .max()
    }

    /// Returns true if there are any non-deleted permanent managed blobs.
    fn has_managed_permanent(&self) -> bool {
        self.instances.iter().any(|i| {
            !i.deleted && !i.deletable && matches!(i.context, StorageContext::BlobManager { .. })
        })
    }

    /// Verifies the BlobStatus matches the expected state based on tracked instances.
    /// With the new combined status model:
    /// - Permanent managed blobs contribute to `Permanent` status with their `end_epoch`.
    /// - Deletable managed blobs contribute to `DeletableCounts`.
    fn verify_status(&self, status: &BlobStatus) {
        let expect_permanent = self.has_permanent() || self.has_managed_permanent();
        let expect_nonexistent = self.is_nonexistent();
        let expected_managed_deletable = self.expected_managed_registered_deletable();
        let expected_regular_deletable = self.expected_deletable_total();
        let expect_deletable = !expect_permanent
            && !expect_nonexistent
            && (expected_managed_deletable > 0 || expected_regular_deletable > 0);

        tracing::info!(
            "Verifying status for blob_id={}: expect_permanent={}, \
            expect_deletable={}, expect_nonexistent={}, expected_managed_deletable={}",
            self.blob_id,
            expect_permanent,
            expect_deletable,
            expect_nonexistent,
            expected_managed_deletable
        );

        // Verify variant using public APIs.
        assert_eq!(
            status.is_permanent(),
            expect_permanent,
            "is_permanent mismatch: expected {}, got {} (status={:?})",
            expect_permanent,
            status.is_permanent(),
            status
        );
        assert_eq!(
            status.is_registered(),
            expect_permanent || expect_deletable,
            "is_registered mismatch: expected {}, got {} (status={:?})",
            expect_permanent || expect_deletable,
            status.is_registered(),
            status
        );

        if expect_nonexistent {
            assert!(
                !status.is_registered(),
                "Expected Nonexistent status, got {:?}",
                status
            );
            return;
        }

        // Verify end_epoch for permanent status.
        // With long epoch duration (200s), we can verify the exact end_epoch.
        if expect_permanent {
            let expected_regular_end = self.expected_permanent_end_epoch();
            let expected_managed_end = self.expected_managed_permanent_end_epoch();
            let expected_end = expected_regular_end.max(expected_managed_end);
            if let Some(expected) = expected_end {
                let actual_end = status.end_epoch().expect("Permanent should have end_epoch");
                assert_eq!(
                    actual_end, expected,
                    "End epoch mismatch: expected {}, got {}",
                    expected, actual_end
                );
            }
        }

        // Verify deletable counts using public API.
        let deletable_counts = status.deletable_counts();
        if expect_permanent {
            // Managed deletable counts are combined into deletable_counts.
            assert!(
                deletable_counts.count_deletable_total >= expected_managed_deletable,
                "Deletable count should include managed deletables: {} >= {}",
                deletable_counts.count_deletable_total,
                expected_managed_deletable
            );
        } else if expect_deletable {
            // Only deletable instances (regular or managed).
            let expected_total = expected_regular_deletable + expected_managed_deletable;
            assert!(
                deletable_counts.count_deletable_total >= expected_total,
                "Deletable count mismatch: expected >= {}, got {}",
                expected_total,
                deletable_counts.count_deletable_total
            );
        }
    }
}

/// Helper to check if two StorageContext match.
/// For Regular contexts, matches if both are Regular with same object_id.
/// For BlobManager contexts, matches if both reference the same manager (by Arc pointer equality).
#[allow(dead_code)]
fn matches_context(a: &StorageContext, b: &StorageContext) -> bool {
    match (a, b) {
        (
            StorageContext::Regular {
                object_id: id_a, ..
            },
            StorageContext::Regular {
                object_id: id_b, ..
            },
        ) => id_a == id_b,
        (
            StorageContext::BlobManager { manager: mgr_a },
            StorageContext::BlobManager { manager: mgr_b },
        ) => Arc::ptr_eq(mgr_a, mgr_b),
        _ => false,
    }
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Stores a regular blob (not through BlobManager).
/// Use `force = true` to disable optimizations and store duplicate.
async fn store_regular_blob(
    client: &WalrusNodeClient<SuiContractClient>,
    data: &[u8],
    persistence: BlobPersistence,
    epochs_ahead: u32,
    force: bool,
) -> TestResult<(BlobId, Epoch)> {
    let mut store_args = StoreArgs::default_with_epochs(epochs_ahead).with_persistence(persistence);

    if force {
        store_args = store_args.no_store_optimizations();
    }

    let results = client
        .reserve_and_store_blobs_retry_committees(vec![data.to_vec()], vec![], &store_args)
        .await?;

    assert_eq!(results.len(), 1, "Should have one result");
    let blob_id = results[0].blob_id().expect("blob ID should be present");

    // Get end_epoch from the blob object.
    let end_epoch = results[0]
        .end_epoch()
        .expect("end_epoch should be present for regular blob");

    Ok((blob_id, end_epoch))
}

/// Stores a blob through the specified BlobManager.
async fn store_managed_blob(
    client: &mut WalrusNodeClient<SuiContractClient>,
    cap_id: ObjectID,
    data: &[u8],
    persistence: BlobPersistence,
    epochs_ahead: u32,
) -> TestResult<BlobId> {
    // Initialize the blob manager with the capability.
    client.init_blob_manager(cap_id).await?;

    let store_args = StoreArgs::default_with_epochs(epochs_ahead)
        .with_blob_manager()
        .with_persistence(persistence);

    let results = client
        .reserve_and_store_blobs_retry_committees(vec![data.to_vec()], vec![], &store_args)
        .await?;

    assert_eq!(results.len(), 1, "Should have one result");
    Ok(results[0].blob_id().expect("blob ID should be present"))
}

/// Gets blob status and verifies it matches expected state.
async fn get_and_verify_status(
    client: &WalrusNodeClient<SuiContractClient>,
    blob_id: &BlobId,
) -> TestResult<BlobStatus> {
    let status = client
        .get_verified_blob_status(
            blob_id,
            client.sui_client().read_client(),
            Duration::from_secs(5),
        )
        .await?;
    Ok(status)
}

/// Verifies that storage was released after blob deletion.
#[allow(dead_code)]
async fn verify_storage_released(
    client: &mut WalrusNodeClient<SuiContractClient>,
    cap_id: ObjectID,
    expected_increase: u64,
    available_before: u64,
) -> TestResult<()> {
    client.init_blob_manager(cap_id).await?;
    let storage_after = client.get_blob_manager_client()?.get_storage_info().await?;

    assert_eq!(
        storage_after.available_capacity,
        available_before + expected_increase,
        "Storage should be released after deletion"
    );
    Ok(())
}

/// Extracts DeletableCounts from BlobStatus.
/// With the combined status model, managed deletable counts are included in deletable_counts.
fn extract_deletable_counts(status: &BlobStatus) -> DeletableCounts {
    match status {
        BlobStatus::Permanent {
            deletable_counts, ..
        } => *deletable_counts,
        BlobStatus::Deletable {
            deletable_counts, ..
        } => *deletable_counts,
        _ => DeletableCounts::default(),
    }
}

/// Reads blob data and verifies it matches expected content.
async fn verify_blob_readable(
    client: &WalrusNodeClient<SuiContractClient>,
    blob_id: &BlobId,
    expected_data: &[u8],
) -> TestResult<()> {
    let read_data = client.read_blob::<Primary>(blob_id).await?;
    assert_eq!(read_data, expected_data, "Read data should match original");
    Ok(())
}

/// Stores a blob with BlobManager and reads it back to verify.
async fn store_and_read_blob_with_blob_manager(
    client: &mut WalrusNodeClient<SuiContractClient>,
    cap_id: sui_types::base_types::ObjectID,
    test_data: &[u8],
    persistence: BlobPersistence,
) -> TestResult<BlobId> {
    // Initialize the blob manager with the capability.
    client.init_blob_manager(cap_id).await?;

    let store_args = StoreArgs::default_with_epochs(1)
        .with_blob_manager()
        .with_persistence(persistence);

    let results = client
        .reserve_and_store_blobs_retry_committees(vec![test_data.to_vec()], vec![], &store_args)
        .await?;

    assert_eq!(results.len(), 1, "Should have one result");

    let blob_result = &results[0];
    let blob_id = blob_result.blob_id().expect("blob ID should be present");

    // Read the blob back and verify the data matches.
    let read_data = client.read_blob::<Primary>(&blob_id).await?;
    assert_eq!(read_data, test_data, "Read data should match original data");

    Ok(blob_id)
}

/// Creates a test wallet funded with multiple small WAL coins for testing coin merging.
///
/// This helper:
/// 1. Creates a new temporary wallet.
/// 2. Funds it with SUI for gas.
/// 3. Sends multiple small WAL coins to it.
/// 4. Returns a SuiContractClient configured with the test wallet.
async fn create_wallet_with_multiple_wal_coins(
    cluster_wallet: &mut Wallet,
    cluster_client: &WithTempDir<WalrusNodeClient<SuiContractClient>>,
    num_coins: u32,
    amount_per_coin: u64,
) -> TestResult<SuiContractClient> {
    use walrus_sui::test_utils::fund_addresses;

    let env = cluster_wallet.get_active_env()?.to_owned();
    let mut test_wallet = test_utils::temp_dir_wallet(None, env)
        .await
        .expect("Failed to create test wallet");
    let test_address = test_wallet.inner.active_address()?;

    // Fund the test wallet with SUI for gas.
    fund_addresses(cluster_wallet, vec![test_address], Some(10_000_000_000))
        .await
        .expect("Failed to fund test wallet with SUI");

    tracing::info!(
        num_coins,
        amount_per_coin,
        "Sending separate WAL coins to test wallet"
    );

    for i in 0..num_coins {
        cluster_client
            .inner
            .sui_client()
            .send_wal(amount_per_coin, test_address)
            .await
            .expect("Failed to send WAL to test wallet");
        tracing::debug!("Sent WAL coin {} of {}", i + 1, num_coins);
    }

    // Wait for transactions to be processed.
    tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

    // Create a SuiContractClient with the test wallet.
    let config = cluster_client.inner.config().clone();
    let test_sui_client = config
        .new_contract_client(test_wallet.inner, None)
        .await
        .expect("Failed to create SuiContractClient for test wallet");

    // Verify the test wallet has multiple WAL coins.
    let wal_coin_type = test_sui_client.read_client.wal_coin_type().to_owned();
    let wal_coins = test_sui_client
        .retriable_sui_client()
        .select_all_coins(test_address, Some(wal_coin_type))
        .await
        .expect("Failed to get WAL coins");

    tracing::info!(
        num_coins = wal_coins.len(),
        total_balance = wal_coins.iter().map(|c| c.balance).sum::<u64>(),
        "Test wallet WAL coins ready"
    );

    Ok(test_sui_client)
}

// =============================================================================
// Test Cases - Mixed Regular and Managed Blob Storage
// =============================================================================

/// Single BlobManager store/delete.
/// Store a deletable blob in BlobManager A, then delete it.
/// Verify status and storage reflect the delete (counts drop, blob gone).
#[ignore = "ignore E2E tests by default"]
#[walrus_simtest]
async fn test_single_blob_manager_store_delete() -> TestResult {
    walrus_test_utils::init_tracing();

    let test_nodes_config = TestNodesConfig {
        node_weights: vec![7, 7, 7, 7, 7],
        ..Default::default()
    };
    // Use 200-second epoch duration to ensure blobs don't expire during test,
    // allowing us to verify the exact end_epoch from BlobStatus.
    let (_sui_cluster_handle, _cluster, mut client, _) = test_cluster::E2eTestSetupBuilder::new()
        .with_test_nodes_config(test_nodes_config)
        .with_epoch_duration(Duration::from_secs(200))
        .build()
        .await?;
    let client = client.as_mut();

    tracing::info!("=== Single BlobManager store/delete ===");

    // Create BlobManager A.
    let initial_capacity = 500 * 1024 * 1024; // 500MB.
    let epochs_ahead_a = 5;

    let (manager_a_id, cap_a_id) = client
        .sui_client()
        .create_blob_manager(initial_capacity, epochs_ahead_a)
        .await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Get end_epoch for BlobManager A.
    client.init_blob_manager(cap_a_id).await?;
    let manager_a_info = client.get_blob_manager_client()?.get_storage_info().await?;
    let manager_a = Arc::new(TestBlobManager {
        id: manager_a_id,
        end_epoch: manager_a_info.end_epoch,
    });

    // Generate test data.
    let test_data = walrus_test_utils::random_data(1024);

    // Store as deletable in BlobManager A.
    let blob_id =
        store_managed_blob(client, cap_a_id, &test_data, BlobPersistence::Deletable, 1).await?;
    tracing::info!("Stored deletable managed blob: {}", blob_id);

    // Create tracker and add instance.
    let mut tracker = TestAggregatedBlobInfo::new(blob_id, test_data.clone());
    tracker.add_instance(
        StorageContext::BlobManager {
            manager: manager_a.clone(),
        },
        true, // deletable
    );

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify blob is readable.
    verify_blob_readable(client, &blob_id, &test_data).await?;

    // Get status and verify using tracker.
    let status = get_and_verify_status(client, &blob_id).await?;
    tracing::info!("Blob status after deletable store: {:?}", status);
    tracker.verify_status(&status);

    // Get storage info before deletion.
    let storage_before = client.get_blob_manager_client()?.get_storage_info().await?;
    tracing::info!(
        "Storage before deletion: available={}",
        storage_before.available_capacity
    );

    // Delete the deletable blob and update tracker.
    let blob_manager_client = client.get_blob_manager_client()?;
    blob_manager_client.delete_blob(blob_id).await?;
    tracker.mark_deleted_for_manager(&manager_a);
    tracing::info!("Deleted managed blob: {}", blob_id);

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify storage was released.
    let storage_after = client.get_blob_manager_client()?.get_storage_info().await?;
    tracing::info!(
        "Storage after deletion: available={}",
        storage_after.available_capacity
    );
    assert!(
        storage_after.available_capacity > storage_before.available_capacity,
        "Storage should be released after deletion"
    );

    // Verify blob status using tracker (should be Nonexistent).
    let status_after = get_and_verify_status(client, &blob_id).await?;
    tracing::info!("Blob status after deletion: {:?}", status_after);
    tracker.verify_status(&status_after);

    tracing::info!("Scenario completed successfully!");
    Ok(())
}

/// Two BlobManagers with Different End Epochs.
/// BlobManager A (5 epochs): Store deletable.
/// BlobManager B (10 epochs): Store deletable.
/// Verify `end_epoch` reflects max (BlobManager B's end_epoch).
/// Delete from BlobManager B -> `end_epoch` should now be BlobManager A's.
#[ignore = "ignore E2E tests by default"]
#[walrus_simtest]
async fn test_mixed_two_blob_managers() -> TestResult {
    walrus_test_utils::init_tracing();

    let test_nodes_config = TestNodesConfig {
        node_weights: vec![7, 7, 7, 7, 7],
        ..Default::default()
    };
    // Use 200-second epoch duration to ensure blobs don't expire during test.
    let (_sui_cluster_handle, _cluster, mut client, _) = test_cluster::E2eTestSetupBuilder::new()
        .with_test_nodes_config(test_nodes_config)
        .with_epoch_duration(Duration::from_secs(200))
        .build()
        .await?;
    let client = client.as_mut();

    tracing::info!("=== Two BlobManagers with Different End Epochs ===");

    // Create BlobManager A with shorter duration.
    let initial_capacity = 500 * 1024 * 1024; // 500MB.
    let epochs_ahead_a = 5;
    let epochs_ahead_b = 10;

    let (manager_a_id, cap_a_id) = client
        .sui_client()
        .create_blob_manager(initial_capacity, epochs_ahead_a)
        .await?;
    tracing::info!("Created BlobManager A: {}", manager_a_id);

    let (manager_b_id, cap_b_id) = client
        .sui_client()
        .create_blob_manager(initial_capacity, epochs_ahead_b)
        .await?;
    tracing::info!("Created BlobManager B: {}", manager_b_id);

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Get end epochs and create TestBlobManager instances.
    client.init_blob_manager(cap_a_id).await?;
    let manager_a_info = client.get_blob_manager_client()?.get_storage_info().await?;
    let manager_a = Arc::new(TestBlobManager {
        id: manager_a_id,
        end_epoch: manager_a_info.end_epoch,
    });

    client.init_blob_manager(cap_b_id).await?;
    let manager_b_info = client.get_blob_manager_client()?.get_storage_info().await?;
    let manager_b = Arc::new(TestBlobManager {
        id: manager_b_id,
        end_epoch: manager_b_info.end_epoch,
    });

    tracing::info!(
        "BlobManager A end_epoch: {}, BlobManager B end_epoch: {}",
        manager_a.end_epoch,
        manager_b.end_epoch
    );
    assert!(
        manager_b.end_epoch > manager_a.end_epoch,
        "BlobManager B should have later end_epoch"
    );

    // Generate test data.
    let test_data = walrus_test_utils::random_data(1024);

    // Store in BlobManager A.
    let blob_id =
        store_managed_blob(client, cap_a_id, &test_data, BlobPersistence::Deletable, 1).await?;
    tracing::info!("Stored in BlobManager A: {}", blob_id);

    // Create tracker and add instance for manager A.
    let mut tracker = TestAggregatedBlobInfo::new(blob_id, test_data.clone());
    tracker.add_instance(
        StorageContext::BlobManager {
            manager: manager_a.clone(),
        },
        true, // deletable
    );

    // Store same blob in BlobManager B.
    let blob_id_b =
        store_managed_blob(client, cap_b_id, &test_data, BlobPersistence::Deletable, 1).await?;
    assert_eq!(blob_id, blob_id_b, "Same data should have same blob_id");
    tracing::info!("Stored in BlobManager B: {}", blob_id_b);

    // Add instance for manager B.
    tracker.add_instance(
        StorageContext::BlobManager {
            manager: manager_b.clone(),
        },
        true, // deletable
    );

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify blob is readable.
    verify_blob_readable(client, &blob_id, &test_data).await?;

    // Get status and verify using tracker.
    let status = get_and_verify_status(client, &blob_id).await?;
    tracing::info!("Blob status with both managers: {:?}", status);
    tracker.verify_status(&status);

    // Get storage info before deletion from BlobManager B.
    client.init_blob_manager(cap_b_id).await?;
    let storage_b_before = client.get_blob_manager_client()?.get_storage_info().await?;

    // Delete from BlobManager B and update tracker.
    let blob_manager_b = client.get_blob_manager_client()?;
    blob_manager_b.delete_blob(blob_id).await?;
    tracker.mark_deleted_for_manager(&manager_b);
    tracing::info!("Deleted blob from BlobManager B");

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify storage released in BlobManager B.
    client.init_blob_manager(cap_b_id).await?;
    let storage_b_after = client.get_blob_manager_client()?.get_storage_info().await?;
    assert!(
        storage_b_after.available_capacity > storage_b_before.available_capacity,
        "Storage should be released in BlobManager B"
    );

    // Verify blob still readable (BlobManager A still has it).
    verify_blob_readable(client, &blob_id, &test_data).await?;

    // Verify status using tracker (should still be Managed with 1 instance).
    let status_after = get_and_verify_status(client, &blob_id).await?;
    tracing::info!("Blob status after deletion from B: {:?}", status_after);
    tracker.verify_status(&status_after);

    tracing::info!("Scenario completed successfully!");
    Ok(())
}

/// Mixed Regular + Managed.
/// Store: Regular Permanent + BlobManager A Deletable + BlobManager B Deletable.
/// Verify status is `Permanent` with managed counts.
/// Delete both managed deletables -> status still `Permanent` (regular permanent exists).
#[ignore = "ignore E2E tests by default"]
#[walrus_simtest]
async fn test_mixed_regular_and_managed() -> TestResult {
    walrus_test_utils::init_tracing();

    let test_nodes_config = TestNodesConfig {
        node_weights: vec![7, 7, 7, 7, 7],
        ..Default::default()
    };
    // Use 200-second epoch duration to ensure blobs don't expire during test.
    let (_sui_cluster_handle, _cluster, mut client, _) = test_cluster::E2eTestSetupBuilder::new()
        .with_test_nodes_config(test_nodes_config)
        .with_epoch_duration(Duration::from_secs(200))
        .build()
        .await?;
    let client = client.as_mut();

    tracing::info!("=== Mixed Regular + Managed ===");

    // Create two BlobManagers.
    let initial_capacity = 500 * 1024 * 1024; // 500MB.

    let (manager_a_id, cap_a_id) = client
        .sui_client()
        .create_blob_manager(initial_capacity, 5)
        .await?;
    tracing::info!("Created BlobManager A: {}", manager_a_id);

    let (manager_b_id, cap_b_id) = client
        .sui_client()
        .create_blob_manager(initial_capacity, 10)
        .await?;
    tracing::info!("Created BlobManager B: {}", manager_b_id);

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Get end epochs and create TestBlobManager instances.
    client.init_blob_manager(cap_a_id).await?;
    let manager_a_info = client.get_blob_manager_client()?.get_storage_info().await?;
    let manager_a = Arc::new(TestBlobManager {
        id: manager_a_id,
        end_epoch: manager_a_info.end_epoch,
    });

    client.init_blob_manager(cap_b_id).await?;
    let manager_b_info = client.get_blob_manager_client()?.get_storage_info().await?;
    let manager_b = Arc::new(TestBlobManager {
        id: manager_b_id,
        end_epoch: manager_b_info.end_epoch,
    });

    // Generate test data.
    let test_data = walrus_test_utils::random_data(1024);

    // Store as Regular Permanent.
    let (blob_id, permanent_end_epoch) =
        store_regular_blob(client, &test_data, BlobPersistence::Permanent, 3, false).await?;
    tracing::info!(
        "Stored regular permanent: {}, end_epoch: {}",
        blob_id,
        permanent_end_epoch
    );

    // Create tracker and add regular permanent instance.
    // Note: For regular blobs, we need the object_id from the result, but store_regular_blob
    // doesn't return it. For now, use a placeholder since we mainly verify status type.
    let mut tracker = TestAggregatedBlobInfo::new(blob_id, test_data.clone());
    tracker.add_instance(
        StorageContext::Regular {
            object_id: ObjectID::ZERO, // Placeholder - not used for verification currently.
            end_epoch: permanent_end_epoch,
        },
        false, // permanent
    );

    // Store in BlobManager A (deletable).
    let blob_id_a =
        store_managed_blob(client, cap_a_id, &test_data, BlobPersistence::Deletable, 1).await?;
    assert_eq!(blob_id, blob_id_a);
    tracing::info!("Stored in BlobManager A");
    tracker.add_instance(
        StorageContext::BlobManager {
            manager: manager_a.clone(),
        },
        true, // deletable
    );

    // Store in BlobManager B (deletable).
    let blob_id_b =
        store_managed_blob(client, cap_b_id, &test_data, BlobPersistence::Deletable, 1).await?;
    assert_eq!(blob_id, blob_id_b);
    tracing::info!("Stored in BlobManager B");
    tracker.add_instance(
        StorageContext::BlobManager {
            manager: manager_b.clone(),
        },
        true, // deletable
    );

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify status using tracker (should be Permanent).
    let status = get_and_verify_status(client, &blob_id).await?;
    tracing::info!("Blob status: {:?}", status);
    tracker.verify_status(&status);

    // Delete from BlobManager A and update tracker.
    client.init_blob_manager(cap_a_id).await?;
    client
        .get_blob_manager_client()?
        .delete_blob(blob_id)
        .await?;
    tracker.mark_deleted_for_manager(&manager_a);
    tracing::info!("Deleted from BlobManager A");

    // Delete from BlobManager B and update tracker.
    client.init_blob_manager(cap_b_id).await?;
    client
        .get_blob_manager_client()?
        .delete_blob(blob_id)
        .await?;
    tracker.mark_deleted_for_manager(&manager_b);
    tracing::info!("Deleted from BlobManager B");

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify status using tracker (should still be Permanent).
    let status_after = get_and_verify_status(client, &blob_id).await?;
    tracing::info!("Blob status after managed deletions: {:?}", status_after);
    tracker.verify_status(&status_after);

    // Verify blob is still readable.
    verify_blob_readable(client, &blob_id, &test_data).await?;

    tracing::info!("Scenario completed successfully!");
    Ok(())
}

/// Deletable-only, all deleted.
/// Store a deletable blob in BlobManager A, then delete it.
/// Verify reads fail and status is `Nonexistent`.
#[ignore = "ignore E2E tests by default"]
#[walrus_simtest]
async fn test_mixed_deletable_only_all_deleted() -> TestResult {
    walrus_test_utils::init_tracing();

    let test_nodes_config = TestNodesConfig {
        node_weights: vec![7, 7, 7, 7, 7],
        ..Default::default()
    };
    // Use 200-second epoch duration to ensure blobs don't expire during test.
    let (_sui_cluster_handle, _cluster, mut client, _) = test_cluster::E2eTestSetupBuilder::new()
        .with_test_nodes_config(test_nodes_config)
        .with_epoch_duration(Duration::from_secs(200))
        .build()
        .await?;
    let client = client.as_mut();

    tracing::info!("=== Deletable-only, all deleted ===");

    // Create BlobManager A.
    let initial_capacity = 500 * 1024 * 1024; // 500MB.

    let (manager_a_id, cap_a_id) = client
        .sui_client()
        .create_blob_manager(initial_capacity, 5)
        .await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Get end_epoch and create TestBlobManager instance.
    client.init_blob_manager(cap_a_id).await?;
    let manager_a_info = client.get_blob_manager_client()?.get_storage_info().await?;
    let manager_a = Arc::new(TestBlobManager {
        id: manager_a_id,
        end_epoch: manager_a_info.end_epoch,
    });

    // Generate test data.
    let test_data = walrus_test_utils::random_data(1024);

    // Store as deletable ONLY.
    let blob_id =
        store_managed_blob(client, cap_a_id, &test_data, BlobPersistence::Deletable, 1).await?;
    tracing::info!("Stored deletable-only blob: {}", blob_id);

    // Create tracker and add instance.
    let mut tracker = TestAggregatedBlobInfo::new(blob_id, test_data.clone());
    tracker.add_instance(
        StorageContext::BlobManager {
            manager: manager_a.clone(),
        },
        true, // deletable
    );

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify blob is readable.
    verify_blob_readable(client, &blob_id, &test_data).await?;
    tracing::info!("Blob is readable before deletion");

    // Get status and verify using tracker.
    let status = get_and_verify_status(client, &blob_id).await?;
    tracing::info!("Blob status before deletion: {:?}", status);
    tracker.verify_status(&status);

    // Delete the only instance and update tracker.
    client.init_blob_manager(cap_a_id).await?;
    client
        .get_blob_manager_client()?
        .delete_blob(blob_id)
        .await?;
    tracker.mark_deleted_for_manager(&manager_a);
    tracing::info!("Deleted the only blob instance");

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify status using tracker (should be Nonexistent).
    let status_after = get_and_verify_status(client, &blob_id).await?;
    tracing::info!("Blob status after deletion: {:?}", status_after);
    tracker.verify_status(&status_after);

    // Verify blob is NOT readable (should fail).
    let read_result = client.read_blob::<Primary>(&blob_id).await;
    assert!(
        read_result.is_err(),
        "Blob should not be readable after all instances deleted"
    );
    tracing::info!("Confirmed blob is not readable after deletion");

    tracing::info!("Scenario completed successfully!");
    Ok(())
}

/// Multiple Regular Blobs (No Reuse).
/// Store the same blob 3x as Regular Deletable (force/no-reuse).
/// Verify `deletable_counts.count_deletable_total >= 3`.
#[ignore = "ignore E2E tests by default"]
#[walrus_simtest]
async fn test_mixed_multiple_regular_no_reuse() -> TestResult {
    walrus_test_utils::init_tracing();

    let test_nodes_config = TestNodesConfig {
        node_weights: vec![7, 7, 7, 7, 7],
        ..Default::default()
    };
    let (_sui_cluster_handle, _cluster, client, _) = test_cluster::E2eTestSetupBuilder::new()
        .with_test_nodes_config(test_nodes_config)
        .build()
        .await?;
    let client = client.as_ref();

    tracing::info!("=== Multiple Regular Blobs (No Reuse) ===");

    // Generate test data.
    let test_data = walrus_test_utils::random_data(1024);

    // Store 3x as deletable with force.
    let (blob_id1, _) =
        store_regular_blob(client, &test_data, BlobPersistence::Deletable, 2, true).await?;
    tracing::info!("Stored deletable #1: {}", blob_id1);

    let (blob_id2, _) =
        store_regular_blob(client, &test_data, BlobPersistence::Deletable, 2, true).await?;
    assert_eq!(blob_id1, blob_id2, "Same data should have same blob_id");
    tracing::info!("Stored deletable #2");

    let (blob_id3, _) =
        store_regular_blob(client, &test_data, BlobPersistence::Deletable, 2, true).await?;
    assert_eq!(blob_id1, blob_id3, "Same data should have same blob_id");
    tracing::info!("Stored deletable #3");

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify deletable count.
    let status = get_and_verify_status(client, &blob_id1).await?;
    tracing::info!("Blob status: {:?}", status);

    let deletable_counts = extract_deletable_counts(&status);
    assert!(
        deletable_counts.count_deletable_total >= 3,
        "Should have at least 3 deletable instances, got {}",
        deletable_counts.count_deletable_total
    );
    tracing::info!(
        "Deletable count: {}",
        deletable_counts.count_deletable_total
    );

    // Verify blob is readable.
    verify_blob_readable(client, &blob_id1, &test_data).await?;

    tracing::info!("Scenario completed successfully!");
    Ok(())
}

/// Comprehensive test that runs through all basic cases sequentially.
/// This is useful for validating the complete mixed storage behavior in one test.
#[ignore = "ignore E2E tests by default"]
#[walrus_simtest]
async fn test_mixed_comprehensive() -> TestResult {
    walrus_test_utils::init_tracing();

    let test_nodes_config = TestNodesConfig {
        node_weights: vec![7, 7, 7, 7, 7],
        ..Default::default()
    };
    let (_sui_cluster_handle, _cluster, mut client, _) = test_cluster::E2eTestSetupBuilder::new()
        .with_test_nodes_config(test_nodes_config)
        .build()
        .await?;
    let client = client.as_mut();

    tracing::info!("=== Comprehensive Mixed Blob Storage Test ===");

    // Create two BlobManagers with DIFFERENT epochs_ahead.
    let initial_capacity = 500 * 1024 * 1024; // 500MB.

    let (manager_a_id, cap_a_id) = client
        .sui_client()
        .create_blob_manager(initial_capacity, 5) // 5 epochs ahead (shorter).
        .await?;
    tracing::info!("Created BlobManager A: {}", manager_a_id);

    let (manager_b_id, cap_b_id) = client
        .sui_client()
        .create_blob_manager(initial_capacity, 10) // 10 epochs ahead (longer).
        .await?;
    tracing::info!("Created BlobManager B: {}", manager_b_id);

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Get end epochs for verification.
    client.init_blob_manager(cap_a_id).await?;
    let manager_a_info = client.get_blob_manager_client()?.get_storage_info().await?;
    let manager_a_end_epoch = manager_a_info.end_epoch;

    client.init_blob_manager(cap_b_id).await?;
    let manager_b_info = client.get_blob_manager_client()?.get_storage_info().await?;
    let manager_b_end_epoch = manager_b_info.end_epoch;

    tracing::info!(
        "BlobManager A end_epoch: {}, BlobManager B end_epoch: {}",
        manager_a_end_epoch,
        manager_b_end_epoch
    );
    assert!(
        manager_b_end_epoch > manager_a_end_epoch,
        "BlobManager B should have later end_epoch"
    );

    // Test data for different cases.
    let test_data_1 = walrus_test_utils::random_data(1024);
    let test_data_2 = walrus_test_utils::random_data(1024);

    // --- Sub-case: Store in both managers, delete from one, verify other still works ---
    tracing::info!("--- Sub-case: Two managers, delete one ---");

    let blob_id = store_managed_blob(
        client,
        cap_a_id,
        &test_data_1,
        BlobPersistence::Deletable,
        1,
    )
    .await?;
    let blob_id_b = store_managed_blob(
        client,
        cap_b_id,
        &test_data_1,
        BlobPersistence::Deletable,
        1,
    )
    .await?;
    assert_eq!(blob_id, blob_id_b);

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Both deletable managed blobs should contribute to deletable_counts.
    let status = get_and_verify_status(client, &blob_id).await?;
    let deletable_counts = extract_deletable_counts(&status);
    assert!(
        deletable_counts.count_deletable_total >= 2,
        "Should have 2 deletable registrations"
    );

    // Delete from A, verify B still keeps blob alive.
    client.init_blob_manager(cap_a_id).await?;
    client
        .get_blob_manager_client()?
        .delete_blob(blob_id)
        .await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    verify_blob_readable(client, &blob_id, &test_data_1).await?;
    tracing::info!("Blob still readable after deleting from A");

    // Delete from B, verify blob becomes unreadable.
    client.init_blob_manager(cap_b_id).await?;
    client
        .get_blob_manager_client()?
        .delete_blob(blob_id)
        .await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    let status_final = get_and_verify_status(client, &blob_id).await?;
    assert!(
        matches!(status_final, BlobStatus::Nonexistent),
        "Should be Nonexistent after deleting from both"
    );

    // --- Sub-case: Regular permanent keeps blob alive even after managed deletions ---
    tracing::info!("--- Sub-case: Regular permanent survives managed deletions ---");

    let (blob_id_2, _) =
        store_regular_blob(client, &test_data_2, BlobPersistence::Permanent, 3, false).await?;
    let _ = store_managed_blob(
        client,
        cap_a_id,
        &test_data_2,
        BlobPersistence::Deletable,
        1,
    )
    .await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    let status = get_and_verify_status(client, &blob_id_2).await?;
    assert!(matches!(status, BlobStatus::Permanent { .. }));

    // Delete managed.
    client.init_blob_manager(cap_a_id).await?;
    client
        .get_blob_manager_client()?
        .delete_blob(blob_id_2)
        .await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Still permanent due to regular blob.
    let status_after = get_and_verify_status(client, &blob_id_2).await?;
    assert!(
        matches!(status_after, BlobStatus::Permanent { .. }),
        "Should still be Permanent"
    );
    verify_blob_readable(client, &blob_id_2, &test_data_2).await?;

    tracing::info!("=== Comprehensive Test PASSED ===");
    Ok(())
}

// =============================================================================
// Test Cases - BlobManager Core Functionality
// =============================================================================

/// Tests basic BlobManager operations: create manager, register blob, retrieve blob.
#[ignore = "ignore E2E tests by default"]
#[walrus_simtest]
async fn test_blob_manager_basic() {
    walrus_test_utils::init_tracing();

    let test_nodes_config = TestNodesConfig {
        node_weights: vec![7, 7, 7, 7, 7],
        ..Default::default()
    };
    let test_cluster_builder =
        test_cluster::E2eTestSetupBuilder::new().with_test_nodes_config(test_nodes_config);
    let (_sui_cluster_handle, _cluster, mut client, _) =
        test_cluster_builder.build().await.unwrap();
    let client_ref = client.as_mut();

    // Create a BlobManager with 500MB capacity (minimum required).
    let initial_capacity = 500 * 1024 * 1024; // 500MB.
    let epochs_ahead = 5;

    let (manager_id, cap_id) = client_ref
        .sui_client()
        .create_blob_manager(initial_capacity, epochs_ahead)
        .await
        .expect("Failed to create BlobManager");

    tracing::info!("Created BlobManager: {}", manager_id);
    tracing::info!("BlobManagerCap: {}", cap_id);

    // Verify the IDs are valid.
    assert_ne!(manager_id, sui_types::base_types::ObjectID::ZERO);
    assert_ne!(cap_id, sui_types::base_types::ObjectID::ZERO);

    // Wait a bit for the objects to be indexed.
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Use the new blob_manager() API to create the client.
    let blob_manager_client = client_ref
        .blob_manager(cap_id)
        .await
        .expect("Failed to create BlobManagerClient");

    tracing::info!("Successfully created BlobManagerClient");
    tracing::info!("  Manager ID: {}", manager_id);
    tracing::info!("  Manager Cap: {}", cap_id);

    // Now test the blob registration and retrieval.
    let test_blob = b"Hello, BlobManager! This is a test blob for registration.";

    // Create and encode a blob (following the pattern from reserve_and_store_blobs).
    use walrus_sdk::client::client_types::EncodedBlob;

    let unencoded_blob = WalrusStoreBlob::new_unencoded(
        test_blob.to_vec(),
        "test-blob".to_string(),
        BlobAttribute::default(),
        client_ref.encoding_config().get_for_type(DEFAULT_ENCODING),
    );

    // Encode the blob.
    let encoded_blobs = client_ref
        .encode_blobs(vec![unencoded_blob], None)
        .expect("Failed to encode blob");

    // Get unfinished blobs.
    let (encoded_unfinished, _): (
        Vec<WalrusStoreBlobUnfinished<EncodedBlob>>,
        Vec<WalrusStoreBlob<BlobStoreResult>>,
    ) = partition_unfinished_finished(encoded_blobs);
    assert_eq!(
        encoded_unfinished.len(),
        1,
        "Should have one unfinished blob"
    );

    let encoded_blob_unfinished = encoded_unfinished.into_iter().next().unwrap();
    let blob_id = encoded_blob_unfinished.state.blob_id();

    tracing::info!("Blob ID: {}", blob_id);

    // Directly create BlobWithStatus using Nonexistent status.
    let blob_with_status = BlobWithStatus {
        encoded_blob: encoded_blob_unfinished.state,
        status: walrus_storage_node_client::api::BlobStatus::Nonexistent,
    };

    // Create WalrusStoreBlobUnfinished<BlobWithStatus> directly.
    let blob_unfinished_with_status = WalrusStoreBlobMaybeFinished::<BlobWithStatus> {
        common: encoded_blob_unfinished.common,
        state: WalrusStoreBlobState::Unfinished(blob_with_status),
    };

    // Register the blob in the BlobManager.
    let registered_blobs = blob_manager_client
        .register_blobs(
            vec![blob_unfinished_with_status],
            BlobPersistence::Permanent,
        )
        .await
        .expect("Failed to register blob in BlobManager");

    assert_eq!(registered_blobs.len(), 1, "Should have one registered blob");
    tracing::info!("Successfully registered blob with ID: {}", blob_id);

    // Wait a moment for the registration to propagate.
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Now try to read the ManagedBlob back from the BlobManager.
    let managed_blob = blob_manager_client
        .get_managed_blob(blob_id, false) // false = permanent (not deletable).
        .await
        .expect("Failed to get ManagedBlob from BlobManager");

    // Verify the managed blob.
    assert_eq!(managed_blob.blob_id, blob_id, "Blob ID should match");
    assert!(!managed_blob.deletable, "Blob should be permanent");
    tracing::info!("Successfully retrieved ManagedBlob from BlobManager");
    tracing::info!("  Blob ID: {}", managed_blob.blob_id);
    tracing::info!("  Deletable: {}", managed_blob.deletable);
    tracing::info!("  Object ID: {}", managed_blob.id);

    tracing::info!("Test blob_manager_basic completed successfully");
}

/// Tests the complete BlobManager flow: create manager, store blob, read blob back.
#[ignore = "ignore E2E tests by default"]
#[walrus_simtest]
async fn test_blob_manager_store_and_read() {
    walrus_test_utils::init_tracing();

    let test_nodes_config = TestNodesConfig {
        node_weights: vec![7, 7, 7, 7, 7],
        ..Default::default()
    };
    let test_cluster_builder =
        test_cluster::E2eTestSetupBuilder::new().with_test_nodes_config(test_nodes_config);
    let (_sui_cluster_handle, _cluster, mut client, _) =
        test_cluster_builder.build().await.unwrap();
    let client_ref = client.as_mut();

    // Create a BlobManager with 500MB capacity.
    let initial_capacity = 500 * 1024 * 1024; // 500MB.
    let epochs_ahead = 5;

    let (manager_id, cap_id) = client_ref
        .sui_client()
        .create_blob_manager(initial_capacity, epochs_ahead)
        .await
        .expect("Failed to create BlobManager");

    tracing::info!("Created BlobManager: {}", manager_id);
    tracing::info!("BlobManagerCap: {}", cap_id);

    // Wait for objects to be indexed.
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Create test blob data for deletable blob.
    let deletable_test_data = b"Hello, BlobManager! This is a deletable blob test.";
    tracing::info!(
        "Deletable test data size: {} bytes",
        deletable_test_data.len()
    );

    // Store a deletable blob using BlobManager and read it back.
    let deletable_blob_id = store_and_read_blob_with_blob_manager(
        client_ref,
        cap_id,
        deletable_test_data,
        BlobPersistence::Deletable,
    )
    .await
    .expect("Failed to store and read deletable blob");

    tracing::info!(
        "Successfully stored and read deletable blob! Blob ID: {}",
        deletable_blob_id
    );

    // Create different test blob data for permanent blob.
    let permanent_test_data = b"Hello, BlobManager! This is a permanent blob test.";
    tracing::info!(
        "Permanent test data size: {} bytes",
        permanent_test_data.len()
    );

    // Store a permanent blob using BlobManager and read it back.
    let permanent_blob_id = store_and_read_blob_with_blob_manager(
        client_ref,
        cap_id,
        permanent_test_data,
        BlobPersistence::Permanent,
    )
    .await
    .expect("Failed to store and read permanent blob");

    tracing::info!(
        "Successfully stored and read permanent blob! Blob ID: {}",
        permanent_blob_id
    );

    // Create blob_manager_client after storing blobs to avoid borrow conflicts.
    let blob_manager_client = client_ref
        .blob_manager(cap_id)
        .await
        .expect("Failed to create BlobManagerClient");

    assert!(
        blob_manager_client
            .delete_blob(deletable_blob_id)
            .await
            .is_ok()
    );
}

/// Tests BlobManager coin stash operations: deposit, buy storage, extend storage, withdraw.
#[ignore = "ignore E2E tests by default"]
#[walrus_simtest]
async fn test_blob_manager_coin_stash_operations() {
    walrus_test_utils::init_tracing();

    let test_nodes_config = TestNodesConfig {
        node_weights: vec![7, 7, 7, 7, 7],
        ..Default::default()
    };
    let test_cluster_builder = test_cluster::E2eTestSetupBuilder::new()
        .with_test_nodes_config(test_nodes_config)
        .with_epoch_duration(Duration::from_secs(200)); // 200 seconds
    let (_sui_cluster_handle, _cluster, mut client, _) =
        test_cluster_builder.build().await.unwrap();
    let client_ref = client.as_mut();

    // Create a BlobManager with 100MB capacity (minimum required).
    let initial_capacity = 500 * 1024 * 1024; // 500MB.
    let epochs_ahead = 2; // Start with just 2 epochs.

    let (manager_id, cap_id) = client_ref
        .sui_client()
        .create_blob_manager(initial_capacity, epochs_ahead)
        .await
        .expect("Failed to create BlobManager");

    tracing::info!(?manager_id, ?cap_id, "BlobManager created");

    let blob_manager_client = client_ref
        .blob_manager(cap_id)
        .await
        .expect("Failed to create BlobManagerClient");

    // Deposit 1 WAL to coin stash.
    let deposit_amount_wal = 1_000_000_000;
    blob_manager_client
        .deposit_wal_to_coin_stash(deposit_amount_wal)
        .await
        .expect("Failed to deposit WAL to coin stash");

    // Test 2: Deposit SUI to coin stash.
    // First create additional SUI coins to avoid conflict with gas coin.
    let deposit_amount_sui = 10_000_000; // 0.01 SUI.
    tracing::info!("Creating additional SUI coins to avoid gas conflict");

    // Send some SUI to ourselves to create additional coin objects.
    // This creates separate coins that won't conflict with the gas coin.
    client_ref
        .sui_client()
        .send_sui(deposit_amount_sui * 2, client_ref.sui_client().address())
        .await
        .expect("Failed to split SUI coins");

    // Wait for split transaction to be processed.
    tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

    tracing::info!("Testing SUI deposit of {} units", deposit_amount_sui);
    client_ref
        .sui_client()
        .deposit_sui_to_blob_manager(manager_id, deposit_amount_sui)
        .await
        .expect("Failed to deposit SUI to coin stash");

    tracing::info!(
        "Successfully deposited {} SUI to coin stash",
        deposit_amount_sui
    );

    // Wait a bit for transaction to be processed.
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Test 3: Buy additional storage using coin stash funds.
    let additional_storage = 100 * 1024 * 1024; // 100MB.
    let storage_epochs = 2;
    tracing::info!(
        "Testing buy storage: {} bytes for {} epochs",
        additional_storage,
        storage_epochs
    );

    client_ref
        .sui_client()
        .buy_storage_from_stash(manager_id, cap_id, additional_storage, storage_epochs)
        .await
        .expect("Failed to buy storage from coin stash");

    tracing::info!(
        "Successfully bought {} MB of storage for {} epochs",
        additional_storage / (1024 * 1024),
        storage_epochs
    );

    // Test 4: Extend storage period using coin stash funds (fund manager version).
    // Note: Public extension (extend_storage_from_stash) would fail here because
    // the default policy is constrained(1, 10), meaning extension is only allowed
    // when within 1 epoch of expiry. Since we just created the BlobManager with
    // epochs_ahead=2, we're not within the threshold yet. Use fund manager extension
    // which bypasses the time constraint.
    let extension_epochs = 1;
    tracing::info!(
        "Testing storage extension by {} epochs (fund manager)",
        extension_epochs
    );

    client_ref
        .sui_client()
        .extend_storage_from_stash_fund_manager(manager_id, cap_id, extension_epochs)
        .await
        .expect("Failed to extend storage from coin stash");

    tracing::info!(
        "Successfully extended storage by {} epochs",
        extension_epochs
    );

    // Test 5: Create a new capability with fund_manager permission.
    tracing::info!("Testing capability creation with fund_manager permission");

    let new_cap_id = client_ref
        .sui_client()
        .create_blob_manager_cap(manager_id, cap_id, false, true) // not admin, but fund_manager.
        .await
        .expect("Failed to create new capability");

    tracing::info!(
        "Successfully created new capability with fund_manager permission: {}",
        new_cap_id
    );

    // Wait for the new capability to be indexed.
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Test 6: Withdraw all WAL using fund_manager capability.
    tracing::info!("Testing WAL withdrawal with fund_manager capability");

    client_ref
        .sui_client()
        .withdraw_all_wal_from_blob_manager(manager_id, new_cap_id)
        .await
        .expect("Failed to withdraw WAL from coin stash");

    tracing::info!("Successfully withdrew all WAL from coin stash");

    // Test 7: Withdraw all SUI using fund_manager capability.
    tracing::info!("Testing SUI withdrawal with fund_manager capability");

    client_ref
        .sui_client()
        .withdraw_all_sui_from_blob_manager(manager_id, new_cap_id)
        .await
        .expect("Failed to withdraw SUI from coin stash");

    tracing::info!("Successfully withdrew all SUI from coin stash");

    // Test 8: Store a blob to verify BlobManager still works after coin operations.
    let test_data = b"Testing blob storage after coin operations";
    tracing::info!("Storing test blob to verify BlobManager functionality");

    let blob_id = store_and_read_blob_with_blob_manager(
        client_ref,
        cap_id,
        test_data,
        BlobPersistence::Permanent,
    )
    .await
    .expect("Failed to store and read blob after coin operations");

    tracing::info!("Successfully stored and verified blob: {}", blob_id);
    tracing::info!("All coin stash operations completed successfully!");
}

/// Tests that WAL deposit handles merging multiple small coins automatically.
/// This tests the scenario where a wallet has many small WAL coins (e.g., 11 coins of 0.1 WAL)
/// and needs to deposit a larger amount (e.g., 1 WAL) that requires merging them.
#[ignore = "ignore E2E tests by default"]
#[walrus_simtest]
async fn test_blob_manager_deposit_wal_coin_merging() {
    walrus_test_utils::init_tracing();

    let test_nodes_config = TestNodesConfig {
        node_weights: vec![7, 7, 7, 7, 7],
        ..Default::default()
    };
    let test_cluster_builder =
        test_cluster::E2eTestSetupBuilder::new().with_test_nodes_config(test_nodes_config);
    let (sui_cluster_handle, _cluster, cluster_client, _) =
        test_cluster_builder.build().await.unwrap();

    // Create a new wallet for testing with multiple small WAL coins.
    let mut cluster_wallet = walrus_sui::config::load_wallet_context_from_path(
        Some(
            sui_cluster_handle
                .lock()
                .await
                .wallet_path()
                .await
                .as_path(),
        ),
        None,
    )
    .expect("Failed to load cluster wallet");

    // Create test wallet with 11 small WAL coins (0.1 WAL each).
    let num_coins = 11;
    let small_amount = 100_000_000; // 0.1 WAL (assuming 9 decimals).
    let test_sui_client = create_wallet_with_multiple_wal_coins(
        &mut cluster_wallet,
        &cluster_client,
        num_coins,
        small_amount,
    )
    .await
    .expect("Failed to create test wallet with multiple WAL coins");

    // Create a BlobManager using the test client.
    let initial_capacity = 500 * 1024 * 1024; // 500MB.
    let epochs_ahead = 2;

    let (manager_id, cap_id) = test_sui_client
        .create_blob_manager(initial_capacity, epochs_ahead)
        .await
        .expect("Failed to create BlobManager");

    tracing::info!(?manager_id, ?cap_id, "BlobManager created with test wallet");

    // Wait for objects to be indexed.
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Query initial BlobManager state - coin stash should be empty.
    let blob_manager_client = cluster_client
        .inner
        .blob_manager(cap_id)
        .await
        .expect("Failed to create BlobManagerClient");

    let initial_balances = blob_manager_client
        .get_coin_stash_balances()
        .await
        .expect("Failed to get initial coin stash balances");

    tracing::info!(
        "Initial coin stash balances: WAL={}, SUI={}",
        initial_balances.wal_balance,
        initial_balances.sui_balance
    );

    assert_eq!(
        initial_balances.wal_balance, 0,
        "Expected initial WAL balance to be 0"
    );

    // Now try to deposit 1 WAL, which requires merging multiple coins.
    let deposit_amount = 1_000_000_000; // 1 WAL.
    tracing::info!(
        deposit_amount,
        num_coins,
        "Attempting to deposit WAL (requires merging coins)"
    );

    test_sui_client
        .deposit_wal_to_blob_manager(manager_id, deposit_amount)
        .await
        .expect("Failed to deposit WAL - coin merging may have failed");

    tracing::info!(
        deposit_amount,
        "Successfully deposited WAL by merging multiple coins"
    );

    // Verify the deposit was successful by querying the coin stash balance.
    let final_balances = blob_manager_client
        .get_coin_stash_balances()
        .await
        .expect("Failed to get final coin stash balances");

    tracing::info!(
        "Final coin stash balances: WAL={}, SUI={}",
        final_balances.wal_balance,
        final_balances.sui_balance
    );

    assert_eq!(
        final_balances.wal_balance, deposit_amount,
        "Expected WAL balance to be {} after deposit, got {}",
        deposit_amount, final_balances.wal_balance
    );
}

/// Tests BlobManager capability management: creating different types of capabilities.
#[ignore = "ignore E2E tests by default"]
#[walrus_simtest]
async fn test_blob_manager_capability_management() {
    walrus_test_utils::init_tracing();

    let test_nodes_config = TestNodesConfig {
        node_weights: vec![7, 7, 7, 7, 7],
        ..Default::default()
    };
    let test_cluster_builder =
        test_cluster::E2eTestSetupBuilder::new().with_test_nodes_config(test_nodes_config);
    let (_sui_cluster_handle, _cluster, mut client, _) =
        test_cluster_builder.build().await.unwrap();
    let client_ref = client.as_mut();

    // Create a BlobManager.
    let initial_capacity = 500 * 1024 * 1024; // 500MB.
    let epochs_ahead = 5;

    let (manager_id, admin_cap_id) = client_ref
        .sui_client()
        .create_blob_manager(initial_capacity, epochs_ahead)
        .await
        .expect("Failed to create BlobManager");

    tracing::info!("Created BlobManager: {}", manager_id);
    tracing::info!("Admin Cap: {}", admin_cap_id);

    // Wait for objects to be indexed.
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Test 1: Create a writer capability (no admin, no fund_manager).
    tracing::info!("Creating writer capability");
    let writer_cap_id = client_ref
        .sui_client()
        .create_blob_manager_cap(manager_id, admin_cap_id, false, false)
        .await
        .expect("Failed to create writer capability");

    tracing::info!("Created writer capability: {}", writer_cap_id);

    // Test 2: Create a fund_manager capability (no admin, yes fund_manager).
    tracing::info!("Creating fund_manager capability");
    let fund_manager_cap_id = client_ref
        .sui_client()
        .create_blob_manager_cap(manager_id, admin_cap_id, false, true)
        .await
        .expect("Failed to create fund_manager capability");

    tracing::info!("Created fund_manager capability: {}", fund_manager_cap_id);

    // Test 3: Create another admin capability (yes admin, yes fund_manager).
    tracing::info!("Creating another admin capability");
    let admin2_cap_id = client_ref
        .sui_client()
        .create_blob_manager_cap(manager_id, admin_cap_id, true, true)
        .await
        .expect("Failed to create second admin capability");

    tracing::info!("Created second admin capability: {}", admin2_cap_id);

    // Wait for capabilities to be indexed.
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Test 4: Verify writer cap can store blobs.
    tracing::info!("Testing writer capability for blob storage");
    let test_data = b"Writer capability test blob";

    // Store a blob using the writer cap via the main client.
    let blob_id = store_and_read_blob_with_blob_manager(
        client_ref,
        writer_cap_id,
        test_data,
        BlobPersistence::Permanent,
    )
    .await
    .expect("Writer cap should be able to store blobs");

    tracing::info!("Writer cap successfully stored blob: {}", blob_id);

    // Test 5: Verify fund_manager cap can withdraw funds.
    tracing::info!("Testing fund_manager capability for fund operations");

    // First deposit some WAL.
    client_ref
        .sui_client()
        .deposit_wal_to_blob_manager(manager_id, 100_000_000)
        .await
        .expect("Failed to deposit WAL");

    // Then withdraw using fund_manager cap.
    client_ref
        .sui_client()
        .withdraw_all_wal_from_blob_manager(manager_id, fund_manager_cap_id)
        .await
        .expect("Fund manager cap should be able to withdraw funds");

    tracing::info!("Fund manager cap successfully withdrew funds");

    // Test 6: Verify admin2 cap can create new capabilities.
    tracing::info!("Testing second admin capability for creating new caps");

    let new_cap_from_admin2 = client_ref
        .sui_client()
        .create_blob_manager_cap(manager_id, admin2_cap_id, false, false)
        .await
        .expect("Admin cap should be able to create new capabilities");

    tracing::info!(
        "Admin2 cap successfully created new capability: {}",
        new_cap_from_admin2
    );

    tracing::info!("All capability management tests completed successfully!");
}

/// Test capability permissions with different capability types.
/// Tests that:
/// - Admin caps with fund_manager can create any cap type.
/// - Admin caps without fund_manager cannot create fund_manager caps.
/// - Writer caps (non-admin) cannot create caps.
/// - Fund_manager caps can withdraw funds but not create caps.
#[tokio::test]
#[ignore = "e2e test"]
async fn test_blob_manager_capability_permissions() {
    walrus_test_utils::init_tracing();

    let test_nodes_config = TestNodesConfig {
        node_weights: vec![7, 7, 7, 7, 7],
        ..Default::default()
    };
    let test_cluster_builder =
        test_cluster::E2eTestSetupBuilder::new().with_test_nodes_config(test_nodes_config);
    let (_sui_cluster_handle, _cluster, mut client, _) =
        test_cluster_builder.build().await.unwrap();
    let client_ref = client.as_mut();

    // Create a BlobManager.
    let initial_capacity = 500 * 1024 * 1024; // 500MB.
    let epochs_ahead = 5;

    let (manager_id, admin_cap_id) = client_ref
        .sui_client()
        .create_blob_manager(initial_capacity, epochs_ahead)
        .await
        .expect("Failed to create BlobManager");

    tracing::info!("Created BlobManager: {}", manager_id);

    // Wait for objects to be indexed.
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Create all capabilities using admin cap in a scope to release the borrow.
    let (admin_fm_cap, admin_no_fm_cap, writer_cap, fm_cap) = {
        let blob_manager_client = client_ref
            .blob_manager(admin_cap_id)
            .await
            .expect("Failed to create BlobManagerClient");

        // Test 1: Admin cap with fund_manager creates admin cap with fund_manager.
        tracing::info!("Test 1: Admin+fund_manager creating admin+fund_manager cap");
        let admin_fm_cap = blob_manager_client
            .create_cap(true, true)
            .await
            .expect("Admin+fund_manager should create admin+fund_manager cap");
        tracing::info!("Created admin+fund_manager cap: {}", admin_fm_cap);

        // Test 2: Admin cap with fund_manager creates admin cap without fund_manager.
        tracing::info!("Test 2: Admin+fund_manager creating admin cap without fund_manager");
        let admin_no_fm_cap = blob_manager_client
            .create_cap(true, false)
            .await
            .expect("Admin+fund_manager should create admin without fund_manager cap");
        tracing::info!("Created admin (no fund_manager) cap: {}", admin_no_fm_cap);

        // Test 3: Admin cap with fund_manager creates writer cap (non-admin, non-fund_manager).
        tracing::info!("Test 3: Admin+fund_manager creating writer cap");
        let writer_cap = blob_manager_client
            .create_cap(false, false)
            .await
            .expect("Admin+fund_manager should create writer cap");
        tracing::info!("Created writer cap: {}", writer_cap);

        // Test 7 (create fm_cap here before we need mutable borrows).
        tracing::info!("Creating fund_manager cap for later tests");
        let fm_cap = blob_manager_client
            .create_cap(false, true)
            .await
            .expect("Should create fund_manager cap");
        tracing::info!("Created fund_manager cap: {}", fm_cap);

        (admin_fm_cap, admin_no_fm_cap, writer_cap, fm_cap)
    };

    // Wait for caps to be indexed.
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Test 4: Admin cap without fund_manager CANNOT create fund_manager cap.
    tracing::info!("Test 4: Admin without fund_manager trying to create fund_manager cap");
    {
        let admin_no_fm_client = client_ref
            .blob_manager(admin_no_fm_cap)
            .await
            .expect("Failed to create BlobManagerClient for admin_no_fm");

        let result = admin_no_fm_client.create_cap(false, true).await;
        assert!(
            result.is_err(),
            "Admin without fund_manager should NOT be able to create fund_manager cap"
        );
        tracing::info!(
            "Correctly rejected: admin without fund_manager cannot create fund_manager cap"
        );
    }

    // Test 5: Writer cap CANNOT create any caps.
    tracing::info!("Test 5: Writer cap trying to create cap");
    {
        let writer_client = client_ref
            .blob_manager(writer_cap)
            .await
            .expect("Failed to create BlobManagerClient for writer");

        let result = writer_client.create_cap(false, false).await;
        assert!(
            result.is_err(),
            "Writer cap (non-admin) should NOT be able to create caps"
        );
        tracing::info!("Correctly rejected: writer cap cannot create caps");
    }

    // Test 6: Writer cap CAN store blobs.
    tracing::info!("Test 6: Writer cap storing blob");
    let test_data = b"Writer capability test blob data";
    let blob_id = store_and_read_blob_with_blob_manager(
        client_ref,
        writer_cap,
        test_data,
        BlobPersistence::Permanent,
    )
    .await
    .expect("Writer cap should be able to store blobs");
    tracing::info!("Writer cap stored blob: {}", blob_id);

    // Test 7: Fund_manager cap (non-admin) CAN withdraw funds.
    tracing::info!("Test 7: Testing fund_manager cap for fund operations");

    // Deposit some WAL first.
    client_ref
        .sui_client()
        .deposit_wal_to_blob_manager(manager_id, 100_000_000)
        .await
        .expect("Failed to deposit WAL");

    // Fund_manager cap can withdraw.
    client_ref
        .sui_client()
        .withdraw_all_wal_from_blob_manager(manager_id, fm_cap)
        .await
        .expect("Fund_manager cap should withdraw funds");
    tracing::info!("Fund_manager cap withdrew funds successfully");

    // Test 8: Fund_manager cap (non-admin) CANNOT create caps.
    tracing::info!("Test 8: Fund_manager cap trying to create cap");
    {
        let fm_client = client_ref
            .blob_manager(fm_cap)
            .await
            .expect("Failed to create BlobManagerClient for fund_manager");

        let result = fm_client.create_cap(false, false).await;
        assert!(
            result.is_err(),
            "Fund_manager cap (non-admin) should NOT be able to create caps"
        );
        tracing::info!("Correctly rejected: fund_manager cap (non-admin) cannot create caps");
    }

    // Suppress unused variable warning.
    let _ = admin_fm_cap;

    tracing::info!("All capability permission tests completed successfully!");
}

/// Test blob attribute operations on ManagedBlobs.
/// Tests: set, read, update, remove, and clear attributes.
#[tokio::test]
#[ignore = "e2e test"]
async fn test_blob_manager_attributes() {
    walrus_test_utils::init_tracing();

    let test_nodes_config = TestNodesConfig {
        node_weights: vec![7, 7, 7, 7, 7],
        ..Default::default()
    };
    let test_cluster_builder =
        test_cluster::E2eTestSetupBuilder::new().with_test_nodes_config(test_nodes_config);
    let (_sui_cluster_handle, _cluster, mut client, _) =
        test_cluster_builder.build().await.unwrap();
    let client_ref = client.as_mut();

    // Create a BlobManager.
    let initial_capacity = 500 * 1024 * 1024; // 500MB.
    let epochs_ahead = 5;

    let (_manager_id, admin_cap_id) = client_ref
        .sui_client()
        .create_blob_manager(initial_capacity, epochs_ahead)
        .await
        .expect("Failed to create BlobManager");

    tracing::info!("Created BlobManager with cap: {}", admin_cap_id);

    // Wait for objects to be indexed.
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Store a test blob first (requires mutable borrow of client_ref).
    let test_data = b"Test blob for attribute operations";
    let blob_id = store_and_read_blob_with_blob_manager(
        client_ref,
        admin_cap_id,
        test_data,
        BlobPersistence::Permanent,
    )
    .await
    .expect("Failed to store test blob");

    tracing::info!("Stored test blob: {}", blob_id);

    // Wait for blob to be indexed.
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Create BlobManagerClient after storing blob (to avoid borrow conflicts).
    let blob_manager_client = client_ref
        .blob_manager(admin_cap_id)
        .await
        .expect("Failed to create BlobManagerClient");

    // Test 1: Set first attribute.
    tracing::info!("Test 1: Setting first attribute");
    blob_manager_client
        .set_attribute(blob_id, "key1".to_string(), "value1".to_string())
        .await
        .expect("Failed to set first attribute");

    // Read and verify.
    let managed_blob = blob_manager_client
        .get_managed_blob(blob_id, false)
        .await
        .expect("Failed to get managed blob");

    assert_eq!(managed_blob.attributes.contents.len(), 1);
    let entry = &managed_blob.attributes.contents[0];
    assert_eq!(entry.key, "key1");
    assert_eq!(entry.value, "value1");
    tracing::info!("Verified first attribute: key1=value1");

    // Test 2: Add a second attribute.
    tracing::info!("Test 2: Adding second attribute");
    blob_manager_client
        .set_attribute(blob_id, "key2".to_string(), "value2".to_string())
        .await
        .expect("Failed to set second attribute");

    let managed_blob = blob_manager_client
        .get_managed_blob(blob_id, false)
        .await
        .expect("Failed to get managed blob");

    assert_eq!(managed_blob.attributes.contents.len(), 2);
    tracing::info!("Verified two attributes present");

    // Test 3: Update an existing attribute.
    tracing::info!("Test 3: Updating first attribute");
    blob_manager_client
        .set_attribute(blob_id, "key1".to_string(), "updated_value1".to_string())
        .await
        .expect("Failed to update attribute");

    let managed_blob = blob_manager_client
        .get_managed_blob(blob_id, false)
        .await
        .expect("Failed to get managed blob");

    // Still 2 attributes (updated, not added).
    assert_eq!(managed_blob.attributes.contents.len(), 2);
    // Find key1 and verify it's updated.
    let key1_entry = managed_blob
        .attributes
        .contents
        .iter()
        .find(|e| e.key == "key1")
        .expect("key1 should exist");
    assert_eq!(key1_entry.value, "updated_value1");
    tracing::info!("Verified attribute update: key1=updated_value1");

    // Test 4: Remove one attribute.
    tracing::info!("Test 4: Removing second attribute");
    blob_manager_client
        .remove_attribute(blob_id, "key2".to_string())
        .await
        .expect("Failed to remove attribute");

    let managed_blob = blob_manager_client
        .get_managed_blob(blob_id, false)
        .await
        .expect("Failed to get managed blob");

    // Only 1 attribute left.
    assert_eq!(managed_blob.attributes.contents.len(), 1);
    assert_eq!(managed_blob.attributes.contents[0].key, "key1");
    tracing::info!("Verified key2 removed, only key1 remains");

    // Test 5: Try to remove non-existent attribute (should fail).
    tracing::info!("Test 5: Trying to remove non-existent attribute");
    let result = blob_manager_client
        .remove_attribute(blob_id, "nonexistent".to_string())
        .await;
    assert!(
        result.is_err(),
        "Removing non-existent attribute should fail"
    );
    tracing::info!("Correctly rejected: cannot remove non-existent attribute");

    // Test 6: Add more attributes then clear all.
    tracing::info!("Test 6: Adding more attributes then clearing all");
    blob_manager_client
        .set_attribute(blob_id, "key3".to_string(), "value3".to_string())
        .await
        .expect("Failed to set key3");
    blob_manager_client
        .set_attribute(blob_id, "key4".to_string(), "value4".to_string())
        .await
        .expect("Failed to set key4");

    let managed_blob = blob_manager_client
        .get_managed_blob(blob_id, false)
        .await
        .expect("Failed to get managed blob");
    assert_eq!(managed_blob.attributes.contents.len(), 3); // key1, key3, key4.
    tracing::info!("Verified 3 attributes before clear");

    // Clear all attributes.
    blob_manager_client
        .clear_attributes(blob_id)
        .await
        .expect("Failed to clear attributes");

    let managed_blob = blob_manager_client
        .get_managed_blob(blob_id, false)
        .await
        .expect("Failed to get managed blob");
    assert!(
        managed_blob.attributes.contents.is_empty(),
        "Attributes should be empty after clear"
    );
    tracing::info!("Verified all attributes cleared");

    tracing::info!("All attribute tests completed successfully!");
}

/// Test extension policy operations on BlobManager.
/// Tests: set policy to disabled, fund_manager_only, constrained, and extension behaviors.
#[tokio::test]
#[ignore = "e2e test"]
async fn test_blob_manager_extension_policy() {
    walrus_test_utils::init_tracing();

    let test_nodes_config = TestNodesConfig {
        node_weights: vec![7, 7, 7, 7, 7],
        ..Default::default()
    };
    let test_cluster_builder =
        test_cluster::E2eTestSetupBuilder::new().with_test_nodes_config(test_nodes_config);
    let (_sui_cluster_handle, _cluster, mut client, _) =
        test_cluster_builder.build().await.unwrap();
    let client_ref = client.as_mut();

    // Create a BlobManager with initial storage.
    let initial_capacity = 500 * 1024 * 1024; // 500MB.
    let epochs_ahead = 5;

    let (manager_id, admin_cap_id) = client_ref
        .sui_client()
        .create_blob_manager(initial_capacity, epochs_ahead)
        .await
        .expect("Failed to create BlobManager");

    tracing::info!(
        "Created BlobManager: {} with cap: {}",
        manager_id,
        admin_cap_id
    );

    // Wait for objects to be indexed.
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Create BlobManagerClient.
    let blob_manager_client = client_ref
        .blob_manager(admin_cap_id)
        .await
        .expect("Failed to create BlobManagerClient");

    // Deposit some WAL to the coin stash for extension operations.
    let deposit_amount_wal = 5_000_000_000; // 5 WAL.
    blob_manager_client
        .deposit_wal_to_coin_stash(deposit_amount_wal)
        .await
        .expect("Failed to deposit WAL to coin stash");

    tracing::info!("Deposited {} MIST WAL to coin stash", deposit_amount_wal);

    // Test 1: Default policy is constrained(1, 10).
    // Try public extension - should work since default policy allows it.
    tracing::info!("Test 1: Public extension with default constrained policy");

    // Note: Public extension will only work if we're within the expiry threshold.
    // Since storage_end_epoch is ahead, public extension might fail due to EExtensionTooEarly.
    // That's expected behavior. Let's test fund manager extension which bypasses the check.

    // Test 2: Test fund manager extension (bypasses policy constraints).
    tracing::info!("Test 2: Fund manager extension (bypasses policy constraints)");
    blob_manager_client
        .extend_storage_from_stash_fund_manager(1)
        .await
        .expect("Failed to extend storage with fund manager");
    tracing::info!("Fund manager extension succeeded");

    // Wait for transaction to be processed.
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Test 3: Set policy to fund_manager_only.
    tracing::info!("Test 3: Setting policy to fund_manager_only");
    blob_manager_client
        .set_extension_policy_fund_manager_only()
        .await
        .expect("Failed to set policy to fund_manager_only");

    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Public extension should now fail.
    tracing::info!("Test 3b: Public extension should fail with fund_manager_only policy");
    let public_result = client_ref
        .sui_client()
        .extend_storage_from_stash(manager_id, 1)
        .await;
    assert!(
        public_result.is_err(),
        "Public extension should fail with fund_manager_only policy"
    );
    tracing::info!("Correctly rejected public extension with fund_manager_only policy");

    // But fund manager extension should still work.
    tracing::info!("Test 3c: Fund manager extension should still work");
    blob_manager_client
        .extend_storage_from_stash_fund_manager(1)
        .await
        .expect("Fund manager extension should succeed even with fund_manager_only policy");
    tracing::info!("Fund manager extension succeeded with fund_manager_only policy");

    // Wait for transaction to be processed.
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Test 4: Set policy back to constrained.
    tracing::info!("Test 4: Setting policy to constrained(2, 5)");
    blob_manager_client
        .set_extension_policy_constrained(2, 5)
        .await
        .expect("Failed to set policy to constrained");

    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Fund manager extension should work again.
    tracing::info!("Test 4b: Fund manager extension should work with constrained policy");
    blob_manager_client
        .extend_storage_from_stash_fund_manager(1)
        .await
        .expect("Fund manager extension should succeed with constrained policy");
    tracing::info!("Fund manager extension succeeded with constrained policy");

    tracing::info!("All extension policy tests completed successfully!");
}

// =============================================================================
// Randomized Test - Mixed Operations
// =============================================================================

/// Represents a store operation for the randomized test.
#[derive(Debug, Clone)]
enum StoreOperation {
    /// Store as regular blob.
    Regular {
        epochs_ahead: u32,
        persistence: BlobPersistence,
    },
    /// Store in a BlobManager.
    Managed {
        manager_index: usize, // Index into the managers array.
        persistence: BlobPersistence,
    },
}

/// Randomized test for mixed regular and managed blob operations.
///
/// This test covers:
/// - Different orderings of storing the same blob (regular, manager1, manager2).
/// - Random deletion of deletable blobs between store operations.
/// - Verification of blob status from storage nodes after each operation.
/// - Verification that ManagedBlobs exist on chain (not just storage nodes).
#[ignore = "ignore E2E tests by default"]
#[walrus_simtest]
async fn test_random_mixed_operations() -> TestResult {
    use rand::{Rng, SeedableRng, seq::SliceRandom};

    walrus_test_utils::init_tracing();

    // Use a fixed seed for reproducibility. Change this to test different scenarios.
    let seed: u64 = 42;
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
    tracing::info!("=== Randomized Mixed Operations Test (seed={}) ===", seed);

    let test_nodes_config = TestNodesConfig {
        node_weights: vec![7, 7, 7, 7, 7],
        ..Default::default()
    };
    // Use 200-second epoch duration so blobs don't expire during test.
    let (_sui_cluster_handle, _cluster, mut client, _) = test_cluster::E2eTestSetupBuilder::new()
        .with_test_nodes_config(test_nodes_config)
        .with_epoch_duration(Duration::from_secs(200))
        .build()
        .await?;
    let client = client.as_mut();

    // Create two BlobManagers with different end_epochs.
    let initial_capacity = 500 * 1024 * 1024; // 500MB.

    let (manager_a_id, cap_a_id) = client
        .sui_client()
        .create_blob_manager(initial_capacity, 5)
        .await?;
    tracing::info!("Created BlobManager A: {}", manager_a_id);

    let (manager_b_id, cap_b_id) = client
        .sui_client()
        .create_blob_manager(initial_capacity, 10)
        .await?;
    tracing::info!("Created BlobManager B: {}", manager_b_id);

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Get end_epochs for managers.
    client.init_blob_manager(cap_a_id).await?;
    let manager_a_info = client.get_blob_manager_client()?.get_storage_info().await?;
    let manager_a = Arc::new(TestBlobManager {
        id: manager_a_id,
        end_epoch: manager_a_info.end_epoch,
    });

    client.init_blob_manager(cap_b_id).await?;
    let manager_b_info = client.get_blob_manager_client()?.get_storage_info().await?;
    let manager_b = Arc::new(TestBlobManager {
        id: manager_b_id,
        end_epoch: manager_b_info.end_epoch,
    });

    let managers = [(manager_a.clone(), cap_a_id), (manager_b.clone(), cap_b_id)];

    tracing::info!(
        "Manager A end_epoch: {}, Manager B end_epoch: {}",
        manager_a.end_epoch,
        manager_b.end_epoch
    );

    // Generate test data.
    let test_data = walrus_test_utils::random_data(1024);

    // Define all possible store operations.
    let mut store_operations: Vec<StoreOperation> = vec![
        // Regular stores.
        StoreOperation::Regular {
            epochs_ahead: 3,
            persistence: BlobPersistence::Permanent,
        },
        StoreOperation::Regular {
            epochs_ahead: 5,
            persistence: BlobPersistence::Deletable,
        },
        // Manager A stores.
        StoreOperation::Managed {
            manager_index: 0,
            persistence: BlobPersistence::Deletable,
        },
        // Manager B stores.
        StoreOperation::Managed {
            manager_index: 1,
            persistence: BlobPersistence::Deletable,
        },
    ];

    // Shuffle the operations.
    store_operations.shuffle(&mut rng);

    tracing::info!(
        "Randomized operation order: {:?}",
        store_operations
            .iter()
            .map(|op| match op {
                StoreOperation::Regular { persistence, .. } =>
                    format!("Regular({:?})", persistence),
                StoreOperation::Managed {
                    manager_index,
                    persistence,
                } => format!("Manager{}({:?})", manager_index, persistence),
            })
            .collect::<Vec<_>>()
    );

    // Track stored instances using TestAggregatedBlobInfo.
    // We'll create the tracker after the first store operation when we know the blob_id.
    let mut tracker: Option<TestAggregatedBlobInfo> = None;

    // Execute operations with random deletions between them.
    for (op_idx, operation) in store_operations.iter().enumerate() {
        tracing::info!(
            "--- Operation {}/{}: {:?} ---",
            op_idx + 1,
            store_operations.len(),
            operation
        );

        // Randomly decide whether to delete a deletable managed blob before this operation.
        if let Some(ref mut t) = tracker {
            // Find deletable managed instances that haven't been deleted.
            let deletable_managers: Vec<Arc<TestBlobManager>> = t
                .instances
                .iter()
                .filter(|inst| {
                    !inst.deleted
                        && inst.deletable
                        && matches!(inst.context, StorageContext::BlobManager { .. })
                })
                .filter_map(|inst| {
                    if let StorageContext::BlobManager { manager } = &inst.context {
                        Some(manager.clone())
                    } else {
                        None
                    }
                })
                .collect();

            if !deletable_managers.is_empty() && rng.gen_bool(0.3) {
                // 30% chance to delete.
                let manager_to_delete = deletable_managers.choose(&mut rng).unwrap();
                tracing::info!("Randomly deleting from manager {}", manager_to_delete.id);

                // Find the cap_id for this manager.
                let cap_id = managers
                    .iter()
                    .find(|(m, _)| Arc::ptr_eq(m, manager_to_delete))
                    .map(|(_, cap)| *cap)
                    .unwrap();

                client.init_blob_manager(cap_id).await?;
                client
                    .get_blob_manager_client()?
                    .delete_blob(t.blob_id)
                    .await?;
                t.mark_deleted_for_manager(manager_to_delete);
                tracing::info!("Deleted managed blob from manager {}", manager_to_delete.id);

                tokio::time::sleep(Duration::from_millis(300)).await;

                // Verify status after deletion.
                let status = get_and_verify_status(client, &t.blob_id).await?;
                tracing::info!("Status after deletion: {:?}", status);
                t.verify_status(&status);
            }
        }

        // Execute the store operation.
        let (new_blob_id, context, deletable) = match operation {
            StoreOperation::Regular {
                epochs_ahead,
                persistence,
            } => {
                let (bid, end_ep) =
                    store_regular_blob(client, &test_data, *persistence, *epochs_ahead, true)
                        .await?;
                tracing::info!("Stored regular blob: {} (end_epoch={})", bid, end_ep);
                let ctx = StorageContext::Regular {
                    object_id: ObjectID::ZERO, // Placeholder - not used for verification.
                    end_epoch: end_ep,
                };
                (bid, ctx, *persistence == BlobPersistence::Deletable)
            }
            StoreOperation::Managed {
                manager_index,
                persistence,
            } => {
                let (manager, cap_id) = &managers[*manager_index];
                let bid = store_managed_blob(client, *cap_id, &test_data, *persistence, 1).await?;
                tracing::info!(
                    "Stored in Manager {}: {} (end_epoch={})",
                    manager_index,
                    bid,
                    manager.end_epoch
                );
                let ctx = StorageContext::BlobManager {
                    manager: manager.clone(),
                };
                (bid, ctx, *persistence == BlobPersistence::Deletable)
            }
        };

        // Initialize or update tracker.
        match &mut tracker {
            Some(t) => {
                assert_eq!(
                    t.blob_id, new_blob_id,
                    "Same data should produce same blob_id"
                );
                t.add_instance(context, deletable);
            }
            None => {
                let mut t = TestAggregatedBlobInfo::new(new_blob_id, test_data.clone());
                t.add_instance(context, deletable);
                tracker = Some(t);
            }
        }

        tokio::time::sleep(Duration::from_millis(500)).await;

        let t = tracker.as_ref().unwrap();

        // Verify blob is readable from storage nodes.
        verify_blob_readable(client, &new_blob_id, &test_data).await?;
        tracing::info!("Blob readable from storage nodes");

        // Verify status from storage nodes using tracker.
        let status = get_and_verify_status(client, &new_blob_id).await?;
        tracing::info!("Status from storage nodes: {:?}", status);
        t.verify_status(&status);

        // For managed blobs, verify ManagedBlob exists on chain.
        if let StoreOperation::Managed {
            manager_index,
            persistence,
        } = operation
        {
            let (_, cap_id) = &managers[*manager_index];
            client.init_blob_manager(*cap_id).await?;
            let managed_blob = client
                .get_blob_manager_client()?
                .get_managed_blob(new_blob_id, *persistence == BlobPersistence::Deletable)
                .await?;

            assert_eq!(managed_blob.blob_id, new_blob_id);
            assert_eq!(
                managed_blob.deletable,
                *persistence == BlobPersistence::Deletable
            );
            tracing::info!(
                "Verified ManagedBlob on chain: id={}, deletable={}",
                managed_blob.id,
                managed_blob.deletable
            );
        }
    }

    // Final verification: delete all remaining deletable managed blobs and verify final status.
    tracing::info!("--- Final cleanup: deleting all remaining deletable managed blobs ---");

    let tracker = tracker.as_mut().unwrap();

    // Collect managers to delete from (to avoid borrow issues).
    let managers_to_delete: Vec<Arc<TestBlobManager>> = tracker
        .instances
        .iter()
        .filter(|inst| {
            !inst.deleted
                && inst.deletable
                && matches!(inst.context, StorageContext::BlobManager { .. })
        })
        .filter_map(|inst| {
            if let StorageContext::BlobManager { manager } = &inst.context {
                Some(manager.clone())
            } else {
                None
            }
        })
        .collect();

    for manager in &managers_to_delete {
        let cap_id = managers
            .iter()
            .find(|(m, _)| Arc::ptr_eq(m, manager))
            .map(|(_, cap)| *cap)
            .unwrap();

        client.init_blob_manager(cap_id).await?;
        client
            .get_blob_manager_client()?
            .delete_blob(tracker.blob_id)
            .await?;
        tracker.mark_deleted_for_manager(manager);
        tracing::info!("Deleted remaining managed blob from manager {}", manager.id);
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify final status.
    let final_status = get_and_verify_status(client, &tracker.blob_id).await?;
    tracing::info!("Final status: {:?}", final_status);
    tracker.verify_status(&final_status);

    // Blob should still be readable if any permanent instance exists.
    if tracker.has_permanent() || tracker.has_managed_permanent() {
        verify_blob_readable(client, &tracker.blob_id, &test_data).await?;
        tracing::info!("Blob still readable (permanent instance exists)");
    } else {
        tracing::info!("No permanent instances remain");
    }

    tracing::info!(
        "=== Randomized Mixed Operations Test PASSED (seed={}) ===",
        seed
    );
    Ok(())
}

/// Tests that BlobStatus::Permanent uses permanent_end_epoch, not deletable_end_epoch.
///
/// This test verifies the fix for a bug where BlobStatus::Permanent was using the max
/// end_epoch across ALL registered BlobManagers (including deletable ones), instead of
/// only using the permanent blobs' end_epoch.
///
/// Scenario:
/// - Create BlobManager A with short end_epoch (2 epochs ahead) for PERMANENT blob.
/// - Create BlobManager B with long end_epoch (10 epochs ahead) for DELETABLE blob.
/// - Store same blob in both managers.
/// - Verify BlobStatus::Permanent reports end_epoch from BlobManager A (permanent),
///   not from BlobManager B (deletable).
#[ignore = "ignore E2E tests by default"]
#[walrus_simtest]
async fn test_managed_permanent_end_epoch_uses_permanent_not_deletable() -> TestResult {
    walrus_test_utils::init_tracing();

    let test_nodes_config = TestNodesConfig {
        node_weights: vec![7, 7, 7, 7, 7],
        ..Default::default()
    };
    // Use 200-second epoch duration to ensure blobs don't expire during test.
    let (_sui_cluster_handle, _cluster, mut client, _) = test_cluster::E2eTestSetupBuilder::new()
        .with_test_nodes_config(test_nodes_config)
        .with_epoch_duration(Duration::from_secs(200))
        .build()
        .await?;
    let client = client.as_mut();

    tracing::info!("=== Testing Permanent End Epoch Uses Permanent, Not Deletable ===");

    let initial_capacity = 500 * 1024 * 1024; // 500MB.

    // Create BlobManager A with SHORT end_epoch (2 epochs) for PERMANENT blob.
    let epochs_ahead_permanent = 2;
    let (manager_permanent_id, cap_permanent_id) = client
        .sui_client()
        .create_blob_manager(initial_capacity, epochs_ahead_permanent)
        .await?;
    tracing::info!(
        "Created BlobManager for PERMANENT blob: {}, epochs_ahead: {}",
        manager_permanent_id,
        epochs_ahead_permanent
    );

    // Create BlobManager B with LONG end_epoch (10 epochs) for DELETABLE blob.
    let epochs_ahead_deletable = 10;
    let (manager_deletable_id, cap_deletable_id) = client
        .sui_client()
        .create_blob_manager(initial_capacity, epochs_ahead_deletable)
        .await?;
    tracing::info!(
        "Created BlobManager for DELETABLE blob: {}, epochs_ahead: {}",
        manager_deletable_id,
        epochs_ahead_deletable
    );

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Get end epochs.
    client.init_blob_manager(cap_permanent_id).await?;
    let permanent_manager_info = client.get_blob_manager_client()?.get_storage_info().await?;
    let permanent_end_epoch = permanent_manager_info.end_epoch;

    client.init_blob_manager(cap_deletable_id).await?;
    let deletable_manager_info = client.get_blob_manager_client()?.get_storage_info().await?;
    let deletable_end_epoch = deletable_manager_info.end_epoch;

    tracing::info!(
        "Permanent manager end_epoch: {}, Deletable manager end_epoch: {}",
        permanent_end_epoch,
        deletable_end_epoch
    );

    // Verify deletable manager has HIGHER end_epoch.
    assert!(
        deletable_end_epoch > permanent_end_epoch,
        "Deletable manager should have later end_epoch: {} > {}",
        deletable_end_epoch,
        permanent_end_epoch
    );

    // Generate test data.
    let test_data = walrus_test_utils::random_data(1024);

    // Store PERMANENT blob in BlobManager A (shorter end_epoch).
    let blob_id = store_managed_blob(
        client,
        cap_permanent_id,
        &test_data,
        BlobPersistence::Permanent,
        1,
    )
    .await?;
    tracing::info!(
        "Stored PERMANENT blob in manager with end_epoch={}: {}",
        permanent_end_epoch,
        blob_id
    );

    // Store DELETABLE blob (same data) in BlobManager B (longer end_epoch).
    let blob_id_deletable = store_managed_blob(
        client,
        cap_deletable_id,
        &test_data,
        BlobPersistence::Deletable,
        1,
    )
    .await?;
    assert_eq!(
        blob_id, blob_id_deletable,
        "Same data should have same blob_id"
    );
    tracing::info!(
        "Stored DELETABLE blob in manager with end_epoch={}: {}",
        deletable_end_epoch,
        blob_id_deletable
    );

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Get blob status.
    let status = get_and_verify_status(client, &blob_id).await?;
    tracing::info!("Blob status: {:?}", status);

    // Verify the status is Permanent.
    assert!(
        status.is_permanent(),
        "Status should be Permanent, got: {:?}",
        status
    );

    // THE KEY ASSERTION: end_epoch should be from PERMANENT blob (shorter end_epoch),
    // NOT from DELETABLE blob (longer end_epoch).
    let reported_end_epoch = status.end_epoch().expect("Permanent should have end_epoch");

    assert_eq!(
        reported_end_epoch, permanent_end_epoch,
        "BlobStatus::Permanent should report end_epoch from PERMANENT blob ({}), \
            not from DELETABLE blob ({}). Got: {}",
        permanent_end_epoch, deletable_end_epoch, reported_end_epoch
    );

    tracing::info!(
        "SUCCESS: BlobStatus::Permanent correctly reports end_epoch={} (from permanent blob), \
            not {} (from deletable blob)",
        permanent_end_epoch,
        deletable_end_epoch
    );

    // Also verify deletable counts are correct.
    let deletable_counts = status.deletable_counts();
    assert_eq!(
        deletable_counts.count_deletable_total, 1,
        "Should have 1 deletable blob"
    );

    // Verify blob is readable.
    verify_blob_readable(client, &blob_id, &test_data).await?;

    tracing::info!("=== Permanent End Epoch Test PASSED ===");
    Ok(())
}

/// Tests dormant mode: BlobManager extension after storage has expired but within grace period.
/// This test verifies the compensation storage mechanism where we buy storage to pay for
/// the dormant period, then extend from the current epoch forward.
///
/// NOTE: This test is disabled because grace period is now deterministic based on storage duration.
/// Without the ability to precisely control epochs or set custom grace periods in tests,
/// it's difficult to reliably test dormant mode behavior. The grace period calculation is:
/// < 5 epochs: 0, < 10: 1, < 20: 2, < 35: 3, < 60: 4, >= 60: 4 + floor((delta-60)/20).
#[ignore = "Disabled: deterministic grace period makes dormant mode testing difficult"]
#[walrus_simtest]
async fn test_blob_manager_dormant_mode_extension() -> TestResult {
    walrus_test_utils::init_tracing();

    let test_nodes_config = TestNodesConfig {
        node_weights: vec![7, 7, 7, 7, 7],
        ..Default::default()
    };

    // Use VERY SHORT epoch duration (5 seconds) to allow BlobManager to expire during test.
    let epoch_duration = Duration::from_secs(5);

    let (_sui_cluster_handle, _cluster, mut client, _) = test_cluster::E2eTestSetupBuilder::new()
        .with_test_nodes_config(test_nodes_config)
        .with_epoch_duration(epoch_duration)
        .build()
        .await?;
    let client = client.as_mut();

    tracing::info!("=== Testing BlobManager Dormant Mode Extension ===");

    let initial_capacity = 500 * 1024 * 1024; // 500MB.
    // Would need to carefully choose epochs to get desired grace period.
    let initial_epochs = 10; // BlobManager will expire after 10 epochs.

    // Create a BlobManager. With deterministic grace period,
    // 10 epochs of storage gives 1 epoch grace period.
    let (manager_id, cap_id) = client
        .sui_client()
        .create_blob_manager(initial_capacity, initial_epochs)
        .await?;
    tracing::info!(
        "Created BlobManager {} with cap {}, expires in {} epochs",
        manager_id,
        cap_id,
        initial_epochs
    );

    // Initialize the BlobManager client.
    client.init_blob_manager(cap_id).await?;

    // Note: Grace period is now deterministic based on storage duration.

    // Add a small delay to ensure the shared object is properly indexed.
    tracing::info!("Waiting for shared object to be indexed...");
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Set extension policy to allow public extension near expiry.
    // Allow extension within last 20 epochs (basically always), max 5 epochs at a time.
    tracing::info!(
        "Setting extension policy for manager_id={}, cap_id={}",
        manager_id,
        cap_id
    );
    client
        .sui_client()
        .set_extension_policy_constrained(manager_id, cap_id, 20, 5)
        .await?;
    tracing::info!("Set extension policy to allow public extension");

    // Add funds to the coin stash for future extension.
    {
        let amount_to_deposit = 1_000_000_000_000u64; // 1000 WAL.
        let blob_manager_client = client.get_blob_manager_client()?;
        blob_manager_client
            .deposit_wal_to_coin_stash(amount_to_deposit)
            .await?;
        tracing::info!("Deposited {} WAL to coin stash", amount_to_deposit);
    }

    // Store a test blob to verify it remains accessible.
    let test_data = walrus_test_utils::random_data(1024);
    let blob_id =
        store_managed_blob(client, cap_id, &test_data, BlobPersistence::Permanent, 1).await?;
    tracing::info!("Stored test blob with ID: {}", blob_id);

    // Get initial storage info.
    let initial_storage_info = client.get_blob_manager_client()?.get_storage_info().await?;
    tracing::info!(
        "Initial storage: end_epoch={}, total_capacity={}, used={}",
        initial_storage_info.end_epoch,
        initial_storage_info.total_capacity,
        initial_storage_info.used_capacity
    );

    // Wait for BlobManager to expire (go past end_epoch).
    // We need to wait at least 2 epochs * 5 seconds = 10 seconds.
    // Add extra time to ensure we're definitely past expiry.
    tracing::info!("Waiting for BlobManager to expire (15 seconds)...");
    tokio::time::sleep(Duration::from_secs(15)).await;

    // After waiting 15 seconds (3 epochs of 5 seconds), the BlobManager should be expired.
    // The end_epoch was only 2 epochs ahead, so we're now past expiry.
    tracing::info!(
        "After waiting 15 seconds, storage should be expired (end_epoch was {})",
        initial_storage_info.end_epoch
    );
    // We don't have a direct way to get current epoch, but we know we've waited long enough
    // for the storage to expire based on the epoch duration and initial epochs.

    // During dormant mode, blob should NOT be accessible (returns BlobIdDoesNotExist).
    // This is expected behavior - the storage has expired.
    tracing::info!("Verifying blob is NOT accessible during dormant mode...");
    let read_result = verify_blob_readable(client, &blob_id, &test_data).await;
    assert!(
        read_result.is_err(),
        "Expected blob to be inaccessible during dormant mode, but read succeeded"
    );
    tracing::info!("Confirmed: Blob is not accessible during dormant mode (expected behavior)");

    // Now extend the BlobManager during dormant mode.
    let extension_epochs = 5u32;
    tracing::info!(
        "Extending BlobManager by {} epochs during dormant mode",
        extension_epochs
    );

    // Use fund manager extension during dormant mode.
    // This will buy compensation storage for dormant period and extend from current epoch forward.
    {
        tracing::info!("About to call extend_storage_from_stash_fund_manager");
        let blob_manager_client = client.get_blob_manager_client()?;
        tracing::info!(
            "Calling extend_storage_from_stash_fund_manager with {} epochs",
            extension_epochs
        );

        let result = blob_manager_client
            .extend_storage_from_stash_fund_manager(extension_epochs)
            .await;

        match &result {
            Ok(_) => tracing::info!("Successfully extended storage"),
            Err(e) => tracing::error!("Error extending storage: {:?}", e),
        }
        result?;
    }

    // Get updated storage info.
    tracing::info!("About to re-init blob manager with cap_id: {}", cap_id);
    client.init_blob_manager(cap_id).await?; // Re-init to refresh cached data.
    tracing::info!("Successfully re-initialized blob manager");

    tracing::info!("Getting storage info after extension");
    let extended_storage_info = client.get_blob_manager_client()?.get_storage_info().await?;
    tracing::info!("Successfully got extended storage info");

    // The dormant extension should extend from the current epoch forward.
    // Since we're in dormant mode (past the original end_epoch), the extension
    // starts from the current epoch and adds the requested extension epochs.
    let expected_minimum_epochs_extended = extension_epochs; // At least the requested extension.
    let actual_extension = extended_storage_info.end_epoch - initial_storage_info.end_epoch;

    tracing::info!(
        "Extended storage: end_epoch={} (was {}), actual extension={} epochs",
        extended_storage_info.end_epoch,
        initial_storage_info.end_epoch,
        actual_extension
    );

    assert!(
        actual_extension >= expected_minimum_epochs_extended,
        "Storage should be extended by at least {} epochs, but only extended by {}",
        expected_minimum_epochs_extended,
        actual_extension
    );

    // Verify blob is now accessible again after extension.
    // This confirms that the dormant mode extension successfully restored access.
    tracing::info!("Verifying blob is now accessible again after extension...");
    assert!(
        verify_blob_readable(client, &blob_id, &test_data)
            .await
            .is_ok(),
        "Blob should be readable after extension"
    );
    tracing::info!("SUCCESS: Blob is readable again after dormant mode extension!");

    // We've verified that:
    // 1. The blob was NOT accessible during dormant mode (as expected)
    // 2. The storage was successfully extended
    // 3. The blob is NOW accessible again after extension
    // This confirms the dormant mode extension worked correctly.

    tracing::info!("=== Dormant Mode Extension Test PASSED ===");
    Ok(())
}

/// Test that managed blobs become unavailable after grace period expires.
///
/// This test:
/// 1. Creates a BlobManager with very short storage duration
/// 2. Stores a managed blob
/// 3. Waits for the BlobManager storage to expire
/// 4. Waits for the grace period to expire
/// 5. Verifies the blob is no longer available (garbage collected)
///
/// NOTE: This test is disabled because grace period is now deterministic based on storage duration.
/// Without the ability to precisely control epochs or set custom grace periods in tests,
/// it's difficult to reliably test garbage collection timing. The grace period calculation is:
/// < 5 epochs: 0, < 10: 1, < 20: 2, < 35: 3, < 60: 4, >= 60: 4 + floor((delta-60)/20).
#[ignore = "Disabled: deterministic grace period makes GC timing testing difficult"]
#[walrus_simtest]
async fn test_blob_manager_garbage_collection() -> TestResult {
    walrus_test_utils::init_tracing();
    tracing::info!("=== Starting Managed Blob Garbage Collection Test ===");

    // Use short epoch duration for faster testing.
    let epoch_duration = Duration::from_secs(5);
    let test_nodes_config = TestNodesConfig {
        node_weights: vec![7, 7, 7, 7, 7],
        ..Default::default()
    };

    let (_sui_cluster_handle, _cluster, mut client, _) = test_cluster::E2eTestSetupBuilder::new()
        .with_test_nodes_config(test_nodes_config)
        .with_epoch_duration(epoch_duration)
        .build()
        .await?;
    let client = client.as_mut();

    // Create a BlobManager with very short storage (just 1 epoch).
    // Note: grace period is 2 epochs by default.
    let initial_capacity = 500 * 1000 * 1000; // 500MB capacity (minimum required).
    let initial_epochs = 1; // BlobManager will expire after 1 epoch.

    let (_manager_id, cap_id) = client
        .sui_client()
        .create_blob_manager(initial_capacity, initial_epochs)
        .await?;
    tracing::info!(
        "Created BlobManager with {} epoch storage, cap_id: {}",
        initial_epochs,
        cap_id
    );

    // Initialize BlobManager client.
    client.init_blob_manager(cap_id).await?;

    // Get expected expiry times.
    // We assume we're starting near epoch 0 since this is a fresh test cluster.
    let storage_end_epoch = 1; // Storage expires after 1 epoch.
    let grace_period_epochs = 2; // Default grace period.
    let gc_eligible_epoch = storage_end_epoch + grace_period_epochs;

    tracing::info!(
        "Storage ends at epoch: {}, GC eligible at epoch: {}",
        storage_end_epoch,
        gc_eligible_epoch
    );

    // Store a blob in the BlobManager.
    let test_data = walrus_test_utils::random_data(1024);

    let store_args = StoreArgs::default_with_epochs(1)
        .with_blob_manager()
        .deletable();

    let results = client
        .reserve_and_store_blobs_retry_committees(vec![test_data.clone()], vec![], &store_args)
        .await?;

    let blob_id = results[0].blob_id().expect("blob ID should be present");
    tracing::info!("Stored blob {} in BlobManager", blob_id);

    // Verify blob is initially accessible.
    verify_blob_readable(client, &blob_id, &test_data).await?;
    tracing::info!("Verified blob is initially readable");

    // Wait for storage to expire (1 epoch = 5 seconds).
    tracing::info!(
        "Waiting for storage to expire at epoch {} (5 seconds)...",
        storage_end_epoch
    );
    tokio::time::sleep(Duration::from_secs(8)).await; // Wait slightly more than 1 epoch.

    // Blob should still be accessible during grace period.
    assert!(
        verify_blob_readable(client, &blob_id, &test_data)
            .await
            .is_ok(),
        "Blob should not be readable during grace period"
    );

    // Wait for grace period to expire (2 more epochs = 10 seconds).
    tracing::info!(
        "Waiting for grace period to expire at epoch {} (10 more seconds)...",
        gc_eligible_epoch
    );
    tokio::time::sleep(Duration::from_secs(20)).await; // Wait slightly more than 2 epochs.

    // Small delay to ensure nodes have processed the epoch change.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Now blob should be unavailable (garbage collected).
    tracing::info!("Checking if blob has been garbage collected...");

    // Check blob status - should be nonexistent.
    let status = client
        .get_verified_blob_status(
            &blob_id,
            client.sui_client().read_client(),
            Duration::from_secs(1),
        )
        .await?;

    assert!(
        matches!(status, BlobStatus::Nonexistent),
        "Expected blob to be garbage collected, but status is: {:?}",
        status
    );
    tracing::info!("Blob status is Nonexistent as expected");

    // Try to read the blob - should fail.
    let read_result = client.read_blob::<Primary>(&blob_id).await;
    assert!(
        read_result.is_err(),
        "Expected blob read to fail after GC, but it succeeded"
    );

    if let Err(e) = read_result {
        assert!(
            matches!(e.kind(), ClientErrorKind::BlobIdDoesNotExist),
            "Expected BlobIdDoesNotExist error, but got: {:?}",
            e.kind()
        );
        tracing::info!("Blob read failed with BlobIdDoesNotExist as expected");
    }

    tracing::info!("=== Managed Blob Garbage Collection Test PASSED ===");
    Ok(())
}

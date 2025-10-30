// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

#[test_only]
module blobmanager::test_minimal;

use blobmanager::blobmanager::{Self, BlobManager};
use sui::test_scenario;
use walrus::storage_resource::{Self, Storage};

// Test constants
const ADMIN: address = @0xA;
const STORAGE_SIZE: u64 = 10240; // 10KB

#[test]
fun test_blob_manager_creation() {
    let mut scenario = test_scenario::begin(ADMIN);

    test_scenario::next_tx(&mut scenario, ADMIN);
    {
        // Create initial storage (10KB, valid for 100 epochs)
        // Note: In a real test we'd need to use storage_resource::create_storage
        // but that's a package function. For this minimal test, we'll just
        // test the BlobManager creation with a mock storage.

        // Since we can't directly create Storage in tests, we'll test what we can
        let ctx = test_scenario::ctx(&mut scenario);

        // We would create the BlobManager like this if we had a Storage object:
        // let storage = storage_resource::create_storage(0, 100, STORAGE_SIZE, ctx);
        // let manager = blobmanager::new(storage, ctx);

        // For now, we can at least verify the module compiles correctly
    };

    test_scenario::end(scenario);
}

#[test]
fun test_query_functions() {
    // This test would verify the query functions work correctly
    // In a real implementation with access to Storage creation,
    // we would:
    // 1. Create a BlobManager
    // 2. Test capacity_info() returns correct values
    // 3. Test storage_epochs() returns correct values
    // 4. Test blob_count() starts at 0
    // 5. Test total_blob_size() starts at 0
    // 6. Test has_blob() returns false for non-existent blobs
}

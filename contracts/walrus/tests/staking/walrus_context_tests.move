// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

module walrus::walrus_context_tests;

use walrus::walrus_context;

#[test]
// Scenario: Test the WalrusContext flow
fun test_walrus_context_flow() {
    let walrus_ctx = walrus_context::new(1, true);

    // assert that the WalrusContext is created correctly
    assert!(walrus_context::epoch(&walrus_ctx) == 1);
    assert!(walrus_context::committee_selected(&walrus_ctx) == true);
}

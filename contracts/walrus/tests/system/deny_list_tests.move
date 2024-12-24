// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[test_only]
module walrus::deny_list_tests;

use std::{bcs, unit_test::assert_eq};
use walrus::messages;

#[test]
fun test_deny_list_update() {
    let bytes = new_deny_list_update_message(@0x1.to_id(), 1, 1, 1u256);
    let message = messages::new_certified_message(bytes, 1, 100);
    let update_message = message.deny_list_update_message();

    assert_eq!(update_message.storage_node_id(), @0x1.to_id());
    assert_eq!(update_message.sequence_number(), 1);
    assert_eq!(update_message.size(), 1);
    assert_eq!(update_message.root(), 1u256);
}

fun new_deny_list_update_message(
    id: ID,
    deny_list_sequence_number: u64,
    deny_list_size: u64,
    deny_list_root: u256,
): vector<u8> {
    let mut bytes = vector[3, 0, 3]; // intent type, version, app id
    bytes.append(bcs::to_bytes(&1u32)); // epoch
    bytes.append(bcs::to_bytes(&id));
    bytes.append(bcs::to_bytes(&deny_list_sequence_number));
    bytes.append(bcs::to_bytes(&deny_list_size));
    bytes.append(bcs::to_bytes(&deny_list_root));
    bytes
}

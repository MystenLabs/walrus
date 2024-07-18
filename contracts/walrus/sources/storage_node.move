// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[allow(unused_field, unused_function, unused_variable)]
module walrus::storage_node {
    use std::string::String;

    public struct StorageNodeInfo has store, drop {}

    public struct StorageNodeCap has key, store { id: UID, pool_id: ID }

    public enum NodeState has store, drop {
        Active,
        Withdrawing,
        Custom(String),
    }
}

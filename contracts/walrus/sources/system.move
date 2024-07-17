// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

// Staking is also done similar to Sui (with “staking pools”) with a few important distinctions
// - The `StakedWal` object that gets returned to stakers does not contain an actual `Balance<WAL>`,
//  since this would prevent us from slashing the principal. Instead it just contains a field
//  containing the staked amount and the combined balance per pool is stored in the staking pool
//
// - In Sui, the validators are tightly coupled to their addresses and e.g. rewards are paid to the
//  addresses directly. This seems to be mostly done to have two levels of access control, where the
//  owner of the address can create capabilities to allow certain operations. In Walrus, we do
//  everything with a capability, but provide `CapabilityManager`s, shared objects that store a
//  capability on behalf of a node or client that itself provides capability objects for which it
//  offers fine-grained access control (see below). There will be two distinct `CapabilityManager`
//  types, one for stakers and one for storage nodes (use of these will be optional at least for
// stakers). This separates access control from the core logic and provides a clean and customizable
// solution to the problem of [separating custody from governance](https://www.notion.so/Token-mechanics-requirements-e6786d62236f4dfd8f56985e2ffea69e?pvs=21). See [below](https://www.notion.so/Walrus-Smart-Contract-Design-163c820d62fe4fcbbec653bf2d39e3b1?pvs=21) for details.
//
// - Storage nodes are identified by the object ID of their `StorageNodeCapability` , instead of,
//  as currently, by their BLS key, or as in Sui, by their address. The advantage of this is that
//  keys can be rotated. Another advantage specifically compared to using the BLS keys is that we
//  don’t need to ensure that all registered keys are distinct.
//
// - Requires changes in `walrus-service`  where nodes are currently identified by BLS public key

/// Module: staking
module walrus::system {
    public struct WalrusSystem has key {
        id: UID,
        version: u16,
    }

    public struct EpochStatus(u16) has store, drop;

    public struct WalrusSystemInner has store {
        current_epoch: u64,
        epoch_status: EpochStatus,
        capacity: u64,
        storage_price: u64,
        write_price: u64,
        next_capacity: Option<u64>,
        next_storage_price: Option<u64>,
        next_write_price: Option<u64>,
        next_epoch_start: Option<u64>,
    }
}

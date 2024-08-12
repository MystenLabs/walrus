// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/// Module: `walrus_context`
///
/// Implements the `WalrusContext` struct which is used to store the current
/// state of the system. Improves testing and readability of signatures by
/// aggregating the parameters into a single struct. Context is used almost
/// everywhere in the system, so it is important to have a single source of
/// truth for the current state.
module walrus::walrus_context;

/// Represents the current values in the Walrus system. Helps avoid passing
/// too many parameters to functions, and allows for easier testing.
public struct WalrusContext has drop {
    /// Current Walrus epoch
    epoch: u64,
    /// Whether the committee has been selected for the next epoch.
    committee_selected: bool,
}

/// Create a new `WalrusContext` object.
public(package) fun new(epoch: u64, committee_selected: bool): WalrusContext {
    WalrusContext { epoch, committee_selected }
}

/// Read the current `epoch` from the context.
public(package) fun epoch(self: &WalrusContext): u64 { self.epoch }

/// Read the current `committee_selected` from the context.
public(package) fun committee_selected(self: &WalrusContext): bool { self.committee_selected }

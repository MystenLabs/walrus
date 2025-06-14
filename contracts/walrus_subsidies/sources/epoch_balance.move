// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

module walrus_subsidies::epoch_balance;

// Error codes
// Error types in `walrus-sui/types/move_errors.rs` are auto-generated from the Move error codes.
/// Trying to access an entry out of bounds.
const EIndexOutOfBounds: u64 = 0;

// === Structs ===

/// A pair mapping an epoch to a balance.
public struct EpochBalance has copy, drop, store {
    epoch: u32,
    balance: u64,
}

/// A ring buffer to hold the epoch balances.
public struct EpochBalanceRingBuffer has store {
    current_index: u32,
    ring_buffer: vector<EpochBalance>,
}

/// Constructor for EpochBalanceRingBuffer.
public(package) fun ring_new_from_balances(
    starting_epoch: u32,
    balances: vector<u64>,
): EpochBalanceRingBuffer {
    let mut epoch = starting_epoch;
    let ring_buffer = balances.map!(|balance| {
        let epoch_balance = EpochBalance {
            epoch,
            balance,
        };
        epoch = epoch + 1;
        epoch_balance
    });
    EpochBalanceRingBuffer { current_index: 0, ring_buffer }
}

// === Accessors for EpochBalance ===

public(package) fun epoch(self: &EpochBalance): u32 {
    self.epoch
}

public(package) fun balance(self: &EpochBalance): u64 {
    self.balance
}

// === Modifiers for EpochBalance ===

public(package) fun set_balance(self: &mut EpochBalance, balance: u64) {
    self.balance = balance;
}

// === Accessors for EpochBalanceRingBuffer ===

public(package) fun length(self: &EpochBalanceRingBuffer): u32 {
    self.ring_buffer.length() as u32
}

/// Immutable access to the element at `index` in the ring buffer.
#[syntax(index)]
public(package) fun ring_lookup(self: &EpochBalanceRingBuffer, index: u32): &EpochBalance {
    // Check for out-of-bounds access.
    assert!(index < self.length(), EIndexOutOfBounds);

    let vector_index = (index + self.current_index) % self.length();
    &self.ring_buffer[vector_index as u64]
}

/// Mutable access to the element at `index` in the ring buffer.
#[syntax(index)]
public(package) fun ring_lookup_mut(
    self: &mut EpochBalanceRingBuffer,
    index: u32,
): &mut EpochBalance {
    // Check for out-of-bounds access.
    assert!(index < self.length(), EIndexOutOfBounds);

    let vector_index = (index + self.current_index) % self.length();
    &mut self.ring_buffer[vector_index as u64]
}

/// Removes the current element from the ring buffer and expands it with a new element.
public(package) fun ring_pop_expand(self: &mut EpochBalanceRingBuffer): EpochBalance {
    // Get current epoch
    let current_index = self.current_index;
    let current_epoch = self.ring_buffer[current_index as u64].epoch;
    let new_epoch = current_epoch + (self.length() as u32);

    // Expand the ring buffer
    self
        .ring_buffer
        .push_back(EpochBalance {
            epoch: new_epoch,
            balance: 0,
        });

    // Now swap remove the current element and increment the current_index
    let epoch_balance = self.ring_buffer.swap_remove(current_index as u64);
    self.current_index = (current_index + 1) % self.length();
    epoch_balance
}

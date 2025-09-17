// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/// TimerWheel is a generic timer wheel data structure that stores values for
/// future epochs. It's implemented as a ring buffer similar to the storage
/// accounting ring buffer in Walrus, but more generic.
///
/// The timer wheel can store values of type T for any epoch in the future up to
/// max_epochs_ahead. When advancing epochs, entries for skipped epochs are
/// automatically cleared.
module blobmanager::timer_wheel;

// === Error Codes ===

/// Trying to access an epoch that is too far in the future
const ETooFarInFuture: u64 = 0;

/// Trying to access an epoch that is in the past
const EEpochInPast: u64 = 1;

/// Trying to move to an epoch that is before the current epoch
const ECannotMoveBackwards: u64 = 2;

/// TimerWheel holds values of type T for future epochs
public struct TimerWheel<T: store> has store {
    /// Current epoch index
    current_epoch: u32,
    /// Index in the ring buffer corresponding to the current epoch
    current_index: u32,
    /// Maximum number of epochs that can be stored ahead
    max_epochs_ahead: u32,
    /// Ring buffer storing optional values for each slot
    ring_buffer: vector<Option<T>>,
}

// === Constructors ===

// TODO: invert max epochs with current epoch
/// Create a new TimerWheel with the specified maximum number of epochs ahead
/// and the current epoch
public(package) fun empty<T: store>(max_epochs_ahead: u32, current_epoch: u32): TimerWheel<T> {
    let ring_buffer = vector::tabulate!(max_epochs_ahead as u64, |_| option::none());

    TimerWheel {
        current_epoch,
        current_index: 0,
        max_epochs_ahead,
        ring_buffer,
    }
}

// === Epoch Management ===

/// Advance the timer wheel forward by a number of epochs, returning cleared entries for skipped epochs
public(package) fun advance_by_and_return<T: store>(
    self: &mut TimerWheel<T>,
    epochs_ahead: u32,
): vector<T> {
    let mut cleared_values = vector[];

    if (epochs_ahead >= self.max_epochs_ahead) {
        // If we advance more than our buffer size, clear everything
        // TODO: just replace the vector with a new empty one instead of clearing each entry.
        let mut i = 0;
        while (i < self.max_epochs_ahead) {
            if (self.ring_buffer[i as u64].is_some()) {
                let value = self.ring_buffer[i as u64].extract();
                cleared_values.push_back(value);
            };
            i = i + 1;
        };
    } else {
        // Clear entries for epochs that are now in the "past" relative to the new current epoch
        // We clear from epoch (current_epoch + 1) to (current_epoch + epochs_ahead - 1) inclusive
        let mut i = 0;
        while (i < epochs_ahead) {
            let index = (self.current_index + i) % self.max_epochs_ahead;
            if (self.ring_buffer[index as u64].is_some()) {
                let value = self.ring_buffer[index as u64].extract();
                cleared_values.push_back(value);
            };
            i = i + 1;
        };
    };

    // Update current epoch and index
    self.current_epoch = self.current_epoch + epochs_ahead;
    self.current_index = (self.current_index + epochs_ahead) % self.max_epochs_ahead;

    cleared_values
}

/// Advance the timer wheel to a future epoch, returning cleared entries for skipped epochs
public(package) fun advance_to_epoch_and_return<T: store>(
    self: &mut TimerWheel<T>,
    target_epoch: u32,
): vector<T> {
    assert!(target_epoch >= self.current_epoch, ECannotMoveBackwards);

    let epochs_to_advance = target_epoch - self.current_epoch;
    self.advance_by_and_return(epochs_to_advance)
}

// === Insert Operations ===

/// Insert a value for a specific epoch (absolute epoch number)
public(package) fun insert_at_epoch<T: store>(
    self: &mut TimerWheel<T>,
    epoch: u32,
    value: T,
): Option<T> {
    assert!(epoch >= self.current_epoch, EEpochInPast);

    let epochs_ahead = epoch - self.current_epoch;
    assert!(epochs_ahead < self.max_epochs_ahead, ETooFarInFuture);

    let index = (self.current_index + epochs_ahead) % self.max_epochs_ahead;
    let slot = &mut self.ring_buffer[index as u64];
    slot.swap_or_fill(value)
}

/// Insert a value for a specific number of epochs ahead
public(package) fun insert<T: store>(
    self: &mut TimerWheel<T>,
    epochs_ahead: u32,
    value: T,
): Option<T> {
    assert!(epochs_ahead < self.max_epochs_ahead, ETooFarInFuture);

    let index = (self.current_index + epochs_ahead) % self.max_epochs_ahead;
    let slot = &mut self.ring_buffer[index as u64];
    slot.swap_or_fill(value)
}

// === Take Operations ===

/// Remove and return the value at a specific epoch
public(package) fun take_at_epoch<T: store>(self: &mut TimerWheel<T>, epoch: u32): Option<T> {
    assert!(epoch >= self.current_epoch, EEpochInPast);

    let epochs_ahead = epoch - self.current_epoch;
    assert!(epochs_ahead < self.max_epochs_ahead, ETooFarInFuture);

    let index = (self.current_index + epochs_ahead) % self.max_epochs_ahead;
    let value_ref = &mut self.ring_buffer[index as u64];
    if (value_ref.is_some()) {
        let value = value_ref.extract();
        option::some(value)
    } else {
        option::none()
    }
}

/// Remove and return the value at a specific number of epochs ahead
public(package) fun take<T: store>(self: &mut TimerWheel<T>, epochs_ahead: u32): Option<T> {
    assert!(epochs_ahead < self.max_epochs_ahead, ETooFarInFuture);

    let index = (self.current_index + epochs_ahead) % self.max_epochs_ahead;
    let value_ref = &mut self.ring_buffer[index as u64];
    if (value_ref.is_some()) {
        let value = value_ref.extract();
        option::some(value)
    } else {
        option::none()
    }
}

// === Borrow Operations ===

/// Borrow a reference to the value at a specific epoch
public(package) fun borrow_at_epoch<T: store>(self: &TimerWheel<T>, epoch: u32): &Option<T> {
    assert!(epoch >= self.current_epoch, EEpochInPast);

    let epochs_ahead = epoch - self.current_epoch;
    assert!(epochs_ahead < self.max_epochs_ahead, ETooFarInFuture);

    let index = (self.current_index + epochs_ahead) % self.max_epochs_ahead;
    &self.ring_buffer[index as u64]
}

/// Borrow a reference to the value at a specific number of epochs ahead
public(package) fun borrow<T: store>(self: &TimerWheel<T>, epochs_ahead: u32): &Option<T> {
    assert!(epochs_ahead < self.max_epochs_ahead, ETooFarInFuture);

    let index = (self.current_index + epochs_ahead) % self.max_epochs_ahead;
    &self.ring_buffer[index as u64]
}

/// Borrow a mutable reference to the value at a specific epoch
public(package) fun borrow_mut_at_epoch<T: store>(
    self: &mut TimerWheel<T>,
    epoch: u32,
): &mut Option<T> {
    assert!(epoch >= self.current_epoch, EEpochInPast);

    let epochs_ahead = epoch - self.current_epoch;
    assert!(epochs_ahead < self.max_epochs_ahead, ETooFarInFuture);

    let index = (self.current_index + epochs_ahead) % self.max_epochs_ahead;
    &mut self.ring_buffer[index as u64]
}

/// Borrow a mutable reference to the value at a specific number of epochs ahead
public(package) fun borrow_mut<T: store>(
    self: &mut TimerWheel<T>,
    epochs_ahead: u32,
): &mut Option<T> {
    assert!(epochs_ahead < self.max_epochs_ahead, ETooFarInFuture);

    let index = (self.current_index + epochs_ahead) % self.max_epochs_ahead;
    &mut self.ring_buffer[index as u64]
}

// === Accessors ===

/// Get the current epoch
public(package) fun current_epoch<T: store>(self: &TimerWheel<T>): u32 {
    self.current_epoch
}

/// Get the maximum number of epochs that can be stored ahead
public(package) fun max_epochs_ahead<T: store>(self: &TimerWheel<T>): u32 {
    self.max_epochs_ahead
}

/// Check if there is a value at a specific epoch
public(package) fun contains_at_epoch<T: store>(self: &TimerWheel<T>, epoch: u32): bool {
    assert!(epoch >= self.current_epoch, EEpochInPast);

    let epochs_ahead = epoch - self.current_epoch;
    assert!(epochs_ahead < self.max_epochs_ahead, ETooFarInFuture);

    let index = (self.current_index + epochs_ahead) % self.max_epochs_ahead;
    self.ring_buffer[index as u64].is_some()
}

/// Check if there is a value at a specific number of epochs ahead
public(package) fun contains<T: store>(self: &TimerWheel<T>, epochs_ahead: u32): bool {
    assert!(epochs_ahead < self.max_epochs_ahead, ETooFarInFuture);

    let index = (self.current_index + epochs_ahead) % self.max_epochs_ahead;
    self.ring_buffer[index as u64].is_some()
}

/// Clear all entries in the timer wheel and return the cleared values
public(package) fun clear_and_return<T: store>(self: &mut TimerWheel<T>): vector<T> {
    let mut cleared_values = vector[];
    let mut i = 0;
    while (i < self.max_epochs_ahead) {
        if (self.ring_buffer[i as u64].is_some()) {
            let value = self.ring_buffer[i as u64].extract();
            cleared_values.push_back(value);
        };
        i = i + 1;
    };
    cleared_values
}

/// Destroy a timer wheel by clearing and returning all values first
public(package) fun destroy_with_clear<T: store>(mut self: TimerWheel<T>): vector<T> {
    let cleared_values = self.clear_and_return();
    let TimerWheel { ring_buffer, .. } = self;
    ring_buffer.destroy!(|opt| {
        if (opt.is_some()) {
            // Should not happen since we cleared everything
            abort 999
        } else {
            opt.destroy_none();
        };
    });
    cleared_values
}

// === Test Helpers ===

#[test_only]
/// Destroy a timer wheel (for testing only)
public(package) fun destroy<T: store + drop>(self: TimerWheel<T>) {
    let TimerWheel { ring_buffer, .. } = self;
    ring_buffer.destroy!(|opt| {
        if (opt.is_some()) {
            let _val = opt.destroy_some();
        } else {
            opt.destroy_none();
        };
    });
}

// === Tests ===

#[test]
fun test_timer_wheel_basic() {
    let mut wheel = empty<u64>(5, 0);
    assert!(wheel.current_epoch() == 0, 0);
    assert!(wheel.max_epochs_ahead() == 5, 1);

    // Insert values
    wheel.insert(0, 100);
    wheel.insert(1, 200);
    wheel.insert(2, 300);

    assert!(wheel.contains(0), 2);
    assert!(wheel.contains(1), 3);
    assert!(wheel.contains(2), 4);

    // Remove value at current epoch
    let val = wheel.take(0);
    assert!(val == option::some(100), 5);
    assert!(!wheel.contains(0), 6);

    wheel.destroy();
}

#[test]
fun test_timer_wheel_advance_epoch() {
    let mut wheel = empty<u64>(10, 10);

    // Insert values at future epochs
    wheel.insert_at_epoch(10, 100);
    wheel.insert_at_epoch(12, 200);
    wheel.insert_at_epoch(14, 300);

    assert!(wheel.contains_at_epoch(10), 0);
    assert!(wheel.contains_at_epoch(12), 1);
    assert!(wheel.contains_at_epoch(14), 2);

    // Advance to epoch 12
    wheel.advance_to_epoch_and_return(12);
    assert!(wheel.current_epoch() == 12, 3);

    // Epoch 12 and 14 should still exist
    assert!(wheel.contains_at_epoch(12), 5);
    assert!(wheel.contains_at_epoch(14), 6);

    // Can retrieve value at current epoch
    let val = wheel.take_at_epoch(12);
    assert!(val == option::some(200), 7);

    wheel.destroy();
}

#[test]
fun test_timer_wheel_wrap_around() {
    let mut wheel = empty<u64>(3, 0);

    // Fill all slots
    wheel.insert(0, 100);
    wheel.insert(1, 200);
    wheel.insert(2, 300);

    // Advance forward by 2 epochs
    wheel.advance_to_epoch_and_return(2);

    // Now insert at epochs ahead - should wrap around in buffer
    wheel.insert(1, 400);
    wheel.insert(2, 500);

    assert!(wheel.contains(0), 0); // Still has old value at current epoch
    assert!(wheel.contains(1), 1);
    assert!(wheel.contains(2), 2);

    let val = wheel.take(0);
    assert!(val == option::some(300), 3); // This was originally at epoch 2

    wheel.destroy();
}

#[test]
#[expected_failure(abort_code = ETooFarInFuture)]
fun test_insert_too_far_ahead() {
    let mut wheel = empty<u64>(10, 5);
    wheel.insert(10, 100); // Should fail, max is 5 epochs ahead
    wheel.destroy();
}

#[test]
#[expected_failure(abort_code = EEpochInPast)]
fun test_insert_in_past() {
    let mut wheel = empty<u64>(5, 10);
    wheel.insert_at_epoch(5, 100); // Should fail, current epoch is 10
    wheel.destroy();
}

#[test]
#[expected_failure(abort_code = ECannotMoveBackwards)]
fun test_advance_backwards() {
    let mut wheel = empty<u64>(5, 10);
    wheel.advance_to_epoch_and_return(5); // Should fail, can't go backwards
    wheel.destroy();
}

#[test]
fun test_borrow_operations() {
    let mut wheel = empty<u64>(10, 5);
    wheel.insert(1, 100);

    // Borrow immutable
    let borrowed = wheel.borrow(1);
    assert!(borrowed.is_some(), 0);
    assert!(*borrowed.borrow() == 100, 1);

    // Borrow mutable and modify
    let borrowed_mut = wheel.borrow_mut(1);
    if (borrowed_mut.is_some()) {
        *borrowed_mut.borrow_mut() = 200;
    };

    // Verify modification
    let val = wheel.take(1);
    assert!(val == option::some(200), 2);

    wheel.destroy();
}

#[test]
fun test_clear() {
    let mut wheel = empty<u64>(10, 5);

    // Add multiple values
    wheel.insert(0, 100);
    wheel.insert(1, 200);
    wheel.insert(2, 300);
    wheel.insert(3, 400);

    assert!(wheel.contains(0), 0);
    assert!(wheel.contains(1), 1);
    assert!(wheel.contains(2), 2);
    assert!(wheel.contains(3), 3);

    // Clear all entries
    wheel.clear_and_return();

    assert!(!wheel.contains(0), 4);
    assert!(!wheel.contains(1), 5);
    assert!(!wheel.contains(2), 6);
    assert!(!wheel.contains(3), 7);

    wheel.destroy();
}

#[test]
#[expected_failure(abort_code = EEpochInPast)]
fun test_contains_at_past_epoch() {
    let wheel = empty<u64>(5, 10);
    wheel.contains_at_epoch(5); // Should abort - epoch is in the past
    wheel.destroy();
}

#[test]
#[expected_failure(abort_code = ETooFarInFuture)]
fun test_contains_at_future_epoch() {
    let wheel = empty<u64>(5, 10);
    wheel.contains_at_epoch(15); // Should abort - epoch is too far in future
    wheel.destroy();
}

#[test]
#[expected_failure(abort_code = ETooFarInFuture)]
fun test_contains_too_far_ahead() {
    let wheel = empty<u64>(10, 5);
    wheel.contains(10); // Should abort - too far ahead
    wheel.destroy();
}

#[test]
#[expected_failure(abort_code = EEpochInPast)]
fun test_take_at_past_epoch() {
    let mut wheel = empty<u64>(5, 10);
    wheel.take_at_epoch(5); // Should abort - epoch is in the past
    wheel.destroy();
}

#[test]
#[expected_failure(abort_code = ETooFarInFuture)]
fun test_take_at_future_epoch() {
    let mut wheel = empty<u64>(5, 10);
    wheel.take_at_epoch(15); // Should abort - epoch is too far in future
    wheel.destroy();
}

#[test]
#[expected_failure(abort_code = ETooFarInFuture)]
fun test_take_too_far_ahead() {
    let mut wheel = empty<u64>(10, 5);
    wheel.take(10); // Should abort - too far ahead
    wheel.destroy();
}

#[test]
fun test_advance_by() {
    let mut wheel = empty<u64>(5, 10);

    // Insert values at future epochs
    wheel.insert_at_epoch(10, 100);
    wheel.insert_at_epoch(12, 200);
    wheel.insert_at_epoch(14, 300);

    assert!(wheel.current_epoch() == 10, 0);
    assert!(wheel.contains_at_epoch(10), 1);
    assert!(wheel.contains_at_epoch(12), 2);
    assert!(wheel.contains_at_epoch(14), 3);

    // Advance forward by 2 epochs using advance_by
    wheel.advance_by_and_return(2);
    assert!(wheel.current_epoch() == 12, 4);

    // Epoch 12 and 14 should still exist
    assert!(wheel.contains_at_epoch(12), 5);
    assert!(wheel.contains_at_epoch(14), 6);

    // Can retrieve value at current epoch
    let val = wheel.take_at_epoch(12);
    assert!(val == option::some(200), 7);

    wheel.destroy();
}

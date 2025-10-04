// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/// BigStack is an arbitrary sized stack-like data structure.
///
/// It is implemented using dynamic fields to support very large stacks that
/// exceed the maximum object size limit.
///
/// The stack is organized as a list of slices, where each slice contains a
/// vector of elements. When a slice becomes full (reaches max_slice_size), a
/// new slice is created .
///
/// Each slice has a unique, sequential `u64` ID that is used to reference it.
module blobmanager::big_stack;

use sui::dynamic_field as df;

use fun sui::object::new as TxContext.new;

/// BigStack struct containing the stack metadata
public struct BigStack<phantom E: store> has key, store {
    /// Object ID for storing dynamic fields
    id: UID,
    /// Total number of elements in the stack
    length: u64,
    /// Maximum size of each slice (vector) in the dynamic fields
    max_slice_size: u64,
    /// ID of the root slice (bottom of the stack)
    root_id: u64,
    /// ID of the last slice (top of the stack)
    last_id: u64,
}

/// A slice containing a vector of values
public struct Slice<E: store> has store {
    /// The vector of values stored in this slice
    vals: vector<E>,
}

// === Error Codes ===

/// Stack is not empty
const ENotEmpty: u64 = 0;

/// Stack is empty
const EEmpty: u64 = 1;

/// Max slice size is too small
const ESliceTooSmall: u64 = 2;

/// Max slice size is too big
const ESliceTooBig: u64 = 3;

// === Constants ===

/// Sentinel representing the absence of a slice
const NO_SLICE: u64 = 0;

/// Minimum size for a slice
const MIN_SLICE_SIZE: u64 = 2;

/// Maximum size for a slice to avoid hitting object size limits
const MAX_SLICE_SIZE: u64 = 256 * 1024;

// === Constructors ===

/// Construct a new, empty BigStack with the specified max_slice_size
public(package) fun empty<E: store>(max_slice_size: u64, ctx: &mut TxContext): BigStack<E> {
    assert!(MIN_SLICE_SIZE <= max_slice_size, ESliceTooSmall);
    assert!(max_slice_size <= MAX_SLICE_SIZE, ESliceTooBig);

    BigStack {
        id: ctx.new(),
        length: 0,
        max_slice_size,
        root_id: NO_SLICE,
        last_id: NO_SLICE,
    }
}

/// Destroy an empty BigStack
public(package) fun destroy_empty<E: store>(self: BigStack<E>) {
    let BigStack {
        id,
        length,
        max_slice_size: _,
        root_id: _,
        last_id: _,
    } = self;

    assert!(length == 0, ENotEmpty);
    id.delete();
}

// === Accessors ===

/// Check if the stack is empty
public(package) fun is_empty<E: store>(self: &BigStack<E>): bool {
    self.length == 0
}

/// Get the number of elements in the stack
public(package) fun length<E: store>(self: &BigStack<E>): u64 {
    self.length
}

/// Peek at the top element without removing it
public(package) fun peek<E: store>(self: &BigStack<E>): &E {
    assert!(!self.is_empty(), EEmpty);

    let slice: &Slice<E> = df::borrow(&self.id, self.last_id);
    let len = slice.vals.length();
    &slice.vals[len - 1]
}

/// Peek at the top element mutably without removing it
public(package) fun peek_mut<E: store>(self: &mut BigStack<E>): &mut E {
    assert!(!self.is_empty(), EEmpty);

    let slice: &mut Slice<E> = df::borrow_mut(&mut self.id, self.last_id);
    let len = slice.vals.length();
    &mut slice.vals[len - 1]
}

// === Mutators ===

/// Push an element onto the stack
public(package) fun push<E: store>(self: &mut BigStack<E>, val: E) {
    self.length = self.length + 1;

    if (self.last_id == NO_SLICE) {
        // Create the first slice
        let slice = Slice { vals: vector[val] };
        self.last_id = self.alloc_slice(slice);
        self.root_id = self.last_id;
    } else {
        let slice: &mut Slice<E> = df::borrow_mut(&mut self.id, self.last_id);

        if (slice.vals.length() < self.max_slice_size) {
            // Current slice has room
            slice.vals.push_back(val);
        } else {
            // Current slice is full, create a new one
            let new_slice = Slice { vals: vector[val] };
            self.last_id = self.alloc_slice(new_slice);
        }
    }
}

/// Pop an element from the stack
public(package) fun pop<E: store>(self: &mut BigStack<E>): E {
    assert!(!self.is_empty(), EEmpty);

    self.length = self.length - 1;

    let slice: &mut Slice<E> = df::borrow_mut(&mut self.id, self.last_id);
    let val = slice.vals.pop_back();

    // If the slice is now empty and it's not the root, remove it
    if (slice.vals.is_empty() && self.last_id != self.root_id) {
        let Slice { vals }: Slice<E> = df::remove(&mut self.id, self.last_id);
        vals.destroy_empty();

        // Find the previous slice by searching backwards
        self.last_id = self.find_previous_slice();
    } else if (slice.vals.is_empty() && self.last_id == self.root_id) {
        // Stack is now completely empty
        let Slice { vals }: Slice<E> = df::remove(&mut self.id, self.last_id);
        vals.destroy_empty();

        self.root_id = NO_SLICE;
        self.last_id = NO_SLICE;
    };

    val
}

// === Private Helpers ===

/// Allocate a new slice and return its ID
fun alloc_slice<E: store>(self: &mut BigStack<E>, slice: Slice<E>): u64 {
    let new_id = if (self.last_id == NO_SLICE) {
        1
    } else {
        self.last_id + 1
    };

    df::add(&mut self.id, new_id, slice);
    new_id
}

/// Find the ID of the slice that comes before the current last_id
fun find_previous_slice<E: store>(self: &BigStack<E>): u64 {
    // Since we allocate slice IDs sequentially, the previous slice
    // is simply last_id - 1
    assert!(self.last_id > 1, EEmpty);
    self.last_id - 1
}

// === Test Helpers ===

#[test_only]
/// Drop a BigStack even if it's not empty (for testing only)
public(package) fun drop<E: store + drop>(self: BigStack<E>) {
    let BigStack {
        mut id,
        length: _,
        max_slice_size: _,
        root_id,
        last_id,
    } = self;

    // Remove all slices
    let mut current_id = root_id;
    while (current_id <= last_id && current_id != NO_SLICE) {
        let Slice { vals: _ }: Slice<E> = df::remove(&mut id, current_id);
        current_id = current_id + 1;
    };

    id.delete();
}

// === Tests ===

#[test]
fun test_empty_stack() {
    use sui::test_scenario;

    let mut scenario = test_scenario::begin(@0x1);
    let ctx = test_scenario::ctx(&mut scenario);

    let stack = empty<u64>(100, ctx);
    assert!(stack.is_empty(), 0);
    assert!(stack.length() == 0, 1);

    stack.destroy_empty();

    test_scenario::end(scenario);
}

#[test]
fun test_push_pop() {
    use sui::test_scenario;

    let mut scenario = test_scenario::begin(@0x1);
    let ctx = test_scenario::ctx(&mut scenario);

    let mut stack = empty<u64>(100, ctx);

    // Push some elements
    stack.push(1);
    stack.push(2);
    stack.push(3);

    assert!(!stack.is_empty(), 0);
    assert!(stack.length() == 3, 1);
    assert!(*stack.peek() == 3, 2);

    // Pop elements
    assert!(stack.pop() == 3, 3);
    assert!(stack.pop() == 2, 4);
    assert!(stack.pop() == 1, 5);

    assert!(stack.is_empty(), 6);
    assert!(stack.length() == 0, 7);

    stack.destroy_empty();

    test_scenario::end(scenario);
}

#[test]
fun test_large_stack() {
    use sui::test_scenario;

    let mut scenario = test_scenario::begin(@0x1);
    let ctx = test_scenario::ctx(&mut scenario);

    let mut stack = empty<u64>(10, ctx);

    // Push more elements than fit in a single slice
    let mut i = 0;
    while (i < 25) {
        stack.push(i);
        i = i + 1;
    };

    assert!(stack.length() == 25, 0);
    assert!(*stack.peek() == 24, 1);

    // Pop all elements and verify LIFO order
    let mut i = 24;
    loop {
        let val = stack.pop();
        assert!(val == i, 2);

        if (i == 0) break;
        i = i - 1;
    };

    assert!(stack.is_empty(), 3);

    stack.destroy_empty();

    test_scenario::end(scenario);
}

#[test]
#[expected_failure(abort_code = EEmpty)]
fun test_pop_empty() {
    use sui::test_scenario;

    let mut scenario = test_scenario::begin(@0x1);
    let ctx = test_scenario::ctx(&mut scenario);

    let mut stack = empty<u64>(100, ctx);
    stack.pop(); // Should abort

    stack.drop();
    test_scenario::end(scenario);
}

#[test]
#[expected_failure(abort_code = EEmpty)]
fun test_peek_empty() {
    use sui::test_scenario;

    let mut scenario = test_scenario::begin(@0x1);
    let ctx = test_scenario::ctx(&mut scenario);

    let stack = empty<u64>(100, ctx);
    stack.peek(); // Should abort

    stack.drop();
    test_scenario::end(scenario);
}

#[test]
fun test_peek_mut() {
    use sui::test_scenario;

    let mut scenario = test_scenario::begin(@0x1);
    let ctx = test_scenario::ctx(&mut scenario);

    let mut stack = empty<u64>(100, ctx);
    stack.push(10);

    let top = stack.peek_mut();
    *top = 20;

    assert!(*stack.peek() == 20, 0);
    assert!(stack.pop() == 20, 1);

    stack.destroy_empty();

    test_scenario::end(scenario);
}

// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/// Holds the objects that implement Sui Object Display for user-owned Walrus objects.
///
/// `ObjectDisplay` stores the package `Publisher` under `PublisherKey`. On networks deployed before
/// Sui 1.84, it also stores the Display V1 objects for `Blob`, `Storage`, and `StakedWal`, keyed by
/// their type names.
module walrus::display;

use sui::{object_bag::{Self, ObjectBag}, package::Publisher};

/// The wrapper that stores the objects.
public struct ObjectDisplay has key {
    id: UID,
    inner: ObjectBag,
}

/// The dynamic field key to use.
public struct PublisherKey() has copy, drop, store;

/// Creates the `ObjectDisplay` instance and stores the publisher in it.
///
/// It does not create any `Display` objects, because Sui 1.84 no longer allows creating Display V1
/// objects (`sui::display::new` aborts). The publisher is kept so that Display V2 objects can be
/// set up later.
public(package) fun create(p: Publisher, ctx: &mut TxContext) {
    let mut inner = object_bag::new(ctx);
    inner.add(PublisherKey(), p);
    transfer::share_object(ObjectDisplay { id: object::new(ctx), inner })
}

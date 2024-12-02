// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

module walrus::commission;

/// Auhorization for the commission receiver. Unlike the base type, it cannot
/// be stored and must be used or ignored in the same transaction.
public struct Auth(Receiver) has drop;

/// Defines the ways to receive commission. It can be either an address - checked
/// with `ctx.sender()`, - or an object - checked with `object::id(..)`.
public enum Receiver has store, copy, drop {
    Address(address),
    ObjectID(ID),
}

/// Creates an authorization for the commission receiver.
public fun auth_as_sender(ctx: &TxContext): Auth {
    Auth(Receiver::Address(ctx.sender()))
}

/// Creates an authorization for the commission receiver.
public fun auth_as_object<T: key>(obj: &T): Auth {
    Auth(Receiver::ObjectID(object::id(obj)))
}

/// Returns the `Receiver` as an address.
public fun receiver_address(addr: address): Receiver {
    Receiver::Address(addr)
}

/// Returns the `Receiver` as an address.
public fun receiver_object(id: ID): Receiver {
    Receiver::ObjectID(id)
}

/// Checks if the authorization matches the receiver.
public(package) fun matches(auth: &Auth, receiver: &Receiver): bool {
    &auth.0 == receiver
}

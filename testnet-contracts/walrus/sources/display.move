// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/// Implements Sui Object Display for user-owned objects.
///
/// The displays are created in the Sui display registry (Display V2). The default fields are:
/// - name
/// - description
/// - image_url
/// - link
/// - project_url
///
/// Optionally:
/// - thumbnail_url
/// - creator
module walrus::display;

use std::type_name;
use sui::{
    display_registry::{Self, DisplayCap, DisplayRegistry},
    object_bag::{Self, ObjectBag},
    package::Publisher
};
use walrus::{blob::Blob, staked_wal::StakedWal, storage_resource::Storage};

/// The wrapper that stores the display caps and the publisher.
public struct ObjectDisplay has key {
    id: UID,
    inner: ObjectBag,
}

/// The dynamic field key to use.
public struct PublisherKey() has copy, drop, store;

/// Creates the shared `Display` objects in the display registry and the `ObjectDisplay` instance
/// that holds their caps and the publisher.
public(package) fun create(mut p: Publisher, registry: &mut DisplayRegistry, ctx: &mut TxContext) {
    let mut inner = object_bag::new(ctx);

    inner.add(type_name::with_defining_ids<Blob>(), init_blob_display(&mut p, registry, ctx));
    inner.add(
        type_name::with_defining_ids<Storage>(),
        init_storage_display(&mut p, registry, ctx),
    );
    inner.add(
        type_name::with_defining_ids<StakedWal>(),
        init_staked_wal_display(&mut p, registry, ctx),
    );
    inner.add(PublisherKey(), p);

    transfer::share_object(ObjectDisplay { id: object::new(ctx), inner })
}

/// Creates and shares the `Display` for the `Blob` type, and returns its cap.
fun init_blob_display(
    p: &mut Publisher,
    registry: &mut DisplayRegistry,
    ctx: &mut TxContext,
): DisplayCap<Blob> {
    let (mut d, cap) = display_registry::new_with_publisher<Blob>(registry, p, ctx);

    d.set(&cap, b"name".to_string(), b"Walrus Blob ({size}b)".to_string());
    d.set(
        &cap,
        b"description".to_string(),
        // `certified_epoch` is `None` until the blob is certified. Display V2 renders the whole
        // field as null when an interpolated value is missing, so fall back to an empty string.
        // editorconfig-checker-disable-next-line
        b"Registered: {registered_epoch}; certified: {certified_epoch | ''}; deletable: {deletable}".to_string(),
    );
    d.set(&cap, b"image_url".to_string(), b"https://www.walrus.xyz/walrus-blob".to_string());
    d.set(&cap, b"project_url".to_string(), b"https://walrus.xyz/".to_string());
    d.set(&cap, b"link".to_string(), b"".to_string());
    d.share();
    cap
}

/// Creates and shares the `Display` for the `Storage` type, and returns its cap.
fun init_storage_display(
    p: &mut Publisher,
    registry: &mut DisplayRegistry,
    ctx: &mut TxContext,
): DisplayCap<Storage> {
    let (mut d, cap) = display_registry::new_with_publisher<Storage>(registry, p, ctx);

    d.set(&cap, b"name".to_string(), b"Walrus Storage Resource ({storage_size}b)".to_string());
    d.set(
        &cap,
        b"description".to_string(),
        b"Start: {start_epoch}; end: {end_epoch}".to_string(),
    );
    d.set(&cap, b"image_url".to_string(), b"https://www.walrus.xyz/walrus-storage".to_string());
    d.set(&cap, b"project_url".to_string(), b"https://walrus.xyz/".to_string());
    d.set(&cap, b"link".to_string(), b"".to_string());
    d.share();
    cap
}

/// Creates and shares the `Display` for the `StakedWal` type, and returns its cap.
fun init_staked_wal_display(
    p: &mut Publisher,
    registry: &mut DisplayRegistry,
    ctx: &mut TxContext,
): DisplayCap<StakedWal> {
    let (mut d, cap) = display_registry::new_with_publisher<StakedWal>(registry, p, ctx);

    // `principal` is a `Balance`, which Display V2 renders through its `value` field.
    d.set(&cap, b"name".to_string(), b"Staked WAL ({principal.value} FROST)".to_string());
    d.set(
        &cap,
        b"description".to_string(),
        b"Staked for node: {node_id}, activates at: {activation_epoch}".to_string(),
    );
    d.set(&cap, b"image_url".to_string(), b"https://www.walrus.xyz/walrus-stakedwal".to_string());
    d.set(&cap, b"project_url".to_string(), b"https://walrus.xyz/".to_string());
    d.set(&cap, b"link".to_string(), b"".to_string());
    d.share();
    cap
}

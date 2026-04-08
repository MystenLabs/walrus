// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

module bucket_object::object_entry;

use bucket_object::object_entry_inner_v1::{Self, ObjectEntryInnerV1};
use bucket_object::object_version::{Self, ObjectVersion};
use std::string::String;
use sui::dynamic_field as df;

const VERSION: u64 = 1;

/// The object entry version does not match the package version.
const EWrongVersion: u64 = 0;
/// The object version belongs to a different bucket object.
const EVersionBucketObjectMismatch: u64 = 1;
/// A pending version is already staged for this entry.
const EPendingVersionAlreadyExists: u64 = 2;
/// No pending version is staged for this entry.
const EPendingVersionMissing: u64 = 3;
/// A newly staged version must advance the object generation by exactly one.
const EGenerationMustAdvance: u64 = 4;

public struct ObjectEntry has key {
    id: UID,
    version: u64,
}

#[test_only]
public fun new_for_testing(
    bucket_object_id: ID,
    key: String,
    current_version: ObjectVersion,
    ctx: &mut TxContext,
): ObjectEntry {
    new_impl(bucket_object_id, key, current_version, ctx)
}

fun new_impl(
    bucket_object_id: ID,
    key: String,
    current_version: ObjectVersion,
    ctx: &mut TxContext,
): ObjectEntry {
    assert!(
        object_version::bucket_object_id(&current_version) == bucket_object_id,
        EVersionBucketObjectMismatch,
    );

    let mut entry = ObjectEntry {
        id: object::new(ctx),
        version: VERSION,
    };
    df::add(
        &mut entry.id,
        VERSION,
        object_entry_inner_v1::new(bucket_object_id, key, current_version),
    );
    entry
}

public fun bucket_object_id(self: &ObjectEntry): ID {
    self.inner().bucket_object_id()
}

public fun key(self: &ObjectEntry): String {
    self.inner().key()
}

public fun generation(self: &ObjectEntry): u64 {
    self.inner().generation()
}

public fun current_version(self: &ObjectEntry): &ObjectVersion {
    self.inner().current_version()
}

public fun has_pending_version(self: &ObjectEntry): bool {
    self.inner().has_pending_version()
}

public fun pending_version(self: &ObjectEntry): &ObjectVersion {
    assert!(self.inner().has_pending_version(), EPendingVersionMissing);
    self.inner().pending_version()
}

public(package) fun stage_pending_version(
    self: &mut ObjectEntry,
    version: ObjectVersion,
) {
    assert!(!self.inner().has_pending_version(), EPendingVersionAlreadyExists);
    assert!(
        object_version::bucket_object_id(&version) == self.inner().bucket_object_id(),
        EVersionBucketObjectMismatch,
    );
    assert!(
        object_version::generation(&version) == self.inner().generation() + 1,
        EGenerationMustAdvance,
    );
    self.inner_mut().stage_pending_version(version);
}

public(package) fun promote_pending_version(self: &mut ObjectEntry) {
    assert!(self.inner().has_pending_version(), EPendingVersionMissing);
    self.inner_mut().promote_pending_version();
}

public(package) fun clear_pending_version(self: &mut ObjectEntry): ObjectVersion {
    assert!(self.inner().has_pending_version(), EPendingVersionMissing);
    self.inner_mut().clear_pending_version()
}

#[test_only]
public fun stage_pending_version_for_testing(
    self: &mut ObjectEntry,
    version: ObjectVersion,
) {
    stage_pending_version(self, version);
}

#[test_only]
public fun promote_pending_version_for_testing(self: &mut ObjectEntry) {
    promote_pending_version(self);
}

#[test_only]
public fun clear_pending_version_for_testing(self: &mut ObjectEntry): ObjectVersion {
    clear_pending_version(self)
}

fun inner(self: &ObjectEntry): &ObjectEntryInnerV1 {
    assert!(self.version == VERSION, EWrongVersion);
    df::borrow(&self.id, self.version)
}

fun inner_mut(self: &mut ObjectEntry): &mut ObjectEntryInnerV1 {
    assert!(self.version == VERSION, EWrongVersion);
    df::borrow_mut(&mut self.id, self.version)
}

#[test_only]
public fun destroy_for_testing(self: ObjectEntry): ObjectEntryInnerV1 {
    let ObjectEntry { mut id, version } = self;
    let inner = df::remove(&mut id, version);
    id.delete();
    inner
}

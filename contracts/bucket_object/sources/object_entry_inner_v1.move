// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

module bucket_object::object_entry_inner_v1;

use bucket_object::object_version::{Self, ObjectVersion};
use std::string::String;

public struct ObjectEntryInnerV1 has store {
    bucket_object_id: ID,
    key: String,
    generation: u64,
    current_version: ObjectVersion,
    pending_version: option::Option<ObjectVersion>,
}

public(package) fun new(
    bucket_object_id: ID,
    key: String,
    current_version: ObjectVersion,
): ObjectEntryInnerV1 {
    ObjectEntryInnerV1 {
        bucket_object_id,
        key,
        generation: object_version::generation(&current_version),
        current_version,
        pending_version: option::none(),
    }
}

public(package) fun bucket_object_id(self: &ObjectEntryInnerV1): ID {
    self.bucket_object_id
}

public(package) fun key(self: &ObjectEntryInnerV1): String {
    self.key
}

public(package) fun generation(self: &ObjectEntryInnerV1): u64 {
    self.generation
}

public(package) fun current_version(self: &ObjectEntryInnerV1): &ObjectVersion {
    &self.current_version
}

public(package) fun has_pending_version(self: &ObjectEntryInnerV1): bool {
    self.pending_version.is_some()
}

public(package) fun pending_version(self: &ObjectEntryInnerV1): &ObjectVersion {
    self.pending_version.borrow()
}

public(package) fun stage_pending_version(
    self: &mut ObjectEntryInnerV1,
    version: ObjectVersion,
) {
    self.pending_version = option::some(version);
}

public(package) fun promote_pending_version(self: &mut ObjectEntryInnerV1) {
    let pending_version = self.pending_version.extract();
    self.generation = object_version::generation(&pending_version);
    self.current_version = pending_version;
}

public(package) fun clear_pending_version(self: &mut ObjectEntryInnerV1): ObjectVersion {
    self.pending_version.extract()
}

#[test_only]
public fun destroy_for_testing(self: ObjectEntryInnerV1) {
    let ObjectEntryInnerV1 {
        bucket_object_id: _,
        key: _,
        generation: _,
        current_version: _,
        pending_version: _,
    } = self;
}

// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

module blob_bucket::object_metadata;

use std::string::String;
use sui::vec_map::{Self as vec_map, VecMap};

public struct ObjectMetadata has copy, drop, store {
    entries: VecMap<String, String>,
}

public fun new(entries: VecMap<String, String>): ObjectMetadata {
    ObjectMetadata { entries }
}

public fun empty(): ObjectMetadata {
    new(vec_map::empty())
}

public fun insert_or_update(self: &mut ObjectMetadata, key: String, value: String) {
    if (self.entries.contains(&key)) {
        self.entries.remove(&key);
    };
    self.entries.insert(key, value);
}

public fun remove(self: &mut ObjectMetadata, key: &String): (String, String) {
    self.entries.remove(key)
}

public fun remove_if_exists(self: &mut ObjectMetadata, key: &String): option::Option<String> {
    if (self.entries.contains(key)) {
        let (_, value) = self.entries.remove(key);
        option::some(value)
    } else {
        option::none()
    }
}

public fun contains(self: &ObjectMetadata, key: &String): bool {
    self.entries.contains(key)
}

public fun try_get(self: &ObjectMetadata, key: &String): option::Option<String> {
    self.entries.try_get(key)
}

public fun length(self: &ObjectMetadata): u64 {
    self.entries.length()
}

public fun is_empty(self: &ObjectMetadata): bool {
    self.entries.is_empty()
}

#[test_only]
public fun new_for_testing(entries: VecMap<String, String>): ObjectMetadata {
    new(entries)
}

#[test_only]
public fun destroy_for_testing(self: ObjectMetadata) {
    let ObjectMetadata { entries } = self;
    entries.destroy_empty();
}

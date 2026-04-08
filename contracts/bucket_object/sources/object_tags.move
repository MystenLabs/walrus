// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

module bucket_object::object_tags;

use std::string::String;
use sui::vec_map::{Self as vec_map, VecMap};

public struct ObjectTags has copy, drop, store {
    entries: VecMap<String, String>,
}

public fun new(entries: VecMap<String, String>): ObjectTags {
    ObjectTags { entries }
}

public fun empty(): ObjectTags {
    new(vec_map::empty())
}

public fun insert_or_update(self: &mut ObjectTags, key: String, value: String) {
    if (self.entries.contains(&key)) {
        self.entries.remove(&key);
    };
    self.entries.insert(key, value);
}

public fun remove(self: &mut ObjectTags, key: &String): (String, String) {
    self.entries.remove(key)
}

public fun remove_if_exists(self: &mut ObjectTags, key: &String): option::Option<String> {
    if (self.entries.contains(key)) {
        let (_, value) = self.entries.remove(key);
        option::some(value)
    } else {
        option::none()
    }
}

public fun contains(self: &ObjectTags, key: &String): bool {
    self.entries.contains(key)
}

public fun try_get(self: &ObjectTags, key: &String): option::Option<String> {
    self.entries.try_get(key)
}

public fun length(self: &ObjectTags): u64 {
    self.entries.length()
}

public fun is_empty(self: &ObjectTags): bool {
    self.entries.is_empty()
}

#[test_only]
public fun new_for_testing(entries: VecMap<String, String>): ObjectTags {
    new(entries)
}

#[test_only]
public fun destroy_for_testing(self: ObjectTags) {
    let ObjectTags { entries } = self;
    entries.destroy_empty();
}

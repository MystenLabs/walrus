// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

module blob_bucket::bucket_object_inner_v1;

use blob_bucket::object_version::{Self, ObjectVersion};
use std::string::String;

public struct BucketObjectInnerV1 has store {
    blob_bucket_id: ID,
    key: String,
    generation: u64,
    current_version: option::Option<ObjectVersion>,
    pending_version: option::Option<ObjectVersion>,
}

public(package) fun new(blob_bucket_id: ID, key: String): BucketObjectInnerV1 {
    BucketObjectInnerV1 {
        blob_bucket_id,
        key,
        generation: 0,
        current_version: option::none(),
        pending_version: option::none(),
    }
}

public(package) fun blob_bucket_id(self: &BucketObjectInnerV1): ID {
    self.blob_bucket_id
}

public(package) fun key(self: &BucketObjectInnerV1): String {
    self.key
}

public(package) fun set_key(self: &mut BucketObjectInnerV1, key: String) {
    self.key = key;
}

public(package) fun generation(self: &BucketObjectInnerV1): u64 {
    self.generation
}

public(package) fun has_current_version(self: &BucketObjectInnerV1): bool {
    self.current_version.is_some()
}

public(package) fun current_version(self: &BucketObjectInnerV1): &ObjectVersion {
    self.current_version.borrow()
}

public(package) fun has_pending_version(self: &BucketObjectInnerV1): bool {
    self.pending_version.is_some()
}

public(package) fun pending_version(self: &BucketObjectInnerV1): &ObjectVersion {
    self.pending_version.borrow()
}

public(package) fun stage_pending_version(
    self: &mut BucketObjectInnerV1,
    version: ObjectVersion,
) {
    self.pending_version = option::some(version);
}

public(package) fun promote_pending_version(self: &mut BucketObjectInnerV1) {
    let pending_version = self.pending_version.extract();
    self.generation = object_version::generation(&pending_version);
    self.current_version = option::some(pending_version);
}

public(package) fun clear_pending_version(self: &mut BucketObjectInnerV1): ObjectVersion {
    self.pending_version.extract()
}

#[test_only]
public fun destroy_for_testing(self: BucketObjectInnerV1) {
    let BucketObjectInnerV1 {
        blob_bucket_id: _,
        key: _,
        generation: _,
        current_version: _,
        pending_version: _,
    } = self;
}

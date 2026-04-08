// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

module bucket_object::bucket_object_inner_v1;

use std::string::String;

public struct BucketObjectInnerV1 has store {
    name: String,
    blob_bucket_id: ID,
    versioning_enabled: bool,
}

public(package) fun new(name: String, blob_bucket_id: ID): BucketObjectInnerV1 {
    BucketObjectInnerV1 {
        name,
        blob_bucket_id,
        versioning_enabled: false,
    }
}

public(package) fun name(self: &BucketObjectInnerV1): &String {
    &self.name
}

public(package) fun blob_bucket_id(self: &BucketObjectInnerV1): ID {
    self.blob_bucket_id
}

public(package) fun versioning_enabled(self: &BucketObjectInnerV1): bool {
    self.versioning_enabled
}

public(package) fun set_versioning_enabled(self: &mut BucketObjectInnerV1, enabled: bool) {
    self.versioning_enabled = enabled;
}

#[test_only]
public fun destroy_for_testing(self: BucketObjectInnerV1) {
    let BucketObjectInnerV1 {
        name: _,
        blob_bucket_id: _,
        versioning_enabled: _,
    } = self;
}

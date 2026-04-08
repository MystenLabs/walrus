// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

module bucket_object::bucket_object_inner_v1;

use std::string::String;

public struct BucketObjectInnerV1 has store {
    blob_bucket_id: ID,
    key: String,
}

public(package) fun new(blob_bucket_id: ID, key: String): BucketObjectInnerV1 {
    BucketObjectInnerV1 {
        blob_bucket_id,
        key,
    }
}

public(package) fun blob_bucket_id(self: &BucketObjectInnerV1): ID {
    self.blob_bucket_id
}

public(package) fun key(self: &BucketObjectInnerV1): String {
    self.key
}

#[test_only]
public fun destroy_for_testing(self: BucketObjectInnerV1) {
    let BucketObjectInnerV1 {
        blob_bucket_id: _,
        key: _,
    } = self;
}

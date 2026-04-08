// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

module bucket_object::object_headers;

use std::string::String;

public struct ObjectHeaders has copy, store, drop {
    content_type: option::Option<String>,
    content_encoding: option::Option<String>,
    content_language: option::Option<String>,
    content_disposition: option::Option<String>,
    cache_control: option::Option<String>,
}

public fun new(
    content_type: option::Option<String>,
    content_encoding: option::Option<String>,
    content_language: option::Option<String>,
    content_disposition: option::Option<String>,
    cache_control: option::Option<String>,
): ObjectHeaders {
    ObjectHeaders {
        content_type,
        content_encoding,
        content_language,
        content_disposition,
        cache_control,
    }
}

public fun empty(): ObjectHeaders {
    new(
        option::none(),
        option::none(),
        option::none(),
        option::none(),
        option::none(),
    )
}

public fun content_type(self: &ObjectHeaders): option::Option<String> {
    self.content_type
}

public fun content_encoding(self: &ObjectHeaders): option::Option<String> {
    self.content_encoding
}

public fun content_language(self: &ObjectHeaders): option::Option<String> {
    self.content_language
}

public fun content_disposition(self: &ObjectHeaders): option::Option<String> {
    self.content_disposition
}

public fun cache_control(self: &ObjectHeaders): option::Option<String> {
    self.cache_control
}

#[test_only]
public fun new_for_testing(
    content_type: option::Option<String>,
    content_encoding: option::Option<String>,
    content_language: option::Option<String>,
    content_disposition: option::Option<String>,
    cache_control: option::Option<String>,
): ObjectHeaders {
    new(
        content_type,
        content_encoding,
        content_language,
        content_disposition,
        cache_control,
    )
}

#[test_only]
public fun destroy_for_testing(self: ObjectHeaders) {
    let ObjectHeaders {
        content_type: _,
        content_encoding: _,
        content_language: _,
        content_disposition: _,
        cache_control: _,
    } = self;
}

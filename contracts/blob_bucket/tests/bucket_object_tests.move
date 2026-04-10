// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

#[test_only]
module blob_bucket::bucket_object_tests;

use blob_bucket::{
    bucket_object,
    object_headers,
    object_metadata,
    object_tags,
    object_version,
};
use std::unit_test::assert_eq;
use sui::vec_map;

#[test]
fun stage_and_promote_pending_version() {
    let mut object_entry = bucket_object::empty_for_testing();
    let pending_version = object_version::new_for_testing(
        1,
        0x111,
        new_id_for_testing(),
        768,
        html_headers(),
        html_metadata(),
        html_tags(),
        b"content-etag-v1".to_string(),
        b"object-etag-v1".to_string(),
        false,
    );

    bucket_object::stage_pending_version_for_testing(&mut object_entry, pending_version);
    assert!(bucket_object::has_pending_version(&object_entry));
    assert_eq!(
        object_version::generation(bucket_object::pending_version(&object_entry)),
        1,
    );
    assert_eq!(
        object_version::object_etag(bucket_object::pending_version(&object_entry)),
        b"object-etag-v1".to_string(),
    );

    bucket_object::promote_pending_version_for_testing(&mut object_entry);

    assert_eq!(bucket_object::generation(&object_entry), 1);
    assert!(bucket_object::has_current_version(&object_entry));
    assert!(!bucket_object::has_pending_version(&object_entry));
    assert_eq!(
        object_version::content_etag(bucket_object::current_version(&object_entry)),
        b"content-etag-v1".to_string(),
    );
    assert_eq!(
        object_headers::content_type(
            &object_version::headers(bucket_object::current_version(&object_entry)),
        ),
        option::some(b"text/html".to_string()),
    );
}

#[test]
fun put_object_if_absent_allows_recreate_after_delete_marker() {
    let mut object_entry = bucket_object::empty_for_testing();

    bucket_object::delete_object(&mut object_entry, b"delete-marker-v1".to_string());
    assert!(bucket_object::is_deleted(&object_entry));
    assert_eq!(bucket_object::generation(&object_entry), 1);

    bucket_object::put_object_if_absent(
        &mut object_entry,
        0x222,
        new_id_for_testing(),
        1024,
        html_headers(),
        html_metadata(),
        html_tags(),
        b"content-etag-v2".to_string(),
        b"object-etag-v2".to_string(),
    );
    bucket_object::promote_pending_version_for_testing(&mut object_entry);

    assert_eq!(bucket_object::generation(&object_entry), 2);
    assert!(!bucket_object::is_deleted(&object_entry));
    assert_eq!(
        object_version::blob_id(bucket_object::current_version(&object_entry)),
        0x222,
    );
    assert_eq!(
        object_version::object_etag(bucket_object::current_version(&object_entry)),
        b"object-etag-v2".to_string(),
    );
}

#[test]
fun copy_current_version_to_allows_deleted_destination() {
    let mut source = bucket_object::empty_for_testing();
    bucket_object::put_object_if_absent(
        &mut source,
        0x333,
        new_id_for_testing(),
        2048,
        html_headers(),
        html_metadata(),
        html_tags(),
        b"content-etag-v1".to_string(),
        b"object-etag-source".to_string(),
    );
    bucket_object::promote_pending_version_for_testing(&mut source);

    let mut destination = bucket_object::empty_for_testing();
    bucket_object::delete_object(&mut destination, b"delete-marker-v1".to_string());

    bucket_object::copy_current_version_to(
        &source,
        &mut destination,
        b"object-etag-copy".to_string(),
    );

    assert_eq!(bucket_object::generation(&destination), 2);
    assert!(!bucket_object::is_deleted(&destination));
    assert_eq!(
        object_version::blob_id(bucket_object::current_version(&destination)),
        object_version::blob_id(bucket_object::current_version(&source)),
    );
    assert_eq!(
        object_version::pooled_blob_object_id(bucket_object::current_version(&destination)),
        object_version::pooled_blob_object_id(bucket_object::current_version(&source)),
    );
    assert_eq!(
        object_version::content_etag(bucket_object::current_version(&destination)),
        object_version::content_etag(bucket_object::current_version(&source)),
    );
    assert_eq!(
        object_version::object_etag(bucket_object::current_version(&destination)),
        b"object-etag-copy".to_string(),
    );
}

#[test, expected_failure(abort_code = bucket_object::EPendingVersionNotCertified)]
fun finalize_requires_certified_live_blob() {
    let mut object_entry = bucket_object::empty_for_testing();
    bucket_object::put_object_if_absent(
        &mut object_entry,
        0x444,
        new_id_for_testing(),
        512,
        empty_headers(),
        empty_metadata(),
        empty_tags(),
        b"content-etag-v1".to_string(),
        b"object-etag-v1".to_string(),
    );

    bucket_object::finalize_pending_version_if_certified_for_testing(&mut object_entry, false);

    abort
}

fun new_id_for_testing(): ID {
    object::id_from_address(@0xBEEF)
}

fun empty_headers(): object_headers::ObjectHeaders {
    object_headers::empty()
}

fun empty_metadata(): object_metadata::ObjectMetadata {
    object_metadata::empty()
}

fun empty_tags(): object_tags::ObjectTags {
    object_tags::empty()
}

fun html_headers(): object_headers::ObjectHeaders {
    object_headers::new_for_testing(
        option::some(b"text/html".to_string()),
        option::none(),
        option::some(b"en-US".to_string()),
        option::none(),
        option::some(b"public, max-age=60".to_string()),
    )
}

fun html_metadata(): object_metadata::ObjectMetadata {
    let mut entries = vec_map::empty();
    entries.insert(b"site".to_string(), b"marketing".to_string());
    object_metadata::new_for_testing(entries)
}

fun html_tags(): object_tags::ObjectTags {
    let mut entries = vec_map::empty();
    entries.insert(b"env".to_string(), b"prod".to_string());
    object_tags::new_for_testing(entries)
}

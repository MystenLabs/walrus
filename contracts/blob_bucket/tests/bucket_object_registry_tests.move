// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

#[test_only]
module blob_bucket::bucket_object_registry_tests;

use blob_bucket::{blob_bucket, blob_bucket_inner_v1};
use blob_bucket::{
    bucket_object,
    bucket_object_inner_v1,
    bucket_object_registry,
    object_headers,
    object_metadata,
    object_tags,
    object_version,
};
use std::unit_test::assert_eq;
use wal::wal::WAL;
use sui::coin::Coin;
use walrus::{
    blob,
    encoding,
    messages,
    storage_pool,
    system,
    test_utils::{Self, bls_min_pk_sign, signers_to_bitmap},
};

const RS2: u8 = 1;
const ROOT_HASH: u256 = 0xABC;
const SIZE: u64 = 5_000_000;
const EPOCH: u32 = 0;
const N_COINS: u64 = 1_000_000_000;
const WRITE_PAYMENT: u64 = 100_000_000_000;

#[test]
fun resolve_or_create_returns_existing_bucket_object_id() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let encoded_size = encoding::encoded_blob_length(SIZE, RS2, system.n_shards());
    let mut pool_payment = test_utils::mint_frost(N_COINS, ctx);
    let (blob_bucket, blob_bucket_cap) = blob_bucket::new_for_testing(
        &mut system,
        encoded_size,
        3,
        &mut pool_payment,
        ctx,
    );
    let mut registry = bucket_object_registry::new_for_testing(object::id(&blob_bucket), ctx);
    let bucket_object_ref = bucket_object_registry::create_bucket_object_for_testing(
        &mut registry,
        b"index.html".to_string(),
        ctx,
    );
    let existing_id = object::id(&bucket_object_ref);

    let resolved_id = bucket_object_registry::resolve_or_create_bucket_object(
        &mut registry,
        b"index.html".to_string(),
        ctx,
    );

    assert_eq!(resolved_id, existing_id);
    assert_eq!(
        bucket_object_registry::resolve(&registry, &b"index.html".to_string()),
        option::some(existing_id),
    );

    destroy_registry_fixture(
        registry,
        vector[bucket_object_ref],
        blob_bucket,
        blob_bucket_cap,
        pool_payment,
        system,
    );
}

#[test]
fun rename_object_if_match_updates_registry_lookup() {
    let sk = test_utils::bls_sk_for_testing();
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let encoded_size = encoding::encoded_blob_length(SIZE, RS2, system.n_shards());
    let mut pool_payment = test_utils::mint_frost(N_COINS, ctx);
    let (mut blob_bucket, blob_bucket_cap) = blob_bucket::new_for_testing(
        &mut system,
        encoded_size,
        3,
        &mut pool_payment,
        ctx,
    );
    let mut registry = bucket_object_registry::new_for_testing(object::id(&blob_bucket), ctx);
    let mut bucket_object_ref = bucket_object_registry::create_bucket_object_for_testing(
        &mut registry,
        b"index.html".to_string(),
        ctx,
    );
    register_and_finalize_initial_object_version(
        &mut bucket_object_ref,
        &mut blob_bucket,
        &blob_bucket_cap,
        &mut system,
        &sk,
        ctx,
    );
    let bucket_object_id = object::id(&bucket_object_ref);

    bucket_object_registry::rename_object_if_match(
        &mut registry,
        &mut bucket_object_ref,
        b"object-etag-v1".to_string(),
        b"home.html".to_string(),
    );

    assert_eq!(bucket_object::key(&bucket_object_ref), b"home.html".to_string());
    assert_eq!(
        bucket_object_registry::resolve(&registry, &b"index.html".to_string()),
        option::none(),
    );
    assert_eq!(
        bucket_object_registry::resolve(&registry, &b"home.html".to_string()),
        option::some(bucket_object_id),
    );

    destroy_registry_fixture(
        registry,
        vector[bucket_object_ref],
        blob_bucket,
        blob_bucket_cap,
        pool_payment,
        system,
    );
}

#[test]
fun copy_object_if_absent_reuses_live_payload_and_attributes() {
    let sk = test_utils::bls_sk_for_testing();
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let encoded_size = encoding::encoded_blob_length(SIZE, RS2, system.n_shards());
    let mut pool_payment = test_utils::mint_frost(N_COINS, ctx);
    let (mut blob_bucket, blob_bucket_cap) = blob_bucket::new_for_testing(
        &mut system,
        encoded_size,
        3,
        &mut pool_payment,
        ctx,
    );
    let mut registry = bucket_object_registry::new_for_testing(object::id(&blob_bucket), ctx);
    let mut source = bucket_object_registry::create_bucket_object_for_testing(
        &mut registry,
        b"index.html".to_string(),
        ctx,
    );
    register_and_finalize_initial_object_version(
        &mut source,
        &mut blob_bucket,
        &blob_bucket_cap,
        &mut system,
        &sk,
        ctx,
    );
    let source_blob_id = object_version::blob_id(bucket_object::current_version(&source));
    let source_pooled_blob_object_id = object_version::pooled_blob_object_id(
        bucket_object::current_version(&source),
    );
    let source_content_etag = object_version::content_etag(bucket_object::current_version(&source));

    let copied = bucket_object_registry::copy_object_if_absent_for_testing(
        &mut registry,
        &source,
        b"app.js".to_string(),
        b"object-etag-copy".to_string(),
        ctx,
    );
    let copied_id = object::id(&copied);

    assert_eq!(bucket_object::key(&copied), b"app.js".to_string());
    assert_eq!(bucket_object::generation(&copied), 1);
    assert_eq!(object_version::blob_id(bucket_object::current_version(&copied)), source_blob_id);
    assert_eq!(
        object_version::pooled_blob_object_id(bucket_object::current_version(&copied)),
        source_pooled_blob_object_id,
    );
    assert_eq!(
        object_version::content_etag(bucket_object::current_version(&copied)),
        source_content_etag,
    );
    assert_eq!(
        object_version::object_etag(bucket_object::current_version(&copied)),
        b"object-etag-copy".to_string(),
    );
    assert_eq!(
        object_metadata::try_get(
            &object_version::metadata(bucket_object::current_version(&copied)),
            &b"site".to_string(),
        ),
        option::some(b"marketing".to_string()),
    );
    assert_eq!(
        object_tags::try_get(
            &object_version::tags(bucket_object::current_version(&copied)),
            &b"env".to_string(),
        ),
        option::some(b"prod".to_string()),
    );
    assert_eq!(
        bucket_object_registry::resolve(&registry, &b"app.js".to_string()),
        option::some(copied_id),
    );

    destroy_registry_fixture(
        registry,
        vector[source, copied],
        blob_bucket,
        blob_bucket_cap,
        pool_payment,
        system,
    );
}

fun register_and_finalize_initial_object_version(
    bucket_object_ref: &mut bucket_object::BucketObject,
    blob_bucket: &mut blob_bucket::BlobBucket,
    blob_bucket_cap: &blob_bucket::BlobBucketCap,
    system: &mut system::System,
    sk: &vector<u8>,
    ctx: &mut TxContext,
) {
    let mut write_payment = test_utils::mint_frost(WRITE_PAYMENT, ctx);
    bucket_object::put_object_if_absent_and_register(
        bucket_object_ref,
        blob_bucket,
        blob_bucket_cap,
        system,
        ROOT_HASH,
        SIZE,
        RS2,
        true,
        &mut write_payment,
        html_headers(),
        html_metadata(),
        html_tags(),
        b"content-etag-v1".to_string(),
        b"object-etag-v1".to_string(),
        ctx,
    );
    write_payment.burn_for_testing();

    let blob_id = blob::derive_blob_id(ROOT_HASH, RS2, SIZE);
    certify_blob_in_bucket(blob_bucket, system, blob_id, sk);
    bucket_object::finalize_pending_version_if_certified_for_testing(bucket_object_ref, blob_bucket);
}

fun certify_blob_in_bucket(
    blob_bucket: &mut blob_bucket::BlobBucket,
    system: &system::System,
    blob_id: u256,
    sk: &vector<u8>,
) {
    let object_id = blob_bucket::get_blob_object_id(blob_bucket, blob_id);
    let confirmation_message = messages::certified_deletable_message_bytes(EPOCH, blob_id, object_id);
    let signature = bls_min_pk_sign(&confirmation_message, sk);
    blob_bucket::certify_blob(
        blob_bucket,
        system,
        blob_id,
        signature,
        signers_to_bitmap(&vector[0]),
        confirmation_message,
    );
}

fun destroy_registry_fixture(
    registry: bucket_object_registry::BucketObjectRegistry,
    bucket_objects: vector<bucket_object::BucketObject>,
    blob_bucket: blob_bucket::BlobBucket,
    blob_bucket_cap: blob_bucket::BlobBucketCap,
    pool_payment: Coin<WAL>,
    system: system::System,
) {
    let mut bucket_objects = bucket_objects;
    while (!bucket_objects.is_empty()) {
        let bucket_object_ref = bucket_objects.pop_back();
        let inner = bucket_object::destroy_for_testing(bucket_object_ref);
        bucket_object_inner_v1::destroy_for_testing(inner);
    };
    bucket_objects.destroy_empty();
    bucket_object_registry::destroy_for_testing(registry);

    let blob_bucket_inner = blob_bucket::destroy_for_testing(blob_bucket);
    let pool = blob_bucket_inner_v1::destroy_for_testing(blob_bucket_inner);
    blob_bucket::destroy_cap_for_testing(blob_bucket_cap);
    storage_pool::destroy_for_testing(pool);
    pool_payment.burn_for_testing();
    system.destroy_for_testing();
}

fun html_headers(): object_headers::ObjectHeaders {
    object_headers::new_for_testing(
        option::some(b"text/html".to_string()),
        option::none(),
        option::some(b"en".to_string()),
        option::none(),
        option::some(b"public, max-age=60".to_string()),
    )
}

fun html_metadata(): object_metadata::ObjectMetadata {
    let mut metadata = object_metadata::empty();
    object_metadata::insert_or_update(&mut metadata, b"site".to_string(), b"marketing".to_string());
    object_metadata::insert_or_update(&mut metadata, b"owner".to_string(), b"walrus".to_string());
    metadata
}

fun html_tags(): object_tags::ObjectTags {
    let mut tags = object_tags::empty();
    object_tags::insert_or_update(&mut tags, b"env".to_string(), b"prod".to_string());
    object_tags::insert_or_update(&mut tags, b"surface".to_string(), b"web".to_string());
    tags
}

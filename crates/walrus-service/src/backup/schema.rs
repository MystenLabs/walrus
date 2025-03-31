// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

// @generated automatically by Diesel CLI.

diesel::table! {
    blob_state (blob_id) {
        blob_id -> Bytea,
        end_epoch -> Int8,
        state -> Text,
        backup_url -> Nullable<Text>,
        orchestrator_version -> Text,
        fetcher_version -> Nullable<Text>,
        created_at -> Timestamptz,
        initiate_fetch_after -> Nullable<Timestamptz>,
        retry_count -> Nullable<Int4>,
        last_error -> Nullable<Text>,
        initiate_gc_after -> Nullable<Timestamptz>,
        size -> Nullable<Int8>,
        sha256 -> Nullable<Bytea>,
        md5 -> Nullable<Bytea>,
    }
}

diesel::table! {
    epoch_change_start_event (epoch) {
        epoch -> Int8,
        created_at -> Timestamptz,
    }
}

diesel::table! {
    stream_event (element_index) {
        checkpoint_sequence_number -> Int8,
        counter -> Int8,
        transaction_digest -> Bytea,
        event_index -> Int8,
        element_index -> Int8,
        element -> Jsonb,
    }
}

diesel::allow_tables_to_appear_in_same_query!(blob_state, epoch_change_start_event, stream_event,);

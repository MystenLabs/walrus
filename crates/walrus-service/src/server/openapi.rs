// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use utoipa::openapi::{schema::Schema, ObjectBuilder, RefOr};

use super::routes::{self, BlobIdString};

pub(super) const GROUP_STORING_BLOBS: &str = "Writing Blobs";
pub(super) const GROUP_READING_BLOBS: &str = "Reading Blobs";
pub(super) const GROUP_RECOVERY: &str = "Recovery";

#[derive(utoipa::OpenApi)]
#[openapi(
    paths(
        routes::get_metadata,
        routes::put_metadata,
        routes::get_sliver,
        routes::put_sliver,
        routes::get_storage_confirmation,
        routes::get_recovery_symbol,
        routes::inconsistency_proof
    ),
    components(schemas(BlobIdString, SliverTypeSchema, SliverPairIndexSchema,))
)]
pub(super) struct RestApiDoc;

/// Index identifying to which of the encoded slivers *pairs* constituting the encoding of the blob
/// the request corresponds.
#[derive(utoipa::ToSchema)]
#[schema(
    as = SliverPairIndex,
    value_type = u16,
    example = json!(17),
    format = "uint16",
)]
struct SliverPairIndexSchema(());

struct SliverTypeSchema;

impl<'s> utoipa::ToSchema<'s> for SliverTypeSchema {
    fn schema() -> (&'s str, RefOr<Schema>) {
        let schema = ObjectBuilder::new()
            .enum_values(Some(vec!["primary", "secondary"]))
            .description(Some(
                "Value identifying either a primary or secondary blob sliver.",
            ))
            .into();

        ("SliverType", schema)
    }
}

/// Convert the path with variables of the form `:id` to the form `{id}`.
pub(crate) fn rewrite_route(path: &str) -> String {
    regex::Regex::new(r":(?<param>\w+)")
        .unwrap()
        .replace_all(path, "{$param}")
        .as_ref()
        .into()
}

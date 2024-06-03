// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use utoipa::{
    openapi::{schema::Schema, RefOr},
    PartialSchema,
    ToSchema,
};
use walrus_core::messages::SignedMessage;
use walrus_sdk::api::BlobStatus;

use super::{
    responses::RestApiJsonError,
    routes::{self, BlobIdString},
};
use crate::server::responses::ApiSuccess;

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
        routes::inconsistency_proof,
        routes::get_blob_status
    ),
    components(schemas(
        BlobIdString,
        SliverTypeSchema,
        SliverPairIndexSchema,
        SignedMessageSchema,
        RestApiJsonError,
        ApiSuccessSignedMessage,
        ApiSuccessMessage,
        ApiSuccessBlobStatus,
        EventIdSchema,
        ApiSuccessStorageConfirmation
    ),)
)]
pub(super) struct RestApiDoc;

/// Index identifying one of the blob's sliver pairs. As blobs are encoded into as many pairs of
/// slivers as there are shards in the committee, this value must be from 0 to the number of shards
/// (exclusive).
#[derive(utoipa::ToSchema)]
#[schema(
    as = SliverPairIndex,
    value_type = u16,
)]
struct SliverPairIndexSchema(());

/// Value identifying either a primary or secondary blob sliver.
#[allow(unused)]
#[derive(utoipa::ToSchema)]
#[schema(as = SliverType, rename_all = "SCREAMING_SNAKE_CASE")]
enum SliverTypeSchema {
    Primary,
    Secondary,
}

/// A base64 encoded protocol message and signature.
#[allow(unused)]
#[derive(utoipa::ToSchema)]
#[schema(as = SignedMessage, rename_all = "camelCase")]
pub(super) struct SignedMessageSchema {
    /// The base64 encoded message.
    #[schema(format = Byte)]
    serialized_message: Vec<u8>,
    /// The base64 encoded signature of this storage node.
    #[schema(format = Byte)]
    signature: Vec<u8>,
}

/// A confirmation of storage, provided as a signature over a signed message.
#[allow(unused)]
#[derive(utoipa::ToSchema)]
#[schema(as = StorageConfirmation, rename_all = "camelCase")]
pub(super) enum StorageConfirmationSchema {
    Signed(SignedMessage<()>),
}

#[allow(unused)]
#[derive(ToSchema)]
#[schema(as = EventID, rename_all = "camelCase")]
struct EventIdSchema {
    #[schema(format = Byte)]
    tx_digest: Vec<u8>,
    // u64 represented as a string
    #[schema(value_type = String)]
    event_seq: u64,
}

macro_rules! api_success_alias {
    (@schema (PartialSchema) $name:ident) => {
        Self::schema_with_data($name::schema())
    };
    (@schema (ToSchema) $name:ident) => {
        <Self as PartialSchema>::schema()
    };
    ($($name:ident as $alias:ident $method:tt),+ $(,)?) => {
        $(
            pub(super) type $alias = ApiSuccess<$name>;

            impl<'r> ToSchema<'r> for $alias {
                fn schema() -> (&'r str, RefOr<Schema>) {
                    (stringify!($alias), api_success_alias!(@schema $method $name))
                }
            }
        )*
    };
}

api_success_alias! {
    StorageConfirmationSchema as ApiSuccessStorageConfirmation (ToSchema),
    SignedMessageSchema as ApiSuccessSignedMessage (ToSchema),
    BlobStatus as ApiSuccessBlobStatus (ToSchema),
    String as ApiSuccessMessage (PartialSchema),
}

/// Convert the path with variables of the form `:id` to the form `{id}`.
pub(crate) fn rewrite_route(path: &str) -> String {
    regex::Regex::new(r":(?<param>\w+)")
        .unwrap()
        .replace_all(path, "{$param}")
        .as_ref()
        .into()
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use utoipa::OpenApi as _;
    use utoipa_redoc::Redoc;

    use super::*;

    #[test]
    fn test_openapi_generation_does_not_panic() {
        std::fs::write(
            // Can also be used to view the api.
            Path::new("/tmp/api.html"),
            Redoc::new(RestApiDoc::openapi()).to_html().as_bytes(),
        )
        .unwrap();
    }
}

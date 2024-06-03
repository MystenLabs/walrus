// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use utoipa::{
    openapi::{schema::Schema, RefOr},
    PartialSchema,
    ToSchema,
};
use walrus_core::{
    messages::{SignedMessage, StorageConfirmation},
    SliverPairIndex,
    SliverType,
};
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
        SliverType,
        SliverPairIndex,
        SignedMessage::<()>,
        RestApiJsonError,
        ApiSuccessSignedMessage,
        ApiSuccessMessage,
        ApiSuccessBlobStatus,
        EventIdSchema,
        ApiSuccessStorageConfirmation
    ),)
)]
pub(super) struct RestApiDoc;

// Schema for EventID type from sui_types.
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

macro_rules! api_success_alias_schema {
    (PartialSchema $name:ident) => {
        Self::schema_with_data($name::schema())
    };
    (ToSchema $name:ident) => {
        <Self as PartialSchema>::schema()
    };
}

// Creates `ToSchema` API implementations and type aliases for `ApiSuccess<T>`.
// This is required as utoipa's current method for handling generics in schemas is not
// working for enums. See https://github.com/juhaku/utoipa/issues/835.
macro_rules! api_success_alias {
    ($name:ident as $alias:ident, $method:tt) => {
        pub(super) type $alias = ApiSuccess<$name>;

        impl<'r> ToSchema<'r> for $alias {
            fn schema() -> (&'r str, RefOr<Schema>) {
                (stringify!($alias), api_success_alias_schema!($method $name))
            }
        }
    };
}

type UntypedSignedMessage = SignedMessage<()>;

api_success_alias!(
    StorageConfirmation as ApiSuccessStorageConfirmation,
    ToSchema
);
api_success_alias!(UntypedSignedMessage as ApiSuccessSignedMessage, ToSchema);
api_success_alias!(BlobStatus as ApiSuccessBlobStatus, ToSchema);
api_success_alias!(String as ApiSuccessMessage, PartialSchema);

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

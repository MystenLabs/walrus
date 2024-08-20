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
use walrus_sdk::api::{BlobStatus, ServiceHealthInfo, SliverStatus};

use super::routes;
use crate::common::api::{api_success_alias, BlobIdString, EventIdSchema, RestApiJsonError};

pub(super) const GROUP_STORING_BLOBS: &str = "Writing Blobs";
pub(super) const GROUP_READING_BLOBS: &str = "Reading Blobs";
pub(super) const GROUP_RECOVERY: &str = "Recovery";
pub(super) const GROUP_STATUS: &str = "Status";
pub(super) const GROUP_SYNC_SHARD: &str = "Sync Shard";

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
        routes::get_blob_status,
        routes::health_info,
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
        ApiSuccessSliverStatus,
        EventIdSchema,
        ApiSuccessStorageConfirmation,
        ApiSuccessServiceHealthInfo,
    ))
)]
pub(super) struct RestApiDoc;

type UntypedSignedMessage = SignedMessage<()>;

api_success_alias!(
    StorageConfirmation as ApiSuccessStorageConfirmation,
    ToSchema
);
api_success_alias!(UntypedSignedMessage as ApiSuccessSignedMessage, ToSchema);
api_success_alias!(BlobStatus as ApiSuccessBlobStatus, ToSchema);
api_success_alias!(SliverStatus as ApiSuccessSliverStatus, ToSchema);
api_success_alias!(String as ApiSuccessMessage, PartialSchema);
api_success_alias!(ServiceHealthInfo as ApiSuccessServiceHealthInfo, ToSchema);

#[cfg(test)]
mod tests {
    use std::path::Path;

    use utoipa::OpenApi as _;
    use utoipa_redoc::Redoc;

    use super::*;

    #[test]
    fn test_openapi_generation_does_not_panic() {
        std::fs::write(
            // Can also be used to view the API.
            Path::new("/tmp/api-node.html"),
            Redoc::new(RestApiDoc::openapi()).to_html().as_bytes(),
        )
        .unwrap();
    }
}

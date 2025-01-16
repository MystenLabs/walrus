// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use utoipa::{
    openapi::{schema::Schema, RefOr},
    PartialSchema,
    ToSchema,
};
use walrus_core::{
    messages::{SignedMessage, StorageConfirmation},
    EpochSchema,
    SliverPairIndex,
    SliverType,
};
use walrus_sdk::api::{
    errors::Status,
    BlobStatus,
    ServiceHealthInfo,
    ShardHealthInfo,
    ShardStatus,
    ShardStatusDetail,
    ShardStatusSummary,
    StoredOnNodeStatus,
};

use super::routes;
use crate::{
    api_success_alias,
    common::api::{BlobIdString, EventIdSchema, ObjectIdSchema},
};

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
        routes::get_permanent_blob_confirmation,
        routes::get_deletable_blob_confirmation,
        routes::get_recovery_symbol,
        routes::inconsistency_proof,
        routes::get_blob_status,
        routes::health_info,
    ),
    components(schemas(
        ApiSuccessBlobStatus,
        ApiSuccessMessage,
        ApiSuccessServiceHealthInfo,
        ApiSuccessSignedMessage,
        ApiSuccessStorageConfirmation,
        ApiSuccessStoredOnNodeStatus,
        BlobIdString,
        EpochSchema,
        EventIdSchema,
        ObjectIdSchema,
        ServiceHealthInfo,
        ShardHealthInfo,
        ShardStatus,
        ShardStatusDetail,
        ShardStatusSummary,
        SignedMessage::<()>,
        SliverPairIndex,
        SliverType,
        Status,
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
api_success_alias!(StoredOnNodeStatus as ApiSuccessStoredOnNodeStatus, ToSchema);
api_success_alias!(String as ApiSuccessMessage, PartialSchema);
api_success_alias!(ServiceHealthInfo as ApiSuccessServiceHealthInfo, ToSchema);

#[cfg(test)]
mod tests {
    use utoipa::OpenApi as _;
    use utoipa_redoc::Redoc;

    use super::*;

    /// Serializes the storage node's open-api spec when this test is run.
    ///
    /// This test ensures that the files `storage_openapi.yaml` and `storage_openapi.html` are
    /// kept in sync with changes to the spec.
    #[test]
    fn check_and_update_openapi_spec() -> walrus_test_utils::Result {
        const NODE_OPENAPI_SPEC_PATH: &str = "storage_openapi.yaml";
        const NODE_OPENAPI_HTML_PATH: &str = "storage_openapi.html";

        let spec = RestApiDoc::openapi();

        std::fs::write(NODE_OPENAPI_HTML_PATH, Redoc::new(spec.clone()).to_html())?;
        std::fs::write(NODE_OPENAPI_SPEC_PATH, spec.clone().to_yaml()?)?;

        Ok(())
    }
}

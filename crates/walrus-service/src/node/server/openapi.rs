// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use walrus_core::{EpochSchema, SliverPairIndex, SliverType, SymbolId, messages::SignedMessage};
use walrus_storage_node_client::api::{
    ServiceHealthInfo, ShardHealthInfo, ShardStatus, ShardStatusDetail, ShardStatusSummary,
    errors::Status,
};
use walrus_sui::{EventIdSchema, ObjectIdSchema};

use super::routes;

pub(super) const GROUP_STORING_BLOBS: &str = "Writing Blobs";
pub(super) const GROUP_READING_BLOBS: &str = "Reading Blobs";
pub(super) const GROUP_RECOVERY: &str = "Recovery";
pub(super) const GROUP_STATUS: &str = "Status";
#[allow(dead_code)] // False positive; this is actually used in the generated OpenAPI spec.
pub(super) const GROUP_SYNC_SHARD: &str = "Sync Shard";

#[derive(utoipa::OpenApi)]
#[openapi(
    paths(
        routes::get_blob_status,
        routes::get_deletable_blob_confirmation,
        routes::get_managed_blob_confirmation,
        routes::get_metadata,
        routes::get_permanent_blob_confirmation,
        routes::get_sliver,
        routes::health_info,
        routes::inconsistency_proof,
        routes::list_recovery_symbols,
        routes::put_metadata,
        routes::put_sliver,
    ),
    components(schemas(
        EpochSchema,
        EventIdSchema,
        ObjectIdSchema,
        ServiceHealthInfo,
        ShardHealthInfo,
        ShardStatus,
        ShardStatusDetail,
        ShardStatusSummary,
        SignedMessage::<u8>,
        SliverPairIndex,
        SliverType,
        Status,
        SymbolId,
    )),
)]
pub(super) struct RestApiDoc;

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

        let mut spec = RestApiDoc::openapi();
        spec.info.version = "<VERSION>".to_string();

        std::fs::write(NODE_OPENAPI_HTML_PATH, Redoc::new(spec.clone()).to_html())?;

        walrus_test_utils::overwrite_file_and_fail_if_not_equal(
            NODE_OPENAPI_SPEC_PATH,
            spec.to_yaml()?,
        )?;

        Ok(())
    }
}

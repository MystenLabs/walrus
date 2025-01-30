// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use utoipa::OpenApi;
use walrus_core::{BlobId, EncodingType, EpochSchema};
use walrus_sdk::api::errors::Status;
use walrus_sui::{
    types::{Blob, StorageResource},
    EventIdSchema,
    ObjectIdSchema,
    SuiAddressSchema,
};

use super::routes;
use crate::{
    client::{resource::RegisterBlobOp, responses::EventOrObjectId, BlobStoreResult},
    common::api::Binary,
};

#[derive(OpenApi)]
#[openapi(
    info(title = "Walrus Aggregator"),
    paths(routes::get_blob),
    components(schemas(BlobId, Status,))
)]
pub(super) struct AggregatorApiDoc;

#[derive(OpenApi)]
#[openapi(
    info(title = "Walrus Publisher"),
    paths(routes::put_blob),
    components(schemas(
        Blob,
        BlobId,
        BlobStoreResult,
        EncodingType,
        EpochSchema,
        EventIdSchema,
        EventOrObjectId,
        ObjectIdSchema,
        RegisterBlobOp,
        Status,
        StorageResource,
        SuiAddressSchema,
        Binary,
    ))
)]
pub(super) struct PublisherApiDoc;

#[derive(OpenApi)]
#[openapi(
    info(title = "Walrus Daemon"),
    paths(routes::get_blob, routes::put_blob),
    components(schemas(
        Blob,
        BlobId,
        BlobStoreResult,
        EncodingType,
        EpochSchema,
        EventIdSchema,
        EventOrObjectId,
        ObjectIdSchema,
        RegisterBlobOp,
        Status,
        StorageResource,
        SuiAddressSchema,
        Binary,
    ))
)]
pub(super) struct DaemonApiDoc;

#[cfg(test)]
mod tests {
    use utoipa::OpenApi as _;
    use utoipa_redoc::Redoc;
    use walrus_test_utils::{param_test, Result as TestResult};

    use super::*;

    #[test]
    fn test_openapi_generation_does_not_panic() {
        std::fs::write(
            // Can also be used to view the API.
            std::env::temp_dir().join("api-daemon.html"),
            Redoc::new(DaemonApiDoc::openapi()).to_html().as_bytes(),
        )
        .unwrap();
    }

    param_test! {
        check_and_update_openapi_spec -> TestResult: [
            publisher: (PublisherApiDoc, "publisher"),
            aggregator: (AggregatorApiDoc, "aggregator"),
            daemon: (DaemonApiDoc, "daemon"),
        ]
    }
    /// Serializes the publisher's, aggregator's, and daemon's open-api spec when this test is run.
    ///
    /// This test ensures that the files `{publisher|aggregator|daemon}_openapi.yaml` and
    /// `{publisher|aggregator|daemon}_openapi.html` are kept in sync with changes to the spec.
    fn check_and_update_openapi_spec<T: OpenApi>(_spec_type: T, label: &str) -> TestResult {
        let spec_path = format!("{label}_openapi.yaml");
        let html_path = format!("{label}_openapi.html");

        let mut spec = T::openapi();
        spec.info.version = "<VERSION>".to_string();

        std::fs::write(html_path, Redoc::new(spec.clone()).to_html())?;

        walrus_test_utils::overwrite_file_and_fail_if_not_equal(spec_path, spec.to_yaml()?)?;

        Ok(())
    }
}

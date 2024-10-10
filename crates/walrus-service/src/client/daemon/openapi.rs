// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use utoipa::OpenApi;
use walrus_core::{EncodingType, EpochSchema};
use walrus_sui::types::{Blob, StorageResource};

use super::routes;
use crate::{
    client::{resource::RegisterBlobOp, responses::EventOrObjectId, BlobStoreResult},
    common::api::{BlobIdString, EventIdSchema, ObjectIdSchema},
};

#[derive(OpenApi)]
#[openapi(
    info(title = "Walrus Aggregator"),
    paths(routes::get_blob),
    components(schemas(BlobIdString,))
)]
pub(super) struct AggregatorApiDoc;

#[derive(OpenApi)]
#[openapi(
    info(title = "Walrus Publisher"),
    paths(routes::put_blob),
    components(schemas(
        Blob,
        BlobIdString,
        BlobStoreResult,
        EncodingType,
        EpochSchema,
        EventIdSchema,
        EventOrObjectId,
        ObjectIdSchema,
        RegisterBlobOp,
        StorageResource,
    ))
)]
pub(super) struct PublisherApiDoc;

#[derive(OpenApi)]
#[openapi(
    info(title = "Walrus Daemon"),
    paths(routes::get_blob, routes::put_blob),
    components(schemas(
        Blob,
        BlobIdString,
        BlobStoreResult,
        EncodingType,
        EpochSchema,
        EventIdSchema,
        EventOrObjectId,
        ObjectIdSchema,
        RegisterBlobOp,
        StorageResource,
    ))
)]
pub(super) struct DaemonApiDoc;

#[cfg(test)]
mod tests {
    use utoipa::OpenApi as _;
    use utoipa_redoc::Redoc;

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
}

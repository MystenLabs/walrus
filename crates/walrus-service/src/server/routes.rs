// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use tracing::Level;
use walrus_core::{
    messages::{InvalidBlobIdAttestation, StorageConfirmation},
    metadata::{BlobMetadata, UnverifiedBlobMetadataWithId, VerifiedBlobMetadataWithId},
    BlobId,
    InconsistencyProof,
    RecoverySymbol,
    Sliver,
    SliverPairIndex,
    SliverType,
};
use walrus_sdk::api::BlobStatus;

use super::{
    extract::Bcs,
    openapi,
    responses::{ApiSuccess, OrRejection},
};
use crate::node::{
    BlobStatusError,
    ComputeStorageConfirmationError,
    InconsistencyProofError,
    RetrieveMetadataError,
    RetrieveSliverError,
    RetrieveSymbolError,
    ServiceState,
    StoreMetadataError,
    StoreSliverError,
};

/// Open API documentation endpoint
pub const API_DOCS: &str = "/v1/api";
/// The path to get and store blob metadata.
pub const METADATA_ENDPOINT: &str = "/v1/blobs/:blob_id/metadata";
/// The path to get and store slivers.
pub const SLIVER_ENDPOINT: &str = "/v1/blobs/:blob_id/slivers/:sliver_pair_index/:sliver_type";
/// The path to get storage confirmations.
pub const STORAGE_CONFIRMATION_ENDPOINT: &str = "/v1/blobs/:blob_id/confirmation";
/// The path to get recovery symbols.
pub const RECOVERY_ENDPOINT: &str =
    "/v1/blobs/:blob_id/slivers/:sliver_pair_index/:sliver_type/:target_pair_index";
/// The path to push inconsistency proofs.
pub const INCONSISTENCY_PROOF_ENDPOINT: &str = "/v1/blobs/:blob_id/inconsistent/:sliver_type";
/// The path to get the status of a blob.
pub const STATUS_ENDPOINT: &str = "/v1/blobs/:blob_id/status";

/// A blob ID encoded as a URL-safe Base64 string, without the trailing equal (=) signs.
#[serde_as]
#[derive(Deserialize, Serialize, utoipa::ToSchema)]
#[schema(
    value_type = String,
    format = Byte,
    example = json!("E7_nNXvFU_3qZVu3OH1yycRG7LZlyn1-UxEDCDDqGGU"),
)]
pub(crate) struct BlobIdString(#[serde_as(as = "DisplayFromStr")] pub(crate) BlobId);

/// Convenience trait to apply bounds on the ServiceState.
trait SyncServiceState: ServiceState + Send + Sync + 'static {}
impl<T: ServiceState + Send + Sync + 'static> SyncServiceState for T {}

/// Get blob metadata.
///
/// Gets the metadata associated with a Walrus blob, as a BCS encoded byte stream.
#[tracing::instrument(skip_all, fields(walrus.blob_id = %blob_id), err(level = Level::DEBUG))]
#[utoipa::path(
    get,
    path = openapi::rewrite_route(METADATA_ENDPOINT),
    params(("blob_id" = BlobIdString,)),
    responses(
        (status = 200, description = "BCS encoded blob metadata", body = [u8]),
        RetrieveMetadataError
    ),
    tag = openapi::GROUP_READING_BLOBS
)]
pub async fn get_metadata<S: SyncServiceState>(
    State(state): State<Arc<S>>,
    Path(BlobIdString(blob_id)): Path<BlobIdString>,
) -> Result<Bcs<VerifiedBlobMetadataWithId>, RetrieveMetadataError> {
    Ok(Bcs(state.retrieve_metadata(&blob_id)?))
}

/// Store blob metadata.
///
/// Stores the metadata associated with a registered Walrus blob at this storage node. This is a
/// pre-requisite for storing the encoded slivers of the blob. The ID of the blob must first be
/// registered on Sui, after which storing the metadata becomes possible.
///
/// This endpoint may return an error if the node has not yet received the registration event from
/// the chain.
#[tracing::instrument(skip_all, fields(walrus.blob_id = %blob_id), err(level = Level::DEBUG))]
#[utoipa::path(
    put,
    path = openapi::rewrite_route(METADATA_ENDPOINT),
    params(("blob_id" = BlobIdString,)),
    request_body(content = [u8], description = "BCS-encoded metadata octet-stream"),
    responses(
        (status = CREATED, description = "Metadata successfully stored", body = ApiSuccessMessage),
        (status = OK, description = "Metadata is already stored", body = ApiSuccessMessage),
        StoreMetadataError,
    ),
    tag = openapi::GROUP_STORING_BLOBS
)]
pub async fn put_metadata<S: SyncServiceState>(
    State(state): State<Arc<S>>,
    Path(BlobIdString(blob_id)): Path<BlobIdString>,
    Bcs(metadata): Bcs<BlobMetadata>,
) -> Result<ApiSuccess<&'static str>, StoreMetadataError> {
    let (code, message) =
        if state.store_metadata(UnverifiedBlobMetadataWithId::new(blob_id, metadata))? {
            (StatusCode::CREATED, "metadata successfully stored")
        } else {
            (StatusCode::OK, "metadata already stored")
        };

    Ok(ApiSuccess::new(code, message))
}

/// Get blob slivers.
///
/// Gets the primary or secondary sliver identified by the specified blob ID and index. The
/// index should represent a sliver that is assigned to be stored at one of the shards managed
/// by this storage node during this epoch.
#[tracing::instrument(skip_all, err(level = Level::DEBUG), fields(
    walrus.blob_id = %blob_id.0,
    walrus.sliver.pair_index = %sliver_pair_index,
    walrus.sliver.type_ = %sliver_type
))]
#[utoipa::path(
    get,
    path = openapi::rewrite_route(SLIVER_ENDPOINT),
    params(
        ("blob_id" = BlobIdString, ),
        ("sliver_pair_index" = SliverPairIndex, ),
        ("sliver_type" = SliverType, ),
    ),
    responses(
        (status = 200, description = "BCS encoded primary or secondary sliver", body = [u8]),
        RetrieveSliverError,
    ),
    tag = openapi::GROUP_READING_BLOBS
)]
pub async fn get_sliver<S: SyncServiceState>(
    State(state): State<Arc<S>>,
    Path((blob_id, sliver_pair_index, sliver_type)): Path<(
        BlobIdString,
        SliverPairIndex,
        SliverType,
    )>,
) -> Result<Response, RetrieveSliverError> {
    let blob_id = blob_id.0;
    let sliver = state.retrieve_sliver(&blob_id, sliver_pair_index, sliver_type)?;

    debug_assert_eq!(sliver.r#type(), sliver_type, "invalid sliver type fetched");
    match sliver {
        Sliver::Primary(inner) => Ok(Bcs(inner).into_response()),
        Sliver::Secondary(inner) => Ok(Bcs(inner).into_response()),
    }
}

/// Store blob slivers.
///
/// Stores a primary or secondary blob sliver at the storage node.
#[tracing::instrument(skip_all, err(level = Level::DEBUG), fields(
    walrus.blob_id = %blob_id.0,
    walrus.sliver.pair_index = %sliver_pair_index,
    walrus.sliver.type_ = %sliver_type
))]
#[utoipa::path(
    put,
    path = openapi::rewrite_route(SLIVER_ENDPOINT),
    params(
        ("blob_id" = BlobIdString, ),
        ("sliver_pair_index" = SliverPairIndex, ),
        ("sliver_type" = SliverType, )
    ),
    request_body(content = [u8], description = "BCS-encoded sliver octet-stream"),
    responses(
        (status = OK, description = "Sliver successfully stored", body = ApiSuccessMessage),
        StoreSliverError,
    ),
    tag = openapi::GROUP_STORING_BLOBS,
)]
pub async fn put_sliver<S: SyncServiceState>(
    State(state): State<Arc<S>>,
    Path((blob_id, sliver_pair_index, sliver_type)): Path<(
        BlobIdString,
        SliverPairIndex,
        SliverType,
    )>,
    body: axum::body::Bytes,
) -> Result<ApiSuccess<&'static str>, OrRejection<StoreSliverError>> {
    let blob_id = blob_id.0;
    let sliver = match sliver_type {
        SliverType::Primary => Sliver::Primary(Bcs::from_bytes(&body)?.0),
        SliverType::Secondary => Sliver::Secondary(Bcs::from_bytes(&body)?.0),
    };

    state.store_sliver(&blob_id, sliver_pair_index, &sliver)?;

    // TODO(jsmith): Change to CREATED
    Ok(ApiSuccess::ok("sliver stored successfully"))
}

/// Get storage confirmation.
///
/// Gets a signed storage confirmation from this storage node, indicating that all shards assigned
/// to this storage node for the current epoch have stored their respective slivers.
#[tracing::instrument(skip_all, fields(walrus.blob_id = %blob_id), err(level = Level::DEBUG))]
#[utoipa::path(
    get,
    path = openapi::rewrite_route(STORAGE_CONFIRMATION_ENDPOINT),
    params(("blob_id" = BlobIdString,)),
    responses(
        (status = 200, description = "A signed confirmation of storage",
        body = ApiSuccessStorageConfirmation),
        ComputeStorageConfirmationError,
    ),
    tag = openapi::GROUP_STORING_BLOBS
)]
pub async fn get_storage_confirmation<S: SyncServiceState>(
    State(state): State<Arc<S>>,
    Path(BlobIdString(blob_id)): Path<BlobIdString>,
) -> Result<ApiSuccess<StorageConfirmation>, ComputeStorageConfirmationError> {
    let confirmation = state.compute_storage_confirmation(&blob_id).await?;

    Ok(ApiSuccess::ok(confirmation))
}

/// Get recovery symbols.
///
/// Gets a symbol held by this storage node to aid in sliver recovery.
///
/// The `sliver_type` is the target type of the sliver that will be recovered.
/// The `sliver_pair_index` is the index of the sliver pair that we want to access.
/// The `target_pair_index` is the index of the target sliver.
#[tracing::instrument(skip_all, err(level = Level::DEBUG), fields(
    walrus.blob_id = %blob_id.0,
    walrus.sliver.pair_index = %sliver_pair_index,
    walrus.sliver.target_pair_index = %target_pair_index,
    walrus.sliver.symbol_type = %sliver_type
))]
#[utoipa::path(
    get,
    path = openapi::rewrite_route(RECOVERY_ENDPOINT),
    params(
        ("blob_id" = BlobIdString,),
        ("sliver_pair_index" = SliverPairIndex, ),
        ("target_pair_index" = SliverPairIndex, ),
        ("sliver_type" = SliverType, )
    ),
    responses(
        (status = 200, description = "BCS encoded symbol", body = [u8]),
        RetrieveSymbolError,
    ),
    tag = openapi::GROUP_RECOVERY
)]
pub async fn get_recovery_symbol<S: SyncServiceState>(
    State(state): State<Arc<S>>,
    Path((blob_id, sliver_pair_index, sliver_type, target_pair_index)): Path<(
        BlobIdString,
        SliverPairIndex,
        SliverType,
        SliverPairIndex,
    )>,
) -> Result<Response, RetrieveSymbolError> {
    let blob_id = blob_id.0;
    let symbol = state.retrieve_recovery_symbol(
        &blob_id,
        sliver_pair_index,
        sliver_type,
        target_pair_index,
    )?;

    match symbol {
        RecoverySymbol::Primary(inner) => Ok(Bcs(inner).into_response()),
        RecoverySymbol::Secondary(inner) => Ok(Bcs(inner).into_response()),
    }
}

/// Verify blob inconsistency.
///
/// Accepts an inconsistency proof from other storage nodes, verifies it, and returns an attestation
/// that the specified blob is inconsistent.
// TODO(jsmith): This endpoint should be a POST endpoint, not a PUT endpoint (#461).
#[tracing::instrument(skip_all, err(level = Level::DEBUG), fields(
    walrus.blob_id = %blob_id.0, walrus.sliver.type_ = %sliver_type
))]
#[utoipa::path(
    put,
    path = openapi::rewrite_route(INCONSISTENCY_PROOF_ENDPOINT),
    params(("blob_id" = BlobIdString,), ("sliver_type" = SliverType,)),
    request_body(content = [u8], description = "BCS-encoded inconsistency proof"),
    responses(
        (status = 200, description = "Signed invalid blob-id attestation",
        body = ApiSuccessSignedMessage),
        InconsistencyProofError,
    ),
    tag = openapi::GROUP_RECOVERY
)]
pub async fn inconsistency_proof<S: SyncServiceState>(
    State(state): State<Arc<S>>,
    Path((blob_id, sliver_type)): Path<(BlobIdString, SliverType)>,
    body: axum::body::Bytes,
) -> Result<ApiSuccess<InvalidBlobIdAttestation>, OrRejection<InconsistencyProofError>> {
    let blob_id = blob_id.0;
    let inconsistency_proof = match sliver_type {
        SliverType::Primary => InconsistencyProof::Primary(Bcs::from_bytes(&body)?.0),
        SliverType::Secondary => InconsistencyProof::Secondary(Bcs::from_bytes(&body)?.0),
    };

    let attestation = state
        .verify_inconsistency_proof(&blob_id, inconsistency_proof)
        .await?;

    Ok(ApiSuccess::ok(attestation))
}

/// Get the status of a blob.
///
/// Gets the status of a blob as viewed by this storage node, such as whether it is registered,
/// certified, or invalid, and the event identifier on Sui that led to the change in status.
#[tracing::instrument(skip_all, fields(walrus.blob_id = %blob_id), err(level = Level::DEBUG))]
#[utoipa::path(
    get,
    path = openapi::rewrite_route(STATUS_ENDPOINT),
    params(("blob_id" = BlobIdString,)),
    responses(
        (status = 200, description = "The status of the blob", body = ApiSuccessBlobStatus),
        BlobStatusError
    ),
    tag = openapi::GROUP_READING_BLOBS
)]
pub async fn get_blob_status<S: SyncServiceState>(
    State(state): State<Arc<S>>,
    Path(BlobIdString(blob_id)): Path<BlobIdString>,
) -> Result<ApiSuccess<BlobStatus>, BlobStatusError> {
    Ok(ApiSuccess::ok(state.blob_status(&blob_id)?))
}

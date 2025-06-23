// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::num::NonZeroU16;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use axum_extra::extract::Query as ExtraQuery;
use serde::Deserialize;
use serde_with::{DisplayFromStr, OneOrMany, serde_as};
use sui_types::base_types::ObjectID;
use tokio::sync::{Semaphore, SemaphorePermit, TryAcquireError};
use tracing::Level;
use walrus_core::{
    BlobId,
    InconsistencyProof,
    RecoverySymbol,
    Sliver,
    SliverIndex,
    SliverPairIndex,
    SliverType,
    SymbolId,
    encoding::{GeneralRecoverySymbol, Primary as PrimaryEncoding, Secondary as SecondaryEncoding},
    messages::{
        BlobPersistenceType,
        InvalidBlobIdAttestation,
        SignedMessage,
        SignedSyncShardRequest,
        StorageConfirmation,
    },
    metadata::{BlobMetadata, UnverifiedBlobMetadataWithId, VerifiedBlobMetadataWithId},
};
use walrus_storage_node_client::{
    RecoverySymbolsFilter,
    api::{BlobStatus, ServiceHealthInfo, StoredOnNodeStatus},
};
use walrus_sui::ObjectIdSchema;

use super::{
    RestApiState,
    extract::{Authorization, Bcs},
    openapi::{self},
    responses::OrRejection,
};
use crate::{
    common::api::{ApiSuccess, BlobIdString},
    node::{
        BlobStatusError,
        ComputeStorageConfirmationError,
        InconsistencyProofError,
        RetrieveMetadataError,
        RetrieveSliverError,
        RetrieveSymbolError,
        ServiceState,
        StoreMetadataError,
        StoreSliverError,
        SyncShardServiceError,
        errors::{IndexOutOfRange, ListSymbolsError, Unavailable},
    },
};

/// OpenAPI documentation endpoint.
pub const API_DOCS_ENDPOINT: &str = "/v1/api";
/// The path to get and store blob metadata.
pub const METADATA_ENDPOINT: &str = "/v1/blobs/{blob_id}/metadata";
/// The path to get the status of metadata for a blob.
pub const METADATA_STATUS_ENDPOINT: &str = "/v1/blobs/{blob_id}/metadata/status";
/// The path to get and store slivers.
pub const SLIVER_ENDPOINT: &str = "/v1/blobs/{blob_id}/slivers/{sliver_pair_index}/{sliver_type}";
/// The path to check if a sliver is stored.
pub const SLIVER_STATUS_ENDPOINT: &str =
    "/v1/blobs/{blob_id}/slivers/{sliver_pair_index}/{sliver_type}/status";
/// The path to get blob confirmations for permanent blobs.
pub const PERMANENT_BLOB_CONFIRMATION_ENDPOINT: &str = "/v1/blobs/{blob_id}/confirmation/permanent";
/// The path to get blob confirmations for deletable blobs.
pub const DELETABLE_BLOB_CONFIRMATION_ENDPOINT: &str =
    "/v1/blobs/{blob_id}/confirmation/deletable/{object_id}";
/// The path to get recovery symbols.
pub const RECOVERY_ENDPOINT: &str =
    "/v1/blobs/{blob_id}/slivers/{sliver_pair_index}/{sliver_type}/{target_pair_index}";
/// The path to get recovery symbols.
pub const RECOVERY_SYMBOL_ENDPOINT: &str = "/v1/blobs/{blob_id}/recoverySymbols/{symbol_id}";
/// The path to get multiple recovery symbols.
pub const RECOVERY_SYMBOL_LIST_ENDPOINT: &str = "/v1/blobs/{blob_id}/recoverySymbols";
/// The path to push inconsistency proofs.
pub const INCONSISTENCY_PROOF_ENDPOINT: &str =
    "/v1/blobs/{blob_id}/inconsistencyProof/{sliver_type}";
/// The path to get the status of a blob.
pub const BLOB_STATUS_ENDPOINT: &str = "/v1/blobs/{blob_id}/status";
pub const HEALTH_ENDPOINT: &str = "/v1/health";
pub const SYNC_SHARD_ENDPOINT: &str = "/v1/migrate/sync_shard";

/// Convenience trait to apply bounds on the ServiceState.
trait SyncServiceState: ServiceState + Send + Sync + 'static {}
impl<T: ServiceState + Send + Sync + 'static> SyncServiceState for T {}

/// Get blob metadata.
///
/// Gets the metadata associated with a Walrus blob, as a BCS encoded byte stream.
#[tracing::instrument(skip_all, fields(walrus.blob_id = %blob_id), err(level = Level::DEBUG))]
#[utoipa::path(
    get,
    path = METADATA_ENDPOINT,
    params(("blob_id" = BlobId,)),
    responses(
        (status = 200, description = "BCS encoded blob metadata", body = [u8]),
        RetrieveMetadataError
    ),
    tag = openapi::GROUP_READING_BLOBS
)]
pub async fn get_metadata<S: SyncServiceState>(
    State(state): State<RestApiState<S>>,
    Path(BlobIdString(blob_id)): Path<BlobIdString>,
) -> Result<Bcs<VerifiedBlobMetadataWithId>, RetrieveMetadataError> {
    Ok(Bcs(state.service.retrieve_metadata(&blob_id)?))
}

/// Check if the metadata for a blob is already stored.
#[tracing::instrument(skip_all, fields(walrus.blob_id = %blob_id), err(level = Level::DEBUG))]
#[utoipa::path(
    get,
    path = METADATA_STATUS_ENDPOINT,
    params(("blob_id" = BlobId,)),
    responses(
        (
            status = 200,
            description = "The storage status of the blob metadata",
            body = ApiSuccess<StoredOnNodeStatus>
        ),
        RetrieveMetadataError
    ),
    tag = openapi::GROUP_STATUS
)]
pub async fn get_metadata_status<S: SyncServiceState>(
    State(state): State<RestApiState<S>>,
    Path(BlobIdString(blob_id)): Path<BlobIdString>,
) -> Result<ApiSuccess<StoredOnNodeStatus>, RetrieveMetadataError> {
    Ok(ApiSuccess::ok(state.service.metadata_status(&blob_id)?))
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
    path = METADATA_ENDPOINT,
    params(("blob_id" = BlobId,)),
    request_body(content = [u8], description = "BCS-encoded metadata octet-stream"),
    responses(
        (status = CREATED, description = "Metadata successfully stored", body = ApiSuccess<String>),
        (status = OK, description = "Metadata is already stored", body = ApiSuccess<String>),
        StoreMetadataError,
    ),
    tag = openapi::GROUP_STORING_BLOBS
)]
pub async fn put_metadata<S: SyncServiceState>(
    State(state): State<RestApiState<S>>,
    Path(BlobIdString(blob_id)): Path<BlobIdString>,
    Bcs(metadata): Bcs<BlobMetadata>,
) -> Result<ApiSuccess<&'static str>, StoreMetadataError> {
    let (code, message) = if state
        .service
        .store_metadata(UnverifiedBlobMetadataWithId::new(blob_id, metadata))
        .await?
    {
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
    walrus.sliver.r#type = %sliver_type
))]
#[utoipa::path(
    get,
    path = SLIVER_ENDPOINT,
    params(
        ("blob_id" = BlobId, ),
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
    State(state): State<RestApiState<S>>,
    Path((blob_id, sliver_pair_index, sliver_type)): Path<(
        BlobIdString,
        SliverPairIndex,
        SliverType,
    )>,
) -> Result<Response, RetrieveSliverError> {
    let blob_id = blob_id.0;
    let sliver = state
        .service
        .retrieve_sliver(&blob_id, sliver_pair_index, sliver_type)
        .await?;

    debug_assert_eq!(sliver.r#type(), sliver_type, "invalid sliver type fetched");
    match sliver {
        Sliver::Primary(inner) => Ok(Bcs(inner).into_response()),
        Sliver::Secondary(inner) => Ok(Bcs(inner).into_response()),
    }
}

/// Store blob slivers.
///
/// Stores a primary or secondary blob sliver at the storage node.
#[tracing::instrument(skip_all, err(level = Level::DEBUG), ret(level = Level::DEBUG), fields(
    walrus.blob_id = %blob_id.0,
    walrus.sliver.pair_index = %sliver_pair_index,
    walrus.sliver.r#type = %sliver_type
))]
#[utoipa::path(
    put,
    path = SLIVER_ENDPOINT,
    params(
        ("blob_id" = BlobId, ),
        ("sliver_pair_index" = SliverPairIndex, ),
        ("sliver_type" = SliverType, )
    ),
    request_body(content = [u8], description = "BCS-encoded sliver octet-stream"),
    responses(
        (status = OK, description = "Sliver successfully stored", body = ApiSuccess<String>),
        StoreSliverError,
    ),
    tag = openapi::GROUP_STORING_BLOBS,
)]
pub async fn put_sliver<S: SyncServiceState>(
    State(state): State<RestApiState<S>>,
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

    state
        .service
        .store_sliver(blob_id, sliver_pair_index, sliver)
        .await?;

    // TODO(WAL-253): Change to CREATED.
    Ok(ApiSuccess::ok("sliver stored successfully"))
}

/// Check if the blob slivers are present.
///
/// Checks if the primary or secondary sliver identified by the specified blob ID and index are
/// present in the database. The index should represent a sliver that is assigned to be stored at
/// one of the shards managed by this storage node during this epoch.
#[tracing::instrument(skip_all, err(level = Level::DEBUG), fields(
    walrus.blob_id = %blob_id.0,
    walrus.sliver.pair_index = %sliver_pair_index,
    walrus.sliver.r#type = %sliver_type
))]
#[utoipa::path(
    get,
    path = SLIVER_STATUS_ENDPOINT,
    params(
        ("blob_id" = BlobId, ),
        ("sliver_pair_index" = SliverPairIndex, ),
        ("sliver_type" = SliverType, ),
    ),
    responses(
        (
            status = 200,
            description = "The storage status of the primary or secondary sliver",
            body = ApiSuccess<StoredOnNodeStatus>,
        ),
        RetrieveSliverError,
    ),
    tag = openapi::GROUP_STATUS
)]
pub async fn get_sliver_status<S: SyncServiceState>(
    State(state): State<RestApiState<S>>,
    Path((blob_id, sliver_pair_index, sliver_type)): Path<(
        BlobIdString,
        SliverPairIndex,
        SliverType,
    )>,
) -> Result<ApiSuccess<StoredOnNodeStatus>, RetrieveSliverError> {
    let blob_id = blob_id.0;
    let status = match sliver_type {
        SliverType::Primary => {
            state
                .service
                .sliver_status::<PrimaryEncoding>(&blob_id, sliver_pair_index)
                .await
        }
        SliverType::Secondary => {
            state
                .service
                .sliver_status::<SecondaryEncoding>(&blob_id, sliver_pair_index)
                .await
        }
    }?;
    Ok(ApiSuccess::ok(status))
}

/// Get storage confirmation for permanent blobs.
///
/// Gets a signed storage confirmation from this storage node, indicating that all shards assigned
/// to this storage node for the current epoch have stored their respective slivers.
#[tracing::instrument(skip_all, fields(walrus.blob_id = %blob_id), err(level = Level::DEBUG))]
#[utoipa::path(
    get,
    path = PERMANENT_BLOB_CONFIRMATION_ENDPOINT,
    params(("blob_id" = BlobId,)),
    responses(
        (status = 200, description = "A signed confirmation of storage",
        body = ApiSuccess<StorageConfirmation>),
        ComputeStorageConfirmationError,
    ),
    tag = openapi::GROUP_STORING_BLOBS
)]
pub async fn get_permanent_blob_confirmation<S: SyncServiceState>(
    State(state): State<RestApiState<S>>,
    Path(BlobIdString(blob_id)): Path<BlobIdString>,
) -> Result<ApiSuccess<StorageConfirmation>, ComputeStorageConfirmationError> {
    let confirmation = state
        .service
        .compute_storage_confirmation(&blob_id, &BlobPersistenceType::Permanent)
        .await?;

    Ok(ApiSuccess::ok(confirmation))
}

/// Get storage confirmation for deletable blobs.
///
/// Gets a signed storage confirmation from this storage node, indicating that all shards assigned
/// to this storage node for the current epoch have stored their respective slivers.
#[tracing::instrument(
    skip_all,
    fields(walrus.blob_id = %blob_id_string.0, walrus.object_id = %object_id),
    err(level = Level::DEBUG)
)]
#[utoipa::path(
    get,
    path = DELETABLE_BLOB_CONFIRMATION_ENDPOINT,
    params(("blob_id" = BlobId,), ("object_id" = ObjectIdSchema,)),
    responses(
        (status = 200, description = "A signed confirmation of storage",
        body = ApiSuccess<StorageConfirmation>),
        ComputeStorageConfirmationError,
    ),
    tag = openapi::GROUP_STORING_BLOBS
)]
pub async fn get_deletable_blob_confirmation<S: SyncServiceState>(
    State(state): State<RestApiState<S>>,
    Path((blob_id_string, object_id)): Path<(BlobIdString, ObjectID)>,
) -> Result<ApiSuccess<StorageConfirmation>, ComputeStorageConfirmationError> {
    let blob_id = blob_id_string.0;
    let confirmation = state
        .service
        .compute_storage_confirmation(
            &blob_id,
            &BlobPersistenceType::Deletable {
                object_id: object_id.into(),
            },
        )
        .await?;

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
    walrus.sliver.remote_pair_index = %target_pair_index,
    walrus.recovery.symbol_type = %sliver_type
))]
#[utoipa::path(
    get,
    path = RECOVERY_ENDPOINT,
    params(
        ("blob_id" = BlobId,),
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
#[deprecated = "use `get_recovery_symbol_by_id` instead"]
pub async fn get_recovery_symbol<S: SyncServiceState>(
    State(state): State<RestApiState<S>>,
    Path((blob_id, sliver_pair_index, sliver_type, target_pair_index)): Path<(
        BlobIdString,
        SliverPairIndex,
        SliverType,
        SliverPairIndex,
    )>,
) -> Result<Response, RetrieveSymbolError> {
    let blob_id = blob_id.0;
    let n_shards = state.service.n_shards();
    let _guard = limit_symbol_recovery_requests(state.recovery_symbols_limit.as_deref())?;

    check_index(sliver_pair_index, n_shards)?;
    check_index(target_pair_index, n_shards)?;

    // The sliver type of the local sliver is orthogonal to the identified sliver type.
    let (primary_index, secondary_index) = match sliver_type {
        SliverType::Primary => {
            // The target_pair_index is the primary sliver.
            (
                target_pair_index.to_sliver_index::<PrimaryEncoding>(n_shards),
                sliver_pair_index.to_sliver_index::<SecondaryEncoding>(n_shards),
            )
        }
        SliverType::Secondary => {
            // The target_pair_index is the secodary sliver.
            (
                sliver_pair_index.to_sliver_index::<PrimaryEncoding>(n_shards),
                target_pair_index.to_sliver_index::<SecondaryEncoding>(n_shards),
            )
        }
    };
    let symbol_id = SymbolId::new(primary_index, secondary_index);
    let symbol = state
        .service
        .retrieve_recovery_symbol(&blob_id, symbol_id, Some(sliver_type))
        .await?
        .into();

    match symbol {
        RecoverySymbol::Primary(inner) => Ok(Bcs(inner).into_response()),
        RecoverySymbol::Secondary(inner) => Ok(Bcs(inner).into_response()),
    }
}

fn check_index(index: SliverPairIndex, n_shards: NonZeroU16) -> Result<(), IndexOutOfRange> {
    if index.get() < n_shards.get() {
        Ok(())
    } else {
        Err(IndexOutOfRange {
            index: index.get(),
            max: n_shards.get(),
        })
    }
}

/// Get a recovery symbol by its ID.
#[tracing::instrument(skip_all, err(level = Level::DEBUG), fields(
    walrus.blob_id = %blob_id.0,
    walrus.recovery.symbol_id = %symbol_id
))]
#[utoipa::path(
    get,
    path = RECOVERY_SYMBOL_ENDPOINT,
    params(("blob_id" = BlobId,), ("symbol_id" = SymbolId, ),),
    responses(
        (status = 200, description = "BCS-encoded primary/secondary recovery symbol", body = [u8]),
        RetrieveSymbolError,
    ),
    tag = openapi::GROUP_RECOVERY
)]
pub async fn get_recovery_symbol_by_id<S: SyncServiceState>(
    State(state): State<RestApiState<S>>,
    Path((blob_id, symbol_id)): Path<(BlobIdString, SymbolId)>,
) -> Result<Response, RetrieveSymbolError> {
    let _guard = limit_symbol_recovery_requests(state.recovery_symbols_limit.as_deref())?;

    let symbol = state
        .service
        .retrieve_recovery_symbol(&blob_id.0, symbol_id, None)
        .await?;

    Ok(Bcs(symbol).into_response())
}

/// Specifies the set of recovery symbols to be returned.
#[derive(Debug, Clone, Deserialize, utoipa::IntoParams)]
#[serde(rename_all = "camelCase")]
#[into_params(style = Form, parameter_in = Query)]
pub struct ListRecoverySymbolsQuery {
    /// The sliver axis from which the proof should be constructed.
    ///
    /// Only necessary if you intend to construct inconsistency proofs with the returned symbols.
    proof_axis: Option<SliverType>,

    #[serde(flatten)]
    #[param(inline)]
    ids: ListRecoverySymbolIdsFilter,
}

#[serde_as]
#[derive(Debug, Clone, Deserialize, utoipa::ToSchema)]
#[serde(untagged, rename_all = "camelCase")]
pub(crate) enum ListRecoverySymbolIdsFilter {
    /// Limit the results to the specified symbols.
    Id {
        #[serde_as(as = "OneOrMany<_>")]
        id: Vec<SymbolId>,
    },

    /// Return all available symbols that can be used to recover the specified sliver.
    #[serde(rename_all = "camelCase")]
    ForSliver {
        /// The ID of the target sliver being recovered.
        #[serde_as(as = "DisplayFromStr")]
        target_sliver: SliverIndex,
        /// The type of the sliver being recovered.
        target_type: SliverType,
    },
}

impl TryFrom<ListRecoverySymbolsQuery> for RecoverySymbolsFilter {
    type Error = ListSymbolsError;

    fn try_from(query: ListRecoverySymbolsQuery) -> Result<Self, Self::Error> {
        let filter = match query.ids {
            ListRecoverySymbolIdsFilter::Id { id: mut symbol_ids } => {
                symbol_ids.sort_unstable();
                symbol_ids.dedup();

                RecoverySymbolsFilter::ids(symbol_ids)
                    .ok_or(ListSymbolsError::NoSymbolsSpecified)?
            }
            ListRecoverySymbolIdsFilter::ForSliver {
                target_sliver,
                target_type,
            } => RecoverySymbolsFilter::recovers(target_sliver, target_type),
        };

        if let Some(proof_axis) = query.proof_axis {
            Ok(filter.require_proof_from_axis(proof_axis))
        } else {
            Ok(filter)
        }
    }
}

/// Get multiple recovery symbols.
#[tracing::instrument(skip_all, err(level = Level::DEBUG), fields(walrus.blob_id = %blob_id))]
#[utoipa::path(
    get,
    path = RECOVERY_SYMBOL_LIST_ENDPOINT,
    params(("blob_id" = BlobId,), ListRecoverySymbolsQuery),
    responses(
        (status = 200, description = "List of BCS-encoded recovery symbols", body = [u8]),
        ListSymbolsError,
    ),
    tag = openapi::GROUP_RECOVERY
)]
pub async fn list_recovery_symbols<S: SyncServiceState>(
    State(state): State<RestApiState<S>>,
    Path(BlobIdString(blob_id)): Path<BlobIdString>,
    ExtraQuery(query): ExtraQuery<ListRecoverySymbolsQuery>,
) -> Result<Bcs<Vec<GeneralRecoverySymbol>>, ListSymbolsError> {
    let _guard = limit_symbol_recovery_requests(state.recovery_symbols_limit.as_deref())?;

    let filter = query.try_into()?;
    let symbols = state
        .service
        .retrieve_multiple_recovery_symbols(&blob_id, filter)
        .await?;

    Ok(Bcs(symbols))
}

/// Acquires and returns a permit that allows making a request for one or more recovery symbols.
///
/// If no semaphore is provided, then this always succeeds but does not return a permit.
/// Returns [`RetrieveSymbolError::Unavailable`] if no permits are available, otherwise returns the
/// acquired permit.
fn limit_symbol_recovery_requests(
    limit: Option<&Semaphore>,
) -> Result<Option<SemaphorePermit<'_>>, RetrieveSymbolError> {
    let Some(semaphore) = limit else {
        return Ok(None);
    };

    match semaphore.try_acquire() {
        Ok(permit) => Ok(Some(permit)),
        Err(TryAcquireError::Closed) => panic!("the semaphore must not be closed"),
        Err(TryAcquireError::NoPermits) => Err(Unavailable.into()),
    }
}

/// Verify blob inconsistency.
///
/// Accepts an inconsistency proof from other storage nodes, verifies it, and returns an attestation
/// that the specified blob is inconsistent.
#[tracing::instrument(skip_all, err(level = Level::DEBUG), fields(
    walrus.blob_id = %blob_id.0, walrus.sliver.r#type = %sliver_type
))]
#[utoipa::path(
    post,
    path = INCONSISTENCY_PROOF_ENDPOINT,
    params(("blob_id" = BlobId,), ("sliver_type" = SliverType,)),
    request_body(content = [u8], description = "BCS-encoded inconsistency proof"),
    responses(
        (status = 200, description = "Signed invalid blob-id attestation",
        body = ApiSuccess<SignedMessage::<u8>>),
        InconsistencyProofError,
    ),
    tag = openapi::GROUP_RECOVERY
)]
pub async fn inconsistency_proof<S: SyncServiceState>(
    State(state): State<RestApiState<S>>,
    Path((blob_id, sliver_type)): Path<(BlobIdString, SliverType)>,
    body: axum::body::Bytes,
) -> Result<ApiSuccess<InvalidBlobIdAttestation>, OrRejection<InconsistencyProofError>> {
    let blob_id = blob_id.0;
    let inconsistency_proof = match sliver_type {
        SliverType::Primary => InconsistencyProof::Primary(Bcs::from_bytes(&body)?.0),
        SliverType::Secondary => InconsistencyProof::Secondary(Bcs::from_bytes(&body)?.0),
    };

    let attestation = state
        .service
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
    path = BLOB_STATUS_ENDPOINT,
    params(("blob_id" = BlobId,)),
    responses(
        (status = 200, description = "The status of the blob", body = ApiSuccess<BlobStatus>),
        BlobStatusError
    ),
    tag = openapi::GROUP_READING_BLOBS
)]
pub async fn get_blob_status<S: SyncServiceState>(
    State(state): State<RestApiState<S>>,
    Path(BlobIdString(blob_id)): Path<BlobIdString>,
) -> Result<ApiSuccess<BlobStatus>, BlobStatusError> {
    Ok(ApiSuccess::ok(state.service.blob_status(&blob_id)?))
}

#[derive(Debug, Clone, serde::Deserialize, utoipa::IntoParams)]
#[serde(rename_all = "camelCase")]
pub struct HealthInfoQuery {
    /// When true, includes the status of each start in the health info.
    #[serde(default)]
    detailed: bool,
}

/// Get storage health information.
///
/// Gets the storage node's health information and basic running stats.
#[tracing::instrument(skip_all)]
#[utoipa::path(
    get,
    path = HEALTH_ENDPOINT,
    params(HealthInfoQuery),
    responses(
        (status = 200, description = "Server is running", body = ApiSuccess<ServiceHealthInfo>),
    ),
    tag = openapi::GROUP_STATUS
)]
pub async fn health_info<S: SyncServiceState>(
    Query(query): Query<HealthInfoQuery>,
    State(state): State<RestApiState<S>>,
) -> ApiSuccess<ServiceHealthInfo> {
    ApiSuccess::ok(state.service.health_info(query.detailed).await)
}

#[tracing::instrument(skip_all)]
#[utoipa::path(
    post,
    path = SYNC_SHARD_ENDPOINT,
    params(
        ("Authorization" = String, Header, description = "Public key for authorization")
    ),
    request_body(content = [u8], description = "BCS-encoded SignedMessage"),
    responses(
        (status = 200, description = "BCS encoded vector of slivers", body = [u8]),
        SyncShardServiceError
    ),
    tag = openapi::GROUP_SYNC_SHARD
)]
pub async fn sync_shard<S: SyncServiceState>(
    State(state): State<RestApiState<S>>,
    Authorization(public_key): Authorization,
    Bcs(signed_request): Bcs<SignedSyncShardRequest>,
) -> Result<Response, OrRejection<SyncShardServiceError>> {
    Ok(Bcs(state.service.sync_shard(public_key, signed_request).await?).into_response())
}

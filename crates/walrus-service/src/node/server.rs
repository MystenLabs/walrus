// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Server for the Walrus service.

use std::{net::SocketAddr, sync::Arc};

use axum::{
    extract::{DefaultBodyLimit, MatchedPath, State},
    routing::{get, post, put},
    Router,
};
use openapi::RestApiDoc;
use prometheus::{register_histogram_vec_with_registry, HistogramVec, Registry};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tower_http::trace::TraceLayer;
use utoipa::OpenApi as _;
use utoipa_redoc::{Redoc, Servable as _};
use walrus_core::encoding::max_sliver_size_for_n_shards;

use crate::{
    common::telemetry::{MakeHttpSpan, UNMATCHED_ROUTE},
    node::ServiceState,
};

mod extract;
mod openapi;
mod responses;
mod routes;

/// Additional space to be added to the maximum body size accepted by the server.
///
/// The maximum body size is set to be the maximum size of primary slivers, which contain at most
/// `n_secondary_source_symbols * u16::MAX` bytes. However, we need a few extra bytes to accommodate
/// the additional information encoded with the slivers.
const HEADROOM: usize = 128;

/// Represents a user server.
#[derive(Debug)]
pub struct UserServer<S> {
    state: Arc<S>,
    metrics: HistogramVec,
    cancel_token: CancellationToken,
}

impl<S> UserServer<S>
where
    S: ServiceState + Send + Sync + 'static,
{
    /// Creates a new user server.
    pub fn new(state: Arc<S>, cancel_token: CancellationToken, registry: &Registry) -> Self {
        Self {
            state,
            metrics: Self::register_http_metrics(registry),
            cancel_token,
        }
    }

    /// Creates a new user server.
    pub async fn run(&self, network_address: &SocketAddr) -> Result<(), std::io::Error> {
        let app = Router::new()
            .merge(Redoc::with_url(routes::API_DOCS, RestApiDoc::openapi()))
            .route(
                routes::METADATA_ENDPOINT,
                get(routes::get_metadata).put(routes::put_metadata),
            )
            .route(
                routes::SLIVER_ENDPOINT,
                put(routes::put_sliver)
                    .route_layer(DefaultBodyLimit::max(
                        usize::try_from(max_sliver_size_for_n_shards(self.state.n_shards()))
                            .expect("running on 64bit arch (see hardware requirements)")
                            + HEADROOM,
                    ))
                    .get(routes::get_sliver),
            )
            .route(
                routes::STORAGE_CONFIRMATION_ENDPOINT,
                get(routes::get_storage_confirmation),
            )
            .route(routes::RECOVERY_ENDPOINT, get(routes::get_recovery_symbol))
            .route(
                routes::INCONSISTENCY_PROOF_ENDPOINT,
                put(routes::inconsistency_proof),
            )
            .route(routes::BLOB_STATUS_ENDPOINT, get(routes::get_blob_status))
            .route(routes::HEALTH_ENDPOINT, get(routes::health_info))
            .route(routes::SYNC_SHARD_ENDPOINT, post(routes::sync_shard))
            .with_state(self.state.clone())
            // The following layers are executed from the bottom up
            .layer(
                TraceLayer::new_for_http()
                    .make_span_with(MakeHttpSpan::new())
                    .on_response(MakeHttpSpan::new()),
            )
            .layer(axum::middleware::from_fn_with_state(
                self.metrics.clone(),
                metrics_middleware,
            ))
            .into_make_service_with_connect_info::<SocketAddr>();

        let listener = tokio::net::TcpListener::bind(network_address).await?;
        axum::serve(listener, app)
            .with_graceful_shutdown(self.cancel_token.clone().cancelled_owned())
            .await
    }

    fn register_http_metrics(registry: &Registry) -> HistogramVec {
        let opts = prometheus::Opts::new(
            "request_duration_seconds",
            "Time (in seconds) spent serving HTTP requests.",
        )
        .namespace("http");

        register_histogram_vec_with_registry!(
            opts.into(),
            &["method", "route", "status_code"],
            registry
        )
        .expect("metric registration must not fail")
    }
}

async fn metrics_middleware(
    State(metrics): State<HistogramVec>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    // Manually record the time in seconds, since we do not yet know the status code which is
    // required to get the concrete histogram.
    let start = Instant::now();
    let method = request.method().clone();
    let route: String = if let Some(path) = request.extensions().get::<MatchedPath>() {
        path.as_str().into()
    } else {
        // We do not want to return the requested URI, as this would lead to a new histogram
        // for each rest to an invalid URI. Use a
        UNMATCHED_ROUTE.into()
    };

    let response = next.run(request).await;

    let histogram =
        metrics.with_label_values(&[method.as_str(), &route, response.status().as_str()]);
    histogram.observe(start.elapsed().as_secs_f64());

    response
}

#[cfg(test)]
mod test {
    use anyhow::anyhow;
    use axum::http::StatusCode;
    use fastcrypto::traits::KeyPair;
    use reqwest::Url;
    use tokio::{task::JoinHandle, time::Duration};
    use tokio_util::sync::CancellationToken;
    use walrus_core::{
        encoding::{EncodingAxis, Primary},
        inconsistency::{
            InconsistencyProof as InconsistencyProofInner,
            InconsistencyVerificationError,
        },
        keys::ProtocolKeyPair,
        merkle::MerkleProof,
        messages::{
            InvalidBlobIdAttestation,
            SignedMessage,
            StorageConfirmation,
            SyncShardMsg,
            SyncShardResponse,
        },
        metadata::{UnverifiedBlobMetadataWithId, VerifiedBlobMetadataWithId},
        BlobId,
        InconsistencyProof,
        PublicKey,
        RecoverySymbol,
        Sliver,
        SliverPairIndex,
        SliverType,
    };
    use walrus_sdk::{
        api::{
            BlobCertificationStatus as SdkBlobCertificationStatus,
            BlobStatus,
            ServiceHealthInfo,
            SliverStatus,
        },
        client::Client,
    };
    use walrus_sui::test_utils::event_id_for_testing;
    use walrus_test_utils::{async_param_test, WithTempDir};

    use super::*;
    use crate::{
        node::{
            config::StorageNodeConfig,
            BlobStatusError,
            ComputeStorageConfirmationError,
            InconsistencyProofError,
            RetrieveMetadataError,
            RetrieveSliverError,
            RetrieveSymbolError,
            StoreMetadataError,
            StoreSliverError,
            SyncShardError,
        },
        test_utils,
    };

    pub struct MockServiceState;

    impl ServiceState for MockServiceState {
        /// Returns a valid response only for blob IDs with the first byte 0, None for those
        /// starting with 1, and otherwise an error.
        fn retrieve_metadata(
            &self,
            blob_id: &BlobId,
        ) -> Result<VerifiedBlobMetadataWithId, RetrieveMetadataError> {
            if blob_id.0[0] == 0 {
                Ok(walrus_core::test_utils::verified_blob_metadata())
            } else if blob_id.0[0] == 1 {
                Err(RetrieveMetadataError::Unavailable)
            } else {
                Err(RetrieveMetadataError::Internal(anyhow::anyhow!(
                    "Invalid shard"
                )))
            }
        }

        fn store_metadata(
            &self,
            _metadata: UnverifiedBlobMetadataWithId,
        ) -> Result<bool, StoreMetadataError> {
            Ok(true)
        }

        fn retrieve_sliver(
            &self,
            _blob_id: &BlobId,
            _sliver_pair_index: SliverPairIndex,
            _sliver_type: SliverType,
        ) -> Result<Sliver, RetrieveSliverError> {
            Ok(walrus_core::test_utils::sliver())
        }

        /// Returns a valid response only for the pair index 0, otherwise, returns
        /// an internal error.
        fn retrieve_recovery_symbol(
            &self,
            _blob_id: &BlobId,
            sliver_pair_index: SliverPairIndex,
            _sliver_type: SliverType,
            _target_pair_index: SliverPairIndex,
        ) -> Result<RecoverySymbol<MerkleProof>, RetrieveSymbolError> {
            if sliver_pair_index == SliverPairIndex(0) {
                Ok(walrus_core::test_utils::recovery_symbol())
            } else {
                Err(RetrieveSliverError::Unavailable.into())
            }
        }

        /// Successful only for the pair index 0, otherwise, returns an internal error.
        fn store_sliver(
            &self,
            _blob_id: &BlobId,
            sliver_pair_index: SliverPairIndex,
            _sliver: &Sliver,
        ) -> Result<bool, StoreSliverError> {
            if sliver_pair_index.as_usize() == 0 {
                Ok(true)
            } else {
                Err(StoreSliverError::Internal(anyhow!("Invalid shard")))
            }
        }

        /// Returns a confirmation for blob ID starting with zero, None when starting with 1,
        /// and otherwise an error.
        async fn compute_storage_confirmation(
            &self,
            blob_id: &BlobId,
        ) -> Result<StorageConfirmation, ComputeStorageConfirmationError> {
            if blob_id.0[0] == 0 {
                let confirmation = walrus_core::test_utils::random_signed_message();
                Ok(StorageConfirmation::Signed(confirmation))
            } else if blob_id.0[0] == 1 {
                Err(ComputeStorageConfirmationError::NotFullyStored)
            } else {
                Err(anyhow::anyhow!("Invalid shard").into())
            }
        }

        /// Returns a "certified" blob status for blob ID starting with zero, `Nonexistent` when
        /// starting with 1, and otherwise an error.
        fn blob_status(&self, blob_id: &BlobId) -> Result<BlobStatus, BlobStatusError> {
            if blob_id.0[0] == 0 {
                Ok(BlobStatus::Existent {
                    end_epoch: 3,
                    status: SdkBlobCertificationStatus::Certified,
                    status_event: event_id_for_testing(),
                })
            } else if blob_id.0[0] == 1 {
                Ok(BlobStatus::Nonexistent)
            } else {
                Err(anyhow::anyhow!("Internal error").into())
            }
        }

        fn is_sliver_stored<A: EncodingAxis>(
            &self,
            blob_id: &BlobId,
            _sliver_pair_index: SliverPairIndex,
        ) -> Result<SliverStatus, RetrieveSliverError> {
            if blob_id.0[0] == 0 {
                Ok(SliverStatus::Stored)
            } else {
                Ok(SliverStatus::Nonexistent)
            }
        }

        /// Returns a signed invalid blob message for blob IDs starting with zero, a
        /// `MissingMetadata` error for IDs starting with 1, a `ProofVerificationError`
        /// for IDs starting with 2, and an internal error otherwise.
        async fn verify_inconsistency_proof(
            &self,
            blob_id: &BlobId,
            _inconsistency_proof: InconsistencyProof<MerkleProof>,
        ) -> Result<InvalidBlobIdAttestation, InconsistencyProofError> {
            match blob_id.0[0] {
                0 => Ok(walrus_core::test_utils::random_signed_message()),
                1 => Err(InconsistencyProofError::MissingMetadata),
                2 => Err(InconsistencyProofError::InvalidProof(
                    InconsistencyVerificationError::SliverNotInconsistent,
                )),
                _ => Err(anyhow!("internal error").into()),
            }
        }

        fn n_shards(&self) -> std::num::NonZeroU16 {
            walrus_core::test_utils::encoding_config().n_shards()
        }

        fn health_info(&self) -> ServiceHealthInfo {
            ServiceHealthInfo {
                uptime: Duration::from_secs(0),
                epoch: 0,
                public_key: ProtocolKeyPair::generate().as_ref().public().clone(),
            }
        }

        fn sync_shard(
            &self,
            _public_key: PublicKey,
            _signed_request: SignedMessage<SyncShardMsg>,
        ) -> Result<SyncShardResponse, SyncShardError> {
            Ok(SyncShardResponse::V1(vec![]))
        }
    }

    async fn start_rest_api_with_config(
        config: &StorageNodeConfig,
    ) -> JoinHandle<Result<(), std::io::Error>> {
        let server = UserServer::new(
            Arc::new(MockServiceState),
            CancellationToken::new(),
            &Registry::new(),
        );
        let network_address = config.rest_api_address;
        let handle = tokio::spawn(async move { server.run(&network_address).await });

        tokio::task::yield_now().await;
        handle
    }

    async fn start_rest_api_with_test_config() -> (
        WithTempDir<StorageNodeConfig>,
        JoinHandle<Result<(), std::io::Error>>,
    ) {
        let config = test_utils::storage_node_config();
        let handle = start_rest_api_with_config(config.as_ref()).await;
        (config, handle)
    }

    fn storage_node_client(config: &StorageNodeConfig) -> Client {
        let network_address = config.rest_api_address;
        let url = Url::parse(&format!("http://{network_address}")).unwrap();

        // Do not load any proxy information from the system, as it's slow (at least on MacOs).
        let inner = reqwest::Client::builder()
            .no_proxy()
            .http2_prior_knowledge()
            .build()
            .unwrap();

        Client::from_url(url, inner)
    }

    fn blob_id_for_valid_response() -> BlobId {
        let mut blob_id = walrus_core::test_utils::random_blob_id();
        blob_id.0[0] = 0; // Triggers a valid response
        blob_id
    }

    fn blob_id_for_nonexistent() -> BlobId {
        let mut blob_id = walrus_core::test_utils::random_blob_id();
        blob_id.0[0] = 1; // Triggers a not found response
        blob_id
    }

    fn blob_id_for_bad_request() -> BlobId {
        let mut blob_id = walrus_core::test_utils::random_blob_id();
        blob_id.0[0] = 2; // Triggers a bad request error.
        blob_id
    }

    fn blob_id_for_internal_server_error() -> BlobId {
        let mut blob_id = walrus_core::test_utils::random_blob_id();
        blob_id.0[0] = 255; // Triggers an internal server error.
        blob_id
    }

    #[tokio::test]
    async fn retrieve_metadata() {
        let (config, _handle) = start_rest_api_with_test_config().await;
        let client = storage_node_client(config.as_ref());

        let blob_id = blob_id_for_valid_response();
        let _metadata = client
            .get_metadata(&blob_id)
            .await
            .expect("should successfully return metadata");
    }

    #[tokio::test]
    async fn retrieve_metadata_not_found() {
        let (config, _handle) = start_rest_api_with_test_config().await;
        let client = storage_node_client(config.as_ref());

        let blob_id = blob_id_for_nonexistent();
        let err = client
            .get_metadata(&blob_id)
            .await
            .expect_err("metadata request mut fail");

        assert_eq!(err.http_status_code(), Some(StatusCode::NOT_FOUND));
    }

    #[tokio::test]
    async fn retrieve_metadata_internal_error() {
        let (config, _handle) = start_rest_api_with_test_config().await;
        let client = storage_node_client(config.as_ref());

        let blob_id = blob_id_for_internal_server_error();
        let err = client
            .get_metadata(&blob_id)
            .await
            .expect_err("metadata request must fail");

        assert_eq!(
            err.http_status_code(),
            Some(StatusCode::INTERNAL_SERVER_ERROR)
        );
    }

    #[tokio::test]
    async fn get_blob_status() {
        let (config, _handle) = start_rest_api_with_test_config().await;
        let client = storage_node_client(config.as_ref());

        let blob_id = blob_id_for_valid_response();
        let _blob_status = client
            .get_blob_status(&blob_id)
            .await
            .expect("should successfully return blob status");
    }

    #[tokio::test]
    async fn get_blob_status_nonexistent() {
        let (config, _handle) = start_rest_api_with_test_config().await;
        let client = storage_node_client(config.as_ref());

        let blob_id = blob_id_for_nonexistent();
        let result = client
            .get_blob_status(&blob_id)
            .await
            .expect("blob status request must not fail");

        assert_eq!(result, BlobStatus::Nonexistent);
    }

    #[tokio::test]
    async fn get_blob_status_internal_error() {
        let (config, _handle) = start_rest_api_with_test_config().await;
        let client = storage_node_client(config.as_ref());

        let blob_id = blob_id_for_internal_server_error();
        let err = client
            .get_blob_status(&blob_id)
            .await
            .expect_err("blob status request must fail");

        assert_eq!(
            err.http_status_code(),
            Some(StatusCode::INTERNAL_SERVER_ERROR)
        );
    }

    #[tokio::test]
    async fn store_metadata() {
        let (config, _handle) = start_rest_api_with_test_config().await;

        let metadata_with_blob_id = walrus_core::test_utils::unverified_blob_metadata();
        let metadata = metadata_with_blob_id.metadata();

        let blob_id = metadata_with_blob_id.blob_id().to_string();
        let path = routes::METADATA_ENDPOINT.replace(":blob_id", &blob_id);
        let url = format!("http://{}{path}", config.as_ref().rest_api_address);

        let client = storage_node_client(config.as_ref()).into_inner();
        let res = client
            .put(url)
            .body(bcs::to_bytes(metadata).unwrap())
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn retrieve_sliver() {
        let (config, _handle) = start_rest_api_with_test_config().await;
        let client = storage_node_client(config.as_ref());

        let blob_id = walrus_core::test_utils::random_blob_id();
        let sliver_pair_id = SliverPairIndex(0); // Triggers an valid response

        let _sliver = client
            .get_sliver::<Primary>(&blob_id, sliver_pair_id)
            .await
            .expect("should successfully retrieve sliver");
    }

    #[tokio::test]
    async fn store_sliver() {
        let (config, _handle) = start_rest_api_with_test_config().await;
        let client = storage_node_client(config.as_ref());

        let sliver = walrus_core::test_utils::sliver();

        let blob_id = walrus_core::test_utils::random_blob_id();
        let sliver_pair_id = SliverPairIndex(0); // Triggers an ok response

        client
            .store_sliver_by_type(&blob_id, sliver_pair_id, &sliver)
            .await
            .expect("sliver should be successfully stored");
    }

    #[tokio::test]
    async fn store_sliver_error() {
        let (config, _handle) = start_rest_api_with_test_config().await;
        let client = storage_node_client(config.as_ref());

        let blob_id = walrus_core::test_utils::random_blob_id();
        let sliver = walrus_core::test_utils::sliver();
        let sliver_pair_id = SliverPairIndex(1); // Triggers an internal server error

        let err = client
            .store_sliver_by_type(&blob_id, sliver_pair_id, &sliver)
            .await
            .expect_err("store sliver should fail");

        assert_eq!(
            err.http_status_code(),
            Some(StatusCode::INTERNAL_SERVER_ERROR)
        );
    }

    #[tokio::test]
    async fn retrieve_storage_confirmation() {
        let (config, _handle) = start_rest_api_with_test_config().await;
        let client = storage_node_client(config.as_ref());

        let blob_id = blob_id_for_valid_response();
        let _confirmation = client
            .get_confirmation(&blob_id)
            .await
            .expect("should return a signed confirmation");
    }

    async_param_test! {
        retrieve_storage_confirmation_fails: [
            not_found: (blob_id_for_nonexistent(), StatusCode::NOT_FOUND),
            internal_error: (blob_id_for_internal_server_error(), StatusCode::INTERNAL_SERVER_ERROR)
        ]
    }
    async fn retrieve_storage_confirmation_fails(blob_id: BlobId, code: StatusCode) {
        let (config, _handle) = start_rest_api_with_test_config().await;
        let client = storage_node_client(config.as_ref());

        let err = client
            .get_confirmation(&blob_id)
            .await
            .expect_err("confirmation request should fail");

        assert_eq!(err.http_status_code(), Some(code));
    }

    #[tokio::test]
    async fn inconsistency_proof() {
        let (config, _handle) = start_rest_api_with_test_config().await;
        let client = storage_node_client(config.as_ref());

        let (_encoding_config, _metadata, target_sliver_index, recovery_symbols) =
            walrus_core::test_utils::generate_config_metadata_and_valid_recovery_symbols()
                .expect("generating metadata and recovery symbols not to fail");
        let inconsistency_proof = InconsistencyProof::Primary(InconsistencyProofInner::new(
            target_sliver_index,
            recovery_symbols,
        ));

        client
            .submit_inconsistency_proof_by_type(&blob_id_for_valid_response(), &inconsistency_proof)
            .await
            .expect("should return a signed blob invalid message");
    }

    async_param_test! {
        inconsistency_proof_fails: [
            not_found: (blob_id_for_nonexistent(), StatusCode::NOT_FOUND),
            invalid_proof: (blob_id_for_bad_request(), StatusCode::BAD_REQUEST),
            internal_error: (blob_id_for_internal_server_error(), StatusCode::INTERNAL_SERVER_ERROR)
        ]
    }
    async fn inconsistency_proof_fails(blob_id: BlobId, code: StatusCode) {
        let (config, _handle) = start_rest_api_with_test_config().await;
        let client = storage_node_client(config.as_ref());

        let (_encoding_config, _metadata, target_sliver_index, recovery_symbols) =
            walrus_core::test_utils::generate_config_metadata_and_valid_recovery_symbols()
                .expect("generating metadata and recovery symbols not to fail");
        let inconsistency_proof = InconsistencyProof::Primary(InconsistencyProofInner::new(
            target_sliver_index,
            recovery_symbols,
        ));

        let err = client
            .submit_inconsistency_proof_by_type(&blob_id, &inconsistency_proof)
            .await
            .expect_err("confirmation request should fail");

        assert_eq!(err.http_status_code(), Some(code));
    }

    #[tokio::test]
    async fn shutdown_server() {
        let cancel_token = CancellationToken::new();
        let server = UserServer::new(
            Arc::new(MockServiceState),
            cancel_token.clone(),
            &Registry::new(),
        );
        let config = test_utils::storage_node_config();
        let handle = tokio::spawn(async move {
            let network_address = config.as_ref().rest_api_address;
            server.run(&network_address).await
        });

        cancel_token.cancel();
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn get_decoding_symbol() {
        let (config, _handle) = start_rest_api_with_test_config().await;
        let client = storage_node_client(config.as_ref());

        let blob_id = walrus_core::test_utils::random_blob_id();
        let sliver_pair_at_remote = SliverPairIndex(0); // Triggers an valid response
        let intersecting_pair_index = SliverPairIndex(0);

        let _symbol = client
            .get_recovery_symbol::<Primary>(
                &blob_id,
                sliver_pair_at_remote,
                intersecting_pair_index,
            )
            .await
            .expect("request should succeed");
    }

    #[tokio::test]
    async fn decoding_symbol_not_found() {
        let (config, _handle) = start_rest_api_with_test_config().await;
        let client = storage_node_client(config.as_ref());

        let sliver_pair_id = SliverPairIndex(1); // Triggers a not found response
        let blob_id = walrus_core::test_utils::random_blob_id();
        let Err(err) = client
            .get_recovery_symbol::<Primary>(&blob_id, sliver_pair_id, SliverPairIndex(0))
            .await
        else {
            panic!("must return an error for pair-id 1");
        };
        dbg!(&err);

        assert_eq!(err.http_status_code(), Some(StatusCode::NOT_FOUND));
    }
}

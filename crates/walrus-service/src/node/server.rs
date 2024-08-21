// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Server for the Walrus service.

use std::{net::SocketAddr, ops::Deref, path::PathBuf, sync::Arc, time::Duration};

use axum::{
    extract::{DefaultBodyLimit, MatchedPath, State},
    middleware,
    routing::{get, post, put},
    Router,
};
use axum_server::{tls_rustls::RustlsConfig, Handle};
use fastcrypto::{secp256r1::Secp256r1PrivateKey, traits::ToFromBytes};
use futures::{future::Either, FutureExt};
use openapi::RestApiDoc;
use p256::{elliptic_curve::pkcs8::EncodePrivateKey as _, SecretKey};
use prometheus::{register_histogram_vec_with_registry, HistogramVec, Registry};
use rcgen::{Certificate as RcGenCertificate, CertificateParams, DnType, KeyPair as RcGenKeyPair};
use tokio::{sync::Mutex, time::Instant};
use tokio_util::sync::CancellationToken;
use tower::ServiceBuilder;
use tower_http::trace::TraceLayer;
use tracing::Instrument;
use utoipa::OpenApi as _;
use utoipa_redoc::{Redoc, Servable as _};
use walrus_core::{
    encoding::max_sliver_size_for_n_shards,
    keys::NetworkKeyPair,
    server_name_from_public_key,
};

use super::config::{defaults, StorageNodeConfig};
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

/// Configuration for the rest API.
#[derive(Debug)]
pub struct UserServerConfig {
    /// The socket address on which the server should listen.
    pub bind_address: SocketAddr,

    /// Source of the TLS certificate used to secure connections.
    ///
    /// If None, TLS will be disabled and only HTTP will be used. However, clients *always* connect
    /// via HTTPS and so this should only be None if middleware is terminating the TLS connection
    /// and TLS is not possible between the middleware and the server.
    pub tls_certificate: Option<TlsCertificateSource>,

    /// Duration for which to wait for connections to close, when shutting down the server.
    ///
    /// Zero waits indefinitely and None immediately closes the connections.
    pub graceful_shutdown_period: Option<Duration>,
}

impl From<&StorageNodeConfig> for UserServerConfig {
    fn from(config: &StorageNodeConfig) -> Self {
        let tls_certificate = if config.tls.disable_tls {
            None
        } else if let Some(paths) = config.tls.pem_files.clone() {
            Some(TlsCertificateSource::PemFiles {
                certificate_path: paths.certificate_path,
                key_path: paths.key_path,
            })
        } else {
            let server_name = config
                .tls
                .server_name
                .clone()
                .unwrap_or_else(|| config.rest_api_address.ip().to_string());

            Some(TlsCertificateSource::GenerateSelfSigned {
                server_name,
                network_key_pair: config.network_key_pair.get().cloned().unwrap(),
            })
        };

        let graceful_shutdown_period = config
            .rest_graceful_shutdown_period_secs
            .unwrap_or(Some(defaults::REST_GRACEFUL_SHUTDOWN_PERIOD_SECS))
            .map(Duration::from_secs);

        UserServerConfig {
            bind_address: config.rest_api_address,
            tls_certificate,
            graceful_shutdown_period,
        }
    }
}

/// Source of the TLS private key and certificate.
#[derive(Debug)]
pub enum TlsCertificateSource {
    /// Load PEM encoded x509 certificate and a PKCS8 encoded private key from the specified paths.
    ///
    /// These ideally should be certificates issued to by a public CA such as Let's Encrypt,
    /// but can also be self-signed certificates.
    // TODO(jsmith): Do we need to occasionally reload this?
    PemFiles {
        /// Path to the x509 PEM encoded certificate.
        certificate_path: PathBuf,
        /// Path to the PEM encoded private key in PKCS8
        ///
        /// The private key should correspond to the
        /// [`NetworkPublicKey`][walrus_core::NetworkPublicKey] published on chain in the
        /// committee.
        key_path: PathBuf,
    },

    /// Generate a self-signed certificate from the provided network key pair.
    ///
    /// This is a convenience for when the storage node has no publicly acceptable certificate
    /// issued, and only allows communication between storage nodes (or other clients accepting
    /// connections verifiable with the key presented on chain).
    GenerateSelfSigned {
        /// The server name (DNS name or IP address) to be placed in the certificate.
        server_name: String,
        /// The network key pair used to create the self-signed certificate.
        network_key_pair: NetworkKeyPair,
    },
}

/// Represents a user server.
#[derive(Debug)]
// TODO(jsmith):  Rename to something more appropriate (#710)
pub struct UserServer<S> {
    state: Arc<S>,
    config: UserServerConfig,
    metrics: HistogramVec,
    cancel_token: CancellationToken,
    handle: Mutex<Option<Handle>>,
}

impl<S> UserServer<S>
where
    S: ServiceState + Send + Sync + 'static,
{
    /// Creates a new user server.
    pub fn new(
        state: Arc<S>,
        cancel_token: CancellationToken,
        config: UserServerConfig,
        registry: &Registry,
    ) -> Self {
        Self {
            state,
            metrics: Self::register_http_metrics(registry),
            cancel_token,
            handle: Default::default(),
            config,
        }
    }

    /// Runs the server, may only be called once for a given instance.
    pub async fn run(&self) -> Result<(), std::io::Error> {
        {
            let handle = self.handle.lock().await;
            assert!(handle.is_none(), "run can only be called once");
        }

        let request_layers = ServiceBuilder::new()
            .layer(middleware::from_fn_with_state(
                self.metrics.clone(),
                metrics_middleware,
            ))
            .layer(
                TraceLayer::new_for_http()
                    .make_span_with(MakeHttpSpan::new())
                    .on_response(MakeHttpSpan::new()),
            );

        let app = self
            .define_routes()
            .with_state(self.state.clone())
            .layer(request_layers)
            .into_make_service_with_connect_info::<SocketAddr>();

        let handle = self.init_handle().await;
        let server = if let Some(tls_config) = self.configure_tls().await? {
            Either::Left(
                axum_server::bind_rustls(self.config.bind_address, tls_config)
                    .handle(handle.clone())
                    .serve(app),
            )
        } else {
            Either::Right(
                axum_server::bind(self.config.bind_address)
                    .handle(handle.clone())
                    .serve(app),
            )
        };

        tokio::spawn(
            Self::handle_shutdown_signal(
                handle,
                self.cancel_token.clone(),
                self.config.graceful_shutdown_period,
            )
            .in_current_span(),
        );

        server
            .inspect(|_| tracing::info!("server run has completed"))
            .await
    }

    async fn handle_shutdown_signal(
        handle: Handle,
        cancel_token: CancellationToken,
        shutdown_duration: Option<Duration>,
    ) {
        cancel_token.cancelled().await;

        match shutdown_duration {
            Some(Duration::ZERO) => {
                tracing::info!("immediately shutting down server");
                handle.shutdown();
            }
            Some(duration) => {
                tracing::info!("gracefully shutting down server in {:?}", duration);
                handle.graceful_shutdown(Some(duration));
            }
            None => {
                tracing::info!("waiting for all connections to close before shutting down server");
                handle.graceful_shutdown(None);
            }
        }

        handle.graceful_shutdown(shutdown_duration);
    }

    async fn init_handle(&self) -> Handle {
        let new_handle = Handle::new();
        let mut handle = self.handle.lock().await;
        *handle = Some(new_handle.clone());
        new_handle
    }

    async fn configure_tls(&self) -> Result<Option<RustlsConfig>, std::io::Error> {
        let Some(ref tls_certificate) = self.config.tls_certificate else {
            return Ok(None);
        };

        match tls_certificate {
            TlsCertificateSource::PemFiles {
                certificate_path,
                key_path,
            } => RustlsConfig::from_pem_file(certificate_path, key_path)
                .await
                .map(Some),
            TlsCertificateSource::GenerateSelfSigned {
                server_name,
                network_key_pair,
            } => {
                let (certificate, encoded_key_pair) =
                    create_self_signed_certificate(network_key_pair, server_name.to_string());
                let tls_config = RustlsConfig::from_der(
                    vec![Vec::from(certificate.der().deref())],
                    encoded_key_pair.serialize_der(),
                )
                .await
                .expect("self signed certificate to result in valid config");
                Ok(Some(tls_config))
            }
        }
    }

    #[cfg(test)]
    async fn ready(&self) {
        let handle = loop {
            let handle = self.handle.lock().await;
            if handle.is_none() {
                drop(handle);
                tokio::time::sleep(Duration::from_millis(5)).await;
            } else {
                break handle;
            }
        };
        // Returns only once bind has been completed
        let _ = handle.as_ref().unwrap().listening().await;
    }

    fn define_routes(&self) -> Router<Arc<S>> {
        Router::new()
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
    next: middleware::Next,
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

fn create_self_signed_certificate(
    key_pair: &NetworkKeyPair,
    public_server_name: String,
) -> (RcGenCertificate, RcGenKeyPair) {
    let generated_server_name = server_name_from_public_key(key_pair.public());
    let pkcs8_key_pair = to_pkcs8_key_pair(key_pair);

    let mut params =
        CertificateParams::new(vec![generated_server_name.clone(), public_server_name])
            .expect("valid subject_alt_names");
    params
        .distinguished_name
        .push(DnType::CommonName, generated_server_name.clone());
    let certificate = params
        .self_signed(&pkcs8_key_pair)
        .expect("self-signing certificate must not fail");

    (certificate, pkcs8_key_pair)
}

fn to_pkcs8_key_pair(keypair: &NetworkKeyPair) -> RcGenKeyPair {
    let secret_key: SecretKey = Secp256r1PrivateKey::from_bytes(keypair.as_ref().as_bytes())
        .expect("encode-decode of private key must not fail")
        .privkey
        .clone()
        .into();
    let document = secret_key.to_pkcs8_der().expect("valid keypair");
    RcGenKeyPair::try_from(document.as_bytes()).expect("constructed keypair is valid")
}

#[cfg(test)]
mod test {
    use anyhow::anyhow;
    use axum::http::StatusCode;
    use fastcrypto::traits::KeyPair;
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
    use walrus_test_utils::{async_param_test, Result as TestResult, WithTempDir};

    use super::*;
    use crate::{
        node::{
            config::{StorageNodeConfig, TlsCertificateAndKey},
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
        let rest_api_config = UserServerConfig::from(config);

        let server = UserServer::new(
            Arc::new(MockServiceState),
            CancellationToken::new(),
            rest_api_config,
            &Registry::new(),
        );
        let server = Arc::new(server);
        let server_copy = server.clone();
        let handle = tokio::spawn(async move { server.run().await });

        server_copy.ready().await;
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
        let network_public_key = config.network_key_pair.get().unwrap().public().clone();

        Client::builder()
            .authenticate_with_public_key(network_public_key)
            .no_proxy()
            .build(&network_address.ip().to_string(), network_address.port())
            .expect("must be able to construct client in tests")
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
        let url = format!("https://{}{path}", config.as_ref().rest_api_address);

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
        let config = test_utils::storage_node_config();
        let server = UserServer::new(
            Arc::new(MockServiceState),
            cancel_token.clone(),
            config.as_ref().into(),
            &Registry::new(),
        );
        let handle = tokio::spawn(async move { server.run().await });

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

        assert_eq!(err.http_status_code(), Some(StatusCode::NOT_FOUND));
    }

    #[tokio::test]
    async fn client_fails_when_tls_disabled() -> TestResult {
        let mut config = test_utils::storage_node_config();
        config.as_mut().tls.disable_tls = true;

        start_rest_api_with_config(config.as_ref()).await;
        let client = storage_node_client(config.as_ref());

        let err = client
            .get_server_health_info()
            .await
            .expect_err("should fail to get health info since TLS is disabled");

        assert!(
            err.is_connect(),
            "error must be due to a failure to connect"
        );

        Ok(())
    }

    #[tokio::test]
    async fn client_accepts_self_signed_certificate() -> TestResult {
        let mut config = test_utils::storage_node_config();

        // Enable self-signed certificates by enabling TLS but removing any certificate.
        config.as_mut().tls.disable_tls = false;
        config.as_mut().tls.pem_files = None;

        start_rest_api_with_config(config.as_ref()).await;
        let client = storage_node_client(config.as_ref());

        let _info = client
            .get_server_health_info()
            .await
            .expect("must successfully fetch health info with TLS");

        Ok(())
    }

    #[tokio::test]
    async fn server_loads_certificates_from_pem() -> TestResult {
        let mut config_with_dir = test_utils::storage_node_config();
        let directory = config_with_dir.temp_dir.path().to_path_buf();
        let config = config_with_dir.as_mut();

        let (certificate, key_pair) = create_self_signed_certificate(
            config.network_key_pair.get().unwrap(),
            config.rest_api_address.ip().to_string(),
        );

        let certificate_path = directory.join("certificate.pem");
        std::fs::write(&certificate_path, certificate.pem().as_bytes())?;
        let key_path = directory.join("private_key.pem");
        std::fs::write(&key_path, key_pair.serialize_pem().as_bytes())?;

        config.tls.disable_tls = false;
        config.tls.pem_files = Some(TlsCertificateAndKey {
            certificate_path,
            key_path,
        });

        start_rest_api_with_config(config).await;
        let client = storage_node_client(config);

        let _info = client
            .get_server_health_info()
            .await
            .expect("must successfully fetch health info with TLS");

        Ok(())
    }
}

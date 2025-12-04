// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Server for the Walrus service.

use std::{net::SocketAddr, ops::Deref, sync::Arc, time::Duration};

use anyhow::{Context, anyhow};
use axum::{
    Router,
    extract::DefaultBodyLimit,
    middleware,
    routing::{get, post, put},
};
use axum_server::{Handle, tls_rustls::RustlsConfig};
use fastcrypto::{secp256r1::Secp256r1PrivateKey, traits::ToFromBytes};
use futures::{FutureExt, future::Either};
use openapi::RestApiDoc;
use p256::{SecretKey, elliptic_curve::pkcs8::EncodePrivateKey as _};
use rcgen::{CertificateParams, CertifiedKey, DnType, KeyPair as RcGenKeyPair};
use tokio::sync::{Mutex, Semaphore};
use tokio_util::sync::CancellationToken;
use tower::ServiceBuilder;
use tower_http::{
    cors::{Any, CorsLayer},
    trace::TraceLayer,
};
use tracing::Instrument as _;
use utoipa::OpenApi as _;
use utoipa_redoc::{Redoc, Servable as _};
use walrus_core::{encoding, keys::NetworkKeyPair};
use walrus_utils::metrics::Registry;
use x509_cert::{Certificate, der::Decode};

use self::telemetry::MetricsMiddlewareState;
use super::config::{Http2Config, PathOrInPlace, StorageNodeConfig, TlsConfig, defaults};
use crate::{
    common::telemetry::{self, MakeHttpSpan},
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
pub struct RestApiConfig {
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

    /// Configuration of HTTP/2 connections.
    pub http2_config: Http2Config,

    /// Limit on the number of active recovery symbol requests.
    pub max_active_recovery_symbols_requests: Option<usize>,

    /// Live-upload based deferral policy.
    pub live_upload_deferral: crate::node::config::LiveUploadDeferralConfig,
}

impl From<&StorageNodeConfig> for RestApiConfig {
    fn from(config: &StorageNodeConfig) -> Self {
        let tls_certificate = match config.tls {
            TlsConfig {
                disable_tls: true, ..
            } => None,

            TlsConfig {
                certificate_path: Some(ref path),
                ..
            } => Some(TlsCertificateSource::Pem {
                certificate: PathOrInPlace::from_path(path),
                key: match config.network_key_pair {
                    PathOrInPlace::InPlace(ref value) => {
                        PathOrInPlace::InPlace(value.to_pem().as_bytes().to_vec())
                    }
                    PathOrInPlace::Path { ref path, .. } => PathOrInPlace::from_path(path),
                },
            }),

            _ => Some(TlsCertificateSource::GenerateSelfSigned {
                server_name: config.public_host.clone(),
                network_key_pair: config.network_key_pair().clone(),
            }),
        };

        let graceful_shutdown_period = config
            .rest_graceful_shutdown_period_secs
            .unwrap_or(Some(defaults::REST_GRACEFUL_SHUTDOWN_PERIOD_SECS))
            .map(Duration::from_secs);

        RestApiConfig {
            bind_address: config.rest_api_address,
            tls_certificate,
            graceful_shutdown_period,
            http2_config: config.rest_server.http2_config.clone(),
            max_active_recovery_symbols_requests: config
                .rest_server
                .experimental_max_active_recovery_symbols_requests,
            live_upload_deferral: config.live_upload_deferral.clone(),
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
    // TODO(jsmith): Reload the certificate on change (#709)
    Pem {
        /// Path to the x509 PEM encoded certificate.
        certificate: PathOrInPlace<Vec<u8>>,
        /// Path to the PEM encoded private key in PKCS8
        ///
        /// The private key should correspond to the
        /// [`NetworkPublicKey`][walrus_core::NetworkPublicKey] published on chain in the
        /// committee.
        key: PathOrInPlace<Vec<u8>>,
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

#[derive(Debug)]
pub(crate) struct RestApiState<S> {
    service: Arc<S>,
    config: Arc<RestApiConfig>,
    recovery_symbols_limit: Option<Arc<Semaphore>>,
}

impl<S> RestApiState<S> {
    fn new(service: Arc<S>, config: Arc<RestApiConfig>) -> Self {
        Self {
            service,
            recovery_symbols_limit: config
                .max_active_recovery_symbols_requests
                .map(|limit| Arc::new(Semaphore::new(limit))),
            config,
        }
    }
}

impl<S> Clone for RestApiState<S> {
    fn clone(&self) -> Self {
        Self {
            service: self.service.clone(),
            config: self.config.clone(),
            recovery_symbols_limit: self.recovery_symbols_limit.clone(),
        }
    }
}

/// Represents a server for the Walrus REST API.
#[derive(Debug)]
pub struct RestApiServer<S> {
    state: RestApiState<S>,
    metrics: MetricsMiddlewareState,
    cancel_token: CancellationToken,
    handle: Mutex<Option<Handle>>,
}

impl<S> RestApiServer<S>
where
    S: ServiceState + Send + Sync + 'static,
{
    /// Creates a new REST API server.
    pub fn new(
        service: Arc<S>,
        cancel_token: CancellationToken,
        config: RestApiConfig,
        registry: &Registry,
    ) -> Self {
        Self {
            state: RestApiState::new(service, Arc::new(config)),
            metrics: MetricsMiddlewareState::new(registry),
            cancel_token,
            handle: Default::default(),
        }
    }

    /// Runs the server, may only be called once for a given instance.
    pub async fn run(&self) -> Result<(), anyhow::Error> {
        {
            let handle = self.handle.lock().await;
            assert!(handle.is_none(), "run can only be called once");
        }

        let request_layers = ServiceBuilder::new()
            .layer(middleware::from_fn_with_state(
                self.metrics.clone(),
                telemetry::metrics_middleware,
            ))
            .layer(
                TraceLayer::new_for_http()
                    .make_span_with(MakeHttpSpan::new())
                    // TODO(jsmith): This silences logs being emitted by TraceLayer.
                    // Until we've resolved too may 503's or customized this further to not log
                    // specifically this error, we disable it.
                    .on_failure(())
                    .on_response(MakeHttpSpan::new()),
            )
            .layer(Self::cors_layer());

        let app = self
            .define_routes()
            .with_state(self.state.clone())
            .layer(request_layers)
            .into_make_service_with_connect_info::<SocketAddr>();

        let handle = self.init_handle().await;

        let server = if let Some(tls_config) = self.configure_tls().await? {
            let server = axum_server::bind_rustls(self.config().bind_address, tls_config)
                .handle(handle.clone());
            Either::Left(self.configure_server(server).serve(app))
        } else {
            let server = axum_server::bind(self.config().bind_address).handle(handle.clone());
            Either::Right(self.configure_server(server).serve(app))
        };

        tokio::spawn(
            Self::handle_shutdown_signal(
                handle,
                self.cancel_token.clone(),
                self.config().graceful_shutdown_period,
            )
            .in_current_span(),
        );

        server
            .inspect(|_| tracing::info!("server run has completed"))
            .await
            .map_err(|error| anyhow!(error))
    }

    fn configure_server<A>(&self, mut server: axum_server::Server<A>) -> axum_server::Server<A> {
        let config = &self.config().http2_config;
        let mut http2_builder = server.http_builder().http2();
        http2_builder
            .max_concurrent_streams(config.http2_max_concurrent_streams)
            .max_pending_accept_reset_streams(config.http2_max_pending_accept_reset_streams)
            .initial_connection_window_size(config.http2_initial_connection_window_size)
            .initial_stream_window_size(config.http2_initial_stream_window_size)
            .adaptive_window(config.http2_adaptive_window);
        server
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

    async fn configure_tls(&self) -> Result<Option<RustlsConfig>, anyhow::Error> {
        let Some(ref tls_certificate) = self.config().tls_certificate else {
            return Ok(None);
        };

        let (tls_config, certificate) = match tls_certificate {
            TlsCertificateSource::Pem { certificate, key } => {
                configure_tls_from_pem(certificate, key).await?
            }

            TlsCertificateSource::GenerateSelfSigned {
                server_name,
                network_key_pair,
            } => configure_self_signed_tls(server_name, network_key_pair).await?,
        };

        self.metrics.set_tls_certificate_expiration_time(
            certificate
                .tbs_certificate
                .validity
                .not_after
                .to_unix_duration(),
        );

        Ok(Some(tls_config))
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
        let _ = handle
            .as_ref()
            .expect("we only break if the handle is some")
            .listening()
            .await;
    }

    fn define_routes(&self) -> Router<RestApiState<S>> {
        Router::new()
            .merge(Redoc::with_url(
                routes::API_DOCS_ENDPOINT,
                RestApiDoc::openapi(),
            ))
            .route(
                routes::METADATA_ENDPOINT,
                get(routes::get_metadata).put(routes::put_metadata),
            )
            .route(
                routes::METADATA_STATUS_ENDPOINT,
                get(routes::get_metadata_status),
            )
            .route(
                routes::SLIVER_ENDPOINT,
                put(routes::put_sliver)
                    .route_layer(DefaultBodyLimit::max(
                        usize::try_from(encoding::max_sliver_size_for_n_shards(
                            self.state.service.n_shards(),
                        ))
                        .expect("running on 64bit arch (see hardware requirements)")
                            + HEADROOM,
                    ))
                    .get(routes::get_sliver),
            )
            .route(
                routes::SLIVER_STATUS_ENDPOINT,
                get(routes::get_sliver_status),
            )
            .route(
                routes::PERMANENT_BLOB_CONFIRMATION_ENDPOINT,
                get(routes::get_permanent_blob_confirmation),
            )
            .route(
                routes::DELETABLE_BLOB_CONFIRMATION_ENDPOINT,
                get(routes::get_deletable_blob_confirmation),
            )
            .route(
                routes::RECOVERY_SYMBOL_ENDPOINT,
                get(routes::get_recovery_symbol_by_id),
            )
            .route(
                routes::RECOVERY_SYMBOL_LIST_ENDPOINT,
                get(routes::list_recovery_symbols),
            )
            .route(
                routes::INCONSISTENCY_PROOF_ENDPOINT,
                post(routes::inconsistency_proof),
            )
            .route(routes::BLOB_STATUS_ENDPOINT, get(routes::get_blob_status))
            .route(routes::HEALTH_ENDPOINT, get(routes::health_info))
            .route(routes::SYNC_SHARD_ENDPOINT, post(routes::sync_shard))
    }

    /// Returns the CORS leayer for the server.
    fn cors_layer() -> CorsLayer {
        CorsLayer::new()
            .allow_origin(Any)
            .allow_methods(Any)
            .allow_headers(Any)
    }

    fn config(&self) -> &RestApiConfig {
        &self.state.config
    }
}

async fn configure_self_signed_tls(
    server_name: &str,
    network_key_pair: &NetworkKeyPair,
) -> Result<(RustlsConfig, Certificate), anyhow::Error> {
    let certified_key_pair =
        create_self_signed_certificate(network_key_pair, server_name.to_string());
    let certificate_der = Vec::from(certified_key_pair.cert.der().deref());

    let tls_config = RustlsConfig::from_der(
        vec![certificate_der.clone()],
        certified_key_pair.key_pair.serialize_der(),
    )
    .await
    .expect("self signed certificate to result in valid config");

    let certificate = x509_cert::Certificate::from_der(&certificate_der)
        .expect("self-signed certificate should be valid DER");

    Ok((tls_config, certificate))
}

async fn configure_tls_from_pem(
    certificate: &PathOrInPlace<Vec<u8>>,
    key: &PathOrInPlace<Vec<u8>>,
) -> Result<(RustlsConfig, Certificate), anyhow::Error> {
    let certificate_pem = certificate
        .load_transient()
        .context("failed to load TLS PEM certificate")?;

    let key_pem = key
        .load_transient()
        .context("failed to load TLS private key")?;

    let tls_config = RustlsConfig::from_pem(certificate_pem.clone(), key_pem)
        .await
        .context("failed to load certificate and key from in-memory contents")?;

    let certificate = x509_cert::Certificate::load_pem_chain(&certificate_pem)?
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("there must be at least one certificate present"))?;

    Ok((tls_config, certificate))
}

fn create_self_signed_certificate(
    key_pair: &NetworkKeyPair,
    public_server_name: String,
) -> CertifiedKey {
    let generated_server_name =
        walrus_storage_node_client::server_name_from_public_key(key_pair.public());
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

    CertifiedKey {
        cert: certificate,
        key_pair: pkcs8_key_pair,
    }
}

pub(crate) fn to_pkcs8_key_pair(keypair: &NetworkKeyPair) -> RcGenKeyPair {
    let secret_key: SecretKey = Secp256r1PrivateKey::from_bytes(keypair.as_ref().as_bytes())
        .expect("encode-decode of private key must not fail")
        .privkey
        .clone()
        .into();
    let document = secret_key.to_pkcs8_der().expect("valid keypair");
    RcGenKeyPair::try_from(document.as_bytes()).expect("constructed keypair is valid")
}

#[cfg(test)]
mod tests {
    use anyhow::anyhow;
    use axum::http::StatusCode;
    use fastcrypto::traits::KeyPair;
    use p256::pkcs8::LineEnding;
    use rcgen::{BasicConstraints, Certificate as RcGenCertificate, CertifiedKey, IsCa};
    use tokio::{task::JoinHandle, time::Duration};
    use tokio_util::sync::CancellationToken;
    use walrus_core::{
        BlobId,
        InconsistencyProof,
        PublicKey,
        RecoverySymbol,
        Sliver,
        SliverIndex,
        SliverPairIndex,
        SliverType,
        SymbolId,
        encoding::{EncodingAxis, GeneralRecoverySymbol, Primary},
        inconsistency::{
            InconsistencyProof as InconsistencyProofInner,
            InconsistencyVerificationError,
        },
        keys::ProtocolKeyPair,
        merkle::MerkleProof,
        messages::{
            BlobPersistenceType,
            InvalidBlobIdAttestation,
            SignedMessage,
            StorageConfirmation,
            SyncShardMsg,
            SyncShardResponse,
        },
        metadata::{UnverifiedBlobMetadataWithId, VerifiedBlobMetadataWithId},
    };
    use walrus_storage_node_client::{
        RecoverySymbolsFilter,
        StorageNodeClient,
        StorageNodeClientBuilder,
        api::{
            BlobStatus,
            DeletableCounts,
            ServiceHealthInfo,
            ShardStatusSummary,
            StoredOnNodeStatus,
            errors::StatusCode as ApiStatusCode,
        },
    };
    use walrus_sui::test_utils::event_id_for_testing;
    use walrus_test_utils::{Result as TestResult, WithTempDir, async_param_test};

    use super::*;
    use crate::{
        node::{
            BlobStatusError,
            ComputeStorageConfirmationError,
            InconsistencyProofError,
            RetrieveMetadataError,
            RetrieveSliverError,
            RetrieveSymbolError,
            StoreMetadataError,
            StoreSliverError,
            SyncShardServiceError,
            UploadIntent,
            config::StorageNodeConfig,
            errors::ListSymbolsError,
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

        async fn store_metadata(
            &self,
            _metadata: UnverifiedBlobMetadataWithId,
            _intent: UploadIntent,
        ) -> Result<bool, StoreMetadataError> {
            Ok(true)
        }

        fn metadata_status(
            &self,
            blob_id: &BlobId,
        ) -> Result<walrus_storage_node_client::api::StoredOnNodeStatus, RetrieveMetadataError>
        {
            if blob_id.0[0] == 0 {
                // A blob ID starting with 0 triggers a valid response.
                Ok(walrus_storage_node_client::api::StoredOnNodeStatus::Stored)
            } else {
                Ok(walrus_storage_node_client::api::StoredOnNodeStatus::Nonexistent)
            }
        }

        async fn retrieve_sliver(
            &self,
            _blob_id: &BlobId,
            _sliver_pair_index: SliverPairIndex,
            _sliver_type: SliverType,
        ) -> Result<Sliver, RetrieveSliverError> {
            Ok(walrus_core::test_utils::sliver())
        }

        /// Returns a valid response only for the pair index 0, otherwise, returns
        /// an internal error.
        async fn retrieve_recovery_symbol(
            &self,
            _blob_id: &BlobId,
            symbol_id: SymbolId,
            _sliver_type: Option<SliverType>,
        ) -> Result<GeneralRecoverySymbol, RetrieveSymbolError> {
            if symbol_id == SymbolId::new(0.into(), 0.into()) {
                if let Some(SliverType::Secondary) = _sliver_type {
                    let RecoverySymbol::Secondary(symbol) =
                        walrus_core::test_utils::recovery_symbol()
                    else {
                        panic!("util method must return secondary recovery symbol");
                    };
                    Ok(GeneralRecoverySymbol::from_recovery_symbol(
                        symbol,
                        SliverIndex(0),
                    ))
                } else {
                    let RecoverySymbol::Primary(symbol) =
                        walrus_core::test_utils::primary_recovery_symbol()
                    else {
                        panic!("util method must return primary recovery symbol");
                    };
                    Ok(GeneralRecoverySymbol::from_recovery_symbol(
                        symbol,
                        SliverIndex(0),
                    ))
                }
            } else {
                Err(RetrieveSliverError::Unavailable.into())
            }
        }

        async fn retrieve_multiple_recovery_symbols(
            &self,
            blob_id: &BlobId,
            _filter: RecoverySymbolsFilter,
        ) -> Result<Vec<GeneralRecoverySymbol>, ListSymbolsError> {
            let symbol = self
                .retrieve_recovery_symbol(blob_id, SymbolId::new(0.into(), 0.into()), None)
                .await
                .unwrap();
            Ok(vec![symbol.clone(), symbol])
        }

        /// Successful only for the pair index 0, otherwise, returns an internal error.
        async fn store_sliver(
            &self,
            _blob_id: BlobId,
            sliver_pair_index: SliverPairIndex,
            _sliver: Sliver,
            _intent: UploadIntent,
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
            _blob_persistence_type: &BlobPersistenceType,
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
                Ok(BlobStatus::Permanent {
                    end_epoch: 3,
                    status_event: event_id_for_testing(),
                    is_certified: true,
                    initial_certified_epoch: Some(1),
                    deletable_counts: DeletableCounts {
                        count_deletable_total: 0,
                        count_deletable_certified: 0,
                    },
                })
            } else if blob_id.0[0] == 1 {
                Ok(BlobStatus::Nonexistent)
            } else {
                Err(anyhow::anyhow!("Internal error").into())
            }
        }

        async fn sliver_status<A: EncodingAxis>(
            &self,
            _blob_id: &BlobId,
            SliverPairIndex(sliver_pair_index): SliverPairIndex,
        ) -> Result<StoredOnNodeStatus, RetrieveSliverError> {
            if sliver_pair_index == 0 {
                Ok(StoredOnNodeStatus::Stored)
            } else {
                Ok(StoredOnNodeStatus::Nonexistent)
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

        async fn health_info(&self, _detailed: bool) -> ServiceHealthInfo {
            ServiceHealthInfo {
                uptime: Duration::from_secs(0),
                epoch: 0,
                public_key: ProtocolKeyPair::generate().as_ref().public().clone(),
                node_status: "Active".to_string(),
                event_progress: walrus_storage_node_client::api::EventProgress::default(),
                shard_detail: None,
                shard_summary: ShardStatusSummary::default(),
                latest_checkpoint_sequence_number: None,
            }
        }

        async fn sync_shard(
            &self,
            _public_key: PublicKey,
            _signed_request: SignedMessage<SyncShardMsg>,
        ) -> Result<SyncShardResponse, SyncShardServiceError> {
            Ok(SyncShardResponse::V1(vec![]))
        }
    }

    async fn start_rest_api_with_config(
        config: &StorageNodeConfig,
    ) -> JoinHandle<Result<(), anyhow::Error>> {
        let rest_api_config = RestApiConfig::from(config);

        let server = RestApiServer::new(
            Arc::new(MockServiceState),
            CancellationToken::new(),
            rest_api_config,
            &Registry::default(),
        );
        let server = Arc::new(server);
        let server_copy = server.clone();
        let handle = tokio::spawn(async move { server.run().await });

        server_copy.ready().await;
        handle
    }

    async fn start_rest_api_with_test_config() -> (
        WithTempDir<StorageNodeConfig>,
        JoinHandle<Result<(), anyhow::Error>>,
    ) {
        let config = test_utils::storage_node_config();
        let handle = start_rest_api_with_config(config.as_ref()).await;
        (config, handle)
    }

    fn default_storage_node_client_builder() -> StorageNodeClientBuilder {
        StorageNodeClient::builder()
            .no_proxy()
            .tls_built_in_root_certs(false)
    }

    fn storage_node_client(config: &StorageNodeConfig) -> StorageNodeClient {
        let network_address = config.rest_api_address;
        let network_public_key = config.network_key_pair.get().unwrap().public().clone();

        default_storage_node_client_builder()
            .authenticate_with_public_key(network_public_key)
            .build(&network_address.to_string())
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
    async fn retrieve_metadata_status() {
        let (config, _handle) = start_rest_api_with_test_config().await;
        let client = storage_node_client(config.as_ref());

        let stored_status = client
            .get_metadata_status(&blob_id_for_valid_response())
            .await
            .expect("metadata status request is valid");
        let nonexistent_status = client
            .get_metadata_status(&blob_id_for_nonexistent())
            .await
            .expect("metadata status request is valid");

        assert_eq!(stored_status, StoredOnNodeStatus::Stored);
        assert_eq!(nonexistent_status, StoredOnNodeStatus::Nonexistent);
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
        let path = routes::METADATA_ENDPOINT.replace("{blob_id}", &blob_id);
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
    async fn retrieve_sliver_status() {
        let (config, _handle) = start_rest_api_with_test_config().await;
        let client = storage_node_client(config.as_ref());

        let blob_id = walrus_core::test_utils::random_blob_id();

        let stored_sliver = client
            .get_sliver_status::<Primary>(&blob_id, SliverPairIndex(0)) // 0 triggers "stored"
            .await
            .expect("should successfully retrieve sliver status");

        let nonexistent_sliver = client
            .get_sliver_status::<Primary>(&blob_id, SliverPairIndex(1)) // 1 triggers "nonexistent"
            .await
            .expect("should successfully retrieve sliver status");

        assert_eq!(stored_sliver, StoredOnNodeStatus::Stored);
        assert_eq!(nonexistent_sliver, StoredOnNodeStatus::Nonexistent);
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
            .get_confirmation(&blob_id, &BlobPersistenceType::Permanent)
            .await
            .expect("should return a signed confirmation");
    }

    async_param_test! {
        retrieve_storage_confirmation_fails: [
            not_found: (blob_id_for_nonexistent(), StatusCode::BAD_REQUEST),
            internal_error: (blob_id_for_internal_server_error(), StatusCode::INTERNAL_SERVER_ERROR)
        ]
    }
    async fn retrieve_storage_confirmation_fails(blob_id: BlobId, code: StatusCode) {
        let (config, _handle) = start_rest_api_with_test_config().await;
        let client = storage_node_client(config.as_ref());

        let err = client
            .get_confirmation(&blob_id, &BlobPersistenceType::Permanent)
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
            not_found: (blob_id_for_nonexistent(), StatusCode::BAD_REQUEST),
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
        let server = RestApiServer::new(
            Arc::new(MockServiceState),
            cancel_token.clone(),
            config.as_ref().into(),
            &Registry::default(),
        );
        let handle = tokio::spawn(async move { server.run().await });

        cancel_token.cancel();
        handle.await.unwrap().unwrap();
    }

    mod tls {
        use walrus_storage_node_client::error::NodeError;

        use super::*;

        async fn try_tls_request(client: StorageNodeClient) -> Result<(), NodeError> {
            client.get_server_health_info(false).await.map(|_| ())
        }

        /// Enable self-signed certificates by enabling TLS but removing any certificate.
        fn use_self_signed_certificates(config: &mut StorageNodeConfig) {
            config.tls.disable_tls = false;
            config.tls.certificate_path = None;
        }

        fn configure_certificates_from_disk(
            certified_key: CertifiedKey,
            config_with_dir: &mut WithTempDir<StorageNodeConfig>,
        ) -> TestResult {
            let directory = config_with_dir.temp_dir.path().to_path_buf();
            let config = config_with_dir.as_mut();

            let certificate_path = directory.join("certificate.pem");
            std::fs::write(&certificate_path, certified_key.cert.pem().as_bytes())?;
            let key_path = directory.join("private_key.pem");
            std::fs::write(&key_path, certified_key.key_pair.serialize_pem().as_bytes())?;

            config.tls.disable_tls = false;
            config.network_key_pair = PathOrInPlace::from_path(key_path);
            config.network_key_pair.load()?;
            config.tls.certificate_path = Some(certificate_path);

            Ok(())
        }

        fn create_non_self_signed_certificate(
            key_pair: &NetworkKeyPair,
            public_server_name: String,
        ) -> TestResult<(CertifiedKey, RcGenCertificate)> {
            let pkcs8_key_pair = to_pkcs8_key_pair(key_pair);
            let issuer = generate_issuer_certificate()?;

            let params = CertificateParams::new(vec![public_server_name])?;
            let certificate = params.signed_by(&pkcs8_key_pair, &issuer.cert, &issuer.key_pair)?;

            let certified_key = CertifiedKey {
                cert: certificate,
                key_pair: pkcs8_key_pair,
            };

            Ok((certified_key, issuer.cert))
        }

        fn generate_issuer_certificate() -> TestResult<CertifiedKey> {
            let key_pair = rcgen::KeyPair::generate()?;
            let mut params = rcgen::CertificateParams::new(["my-issuer-ca".to_owned()])?;
            params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
            let cert = params.self_signed(&key_pair)?;

            Ok(CertifiedKey { cert, key_pair })
        }

        #[tokio::test]
        async fn client_fails_when_tls_disabled() -> TestResult {
            let mut config = test_utils::storage_node_config();
            config.as_mut().tls.disable_tls = true;

            start_rest_api_with_config(config.as_ref()).await;

            let err = try_tls_request(storage_node_client(config.as_ref()))
                .await
                .expect_err("must fail since TLS is disabled");
            assert!(err.is_connect(), "should fail to connect");

            Ok(())
        }

        #[tokio::test]
        async fn client_accepts_self_signed_certificate() -> TestResult {
            let mut config = test_utils::storage_node_config();

            use_self_signed_certificates(config.as_mut());

            start_rest_api_with_config(config.as_ref()).await;
            try_tls_request(storage_node_client(config.as_ref())).await?;

            Ok(())
        }

        #[tokio::test]
        async fn server_loads_certificates_from_pem() -> TestResult {
            let mut config = test_utils::storage_node_config();

            let certified_key_pair = create_self_signed_certificate(
                config.as_ref().network_key_pair(),
                config.as_ref().rest_api_address.ip().to_string(),
            );

            configure_certificates_from_disk(certified_key_pair, &mut config)?;

            start_rest_api_with_config(config.as_ref()).await;

            try_tls_request(storage_node_client(config.as_ref())).await?;

            Ok(())
        }

        #[tokio::test]
        async fn server_can_use_non_self_signed_certificates() -> TestResult {
            let mut config = test_utils::storage_node_config();
            let network_key_pair = config.as_ref().network_key_pair().clone();
            let rest_api_address = config.as_ref().rest_api_address;

            let (certified_key_pair, issuer_cert) = create_non_self_signed_certificate(
                &network_key_pair,
                rest_api_address.ip().to_string(),
            )?;

            configure_certificates_from_disk(certified_key_pair, &mut config)?;
            start_rest_api_with_config(config.as_ref()).await;

            let client = default_storage_node_client_builder()
                .add_root_certificate(issuer_cert.der())
                .authenticate_with_public_key(network_key_pair.public().clone())
                .build(&rest_api_address.to_string())
                .expect("must be able to construct client in tests");

            try_tls_request(client).await?;

            Ok(())
        }

        #[tokio::test]
        async fn client_rejects_valid_certificate_with_wrong_key() -> TestResult {
            let mut config = test_utils::storage_node_config();
            let network_key_pair = config.as_ref().network_key_pair().clone();
            let other_network_public_key = NetworkKeyPair::generate().public().clone();
            let rest_api_address = config.as_ref().rest_api_address;

            let (certified_key_pair, issuer_cert) = create_non_self_signed_certificate(
                &network_key_pair,
                rest_api_address.ip().to_string(),
            )?;

            configure_certificates_from_disk(certified_key_pair, &mut config)?;
            start_rest_api_with_config(config.as_ref()).await;

            let client = default_storage_node_client_builder()
                .add_root_certificate(issuer_cert.der())
                .authenticate_with_public_key(other_network_public_key)
                .build(&rest_api_address.to_string())
                .expect("must be able to construct client in tests");

            let err = try_tls_request(client)
                .await
                .expect_err("must fail since TLS is disabled");
            assert!(err.is_connect(), "should fail to connect");

            Ok(())
        }
    }

    async_param_test! {
        list_recovery_symbols: [
            one_id: (
                RecoverySymbolsFilter::ids(vec![SymbolId::new(1.into(), 2.into())]).unwrap()
            ),
            multiple_ids: (
                RecoverySymbolsFilter::ids(vec![
                    SymbolId::new(1.into(), 2.into()),
                    SymbolId::new(3.into(), 4.into())
                ]).unwrap()
            ),
            ids_with_proof_type: (
                RecoverySymbolsFilter::ids(vec![
                    SymbolId::new(1.into(), 2.into()),
                    SymbolId::new(3.into(), 4.into())
                ])
                .unwrap()
                .require_proof_from_axis(SliverType::Primary)
            ),
            for_sliver: (RecoverySymbolsFilter::recovers(17.into(), SliverType::Primary)),
            for_sliver_with_proof_type: (
                RecoverySymbolsFilter::recovers(17.into(), SliverType::Primary)
                    .require_proof_from_axis(SliverType::Secondary)
            )
        ]
    }
    async fn list_recovery_symbols(filter: RecoverySymbolsFilter) {
        let (config, _handle) = start_rest_api_with_test_config().await;
        let client = storage_node_client(config.as_ref());
        let blob_id = walrus_core::test_utils::random_blob_id();

        let symbols = client
            .list_recovery_symbols(&blob_id, &filter)
            .await
            .expect("request should succeed");
        assert!(symbols.len() >= 2);
    }

    #[tokio::test]
    async fn limit_recovery_symbols() {
        let mut config = test_utils::storage_node_config();
        config
            .as_mut()
            .rest_server
            .experimental_max_active_recovery_symbols_requests = Some(0);
        let _handle = start_rest_api_with_config(config.as_ref()).await;

        let client = storage_node_client(config.as_ref());
        let blob_id = walrus_core::test_utils::random_blob_id();

        let error = client
            .list_recovery_symbols(
                &blob_id,
                &RecoverySymbolsFilter::recovers(17.into(), SliverType::Primary),
            )
            .await
            .expect_err("request should fail due to the limit");
        let error_status = error.status().expect("there should be a structured error");
        assert_eq!(error_status.code(), ApiStatusCode::Unavailable)
    }

    #[tokio::test]
    async fn rustls_reads_serialized_pem_network_keypair() -> TestResult {
        let mut config_with_dir = test_utils::storage_node_config();
        let certified_key = create_self_signed_certificate(
            config_with_dir.as_ref().network_key_pair(),
            config_with_dir.as_ref().rest_api_address.ip().to_string(),
        );

        let directory = config_with_dir.temp_dir.path().to_path_buf();
        let config = config_with_dir.as_mut();

        let certificate_path = directory.join("certificate.pem");
        std::fs::write(&certificate_path, certified_key.cert.pem().as_bytes())?;

        // Use the PEM generated by serializing the key-pair as the data written to the file,
        // as we are testing its serialization.
        let key_path = directory.join("private_key.pem");
        std::fs::write(
            &key_path,
            config
                .network_key_pair()
                .to_pkcs8_pem(LineEnding::default())?,
        )?;

        RustlsConfig::from_pem_file(certificate_path, key_path)
            .await
            .expect("Rustls must recognise key as valid");

        Ok(())
    }
}

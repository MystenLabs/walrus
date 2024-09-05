// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Client for interacting with the StorageNode API.

use fastcrypto::traits::{EncodeDecodeBase64, KeyPair};
use futures::TryFutureExt;
use opentelemetry::propagation::Injector;
use reqwest::{
    header::{HeaderMap, HeaderName, HeaderValue},
    Client as ReqwestClient,
    Method,
    Request,
    Response,
    Url,
};
use serde::{de::DeserializeOwned, Serialize};
use tracing::{field, Instrument, Level, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use walrus_core::{
    encoding::{EncodingAxis, EncodingConfig, Primary, RecoverySymbol, Secondary, Sliver},
    inconsistency::InconsistencyProof,
    keys::ProtocolKeyPair,
    merkle::MerkleProof,
    messages::{
        InvalidBlobIdAttestation,
        SignedStorageConfirmation,
        StorageConfirmation,
        SyncShardMsg,
        SyncShardRequest,
        SyncShardResponse,
    },
    metadata::{UnverifiedBlobMetadataWithId, VerifiedBlobMetadataWithId},
    BlobId,
    Epoch,
    InconsistencyProof as InconsistencyProofEnum,
    PublicKey,
    ShardIndex,
    Sliver as SliverEnum,
    SliverPairIndex,
    SliverType,
};

use crate::{
    api::{BlobStatus, ServiceHealthInfo},
    error::NodeError,
    node_response::NodeResponse as _,
};

const METADATA_URL_TEMPLATE: &str = "/v1/blobs/:blob_id/metadata";
const SLIVER_URL_TEMPLATE: &str = "/v1/blobs/:blob_id/slivers/:sliver_pair_index/:sliver_type";
const STORAGE_CONFIRMATION_URL_TEMPLATE: &str = "/v1/blobs/:blob_id/confirmation";
const RECOVERY_URL_TEMPLATE: &str =
    "/v1/blobs/:blob_id/slivers/:sliver_pair_index/:sliver_type/:target_pair_index";
const INCONSISTENCY_PROOF_URL_TEMPLATE: &str = "/v1/blobs/:blob_id/inconsistent/:sliver_type";
const STATUS_URL_TEMPLATE: &str = "/v1/blobs/:blob_id/status";
const HEALTH_URL_TEMPLATE: &str = "/v1/health";
const SYNC_SHARD_TEMPLATE: &str = "/v1/migrate/sync_shard";

#[derive(Debug, Clone)]
struct UrlEndpoints(Url);

impl UrlEndpoints {
    fn blob_resource(&self, blob_id: &BlobId) -> Url {
        self.0.join(&format!("/v1/blobs/{blob_id}/")).unwrap()
    }

    fn metadata(&self, blob_id: &BlobId) -> (Url, &'static str) {
        (
            self.blob_resource(blob_id).join("metadata").unwrap(),
            METADATA_URL_TEMPLATE,
        )
    }

    fn confirmation(&self, blob_id: &BlobId) -> (Url, &'static str) {
        (
            self.blob_resource(blob_id).join("confirmation").unwrap(),
            STORAGE_CONFIRMATION_URL_TEMPLATE,
        )
    }

    fn blob_status(&self, blob_id: &BlobId) -> (Url, &'static str) {
        (
            self.blob_resource(blob_id).join("status").unwrap(),
            STATUS_URL_TEMPLATE,
        )
    }

    fn recovery_symbol<A: EncodingAxis>(
        &self,
        blob_id: &BlobId,
        sliver_pair_at_remote: SliverPairIndex,
        intersecting_pair_index: SliverPairIndex,
    ) -> (Url, &'static str) {
        let (mut url, _) = self.sliver::<A>(blob_id, sliver_pair_at_remote);
        url.path_segments_mut()
            .unwrap()
            .push(&intersecting_pair_index.0.to_string());
        (url, RECOVERY_URL_TEMPLATE)
    }

    fn sliver<A: EncodingAxis>(
        &self,
        blob_id: &BlobId,
        SliverPairIndex(sliver_pair_index): SliverPairIndex,
    ) -> (Url, &'static str) {
        let sliver_type = SliverType::for_encoding::<A>();
        let path = format!("slivers/{sliver_pair_index}/{sliver_type}");
        (
            self.blob_resource(blob_id).join(&path).unwrap(),
            SLIVER_URL_TEMPLATE,
        )
    }

    fn inconsistency_proof<A: EncodingAxis>(&self, blob_id: &BlobId) -> (Url, &'static str) {
        let sliver_type = SliverType::for_encoding::<A>();
        let path = format!("inconsistent/{sliver_type}");
        (
            self.blob_resource(blob_id).join(&path).unwrap(),
            INCONSISTENCY_PROOF_URL_TEMPLATE,
        )
    }

    fn server_health_info(&self) -> (Url, &'static str) {
        (self.0.join("/v1/health").unwrap(), HEALTH_URL_TEMPLATE)
    }

    fn sync_shard(&self) -> (Url, &'static str) {
        (
            self.0.join("/v1/migrate/sync_shard").unwrap(),
            SYNC_SHARD_TEMPLATE,
        )
    }
}

/// A client for communicating with a StorageNode.
#[derive(Debug, Clone)]
pub struct Client {
    inner: ReqwestClient,
    endpoints: UrlEndpoints,
}

impl Client {
    /// Creates a new client for the storage node at the specified URL.
    pub fn from_url(url: Url, inner: ReqwestClient) -> Self {
        Self {
            endpoints: UrlEndpoints(url),
            inner,
        }
    }

    /// Converts this to the inner client.
    pub fn into_inner(self) -> ReqwestClient {
        self.inner
    }

    /// Requests the metadata for a blob ID from the node.
    #[tracing::instrument(skip_all, fields(walrus.blob_id = %blob_id), err(level = Level::DEBUG))]
    pub async fn get_metadata(
        &self,
        blob_id: &BlobId,
    ) -> Result<UnverifiedBlobMetadataWithId, NodeError> {
        let (url, template) = self.endpoints.metadata(blob_id);
        self.send_and_parse_bcs_response(Request::new(Method::GET, url), template)
            .await
    }

    /// Get the metadata and verify it against the provided config.
    #[tracing::instrument(skip_all, fields(walrus.blob_id = %blob_id), err(level = Level::DEBUG))]
    pub async fn get_and_verify_metadata(
        &self,
        blob_id: &BlobId,
        encoding_config: &EncodingConfig,
    ) -> Result<VerifiedBlobMetadataWithId, NodeError> {
        self.get_metadata(blob_id)
            .await?
            .verify(encoding_config)
            .map_err(NodeError::other)
    }

    /// Requests the status of a blob ID from the node.
    #[tracing::instrument(skip_all, fields(walrus.blob_id = %blob_id), err(level = Level::DEBUG))]
    pub async fn get_blob_status(&self, blob_id: &BlobId) -> Result<BlobStatus, NodeError> {
        let (url, template) = self.endpoints.blob_status(blob_id);
        self.send_and_parse_service_response(Request::new(Method::GET, url), template)
            .await
    }

    /// Requests a storage confirmation from the node for the Blob specified by the given ID
    // TODO: This function is only used internally and in test functions in walrus-service. (#498)
    #[tracing::instrument(skip_all, fields(walrus.blob_id = %blob_id), err(level = Level::DEBUG))]
    pub async fn get_confirmation(
        &self,
        blob_id: &BlobId,
    ) -> Result<SignedStorageConfirmation, NodeError> {
        let (url, template) = self.endpoints.confirmation(blob_id);
        // NOTE(giac): in the future additional values may be possible here.
        let StorageConfirmation::Signed(confirmation) = self
            .send_and_parse_service_response(Request::new(Method::GET, url), template)
            .await?;
        Ok(confirmation)
    }

    /// Requests a storage confirmation from the node for the Blob specified by the given ID
    #[tracing::instrument(
        skip_all,
        fields(
            walrus.blob_id = %blob_id,
            walrus.epoch = epoch,
            walrus.node.public_key = %public_key
        ),
        err(level = Level::DEBUG)
    )]
    pub async fn get_and_verify_confirmation(
        &self,
        blob_id: &BlobId,
        epoch: Epoch,
        public_key: &PublicKey,
    ) -> Result<SignedStorageConfirmation, NodeError> {
        let confirmation = self.get_confirmation(blob_id).await?;
        let _ = confirmation
            .verify(public_key, epoch, blob_id)
            .map_err(NodeError::other)?;
        Ok(confirmation)
    }

    /// Gets a primary or secondary sliver for the identified sliver pair.
    // TODO: This function is only used internally and in test functions in walrus-service. (#498)
    #[tracing::instrument(
        skip_all,
        fields(
            walrus.blob_id = %blob_id,
            walrus.sliver.pair_index = %sliver_pair_index,
            walrus.sliver.r#type = A::NAME,
        ),
        err(level = Level::DEBUG)
    )]
    pub async fn get_sliver<A: EncodingAxis>(
        &self,
        blob_id: &BlobId,
        sliver_pair_index: SliverPairIndex,
    ) -> Result<Sliver<A>, NodeError> {
        let (url, template) = self.endpoints.sliver::<A>(blob_id, sliver_pair_index);
        self.send_and_parse_bcs_response(Request::new(Method::GET, url), template)
            .await
    }

    /// Gets a primary or secondary sliver for the identified sliver pair.
    // TODO: This function is only used internally and in test functions in walrus-service. (#498)
    #[tracing::instrument(skip_all, err(level = Level::DEBUG))]
    pub async fn get_sliver_by_type(
        &self,
        blob_id: &BlobId,
        sliver_pair_index: SliverPairIndex,
        sliver_type: SliverType,
    ) -> Result<SliverEnum, NodeError> {
        match sliver_type {
            SliverType::Primary => self
                .get_sliver::<Primary>(blob_id, sliver_pair_index)
                .await
                .map(SliverEnum::Primary),
            SliverType::Secondary => self
                .get_sliver::<Secondary>(blob_id, sliver_pair_index)
                .await
                .map(SliverEnum::Secondary),
        }
    }

    /// Requests the sliver identified by `metadata.blob_id()` and the pair index from the storage
    /// node, and verifies it against the provided metadata and encoding config.
    ///
    /// # Panics
    ///
    /// Panics if the provided encoding config is not applicable to the metadata, i.e., if
    /// [`VerifiedBlobMetadataWithId::is_encoding_config_applicable`] returns false.
    #[tracing::instrument(
        skip_all,
        fields(
            walrus.blob_id = %metadata.blob_id(),
            walrus.sliver.pair_index = %sliver_pair_index,
            walrus.sliver.r#type = A::NAME,
        ),
        err(level = Level::DEBUG)
    )]
    pub async fn get_and_verify_sliver<A: EncodingAxis>(
        &self,
        sliver_pair_index: SliverPairIndex,
        metadata: &VerifiedBlobMetadataWithId,
        encoding_config: &EncodingConfig,
    ) -> Result<Sliver<A>, NodeError> {
        assert!(
            metadata.is_encoding_config_applicable(encoding_config),
            "encoding config is not applicable to the provided metadata and blob"
        );

        let sliver = self
            .get_sliver(metadata.blob_id(), sliver_pair_index)
            .await?;

        sliver
            .verify(encoding_config, metadata.metadata())
            .map_err(NodeError::other)?;

        Ok(sliver)
    }

    /// Gets the recovery symbol for a primary or secondary sliver.
    ///
    /// The symbol is identified by the (A, sliver_pair_at_remote, intersecting_pair_index) tuple.
    #[tracing::instrument(
        skip_all,
        fields(
            walrus.blob_id = %blob_id,
            walrus.sliver.pair_index = %local_sliver_pair,
            walrus.sliver.remote_pair_index = %remote_sliver_pair,
            walrus.recovery.symbol_type = A::NAME,
        ),
        err(level = Level::DEBUG)
    )]
    pub async fn get_recovery_symbol<A: EncodingAxis>(
        &self,
        blob_id: &BlobId,
        remote_sliver_pair: SliverPairIndex,
        local_sliver_pair: SliverPairIndex,
    ) -> Result<RecoverySymbol<A, MerkleProof>, NodeError> {
        let (url, template) =
            self.endpoints
                .recovery_symbol::<A>(blob_id, remote_sliver_pair, local_sliver_pair);
        self.send_and_parse_bcs_response(Request::new(Method::GET, url), template)
            .await
    }

    /// Gets the recovery symbol for a primary or secondary sliver.
    ///
    /// The symbol is identified by the (A, sliver_pair_at_remote, intersecting_pair_index) tuple.
    #[tracing::instrument(
        skip_all,
        fields(
            walrus.blob_id = %metadata.blob_id(),
            walrus.sliver.pair_index = %local_sliver_pair,
            walrus.sliver.remote_pair_index = %remote_sliver_pair,
            walrus.recovery.symbol_type = A::NAME,
        ),
        err(level = Level::DEBUG)
    )]
    pub async fn get_and_verify_recovery_symbol<A: EncodingAxis>(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
        encoding_config: &EncodingConfig,
        remote_sliver_pair: SliverPairIndex,
        local_sliver_pair: SliverPairIndex,
    ) -> Result<RecoverySymbol<A, MerkleProof>, NodeError> {
        let symbol = self
            .get_recovery_symbol::<A>(metadata.blob_id(), remote_sliver_pair, local_sliver_pair)
            .await?;

        symbol
            .verify(
                metadata.as_ref(),
                encoding_config,
                local_sliver_pair.to_sliver_index::<A>(encoding_config.n_shards()),
            )
            .map_err(NodeError::other)?;

        Ok(symbol)
    }

    /// Stores the metadata on the node.
    #[tracing::instrument(
        skip_all, fields(walrus.blob_id = %metadata.blob_id()), err(level = Level::DEBUG)
    )]
    pub async fn store_metadata(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
    ) -> Result<(), NodeError> {
        let (url, template) = self.endpoints.metadata(metadata.blob_id());
        let request = self.create_request_with_payload(Method::PUT, url, metadata.as_ref())?;
        self.send_and_parse_service_response::<String>(request, template)
            .await?;
        Ok(())
    }

    /// Stores a sliver on a node.
    #[tracing::instrument(
        skip_all,
        fields(
            walrus.blob_id = %blob_id,
            walrus.sliver.pair_index = %pair_index,
            walrus.sliver.type_ = %A::NAME,
        ),
        err(level = Level::DEBUG)
    )]
    pub async fn store_sliver<A: EncodingAxis>(
        &self,
        blob_id: &BlobId,
        pair_index: SliverPairIndex,
        sliver: &Sliver<A>,
    ) -> Result<(), NodeError> {
        tracing::trace!("starting to store sliver");
        let (url, template) = self.endpoints.sliver::<A>(blob_id, pair_index);
        let request = self.create_request_with_payload(Method::PUT, url, &sliver)?;
        self.send_and_parse_service_response::<String>(request, template)
            .await?;

        Ok(())
    }

    /// Stores a sliver on a node.
    // TODO: This function is only used internally and in test functions in walrus-service. (#498)
    #[tracing::instrument(skip_all, err(level = Level::DEBUG))]
    pub async fn store_sliver_by_type(
        &self,
        blob_id: &BlobId,
        pair_index: SliverPairIndex,
        sliver: &SliverEnum,
    ) -> Result<(), NodeError> {
        match sliver {
            SliverEnum::Primary(sliver) => self.store_sliver(blob_id, pair_index, sliver).await,
            SliverEnum::Secondary(sliver) => self.store_sliver(blob_id, pair_index, sliver).await,
        }
    }

    /// Sends an inconsistency proof for the specified [`EncodingAxis`] to a node.
    #[tracing::instrument(skip_all, fields( walrus.blob_id = %blob_id), err(level = Level::DEBUG))]
    async fn submit_inconsistency_proof<A: EncodingAxis>(
        &self,
        blob_id: &BlobId,
        inconsistency_proof: &InconsistencyProof<A, MerkleProof>,
    ) -> Result<InvalidBlobIdAttestation, NodeError> {
        let (url, template) = self.endpoints.inconsistency_proof::<A>(blob_id);
        let request = self.create_request_with_payload(Method::PUT, url, &inconsistency_proof)?;

        self.send_and_parse_service_response(request, template)
            .await
    }

    /// Sends an inconsistency proof to a node and requests the invalid blob id
    /// attestation from the node.
    #[tracing::instrument(skip_all, err(level = Level::DEBUG))]
    pub async fn submit_inconsistency_proof_by_type(
        &self,
        blob_id: &BlobId,
        inconsistency_proof: &InconsistencyProofEnum,
    ) -> Result<InvalidBlobIdAttestation, NodeError> {
        match inconsistency_proof {
            InconsistencyProofEnum::Primary(proof) => {
                self.submit_inconsistency_proof(blob_id, proof).await
            }
            InconsistencyProofEnum::Secondary(proof) => {
                self.submit_inconsistency_proof(blob_id, proof).await
            }
        }
    }

    /// Sends an inconsistency proof to a node and verifies the returned invalid blob id
    /// attestation.
    #[tracing::instrument(
        skip_all,
        fields(
            walrus.blob_id = %blob_id,
            walrus.epoch = epoch,
            walrus.node.public_key = %public_key,
        ),
        err(level = Level::DEBUG)
    )]
    pub async fn submit_inconsistency_proof_and_verify_attestation(
        &self,
        blob_id: &BlobId,
        inconsistency_proof: &InconsistencyProofEnum,
        epoch: Epoch,
        public_key: &PublicKey,
    ) -> Result<InvalidBlobIdAttestation, NodeError> {
        let attestation = self
            .submit_inconsistency_proof_by_type(blob_id, inconsistency_proof)
            .await?;
        let _ = attestation
            .verify(public_key, epoch, blob_id)
            .map_err(NodeError::other)?;
        Ok(attestation)
    }

    /// Gets the health information of the storage node.
    #[tracing::instrument(skip_all, err(level = Level::DEBUG))]
    pub async fn get_server_health_info(&self) -> Result<ServiceHealthInfo, NodeError> {
        let (url, template) = self.endpoints.server_health_info();
        self.send_and_parse_service_response(Request::new(Method::GET, url), template)
            .await
    }

    /// Syncs a shard from the storage node.
    #[tracing::instrument(
        skip_all,
        fields(
            walrus.shard_index = %shard_index,
            walrus.epoch = epoch,
            walrus.blob_id = %starting_blob_id,
            walrus.sync.sliver_count = %sliver_count,
        ),
        err(level = Level::DEBUG)
    )]
    pub async fn sync_shard<A: EncodingAxis>(
        &self,
        shard_index: ShardIndex,
        starting_blob_id: BlobId,
        sliver_count: u64,
        epoch: Epoch,
        key_pair: &ProtocolKeyPair,
    ) -> Result<SyncShardResponse, NodeError> {
        let (url, template) = self.endpoints.sync_shard();
        let request = SyncShardRequest::new(
            shard_index,
            A::sliver_type(),
            starting_blob_id,
            sliver_count,
            epoch,
        );

        let sync_shard_msg = SyncShardMsg::new(epoch, request);
        let signed_request = key_pair.sign_message(&sync_shard_msg);
        let http_request = self.create_request_with_payload_and_public_key(
            Method::POST,
            url,
            &signed_request,
            key_pair.as_ref().public(),
        )?;
        self.send_and_parse_bcs_response(http_request, template)
            .await
    }

    /// Send a request with tracing and context propagation.
    ///
    /// The HTTP span ends after the parsing of the headers, since the response may be streamed.
    async fn send_request(
        &self,
        mut request: Request,
        url_template: &'static str,
    ) -> Result<(Response, Span), NodeError> {
        let url = request.url();
        let span = tracing::info_span!(
            "http_request",
            otel.name = format!("{} {}", request.method().as_str(), url_template),
            otel.kind = "CLIENT",
            otel.status_code = field::Empty,
            otel.status_message = field::Empty,
            http.request.method = request.method().as_str(),
            http.response.status_code = field::Empty,
            server.address = url.host_str(),
            server.port = url.port_or_known_default(),
            url.full = url.as_str(),
            "error.type" = field::Empty,
        );

        // We use the global propagator as in the examples, since this allows a client using the
        // library to completely disable propagation for contextual information.
        opentelemetry::global::get_text_map_propagator(|propagator| {
            propagator.inject_context(&span.context(), &mut HeaderInjector(request.headers_mut()));
        });

        let result = self.inner.execute(request).instrument(span.clone()).await;

        match result {
            Ok(response) => {
                // Check that the response is using HTTP 2 when not in release mode.
                debug_assert!(response.version() == reqwest::Version::HTTP_2);

                let status_code = response.status();
                span.record("http.response.status_code", status_code.as_str());

                // We don't handle anything that is not a 2xx, so they're all failures to us.
                if !status_code.is_success() {
                    span.record("otel.status_code", "ERROR");
                    // For HTTP errors, otel recommends not setting status_message.
                    span.record("error.type", status_code.as_str());
                }

                response
                    .response_error_for_status()
                    .instrument(span.clone())
                    .await
                    .map(|response| (response, span))
            }
            Err(err) => {
                span.record("otel.status_code", "ERROR");
                span.record("otel.status_message", err.to_string());
                span.record("error.type", "reqwest::Error");

                Err(NodeError::reqwest(err))
            }
        }
    }

    async fn send_and_parse_bcs_response<T: DeserializeOwned>(
        &self,
        request: Request,
        url_template: &'static str,
    ) -> Result<T, NodeError> {
        async {
            let (response, span) = self.send_request(request, url_template).await?;
            response.bcs().instrument(span).await
        }
        .inspect_err(trace_error)
        .await
    }

    async fn send_and_parse_service_response<T: DeserializeOwned>(
        &self,
        request: Request,
        url_template: &'static str,
    ) -> Result<T, NodeError> {
        async {
            let (response, span) = self.send_request(request, url_template).await?;
            response.service_response().instrument(span).await
        }
        .inspect_err(trace_error)
        .await
    }

    fn create_request_with_payload<T: Serialize>(
        &self,
        method: Method,
        url: Url,
        body: &T,
    ) -> Result<Request, NodeError> {
        self.inner
            .request(method, url)
            .body(bcs::to_bytes(body).expect("type must be bcs encodable"))
            .build()
            .map_err(NodeError::reqwest)
    }

    // Creates a request with a payload and a public key in the Authorization header.
    fn create_request_with_payload_and_public_key<T: Serialize>(
        &self,
        method: Method,
        url: Url,
        body: &T,
        public_key: &PublicKey,
    ) -> Result<Request, NodeError> {
        let mut request = self.create_request_with_payload(method, url, body)?;
        let encoded_key = public_key.encode_base64();
        let public_key_header = HeaderValue::from_str(&encoded_key).map_err(NodeError::other)?;
        request
            .headers_mut()
            .insert(reqwest::header::AUTHORIZATION, public_key_header);
        Ok(request)
    }
}

// opentelemetry_http currently uses too low a version of the http crate,
// so we reimplement the injector here.
struct HeaderInjector<'a>(pub &'a mut HeaderMap);

impl<'a> Injector for HeaderInjector<'a> {
    fn set(&mut self, key: &str, value: String) {
        if let Ok(name) = HeaderName::from_bytes(key.as_bytes()) {
            if let Ok(val) = HeaderValue::from_str(&value) {
                self.0.insert(name, val);
            }
        }
    }
}

fn trace_error(error: &NodeError) {
    tracing::trace!(error_details = ?error);
}

#[cfg(test)]
mod tests {
    use walrus_core::{encoding::Primary, test_utils};
    use walrus_test_utils::param_test;

    use super::*;

    const BLOB_ID: BlobId = test_utils::blob_id_from_u64(99);

    param_test! {
        test_blob_url_endpoint: [
            blob: (|e| e.blob_resource(&BLOB_ID), ""),
            metadata: (|e| e.metadata(&BLOB_ID).0, "metadata"),
            confirmation: (|e| e.confirmation(&BLOB_ID).0, "confirmation"),
            sliver: (|e| e.sliver::<Primary>(&BLOB_ID, SliverPairIndex(1)).0, "slivers/1/primary"),
            recovery_symbol: (
                |e| e.recovery_symbol::<Primary>(&BLOB_ID, SliverPairIndex(1), SliverPairIndex(2)).0,
                "slivers/1/primary/2"
            ),
            inconsistency_proof: (
                |e| e.inconsistency_proof::<Primary>(&BLOB_ID).0, "inconsistent/primary"
            ),
        ]
    }
    fn test_blob_url_endpoint<F>(url_fn: F, expected_path: &str)
    where
        F: FnOnce(UrlEndpoints) -> Url,
    {
        let endpoints = UrlEndpoints(Url::parse("http://node.com").unwrap());
        let url = url_fn(endpoints);
        let expected = format!("http://node.com/v1/blobs/{BLOB_ID}/{expected_path}");

        assert_eq!(url.to_string(), expected);
    }

    #[test]
    fn test_url_health_info_endpoint() {
        let endpoints = UrlEndpoints(Url::parse("http://node.com").unwrap());
        let (url, _) = endpoints.server_health_info();

        assert_eq!(url.to_string(), "http://node.com/v1/health");
    }
}

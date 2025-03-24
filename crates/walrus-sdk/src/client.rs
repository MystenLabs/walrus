// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Client for interacting with the StorageNode API.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use fastcrypto::traits::{EncodeDecodeBase64, KeyPair};
use futures::TryFutureExt as _;
use opentelemetry::propagation::Injector;
use reqwest::{
    header::{HeaderMap, HeaderName, HeaderValue},
    Client as ReqwestClient,
    ClientBuilder as ReqwestClientBuilder,
    Method,
    Request,
    Response,
    Url,
};
use rustls::pki_types::CertificateDer;
use rustls_native_certs::CertificateResult;
use serde::{de::DeserializeOwned, Serialize, Serializer};
use sui_types::base_types::ObjectID;
use tracing::{field, Instrument as _, Level, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use walrus_core::{
    encoding::{
        EncodingAxis,
        EncodingConfig,
        GeneralRecoverySymbol,
        Primary,
        RecoverySymbol,
        Secondary,
        SliverData,
    },
    inconsistency::InconsistencyProof,
    keys::ProtocolKeyPair,
    merkle::MerkleProof,
    messages::{
        BlobPersistenceType,
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
    NetworkPublicKey,
    PublicKey,
    ShardIndex,
    Sliver,
    SliverIndex,
    SliverPairIndex,
    SliverType,
    SymbolId,
};

use crate::{
    api::{BlobStatus, ServiceHealthInfo, StoredOnNodeStatus},
    error::{BuildErrorKind, ClientBuildError, ListAndVerifyRecoverySymbolsError, NodeError},
    node_response::NodeResponse,
    tls::TlsCertificateVerifier,
};

const METADATA_URL_TEMPLATE: &str = "/v1/blobs/:blob_id/metadata";
const METADATA_STATUS_URL_TEMPLATE: &str = "/v1/blobs/:blob_id/metadata/status";
const SLIVER_URL_TEMPLATE: &str = "/v1/blobs/:blob_id/slivers/:sliver_pair_index/:sliver_type";
const SLIVER_STATUS_TEMPLATE: &str =
    "/v1/blobs/:blob_id/slivers/:sliver_pair_index/:sliver_type/status";
const PERMANENT_BLOB_CONFIRMATION_URL_TEMPLATE: &str = "/v1/blobs/:blob_id/confirmation/permanent";
const DELETABLE_BLOB_CONFIRMATION_URL_TEMPLATE: &str =
    "/v1/blobs/:blob_id/confirmation/deletable/:object_id";
const LEGACY_RECOVERY_URL_TEMPLATE: &str =
    "/v1/blobs/:blob_id/slivers/:sliver_pair_index/:sliver_type/:target_pair_index";
const RECOVERY_SYMBOL_URL_TEMPLATE: &str = "/v1/blobs/:blob_id/recoverySymbols/:symbol_id";
const LIST_RECOVERY_SYMBOLS_URL_TEMPLATE: &str = "/v1/blobs/:blob_id/recoverySymbols";
const INCONSISTENCY_PROOF_URL_TEMPLATE: &str = "/v1/blobs/:blob_id/inconsistencyProof/:sliver_type";
const BLOB_STATUS_URL_TEMPLATE: &str = "/v1/blobs/:blob_id/status";
const HEALTH_URL_TEMPLATE: &str = "/v1/health";
const SYNC_SHARD_TEMPLATE: &str = "/v1/migrate/sync_shard";

#[derive(Debug, Clone)]
struct UrlEndpoints(Url);

impl UrlEndpoints {
    /// Constructs a URL for the given `blob_id` and a subpath.
    ///
    /// # Panics
    ///
    /// Panics if the result is not a valid URL.
    fn blob_resource(&self, blob_id: &BlobId, subpath: &str) -> Url {
        self.0
            .join(&format!("/v1/blobs/{blob_id}/{subpath}"))
            .expect("this should be a valid URL")
    }

    fn metadata(&self, blob_id: &BlobId) -> (Url, &'static str) {
        (
            self.blob_resource(blob_id, "metadata"),
            METADATA_URL_TEMPLATE,
        )
    }

    fn metadata_status(&self, blob_id: &BlobId) -> (Url, &'static str) {
        (
            self.blob_resource(blob_id, "metadata/status"),
            METADATA_STATUS_URL_TEMPLATE,
        )
    }

    fn confirmation(
        &self,
        blob_id: &BlobId,
        blob_persistence_type: &BlobPersistenceType,
    ) -> (Url, &'static str) {
        match blob_persistence_type {
            BlobPersistenceType::Permanent => self.permanent_blob_confirmation(blob_id),
            BlobPersistenceType::Deletable { object_id } => {
                self.deletable_blob_confirmation(blob_id, &object_id.into())
            }
        }
    }

    fn permanent_blob_confirmation(&self, blob_id: &BlobId) -> (Url, &'static str) {
        (
            self.blob_resource(blob_id, "confirmation/permanent"),
            PERMANENT_BLOB_CONFIRMATION_URL_TEMPLATE,
        )
    }

    fn deletable_blob_confirmation(
        &self,
        blob_id: &BlobId,
        object_id: &ObjectID,
    ) -> (Url, &'static str) {
        (
            self.blob_resource(blob_id, &format!("confirmation/deletable/{object_id}")),
            DELETABLE_BLOB_CONFIRMATION_URL_TEMPLATE,
        )
    }

    fn blob_status(&self, blob_id: &BlobId) -> (Url, &'static str) {
        (
            self.blob_resource(blob_id, "status"),
            BLOB_STATUS_URL_TEMPLATE,
        )
    }

    fn sliver_path<A: EncodingAxis>(
        &self,
        blob_id: &BlobId,
        SliverPairIndex(sliver_pair_index): SliverPairIndex,
        subpath: Option<&str>,
    ) -> Url {
        let sliver_type = SliverType::for_encoding::<A>();
        let mut blob_subpath = format!("slivers/{sliver_pair_index}/{sliver_type}");
        if let Some(subpath) = subpath {
            blob_subpath = blob_subpath + "/" + subpath;
        }
        self.blob_resource(blob_id, &blob_subpath)
    }

    fn sliver<A: EncodingAxis>(
        &self,
        blob_id: &BlobId,
        sliver_pair_index: SliverPairIndex,
    ) -> (Url, &'static str) {
        (
            self.sliver_path::<A>(blob_id, sliver_pair_index, None),
            SLIVER_URL_TEMPLATE,
        )
    }

    fn sliver_status<A: EncodingAxis>(
        &self,
        blob_id: &BlobId,
        sliver_pair_index: SliverPairIndex,
    ) -> (Url, &'static str) {
        (
            self.sliver_path::<A>(blob_id, sliver_pair_index, Some("status")),
            SLIVER_STATUS_TEMPLATE,
        )
    }

    fn legacy_recovery_symbol<A: EncodingAxis>(
        &self,
        blob_id: &BlobId,
        sliver_pair_at_remote: SliverPairIndex,
        intersecting_pair_index: SliverPairIndex,
    ) -> (Url, &'static str) {
        (
            self.sliver_path::<A>(
                blob_id,
                sliver_pair_at_remote,
                Some(&intersecting_pair_index.0.to_string()),
            ),
            LEGACY_RECOVERY_URL_TEMPLATE,
        )
    }

    fn recovery_symbol(&self, blob_id: &BlobId, symbol_id: SymbolId) -> (Url, &'static str) {
        (
            self.blob_resource(blob_id, &format!("recoverySymbols/{}", symbol_id)),
            RECOVERY_SYMBOL_URL_TEMPLATE,
        )
    }

    fn list_recovery_symbols(&self, blob_id: &BlobId) -> (Url, &'static str) {
        (
            self.blob_resource(blob_id, "recoverySymbols"),
            LIST_RECOVERY_SYMBOLS_URL_TEMPLATE,
        )
    }

    fn inconsistency_proof<A: EncodingAxis>(&self, blob_id: &BlobId) -> (Url, &'static str) {
        let sliver_type = SliverType::for_encoding::<A>();
        (
            self.blob_resource(blob_id, &format!("inconsistencyProof/{sliver_type}")),
            INCONSISTENCY_PROOF_URL_TEMPLATE,
        )
    }

    fn server_health_info(&self, detailed: bool) -> (Url, &'static str) {
        let mut url = self.0.join("/v1/health").expect("this is a valid URL");
        url.set_query(detailed.then_some("detailed=true"));
        (url, HEALTH_URL_TEMPLATE)
    }

    fn sync_shard(&self) -> (Url, &'static str) {
        (
            self.0
                .join("/v1/migrate/sync_shard")
                .expect("this is a valid URL"),
            SYNC_SHARD_TEMPLATE,
        )
    }
}

/// Filter for [`Client::list_recovery_symbols()`] endpoint.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RecoverySymbolsFilter {
    /// The type of proof expected to be used within the symbol.
    proof_axis: Option<SliverType>,

    /// Specification of the symbol IDs.
    #[serde(flatten)]
    id_spec: SymbolIdFilter,
}

/// Filter for [`Client::list_recovery_symbols()`] endpoint.
#[derive(Debug, Clone, Serialize)]
pub enum SymbolIdFilter {
    /// Limit the results to the specified symbols.
    #[serde(untagged, serialize_with = "serialize_ids_in_query")]
    Ids(Vec<SymbolId>),

    /// Return all available symbols that can be used to recover the specified sliver.
    #[serde(untagged, rename_all = "camelCase")]
    Recovers {
        /// The ID of the target sliver being recovered.
        target_sliver: SliverIndex,
        /// The type of the sliver being recovered.
        target_type: SliverType,
    },
}

impl RecoverySymbolsFilter {
    /// Returns a new filter for the identified list of symbols, or None if the list is empty.
    pub fn ids(ids: Vec<SymbolId>) -> Option<Self> {
        if ids.is_empty() {
            return None;
        }

        Some(Self {
            proof_axis: None,
            id_spec: SymbolIdFilter::Ids(ids),
        })
    }

    /// Returns a new filter that filters to all held symbols for the identified sliver.
    pub fn recovers(target: SliverIndex, target_type: SliverType) -> Self {
        Self {
            proof_axis: None,
            id_spec: SymbolIdFilter::Recovers {
                target_sliver: target,
                target_type,
            },
        }
    }

    /// Sets the expectation that the proof is of a specific type.
    ///
    /// This is currently only necessary if the intention is to construct an inconsistency proof
    /// of a specific type with the returned symbols.
    pub fn require_proof_from_axis(mut self, proof_axis: SliverType) -> Self {
        self.proof_axis = Some(proof_axis);
        self
    }

    /// Returns true if the provided symbol is accepted by the filter.
    pub fn accepts(&self, symbol: &GeneralRecoverySymbol) -> bool {
        if self
            .proof_axis
            .map(|required_axis| required_axis != symbol.proof_axis())
            .unwrap_or(false)
        {
            return false;
        }

        match self.id_spec {
            SymbolIdFilter::Ids(ref vec) => vec.contains(&symbol.id()),
            SymbolIdFilter::Recovers {
                target_sliver,
                target_type,
            } => {
                // Check that the axis of the target's type matches the target sliver's index.
                symbol.id().sliver_index(target_type) == target_sliver
            }
        }
    }

    /// Returns specification of which ids should be returned.
    pub fn id_filter(&self) -> &SymbolIdFilter {
        &self.id_spec
    }

    /// Returns the axis over which the proof is expected to be computed.
    pub fn proof_axis(&self) -> Option<SliverType> {
        self.proof_axis
    }
}

fn serialize_ids_in_query<S>(symbols: &[SymbolId], serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.collect_map(symbols.iter().map(|id| ("id", id)))
}

/// A builder that can be used to construct a [`Client`].
///
/// Can be created with [`Client::builder()`].
#[derive(Debug, Default)]
pub struct ClientBuilder {
    inner: ReqwestClientBuilder,
    server_public_key: Option<NetworkPublicKey>,
    roots: Vec<CertificateDer<'static>>,
    no_built_in_root_certs: bool,
    connect_timeout: Option<Duration>,
}

impl ClientBuilder {
    /// Default timeout that is configured for connecting to the remote server.
    ///
    /// Modern advice is that TCP implementations should have a retry timeout of 1 second
    /// (previously, it was 3 seconds). In the event of a lossy network, the SYN/SYN-ACK
    /// packets may be lost necessitating one or several retries until actually connecting to the
    /// server. We therefore want to allow for several retries.
    ///
    /// The default of 5 seconds should allow for around 2-3 SYN attempts before failing.
    ///
    /// See RFC6298 for more information.
    const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

    /// Creates a new builder to construct a [`Client`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a new builder using an existing request client builder.
    ///
    /// This can be used to preconfigure options on the client. These options may be overwritten,
    /// however, during construction of the final client.
    ///
    /// TLS settings are not preserved.
    pub fn from_reqwest(builder: ReqwestClientBuilder) -> Self {
        Self {
            inner: builder,
            ..Self::default()
        }
    }

    /// Authenticate the server with the provided public key instead of the Web PKI.
    ///
    /// By default, to authenticate the connection to the storage node, the client verifies that the
    /// storage node provides a valid, unexpired certificate which matches the `authority` provided
    /// to [`build()`][Self::build]. Calling this method instead authenticates the storage node
    /// solely on the basis that it is able to establish the TLS connection with the private key
    /// corresponding to the provided public key.
    pub fn authenticate_with_public_key(mut self, public_key: NetworkPublicKey) -> Self {
        self.server_public_key = Some(public_key);
        self
    }

    /// Clears proxy settings in the client, and disables fetching proxy settings from the OS.
    ///
    /// On some systems, this can speed up the construction of the client.
    pub fn no_proxy(mut self) -> Self {
        self.inner = self.inner.no_proxy();
        self
    }

    /// Add a custom DER-encoded root certificate.
    ///
    /// It is the responsibility of the caller to check the certificate for validity.
    pub fn add_root_certificate(mut self, certificate: &[u8]) -> Self {
        self.roots
            .push(CertificateDer::from(certificate).into_owned());
        self
    }

    /// Add a custom DER-encoded root certificate.
    ///
    /// It is the responsibility of the caller to check the certificate for validity.
    pub fn add_root_certificates(mut self, certificates: &[CertificateDer<'static>]) -> Self {
        self.roots.extend(certificates.iter().cloned());
        self
    }

    /// Controls the use of built-in/preloaded certificates during certificate validation.
    ///
    /// Defaults to true – built-in system certs will be used.
    pub fn tls_built_in_root_certs(mut self, tls_built_in_root_certs: bool) -> Self {
        self.no_built_in_root_certs = !tls_built_in_root_certs;
        self
    }

    /// Set a timeout for only the connect phase of a Client.
    ///
    /// The default is 5 seconds.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = Some(timeout);
        self
    }

    /// Convenience function to build the client where the server is identified by a [`SocketAddr`].
    ///
    /// Equivalent `self.build(&remote.to_string())`
    pub fn build_for_remote_ip(self, remote: SocketAddr) -> Result<Client, ClientBuildError> {
        self.build(&remote.to_string())
    }

    /// Consume the `ClientBuilder` and return a configured [`Client`].
    ///
    /// This method fails if a valid URL cannot be created with the provided address, the
    /// Rustls TLS backend cannot be initialized, or the resolver cannot load the system
    /// configuration.
    pub fn build(mut self, address: &str) -> Result<Client, ClientBuildError> {
        #[cfg(msim)]
        {
            self = self.no_proxy();
        }

        let url = Url::parse(&format!("https://{address}"))
            .map_err(|_| BuildErrorKind::InvalidHostOrPort)?;
        // We extract the host from the URL, since the provided host string may have details like a
        // username or password.
        let host = url
            .host_str()
            .ok_or(BuildErrorKind::InvalidHostOrPort)?
            .to_string();
        let endpoints = UrlEndpoints(url);

        if !self.no_built_in_root_certs {
            let CertificateResult { certs, errors, .. } = rustls_native_certs::load_native_certs();
            if certs.is_empty() {
                return Err(BuildErrorKind::FailedToLoadCerts(errors).into());
            };
            if !errors.is_empty() {
                tracing::warn!(
                    "encountered {} errors when trying to load native certs",
                    errors.len(),
                );
                tracing::debug!(?errors, "errors encountered when loading native certs");
            }
            self.roots.extend(certs);
        }

        let verifier = if let Some(public_key) = self.server_public_key {
            TlsCertificateVerifier::new_with_pinned_public_key(public_key, host, self.roots)
                .map_err(BuildErrorKind::Tls)?
        } else {
            TlsCertificateVerifier::new(self.roots).map_err(BuildErrorKind::Tls)?
        };

        let rustls_config = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(verifier))
            .with_no_client_auth();

        let inner = self
            .inner
            .https_only(true)
            .http2_prior_knowledge()
            .http2_adaptive_window(true)
            .use_preconfigured_tls(rustls_config)
            .connect_timeout(
                self.connect_timeout
                    .unwrap_or(Self::DEFAULT_CONNECT_TIMEOUT),
            )
            .build()
            .map_err(ClientBuildError::reqwest)?;

        Ok(Client { inner, endpoints })
    }
}

/// A client for communicating with a StorageNode.
#[derive(Debug, Clone)]
pub struct Client {
    inner: ReqwestClient,
    endpoints: UrlEndpoints,
}

impl Client {
    /// Returns a new [`ClientBuilder`] that can be used to construct a client.
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    /// Creates a new client for the storage node.
    ///
    /// The storage node is identified by the DNS name or socket address, and authenticated with
    /// the provided public key.
    ///
    /// This method ensures that the storage node is authenticated: Only the storage node can
    /// establish the connection using the self-signed certificate corresponding to the provided
    /// identity and public key.
    pub fn for_storage_node(
        address: &str,
        public_key: &NetworkPublicKey,
    ) -> Result<Client, ClientBuildError> {
        Self::builder()
            .authenticate_with_public_key(public_key.clone())
            .build(address)
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

    /// Requests the status of metadata for a blob ID from the node.
    #[tracing::instrument(skip_all, fields(walrus.blob_id = %blob_id), err(level = Level::DEBUG))]
    pub async fn get_metadata_status(
        &self,
        blob_id: &BlobId,
    ) -> Result<StoredOnNodeStatus, NodeError> {
        let (url, template) = self.endpoints.metadata_status(blob_id);
        self.send_and_parse_service_response(Request::new(Method::GET, url), template)
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
    #[tracing::instrument(skip_all, fields(walrus.blob_id = %blob_id), err(level = Level::DEBUG))]
    pub async fn get_confirmation(
        &self,
        blob_id: &BlobId,
        blob_persistence_type: &BlobPersistenceType,
    ) -> Result<SignedStorageConfirmation, NodeError> {
        let (url, template) = self.endpoints.confirmation(blob_id, blob_persistence_type);
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
        blob_persistence_type: BlobPersistenceType,
    ) -> Result<SignedStorageConfirmation, NodeError> {
        let confirmation = self
            .get_confirmation(blob_id, &blob_persistence_type)
            .await?;
        let _ = confirmation
            .verify(public_key, epoch, *blob_id, blob_persistence_type)
            .map_err(NodeError::other)?;
        Ok(confirmation)
    }

    /// Gets a primary or secondary sliver for the identified sliver pair.
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
    ) -> Result<SliverData<A>, NodeError> {
        let (url, template) = self.endpoints.sliver::<A>(blob_id, sliver_pair_index);
        self.send_and_parse_bcs_response(Request::new(Method::GET, url), template)
            .await
    }

    /// Requests the status of a sliver from the node.
    #[tracing::instrument(
        skip_all,
        fields(
            walrus.blob_id = %blob_id,
            walrus.sliver.pair_index = %sliver_pair_index,
            walrus.sliver.r#type = A::NAME,
            ),
        err(level = Level::DEBUG))]
    pub async fn get_sliver_status<A: EncodingAxis>(
        &self,
        blob_id: &BlobId,
        sliver_pair_index: SliverPairIndex,
    ) -> Result<StoredOnNodeStatus, NodeError> {
        let (url, template) = self
            .endpoints
            .sliver_status::<A>(blob_id, sliver_pair_index);
        self.send_and_parse_service_response(Request::new(Method::GET, url), template)
            .await
    }

    /// Gets a primary or secondary sliver for the identified sliver pair.
    #[tracing::instrument(skip_all, err(level = Level::DEBUG))]
    pub async fn get_sliver_by_type(
        &self,
        blob_id: &BlobId,
        sliver_pair_index: SliverPairIndex,
        sliver_type: SliverType,
    ) -> Result<Sliver, NodeError> {
        match sliver_type {
            SliverType::Primary => self
                .get_sliver::<Primary>(blob_id, sliver_pair_index)
                .await
                .map(Sliver::Primary),
            SliverType::Secondary => self
                .get_sliver::<Secondary>(blob_id, sliver_pair_index)
                .await
                .map(Sliver::Secondary),
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
    ) -> Result<SliverData<A>, NodeError> {
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
    pub async fn get_recovery_symbol_legacy<A: EncodingAxis>(
        &self,
        blob_id: &BlobId,
        remote_sliver_pair: SliverPairIndex,
        local_sliver_pair: SliverPairIndex,
    ) -> Result<RecoverySymbol<A, MerkleProof>, NodeError> {
        let (url, template) = self.endpoints.legacy_recovery_symbol::<A>(
            blob_id,
            remote_sliver_pair,
            local_sliver_pair,
        );
        self.send_and_parse_bcs_response(Request::new(Method::GET, url), template)
            .await
    }

    /// Gets a recovery symbol that can be used to recover a sliver.
    #[tracing::instrument(
        skip_all,
        fields(walrus.blob_id = %blob_id, walrus.recovery.symbol_id = %symbol_id),
        err(level = Level::DEBUG)
    )]
    pub async fn get_recovery_symbol(
        &self,
        blob_id: &BlobId,
        symbol_id: SymbolId,
    ) -> Result<GeneralRecoverySymbol, NodeError> {
        let (url, template) = self.endpoints.recovery_symbol(blob_id, symbol_id);
        self.send_and_parse_bcs_response(Request::new(Method::GET, url), template)
            .await
    }

    /// Gets multiple recovery symbols.
    #[tracing::instrument(
        skip_all,
        fields(
            walrus.blob_id = %blob_id,
        ),
        err(level = Level::DEBUG)
    )]
    pub async fn list_recovery_symbols(
        &self,
        blob_id: &BlobId,
        filter: &RecoverySymbolsFilter,
    ) -> Result<Vec<GeneralRecoverySymbol>, NodeError> {
        let (url, template) = self.endpoints.list_recovery_symbols(blob_id);
        let request = self
            .inner
            .get(url)
            .query(&filter)
            .build()
            .expect("creating a URL from typed arguments should always succeed");
        self.send_and_parse_bcs_response(request, template).await
    }

    /// Gets and verifies multiple recovery symbols.
    #[tracing::instrument(
        skip_all, fields(walrus.blob_id = %metadata.blob_id(),), err(level = Level::DEBUG)
    )]
    pub async fn list_and_verify_recovery_symbols(
        &self,
        filter: RecoverySymbolsFilter,
        metadata: Arc<VerifiedBlobMetadataWithId>,
        encoding_config: Arc<EncodingConfig>,
        target_index: SliverIndex,
        target_type: SliverType,
    ) -> Result<Vec<GeneralRecoverySymbol>, NodeError> {
        let mut symbols = self
            .list_recovery_symbols(metadata.blob_id(), &filter)
            .await?;
        tracing::trace!(
            n_symbols = symbols.len(),
            "the server returned recovery symbols"
        );

        tokio::task::spawn_blocking(move || {
            let mut final_error =
                NodeError::other(ListAndVerifyRecoverySymbolsError::EmptyResponse);

            symbols.retain(|symbol| {
                let _guard = tracing::info_span!(
                    "list_and_verify_recovery_symbols__retain",
                    walrus.symbol.id = %symbol.id()
                )
                .entered();

                if !filter.accepts(symbol) {
                    tracing::warn!("server returned a symbol with an unrequested proof axis");
                    return false;
                }

                if let Err(error) = symbol.verify(
                    metadata.metadata(),
                    &encoding_config,
                    target_index,
                    target_type,
                ) {
                    tracing::warn!(?error, "recovery symbol verification failed");
                    final_error = NodeError::other(error);
                    return false;
                }

                true
            });

            if symbols.is_empty() {
                Err(final_error)
            } else {
                Ok(symbols)
            }
        })
        .await
        .map_err(|_| NodeError::other(ListAndVerifyRecoverySymbolsError::BackgroundWorkerFailed))?
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
            .get_recovery_symbol_legacy::<A>(
                metadata.blob_id(),
                remote_sliver_pair,
                local_sliver_pair,
            )
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
        sliver: &SliverData<A>,
    ) -> Result<(), NodeError> {
        tracing::trace!("starting to store sliver");
        let (url, template) = self.endpoints.sliver::<A>(blob_id, pair_index);
        let request = self.create_request_with_payload(Method::PUT, url, &sliver)?;
        self.send_and_parse_service_response::<String>(request, template)
            .await?;

        Ok(())
    }

    /// Stores a sliver on a node.
    #[tracing::instrument(skip_all, err(level = Level::DEBUG))]
    pub async fn store_sliver_by_type(
        &self,
        blob_id: &BlobId,
        pair_index: SliverPairIndex,
        sliver: &Sliver,
    ) -> Result<(), NodeError> {
        match sliver {
            Sliver::Primary(sliver) => self.store_sliver(blob_id, pair_index, sliver).await,
            Sliver::Secondary(sliver) => self.store_sliver(blob_id, pair_index, sliver).await,
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
        let request = self.create_request_with_payload(Method::POST, url, &inconsistency_proof)?;

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
    pub async fn get_server_health_info(
        &self,
        detailed: bool,
    ) -> Result<ServiceHealthInfo, NodeError> {
        let (url, template) = self.endpoints.server_health_info(detailed);
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
            parent: &Span::current(),
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
        self.send_request(request, url_template)
            .and_then(|(response, span)| response.bcs().instrument(span))
            .inspect_err(|error| tracing::trace!(?error))
            .await
    }

    async fn send_and_parse_service_response<T: DeserializeOwned>(
        &self,
        request: Request,
        url_template: &'static str,
    ) -> Result<T, NodeError> {
        self.send_request(request, url_template)
            .and_then(|(response, span)| response.service_response().instrument(span))
            .inspect_err(|error| tracing::trace!(?error))
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

impl Injector for HeaderInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        if let Ok(name) = HeaderName::from_bytes(key.as_bytes()) {
            if let Ok(val) = HeaderValue::from_str(&value) {
                self.0.insert(name, val);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use walrus_core::{encoding::Primary, test_utils, SuiObjectId};
    use walrus_test_utils::{param_test, Result as TestResult};

    use super::*;

    const BLOB_ID: BlobId = test_utils::blob_id_from_u64(99);

    param_test! {
        test_blob_url_endpoint: [
            blob: (|e| e.blob_resource(&BLOB_ID, ""), ""),
            metadata: (|e| e.metadata(&BLOB_ID).0, "metadata"),
            permanent_confirmation: (
                |e| e.confirmation(&BLOB_ID, &BlobPersistenceType::Permanent).0,
                "confirmation/permanent"
            ),
            deletable_confirmation: (
                |e| e.confirmation(
                    &BLOB_ID,
                    &BlobPersistenceType::Deletable { object_id: SuiObjectId([42; 32]) }
                ).0,
                concat!(
                    "confirmation/deletable/",
                    "0x2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a"
                )
            ),
            sliver: (|e| e.sliver::<Primary>(&BLOB_ID, SliverPairIndex(1)).0, "slivers/1/primary"),
            recovery_symbol: (
                |e| e.legacy_recovery_symbol::<Primary>(
                    &BLOB_ID, SliverPairIndex(1), SliverPairIndex(2)
                ).0,
                "slivers/1/primary/2"
            ),
            inconsistency_proof: (
                |e| e.inconsistency_proof::<Primary>(&BLOB_ID).0, "inconsistencyProof/primary"
            ),
        ]
    }
    fn test_blob_url_endpoint<F>(url_fn: F, expected_path: &str)
    where
        F: FnOnce(UrlEndpoints) -> Url,
    {
        let endpoints = UrlEndpoints(Url::parse("https://node.com").unwrap());
        let url = url_fn(endpoints);
        let expected = format!("https://node.com/v1/blobs/{BLOB_ID}/{expected_path}");

        assert_eq!(url.to_string(), expected);
    }

    param_test! {
        test_url_health_info_endpoint: [
            default: (false, "https://node.com/v1/health"),
            detailed: (true, "https://node.com/v1/health?detailed=true"),
        ]
    }
    fn test_url_health_info_endpoint(detailed: bool, expected_url: &str) {
        let endpoints = UrlEndpoints(Url::parse("https://node.com").unwrap());
        let (url, _) = endpoints.server_health_info(detailed);

        assert_eq!(url.to_string(), expected_url);
    }

    #[test]
    fn test_url_shard_sync_endpoint() {
        let endpoints = UrlEndpoints(Url::parse("https://node.com").unwrap());
        let (url, _) = endpoints.sync_shard();

        assert_eq!(url.to_string(), "https://node.com/v1/migrate/sync_shard");
    }

    param_test! {
        recovery_symbols_filter_to_query -> TestResult: [
            id_single: (
                RecoverySymbolsFilter::ids(vec![SymbolId::new(1.into(), 2.into())])
                    .unwrap()
                    .require_proof_from_axis(SliverType::Primary),
                "proofAxis=primary&id=1-2"
            ),
            id_multiple: (
                RecoverySymbolsFilter::ids(
                    vec![SymbolId::new(1.into(), 2.into()), SymbolId::new(3.into(), 4.into())],
                )
                .unwrap()
                .require_proof_from_axis(SliverType::Secondary),
                "proofAxis=secondary&id=1-2&id=3-4"
            ),
            all: (
                RecoverySymbolsFilter::recovers(SliverIndex(72), SliverType::Primary),
                "targetSliver=72&targetType=primary"
            ),
            all_with_proof_type: (
                RecoverySymbolsFilter::recovers(SliverIndex(18), SliverType::Secondary)
                    .require_proof_from_axis(SliverType::Primary),
                "proofAxis=primary&targetSliver=18&targetType=secondary"
            )
        ]
    }
    fn recovery_symbols_filter_to_query(
        filter: RecoverySymbolsFilter,
        expected_query: &str,
    ) -> TestResult {
        let request = reqwest::Client::new()
            .get("https://node.com")
            .query(&filter)
            .build()
            .expect("query should serialize successfully");

        assert_eq!(
            request.url().query().expect("query should be present"),
            expected_query
        );
        Ok(())
    }
}

// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Client for interacting with the StorageNode API.

use std::sync::Arc;

use fastcrypto::traits::{EncodeDecodeBase64, KeyPair};
use futures::TryFutureExt as _;
use middleware::{HttpClientMetrics, HttpMiddleware, UrlTemplate};
use reqwest::{Client as ReqwestClient, Method, Request, Response, Url, header::HeaderValue};
use serde::{Serialize, Serializer, de::DeserializeOwned};
use sui_types::base_types::ObjectID;
use tower::ServiceExt;
use tracing::Level;
use walrus_core::{
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
    encoding::{
        EncodingAxis,
        EncodingConfig,
        GeneralRecoverySymbol,
        Primary,
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
};

use crate::{
    api::{BlobStatus, ServiceHealthInfo, StoredOnNodeStatus},
    error::{ClientBuildError, ListAndVerifyRecoverySymbolsError, NodeError},
    node_response::NodeResponse,
};

mod builder;
pub use builder::StorageNodeClientBuilder;

mod middleware;

const METADATA_URL_TEMPLATE: &str = "/v1/blobs/:blob_id/metadata";
const METADATA_STATUS_URL_TEMPLATE: &str = "/v1/blobs/:blob_id/metadata/status";
const SLIVER_URL_TEMPLATE: &str = "/v1/blobs/:blob_id/slivers/:sliver_pair_index/:sliver_type";
const SLIVER_STATUS_TEMPLATE: &str =
    "/v1/blobs/:blob_id/slivers/:sliver_pair_index/:sliver_type/status";
const PERMANENT_BLOB_CONFIRMATION_URL_TEMPLATE: &str = "/v1/blobs/:blob_id/confirmation/permanent";
const DELETABLE_BLOB_CONFIRMATION_URL_TEMPLATE: &str =
    "/v1/blobs/:blob_id/confirmation/deletable/:object_id";
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

    fn recovery_symbol(&self, blob_id: &BlobId, symbol_id: SymbolId) -> (Url, &'static str) {
        (
            self.blob_resource(blob_id, &format!("recoverySymbols/{symbol_id}")),
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

/// Filter for [`StorageNodeClient::list_recovery_symbols()`] endpoint.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RecoverySymbolsFilter {
    /// The type of proof expected to be used within the symbol.
    proof_axis: Option<SliverType>,

    /// Specification of the symbol IDs.
    #[serde(flatten)]
    id_spec: SymbolIdFilter,
}

/// Filter for [`StorageNodeClient::list_recovery_symbols()`] endpoint.
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

/// A client for communicating with a StorageNode.
#[derive(Debug, Clone)]
pub struct StorageNodeClient {
    inner: HttpMiddleware<ReqwestClient>,
    endpoints: UrlEndpoints,

    /// A clone of the client used to create requests via the reqwest::RequestBuilder.
    ///
    /// This is needed, because the reqwest builder wants the client for the ergonmics of being
    /// able to send the request directly from the builder.
    client_clone: ReqwestClient,
}

impl StorageNodeClient {
    /// Returns a new [`StorageNodeClientBuilder`] that can be used to construct a client.
    pub fn builder() -> StorageNodeClientBuilder {
        StorageNodeClientBuilder::default()
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
    ) -> Result<StorageNodeClient, ClientBuildError> {
        Self::builder()
            .authenticate_with_public_key(public_key.clone())
            .build(address)
    }

    /// Converts this to the inner client.
    pub fn into_inner(self) -> ReqwestClient {
        self.inner.into_inner()
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
            .client_clone
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

    /// Stores the metadata on the node.
    #[tracing::instrument(
        skip_all, fields(walrus.blob_id = %metadata.blob_id()), err(level = Level::DEBUG)
    )]
    pub async fn store_metadata(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
    ) -> Result<(), NodeError> {
        let (url, template) = self.endpoints.metadata(metadata.blob_id());
        let request = self.create_request_with_payload(Method::PUT, url, metadata.as_ref());
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
        let request = self.create_request_with_payload(Method::PUT, url, &sliver);
        self.send_and_parse_service_response::<String>(request, template)
            .await
            .inspect_err(|error| tracing::debug!(?error, "error storing sliver"))?;
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
        let request = self.create_request_with_payload(Method::POST, url, &inconsistency_proof);

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
        request: Request,
        url_template: &'static str,
    ) -> Result<Response, NodeError> {
        let output = self
            .inner
            .clone()
            .oneshot((request, UrlTemplate(url_template)))
            .await;

        match output {
            Ok(response) => response.response_error_for_status().await,
            Err(err) => Err(NodeError::reqwest(err)),
        }
    }

    async fn send_and_parse_bcs_response<T: DeserializeOwned>(
        &self,
        request: Request,
        url_template: &'static str,
    ) -> Result<T, NodeError> {
        self.send_request(request, url_template)
            .and_then(|response| response.bcs())
            .inspect_err(|error| tracing::trace!(?error))
            .await
    }

    async fn send_and_parse_service_response<T: DeserializeOwned>(
        &self,
        request: Request,
        url_template: &'static str,
    ) -> Result<T, NodeError> {
        self.send_request(request, url_template)
            .and_then(|response| response.service_response())
            .inspect_err(|error| tracing::trace!(?error))
            .await
    }

    fn create_request_with_payload<T: Serialize>(
        &self,
        method: Method,
        url: Url,
        body: &T,
    ) -> Request {
        let mut request = Request::new(method, url);
        *request.body_mut() = Some(
            bcs::to_bytes(body)
                .expect("type must be bcs encodable")
                .into(),
        );
        request
    }

    // Creates a request with a payload and a public key in the Authorization header.
    fn create_request_with_payload_and_public_key<T: Serialize>(
        &self,
        method: Method,
        url: Url,
        body: &T,
        public_key: &PublicKey,
    ) -> Result<Request, NodeError> {
        let mut request = self.create_request_with_payload(method, url, body);
        let encoded_key = public_key.encode_base64();
        let public_key_header = HeaderValue::from_str(&encoded_key).map_err(NodeError::other)?;
        request
            .headers_mut()
            .insert(reqwest::header::AUTHORIZATION, public_key_header);
        Ok(request)
    }
}

#[cfg(test)]
mod tests {
    use walrus_core::{SuiObjectId, encoding::Primary, test_utils};
    use walrus_test_utils::{Result as TestResult, param_test};

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

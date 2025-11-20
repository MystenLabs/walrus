// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0
//! Services for communicating with Storage Nodes.
//!
//! This module defines the [`NodeService`] trait. It is a marker trait for a [`Clone`]-able
//! [`tower::Service`] trait implementation on the defined [`Request`] and [`Response`] types.
//!
//! It also defines [`RemoteStorageNode`] which implements the trait for
//! [`walrus_storage_node_client::client::Client`], and implements the trait for
//! [`LocalStorageNode`], an alias to [`Arc<StorageNodeInner>`][StorageNodeInner].
//!
//! The use of [`tower::Service`] will allow us to add layers to monitor a given node's
//! communication with all others, to monitor and disable nodes which fail frequently, and to later
//! apply back-pressure.
//
// NB: Ideally we would *additionally* have a single service trait which would represent the
// storage node. Both clients and servers would implement this trait. This would allow us to treat
// the remote and storage local nodes the same.
use std::{
    fmt::Debug,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use futures::{FutureExt, future::BoxFuture};
use tower::{Service, limit::ConcurrencyLimit};
use walrus_core::{
    BlobId, Epoch, InconsistencyProof as InconsistencyProofEnum, PublicKey, ShardIndex, Sliver,
    SliverIndex, SliverType,
    encoding::{EncodingConfig, GeneralRecoverySymbol, Primary, Secondary},
    keys::ProtocolKeyPair,
    messages::InvalidBlobIdAttestation,
    metadata::VerifiedBlobMetadataWithId,
};
use walrus_storage_node_client::{
    ClientBuildError, NodeError, RecoverySymbolsFilter, StorageNodeClient,
};
use walrus_sui::types::StorageNode as SuiStorageNode;
use walrus_utils::metrics::Registry;

use super::{DefaultRecoverySymbol, NodeServiceFactory};
use crate::node::config::defaults::REST_HTTP2_MAX_CONCURRENT_STREAMS;

/// Requests used with a [`NodeService`].
#[derive(Debug, Clone)]
pub(crate) enum Request {
    GetVerifiedMetadata(BlobId),
    SubmitProofForInvalidBlobAttestation {
        blob_id: BlobId,
        proof: InconsistencyProofEnum,
        epoch: Epoch,
        public_key: PublicKey,
    },
    SyncShardAsOfEpoch {
        shard: ShardIndex,
        starting_blob_id: BlobId,
        sliver_count: u64,
        sliver_type: SliverType,
        current_epoch: Epoch,
        key_pair: ProtocolKeyPair,
    },
    ListVerifiedRecoverySymbols {
        filter: RecoverySymbolsFilter,
        metadata: Arc<VerifiedBlobMetadataWithId>,
        target_index: SliverIndex,
        target_type: SliverType,
    },
}

/// Responses to [`Request`]s sent to a node service.
///
/// The convenience method [`into_value::<T>()`][Self::into_value] can be used to convert the
/// response into the expected response type.
#[derive(Debug)]
pub(crate) enum Response {
    VerifiedMetadata(VerifiedBlobMetadataWithId),
    VerifiedRecoverySymbol(DefaultRecoverySymbol),
    // Wrapped in a `Box` as this variant is very large and rarely used.
    InvalidBlobAttestation(Box<InvalidBlobIdAttestation>),
    ShardSlivers(Vec<(BlobId, Sliver)>),
    VerifiedRecoverySymbols(Vec<GeneralRecoverySymbol>),
}

impl Response {
    /// Convert a response to its inner type and panic if there is a mismatch.
    ///
    /// As responses correspond to the request, this should never panic except in the case of
    /// programmer error.
    pub fn into_value<T>(self) -> T
    where
        T: TryFrom<Self>,
        <T as TryFrom<Self>>::Error: std::fmt::Debug,
    {
        self.try_into()
            .expect("response must be of the correct type")
    }
}

#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("the response variant does not match the expected type")]
pub(crate) struct InvalidResponseVariant;

macro_rules! impl_response_conversion {
    ($type:ty, $($variant:tt)+) => {
        impl TryFrom<Response> for $type {
            type Error = InvalidResponseVariant;

            fn try_from(value: Response) -> Result<Self, Self::Error> {
                if let $($variant)+(inner) = value {
                    Ok(inner)
                } else {
                    Err(InvalidResponseVariant)
                }
            }
        }

        impl From<$type> for Response {
            fn from(value: $type) -> Self {
                $($variant)+(value)
            }
        }
    };
}

impl_response_conversion!(VerifiedBlobMetadataWithId, Response::VerifiedMetadata);
impl_response_conversion!(DefaultRecoverySymbol, Response::VerifiedRecoverySymbol);
impl_response_conversion!(
    Vec<GeneralRecoverySymbol>,
    Response::VerifiedRecoverySymbols
);
impl_response_conversion!(
    Box<InvalidBlobIdAttestation>,
    Response::InvalidBlobAttestation
);
impl_response_conversion!(Vec<(BlobId, Sliver)>, Response::ShardSlivers);

impl TryFrom<Response> for InvalidBlobIdAttestation {
    type Error = InvalidResponseVariant;

    fn try_from(value: Response) -> Result<Self, Self::Error> {
        if let Response::InvalidBlobAttestation(inner) = value {
            Ok(*inner)
        } else {
            Err(InvalidResponseVariant)
        }
    }
}

impl From<InvalidBlobIdAttestation> for Response {
    fn from(value: InvalidBlobIdAttestation) -> Self {
        Response::InvalidBlobAttestation(Box::new(value))
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum NodeServiceError {
    #[error(transparent)]
    Node(#[from] NodeError),
    #[allow(unused)]
    #[error(transparent)]
    Other(Box<dyn std::error::Error + Send + Sync>),
}

/// Marker trait for types implementing the [`tower::Service`] signature expected of services
/// used for communication with the committee.
pub(crate) trait NodeService
where
    Self: Send + Clone,
    Self: Service<Request, Response = Response, Error = NodeServiceError, Future: Send>,
{
}

impl<T> NodeService for T
where
    T: Send + Clone,
    T: Service<Request, Response = Response, Error = NodeServiceError, Future: Send>,
{
}

pub(crate) type RemoteStorageNode = ConcurrencyLimit<UnboundedRemoteStorageNode>;

/// A [`NodeService`] that is reachable via a [`walrus_storage_node_client::client::Client`].
#[derive(Clone, Debug)]
pub(crate) struct UnboundedRemoteStorageNode {
    client: StorageNodeClient,
    encoding_config: Arc<EncodingConfig>,
}

impl Service<Request> for UnboundedRemoteStorageNode {
    type Error = NodeServiceError;
    type Response = Response;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request) -> Self::Future {
        let client = self.client.clone();
        let encoding_config = self.encoding_config.clone();
        async move {
            let response = match req {
                Request::GetVerifiedMetadata(blob_id) => client
                    .get_and_verify_metadata(&blob_id, &encoding_config)
                    .await
                    .map(Response::VerifiedMetadata)?,

                Request::SubmitProofForInvalidBlobAttestation {
                    blob_id,
                    proof,
                    epoch,
                    public_key,
                } => client
                    .submit_inconsistency_proof_and_verify_attestation(
                        &blob_id,
                        &proof,
                        epoch,
                        &public_key,
                    )
                    .await
                    .map(Response::from)?,

                Request::SyncShardAsOfEpoch {
                    shard,
                    starting_blob_id,
                    sliver_count,
                    sliver_type,
                    current_epoch,
                    key_pair,
                } => {
                    let result = if sliver_type == SliverType::Primary {
                        client
                            .sync_shard::<Primary>(
                                shard,
                                starting_blob_id,
                                sliver_count,
                                current_epoch,
                                &key_pair,
                            )
                            .await
                    } else {
                        client
                            .sync_shard::<Secondary>(
                                shard,
                                starting_blob_id,
                                sliver_count,
                                current_epoch,
                                &key_pair,
                            )
                            .await
                    };
                    result.map(|value| Response::ShardSlivers(value.into()))?
                }

                Request::ListVerifiedRecoverySymbols {
                    filter,
                    metadata,
                    target_index,
                    target_type,
                } => client
                    .list_and_verify_recovery_symbols(
                        filter,
                        metadata.clone(),
                        encoding_config.clone(),
                        target_index,
                        target_type,
                    )
                    .await
                    .map(Response::VerifiedRecoverySymbols)?,
            };
            Ok(response)
        }
        .boxed()
    }
}

// TODO(jsmith): Define a LocalStorageNode that can be used within process.
// Such a service would need to hold only a `Weak` to `StorageNodeInner`, so as to avoid a memory
// leak due to cyclic Arcs.
//
// /// A *trusted* [`NodeService`] that can be communicated with within the process.
// pub(crate) type LocalStorageNode = Weak<StorageNodeInner>;

/// A [`NodeServiceFactory`] creating [`RemoteStorageNode`] services.
#[derive(Debug, Clone)]
pub(crate) struct DefaultNodeServiceFactory {
    /// If true, disables the use of proxies.
    ///
    /// This speeds up the construction of new instances.
    pub disable_use_proxy: bool,

    /// If true, disables the loading of native certificates.
    ///
    /// This speeds up the construction of new instances.
    pub disable_loading_native_certs: bool,

    /// The timeout to configure when connecting to remote nodes.
    pub connect_timeout: Option<Duration>,

    /// The registry to use for registering node metrics.
    pub registry: Option<Registry>,

    /// The maximum number of outstanding requests at a time.
    pub concurrency_limit: usize,
}

impl Default for DefaultNodeServiceFactory {
    fn default() -> Self {
        Self {
            disable_use_proxy: Default::default(),
            disable_loading_native_certs: Default::default(),
            connect_timeout: Default::default(),
            registry: Default::default(),
            concurrency_limit: usize::try_from(REST_HTTP2_MAX_CONCURRENT_STREAMS)
                .expect("number of concurrent streams fits in usize"),
        }
    }
}

impl DefaultNodeServiceFactory {
    /// Creates a new instance with metrics written to the provided registry.
    pub fn new_with_metrics(registry: Registry) -> Self {
        Self {
            registry: Some(registry),
            ..Self::default()
        }
    }

    /// Sets the timeout for connecting to nodes, for all subsequently created nodes.
    pub fn connect_timeout(&mut self, timeout: Duration) {
        self.connect_timeout = Some(timeout);
    }

    /// Skips the use of proxies or the loading of native certificates, as these require interacting
    /// with the operating system and can significantly slow down the construction of new instances.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn avoid_system_services() -> Self {
        Self {
            disable_use_proxy: true,
            disable_loading_native_certs: true,
            ..Self::default()
        }
    }
}

#[async_trait::async_trait]
impl NodeServiceFactory for DefaultNodeServiceFactory {
    type Service = RemoteStorageNode;

    async fn make_service(
        &mut self,
        member: &SuiStorageNode,
        encoding_config: &Arc<EncodingConfig>,
    ) -> Result<Self::Service, ClientBuildError> {
        let mut builder = walrus_storage_node_client::StorageNodeClient::builder()
            .authenticate_with_public_key(member.network_public_key.clone());

        if self.disable_loading_native_certs {
            builder = builder.tls_built_in_root_certs(false);
        }
        if self.disable_use_proxy {
            builder = builder.no_proxy();
        }
        if let Some(timeout) = self.connect_timeout.as_ref() {
            builder = builder.connect_timeout(*timeout);
        }
        if let Some(registry) = self.registry.as_ref() {
            builder = builder.metric_registry(registry.clone());
        }

        builder.build(&member.network_address.0).map(|client| {
            ConcurrencyLimit::new(
                UnboundedRemoteStorageNode {
                    client,
                    encoding_config: encoding_config.clone(),
                },
                self.concurrency_limit,
            )
        })
    }

    fn connect_timeout(&mut self, timeout: Duration) {
        self.connect_timeout(timeout);
    }
}

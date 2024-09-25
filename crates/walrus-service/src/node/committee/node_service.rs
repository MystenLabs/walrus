// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Services for communicating with Storage Nodes.
//!
//! This module defines the [`NodeService`] trait. It is a marker trait for a [`Clone`]-able
//! [`tower::Service`] trait implementation on the defined [`Request`] and [`Response`] types.
//!
//! It also defines [`RemoteStorageNode`] which implements the trait for
//! [`walrus_sdk::client::Client`], and implements the trait for [`LocalStorageNode`], an alias to
//! [`Arc<StorageNodeInner>`][StorageNodeInner].
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
    future::Future,
    sync::Arc,
    task::{Context, Poll},
};

use futures::{future::BoxFuture, FutureExt};
use tower::Service;
use walrus_core::{
    encoding::{EncodingConfig, Primary, Secondary},
    keys::ProtocolKeyPair,
    messages::InvalidBlobIdAttestation,
    metadata::VerifiedBlobMetadataWithId,
    BlobId,
    Epoch,
    InconsistencyProof as InconsistencyProofEnum,
    PublicKey,
    ShardIndex,
    Sliver,
    SliverPairIndex,
    SliverType,
};
use walrus_sdk::{
    client::Client,
    error::{ClientBuildError, NodeError},
};
use walrus_sui::types::StorageNode as SuiStorageNode;

use super::DefaultRecoverySymbol;

/// Requests used with a [`NodeService`].
#[derive(Debug, Clone)]
pub(crate) enum Request {
    GetVerifiedMetadata(BlobId),
    GetVerifiedRecoverySymbol {
        sliver_type: SliverType,
        metadata: Arc<VerifiedBlobMetadataWithId>,
        sliver_pair_at_remote: SliverPairIndex,
        intersecting_pair_index: SliverPairIndex,
    },
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
}

/// Responses to [`Request`]s sent to a node service.
///
/// The convenience method [`into_value::<T>()`][Self::into_value] can be used to convert the
/// response into the expected response type.
#[derive(Debug)]
pub(crate) enum Response {
    VerifiedMetadata(VerifiedBlobMetadataWithId),
    VerifiedRecoverySymbol(DefaultRecoverySymbol),
    InvalidBlobAttestation(InvalidBlobIdAttestation),
    ShardSlivers(Vec<(BlobId, Sliver)>),
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
impl_response_conversion!(InvalidBlobIdAttestation, Response::InvalidBlobAttestation);
impl_response_conversion!(Vec<(BlobId, Sliver)>, Response::ShardSlivers);

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

/// A [`NodeService`] that is reachable via a [`walrus_sdk::client::Client`].
#[derive(Clone, Debug)]
pub(crate) struct RemoteStorageNode {
    client: Client,
    encoding_config: Arc<EncodingConfig>,
}

impl Service<Request> for RemoteStorageNode {
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

                Request::GetVerifiedRecoverySymbol {
                    sliver_type,
                    metadata,
                    sliver_pair_at_remote,
                    intersecting_pair_index,
                } => {
                    let symbol = if sliver_type == SliverType::Primary {
                        client
                            .get_and_verify_recovery_symbol::<Primary>(
                                &metadata,
                                &encoding_config,
                                sliver_pair_at_remote,
                                intersecting_pair_index,
                            )
                            .await
                            .map(DefaultRecoverySymbol::Primary)
                    } else {
                        client
                            .get_and_verify_recovery_symbol::<Secondary>(
                                &metadata,
                                &encoding_config,
                                sliver_pair_at_remote,
                                intersecting_pair_index,
                            )
                            .await
                            .map(DefaultRecoverySymbol::Secondary)
                    };
                    symbol.map(Response::VerifiedRecoverySymbol)?
                }

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
pub(crate) fn default_node_service_factory<'a>(
    member: &'a SuiStorageNode,
    encoding_config: &'a Arc<EncodingConfig>,
) -> impl Future<Output = Result<RemoteStorageNode, ClientBuildError>> + 'static {
    let result = walrus_sdk::client::Client::for_storage_node(
        &member.network_address.host,
        member.network_address.port,
        &member.network_public_key,
    )
    .map(|client| RemoteStorageNode {
        client,
        encoding_config: encoding_config.clone(),
    });

    std::future::ready(result)
}

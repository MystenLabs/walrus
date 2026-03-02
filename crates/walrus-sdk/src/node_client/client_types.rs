// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Blob lifecycle in the client.

use std::{fmt::Debug, sync::Arc};

use serde::{Deserialize, Serialize};
use tracing::{Level, Span, field};
use utoipa::ToSchema;
use walrus_core::{
    BlobId,
    EpochCount,
    QuiltPatchId,
    encoding::{
        EncodingConfigEnum,
        EncodingFactory as _,
        SliverPair,
        quilt_encoding::{QuiltIndexApi as _, QuiltPatchApi as _, QuiltPatchInternalIdApi},
    },
    messages::ConfirmationCertificate,
    metadata::{BlobMetadataApi, QuiltIndex, VerifiedBlobMetadataWithId},
};
use walrus_storage_node_client::api::BlobStatus;
use walrus_sui::{
    client::{CertifyAndExtendBlobParams, CertifyAndExtendBlobResult},
    types::{Blob, move_structs::BlobAttribute},
};

use super::{
    ClientError,
    ClientResult,
    resource::{PriceComputation, RegisterBlobOp},
    responses::{BlobStoreResult, EventOrObjectId},
};
use crate::{
    node_client::{store_args::StoreArgs, upload_relay_client::UploadRelayClient},
    utils::Either,
};

/// The log level for all WalrusStoreBlob spans.
const BLOB_SPAN_LEVEL: Level = Level::DEBUG;

/// The trait for the blob state to manage the state-dependent data.
pub trait WalrusStoreBlobStateApi: Debug + Send + Sync + PartialEq {
    /// The state of the blob.
    fn state(&self) -> &'static str;

    /// Returns the blob ID of the blob if it is already computed.
    fn maybe_blob_id(&self) -> Option<BlobId>;
}

/// The trait for the encoded blob to manage the encoded blob state-dependent data.
pub trait WalrusStoreEncodedBlobApi: Debug + Send + Sync + PartialEq {
    /// The state of the blob.
    const STATE: &'static str;

    /// Returns the blob ID of the blob.
    fn blob_id(&self) -> BlobId;
}

impl<T> WalrusStoreBlobStateApi for T
where
    T: WalrusStoreEncodedBlobApi,
{
    fn state(&self) -> &'static str {
        T::STATE
    }

    fn maybe_blob_id(&self) -> Option<BlobId> {
        Some(self.blob_id())
    }
}

impl WalrusStoreBlobStateApi for BlobStoreResult {
    fn state(&self) -> &'static str {
        "Finished"
    }

    fn maybe_blob_id(&self) -> Option<BlobId> {
        self.blob_id()
    }
}

impl<S> From<BlobStoreResult> for WalrusStoreBlobState<S> {
    fn from(value: BlobStoreResult) -> Self {
        WalrusStoreBlobState::Finished(value)
    }
}

/// The state-independent parameters of a blob.
#[derive(Clone, PartialEq)]
pub struct CommonBlobParameters {
    /// A unique identifier for this blob.
    pub identifier: String,
    /// The span for this blob's lifecycle.
    span: Span,
    /// The attribute of the blob.
    pub attribute: BlobAttribute,
    /// The unencoded length of the blob.
    pub unencoded_length: usize,
    /// The encoding config used to encode the blob.
    pub encoding_config: EncodingConfigEnum,
}

impl CommonBlobParameters {
    /// Returns the encoded size of the blob.
    pub fn encoded_size(&self) -> Option<u64> {
        self.encoding_config
            .encoded_blob_length_from_usize(self.unencoded_length)
    }
}

impl Debug for CommonBlobParameters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommonBlobParameters")
            .field("identifier", &self.identifier)
            .field("attribute", &self.attribute)
            .field("unencoded_length", &self.unencoded_length)
            .field("encoding_config", &self.encoding_config)
            .finish()
    }
}

/// A blob that is being stored in Walrus, containing the common fields for all states and some
/// state-dependent data.
#[derive(Clone, PartialEq, Debug)]
pub struct WalrusStoreBlob<S> {
    /// The state-independent parameters of the blob.
    pub common: CommonBlobParameters,
    /// The state-dependent data of the blob.
    pub state: S,
}

/// A blob that is being stored in Walrus, in a particular state.
pub type WalrusStoreBlobUnfinished<S = UnencodedBlob> = WalrusStoreBlob<S>;

/// A blob that is being stored in Walrus, which can be in a particular state or finished
/// (successfully or with an error).
///
/// See also [`WalrusStoreBlobState`].
pub type WalrusStoreBlobMaybeFinished<S = UnencodedBlob> = WalrusStoreBlob<WalrusStoreBlobState<S>>;

/// A blob that is being stored in Walrus, and has been finished (successfully or with an error).
pub type WalrusStoreBlobFinished = WalrusStoreBlob<BlobStoreResult>;

impl<S: WalrusStoreBlobStateApi> From<WalrusStoreBlobUnfinished<S>>
    for WalrusStoreBlobMaybeFinished<S>
{
    fn from(value: WalrusStoreBlobUnfinished<S>) -> Self {
        value.into_maybe_finished()
    }
}

impl<S: WalrusStoreBlobStateApi> WalrusStoreBlobUnfinished<S> {
    /// A state-transition helper that transforms the blob state into a new one using a function
    /// that cannot fail.
    ///
    /// For a more general transition, use [`Self::into_maybe_finished`] and [`Self::map`].
    pub(crate) fn map_infallible<T: WalrusStoreBlobStateApi>(
        self,
        f: impl Fn(S) -> T,
        phase: &'static str,
    ) -> WalrusStoreBlob<T> {
        let was_blob_id_known = self.state.maybe_blob_id().is_some();
        let new_state = self.common.span.in_scope(|| {
            tracing::event!(BLOB_SPAN_LEVEL, phase, "entering map");

            let new_state = f(self.state);
            if !was_blob_id_known && let Some(blob_id) = new_state.maybe_blob_id() {
                self.common.span.record("blob_id", blob_id.to_string());
            }
            new_state
        });

        let new_blob = WalrusStoreBlob {
            common: self.common,
            state: new_state,
        };
        new_blob.log_state("map completed");
        new_blob
    }

    /// A state-transition helper that transforms the blob state into one of two possible new
    /// states.
    pub(crate) fn map_either<T, U, F>(
        self,
        f: F,
        phase: &'static str,
    ) -> Either<WalrusStoreBlobUnfinished<T>, WalrusStoreBlobUnfinished<U>>
    where
        T: WalrusStoreBlobStateApi,
        U: WalrusStoreBlobStateApi,
        F: Fn(S) -> Either<T, U>,
    {
        let new_state = self.common.span.in_scope(|| {
            tracing::event!(BLOB_SPAN_LEVEL, phase, "entering map");

            f(self.state)
        });

        match new_state {
            Either::Left(new_state) => Either::Left({
                let result = WalrusStoreBlob {
                    common: self.common,
                    state: new_state,
                };
                result.log_state("map_either completed");
                result
            }),
            Either::Right(new_state) => Either::Right({
                let result = WalrusStoreBlob {
                    common: self.common,
                    state: new_state,
                };
                result.log_state("map_either completed");
                result
            }),
        }
    }

    /// Converts the blob state into a new blob state that is either unfinished or finished.
    ///
    /// This can be used to subsequently apply a fallible state transition.
    pub(crate) fn into_maybe_finished(self) -> WalrusStoreBlobMaybeFinished<S> {
        WalrusStoreBlobMaybeFinished {
            common: self.common,
            state: WalrusStoreBlobState::new(self.state),
        }
    }

    /// Converts the blob state into a new blob state that is finished with the given error.
    pub(crate) fn fail_with<T: WalrusStoreBlobStateApi>(
        self,
        error: ClientError,
        failure_phase: &str,
    ) -> WalrusStoreBlobMaybeFinished<T> {
        WalrusStoreBlobMaybeFinished {
            common: self.common,
            state: WalrusStoreBlobState::new_failure(
                error,
                failure_phase,
                self.state.maybe_blob_id(),
            ),
        }
    }
}

impl<S: WalrusStoreBlobStateApi> WalrusStoreBlobMaybeFinished<S> {
    /// A state-transition helper that transforms the blob state into a new one using a function
    /// that can fail.
    pub(crate) fn map<T, F>(
        self,
        f: F,
        phase: &'static str,
    ) -> ClientResult<WalrusStoreBlobMaybeFinished<T>>
    where
        T: WalrusStoreBlobStateApi,
        F: FnOnce(S) -> Result<T, ClientError>,
    {
        let was_blob_id_known = self.state.maybe_blob_id().is_some();
        let new_state =
            self.common
                .span
                .in_scope(|| -> ClientResult<WalrusStoreBlobState<T>> {
                    tracing::event!(BLOB_SPAN_LEVEL, phase, "entering map");

                    let new_state = self.state.map(f, phase)?;
                    if !was_blob_id_known && let Some(blob_id) = new_state.maybe_blob_id() {
                        self.common.span.record("blob_id", blob_id.to_string());
                    }
                    Ok(new_state)
                })?;

        let new_blob = WalrusStoreBlobMaybeFinished {
            common: self.common,
            state: new_state,
        };
        new_blob.log_state("map completed");
        Ok(new_blob)
    }

    /// Checks if the blob state is finished and converts it to a finished blob state if it is.
    ///
    /// If the blob state is unfinished, the original blob is returned as a
    /// [`WalrusStoreBlobUnfinished`].
    pub(crate) fn try_finish<T: From<BlobStoreResult>>(
        self,
    ) -> Result<WalrusStoreBlob<T>, WalrusStoreBlobUnfinished<S>> {
        let WalrusStoreBlob { common, state } = self;
        match state {
            WalrusStoreBlobState::Unfinished(state) => {
                Err(WalrusStoreBlobUnfinished { common, state })
            }
            WalrusStoreBlobState::Finished(result) => Ok(WalrusStoreBlob {
                common,
                state: result.into(),
            }),
        }
    }
}

/// Partitions a vector of maybe finished blobs into unfinished and finished blobs.
pub fn partition_unfinished_finished<S, T>(
    maybe_finished_blobs: Vec<WalrusStoreBlobMaybeFinished<S>>,
) -> (Vec<WalrusStoreBlobUnfinished<S>>, Vec<WalrusStoreBlob<T>>)
where
    S: WalrusStoreBlobStateApi,
    T: From<BlobStoreResult>,
{
    let mut unfinished_blobs = Vec::new();
    let mut finished_blobs = Vec::new();
    for blob in maybe_finished_blobs {
        match blob.try_finish() {
            Ok(blob) => finished_blobs.push(blob),
            Err(blob) => unfinished_blobs.push(blob),
        }
    }
    (unfinished_blobs, finished_blobs)
}

impl<S: WalrusStoreBlobStateApi> WalrusStoreBlob<S> {
    /// Logs the current state with the provided message.
    fn log_state(&self, message: &'static str) {
        tracing::event!(BLOB_SPAN_LEVEL, state = ?self.state.state(), message);
    }
}

/// A blob that is being stored in Walrus, representing its current phase in the lifecycle.
#[derive(Debug, Clone, PartialEq)]
pub enum WalrusStoreBlobState<S> {
    /// A blob in a specific state.
    Unfinished(S),
    /// Final phase with the complete result.
    Finished(BlobStoreResult),
}

impl<S: WalrusStoreBlobStateApi> WalrusStoreBlobStateApi for WalrusStoreBlobState<S> {
    fn state(&self) -> &'static str {
        match self {
            Self::Unfinished(state) => state.state(),
            Self::Finished(result) => result.state(),
        }
    }

    fn maybe_blob_id(&self) -> Option<BlobId> {
        match self {
            Self::Unfinished(state) => state.maybe_blob_id(),
            Self::Finished(result) => result.maybe_blob_id(),
        }
    }
}

impl<S: WalrusStoreEncodedBlobApi> WalrusStoreEncodedBlobApi for WalrusStoreBlobUnfinished<S> {
    const STATE: &'static str = S::STATE;

    fn blob_id(&self) -> BlobId {
        self.state.blob_id()
    }
}

impl<S> WalrusStoreBlobState<S> {
    /// Creates a new blob state.
    pub fn new(state: S) -> Self {
        Self::Unfinished(state)
    }

    /// Creates a new blob state with an error.
    pub fn new_failure(error: ClientError, failure_phase: &str, blob_id: Option<BlobId>) -> Self {
        Self::Finished(BlobStoreResult::Error {
            blob_id,
            failure_phase: failure_phase.to_string(),
            error_msg: error.to_string(),
        })
    }
}
impl<S: WalrusStoreBlobStateApi> WalrusStoreBlobState<S> {
    /// Maps the blob state to a new blob state with the given function.
    ///
    /// If the blob state is already in a [WalrusStoreBlobState::Finished], it is returned
    /// unchanged. If it is [WalrusStoreBlobState::Unfinished], the function is applied to the
    /// state. If the function returns an error, the blob state is set to
    /// [WalrusStoreBlobState::Finished] with an error, unless the error indicates that the
    /// operation should fail immediately, in which case the error is returned.
    pub(crate) fn map<T, F>(
        self,
        f: F,
        phase: &'static str,
    ) -> ClientResult<WalrusStoreBlobState<T>>
    where
        F: FnOnce(S) -> Result<T, ClientError>,
    {
        Ok(match self {
            Self::Unfinished(state) => {
                let blob_id = state.maybe_blob_id();
                match f(state) {
                    Ok(result) => WalrusStoreBlobState::Unfinished(result),
                    Err(error) => {
                        if should_fail_early(&error) {
                            return Err(error);
                        }

                        WalrusStoreBlobState::new_failure(error, phase, blob_id)
                    }
                }
            }
            Self::Finished(result) => WalrusStoreBlobState::Finished(result),
        })
    }
}

/// Returns true if we should fail early based on the error.
fn should_fail_early(error: &ClientError) -> bool {
    error.may_be_caused_by_epoch_change() || error.is_no_valid_status_received()
}

impl WalrusStoreBlobMaybeFinished {
    /// Creates a new unencoded blob.
    pub fn new_unencoded(
        blob: Vec<u8>,
        identifier: String,
        attribute: BlobAttribute,
        encoding_config: EncodingConfigEnum,
    ) -> Self {
        let span = tracing::span!(
            BLOB_SPAN_LEVEL,
            "store_blob_tracing",
            blob_id = field::Empty,
            identifier,
        );

        WalrusStoreBlobMaybeFinished {
            common: CommonBlobParameters {
                identifier,
                span,
                attribute,
                unencoded_length: blob.len(),
                encoding_config,
            },
            state: WalrusStoreBlobState::new(UnencodedBlob {
                unencoded_data: blob,
            }),
        }
    }

    /// Create a list of UnencodedBlobs with default identifiers in the form of "blob_{:06}".
    ///
    /// The `attributes` vector must be either empty or have the same length as the `blobs` vector.
    /// If it is empty, a default (empty) attribute will be used for each blob.
    ///
    /// The length of the output vector is the same as the input vector.
    pub(crate) fn unencoded_blobs_with_default_identifiers(
        blobs: Vec<Vec<u8>>,
        mut attributes: Vec<BlobAttribute>,
        encoding_config: EncodingConfigEnum,
    ) -> Vec<Self> {
        let blobs_count = blobs.len();
        if attributes.is_empty() {
            attributes = vec![BlobAttribute::default(); blobs_count];
        }
        assert!(attributes.len() == blobs_count);

        blobs
            .into_iter()
            .zip(attributes)
            .enumerate()
            .map(|(i, (blob, attribute))| {
                Self::new_unencoded(blob, format!("blob_{i:06}"), attribute, encoding_config)
            })
            .collect()
    }
}

impl<S: WalrusStoreBlobStateApi> WalrusStoreBlobMaybeFinished<S> {
    /// Returns true if the store blob operation is completed.
    pub fn is_finished(&self) -> bool {
        matches!(self.state, WalrusStoreBlobState::Finished(..))
    }
}

/// A blob before it is encoded.
#[derive(Clone, PartialEq)]
pub struct UnencodedBlob {
    /// The raw blob data to be stored.
    // TODO(WAL-1093): Consider using a `Bytes` object here instead.
    pub unencoded_data: Vec<u8>,
}

impl Debug for UnencodedBlob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnencodedBlob")
            .field("blob_len", &self.unencoded_data.len())
            .field(
                "data_prefix",
                &walrus_core::utils::data_prefix_string(&self.unencoded_data, 5),
            )
            .finish()
    }
}

impl WalrusStoreBlobStateApi for UnencodedBlob {
    fn state(&self) -> &'static str {
        "Unencoded"
    }

    fn maybe_blob_id(&self) -> Option<BlobId> {
        None
    }
}

impl UnencodedBlob {
    /// Encodes the blob using the given encoding config.
    ///
    /// If no upload relay client is provided, the encoded blob contains all sliver pairs.
    /// Otherwise, the encoded blob contains the unencoded blob data to be sent to the upload relay.
    pub fn encode(
        self,
        encoding_config: EncodingConfigEnum,
        upload_relay_client: Option<Arc<UploadRelayClient>>,
    ) -> ClientResult<EncodedBlob> {
        tracing::event!(BLOB_SPAN_LEVEL, "encoding the blob");

        let (metadata, data) = if let Some(upload_relay_client) = upload_relay_client {
            let metadata = encoding_config
                .compute_metadata(&self.unencoded_data)
                .map_err(ClientError::other)?;
            (
                metadata,
                BlobData::BlobForUploadRelay(Arc::new(self.unencoded_data), upload_relay_client),
            )
        } else {
            let (pairs, metadata) = encoding_config
                .encode_with_metadata(self.unencoded_data)
                .map_err(ClientError::other)?;
            (metadata, BlobData::SliverPairs(Arc::new(pairs)))
        };

        Ok(EncodedBlob {
            metadata: Arc::new(metadata),
            data,
        })
    }
}

/// The data of the blob to be stored.
///
/// This can either be sliver pairs to be sent directly to storage nodes, or the raw blob data to be
/// sent to the upload relay (together with the upload relay client).
#[derive(Clone, PartialEq)]
pub enum BlobData {
    /// The encoded sliver pairs generated from the blob to be sent directly to storage nodes.
    SliverPairs(Arc<Vec<SliverPair>>),
    /// The raw blob data to be sent to the upload relay (together with the upload relay client).
    BlobForUploadRelay(Arc<Vec<u8>>, Arc<UploadRelayClient>),
}

impl Debug for BlobData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SliverPairs(sliver_pairs) => f
                .debug_struct("BlobData::SliverPairs")
                .field("count", &sliver_pairs.len())
                .finish(),

            Self::BlobForUploadRelay(blob, upload_relay_client) => f
                .debug_struct("BlobData::BlobForUploadRelay")
                .field("blob_len", &blob.len())
                .field("upload_relay_client", &upload_relay_client)
                .finish(),
        }
    }
}

/// Encoded blob.
#[derive(Debug, Clone, PartialEq)]
pub struct EncodedBlob {
    /// The verified metadata associated with the encoded blob.
    pub metadata: Arc<VerifiedBlobMetadataWithId>,
    /// The data of the blob.
    pub data: BlobData,
}

impl WalrusStoreEncodedBlobApi for EncodedBlob {
    const STATE: &'static str = "Encoded";

    fn blob_id(&self) -> BlobId {
        *self.metadata.blob_id()
    }
}

impl EncodedBlob {
    /// Converts the encoded blob to a blob with status information.
    pub fn with_status(
        self,
        status: Result<BlobStatus, ClientError>,
    ) -> ClientResult<BlobWithStatus> {
        tracing::event!(BLOB_SPAN_LEVEL, ?status, "entering with_status");

        Ok(BlobWithStatus {
            encoded_blob: self,
            status: status?,
        })
    }
}

/// Encoded blob with status information.
#[derive(Debug, Clone, PartialEq)]
pub struct BlobWithStatus {
    /// The encoded blob.
    pub encoded_blob: EncodedBlob,
    /// The current status of the blob in the system.
    pub status: BlobStatus,
}

impl WalrusStoreEncodedBlobApi for BlobWithStatus {
    const STATE: &'static str = "WithStatus";

    fn blob_id(&self) -> BlobId {
        self.encoded_blob.blob_id()
    }
}

impl BlobWithStatus {
    /// Converts the blob with status information to a registered blob based on the given blob
    /// object and operation.
    pub fn with_register_result(
        self,
        blob_object: Blob,
        operation: RegisterBlobOp,
    ) -> WalrusStoreBlobState<RegisteredBlob> {
        tracing::event!(
            BLOB_SPAN_LEVEL,
            ?blob_object,
            ?operation,
            "entering with_register_result"
        );

        if blob_object.is_certified() && operation.is_reuse_registration() {
            WalrusStoreBlobState::Finished(BlobStoreResult::AlreadyCertified {
                blob_id: blob_object.blob_id,
                event_or_object: EventOrObjectId::Object(blob_object.id),
                end_epoch: blob_object.storage.end_epoch,
            })
        } else {
            WalrusStoreBlobState::Unfinished(RegisteredBlob {
                encoded_blob: self.encoded_blob,
                status: self.status,
                blob_object,
                operation,
            })
        }
    }
}

impl WalrusStoreBlobMaybeFinished<BlobWithStatus> {
    /// Tries to complete the blob if it is certified beyond the given epoch.
    pub fn try_complete_if_certified_beyond_epoch(
        &mut self,
        target_epoch: u32,
    ) -> ClientResult<&mut Self> {
        let _guard = self.common.span.enter();
        let WalrusStoreBlobState::Unfinished(ref this) = self.state else {
            tracing::event!(
                BLOB_SPAN_LEVEL,
                ?self,
                "skipping try_complete_if_certified_beyond_epoch for already completed blob"
            );
            drop(_guard);
            return Ok(self);
        };

        let status = this.status;
        let blob_id = this.encoded_blob.blob_id();
        match status {
            BlobStatus::Permanent {
                end_epoch,
                is_certified: true,
                status_event,
                ..
            } if end_epoch >= target_epoch => {
                self.state = WalrusStoreBlobState::Finished(BlobStoreResult::AlreadyCertified {
                    blob_id,
                    event_or_object: EventOrObjectId::Event(status_event),
                    end_epoch,
                });
            }
            BlobStatus::Invalid { event } => {
                self.state = WalrusStoreBlobState::Finished(BlobStoreResult::MarkedInvalid {
                    blob_id,
                    event,
                });
            }
            _ => (),
        };

        self.log_state("try_complete_if_certified_beyond_epoch completed");
        drop(_guard);
        Ok(self)
    }

    /// Returns the metadata and sliver pairs needed for a pending upload if eligible.
    pub fn pending_upload_payload(
        &self,
        pending_uploads_enabled: bool,
        store_args: &StoreArgs,
        pending_upload_max_blob_bytes: u64,
    ) -> Option<(VerifiedBlobMetadataWithId, Arc<Vec<SliverPair>>)> {
        let WalrusStoreBlobState::Unfinished(blob_with_status) = &self.state else {
            return None;
        };
        if blob_with_status.status.is_registered()
            || !pending_uploads_enabled
            || !store_args.store_optimizations.pending_uploads_enabled()
        {
            return None;
        }
        let encoded = &blob_with_status.encoded_blob;
        let BlobData::SliverPairs(sliver_pairs) = &encoded.data else {
            return None;
        };
        let unencoded_len = encoded.metadata.metadata().unencoded_length();
        if unencoded_len > pending_upload_max_blob_bytes {
            return None;
        }

        Some((encoded.metadata.as_ref().clone(), sliver_pairs.clone()))
    }
}

/// Registered blob.
#[derive(Debug, Clone, PartialEq)]
pub struct RegisteredBlob {
    /// The encoded blob.
    pub encoded_blob: EncodedBlob,
    /// The current status of the blob in the system.
    pub status: BlobStatus,
    /// The blob object that was registered.
    pub blob_object: Blob,
    /// The operation that is performed to store the blob.
    pub operation: RegisterBlobOp,
}

impl WalrusStoreEncodedBlobApi for RegisteredBlob {
    const STATE: &'static str = "Registered";

    fn blob_id(&self) -> BlobId {
        self.encoded_blob.blob_id()
    }
}

impl RegisteredBlob {
    /// Classifies the registered blob into either a blob that needs to be certified or a blob that
    /// is ready to be extended.
    pub fn classify(self) -> Either<BlobAwaitingUpload, BlobPendingCertifyAndExtend> {
        match self.operation {
            RegisterBlobOp::ReuseAndExtend {
                epochs_extended, ..
            } => Either::Right(BlobPendingCertifyAndExtend {
                blob_object: self.blob_object,
                operation: self.operation,
                certificate: None,
                epochs_extended: Some(epochs_extended),
            }),
            _ => {
                let epochs_extended = if let RegisterBlobOp::ReuseAndExtendNonCertified {
                    epochs_extended,
                    ..
                } = &self.operation
                {
                    Some(*epochs_extended)
                } else {
                    None
                };
                Either::Left(BlobAwaitingUpload {
                    encoded_blob: self.encoded_blob,
                    status: self.status,
                    blob_object: self.blob_object,
                    operation: self.operation,
                    epochs_extended,
                })
            }
        }
    }
}

/// A blob that needs to be certified.
// INV: The operation is NOT `ReuseAndExtend`.
// INV: The epochs_extended is `Some` iff the operation is `ReuseAndExtendNonCertified`.
#[derive(Debug, Clone, PartialEq)]
pub struct BlobAwaitingUpload {
    /// The encoded blob.
    pub encoded_blob: EncodedBlob,
    /// The current status of the blob in the system.
    pub status: BlobStatus,
    /// The blob object that was registered.
    pub blob_object: Blob,
    /// The operation that is performed to store the blob.
    pub operation: RegisterBlobOp,
    /// The number of epochs by which to extend the blob after certification.
    pub epochs_extended: Option<EpochCount>,
}

impl WalrusStoreBlobUnfinished<BlobAwaitingUpload> {
    /// Converts the blob to a blob ready for certification and (optionally) extension based on the
    /// given certificate result.
    pub fn with_certificate_result(
        self,
        certificate: ClientResult<ConfirmationCertificate>,
    ) -> ClientResult<WalrusStoreBlobMaybeFinished<BlobPendingCertifyAndExtend>> {
        let certificate = certificate?;
        self.into_maybe_finished().map(
            move |blob| {
                Ok(BlobPendingCertifyAndExtend {
                    blob_object: blob.blob_object,
                    operation: blob.operation,
                    certificate: Some(Arc::new(certificate)),
                    epochs_extended: blob.epochs_extended,
                })
            },
            "with_certificate_result",
        )
    }
}

impl WalrusStoreEncodedBlobApi for BlobAwaitingUpload {
    const STATE: &'static str = "AwaitingUpload";

    fn blob_id(&self) -> BlobId {
        self.encoded_blob.blob_id()
    }
}

/// A blob that is ready to be certified and/or extended.
// INV: The certificate is `None` iff the operation is `ReuseAndExtend`.
// INV: The epochs_extended is `Some` iff the operation is `ReuseAndExtend` or
// `ReuseAndExtendNonCertified`.
#[derive(Clone, PartialEq, Debug)]
pub struct BlobPendingCertifyAndExtend {
    /// The blob object that was registered.
    pub blob_object: Blob,
    /// The operation that is performed to store the blob.
    pub operation: RegisterBlobOp,
    /// The certificate for the blob.
    pub certificate: Option<Arc<ConfirmationCertificate>>,
    /// The number of epochs by which to extend the blob.
    pub epochs_extended: Option<EpochCount>,
}

impl WalrusStoreEncodedBlobApi for BlobPendingCertifyAndExtend {
    const STATE: &'static str = "PendingCertifyAndExtend";

    fn blob_id(&self) -> BlobId {
        self.blob_object.blob_id
    }
}

impl<'a> WalrusStoreBlobUnfinished<BlobPendingCertifyAndExtend> {
    /// Returns the parameters for the Sui transaction to certify and extend the blob.
    pub fn get_certify_and_extend_params(&'a self) -> CertifyAndExtendBlobParams<'a> {
        CertifyAndExtendBlobParams {
            blob: &self.state.blob_object,
            attribute: &self.common.attribute,
            certificate: self.state.certificate.clone(),
            epochs_extended: self.state.epochs_extended,
        }
    }
}

impl BlobPendingCertifyAndExtend {
    /// Converts the blob to a blob store result based on the given certify and extend result and
    /// price computation.
    pub fn with_certify_and_extend_result(
        self,
        certify_and_extend_result: Option<&CertifyAndExtendBlobResult>,
        price_computation: &PriceComputation,
    ) -> BlobStoreResult {
        let Some(certify_and_extend_result) = certify_and_extend_result else {
            return BlobStoreResult::Error {
                blob_id: Some(self.blob_object.blob_id),
                failure_phase: "with_certify_and_extend_result".to_string(),
                error_msg: "Sui transaction result did not contain a result for the blob"
                    .to_string(),
            };
        };

        let cost = price_computation.operation_cost(&self.operation);
        let shared_blob_object = certify_and_extend_result.shared_blob_object();
        BlobStoreResult::NewlyCreated {
            blob_object: self.blob_object,
            resource_operation: self.operation,
            cost,
            shared_blob_object,
        }
    }
}

/// Identifies a stored quilt patch.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct StoredQuiltPatch {
    /// The identifier of the quilt patch.
    pub identifier: String,
    /// The quilt patch id.
    pub quilt_patch_id: String,
    /// The range of the quilt patch.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub range: Option<(u64, u64)>,
}

impl StoredQuiltPatch {
    /// Create a new stored quilt patch.
    pub fn new<T: QuiltPatchInternalIdApi>(blob_id: BlobId, identifier: &str, patch_id: T) -> Self {
        Self {
            identifier: identifier.to_string(),
            quilt_patch_id: QuiltPatchId::new(blob_id, patch_id.to_bytes()).to_string(),
            range: None,
        }
    }

    /// Create a new stored quilt patch with range.
    pub fn with_range(self, start_index: u64, end_index: u64) -> Self {
        Self {
            identifier: self.identifier,
            quilt_patch_id: self.quilt_patch_id,
            range: Some((start_index, end_index)),
        }
    }
}

/// Get the stored quilt patches from a quilt index.
pub fn get_stored_quilt_patches(
    quilt_index: &QuiltIndex,
    quilt_id: BlobId,
) -> Vec<StoredQuiltPatch> {
    assert!(matches!(quilt_index, QuiltIndex::V1(_)));
    let QuiltIndex::V1(quilt_index_v1) = quilt_index;
    quilt_index_v1
        .patches()
        .iter()
        .map(|patch| {
            StoredQuiltPatch::new(quilt_id, &patch.identifier, patch.quilt_patch_internal_id())
                .with_range(patch.start_index.into(), patch.end_index.into())
        })
        .collect()
}

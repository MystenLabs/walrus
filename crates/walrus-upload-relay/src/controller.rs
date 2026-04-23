// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! The proxy's main controller logic.

use std::{
    collections::HashMap,
    net::SocketAddr,
    num::NonZeroU16,
    path::Path as StdPath,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::Result;
use axum::{
    Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Path as AxumPath, Query, State},
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::{get, post, put},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use fastcrypto::hash::{HashFunction as _, Sha256};
use rand::RngCore as _;
use serde::{Deserialize, Serialize};
use serde_with::{DurationSeconds, serde_as};
use sui_types::digests::TransactionDigest;
use tokio::{
    sync::{Mutex, Notify},
    task::JoinHandle,
    time::Instant,
};
use tower_http::cors::{Any, CorsLayer};
use tracing::Level;
use utoipa::{OpenApi, ToSchema};
use utoipa_redoc::{Redoc, Servable};
use walrus_sdk::{
    SuiReadClient,
    config::{ClientConfig, load_configuration},
    core::{
        BlobId,
        EncodingType,
        SliverIndex,
        SliverPairIndex,
        encoding::{
            EncodingConfigEnum,
            EncodingFactory as _,
            Primary,
            PrimarySliver,
            Secondary,
            SecondarySliver,
            SliverPair,
        },
        messages::{BlobPersistenceType, ConfirmationCertificate},
        metadata::{
            BlobMetadataApi as _,
            UnverifiedBlobMetadataWithId,
            VerifiedBlobMetadataWithId,
        },
    },
    core_utils::metrics::Registry,
    node_client::WalrusNodeClient,
    sui::{
        ObjectIdSchema,
        client::{SuiClientMetricSet, retry_client::RetriableSuiClient},
    },
    upload_relay::{
        API_DOCS,
        BLOB_UPLOAD_RELAY_ROUTE,
        CreateSliverUploadSessionResponse,
        ResponseType,
        SLIVER_UPLOAD_RELAY_COMPLETE_ROUTE,
        SLIVER_UPLOAD_RELAY_PRIMARY_ROUTE,
        SLIVER_UPLOAD_RELAY_SESSION_ROUTE,
        SliverUploadSessionStatus,
        TIP_CONFIG_ROUTE,
        params::{
            DIGEST_LEN,
            DigestSchema,
            HashedAuthPackage,
            NONCE_LEN,
            Params,
            TransactionDigestSchema,
        },
        tip_config::{TipConfig, TipKind},
    },
    uploader::TailHandling,
};
use walrus_storage_node_client::UploadIntent;

use crate::{
    error::WalrusUploadRelayError,
    metrics::WalrusUploadRelayMetricSet,
    tip::check::{check_response_tip, check_tx_freshness},
    utils::{auth_package_preflight_checks, check_tx_auth_package, extract_hashed_auth_package},
};

/// The default socket bind address for the Walrus Upload Relay.
pub const DEFAULT_SERVER_ADDRESS: &str = "0.0.0.0:57391";

/// The configuration for the Walrus Upload Relay.
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct WalrusUploadRelayConfig {
    /// The configuration for tipping.
    pub tip_config: TipConfig,
    /// The transaction freshness threshold.
    ///
    /// The maximum time gap (in seconds) between the time the tip transaction is executed (i.e.,
    /// the tip is paid), and the request to store is made to the Walrus upload relay.
    #[serde(rename = "tx_freshness_threshold_secs")]
    #[serde_as(as = "DurationSeconds")]
    pub tx_freshness_threshold: Duration,
    /// The maximum time in the future (in seconds) a transaction timestamp can be.
    ///
    /// This is to account for clock skew between the Walrus upload relay and the full nodes.
    pub tx_max_future_threshold: Duration,
}

const SLIVER_UPLOAD_SESSION_ID_BYTES: usize = 16;
const SLIVER_UPLOAD_SESSION_TTL: Duration = Duration::from_secs(30 * 60);

type SliverUploadSessions = Arc<Mutex<HashMap<String, Arc<Mutex<SliverUploadSession>>>>>;
type PaidTipTransactions = Arc<Mutex<HashMap<TransactionDigest, Instant>>>;

#[derive(Debug, Clone)]
struct SliverUploadPaidTip {
    tx_id: TransactionDigest,
    auth_package: HashedAuthPackage,
}

#[derive(Debug)]
struct SliverUploadSession {
    upload_session_id: String,
    metadata: VerifiedBlobMetadataWithId,
    config: EncodingConfigEnum,
    symbol_size: NonZeroU16,
    deletable_blob_object: Option<walrus_sdk::ObjectID>,
    paid_tip: Option<SliverUploadPaidTip>,
    systematic_primary_slivers: Vec<Option<PrimarySliver>>,
    secondary_slivers: Vec<SecondarySliver>,
    metadata_upload_completed: bool,
    metadata_upload_notify: Arc<Notify>,
    in_flight_systematic_primary_uploads: usize,
    failed_systematic_primary_uploads: usize,
    systematic_primary_upload_notify: Arc<Notify>,
    expires_at: Instant,
    finalizing: bool,
}

impl SliverUploadSession {
    fn new(
        upload_session_id: String,
        metadata: VerifiedBlobMetadataWithId,
        config: EncodingConfigEnum,
        symbol_size: NonZeroU16,
        deletable_blob_object: Option<walrus_sdk::ObjectID>,
        paid_tip: Option<SliverUploadPaidTip>,
        expires_at: Instant,
    ) -> Self {
        let systematic_primary_count = usize::from(config.n_source_symbols::<Primary>().get());
        let secondary_slivers = (0..config.n_shards().get())
            .map(|index| {
                SecondarySliver::new_empty(
                    config.n_source_symbols::<Primary>().get(),
                    symbol_size,
                    SliverIndex(index),
                )
            })
            .collect();

        Self {
            upload_session_id,
            metadata,
            config,
            symbol_size,
            deletable_blob_object,
            paid_tip,
            systematic_primary_slivers: vec![None; systematic_primary_count],
            secondary_slivers,
            metadata_upload_completed: false,
            metadata_upload_notify: Arc::new(Notify::new()),
            in_flight_systematic_primary_uploads: 0,
            failed_systematic_primary_uploads: 0,
            systematic_primary_upload_notify: Arc::new(Notify::new()),
            expires_at,
            finalizing: false,
        }
    }

    fn ensure_not_expired(&self) -> Result<(), WalrusUploadRelayError> {
        if self.expires_at <= Instant::now() {
            return Err(WalrusUploadRelayError::SliverUploadSessionNotFound);
        }
        Ok(())
    }

    fn ensure_active(&self) -> Result<(), WalrusUploadRelayError> {
        self.ensure_not_expired()?;
        if self.finalizing {
            return Err(WalrusUploadRelayError::InvalidSliverUploadSessionRequest(
                "session is already finalizing".to_string(),
            ));
        }
        Ok(())
    }

    fn missing_systematic_primary_slivers(&self) -> Vec<usize> {
        self.systematic_primary_slivers
            .iter()
            .enumerate()
            .filter_map(|(index, sliver)| sliver.is_none().then_some(index))
            .collect()
    }

    fn total_systematic_primary_slivers(&self) -> usize {
        self.systematic_primary_slivers.len()
    }

    fn received_systematic_primary_slivers(&self) -> usize {
        self.systematic_primary_slivers
            .iter()
            .filter(|sliver| sliver.is_some())
            .count()
    }

    fn systematic_primary_blob_digest(&self) -> Result<[u8; DIGEST_LEN], WalrusUploadRelayError> {
        let mut remaining_blob_bytes = usize::try_from(self.metadata.metadata().unencoded_length())
            .map_err(|_| {
                WalrusUploadRelayError::InvalidSliverUploadSessionRequest(
                    "blob length does not fit on this architecture".to_string(),
                )
            })?;
        let mut hasher = Sha256::new();

        for sliver in &self.systematic_primary_slivers {
            if remaining_blob_bytes == 0 {
                break;
            }
            let sliver = sliver
                .as_ref()
                .expect("systematic primary slivers are checked before digesting");
            let data = sliver.symbols.data();
            let bytes_to_hash = remaining_blob_bytes.min(data.len());
            hasher.update(&data[..bytes_to_hash]);
            remaining_blob_bytes -= bytes_to_hash;
        }

        if remaining_blob_bytes != 0 {
            return Err(WalrusUploadRelayError::InvalidSliverUploadSessionRequest(
                "uploaded systematic primary slivers are shorter than the blob length".to_string(),
            ));
        }

        Ok(hasher.finalize().digest)
    }

    fn status(&self) -> SliverUploadSessionStatus {
        SliverUploadSessionStatus {
            upload_session_id: self.upload_session_id.clone(),
            blob_id: *self.metadata.blob_id(),
            received_systematic_primary_slivers: self.received_systematic_primary_slivers(),
            total_systematic_primary_slivers: self.total_systematic_primary_slivers(),
        }
    }

    fn insert_systematic_primary_sliver(
        &mut self,
        sliver_index: u16,
        sliver: PrimarySliver,
        encoding_config: &walrus_sdk::core::encoding::EncodingConfig,
    ) -> Result<(PrimarySliver, SliverUploadSessionStatus, bool), WalrusUploadRelayError> {
        self.ensure_active()?;

        let row_index = usize::from(sliver_index);
        if row_index >= self.systematic_primary_slivers.len() {
            return Err(WalrusUploadRelayError::InvalidSliverUploadSessionRequest(
                format!(
                    "primary sliver index {sliver_index} is outside the systematic range 0..{}",
                    self.systematic_primary_slivers.len()
                ),
            ));
        }

        if sliver.index != SliverIndex(sliver_index) {
            return Err(WalrusUploadRelayError::InvalidSliverUploadSessionRequest(
                format!(
                    "primary sliver body index {} does not match path index {sliver_index}",
                    sliver.index
                ),
            ));
        }

        sliver
            .verify(encoding_config, self.metadata.metadata())
            .map_err(|error| {
                WalrusUploadRelayError::InvalidSliverUploadSessionRequest(format!(
                    "primary sliver failed metadata verification: {error}"
                ))
            })?;

        if let Some(existing) = &self.systematic_primary_slivers[row_index] {
            if existing != &sliver {
                return Err(WalrusUploadRelayError::InvalidSliverUploadSessionRequest(
                    format!(
                        "primary sliver {sliver_index} was already uploaded with different bytes"
                    ),
                ));
            }
            return Ok((sliver, self.status(), false));
        }

        let n_systematic_secondary = usize::from(self.config.n_source_symbols::<Secondary>().get());
        for (column_index, symbol) in sliver.symbols.to_symbols().enumerate() {
            self.secondary_slivers[column_index].copy_symbol_to(row_index, symbol);
        }

        let recovery_symbols = self
            .config
            .encode_all_repair_symbols::<Secondary>(sliver.symbols.data())
            .map_err(|error| {
                WalrusUploadRelayError::InvalidSliverUploadSessionRequest(format!(
                    "failed to derive secondary recovery symbols from primary sliver \
                    {sliver_index}: {error:?}"
                ))
            })?;
        for (symbol, secondary_sliver) in recovery_symbols.to_symbols().zip(
            self.secondary_slivers
                .iter_mut()
                .skip(n_systematic_secondary),
        ) {
            secondary_sliver.copy_symbol_to(row_index, symbol);
        }

        self.systematic_primary_slivers[row_index] = Some(sliver.clone());
        self.in_flight_systematic_primary_uploads += 1;
        Ok((sliver, self.status(), true))
    }

    fn complete_systematic_primary_upload(&mut self, success: bool) {
        self.in_flight_systematic_primary_uploads =
            self.in_flight_systematic_primary_uploads.saturating_sub(1);
        if !success {
            self.failed_systematic_primary_uploads += 1;
        }
        self.systematic_primary_upload_notify.notify_waiters();
    }

    fn complete_metadata_upload(&mut self) {
        self.metadata_upload_completed = true;
        self.metadata_upload_notify.notify_waiters();
    }

    fn prepare_for_finalize(&mut self) -> Result<(), WalrusUploadRelayError> {
        self.ensure_not_expired()?;
        if self.finalizing {
            return Err(WalrusUploadRelayError::InvalidSliverUploadSessionRequest(
                "session is already finalizing".to_string(),
            ));
        }

        let missing = self.missing_systematic_primary_slivers();
        if !missing.is_empty() {
            return Err(WalrusUploadRelayError::InvalidSliverUploadSessionRequest(
                format!("missing systematic primary slivers: {missing:?}"),
            ));
        }

        self.finalizing = true;
        Ok(())
    }

    fn systematic_primary_skip_count(&self) -> usize {
        if self.in_flight_systematic_primary_uploads == 0
            && self.failed_systematic_primary_uploads == 0
        {
            self.total_systematic_primary_slivers()
        } else {
            0
        }
    }

    fn finalize(
        &mut self,
        encoding_config: &walrus_sdk::core::encoding::EncodingConfig,
    ) -> Result<FinalizedSliverUploadSession, WalrusUploadRelayError> {
        self.ensure_not_expired()?;

        let missing = self.missing_systematic_primary_slivers();
        if !missing.is_empty() {
            return Err(WalrusUploadRelayError::InvalidSliverUploadSessionRequest(
                format!("missing systematic primary slivers: {missing:?}"),
            ));
        }

        if let Some(paid_tip) = &self.paid_tip
            && self.systematic_primary_blob_digest()? != paid_tip.auth_package.blob_digest
        {
            return Err(WalrusUploadRelayError::BlobDigestMismatch);
        }

        let n_systematic_primary = usize::from(self.config.n_source_symbols::<Primary>().get());
        let n_systematic_secondary = usize::from(self.config.n_source_symbols::<Secondary>().get());
        let n_shards = usize::from(self.config.n_shards().get());

        let mut primary_slivers = self
            .systematic_primary_slivers
            .iter()
            .cloned()
            .map(|sliver| sliver.expect("missing slivers checked above"))
            .collect::<Vec<_>>();

        primary_slivers.extend((n_systematic_primary..n_shards).map(|index| {
            PrimarySliver::new_empty(
                self.config.n_source_symbols::<Secondary>().get(),
                self.symbol_size,
                SliverIndex(index.try_into().expect("n_shards fits in u16")),
            )
        }));

        for (column_index, secondary_sliver) in self
            .secondary_slivers
            .iter()
            .take(n_systematic_secondary)
            .enumerate()
        {
            let symbols = self
                .config
                .encode_all_symbols::<Primary>(secondary_sliver.symbols.data())
                .map_err(|error| {
                    WalrusUploadRelayError::InvalidSliverUploadSessionRequest(format!(
                        "failed to derive non-systematic primary symbols for column \
                        {column_index}: {error:?}"
                    ))
                })?;
            for (symbol, primary_sliver) in symbols
                .to_symbols()
                .zip(primary_slivers.iter_mut())
                .skip(n_systematic_primary)
            {
                primary_sliver.copy_symbol_to(column_index, symbol);
            }
        }

        let secondary_slivers = self.secondary_slivers.clone();
        let sliver_pairs = primary_slivers
            .into_iter()
            .zip(secondary_slivers.into_iter().rev())
            .map(|(primary, secondary)| SliverPair { primary, secondary })
            .collect::<Vec<_>>();

        for pair in &sliver_pairs {
            pair.primary
                .verify(encoding_config, self.metadata.metadata())
                .map_err(|error| {
                    WalrusUploadRelayError::InvalidSliverUploadSessionRequest(format!(
                        "derived primary sliver {} failed metadata verification: {error}",
                        pair.primary.index
                    ))
                })?;
            pair.secondary
                .verify(encoding_config, self.metadata.metadata())
                .map_err(|error| {
                    WalrusUploadRelayError::InvalidSliverUploadSessionRequest(format!(
                        "derived secondary sliver {} failed metadata verification: {error}",
                        pair.secondary.index
                    ))
                })?;
        }

        self.finalizing = true;

        let blob_persistence = if let Some(object_id) = self.deletable_blob_object {
            BlobPersistenceType::Deletable {
                object_id: object_id.into(),
            }
        } else {
            BlobPersistenceType::Permanent
        };

        Ok(FinalizedSliverUploadSession {
            metadata: self.metadata.clone(),
            sliver_pairs,
            systematic_primary_sliver_count: self.systematic_primary_skip_count(),
            blob_persistence,
            paid_tip_tx_id: self.paid_tip.as_ref().map(|paid_tip| paid_tip.tx_id),
        })
    }
}

#[derive(Debug)]
struct FinalizedSliverUploadSession {
    metadata: VerifiedBlobMetadataWithId,
    sliver_pairs: Vec<SliverPair>,
    systematic_primary_sliver_count: usize,
    blob_persistence: BlobPersistenceType,
    paid_tip_tx_id: Option<TransactionDigest>,
}

/// The subset of query parameters of the Walrus Upload Relay, necessary to check the tip.
///
/// Compared to `Params`, the `tx_id` and the `nonce` are not optional, and `blob_id` and
/// `deletable_blob_object` are not necessary.
#[derive(Debug, Clone)]
pub struct PaidTipParams {
    /// The ID of the transaction that paid the tip.
    pub tx_id: TransactionDigest,
    /// The nonce used to generate the hash.
    pub nonce: [u8; NONCE_LEN],
    /// The encoding type of the blob.
    pub encoding_type: Option<EncodingType>,
}

impl TryFrom<&Params> for PaidTipParams {
    type Error = WalrusUploadRelayError;

    fn try_from(value: &Params) -> Result<Self, Self::Error> {
        // Checks that the `Params` contain the `tx_id` and the `nonce`, and returns an instance of
        // `PaidParams`.
        let Params {
            tx_id,
            nonce,
            encoding_type,
            ..
        } = value;

        Ok(PaidTipParams {
            tx_id: tx_id.ok_or(WalrusUploadRelayError::MissingTxIdOrNonce)?,
            nonce: nonce.ok_or(WalrusUploadRelayError::MissingTxIdOrNonce)?,
            encoding_type: *encoding_type,
        })
    }
}

/// The controller for the Walrus Upload Relay.
///
/// It is shared by all Walrus Upload Relay route handlers, and is responsible for checking incoming
/// requests and pushing slivers and metadata to storage nodes.
pub(crate) struct Controller {
    pub(crate) client: WalrusNodeClient<SuiReadClient>,
    pub(crate) relay_config: WalrusUploadRelayConfig,
    pub(crate) n_shards: NonZeroU16,
    pub(crate) metric_set: WalrusUploadRelayMetricSet,
    sliver_upload_sessions: SliverUploadSessions,
    paid_tip_transactions: PaidTipTransactions,
}

impl Controller {
    /// Creates a new controller.
    pub(crate) fn new(
        client: WalrusNodeClient<SuiReadClient>,
        n_shards: NonZeroU16,
        relay_config: WalrusUploadRelayConfig,
        metric_set: WalrusUploadRelayMetricSet,
    ) -> Self {
        Self {
            client,
            relay_config,
            n_shards,
            metric_set,
            sliver_upload_sessions: Arc::new(Mutex::new(HashMap::new())),
            paid_tip_transactions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Checks the request and fans out the data to the storage nodes.
    #[tracing::instrument(level = Level::DEBUG, skip_all)]
    pub(crate) async fn fan_out(
        &self,
        body: Bytes,
        params: Params,
    ) -> Result<ResponseType, WalrusUploadRelayError> {
        if self.relay_config.tip_config.requires_payment() {
            // Check authentication pre-conditions for upload relay, if the configuration requires a
            // tip.
            let paid_params = PaidTipParams::try_from(&params)?;
            self.validate_auth_package(&paid_params, body.as_ref())
                .await?;
        }

        let encode_start_timer = Instant::now();
        // PERF: encoding should probably be done on a separate thread pool.
        let (sliver_pairs, metadata) = self
            .client
            .encoding_config()
            .get_for_type(
                params
                    .encoding_type
                    .unwrap_or(walrus_sdk::core::DEFAULT_ENCODING),
            )
            .encode_with_metadata(body.into())?;
        let duration = encode_start_timer.elapsed();

        tracing::debug!(
            computed_blob_id=%metadata.blob_id(),
            expected_blob_id=%params.blob_id,
            "blob id computed"
        );

        if *metadata.blob_id() != params.blob_id {
            self.metric_set.blob_id_mismatch.inc();
            return Err(WalrusUploadRelayError::BlobIdMismatch);
        }

        let pair = sliver_pairs
            .first()
            .expect("the encoding produces sliver pairs");
        let symbol_size = pair.primary.symbols.symbol_size().get();

        tracing::debug!(
            symbol_size,
            primary_sliver_size = pair.primary.symbols.data().len(),
            secondary_sliver_size = pair.secondary.symbols.data().len(),
            ?duration,
            "encoded sliver pairs and metadata"
        );

        // Attempt to upload the slivers.
        let blob_persistence = if let Some(object_id) = params.deletable_blob_object {
            BlobPersistenceType::Deletable {
                object_id: object_id.into(),
            }
        } else {
            BlobPersistenceType::Permanent
        };
        let confirmation_certificate: ConfirmationCertificate = self
            .client
            .send_blob_data_and_get_certificate(
                &metadata,
                Arc::new(sliver_pairs),
                &blob_persistence,
                None,
                TailHandling::Blocking,
                None,
                None,
                None,
                None,
            )
            .await?;

        self.metric_set.blobs_uploaded.inc();

        // Reply with the confirmation certificate.
        Ok(ResponseType {
            blob_id: params.blob_id,
            confirmation_certificate,
        })
    }

    /// Creates a session for uploading systematic primary slivers in parallel.
    #[tracing::instrument(level = Level::DEBUG, skip_all, fields(blob_id=%params.blob_id))]
    pub(crate) async fn create_sliver_upload_session(
        &self,
        params: Params,
        body: Bytes,
    ) -> Result<CreateSliverUploadSessionResponse, WalrusUploadRelayError> {
        self.cleanup_expired_sliver_upload_sessions().await;
        self.cleanup_expired_paid_tip_transactions().await;

        let metadata = bcs::from_bytes::<UnverifiedBlobMetadataWithId>(&body)
            .map_err(|error| {
                WalrusUploadRelayError::InvalidSliverUploadSessionRequest(format!(
                    "failed to decode BCS blob metadata: {error}"
                ))
            })?
            .verify(self.client.encoding_config())
            .map_err(|error| {
                WalrusUploadRelayError::InvalidSliverUploadSessionRequest(format!(
                    "metadata verification failed: {error}"
                ))
            })?;

        if *metadata.blob_id() != params.blob_id {
            self.metric_set.blob_id_mismatch.inc();
            return Err(WalrusUploadRelayError::BlobIdMismatch);
        }
        if let Some(encoding_type) = params.encoding_type
            && encoding_type != metadata.metadata().encoding_type()
        {
            return Err(WalrusUploadRelayError::InvalidSliverUploadSessionRequest(
                format!(
                    "query encoding_type {encoding_type:?} does not match metadata \
                    encoding_type {:?}",
                    metadata.metadata().encoding_type()
                ),
            ));
        }

        let config = self
            .client
            .encoding_config()
            .get_for_type(metadata.metadata().encoding_type());
        let symbol_size = metadata
            .metadata()
            .symbol_size(self.client.encoding_config())?;
        let upload_session_id = generate_sliver_upload_session_id();
        let expires_at = Instant::now() + SLIVER_UPLOAD_SESSION_TTL;

        let paid_tip = if self.relay_config.tip_config.requires_payment() {
            let paid_params = PaidTipParams::try_from(&params)?;
            let paid_tip = self
                .validate_paid_tip_for_sliver_upload_session(&paid_params, &metadata)
                .await?;
            self.reserve_paid_tip_transaction(paid_tip.tx_id, expires_at)
                .await?;
            Some(paid_tip)
        } else {
            None
        };

        let session = Arc::new(Mutex::new(SliverUploadSession::new(
            upload_session_id.clone(),
            metadata.clone(),
            config,
            symbol_size,
            params.deletable_blob_object,
            paid_tip,
            expires_at,
        )));
        self.sliver_upload_sessions
            .lock()
            .await
            .insert(upload_session_id.clone(), session.clone());

        let client = self.client.clone();
        let session_for_metadata_upload = session.clone();
        let metadata_for_upload = metadata.clone();
        let upload_session_id_for_log = upload_session_id.clone();
        tokio::spawn(async move {
            if let Err(error) = client
                .store_metadata(&metadata_for_upload, UploadIntent::Immediate)
                .await
            {
                tracing::warn!(
                    blob_id = %metadata_for_upload.blob_id(),
                    upload_session_id = upload_session_id_for_log,
                    %error,
                    "background metadata upload failed for sliver relay session"
                );
            }

            let mut session = session_for_metadata_upload.lock().await;
            session.complete_metadata_upload();
        });

        Ok(CreateSliverUploadSessionResponse { upload_session_id })
    }

    /// Accepts a systematic primary sliver for an upload session.
    #[tracing::instrument(level = Level::DEBUG, skip_all, fields(%upload_session_id, sliver_index))]
    pub(crate) async fn put_sliver_upload_session_primary(
        &self,
        upload_session_id: String,
        sliver_index: u16,
        body: Bytes,
    ) -> Result<SliverUploadSessionStatus, WalrusUploadRelayError> {
        let session = self.get_sliver_upload_session(&upload_session_id).await?;
        let primary_sliver = bcs::from_bytes::<PrimarySliver>(&body).map_err(|error| {
            WalrusUploadRelayError::InvalidSliverUploadSessionRequest(format!(
                "failed to decode BCS primary sliver: {error}"
            ))
        })?;

        let (metadata, pair_index, sliver, status, should_upload) = {
            let mut session = session.lock().await;
            let pair_index = SliverPairIndex(sliver_index);
            let (sliver, status, should_upload) = session.insert_systematic_primary_sliver(
                sliver_index,
                primary_sliver,
                self.client.encoding_config(),
            )?;
            (
                session.metadata.clone(),
                pair_index,
                sliver,
                status,
                should_upload,
            )
        };

        if should_upload {
            let client = self.client.clone();
            let session = session.clone();
            let upload_session_id_for_log = upload_session_id.clone();
            tokio::spawn(async move {
                wait_for_metadata_upload(&session).await;

                let result = client
                    .store_sliver(&metadata, pair_index, &sliver, UploadIntent::Immediate)
                    .await;
                if let Err(error) = &result {
                    tracing::warn!(
                        blob_id = %metadata.blob_id(),
                        upload_session_id = upload_session_id_for_log,
                        %pair_index,
                        %error,
                        "background systematic primary upload failed"
                    );
                }

                let mut session = session.lock().await;
                session.complete_systematic_primary_upload(result.is_ok());
            });
        }

        Ok(status)
    }

    /// Finalizes a sliver upload session and returns a storage confirmation certificate.
    #[tracing::instrument(level = Level::DEBUG, skip_all, fields(%upload_session_id))]
    pub(crate) async fn complete_sliver_upload_session(
        &self,
        upload_session_id: String,
    ) -> Result<ResponseType, WalrusUploadRelayError> {
        let session = self.get_sliver_upload_session(&upload_session_id).await?;

        {
            let mut session = session.lock().await;
            session.prepare_for_finalize()?;
        }

        self.wait_for_pending_systematic_primary_uploads(&session)
            .await;

        let finalized = {
            let mut session = session.lock().await;
            session.finalize(self.client.encoding_config())
        }
        .inspect_err(|error| {
            if matches!(error, WalrusUploadRelayError::BlobDigestMismatch) {
                self.metric_set.auth_package_check_error.inc();
            }
        })?;

        let result = self
            .client
            .send_blob_data_and_get_certificate_skipping_systematic_primaries(
                &finalized.metadata,
                Arc::new(finalized.sliver_pairs),
                finalized.systematic_primary_sliver_count,
                &finalized.blob_persistence,
                None,
                TailHandling::Blocking,
                None,
                None,
                None,
                None,
            )
            .await;

        match result {
            Ok(confirmation_certificate) => {
                self.sliver_upload_sessions
                    .lock()
                    .await
                    .remove(&upload_session_id);
                if let Some(tx_id) = finalized.paid_tip_tx_id {
                    self.release_paid_tip_transaction(tx_id).await;
                }
                self.metric_set.blobs_uploaded.inc();
                Ok(ResponseType {
                    blob_id: *finalized.metadata.blob_id(),
                    confirmation_certificate,
                })
            }
            Err(error) => {
                let session = self
                    .sliver_upload_sessions
                    .lock()
                    .await
                    .get(&upload_session_id)
                    .cloned();
                if let Some(session) = session {
                    session.lock().await.finalizing = false;
                }
                Err(error.into())
            }
        }
    }

    async fn get_sliver_upload_session(
        &self,
        upload_session_id: &str,
    ) -> Result<Arc<Mutex<SliverUploadSession>>, WalrusUploadRelayError> {
        self.sliver_upload_sessions
            .lock()
            .await
            .get(upload_session_id)
            .cloned()
            .ok_or(WalrusUploadRelayError::SliverUploadSessionNotFound)
    }

    async fn wait_for_pending_systematic_primary_uploads(
        &self,
        session: &Arc<Mutex<SliverUploadSession>>,
    ) {
        loop {
            let notify = {
                let session = session.lock().await;
                if session.in_flight_systematic_primary_uploads == 0 {
                    return;
                }
                session.systematic_primary_upload_notify.clone()
            };

            notify.notified().await;
        }
    }

    async fn cleanup_expired_sliver_upload_sessions(&self) {
        let now = Instant::now();
        let mut expired_tip_transactions = Vec::new();
        self.sliver_upload_sessions
            .lock()
            .await
            .retain(|_, session| {
                session
                    .try_lock()
                    .map(|session| {
                        if session.expires_at > now {
                            true
                        } else {
                            if let Some(paid_tip) = &session.paid_tip {
                                expired_tip_transactions.push(paid_tip.tx_id);
                            }
                            false
                        }
                    })
                    .unwrap_or(true)
            });

        for tx_id in expired_tip_transactions {
            self.release_paid_tip_transaction(tx_id).await;
        }
    }

    async fn cleanup_expired_paid_tip_transactions(&self) {
        let now = Instant::now();
        self.paid_tip_transactions
            .lock()
            .await
            .retain(|_, expires_at| *expires_at > now);
    }

    async fn reserve_paid_tip_transaction(
        &self,
        tx_id: TransactionDigest,
        expires_at: Instant,
    ) -> Result<(), WalrusUploadRelayError> {
        self.cleanup_expired_paid_tip_transactions().await;

        let mut transactions = self.paid_tip_transactions.lock().await;
        if transactions.contains_key(&tx_id) {
            return Err(WalrusUploadRelayError::InvalidSliverUploadSessionRequest(
                format!(
                    "tip transaction {tx_id} is already reserved by an active sliver upload session"
                ),
            ));
        }

        transactions.insert(tx_id, expires_at);

        Ok(())
    }

    async fn release_paid_tip_transaction(&self, tx_id: TransactionDigest) {
        self.paid_tip_transactions.lock().await.remove(&tx_id);
    }

    async fn validate_paid_tip_for_sliver_upload_session(
        &self,
        params: &PaidTipParams,
        metadata: &VerifiedBlobMetadataWithId,
    ) -> Result<SliverUploadPaidTip, WalrusUploadRelayError> {
        let tx = self
            .client
            .sui_client()
            .retriable_sui_client()
            .get_transaction_with_options(
                params.tx_id,
                walrus_sdk::sui::types::TransactionResponseOptions::new()
                    .with_raw_input()
                    .with_balance_changes(),
            )
            .await
            .map_err(|err| {
                self.metric_set.get_transaction_error.inc();
                Box::new(err)
            })?;

        check_tx_freshness(
            &tx,
            self.relay_config.tx_freshness_threshold,
            self.relay_config.tx_max_future_threshold,
        )
        .inspect_err(|_| self.metric_set.freshness_check_error.inc())?;
        check_response_tip(
            &self.relay_config.tip_config,
            &tx,
            metadata.metadata().unencoded_length(),
            self.n_shards,
            metadata.metadata().encoding_type(),
        )
        .inspect_err(|_| self.metric_set.tip_check_error.inc())?;

        let auth_package = extract_hashed_auth_package(tx).inspect_err(|_| {
            self.metric_set.auth_package_check_error.inc();
        })?;
        auth_package_preflight_checks(
            &auth_package,
            metadata.metadata().unencoded_length(),
            &params.nonce,
        )
        .inspect_err(|_| self.metric_set.auth_package_check_error.inc())?;

        Ok(SliverUploadPaidTip {
            tx_id: params.tx_id,
            auth_package,
        })
    }

    async fn validate_auth_package(
        &self,
        params: &PaidTipParams,
        blob: &[u8],
    ) -> Result<(), WalrusUploadRelayError> {
        // Get transaction inputs from tx_id.
        let tx = self
            .client
            .sui_client()
            .retriable_sui_client()
            .get_transaction_with_options(
                params.tx_id,
                walrus_sdk::sui::types::TransactionResponseOptions::new()
                    .with_raw_input()
                    .with_balance_changes(),
            )
            .await
            .map_err(|err| {
                self.metric_set.get_transaction_error.inc();
                Box::new(err)
            })?;

        check_tx_freshness(
            &tx,
            self.relay_config.tx_freshness_threshold,
            self.relay_config.tx_max_future_threshold,
        )
        .inspect_err(|_| self.metric_set.freshness_check_error.inc())?;
        check_response_tip(
            &self.relay_config.tip_config,
            &tx,
            blob.len().try_into().expect("using 32 or 64 bit arch"),
            self.n_shards,
            params
                .encoding_type
                .unwrap_or(walrus_sdk::core::DEFAULT_ENCODING),
        )
        .inspect_err(|_| self.metric_set.tip_check_error.inc())?;
        check_tx_auth_package(blob, &params.nonce, tx).inspect_err(|_| {
            self.metric_set.auth_package_check_error.inc();
        })?;

        // This request looks OK.
        Ok(())
    }
}

async fn wait_for_signal(flag: Arc<AtomicBool>, notify: Arc<Notify>) {
    loop {
        if flag.load(Ordering::SeqCst) {
            break;
        }
        notify.notified().await;
    }
}

async fn wait_for_metadata_upload(session: &Arc<Mutex<SliverUploadSession>>) {
    loop {
        let notify = {
            let session = session.lock().await;
            if session.metadata_upload_completed {
                return;
            }
            session.metadata_upload_notify.clone()
        };

        notify.notified().await;
    }
}

fn generate_sliver_upload_session_id() -> String {
    let mut bytes = [0; SLIVER_UPLOAD_SESSION_ID_BYTES];
    rand::thread_rng().fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

/// A handle to the upload relay, which can be used to shut it down.
pub struct UploadRelayHandle {
    shutdown_flag: Arc<AtomicBool>,
    shutdown_notify: Arc<Notify>,
    tcp_bind_flag: Arc<AtomicBool>,
    tcp_bind_notify: Arc<Notify>,
    join_handle: JoinHandle<Result<(), anyhow::Error>>,
}

impl std::fmt::Debug for UploadRelayHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UploadRelayHandle")
            .field("shutdown_flag", &self.shutdown_flag.load(Ordering::SeqCst))
            .finish_non_exhaustive()
    }
}

impl UploadRelayHandle {
    /// Creates a new `UploadRelayCancelTrigger`.
    fn with_join_handle(
        shutdown_flag: Arc<AtomicBool>,
        shutdown_notify: Arc<Notify>,
        tcp_bind_flag: Arc<AtomicBool>,
        tcp_bind_notify: Arc<Notify>,
        join_handle: JoinHandle<Result<(), anyhow::Error>>,
    ) -> Self {
        Self {
            shutdown_flag,
            shutdown_notify,
            tcp_bind_flag,
            tcp_bind_notify,
            join_handle,
        }
    }

    /// Allows the upload relay to run indefinitely.
    pub async fn run_forever(self) -> ! {
        drop(self.shutdown_notify);
        drop(self.shutdown_flag);
        let result = self
            .join_handle
            .await
            .expect("join handle should never complete");
        panic!("upload relay task completed unexpectedly: {result:?}")
    }

    /// Cancels the upload relay.
    pub async fn shutdown(self) -> Result<(), anyhow::Error> {
        assert!(!self.shutdown_flag.load(Ordering::SeqCst));
        self.shutdown_flag.store(true, Ordering::SeqCst);
        self.shutdown_notify.notify_waiters();
        self.join_handle.await.expect("join handle should complete")
    }

    /// Waits for the upload relay to bind to the TCP socket.
    pub async fn wait_for_tcp_bind(&self) -> Result<(), anyhow::Error> {
        loop {
            if self.tcp_bind_flag.load(Ordering::Relaxed) {
                return Ok(());
            }
            if self.shutdown_flag.load(Ordering::Relaxed) {
                anyhow::bail!("upload relay shutdown while waiting for TCP bind");
            }

            tokio::select! {
                _ = self.tcp_bind_notify.notified() => {
                    continue;
                },
                _ = self.shutdown_notify.notified() => {
                    anyhow::bail!("upload relay shutdown while waiting for TCP bind");
                },
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_server(
    client: WalrusNodeClient<SuiReadClient>,
    relay_config: WalrusUploadRelayConfig,
    metric_set: WalrusUploadRelayMetricSet,
    server_address: SocketAddr,
    shutdown_flag: Arc<AtomicBool>,
    shutdown_notify: Arc<Notify>,
    tcp_bind_flag: Arc<AtomicBool>,
    tcp_bind_notify: Arc<Notify>,
) -> Result<(), anyhow::Error> {
    let n_shards = client.get_committees().await?.n_shards();
    tracing::debug!(?relay_config, "loaded relay config");

    // Build our HTTP application to handle the blob fan-out operations.
    let app = Router::new()
        .merge(Redoc::with_url(
            API_DOCS,
            WalrusUploadRelayApiDoc::openapi(),
        ))
        .route(TIP_CONFIG_ROUTE, get(send_tip_config))
        .route(BLOB_UPLOAD_RELAY_ROUTE, post(blob_upload_relay_handler))
        .route(
            SLIVER_UPLOAD_RELAY_SESSION_ROUTE,
            post(create_sliver_upload_session_handler),
        )
        .route(
            SLIVER_UPLOAD_RELAY_PRIMARY_ROUTE,
            put(put_sliver_upload_session_primary_handler),
        )
        .route(
            SLIVER_UPLOAD_RELAY_COMPLETE_ROUTE,
            post(complete_sliver_upload_session_handler),
        )
        .layer(DefaultBodyLimit::max(1024 * 1024 * 1024))
        .with_state(Arc::new(Controller::new(
            client,
            n_shards,
            relay_config,
            metric_set,
        )))
        .layer(cors_layer());

    let listener = tokio::net::TcpListener::bind(&server_address).await?;

    // Notify that the server has bound to the TCP socket.
    tcp_bind_flag.store(true, Ordering::SeqCst);
    tcp_bind_notify.notify_waiters();

    tracing::info!(?server_address, n_shards, "Serving Walrus Upload Relay...");
    Ok(axum::serve(listener, app)
        .with_graceful_shutdown(wait_for_signal(shutdown_flag, shutdown_notify))
        .await?)
}

/// Runs the upload relay.
pub fn start_upload_relay(
    client: WalrusNodeClient<SuiReadClient>,
    relay_config: WalrusUploadRelayConfig,
    server_address: SocketAddr,
    registry: Registry,
) -> Result<UploadRelayHandle> {
    let metric_set = WalrusUploadRelayMetricSet::new(&registry);
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let shutdown_notify = Arc::new(Notify::new());
    let tcp_bind_flag = Arc::new(AtomicBool::new(false));
    let tcp_bind_notify = Arc::new(Notify::new());

    // Run the upload relay in a new asynchronous task.
    let join_handle = tokio::spawn({
        let shutdown_flag = shutdown_flag.clone();
        let shutdown_notify = shutdown_notify.clone();
        let tcp_bind_flag = tcp_bind_flag.clone();
        let tcp_bind_notify = tcp_bind_notify.clone();

        async move {
            let result = run_server(
                client,
                relay_config,
                metric_set,
                server_address,
                shutdown_flag.clone(),
                shutdown_notify.clone(),
                tcp_bind_flag,
                tcp_bind_notify,
            )
            .await;

            // Notify that the server is shutting down.
            shutdown_flag.store(true, Ordering::SeqCst);
            shutdown_notify.notify_waiters();

            tracing::warn!(result = ?result, "upload relay server task exited");
            result
        }
    });

    Ok(UploadRelayHandle::with_join_handle(
        shutdown_flag,
        shutdown_notify,
        tcp_bind_flag,
        tcp_bind_notify,
        join_handle,
    ))
}

/// Returns a `CorsLayer` for the controller endpoints.
pub(crate) fn cors_layer() -> CorsLayer {
    CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .max_age(Duration::from_hours(24))
        .allow_headers(Any)
}

#[derive(OpenApi)]
#[openapi(
    info(title = "Walrus Upload Relay"),
    paths(
        blob_upload_relay_handler,
        create_sliver_upload_session_handler,
        put_sliver_upload_session_primary_handler,
        complete_sliver_upload_session_handler,
        send_tip_config
    ),
    components(schemas(
        BlobId,
        EncodingType,
        CreateSliverUploadSessionResponse,
        ObjectIdSchema,
        SliverUploadSessionStatus,
        TransactionDigestSchema,
        DigestSchema,
        TipKind,
        TipConfig,
    ))
)]
pub(super) struct WalrusUploadRelayApiDoc;

/// Returns the tip configuration for the current Walrus Upload Relay.
///
/// Allows clients to refresh their configuration of the proxy's address and tip amounts.
#[utoipa::path(
    get,
    path = TIP_CONFIG_ROUTE,
    responses(
        (
            status = 200,
            description = "The tip configuration was retrieved successfully",
            body = TipConfig
        ),
    ),
)]
#[tracing::instrument(level = Level::ERROR, skip_all)]
pub(crate) async fn send_tip_config(
    State(controller): State<Arc<Controller>>,
) -> impl IntoResponse {
    tracing::debug!("returning tip config");
    (StatusCode::OK, Json(&controller.relay_config.tip_config)).into_response()
}

// NOTE: Copied from walrus service
#[derive(Debug, ToSchema)]
#[schema(value_type = String, format = Binary)]
pub(crate) struct Binary(());

/// Upload a Blob to the Walrus Network
///
/// Note that the Blob must have previously been registered.
///
/// This endpoint checks that any required Tip has been supplied, then fulfills a request to store
/// slivers.
#[utoipa::path(
    post,
    path = BLOB_UPLOAD_RELAY_ROUTE,
    request_body(
        content = Binary,
        content_type = "application/octet-stream",
        description = "Binary data of the unencoded blob to be stored."
        ),
    params(Params),
    responses(
        (status = 200, description = "The blob was relayed to the Walrus Network successfully"),
        // TODO(WAL-913): extend these docs with the the WalrusUploadRelayError variants.
    ),
)]
#[tracing::instrument(level = Level::ERROR, skip_all, fields(blob_id=%params.blob_id))]
pub(crate) async fn blob_upload_relay_handler(
    State(controller): State<Arc<Controller>>,
    Query(params): Query<Params>,
    body: Bytes,
) -> Result<impl IntoResponse, WalrusUploadRelayError> {
    let start = Instant::now();
    let blob_id = params.blob_id;
    tracing::info!(?params, "starting to process a relay request");
    let response = controller
        .fan_out(body, params)
        .await
        .inspect_err(|error| tracing::debug!(?error, "responding to request with error"))?;
    tracing::info!(
        duration = ?start.elapsed(),
        ?blob_id,
        "finished processing a blob upload relay request",
    );

    Ok((StatusCode::OK, Json(response)).into_response())
}

/// Create a session for uploading systematic primary slivers to the Walrus Network.
///
/// The request body is a BCS-encoded [`UnverifiedBlobMetadataWithId`]. When the relay requires a
/// tip, the request must include the tip transaction ID and nonce query parameters.
#[utoipa::path(
    post,
    path = SLIVER_UPLOAD_RELAY_SESSION_ROUTE,
    request_body(
        content = Binary,
        content_type = "application/octet-stream",
        description = "BCS-encoded unverified blob metadata with ID."
    ),
    params(Params),
    responses(
        (
            status = 201,
            description = "Sliver upload relay session created",
            body = CreateSliverUploadSessionResponse
        ),
    ),
)]
#[tracing::instrument(level = Level::ERROR, skip_all, fields(blob_id=%params.blob_id))]
pub(crate) async fn create_sliver_upload_session_handler(
    State(controller): State<Arc<Controller>>,
    Query(params): Query<Params>,
    body: Bytes,
) -> Result<impl IntoResponse, WalrusUploadRelayError> {
    let response = controller
        .create_sliver_upload_session(params, body)
        .await
        .inspect_err(|error| tracing::debug!(?error, "responding to request with error"))?;
    Ok((StatusCode::CREATED, Json(response)).into_response())
}

/// Upload one complete BCS-encoded systematic primary sliver for a relay session.
#[utoipa::path(
    put,
    path = SLIVER_UPLOAD_RELAY_PRIMARY_ROUTE,
    request_body(
        content = Binary,
        content_type = "application/octet-stream",
        description = "BCS-encoded systematic primary sliver."
    ),
    params(
        ("session_id" = String, Path, description = "Upload session ID"),
        ("sliver_index" = u16, Path, description = "Systematic primary sliver index")
    ),
    responses(
        (
            status = 202,
            description = "Primary sliver accepted",
            body = SliverUploadSessionStatus
        ),
    ),
)]
#[tracing::instrument(level = Level::ERROR, skip_all, fields(%session_id, sliver_index))]
pub(crate) async fn put_sliver_upload_session_primary_handler(
    State(controller): State<Arc<Controller>>,
    AxumPath((session_id, sliver_index)): AxumPath<(String, u16)>,
    body: Bytes,
) -> Result<impl IntoResponse, WalrusUploadRelayError> {
    let response = controller
        .put_sliver_upload_session_primary(session_id, sliver_index, body)
        .await
        .inspect_err(|error| tracing::debug!(?error, "responding to request with error"))?;
    Ok((StatusCode::ACCEPTED, Json(response)).into_response())
}

/// Finalize a sliver upload relay session and return a confirmation certificate.
#[utoipa::path(
    post,
    path = SLIVER_UPLOAD_RELAY_COMPLETE_ROUTE,
    params(("session_id" = String, Path, description = "Upload session ID")),
    responses(
        (
            status = 200,
            description = "The blob was relayed to the Walrus Network successfully"
        ),
    ),
)]
#[tracing::instrument(level = Level::ERROR, skip_all, fields(%session_id))]
pub(crate) async fn complete_sliver_upload_session_handler(
    State(controller): State<Arc<Controller>>,
    AxumPath(session_id): AxumPath<String>,
) -> Result<impl IntoResponse, WalrusUploadRelayError> {
    let response = controller
        .complete_sliver_upload_session(session_id)
        .await
        .inspect_err(|error| tracing::debug!(?error, "responding to request with error"))?;
    Ok((StatusCode::OK, Json(response)).into_response())
}

/// Returns a Walrus read client from the context and Walrus configuration.
pub async fn get_client(
    context: Option<&str>,
    walrus_config: &StdPath,
    registry: &Registry,
) -> Result<WalrusNodeClient<SuiReadClient>> {
    get_client_with_config(load_configuration(Some(walrus_config), context)?, registry).await
}

/// Returns a Walrus read client with the specified configuration.
pub async fn get_client_with_config(
    client_config: ClientConfig,
    registry: &Registry,
) -> Result<WalrusNodeClient<SuiReadClient>> {
    tracing::debug!(?client_config, "loaded client config");

    if client_config.rpc_urls.is_empty() {
        tracing::error!(
            "No RPC URLs provided in the client configuration. Upload relay requires at least one \
            RPC URL specified within your client config. (Run with RUST_LOG=debug for more \
            information.)"
        );
        std::process::exit(1);
    }

    let retriable_sui_client = RetriableSuiClient::new_for_rpc_urls(
        &client_config.rpc_urls,
        client_config.backoff_config().clone(),
        None,
        client_config.checkpoint_wait_timeout(),
    )?
    .with_metrics(Some(Arc::new(SuiClientMetricSet::new(registry))));

    let sui_read_client = client_config.new_read_client(retriable_sui_client).await?;

    let refresh_handle = client_config
        .build_refresher_and_run(sui_read_client.clone())
        .await?;
    Ok(WalrusNodeClient::new_read_client(
        client_config,
        refresh_handle,
        sui_read_client,
    )?)
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroU16, time::Duration};

    use fastcrypto::hash::{HashFunction as _, Sha256};
    use sui_types::{base_types::SuiAddress, digests::TransactionDigest};
    use tokio::time::Instant;
    use utoipa::OpenApi;
    use utoipa_redoc::Redoc;
    use walrus_sdk::{
        core::{
            DEFAULT_ENCODING,
            encoding::{EncodingConfig, EncodingFactory as _, Primary, SliverPair},
            messages::BlobPersistenceType,
            metadata::BlobMetadataApi as _,
        },
        upload_relay::{
            params::{DIGEST_LEN, HashedAuthPackage, NONCE_LEN},
            tip_config::{TipConfig, TipKind},
        },
    };

    use super::{
        SLIVER_UPLOAD_SESSION_TTL,
        SliverUploadPaidTip,
        SliverUploadSession,
        WalrusUploadRelayApiDoc,
        WalrusUploadRelayConfig,
    };
    use crate::error::WalrusUploadRelayError;

    const EXAMPLE_CONFIG_PATH: &str = "walrus_upload_relay_config_example.yaml";

    #[test]
    fn check_and_update_example_config() {
        let config = WalrusUploadRelayConfig {
            tip_config: TipConfig::SendTip {
                address: SuiAddress::from_bytes([42; 32]).expect("valid bytes"),
                kind: TipKind::Const(42),
            },
            tx_freshness_threshold: Duration::from_hours(10),
            tx_max_future_threshold: Duration::from_secs(30),
        };

        walrus_test_utils::overwrite_file_and_fail_if_not_equal(
            EXAMPLE_CONFIG_PATH,
            serde_yaml::to_string(&config).expect("serialization succeeds"),
        )
        .expect("overwrite failed");
    }

    #[test]
    fn sliver_upload_session_reconstructs_full_sliver_pairs() {
        let encoding_config =
            EncodingConfig::new(NonZeroU16::new(10).expect("test shard count is nonzero"));
        let config = encoding_config.get_for_type(DEFAULT_ENCODING);
        let blob: Vec<u8> = (0..2048).map(|index| (index % 251) as u8).collect();
        let (expected_sliver_pairs, metadata) = config
            .encode_with_metadata(blob)
            .expect("blob should encode");
        let symbol_size = metadata
            .metadata()
            .symbol_size(&encoding_config)
            .expect("metadata should have a valid symbol size");
        let mut session = SliverUploadSession::new(
            "test-session".to_string(),
            metadata.clone(),
            config,
            symbol_size,
            None,
            None,
            Instant::now() + SLIVER_UPLOAD_SESSION_TTL,
        );

        let n_systematic_primary = usize::from(config.n_source_symbols::<Primary>().get());
        for sliver_index in (0..n_systematic_primary).rev() {
            let primary = expected_sliver_pairs[sliver_index].primary.clone();
            let (_, status, _) = session
                .insert_systematic_primary_sliver(
                    sliver_index.try_into().expect("test index fits in u16"),
                    primary,
                    &encoding_config,
                )
                .expect("systematic primary sliver should be accepted");
            assert_eq!(
                status.received_systematic_primary_slivers,
                n_systematic_primary - sliver_index
            );
            assert_eq!(
                status.total_systematic_primary_slivers,
                n_systematic_primary
            );
        }

        mark_systematic_primary_uploads_complete(&mut session, n_systematic_primary, true);
        session
            .prepare_for_finalize()
            .expect("session should be ready to finalize");
        let finalized = session
            .finalize(&encoding_config)
            .expect("session should finalize");

        assert_eq!(finalized.metadata, metadata);
        assert_eq!(finalized.sliver_pairs, expected_sliver_pairs);
        assert_eq!(
            finalized.systematic_primary_sliver_count,
            n_systematic_primary
        );
        assert_eq!(finalized.blob_persistence, BlobPersistenceType::Permanent);
    }

    #[test]
    fn sliver_upload_session_validates_paid_tip_blob_digest() {
        let encoding_config =
            EncodingConfig::new(NonZeroU16::new(10).expect("test shard count is nonzero"));
        let config = encoding_config.get_for_type(DEFAULT_ENCODING);
        let blob: Vec<u8> = (0..2048).map(|index| (index % 251) as u8).collect();
        let expected_blob_digest = Sha256::digest(&blob).digest;
        let (expected_sliver_pairs, metadata) = config
            .encode_with_metadata(blob)
            .expect("blob should encode");
        let symbol_size = metadata
            .metadata()
            .symbol_size(&encoding_config)
            .expect("metadata should have a valid symbol size");
        let nonce = [23; NONCE_LEN];
        let paid_tip = SliverUploadPaidTip {
            tx_id: TransactionDigest::new([7; DIGEST_LEN]),
            auth_package: HashedAuthPackage {
                blob_digest: expected_blob_digest,
                nonce_digest: Sha256::digest(nonce).digest,
                unencoded_length: metadata.metadata().unencoded_length(),
            },
        };
        let mut session = SliverUploadSession::new(
            "test-session".to_string(),
            metadata,
            config,
            symbol_size,
            None,
            Some(paid_tip),
            Instant::now() + SLIVER_UPLOAD_SESSION_TTL,
        );

        insert_systematic_primary_slivers(
            &mut session,
            &expected_sliver_pairs,
            &encoding_config,
            config.n_source_symbols::<Primary>().get().into(),
        );

        mark_systematic_primary_uploads_complete(
            &mut session,
            config.n_source_symbols::<Primary>().get().into(),
            true,
        );
        session
            .prepare_for_finalize()
            .expect("session should be ready to finalize");
        session
            .finalize(&encoding_config)
            .expect("paid session with matching blob digest should finalize");
    }

    #[test]
    fn sliver_upload_session_rejects_paid_tip_blob_digest_mismatch() {
        let encoding_config =
            EncodingConfig::new(NonZeroU16::new(10).expect("test shard count is nonzero"));
        let config = encoding_config.get_for_type(DEFAULT_ENCODING);
        let blob: Vec<u8> = (0..2048).map(|index| (index % 251) as u8).collect();
        let (expected_sliver_pairs, metadata) = config
            .encode_with_metadata(blob)
            .expect("blob should encode");
        let symbol_size = metadata
            .metadata()
            .symbol_size(&encoding_config)
            .expect("metadata should have a valid symbol size");
        let nonce = [23; NONCE_LEN];
        let paid_tip = SliverUploadPaidTip {
            tx_id: TransactionDigest::new([7; DIGEST_LEN]),
            auth_package: HashedAuthPackage {
                blob_digest: [42; DIGEST_LEN],
                nonce_digest: Sha256::digest(nonce).digest,
                unencoded_length: metadata.metadata().unencoded_length(),
            },
        };
        let mut session = SliverUploadSession::new(
            "test-session".to_string(),
            metadata,
            config,
            symbol_size,
            None,
            Some(paid_tip),
            Instant::now() + SLIVER_UPLOAD_SESSION_TTL,
        );

        insert_systematic_primary_slivers(
            &mut session,
            &expected_sliver_pairs,
            &encoding_config,
            config.n_source_symbols::<Primary>().get().into(),
        );

        mark_systematic_primary_uploads_complete(
            &mut session,
            config.n_source_symbols::<Primary>().get().into(),
            true,
        );
        session
            .prepare_for_finalize()
            .expect("session should be ready to finalize");
        let error = session
            .finalize(&encoding_config)
            .expect_err("paid session with mismatched blob digest should fail");

        assert!(matches!(error, WalrusUploadRelayError::BlobDigestMismatch));
    }

    #[test]
    fn sliver_upload_session_falls_back_to_primary_reupload_after_fanout_failure() {
        let encoding_config =
            EncodingConfig::new(NonZeroU16::new(10).expect("test shard count is nonzero"));
        let config = encoding_config.get_for_type(DEFAULT_ENCODING);
        let blob: Vec<u8> = (0..2048).map(|index| (index % 251) as u8).collect();
        let (expected_sliver_pairs, metadata) = config
            .encode_with_metadata(blob)
            .expect("blob should encode");
        let symbol_size = metadata
            .metadata()
            .symbol_size(&encoding_config)
            .expect("metadata should have a valid symbol size");
        let mut session = SliverUploadSession::new(
            "test-session".to_string(),
            metadata,
            config,
            symbol_size,
            None,
            None,
            Instant::now() + SLIVER_UPLOAD_SESSION_TTL,
        );

        let n_systematic_primary = usize::from(config.n_source_symbols::<Primary>().get());
        insert_systematic_primary_slivers(
            &mut session,
            &expected_sliver_pairs,
            &encoding_config,
            n_systematic_primary,
        );
        if n_systematic_primary > 1 {
            mark_systematic_primary_uploads_complete(&mut session, n_systematic_primary - 1, true);
        }
        session.complete_systematic_primary_upload(false);
        session
            .prepare_for_finalize()
            .expect("session should be ready to finalize");

        let finalized = session
            .finalize(&encoding_config)
            .expect("session should finalize after falling back");

        assert_eq!(finalized.systematic_primary_sliver_count, 0);
    }

    fn insert_systematic_primary_slivers(
        session: &mut SliverUploadSession,
        sliver_pairs: &[SliverPair],
        encoding_config: &EncodingConfig,
        n_systematic_primary: usize,
    ) {
        for (sliver_index, sliver_pair) in
            sliver_pairs.iter().enumerate().take(n_systematic_primary)
        {
            let primary = sliver_pair.primary.clone();
            session
                .insert_systematic_primary_sliver(
                    sliver_index.try_into().expect("test index fits in u16"),
                    primary,
                    encoding_config,
                )
                .expect("systematic primary sliver should be accepted");
        }
    }

    fn mark_systematic_primary_uploads_complete(
        session: &mut SliverUploadSession,
        count: usize,
        success: bool,
    ) {
        for _ in 0..count {
            session.complete_systematic_primary_upload(success);
        }
    }

    /// Serializes the upload relay's openAPI spec when this test is run.
    ///
    /// This test ensures that the files `upload_relay_openapi.yaml` and
    /// `upload_relay_openapi.html` are kept in sync with changes to the spec.
    #[test]
    fn check_and_update_openapi_spec() {
        let label = "upload_relay";
        let spec_path = format!("{label}_openapi.yaml");
        let html_path = format!("{label}_openapi.html");

        let mut spec = WalrusUploadRelayApiDoc::openapi();
        spec.info.version = "<VERSION>".to_string();

        std::fs::write(html_path, Redoc::new(spec.clone()).to_html())
            .expect("should be able to write to disk");

        if let Err(error) = walrus_test_utils::overwrite_file_and_fail_if_not_equal(
            spec_path,
            spec.to_yaml().expect("should be a ble to encode to yaml"),
        ) {
            panic!("the OpenAPI spec was updated by the test: {error:?}");
        };
    }
}

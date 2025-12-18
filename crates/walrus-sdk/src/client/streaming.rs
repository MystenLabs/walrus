// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Client for streaming blob data sliver-by-sliver.

use std::{
    collections::BTreeMap,
    num::NonZeroU32,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use futures::{
    Stream,
    StreamExt as _,
    future::{AbortHandle, AbortRegistration, Abortable},
};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, Notify};
use walrus_core::{
    BlobId,
    EncodingType,
    Epoch,
    SliverIndex,
    encoding::{EncodingFactory, Primary, SliverData},
    metadata::{BlobMetadataApi as _, VerifiedBlobMetadataWithId},
};
use walrus_sui::client::ReadClient;

use crate::{
    client::WalrusNodeClient,
    error::{ClientError, ClientErrorKind, ClientResult},
};

/// Configuration for the StreamingReadClient.
#[serde_with::serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StreamingConfig {
    /// Maximum number of retry attempts per sliver before aborting.
    pub max_sliver_retry_attempts: usize,
    /// Timeout duration for individual sliver retrieval.
    #[serde_as(as = "serde_with::DurationSeconds")]
    #[serde(rename = "sliver_timeout_secs")]
    pub sliver_timeout: Duration,
    /// Number of slivers to prefetch ahead of the current streaming position.
    pub prefetch_count: u16,
    /// Base backoff duration for retry logic (in milliseconds).
    #[serde_as(as = "serde_with::DurationMilliSeconds")]
    #[serde(rename = "retry_base_backoff_ms")]
    pub retry_base_backoff: Duration,
    /// Maximum backoff duration for retry logic (in milliseconds).
    #[serde_as(as = "serde_with::DurationMilliSeconds")]
    #[serde(rename = "retry_max_backoff_ms")]
    pub retry_max_backoff: Duration,
}

impl StreamingConfig {
    /// Creates a new StreamingReadClientConfig.
    pub fn new(
        max_sliver_retry_attempts: usize,
        sliver_timeout: Duration,
        prefetch_count: u16,
    ) -> Self {
        Self {
            max_sliver_retry_attempts,
            sliver_timeout,
            prefetch_count,
            retry_base_backoff: Duration::from_millis(100),
            retry_max_backoff: Duration::from_millis(2000),
        }
    }
}

impl Default for StreamingConfig {
    fn default() -> Self {
        Self {
            max_sliver_retry_attempts: 5,
            sliver_timeout: Duration::from_secs(30),
            prefetch_count: 4,
            retry_base_backoff: Duration::from_millis(100),
            retry_max_backoff: Duration::from_millis(2000),
        }
    }
}

/// Represents the state of a single sliver in the streaming pipeline.
///
/// Note: Slivers not yet requested are simply absent from the map rather than
/// having an explicit "pending" state.
#[derive(Debug)]
enum SliverFetchState {
    /// Sliver retrieval is in progress. Contains the abort handle to cancel if needed.
    InFlight(AbortHandle),
    /// Sliver was successfully retrieved.
    Ready(SliverData<Primary>),
    /// Sliver retrieval failed after all retries.
    Failed(ClientError),
}

/// Manages the prefetch buffer and ordering of slivers for streaming.
struct SliverPrefetchBuffer {
    /// Map of sliver index to its fetch state.
    slivers: BTreeMap<u16, SliverFetchState>,
    /// The next sliver index to stream to the client.
    next_to_stream: u16,
    /// Total number of slivers in the blob.
    total_slivers: u16,
    /// Number of slivers to keep in-flight ahead of current position.
    prefetch_count: u16,
}

impl SliverPrefetchBuffer {
    /// Creates a new SliverPrefetchBuffer.
    fn new(total_slivers: u16, prefetch_count: u16) -> Self {
        Self {
            slivers: BTreeMap::new(),
            next_to_stream: 0,
            total_slivers,
            prefetch_count,
        }
    }

    /// Returns sliver indices that should be fetched next.
    ///
    /// Returns indices for slivers that are not yet in the map (i.e., not yet requested).
    fn get_indices_to_fetch(&self) -> Vec<SliverIndex> {
        let mut indices = Vec::new();
        let end = std::cmp::min(
            self.next_to_stream.saturating_add(self.prefetch_count),
            self.total_slivers,
        );

        for idx in self.next_to_stream..end {
            if !self.slivers.contains_key(&idx) {
                indices.push(SliverIndex::new(idx));
            }
        }
        indices
    }

    /// Marks a sliver as in-flight with its abort handle.
    fn set_in_flight(&mut self, index: u16, abort_handle: AbortHandle) {
        self.slivers
            .insert(index, SliverFetchState::InFlight(abort_handle));
    }

    /// Updates a sliver to ready state with the fetched data.
    fn set_ready(&mut self, index: u16, data: SliverData<Primary>) {
        self.slivers.insert(index, SliverFetchState::Ready(data));
    }

    /// Updates a sliver to failed state.
    fn set_failed(&mut self, index: u16, error: ClientError) {
        self.slivers.insert(index, SliverFetchState::Failed(error));
    }

    /// Attempts to take the next sliver to stream (if ready).
    /// Returns None if streaming is complete or next sliver is still in-flight.
    /// Returns Some(Ok(sliver)) and advances the next_to_stream if the next sliver is ready.
    /// Returns Some(Err(error)) if the next sliver failed.
    #[must_use]
    fn try_take_next(&mut self) -> Option<Result<SliverData<Primary>, ClientError>> {
        if self.next_to_stream >= self.total_slivers {
            return None;
        }

        match self.slivers.remove(&self.next_to_stream) {
            Some(SliverFetchState::Ready(data)) => {
                self.next_to_stream += 1;
                Some(Ok(data))
            }
            Some(SliverFetchState::Failed(e)) => Some(Err(e)),
            Some(in_flight @ SliverFetchState::InFlight(_)) => {
                // Still fetching, put it back
                self.slivers.insert(self.next_to_stream, in_flight);
                None
            }
            // Not yet requested - shouldn't happen if prefetching is working correctly.
            None => None,
        }
    }

    /// Returns true if all slivers have been streamed.
    fn is_complete(&self) -> bool {
        self.next_to_stream >= self.total_slivers
    }
}

impl Drop for SliverPrefetchBuffer {
    fn drop(&mut self) {
        // Abort all in-flight tasks to avoid resource leaks.
        for (_, state) in self.slivers.iter() {
            if let SliverFetchState::InFlight(abort_handle) = state {
                abort_handle.abort();
            }
        }
    }
}

#[derive(Clone)]
struct StreamingState {
    /// Prefetch buffer managing sliver states.
    prefetch_buffer: Arc<Mutex<SliverPrefetchBuffer>>,
    /// Notifier for when slivers become ready.
    notify: Arc<Notify>,
    /// Whether the stream has been aborted due to an error.
    aborted: Arc<AtomicBool>,
    /// Blob metadata.
    metadata: Arc<VerifiedBlobMetadataWithId>,
    /// Certified epoch.
    certified_epoch: Epoch,
    /// Size of each primary sliver in bytes.
    primary_sliver_size: NonZeroU32,
    /// Total blob size in bytes.
    blob_size: u64,
    /// Total slivers in the blob.
    total_slivers: u16,
}

impl StreamingState {
    fn is_aborted(&self) -> bool {
        self.aborted.load(Ordering::Relaxed)
    }

    async fn is_complete(&self) -> bool {
        self.prefetch_buffer.lock().await.is_complete()
    }

    fn set_aborted(&self) {
        self.aborted.store(true, Ordering::Relaxed)
    }

    async fn wait_for_notify(&self) {
        self.notify.notified().await
    }

    async fn poll_for_next_sliver(&self) -> Option<Result<Vec<u8>, ClientError>> {
        // Check if next sliver is ready
        let mut prefetch_buffer = self.prefetch_buffer.lock().await;
        prefetch_buffer.try_take_next().map(|result| {
            result.map(|sliver| {
                extract_sliver_data(
                    self.primary_sliver_size,
                    self.blob_size,
                    self.total_slivers,
                    sliver,
                )
            })
        })
    }
}

/// Creates a stream that yields blob data chunks in order, sliver by sliver.
///
/// This method retrieves blob data progressively, prefetching slivers ahead
/// of the current streaming position to minimize latency. Each chunk yielded
/// corresponds to one sliver's worth of data (except the last sliver which
/// may be trimmed to the actual blob size).
///
/// # Errors
///
/// Returns an error if:
/// - The blob is blocked
/// - The blob doesn't exist
/// - Metadata retrieval fails
/// - The encoding type is not supported
///
/// Returns the stream and the total blob size in bytes (for progress tracking).
pub async fn start_streaming_blob<T: ReadClient + Sync + Send + 'static>(
    client: Arc<WalrusNodeClient<T>>,
    config: StreamingConfig,
    blob_id: BlobId,
) -> ClientResult<(impl Stream<Item = Result<Bytes, ClientError>> + Send, u64)> {
    tracing::debug!(%blob_id, "starting to stream blob");

    let (certified_epoch, _) = client
        .get_blob_status_and_certified_epoch(&blob_id, None)
        .await?;

    let metadata = client.retrieve_metadata(certified_epoch, &blob_id).await?;

    if metadata.metadata().encoding_type() != EncodingType::RS2 {
        return Err(ClientError::from(ClientErrorKind::Other(
            format!(
                "streaming read client only supports RS2 encoding, got {}",
                metadata.metadata().encoding_type()
            )
            .into(),
        )));
    }

    let blob_size = metadata.metadata().unencoded_length();

    // Handle zero-size blobs early - return empty stream
    if blob_size == 0 {
        tracing::debug!(%blob_id, "zero-size blob, returning empty stream");
        return Ok((futures::stream::empty().left_stream(), 0));
    }

    let primary_sliver_size = get_primary_sliver_size(&client, blob_size, &metadata)?;
    let total_slivers = u16::try_from(blob_size.div_ceil(u64::from(primary_sliver_size.get())))
        .map_err(|_| {
            ClientError::from(ClientErrorKind::Other(
                "primary sliver count exceeds u16 max".into(),
            ))
        })?;

    tracing::debug!(
        %blob_id,
        blob_size,
        primary_sliver_size,
        total_slivers,
        "blob metadata retrieved for streaming"
    );

    // Create the streaming state
    let state = StreamingState {
        prefetch_buffer: Arc::new(Mutex::new(SliverPrefetchBuffer::new(
            total_slivers,
            config.prefetch_count,
        ))),
        notify: Arc::new(Notify::new()),
        aborted: Arc::new(AtomicBool::from(false)),
        metadata: Arc::new(metadata),
        certified_epoch,
        primary_sliver_size,
        blob_size,
        total_slivers,
    };

    Ok((
        create_sliver_stream(client, config, state).right_stream(),
        blob_size,
    ))
}

/// Gets the size of the primary sliver for the given blob size and metadata.
fn get_primary_sliver_size<T: ReadClient + Sync + Send + 'static>(
    client: &WalrusNodeClient<T>,
    blob_size: u64,
    metadata: &VerifiedBlobMetadataWithId,
) -> ClientResult<NonZeroU32> {
    let encoding_config = client
        .encoding_config()
        .get_for_type(metadata.metadata().encoding_type());
    let primary_sliver_size = encoding_config
        .sliver_size_for_blob::<Primary>(blob_size)
        .map_err(|_| {
            ClientError::from(ClientErrorKind::Other(
                "blob too large to determine sliver size".into(),
            ))
        })?;
    Ok(primary_sliver_size)
}

/// Creates the async stream that prefetches and yields sliver data.
fn create_sliver_stream<T: ReadClient + Sync + Send + 'static>(
    client: Arc<WalrusNodeClient<T>>,
    config: StreamingConfig,
    state: StreamingState,
) -> impl Stream<Item = Result<Bytes, ClientError>> + Send {
    futures::stream::unfold(
        (state, client, config),
        |(state, client, config)| async move {
            loop {
                if state.is_aborted() {
                    return None;
                }

                spawn_prefetch_tasks(client.clone(), config.clone(), state.clone()).await;

                if state.is_complete().await {
                    return None;
                }

                match state.poll_for_next_sliver().await {
                    Some(Ok(data)) => {
                        return Some((Ok(Bytes::from(data)), (state, client, config)));
                    }
                    Some(Err(e)) => {
                        state.set_aborted();
                        return Some((Err(e), (state, client, config)));
                    }
                    None => state.wait_for_notify().await,
                }
            }
        },
    )
}

/// Retrieves a single sliver with aggressive retry logic.
async fn retrieve_single_sliver_with_retry<T: ReadClient + Sync + Send + 'static>(
    client: &WalrusNodeClient<T>,
    config: &StreamingConfig,
    metadata: Arc<VerifiedBlobMetadataWithId>,
    sliver_index: SliverIndex,
    certified_epoch: Epoch,
) -> ClientResult<SliverData<Primary>> {
    let mut attempts = 0;
    let mut backoff = config.retry_base_backoff;

    loop {
        // TODO: let's avoid calling retrieve_slivers_retry_committees, and consider writing a
        // sincle sliver fetch function (rather than the multi-sliver fetch which scans all nodes
        // for sliver/shard matches).
        let result: Result<Vec<SliverData<Primary>>, ClientError> = client
            .retrieve_slivers_retry_committees::<Primary>(
                metadata.as_ref(),
                &[sliver_index],
                certified_epoch,
                1, // Single attempt per round
                config.sliver_timeout,
            )
            .await;

        match result {
            Ok(slivers) if !slivers.is_empty() => {
                return slivers.into_iter().next().ok_or_else(|| {
                    ClientError::from(ClientErrorKind::Other(
                        format!(
                            "unexpected empty sliver result for sliver {}",
                            sliver_index.get()
                        )
                        .into(),
                    ))
                });
            }
            Ok(_) => {
                tracing::warn!(
                    sliver_index = sliver_index.get(),
                    attempt = attempts,
                    "sliver retrieval returned empty, will retry"
                );
            }
            Err(e) => {
                tracing::warn!(
                    sliver_index = sliver_index.get(),
                    attempt = attempts,
                    error = ?e,
                    "sliver retrieval failed, will retry"
                );
            }
        }

        attempts += 1;
        if attempts >= config.max_sliver_retry_attempts {
            return Err(ClientError::from(ClientErrorKind::Other(
                format!(
                    "failed to retrieve sliver {} after {} attempts",
                    sliver_index.get(),
                    attempts
                )
                .into(),
            )));
        }

        // Exponential backoff with cap
        tokio::time::sleep(backoff).await;
        backoff = std::cmp::min(backoff * 2, config.retry_max_backoff);
    }
}

/// Spawns prefetch tasks for slivers that need to be fetched.
async fn spawn_prefetch_tasks<T: ReadClient + Sync + Send + 'static>(
    client: Arc<WalrusNodeClient<T>>,
    config: StreamingConfig,
    state: StreamingState,
) {
    let tasks_to_spawn: Vec<(SliverIndex, AbortRegistration)> = {
        let mut prefetch_buffer = state.prefetch_buffer.lock().await;
        let indices = prefetch_buffer.get_indices_to_fetch();

        let tasks: Vec<_> = indices
            .into_iter()
            .map(|index| {
                let (abort_handle, abort_registration) = AbortHandle::new_pair();
                prefetch_buffer.set_in_flight(index.get(), abort_handle);
                (index, abort_registration)
            })
            .collect();

        tasks
    };
    let certified_epoch = state.certified_epoch;

    // Spawn all tasks outside the lock
    for (index, abort_registration) in tasks_to_spawn {
        let client = client.clone();
        let config = config.clone();
        let state = state.clone();

        tokio::spawn(Abortable::new(
            async move {
                let result = retrieve_single_sliver_with_retry(
                    &client,
                    &config,
                    state.metadata.clone(),
                    index,
                    certified_epoch,
                )
                .await;

                {
                    let mut prefetch_buffer = state.prefetch_buffer.lock().await;
                    match result {
                        Ok(sliver) => prefetch_buffer.set_ready(index.get(), sliver),
                        Err(e) => prefetch_buffer.set_failed(index.get(), e),
                    }
                }
                state.notify.clone().notify_one();
            },
            abort_registration,
        ));
    }
}

/// Extracts the data from a sliver, handling the last sliver specially.
fn extract_sliver_data(
    primary_sliver_size: NonZeroU32,
    blob_size: u64,
    total_slivers: u16,
    sliver: SliverData<Primary>,
) -> Vec<u8> {
    let mut sliver_data = sliver.symbols.into_vec();
    let sliver_index = u64::from(sliver.index.get());
    let total_slivers = u64::from(total_slivers);

    let is_last_sliver = total_slivers > 0 && sliver_index == total_slivers.saturating_sub(1);

    if is_last_sliver && blob_size > 0 {
        // Calculate expected data in last sliver
        let full_slivers_size = (total_slivers - 1) * u64::from(primary_sliver_size.get());
        let last_sliver_data_size =
            usize::try_from(blob_size - full_slivers_size).expect("should fit in u64");

        // Trim padding from last sliver
        if last_sliver_data_size < sliver_data.len() {
            sliver_data.truncate(last_sliver_data_size)
        }
    }
    sliver_data
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sliver_prefetch_buffer_basic() {
        let mut buffer = SliverPrefetchBuffer::new(10, 4);

        // Initially should want to fetch first 4 slivers
        let indices = buffer.get_indices_to_fetch();
        assert_eq!(indices.len(), 4);
        assert_eq!(indices[0].get(), 0);
        assert_eq!(indices[3].get(), 3);

        // Mark some as in-flight
        let (abort_handle, _) = AbortHandle::new_pair();
        buffer.set_in_flight(0, abort_handle);

        // Should not return in-flight slivers
        let indices = buffer.get_indices_to_fetch();
        assert_eq!(indices.len(), 3);
        assert_eq!(indices[0].get(), 1);
    }

    #[test]
    fn test_sliver_prefetch_buffer_advance() {
        let mut buffer = SliverPrefetchBuffer::new(3, 2);

        // Set up first sliver as ready
        buffer.set_ready(
            0,
            SliverData::new(
                vec![1, 2, 3],
                std::num::NonZeroU16::new(1).unwrap(),
                SliverIndex::new(0),
            ),
        );

        // Take it
        let result = buffer.try_take_next();
        assert!(result.is_some());
        assert!(result.unwrap().is_ok());

        // Advance
        assert_eq!(buffer.next_to_stream, 1);
        assert!(!buffer.is_complete());

        // Advance through remaining
        buffer.set_ready(
            1,
            SliverData::new(
                vec![4, 5, 6],
                std::num::NonZeroU16::new(1).unwrap(),
                SliverIndex::new(1),
            ),
        );
        let _ = buffer.try_take_next();

        buffer.set_ready(
            2,
            SliverData::new(
                vec![7, 8, 9],
                std::num::NonZeroU16::new(1).unwrap(),
                SliverIndex::new(2),
            ),
        );
        let _ = buffer.try_take_next();

        assert!(buffer.is_complete());
    }

    #[test]
    fn test_config_defaults() {
        let config = StreamingConfig::default();
        assert_eq!(config.max_sliver_retry_attempts, 5);
        assert_eq!(config.sliver_timeout, Duration::from_secs(30));
        assert_eq!(config.prefetch_count, 4);
    }
}

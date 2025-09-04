// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::HashMap,
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime},
};

use axum::{
    body::Bytes,
    extract::Request,
    http::{HeaderMap, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use clap::Args;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::RwLock,
};
use uuid::Uuid;

/// Headers used for chunked upload protocol
pub const UPLOAD_SESSION_ID_HEADER: &str = "X-Upload-Session-Id";
pub const UPLOAD_CHUNK_INDEX_HEADER: &str = "X-Upload-Chunk-Index";
pub const UPLOAD_TOTAL_CHUNKS_HEADER: &str = "X-Upload-Total-Chunks";
pub const UPLOAD_CHUNK_SIZE_HEADER: &str = "X-Upload-Chunk-Size";
pub const UPLOAD_TOTAL_SIZE_HEADER: &str = "X-Upload-Total-Size";

/// Configuration for chunked upload middleware
#[derive(Debug, Clone)]
pub struct ChunkedUploadConfig {
    /// Directory to store partial uploads
    pub storage_dir: PathBuf,
    /// Maximum time to keep incomplete upload sessions
    pub session_timeout: Duration,
    /// Interval between cleanup runs for expired sessions
    pub cleanup_interval: Duration,
    /// Maximum size of any total blob.
    pub max_total_size: u64,
}

#[cfg(test)]
impl Default for ChunkedUploadConfig {
    fn default() -> Self {
        Self {
            storage_dir: defaults::storage_dir(),
            session_timeout: Duration::from_secs(defaults::session_timeout_seconds().into()),
            cleanup_interval: Duration::from_secs(defaults::cleanup_interval_seconds().into()),
            max_total_size: 1024 * 1024 * 1024, // 1 GiB
        }
    }
}

impl ChunkedUploadConfig {
    pub fn new(args: ChunkedUploadArgs, max_total_size: u64) -> Self {
        Self {
            storage_dir: args.storage_dir,
            session_timeout: Duration::from_secs(args.session_timeout_seconds.into()),
            cleanup_interval: Duration::from_secs(args.cleanup_interval_seconds.into()),
            max_total_size,
        }
    }
}

/// Configuration for chunked upload middleware
#[derive(Debug, Clone, Args, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct ChunkedUploadArgs {
    /// Directory to store partial uploads
    #[arg(long = "chunked-upload-storage-dir")]
    pub storage_dir: PathBuf,
    /// Maximum time to keep incomplete upload sessions
    #[arg(long = "chunked-upload-session-timeout-seconds")]
    pub session_timeout_seconds: u32,
    /// Interval between cleanup runs for expired sessions
    #[arg(long = "chunked-upload-cleanup-interval-seconds")]
    pub cleanup_interval_seconds: u32,
}

impl Default for ChunkedUploadArgs {
    fn default() -> Self {
        Self {
            storage_dir: defaults::storage_dir(),
            session_timeout_seconds: defaults::session_timeout_seconds(),
            cleanup_interval_seconds: defaults::cleanup_interval_seconds(),
        }
    }
}

/// Metadata about an upload session
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadSession {
    pub session_id: Uuid,
    pub total_chunks: u32,
    pub total_size: u64,
    pub chunk_size: u64, // Expected size for all chunks except the last
    pub created_at: SystemTime,
    pub last_activity: SystemTime,
    pub received_chunks: Vec<bool>, // Track which chunks have been received
    pub chunks_received: u32,
}

/// State management for chunked uploads
#[derive(Debug)]
pub struct ChunkedUploadState {
    config: ChunkedUploadConfig,
    sessions: Arc<RwLock<HashMap<Uuid, UploadSession>>>,
}

impl ChunkedUploadState {
    pub fn new(config: ChunkedUploadConfig) -> Self {
        Self {
            config,
            sessions: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Start a background task that periodically cleans up expired sessions
    /// This ensures only one cleanup task runs globally, regardless of how many ChunkedUploadState
    /// instances exist. Returns true if a new task was started, false if one was already running.
    pub async fn start_cleanup_task(self: Arc<Self>) -> bool {
        let mut handle_guard = CLEANUP_TASK_HANDLE.lock().await;

        // Check if cleanup task is already running
        if handle_guard.as_ref().is_some_and(|h| !h.is_finished()) {
            tracing::debug!("Cleanup task already running, skipping startup");
            return false;
        }

        let cleanup_interval = self.config.cleanup_interval;
        let state = Arc::clone(&self);

        let task_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(cleanup_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                interval.tick().await;
                tracing::info!("Running periodic cleanup of expired upload sessions");
                state.cleanup_expired_sessions().await;
            }
        });

        *handle_guard = Some(task_handle);
        tracing::info!("Chunked upload cleanup task started as singleton");
        true
    }

    /// Initialize storage directory and restore any existing sessions
    pub async fn init(&self) -> Result<(), std::io::Error> {
        tokio::fs::create_dir_all(&self.config.storage_dir).await?;
        tracing::info!(
            "Initialized chunked upload storage at {:?}",
            self.config.storage_dir
        );

        self.restore_sessions_from_disk().await?;

        Ok(())
    }

    /// Restore sessions from disk on startup
    async fn restore_sessions_from_disk(&self) -> Result<(), std::io::Error> {
        let mut dir_entries = tokio::fs::read_dir(&self.config.storage_dir).await?;
        let mut restored_count = 0;

        while let Some(entry) = dir_entries.next_entry().await? {
            let path = entry.path();
            if let Some(file_name) = path.file_name().and_then(|n| n.to_str())
                && file_name.ends_with(".json")
            {
                let session_id: Uuid = file_name
                    .strip_suffix(".json")
                    .expect("checked above")
                    .parse()
                    .expect("invalid UUID in filename");

                match self.load_session_from_disk(&session_id).await {
                    Ok(session) => {
                        let mut sessions = self.sessions.write().await;
                        sessions.insert(session_id, session);
                        restored_count += 1;
                    }
                    Err(e) => {
                        tracing::warn!("Failed to restore session {}: {}", session_id, e);
                        // Clean up corrupted metadata file
                        let _ = tokio::fs::remove_file(&path).await;
                    }
                }
            }
        }

        if restored_count > 0 {
            tracing::info!("Restored {} upload sessions from disk", restored_count);
        }

        Ok(())
    }

    /// Load session metadata from disk
    async fn load_session_from_disk(
        &self,
        session_id: &Uuid,
    ) -> Result<UploadSession, std::io::Error> {
        let meta_path = self.get_metadata_path(session_id);
        let meta_content = tokio::fs::read_to_string(&meta_path).await?;
        let session: UploadSession = serde_json::from_str(&meta_content)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        Ok(session)
    }

    /// Save session metadata to disk
    async fn save_session_to_disk(&self, session: &UploadSession) -> Result<(), std::io::Error> {
        let meta_path = self.get_metadata_path(&session.session_id);
        let meta_content = serde_json::to_string_pretty(session)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        tokio::fs::write(&meta_path, meta_content).await?;
        Ok(())
    }

    /// Get the path for session metadata file
    fn get_metadata_path(&self, session_id: &Uuid) -> PathBuf {
        self.config.storage_dir.join(format!("{}.json", session_id))
    }

    /// Get the path for session data file
    fn get_data_path(&self, session_id: &Uuid) -> PathBuf {
        self.config.storage_dir.join(format!("{}.data", session_id))
    }

    /// Clean up expired sessions
    pub async fn cleanup_expired_sessions(&self) {
        let now = SystemTime::now();
        let mut sessions = self.sessions.write().await;
        let expired_sessions: Vec<Uuid> = sessions
            .iter()
            .filter_map(|(id, session)| {
                if now
                    .duration_since(session.last_activity)
                    .unwrap_or_default()
                    > self.config.session_timeout
                {
                    Some(id)
                } else {
                    None
                }
            })
            .cloned()
            .collect();

        for session_id in expired_sessions {
            if let Some(_session) = sessions.remove(&session_id) {
                tracing::warn!("Cleaning up expired upload session: {}", session_id);

                let data_path = self.get_data_path(&session_id);

                if let Err(e) = tokio::fs::remove_file(&data_path).await {
                    tracing::error!(
                        "Failed to remove expired session data file {}: {}",
                        session_id,
                        e
                    );
                }

                let meta_path = self.get_metadata_path(&session_id);
                if let Err(e) = tokio::fs::remove_file(&meta_path).await {
                    tracing::error!(
                        "Failed to remove expired session metadata file {}: {}",
                        session_id,
                        e
                    );
                }
            }
        }
    }

    /// Get or create an upload session
    async fn get_or_create_session(
        &self,
        session_id: &Uuid,
        total_chunks: u32,
        total_size: u64,
        chunk_size: u64,
    ) -> Result<UploadSession, ChunkedUploadError> {
        let mut sessions = self.sessions.write().await;
        if let Some(existing_session) = sessions.get_mut(session_id) {
            // Validate session parameters match
            if existing_session.total_chunks != total_chunks
                || existing_session.total_size != total_size
                || existing_session.chunk_size != chunk_size
            {
                return Err(ChunkedUploadError::SessionMismatch);
            }
            existing_session.last_activity = SystemTime::now();
            let session_clone = existing_session.clone();

            // Save updated last activity to disk
            if let Err(e) = self.save_session_to_disk(&session_clone).await {
                tracing::error!(
                    "Failed to update session metadata for {}: {}",
                    session_id,
                    e
                );
                // Don't fail the request, but log the error
            }

            return Ok(session_clone);
        }

        // Validate limits
        if total_size == 0 {
            return Err(ChunkedUploadError::TotalSizeZero);
        }

        if total_size > self.config.max_total_size {
            return Err(ChunkedUploadError::TotalSizeTooLarge);
        }

        if chunk_size > self.config.max_total_size {
            return Err(ChunkedUploadError::ChunkTooLarge);
        }

        if total_chunks == 0 {
            return Err(ChunkedUploadError::ZeroChunks);
        }

        if u64::from(total_chunks) * 2 > total_size {
            return Err(ChunkedUploadError::TooManyChunks);
        }

        // Validate that chunk_size and total_size are compatible.
        // All chunks except the last must be chunk_size.
        let expected_full_chunks = total_chunks - 1;
        if u64::from(expected_full_chunks) * chunk_size > total_size {
            return Err(ChunkedUploadError::ChunkTooLarge);
        }

        let last_chunk_size = total_size - (u64::from(expected_full_chunks) * chunk_size);
        if last_chunk_size > chunk_size {
            return Err(ChunkedUploadError::SessionMismatch);
        }

        // Create new session
        let session = UploadSession {
            session_id: *session_id,
            total_chunks,
            total_size,
            chunk_size,
            created_at: SystemTime::now(),
            last_activity: SystemTime::now(),
            received_chunks: vec![false; total_chunks as usize],
            chunks_received: 0,
        };

        sessions.insert(*session_id, session.clone());

        self.save_session_to_disk(&session).await?;

        tracing::info!(
            "Created new upload session: {} ({} chunks, {} bytes)",
            session_id,
            total_chunks,
            total_size
        );

        Ok(session)
    }

    /// Store a chunk for an upload session
    async fn store_chunk(
        &self,
        session_id: &Uuid,
        chunk_index: u32,
        chunk_data: Bytes,
    ) -> Result<UploadSession, ChunkedUploadError> {
        let session_data_path = self.get_data_path(session_id);

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .truncate(false)
            .open(&session_data_path)
            .await
            .map_err(ChunkedUploadError::IoError)?;

        let mut sessions = self.sessions.write().await;
        let session = sessions
            .get_mut(session_id)
            .ok_or(ChunkedUploadError::SessionNotFound)?;

        // Validate chunk size - all chunks except the last must match session chunk_size
        let is_last_chunk = chunk_index == session.total_chunks - 1;
        let expected_chunk_size = if is_last_chunk {
            // Last chunk can be smaller - calculate remaining bytes
            session.total_size - (u64::from(session.total_chunks - 1) * session.chunk_size)
        } else {
            // Non-last chunks must be exactly the session chunk_size
            session.chunk_size
        };

        if chunk_data.len() as u64 != expected_chunk_size {
            return Err(ChunkedUploadError::ChunkSizeMismatch {
                expected: expected_chunk_size,
                actual: chunk_data.len() as u64,
                chunk_index,
            });
        }

        // Check if chunk was already received
        if session.received_chunks[chunk_index as usize] {
            tracing::debug!(
                "Chunk {} already received for session {}",
                chunk_index,
                session_id
            );
            return Ok(session.clone());
        }

        // Calculate offset and write chunk
        let chunk_offset = u64::from(chunk_index) * session.chunk_size;
        file.seek(std::io::SeekFrom::Start(chunk_offset))
            .await
            .map_err(ChunkedUploadError::IoError)?;
        file.write_all(&chunk_data)
            .await
            .map_err(ChunkedUploadError::IoError)?;
        file.flush().await.map_err(ChunkedUploadError::IoError)?;

        // Mark chunk as received
        session.received_chunks[chunk_index as usize] = true;
        session.chunks_received += 1;
        session.last_activity = SystemTime::now();

        let session_clone = session.clone();

        // Save updated session metadata to disk
        if let Err(e) = self.save_session_to_disk(&session_clone).await {
            tracing::error!("Failed to save session metadata for {}: {}", session_id, e);
            // Don't fail the request, but log the error
        }

        tracing::debug!(
            "Stored chunk {} for session {} ({}/{} chunks received)",
            chunk_index,
            session_id,
            session.chunks_received,
            session.total_chunks
        );

        Ok(session_clone)
    }

    /// Check if upload session is complete and return the full blob data
    async fn try_complete_upload(
        &self,
        session_id: &Uuid,
    ) -> Result<Option<Bytes>, ChunkedUploadError> {
        let total_size = {
            let sessions = self.sessions.read().await;
            let session = sessions
                .get(session_id)
                .ok_or(ChunkedUploadError::SessionNotFound)?;

            // Check if all chunks are received
            if session.chunks_received != session.total_chunks {
                return Ok(None);
            }

            session.total_size
        };

        // Read complete file
        let session_data_path = self.get_data_path(session_id);
        let mut file = File::open(&session_data_path)
            .await
            .map_err(ChunkedUploadError::IoError)?;

        let total_size = usize::try_from(total_size).expect("size too large");
        let mut buffer = Vec::with_capacity(total_size);
        let bytes_read = file
            .read_to_end(&mut buffer)
            .await
            .map_err(ChunkedUploadError::IoError)?;

        // Verify we read the expected amount
        if bytes_read != total_size {
            return Err(ChunkedUploadError::IncompleteUpload);
        }

        // Clean up session from memory and disk
        let mut sessions = self.sessions.write().await;
        sessions.remove(session_id);

        // Remove both data and metadata files
        let session_meta_path = self.get_metadata_path(session_id);

        if let Err(e) = tokio::fs::remove_file(&session_data_path).await {
            tracing::warn!(
                "Failed to clean up completed session data file {}: {}",
                session_id,
                e
            );
        }

        if let Err(e) = tokio::fs::remove_file(&session_meta_path).await {
            tracing::warn!(
                "Failed to clean up completed session metadata file {}: {}",
                session_id,
                e
            );
        }

        tracing::info!(
            "Completed upload session: {} ({} bytes)",
            session_id,
            bytes_read
        );
        Ok(Some(Bytes::from(buffer)))
    }
}

static CLEANUP_TASK_HANDLE: Lazy<tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>> =
    Lazy::new(|| tokio::sync::Mutex::new(None));

/// Errors that can occur during chunked uploads
#[derive(Debug, thiserror::Error)]
pub enum ChunkedUploadError {
    #[error("Upload session not found")]
    SessionNotFound,
    #[error("Session parameters mismatch")]
    SessionMismatch,
    #[error("Total size is zero")]
    TotalSizeZero,
    #[error("Total size exceeds limit")]
    TotalSizeTooLarge,
    #[error("Zero is an invalid number for total_chunks")]
    ZeroChunks,
    #[error("Too many chunks")]
    TooManyChunks,
    #[error("Chunk size exceeds limit")]
    ChunkTooLarge,
    #[error(
        "Chunk size mismatch: expected {expected} bytes, got {actual} bytes for chunk {chunk_index}"
    )]
    ChunkSizeMismatch {
        expected: u64,
        actual: u64,
        chunk_index: u32,
    },
    #[error("Upload is incomplete")]
    IncompleteUpload,
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Invalid header value: {0}")]
    InvalidHeader(String),
    #[error("Missing required header: {0}")]
    MissingHeader(String),
}

impl IntoResponse for ChunkedUploadError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            ChunkedUploadError::SessionNotFound => (StatusCode::NOT_FOUND, self.to_string()),
            ChunkedUploadError::TotalSizeTooLarge => {
                (StatusCode::PAYLOAD_TOO_LARGE, self.to_string())
            }
            ChunkedUploadError::ChunkTooLarge => (StatusCode::PAYLOAD_TOO_LARGE, self.to_string()),
            ChunkedUploadError::IoError(_) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                String::from("Internal server error"),
            ),
            ChunkedUploadError::SessionMismatch
            | ChunkedUploadError::MissingHeader(_)
            | ChunkedUploadError::InvalidHeader(_)
            | ChunkedUploadError::IncompleteUpload
            | ChunkedUploadError::ChunkSizeMismatch { .. }
            | ChunkedUploadError::TooManyChunks
            | ChunkedUploadError::ZeroChunks
            | ChunkedUploadError::TotalSizeZero => (StatusCode::BAD_REQUEST, self.to_string()),
        };
        (status, message).into_response()
    }
}

/// Extract chunked upload metadata from headers
#[derive(Debug)]
struct ChunkMetadata {
    session_id: Uuid,
    chunk_index: u32,
    total_chunks: u32,
    chunk_size: u64,
    total_size: u64,
}

impl ChunkMetadata {
    fn from_headers(headers: &HeaderMap) -> Result<Self, ChunkedUploadError> {
        let session_id = headers
            .get(UPLOAD_SESSION_ID_HEADER)
            .ok_or_else(|| ChunkedUploadError::MissingHeader(UPLOAD_SESSION_ID_HEADER.to_string()))?
            .to_str()
            .map_err(|_| ChunkedUploadError::InvalidHeader(UPLOAD_SESSION_ID_HEADER.to_string()))?
            .parse()
            .map_err(|_| ChunkedUploadError::InvalidHeader(UPLOAD_SESSION_ID_HEADER.to_string()))?;

        let chunk_index = headers
            .get(UPLOAD_CHUNK_INDEX_HEADER)
            .ok_or_else(|| {
                ChunkedUploadError::MissingHeader(UPLOAD_CHUNK_INDEX_HEADER.to_string())
            })?
            .to_str()
            .map_err(|_| ChunkedUploadError::InvalidHeader(UPLOAD_CHUNK_INDEX_HEADER.to_string()))?
            .parse()
            .map_err(|_| {
                ChunkedUploadError::InvalidHeader(UPLOAD_CHUNK_INDEX_HEADER.to_string())
            })?;

        let total_chunks = headers
            .get(UPLOAD_TOTAL_CHUNKS_HEADER)
            .ok_or_else(|| {
                ChunkedUploadError::MissingHeader(UPLOAD_TOTAL_CHUNKS_HEADER.to_string())
            })?
            .to_str()
            .map_err(|_| ChunkedUploadError::InvalidHeader(UPLOAD_TOTAL_CHUNKS_HEADER.to_string()))?
            .parse()
            .map_err(|_| {
                ChunkedUploadError::InvalidHeader(UPLOAD_TOTAL_CHUNKS_HEADER.to_string())
            })?;

        let chunk_size = headers
            .get(UPLOAD_CHUNK_SIZE_HEADER)
            .ok_or_else(|| ChunkedUploadError::MissingHeader(UPLOAD_CHUNK_SIZE_HEADER.to_string()))?
            .to_str()
            .map_err(|_| ChunkedUploadError::InvalidHeader(UPLOAD_CHUNK_SIZE_HEADER.to_string()))?
            .parse()
            .map_err(|_| ChunkedUploadError::InvalidHeader(UPLOAD_CHUNK_SIZE_HEADER.to_string()))?;

        let total_size = headers
            .get(UPLOAD_TOTAL_SIZE_HEADER)
            .ok_or_else(|| ChunkedUploadError::MissingHeader(UPLOAD_TOTAL_SIZE_HEADER.to_string()))?
            .to_str()
            .map_err(|_| ChunkedUploadError::InvalidHeader(UPLOAD_TOTAL_SIZE_HEADER.to_string()))?
            .parse()
            .map_err(|_| ChunkedUploadError::InvalidHeader(UPLOAD_TOTAL_SIZE_HEADER.to_string()))?;

        Ok(ChunkMetadata {
            session_id,
            chunk_index,
            total_chunks,
            chunk_size,
            total_size,
        })
    }
}

/// Check if request has chunked upload headers
fn is_chunked_upload(headers: &HeaderMap) -> bool {
    headers.contains_key(UPLOAD_SESSION_ID_HEADER)
}

/// Chunked upload middleware function
pub async fn chunked_upload_middleware(
    axum::extract::State(upload_state): axum::extract::State<Arc<ChunkedUploadState>>,
    request: Request,
    next: Next,
) -> Result<Response, Response> {
    // Check if this is a chunked upload request
    if !is_chunked_upload(request.headers()) {
        // Not a chunked upload, pass through
        return Ok(next.run(request).await);
    }

    tracing::debug!("Processing chunked upload request");

    // Extract chunk metadata from headers
    let metadata = ChunkMetadata::from_headers(request.headers()).map_err(|e| e.into_response())?;

    // Extract request body
    let (parts, body) = request.into_parts();
    let body_bytes = axum::body::to_bytes(body, usize::MAX)
        .await
        .map_err(|_| (StatusCode::BAD_REQUEST, "Failed to read request body").into_response())?;

    // Reconstruct request
    let mut request = Request::from_parts(parts, axum::body::Body::empty());

    // Get or create upload session
    upload_state
        .get_or_create_session(
            &metadata.session_id,
            metadata.total_chunks,
            metadata.total_size,
            metadata.chunk_size,
        )
        .await
        .map_err(|e| e.into_response())?;

    // Store the chunk
    let session = upload_state
        .store_chunk(&metadata.session_id, metadata.chunk_index, body_bytes)
        .await
        .map_err(|e| e.into_response())?;

    // Check if upload is complete
    if let Some(complete_blob) = upload_state
        .try_complete_upload(&metadata.session_id)
        .await
        .map_err(|e| e.into_response())?
    {
        // Upload is complete, reconstruct request with full blob
        tracing::info!(
            "Upload session {} complete, forwarding to handler",
            metadata.session_id
        );

        // Replace request with complete blob
        let (parts, _) = request.into_parts();
        request = Request::from_parts(parts, axum::body::Body::from(complete_blob));

        // Remove chunked upload headers
        let headers = request.headers_mut();
        headers.remove(UPLOAD_SESSION_ID_HEADER);
        headers.remove(UPLOAD_CHUNK_INDEX_HEADER);
        headers.remove(UPLOAD_TOTAL_CHUNKS_HEADER);
        headers.remove(UPLOAD_CHUNK_SIZE_HEADER);
        headers.remove(UPLOAD_TOTAL_SIZE_HEADER);

        // Forward to the actual blob handler
        Ok(next.run(request).await)
    } else {
        // Upload still in progress, return status
        // Find any missing chunks up to the highest received chunk index
        let highest_received_index = session
            .received_chunks
            .iter()
            .enumerate()
            .filter_map(|(i, &received)| {
                if received {
                    Some(u32::try_from(i).expect("index too large"))
                } else {
                    None
                }
            })
            .max();

        let missing_chunks = if let Some(max_index) = highest_received_index {
            // Check for gaps in chunks from 0 to max_index
            (0..=max_index)
                .filter(|&i| !session.received_chunks[i as usize])
                .collect::<Vec<u32>>()
        } else {
            // No chunks received yet
            Vec::new()
        };

        let response_body = serde_json::json!({
            "session_id": metadata.session_id,
            "chunks_received": session.chunks_received,
            "total_chunks": session.total_chunks,
            "complete": false,
            "missing_chunks": missing_chunks
        });

        Ok((StatusCode::ACCEPTED, axum::Json(response_body)).into_response())
    }
}

/// Default values for chunked upload configuration.
pub mod defaults {
    use std::path::PathBuf;

    /// Default storage directory path.
    pub fn storage_dir() -> PathBuf {
        std::env::temp_dir().join("walrus-publisher-chunked-uploads")
    }

    /// Default session timeout (1 hour).
    pub fn session_timeout_seconds() -> u32 {
        3600
    }

    /// Default cleanup interval (5 minutes).
    pub fn cleanup_interval_seconds() -> u32 {
        300
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[tokio::test]
    async fn test_chunked_upload_session_creation() {
        let temp_dir = TempDir::new().unwrap();
        let config = ChunkedUploadConfig {
            storage_dir: temp_dir.path().to_path_buf(),
            ..Default::default()
        };

        let state = ChunkedUploadState::new(config);
        state.init().await.unwrap();

        let session_id = Uuid::new_v4();
        let session = state
            .get_or_create_session(&session_id, 3, 1000, 400)
            .await
            .unwrap();

        assert_eq!(session.session_id, session_id);
        assert_eq!(session.total_chunks, 3);
        assert_eq!(session.total_size, 1000);
        assert_eq!(session.chunks_received, 0);
    }

    #[tokio::test]
    async fn test_chunk_storage_and_completion() {
        let temp_dir = TempDir::new().unwrap();
        let config = ChunkedUploadConfig {
            storage_dir: temp_dir.path().to_path_buf(),
            ..Default::default()
        };

        let state = ChunkedUploadState::new(config);
        state.init().await.unwrap();

        let session_id = Uuid::new_v4();
        let total_size = 250u64;
        let total_chunks = 3u32;

        // Create session
        state
            .get_or_create_session(&session_id, total_chunks, total_size, 100)
            .await
            .unwrap();

        // Store chunks
        let chunk1 = Bytes::from(vec![1u8; 100]);
        let chunk2 = Bytes::from(vec![2u8; 100]);
        let chunk3 = Bytes::from(vec![3u8; 50]);

        state.store_chunk(&session_id, 0, chunk1).await.unwrap();
        state.store_chunk(&session_id, 1, chunk2).await.unwrap();

        // Should not be complete yet
        assert!(
            state
                .try_complete_upload(&session_id)
                .await
                .unwrap()
                .is_none()
        );

        // Store final chunk
        state.store_chunk(&session_id, 2, chunk3).await.unwrap();

        // Should now be complete
        let complete_blob = state
            .try_complete_upload(&session_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(complete_blob.len(), 250);
    }

    #[tokio::test]
    async fn test_session_persistence_and_restoration() {
        let temp_dir = TempDir::new().unwrap();
        let config = ChunkedUploadConfig {
            storage_dir: temp_dir.path().to_path_buf(),
            ..Default::default()
        };

        let session_id = Uuid::new_v4();
        let total_size = 200u64;
        let total_chunks = 2u32;

        // First state instance - create session and store one chunk
        {
            let state = ChunkedUploadState::new(config.clone());
            state.init().await.unwrap();

            // Create session and store first chunk
            state
                .get_or_create_session(&session_id, total_chunks, total_size, 100)
                .await
                .unwrap();

            let chunk1 = Bytes::from(vec![1u8; 100]);
            let session = state.store_chunk(&session_id, 0, chunk1).await.unwrap();

            assert_eq!(session.chunks_received, 1);
            assert_eq!(session.total_chunks, 2);

            // Verify files exist on disk
            let data_path = state.get_data_path(&session_id);
            let meta_path = state.get_metadata_path(&session_id);
            assert!(tokio::fs::metadata(&data_path).await.is_ok());
            assert!(tokio::fs::metadata(&meta_path).await.is_ok());
        }

        // Second state instance - should restore session and complete upload
        {
            let state = ChunkedUploadState::new(config);
            state.init().await.unwrap(); // This should restore the session

            // Verify session was restored
            let sessions = state.sessions.read().await;
            assert!(sessions.contains_key(&session_id));
            let restored_session = sessions.get(&session_id).unwrap();
            assert_eq!(restored_session.chunks_received, 1);
            assert_eq!(restored_session.total_chunks, 2);
            drop(sessions);

            // Store second chunk to complete upload
            let chunk2 = Bytes::from(vec![2u8; 100]);
            state.store_chunk(&session_id, 1, chunk2).await.unwrap();

            // Should now be complete
            let complete_blob = state
                .try_complete_upload(&session_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(complete_blob.len(), 200);

            // Verify files were cleaned up
            let data_path = state.get_data_path(&session_id);
            let meta_path = state.get_metadata_path(&session_id);
            assert!(tokio::fs::metadata(&data_path).await.is_err());
            assert!(tokio::fs::metadata(&meta_path).await.is_err());
        }
    }

    #[tokio::test]
    async fn test_background_cleanup_task() {
        let temp_dir = TempDir::new().unwrap();
        let config = ChunkedUploadConfig {
            storage_dir: temp_dir.path().to_path_buf(),
            session_timeout: Duration::from_millis(100), // Very short timeout for testing
            ..Default::default()
        };

        let state = Arc::new(ChunkedUploadState::new(config));
        state.init().await.unwrap();

        // Create a session that will expire quickly
        let session_id = Uuid::new_v4();
        let _session = state
            .get_or_create_session(&session_id, 2, 200, 100)
            .await
            .unwrap();

        // Store one chunk
        let chunk = Bytes::from(vec![1u8; 100]);
        state.store_chunk(&session_id, 0, chunk).await.unwrap();

        // Verify session exists
        {
            let sessions = state.sessions.read().await;
            assert!(sessions.contains_key(&session_id));
        }

        // Wait for session to expire
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Manually call cleanup (simulating what the background task would do)
        state.cleanup_expired_sessions().await;

        // Verify session was cleaned up
        {
            let sessions = state.sessions.read().await;
            assert!(!sessions.contains_key(&session_id));
        }

        // Verify files were cleaned up
        let data_path = state.get_data_path(&session_id);
        let meta_path = state.get_metadata_path(&session_id);
        assert!(tokio::fs::metadata(&data_path).await.is_err());
        assert!(tokio::fs::metadata(&meta_path).await.is_err());
    }

    #[tokio::test]
    async fn test_missing_chunk_detection() {
        let temp_dir = TempDir::new().unwrap();
        let config = ChunkedUploadConfig {
            storage_dir: temp_dir.path().to_path_buf(),
            ..Default::default()
        };

        let state = ChunkedUploadState::new(config);
        state.init().await.unwrap();

        let session_id = Uuid::new_v4();
        let total_chunks = 5u32;
        let total_size = 500u64;

        // Create session
        state
            .get_or_create_session(&session_id, total_chunks, total_size, 100)
            .await
            .unwrap();

        // Store chunks 0, 2, and 4 (missing 1 and 3)
        let chunk = Bytes::from(vec![1u8; 100]);
        state
            .store_chunk(&session_id, 0, chunk.clone())
            .await
            .unwrap();
        state
            .store_chunk(&session_id, 2, chunk.clone())
            .await
            .unwrap();
        state
            .store_chunk(&session_id, 4, chunk.clone())
            .await
            .unwrap();

        // Check session state directly to test our missing chunk logic
        let sessions = state.sessions.read().await;
        let session = sessions.get(&session_id).unwrap();

        // Find highest received index
        let highest_received_index = session
            .received_chunks
            .iter()
            .enumerate()
            .filter_map(|(i, &received)| {
                if received {
                    u32::try_from(i).ok()
                } else {
                    None
                }
            })
            .max();

        assert_eq!(highest_received_index, Some(4));

        // Find missing chunks up to highest received index
        let missing_chunks: Vec<u32> = (0..=4)
            .filter(|&i| !session.received_chunks[i as usize])
            .collect();

        assert_eq!(missing_chunks, vec![1, 3]);
    }

    #[tokio::test]
    async fn test_chunk_size_enforcement() {
        let temp_dir = TempDir::new().unwrap();
        let config = ChunkedUploadConfig {
            storage_dir: temp_dir.path().to_path_buf(),
            ..Default::default()
        };

        let state = ChunkedUploadState::new(config);
        state.init().await.unwrap();

        let session_id = Uuid::new_v4();
        let total_chunks = 3u32;
        let total_size = 250u64;
        let chunk_size = 100u64;

        // Create session
        state
            .get_or_create_session(&session_id, total_chunks, total_size, chunk_size)
            .await
            .unwrap();

        // Try to store first chunk with correct size
        let correct_chunk = Bytes::from(vec![1u8; 100]);
        state
            .store_chunk(&session_id, 0, correct_chunk)
            .await
            .unwrap();

        // Try to store second chunk with wrong size (should fail)
        let wrong_size_chunk = Bytes::from(vec![2u8; 50]); // 50 bytes instead of 100
        let result = state.store_chunk(&session_id, 1, wrong_size_chunk).await;
        assert!(matches!(
            result,
            Err(ChunkedUploadError::ChunkSizeMismatch { .. })
        ));

        // Try to store last chunk with correct smaller size
        let last_chunk = Bytes::from(vec![3u8; 50]); // Last chunk can be 50 bytes (250 - 2*100)
        state.store_chunk(&session_id, 2, last_chunk).await.unwrap();

        // Try to store last chunk with wrong size (should fail)
        let wrong_last_chunk = Bytes::from(vec![4u8; 60]); // 60 bytes instead of expected 50
        let result = state.store_chunk(&session_id, 2, wrong_last_chunk).await;
        assert!(matches!(
            result,
            Err(ChunkedUploadError::ChunkSizeMismatch { .. })
        ));
    }
}

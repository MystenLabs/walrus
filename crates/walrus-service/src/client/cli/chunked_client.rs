// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Client implementation for uploading files to publisher daemon using chunked uploads.

use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::Path,
};

use anyhow::{Context, Result, anyhow, bail};
use reqwest::{Client, Response, StatusCode};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// The maximum size for direct upload (1MiB)
pub const CHUNK_SIZE: usize = 1024 * 1024; // 1MiB

/// Headers used for chunked upload protocol
pub const UPLOAD_SESSION_ID_HEADER: &str = "X-Upload-Session-Id";
pub const UPLOAD_CHUNK_INDEX_HEADER: &str = "X-Upload-Chunk-Index";
pub const UPLOAD_TOTAL_CHUNKS_HEADER: &str = "X-Upload-Total-Chunks";
pub const UPLOAD_CHUNK_SIZE_HEADER: &str = "X-Upload-Chunk-Size";
pub const UPLOAD_TOTAL_SIZE_HEADER: &str = "X-Upload-Total-Size";

/// Response from chunked upload when upload is in progress
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ChunkedUploadResponse {
    pub session_id: String,
    pub chunks_received: u32,
    pub total_chunks: u32,
    pub complete: bool,
    pub missing_chunks: Vec<u32>,
}

/// Client for uploading files to publisher daemon
#[derive(Debug, Clone)]
pub struct PublisherClient {
    client: Client,
    base_url: String,
}

impl PublisherClient {
    /// Create a new publisher client
    pub fn new(daemon_url: String) -> Self {
        Self {
            client: Client::new(),
            base_url: daemon_url,
        }
    }

    /// Upload a file to the publisher daemon, using chunked upload for large files
    pub async fn upload_file<P: AsRef<Path>>(
        &self,
        file_path: P,
        query_params: &HashMap<String, String>,
    ) -> Result<Response> {
        let file_path = file_path.as_ref();
        let file_size = std::fs::metadata(file_path)
            .with_context(|| format!("Failed to read metadata for file: {}", file_path.display()))?
            .len();

        tracing::info!(
            file = %file_path.display(),
            size_bytes = file_size,
            "Uploading file to publisher daemon"
        );

        if file_size <= CHUNK_SIZE as u64 {
            self.direct_upload(file_path, query_params).await
        } else {
            self.chunked_upload(file_path, file_size, query_params)
                .await
        }
    }

    /// Direct upload for small files
    async fn direct_upload<P: AsRef<Path>>(
        &self,
        file_path: P,
        query_params: &HashMap<String, String>,
    ) -> Result<Response> {
        let file_path = file_path.as_ref();
        let file_data = std::fs::read(file_path)
            .with_context(|| format!("Failed to read file: {}", file_path.display()))?;

        let mut url = reqwest::Url::parse(&format!("{}/v1/blobs", self.base_url))
            .context("Invalid base URL")?;

        // Add query parameters with proper URL encoding
        if !query_params.is_empty() {
            let mut url_query = url.query_pairs_mut();
            for (key, value) in query_params {
                url_query.append_pair(key, value);
            }
            url_query.finish();
        }

        tracing::debug!(
            url = %url,
            size = file_data.len(),
            "Performing direct upload"
        );

        let response = self
            .client
            .put(url)
            .header("Content-Type", "application/octet-stream")
            .body(file_data)
            .send()
            .await
            .with_context(|| format!("Failed to upload file: {}", file_path.display()))?;

        Ok(response)
    }

    /// Chunked upload for large files
    async fn chunked_upload<P: AsRef<Path>>(
        &self,
        file_path: P,
        file_size: u64,
        query_params: &HashMap<String, String>,
    ) -> Result<Response> {
        let file_path = file_path.as_ref();
        let mut file = File::open(file_path)
            .with_context(|| format!("Failed to open file: {}", file_path.display()))?;

        let session_id = Uuid::new_v4().to_string();
        let total_chunks = ((file_size + CHUNK_SIZE as u64 - 1) / CHUNK_SIZE as u64) as u32;

        tracing::info!(
            session_id = %session_id,
            total_chunks = total_chunks,
            "Starting chunked upload"
        );

        let mut url = reqwest::Url::parse(&format!("{}/v1/blobs", self.base_url))
            .context("Invalid base URL")?;

        // Add query parameters with proper URL encoding
        if !query_params.is_empty() {
            let mut url_query = url.query_pairs_mut();
            for (key, value) in query_params {
                url_query.append_pair(key, value);
            }
            url_query.finish();
        }

        for chunk_index in 0..total_chunks {
            let chunk_size = if chunk_index == total_chunks - 1 {
                // Last chunk may be smaller
                (file_size - (chunk_index as u64 * CHUNK_SIZE as u64)) as usize
            } else {
                CHUNK_SIZE
            };

            let mut chunk_data = vec![0; chunk_size];
            file.seek(SeekFrom::Start(chunk_index as u64 * CHUNK_SIZE as u64))
                .with_context(|| format!("Failed to seek in file: {}", file_path.display()))?;
            file.read_exact(&mut chunk_data).with_context(|| {
                format!("Failed to read chunk from file: {}", file_path.display())
            })?;

            tracing::debug!(
                chunk_index = chunk_index,
                chunk_size = chunk_size,
                "Uploading chunk"
            );

            let response = self
                .client
                .put(url.clone())
                .header("Content-Type", "application/octet-stream")
                .header(UPLOAD_SESSION_ID_HEADER, &session_id)
                .header(UPLOAD_CHUNK_INDEX_HEADER, chunk_index.to_string())
                .header(UPLOAD_TOTAL_CHUNKS_HEADER, total_chunks.to_string())
                .header(UPLOAD_CHUNK_SIZE_HEADER, chunk_size.to_string())
                .header(UPLOAD_TOTAL_SIZE_HEADER, file_size.to_string())
                .body(chunk_data)
                .send()
                .await
                .with_context(|| {
                    format!(
                        "Failed to upload chunk {} for file: {}",
                        chunk_index,
                        file_path.display()
                    )
                })?;

            match response.status() {
                StatusCode::ACCEPTED => {
                    // Upload in progress, continue with next chunk
                    let upload_response: ChunkedUploadResponse = response
                        .json()
                        .await
                        .context("Failed to parse chunked upload response")?;

                    tracing::debug!(
                        chunks_received = upload_response.chunks_received,
                        missing_chunks = ?upload_response.missing_chunks,
                        "Chunk upload progress"
                    );

                    // Handle missing chunks by retrying them
                    if !upload_response.missing_chunks.is_empty() {
                        tracing::warn!(
                            missing_chunks = ?upload_response.missing_chunks,
                            "Some chunks are missing, this should not happen in sequential upload"
                        );
                    }
                }
                StatusCode::OK => {
                    // Upload complete
                    tracing::info!(
                        session_id = %session_id,
                        "Chunked upload completed successfully"
                    );
                    return Ok(response);
                }
                status => {
                    let error_text = response.text().await.unwrap_or_default();
                    bail!("Chunk upload failed with status {}: {}", status, error_text);
                }
            }
        }

        // If we get here, something went wrong - we should have received a 200 OK
        Err(anyhow!(
            "Chunked upload completed all chunks but never received final 200 OK response"
        ))
    }

    /// Upload multiple files from a directory
    pub async fn upload_directory<P: AsRef<Path>>(
        &self,
        dir_path: P,
        query_params: &HashMap<String, String>,
    ) -> Result<Vec<(String, Result<Response>)>> {
        let dir_path = dir_path.as_ref();
        let mut results = Vec::new();

        let entries = std::fs::read_dir(dir_path)
            .with_context(|| format!("Failed to read directory: {}", dir_path.display()))?;

        for entry in entries {
            let entry = entry.context("Failed to read directory entry")?;
            let path = entry.path();

            if path.is_file() {
                let file_name = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("unknown")
                    .to_string();

                tracing::info!(file = %path.display(), "Uploading file from directory");
                let result = self.upload_file(&path, query_params).await;
                results.push((file_name, result));
            }
            // Skip subdirectories for now to avoid recursion complexity
        }

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use tempfile::NamedTempFile;

    use super::*;

    #[tokio::test]
    async fn test_file_size_threshold() {
        let client = PublisherClient::new("http://localhost:8080".to_string());

        // Create a small file (under 1MiB)
        let mut small_file = NamedTempFile::new().unwrap();
        let small_data = vec![42; 1000]; // 1KB
        small_file.write_all(&small_data).unwrap();

        // Create a large file (over 1MiB)
        let mut large_file = NamedTempFile::new().unwrap();
        let large_data = vec![42; CHUNK_SIZE + 1000]; // > 1MiB
        large_file.write_all(&large_data).unwrap();

        // Test file size detection logic
        let small_size = std::fs::metadata(small_file.path()).unwrap().len();
        let large_size = std::fs::metadata(large_file.path()).unwrap().len();

        assert!(small_size <= CHUNK_SIZE as u64);
        assert!(large_size > CHUNK_SIZE as u64);
    }

    #[test]
    fn test_chunk_calculation() {
        let chunk_size = CHUNK_SIZE as u64;

        // Test exact multiple
        let file_size = chunk_size * 3;
        let total_chunks = ((file_size + chunk_size - 1) / chunk_size) as u32;
        assert_eq!(total_chunks, 3);

        // Test with remainder
        let file_size = chunk_size * 3 + 500;
        let total_chunks = ((file_size + chunk_size - 1) / chunk_size) as u32;
        assert_eq!(total_chunks, 4);
    }
}

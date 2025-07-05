// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! S3 object operations handlers.

use crate::error::{S3Error, S3Result};
use crate::handlers::S3State;
use crate::metadata::ObjectMetadata;
use crate::s3_types::{ListObjectsResponse, S3Object};
use crate::utils;
use axum::body::Body;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, Method, Uri};
use axum::response::Response;
use bytes::Bytes;
use std::collections::HashMap;
use tracing::{debug, error, info};
use walrus_core::EncodingType;
use walrus_sdk::client::responses::BlobStoreResult;
use walrus_sui::client::{BlobPersistence, PostStoreAction};

/// List objects in a bucket.
pub async fn list_objects(
    State(state): State<S3State>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> S3Result<Response> {
    debug!("Handling ListObjects request");
    
    // Authenticate the request
    state.authenticate(&method, &uri, &headers, &[])?;
    
    // Parse bucket name from URI
    let (bucket_name, _) = utils::parse_s3_path(&uri)?;
    utils::validate_bucket_name(&bucket_name)?;
    
    info!("ListObjects request for bucket '{}' authenticated successfully", bucket_name);
    
    // Parse query parameters
    let prefix = params.get("prefix").cloned();
    let max_keys = params.get("max-keys")
        .and_then(|s| s.parse::<i32>().ok())
        .unwrap_or(1000);
    let marker = params.get("marker").cloned();
    
    // Get objects from metadata store
    let (objects, is_truncated) = state.metadata_store.list_objects(
        &bucket_name,
        prefix.as_deref(),
        max_keys,
        marker.as_deref(),
    ).await?;
    
    let mut response = ListObjectsResponse::new(bucket_name);
    response.prefix = prefix;
    response.marker = marker;
    response.max_keys = max_keys;
    response.is_truncated = is_truncated;
    
    // Convert metadata to S3 objects
    for metadata in objects {
        let s3_object = S3Object {
            key: metadata.key,
            size: metadata.size,
            last_modified: metadata.last_modified,
            etag: metadata.etag,
            storage_class: "STANDARD".to_string(),
            content_type: metadata.content_type,
            metadata: HashMap::new(),
        };
        response.contents.push(s3_object);
    }
    
    Ok(Response::builder()
        .status(200)
        .header("Content-Type", "application/xml")
        .body(response.to_xml().into())
        .unwrap())
}

/// Get an object.
pub async fn get_object(
    State(state): State<S3State>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
) -> S3Result<Response> {
    debug!("Handling GetObject request");
    
    // Authenticate the request
    state.authenticate(&method, &uri, &headers, &[])?;
    
    // Parse bucket and object key from URI
    let (bucket_name, key) = utils::parse_s3_path(&uri)?;
    let key = key.ok_or(S3Error::InvalidRequest("Object key is required".to_string()))?;
    
    utils::validate_bucket_name(&bucket_name)?;
    utils::validate_object_key(&key)?;
    
    info!("GetObject request for bucket '{}', key '{}' authenticated successfully", bucket_name, key);
    
    // Get object metadata from metadata store
    let metadata = match state.metadata_store.get_object(&bucket_name, &key).await? {
        Some(metadata) => metadata,
        None => {
            info!("Object not found: bucket '{}', key '{}'", bucket_name, key);
            return Err(S3Error::NoSuchKey);
        }
    };
    
    // Parse blob ID from string
    let blob_id = metadata.blob_id.parse::<walrus_core::BlobId>()
        .map_err(|e| S3Error::InternalError(format!("Invalid blob ID: {}", e)))?;
    
    // Retrieve the blob from Walrus using the read client
    match state.read_client.read_blob_retry_committees::<walrus_core::encoding::Primary>(&blob_id).await {
        Ok(data) => {
            info!("Successfully retrieved blob {} from Walrus", blob_id);
            
            let mut response_builder = Response::builder()
                .status(200)
                .header("Content-Length", data.len())
                .header("ETag", metadata.etag);
            
            if let Some(content_type) = metadata.content_type {
                response_builder = response_builder.header("Content-Type", content_type);
            } else {
                response_builder = response_builder.header("Content-Type", "application/octet-stream");
            }
            
            // Add user metadata as x-amz-meta-* headers
            for (key, value) in metadata.user_metadata {
                response_builder = response_builder.header(format!("x-amz-meta-{}", key), value);
            }
            
            Ok(response_builder
                .body(Body::from(data))
                .unwrap())
        }
        Err(e) => {
            error!("Failed to retrieve blob {} from Walrus: {}", blob_id, e);
            Err(S3Error::from(e))
        }
    }
}

/// Put an object.
pub async fn put_object(
    State(state): State<S3State>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> S3Result<Response> {
    debug!("Handling PutObject request");
    
    // Parse bucket and object key from URI
    let (bucket_name, key) = utils::parse_s3_path(&uri)?;
    let key = key.ok_or(S3Error::InvalidRequest("Object key is required".to_string()))?;
    
    utils::validate_bucket_name(&bucket_name)?;
    utils::validate_object_key(&key)?;
    
    // Authenticate the request and extract credentials
    let (access_key, secret_key) = state.authenticator.authenticate_and_extract(
        &method, &uri, &headers, &body
    )?;
    
    info!("PutObject request for bucket '{}', key '{}' authenticated successfully", bucket_name, key);
    
    // Create a write client for this user
    let authorization_header = headers.get("authorization").and_then(|v| v.to_str().ok());
    let write_client = state.create_write_client(&access_key, &secret_key, authorization_header).await?;
    
    // Parse headers
    let content_type = utils::parse_content_type(&headers);
    let metadata = utils::extract_metadata(&headers);
    
    // Store the blob in Walrus
    let blob_data = body.as_ref();
    let blob_slice = [blob_data];
    
    // Store the blob using the user's authenticated client
    match write_client.reserve_and_store_blobs_retry_committees(
        &blob_slice,
        EncodingType::RS2,
        1, // epochs_ahead
        walrus_sdk::store_optimizations::StoreOptimizations::none(),
        BlobPersistence::Permanent,
        PostStoreAction::Keep,
        None, // metrics
    ).await {
        Ok(mut results) => {
            if let Some(result) = results.pop() {
                match result {
                    BlobStoreResult::NewlyCreated { blob_object, .. } => {
                        let blob_id = blob_object.blob_id;
                        
                        // Store metadata
                        let object_metadata = ObjectMetadata {
                            bucket: bucket_name.clone(),
                            key: key.clone(),
                            blob_id: blob_id.to_string(),
                            size: blob_data.len() as u64,
                            content_type,
                            etag: format!("\"{}\"", blob_id.to_string()),
                            last_modified: chrono::Utc::now(),
                            user_metadata: metadata,
                        };
                        
                        state.metadata_store.put_object(object_metadata).await?;
                        
                        info!("Successfully stored object '{}' in bucket '{}' with blob ID: {}", 
                             key, bucket_name, blob_id);
                        
                        Ok(Response::builder()
                            .status(200)
                            .header("ETag", format!("\"{}\"", blob_id.to_string()))
                            .header("Content-Length", "0")
                            .body(Body::empty())
                            .unwrap())
                    }
                    BlobStoreResult::AlreadyCertified { blob_id, .. } => {
                        // Update metadata for existing blob
                        let object_metadata = ObjectMetadata {
                            bucket: bucket_name.clone(),
                            key: key.clone(),
                            blob_id: blob_id.to_string(),
                            size: blob_data.len() as u64,
                            content_type,
                            etag: format!("\"{}\"", blob_id.to_string()),
                            last_modified: chrono::Utc::now(),
                            user_metadata: metadata,
                        };
                        
                        state.metadata_store.put_object(object_metadata).await?;
                        
                        info!("Updated metadata for existing object '{}' in bucket '{}' with blob ID: {}", 
                             key, bucket_name, blob_id);
                        
                        Ok(Response::builder()
                            .status(200)
                            .header("ETag", format!("\"{}\"", blob_id.to_string()))
                            .header("Content-Length", "0")
                            .body(Body::empty())
                            .unwrap())
                    }
                    BlobStoreResult::Error { error_msg, .. } => {
                        error!("Failed to store blob: {}", error_msg);
                        Err(S3Error::InternalError(format!("Failed to store blob: {}", error_msg)))
                    }
                    BlobStoreResult::MarkedInvalid { blob_id, .. } => {
                        error!("Blob {} was marked as invalid", blob_id);
                        Err(S3Error::InternalError("Blob was marked as invalid".to_string()))
                    }
                }
            } else {
                Err(S3Error::InternalError("No result returned from blob store".to_string()))
            }
        }
        Err(e) => {
            error!("Failed to store blob in Walrus: {}", e);
            Err(S3Error::InternalError(format!("Failed to store blob: {}", e)))
        }
    }
}

/// Delete an object.
pub async fn delete_object(
    State(state): State<S3State>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
) -> S3Result<Response> {
    debug!("Handling DeleteObject request");
    
    // Parse bucket and object key from URI
    let (bucket_name, key) = utils::parse_s3_path(&uri)?;
    let key = key.ok_or(S3Error::InvalidRequest("Object key is required".to_string()))?;
    
    utils::validate_bucket_name(&bucket_name)?;
    utils::validate_object_key(&key)?;
    
    // Authenticate the request and extract credentials
    let (_access_key, _secret_key) = state.authenticator.authenticate_and_extract(
        &method, &uri, &headers, &[]
    )?;
    
    info!("DeleteObject request for bucket '{}', key '{}' authenticated successfully", bucket_name, key);
    
    // Check if object exists
    if !state.metadata_store.object_exists(&bucket_name, &key).await {
        return Err(S3Error::NoSuchKey);
    }
    
    // Remove from metadata store
    state.metadata_store.delete_object(&bucket_name, &key).await?;
    
    // Note: Walrus doesn't support deletion of blobs, so the blob remains in storage
    // but is no longer accessible via S3 API. This is the expected behavior for 
    // immutable storage systems.
    
    info!("DeleteObject for key '{}' completed (blob remains in Walrus)", key);
    
    Ok(Response::builder()
        .status(204)
        .body("".into())
        .unwrap())
}

/// Head an object (get metadata without body).
pub async fn head_object(
    State(state): State<S3State>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
) -> S3Result<Response> {
    debug!("Handling HeadObject request");
    
    // Authenticate the request
    state.authenticate(&method, &uri, &headers, &[])?;
    
    // Parse bucket and object key from URI
    let (bucket_name, key) = utils::parse_s3_path(&uri)?;
    let key = key.ok_or(S3Error::InvalidRequest("Object key is required".to_string()))?;
    
    utils::validate_bucket_name(&bucket_name)?;
    utils::validate_object_key(&key)?;
    
    info!("HeadObject request for bucket '{}', key '{}' authenticated successfully", bucket_name, key);
    
    // Get metadata from store
    let metadata = match state.metadata_store.get_object(&bucket_name, &key).await? {
        Some(metadata) => metadata,
        None => {
            return Err(S3Error::NoSuchKey);
        }
    };
    
    // Build response with object metadata
    let mut response_builder = Response::builder()
        .status(200)
        .header("Content-Length", metadata.size)
        .header("ETag", metadata.etag)
        .header("Last-Modified", metadata.last_modified.format("%a, %d %b %Y %H:%M:%S GMT").to_string());
    
    if let Some(content_type) = metadata.content_type {
        response_builder = response_builder.header("Content-Type", content_type);
    } else {
        response_builder = response_builder.header("Content-Type", "application/octet-stream");
    }
    
    // Add user metadata as x-amz-meta-* headers
    for (key, value) in metadata.user_metadata {
        response_builder = response_builder.header(format!("x-amz-meta-{}", key), value);
    }
    
    Ok(response_builder
        .body("".into())
        .unwrap())
}

/// Copy an object.
pub async fn copy_object(
    State(state): State<S3State>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
) -> S3Result<Response> {
    debug!("Handling CopyObject request");
    
    // Authenticate the request
    state.authenticate(&method, &uri, &headers, &[])?;
    
    // Parse destination bucket and object key from URI
    let (dest_bucket, dest_key) = utils::parse_s3_path(&uri)?;
    let dest_key = dest_key.ok_or(S3Error::InvalidRequest("Object key is required".to_string()))?;
    
    utils::validate_bucket_name(&dest_bucket)?;
    utils::validate_object_key(&dest_key)?;
    
    // Parse source from x-amz-copy-source header
    let copy_source = headers
        .get("x-amz-copy-source")
        .and_then(|v| v.to_str().ok())
        .ok_or(S3Error::InvalidRequest("x-amz-copy-source header is required".to_string()))?;
    
    // Parse source bucket and key
    let source_path = copy_source.trim_start_matches('/');
    let source_parts: Vec<&str> = source_path.splitn(2, '/').collect();
    if source_parts.len() != 2 {
        return Err(S3Error::InvalidRequest("Invalid copy source format".to_string()));
    }
    
    let source_bucket = source_parts[0];
    let source_key = source_parts[1];
    
    utils::validate_bucket_name(source_bucket)?;
    utils::validate_object_key(source_key)?;
    
    info!("CopyObject from '{}:{}' to '{}:{}' authenticated successfully", 
          source_bucket, source_key, dest_bucket, dest_key);
    
    // TODO: Implement object copying
    // For Walrus, this would involve:
    // 1. Getting the source blob ID from the source key
    // 2. Reading the blob data from Walrus
    // 3. Storing it again with the destination key
    // 4. Creating a new mapping for the destination key
    
    // For now, return an error as it's not implemented
    Err(S3Error::NotImplemented)
}

/// Initiate multipart upload.
pub async fn create_multipart_upload(
    State(state): State<S3State>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
) -> S3Result<Response> {
    debug!("Handling CreateMultipartUpload request");
    
    // Parse bucket and object key from URI
    let (bucket_name, key) = utils::parse_s3_path(&uri)?;
    let key = key.ok_or(S3Error::InvalidRequest("Object key is required".to_string()))?;
    
    utils::validate_bucket_name(&bucket_name)?;
    utils::validate_object_key(&key)?;
    
    // Authenticate the request and extract credentials
    let (_access_key, _secret_key) = state.authenticator.authenticate_and_extract(
        &method, &uri, &headers, &[]
    )?;
    
    info!("CreateMultipartUpload request for bucket '{}', key '{}' authenticated successfully", bucket_name, key);
    
    // Generate a unique upload ID
    let upload_id = uuid::Uuid::new_v4().to_string();
    
    // TODO: Store multipart upload metadata
    // This would typically include:
    // - Upload ID
    // - Bucket and key
    // - Metadata from headers
    // - User credentials for subsequent operations
    // - Timestamp
    
    let response_xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Bucket>{}</Bucket>
    <Key>{}</Key>
    <UploadId>{}</UploadId>
</InitiateMultipartUploadResult>"#,
        bucket_name, key, upload_id
    );
    
    Ok(Response::builder()
        .status(200)
        .header("Content-Type", "application/xml")
        .body(response_xml.into())
        .unwrap())
}

/// Upload a part for multipart upload.
pub async fn upload_part(
    State(state): State<S3State>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
    Query(params): Query<HashMap<String, String>>,
) -> S3Result<Response> {
    debug!("Handling UploadPart request");
    
    // Authenticate the request
    state.authenticate(&method, &uri, &headers, &body)?;
    
    // Parse bucket and object key from URI
    let (bucket_name, key) = utils::parse_s3_path(&uri)?;
    let key = key.ok_or(S3Error::InvalidRequest("Object key is required".to_string()))?;
    
    utils::validate_bucket_name(&bucket_name)?;
    utils::validate_object_key(&key)?;
    
    // Parse query parameters
    let upload_id = params.get("uploadId")
        .ok_or(S3Error::InvalidRequest("uploadId is required".to_string()))?;
    let part_number = params.get("partNumber")
        .ok_or(S3Error::InvalidRequest("partNumber is required".to_string()))?
        .parse::<u32>()
        .map_err(|_| S3Error::InvalidRequest("Invalid partNumber".to_string()))?;
    
    info!("UploadPart request for bucket '{}', key '{}', upload '{}', part {} authenticated successfully", 
          bucket_name, key, upload_id, part_number);
    
    // TODO: Implement part upload
    // For now, we'll generate a mock ETag
    let etag = format!("\"{}\"", uuid::Uuid::new_v4().to_string().replace('-', ""));
    
    Ok(Response::builder()
        .status(200)
        .header("ETag", etag)
        .body("".into())
        .unwrap())
}

/// Complete multipart upload.
pub async fn complete_multipart_upload(
    State(state): State<S3State>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
    Query(params): Query<HashMap<String, String>>,
) -> S3Result<Response> {
    debug!("Handling CompleteMultipartUpload request");
    
    // Authenticate the request
    state.authenticate(&method, &uri, &headers, &body)?;
    
    // Parse bucket and object key from URI
    let (bucket_name, key) = utils::parse_s3_path(&uri)?;
    let key = key.ok_or(S3Error::InvalidRequest("Object key is required".to_string()))?;
    
    utils::validate_bucket_name(&bucket_name)?;
    utils::validate_object_key(&key)?;
    
    // Parse query parameters
    let upload_id = params.get("uploadId")
        .ok_or(S3Error::InvalidRequest("uploadId is required".to_string()))?;
    
    info!("CompleteMultipartUpload request for bucket '{}', key '{}', upload '{}' authenticated successfully", 
          bucket_name, key, upload_id);
    
    // TODO: Implement multipart upload completion
    // This would involve:
    // 1. Parsing the XML body to get the list of parts
    // 2. Assembling the parts into a complete object
    // 3. Storing the complete object in Walrus
    // 4. Cleaning up the temporary parts
    
    // For now, return a mock response
    let etag = format!("\"{}\"", uuid::Uuid::new_v4().to_string().replace('-', ""));
    
    let response_xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Location>https://s3.amazonaws.com/{}/{}</Location>
    <Bucket>{}</Bucket>
    <Key>{}</Key>
    <ETag>{}</ETag>
</CompleteMultipartUploadResult>"#,
        bucket_name, key, bucket_name, key, etag
    );
    
    Ok(Response::builder()
        .status(200)
        .header("Content-Type", "application/xml")
        .body(response_xml.into())
        .unwrap())
}

/// Abort multipart upload.
pub async fn abort_multipart_upload(
    State(state): State<S3State>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> S3Result<Response> {
    debug!("Handling AbortMultipartUpload request");
    
    // Authenticate the request
    state.authenticate(&method, &uri, &headers, &[])?;
    
    // Parse bucket and object key from URI
    let (bucket_name, key) = utils::parse_s3_path(&uri)?;
    let key = key.ok_or(S3Error::InvalidRequest("Object key is required".to_string()))?;
    
    utils::validate_bucket_name(&bucket_name)?;
    utils::validate_object_key(&key)?;
    
    // Parse query parameters
    let upload_id = params.get("uploadId")
        .ok_or(S3Error::InvalidRequest("uploadId is required".to_string()))?;
    
    info!("AbortMultipartUpload request for bucket '{}', key '{}', upload '{}' authenticated successfully", 
          bucket_name, key, upload_id);
    
    // TODO: Implement multipart upload abort
    // This would involve cleaning up any temporary parts
    
    Ok(Response::builder()
        .status(204)
        .body("".into())
        .unwrap())
}

// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! S3 bucket operations handlers.

use crate::error::{S3Error, S3Result};
use crate::handlers::S3State;
use crate::s3_types::{ListBucketsResponse, S3Bucket};
use crate::utils;
use axum::extract::State;
use axum::http::{HeaderMap, Method, Uri};
use axum::response::Response;
use tracing::{debug, info};

/// List all buckets.
pub async fn list_buckets(
    State(state): State<S3State>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
) -> S3Result<Response> {
    debug!("Handling ListBuckets request");
    
    // Authenticate the request
    state.authenticate(&method, &uri, &headers, &[])?;
    
    info!("ListBuckets request authenticated successfully");
    
    // Get buckets from metadata store
    let buckets = state.metadata_store.list_buckets().await?;
    
    let mut response_buckets = Vec::new();
    for bucket_name in buckets {
        response_buckets.push(S3Bucket::new(bucket_name));
    }
    
    // If no buckets exist, include the default bucket
    if response_buckets.is_empty() {
        response_buckets.push(S3Bucket::new(state.default_bucket.clone()));
    }
    
    let response = ListBucketsResponse {
        buckets: response_buckets,
    };
    
    Ok(Response::builder()
        .status(200)
        .header("Content-Type", "application/xml")
        .body(response.to_xml().into())
        .unwrap())
}

/// Create a bucket.
pub async fn create_bucket(
    State(state): State<S3State>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
) -> S3Result<Response> {
    debug!("Handling CreateBucket request");
    
    // Authenticate the request
    state.authenticate(&method, &uri, &headers, &[])?;
    
    // Parse bucket name from URI
    let (bucket_name, _) = utils::parse_s3_path(&uri)?;
    utils::validate_bucket_name(&bucket_name)?;
    
    info!("CreateBucket request for bucket '{}' authenticated successfully", bucket_name);
    
    // Create bucket in metadata store
    match state.metadata_store.create_bucket(&bucket_name).await {
        Ok(_) => {
            info!("Created bucket '{}'", bucket_name);
            Ok(Response::builder()
                .status(200)
                .header("Location", format!("/{}", bucket_name))
                .body("".into())
                .unwrap())
        }
        Err(S3Error::BucketAlreadyExists) => {
            info!("Bucket '{}' already exists", bucket_name);
            Err(S3Error::BucketAlreadyExists)
        }
        Err(e) => Err(e),
    }
}

/// Delete a bucket.
pub async fn delete_bucket(
    State(state): State<S3State>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
) -> S3Result<Response> {
    debug!("Handling DeleteBucket request");
    
    // Authenticate the request
    state.authenticate(&method, &uri, &headers, &[])?;
    
    // Parse bucket name from URI
    let (bucket_name, _) = utils::parse_s3_path(&uri)?;
    utils::validate_bucket_name(&bucket_name)?;
    
    info!("DeleteBucket request for bucket '{}' authenticated successfully", bucket_name);
    
    // Delete bucket from metadata store
    match state.metadata_store.delete_bucket(&bucket_name).await {
        Ok(_) => {
            info!("Deleted bucket '{}'", bucket_name);
            Ok(Response::builder()
                .status(204)
                .body("".into())
                .unwrap())
        }
        Err(S3Error::BucketNotEmpty) => {
            info!("Cannot delete bucket '{}' - not empty", bucket_name);
            Err(S3Error::BucketNotEmpty)
        }
        Err(e) => Err(e),
    }
}

/// Check if a bucket exists (HEAD operation).
pub async fn head_bucket(
    State(state): State<S3State>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
) -> S3Result<Response> {
    debug!("Handling HeadBucket request");
    
    // Authenticate the request
    state.authenticate(&method, &uri, &headers, &[])?;
    
    // Parse bucket name from URI
    let (bucket_name, _) = utils::parse_s3_path(&uri)?;
    utils::validate_bucket_name(&bucket_name)?;
    
    info!("HeadBucket request for bucket '{}' authenticated successfully", bucket_name);
    
    // Check if bucket exists in metadata store
    if state.metadata_store.bucket_exists(&bucket_name).await {
        Ok(Response::builder()
            .status(200)
            .body("".into())
            .unwrap())
    } else {
        // Check if it's the default bucket (always exists conceptually)
        if bucket_name == state.default_bucket {
            Ok(Response::builder()
                .status(200)
                .body("".into())
                .unwrap())
        } else {
            Err(S3Error::NoSuchBucket)
        }
    }
}

/// Get bucket location.
pub async fn get_bucket_location(
    State(state): State<S3State>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
) -> S3Result<Response> {
    debug!("Handling GetBucketLocation request");
    
    // Authenticate the request
    state.authenticate(&method, &uri, &headers, &[])?;
    
    // Parse bucket name from URI
    let (bucket_name, _) = utils::parse_s3_path(&uri)?;
    utils::validate_bucket_name(&bucket_name)?;
    
    info!("GetBucketLocation request for bucket '{}' authenticated successfully", bucket_name);
    
    // Return the configured region
    let location_xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">us-east-1</LocationConstraint>"#
    );
    
    Ok(Response::builder()
        .status(200)
        .header("Content-Type", "application/xml")
        .body(location_xml.into())
        .unwrap())
}

/// Get bucket versioning.
pub async fn get_bucket_versioning(
    State(state): State<S3State>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
) -> S3Result<Response> {
    debug!("Handling GetBucketVersioning request");
    
    // Authenticate the request
    state.authenticate(&method, &uri, &headers, &[])?;
    
    // Parse bucket name from URI
    let (bucket_name, _) = utils::parse_s3_path(&uri)?;
    utils::validate_bucket_name(&bucket_name)?;
    
    info!("GetBucketVersioning request for bucket '{}' authenticated successfully", bucket_name);
    
    // Return versioning disabled (Walrus doesn't support versioning)
    let versioning_xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Status>Disabled</Status>
</VersioningConfiguration>"#
    );
    
    Ok(Response::builder()
        .status(200)
        .header("Content-Type", "application/xml")
        .body(versioning_xml.into())
        .unwrap())
}

/// Get bucket ACL.
pub async fn get_bucket_acl(
    State(state): State<S3State>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
) -> S3Result<Response> {
    debug!("Handling GetBucketAcl request");
    
    // Authenticate the request
    state.authenticate(&method, &uri, &headers, &[])?;
    
    // Parse bucket name from URI
    let (bucket_name, _) = utils::parse_s3_path(&uri)?;
    utils::validate_bucket_name(&bucket_name)?;
    
    info!("GetBucketAcl request for bucket '{}' authenticated successfully", bucket_name);
    
    // Return default ACL
    let acl_xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Owner>
        <ID>walrus-owner</ID>
        <DisplayName>Walrus</DisplayName>
    </Owner>
    <AccessControlList>
        <Grant>
            <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
                <ID>walrus-owner</ID>
                <DisplayName>Walrus</DisplayName>
            </Grantee>
            <Permission>FULL_CONTROL</Permission>
        </Grant>
    </AccessControlList>
</AccessControlPolicy>"#
    );
    
    Ok(Response::builder()
        .status(200)
        .header("Content-Type", "application/xml")
        .body(acl_xml.into())
        .unwrap())
}

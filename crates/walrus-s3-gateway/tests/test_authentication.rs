//! Authentication tests for Walrus S3 Gateway

use anyhow::Result;
use axum::{
    body::Body,
    http::{Method, Request, StatusCode},
};
use tower::ServiceExt;

mod common;
use common::*;

#[tokio::test]
async fn test_authentication_required() -> Result<()> {
    let app = create_test_app().await?;
    
    // Test unauthenticated request should fail
    let request = Request::builder()
        .method(Method::GET)
        .uri("/")
        .body(Body::empty())?;
    
    let response = app.clone().oneshot(request).await?;
    
    // Should return 401 or 403 for missing authentication
    assert!(
        response.status() == StatusCode::UNAUTHORIZED || 
        response.status() == StatusCode::FORBIDDEN,
        "Expected 401 or 403, got {}",
        response.status()
    );
    
    Ok(())
}

#[tokio::test]
async fn test_invalid_signature() -> Result<()> {
    let app = create_test_app().await?;
    
    // Create request with invalid signature
    let request = Request::builder()
        .method(Method::GET)
        .uri("/")
        .header("Authorization", "AWS4-HMAC-SHA256 Credential=test-access-key/20231201/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=invalid_signature")
        .header("Host", "localhost")
        .header("X-Amz-Date", "20231201T120000Z")
        .body(Body::empty())?;
    
    let response = app.clone().oneshot(request).await?;
    
    // Should return 403 for invalid signature
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    
    Ok(())
}

#[tokio::test]
async fn test_valid_authentication() -> Result<()> {
    let app = create_test_app().await?;
    let client = create_test_client();
    
    // Create properly signed request
    let (headers, body) = client.create_signed_request(
        &Method::GET,
        "/",
        b"",
        None,
    )?;
    
    let mut request_builder = Request::builder()
        .method(Method::GET)
        .uri("/");
    
    // Add all headers from the signed request
    for (name, value) in headers.iter() {
        request_builder = request_builder.header(name, value);
    }
    
    let request = request_builder.body(Body::from(body))?;
    let response = app.clone().oneshot(request).await?;
    
    // Should succeed or return a proper S3 response
    assert!(
        response.status().is_success() || 
        response.status() == StatusCode::NOT_FOUND,
        "Expected success or 404, got {}",
        response.status()
    );
    
    Ok(())
}

#[tokio::test]
async fn test_expired_timestamp() -> Result<()> {
    let app = create_test_app().await?;
    
    // Create request with expired timestamp (older than 15 minutes)
    let request = Request::builder()
        .method(Method::GET)
        .uri("/")
        .header("Authorization", "AWS4-HMAC-SHA256 Credential=test-access-key/20200101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=test_signature")
        .header("Host", "localhost")
        .header("X-Amz-Date", "20200101T120000Z")
        .body(Body::empty())?;
    
    let response = app.clone().oneshot(request).await?;
    
    // Should return 403 for expired timestamp
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    
    Ok(())
}

#[tokio::test]
async fn test_missing_required_headers() -> Result<()> {
    let app = create_test_app().await?;
    
    // Test missing X-Amz-Date header
    let request = Request::builder()
        .method(Method::GET)
        .uri("/")
        .header("Authorization", "AWS4-HMAC-SHA256 Credential=test-access-key/20231201/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=test_signature")
        .header("Host", "localhost")
        .body(Body::empty())?;
    
    let response = app.clone().oneshot(request).await?;
    
    // Should return 400 or 403 for missing required header
    assert!(
        response.status() == StatusCode::BAD_REQUEST || 
        response.status() == StatusCode::FORBIDDEN,
        "Expected 400 or 403, got {}",
        response.status()
    );
    
    Ok(())
}

#[tokio::test]
async fn test_malformed_authorization_header() -> Result<()> {
    let app = create_test_app().await?;
    
    // Test malformed authorization header
    let request = Request::builder()
        .method(Method::GET)
        .uri("/")
        .header("Authorization", "Invalid authorization header format")
        .header("Host", "localhost")
        .header("X-Amz-Date", "20231201T120000Z")
        .body(Body::empty())?;
    
    let response = app.clone().oneshot(request).await?;
    
    // Should return 400 or 403 for malformed header
    assert!(
        response.status() == StatusCode::BAD_REQUEST || 
        response.status() == StatusCode::FORBIDDEN,
        "Expected 400 or 403, got {}",
        response.status()
    );
    
    Ok(())
}

#[tokio::test]
async fn test_wrong_access_key() -> Result<()> {
    let app = create_test_app().await?;
    let client = AwsSigV4TestClient::new(
        "wrong-access-key".to_string(),
        "test-secret-key".to_string(),
        "us-east-1".to_string(),
    );
    
    // Create request with wrong access key
    let (headers, body) = client.create_signed_request(
        &Method::GET,
        "/",
        b"",
        None,
    )?;
    
    let mut request_builder = Request::builder()
        .method(Method::GET)
        .uri("/");
    
    for (name, value) in headers.iter() {
        request_builder = request_builder.header(name, value);
    }
    
    let request = request_builder.body(Body::from(body))?;
    let response = app.clone().oneshot(request).await?;
    
    // Should return 403 for wrong access key
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    
    Ok(())
}

#[tokio::test]
async fn test_different_http_methods() -> Result<()> {
    let app = create_test_app().await?;
    let client = create_test_client();
    
    // Test GET method
    let (headers, body) = client.create_signed_request(
        &Method::GET,
        "/test-bucket/test-object",
        b"",
        None,
    )?;
    
    let mut request_builder = Request::builder()
        .method(Method::GET)
        .uri("/test-bucket/test-object");
    
    for (name, value) in headers.iter() {
        request_builder = request_builder.header(name, value);
    }
    
    let request = request_builder.body(Body::from(body))?;
    let response = app.clone().oneshot(request).await?;
    
    // Should be properly authenticated (404 is OK, object doesn't exist)
    assert!(
        response.status().is_success() || 
        response.status() == StatusCode::NOT_FOUND,
        "GET request failed authentication"
    );
    
    // Test PUT method
    let test_content = TestData::sample_blob_content().as_bytes();
    let (headers, body) = client.create_signed_request(
        &Method::PUT,
        "/test-bucket/test-object",
        test_content,
        Some("text/plain"),
    )?;
    
    let mut request_builder = Request::builder()
        .method(Method::PUT)
        .uri("/test-bucket/test-object");
    
    for (name, value) in headers.iter() {
        request_builder = request_builder.header(name, value);
    }
    
    let request = request_builder.body(Body::from(body))?;
    let response = app.clone().oneshot(request).await?;
    
    // Should be properly authenticated
    assert!(
        response.status().is_success() || 
        response.status() == StatusCode::ACCEPTED ||
        response.status() == StatusCode::NOT_FOUND,
        "PUT request failed authentication, got {}",
        response.status()
    );
    
    Ok(())
}

#[tokio::test]
async fn test_authentication_with_query_parameters() -> Result<()> {
    let app = create_test_app().await?;
    let client = create_test_client();
    
    // Test request with query parameters
    let (headers, body) = client.create_signed_request(
        &Method::PUT,
        "/test-bucket/test-object?X-Walrus-Client-Signing=true",
        TestData::sample_blob_content().as_bytes(),
        Some("text/plain"),
    )?;
    
    let mut request_builder = Request::builder()
        .method(Method::PUT)
        .uri("/test-bucket/test-object?X-Walrus-Client-Signing=true");
    
    for (name, value) in headers.iter() {
        request_builder = request_builder.header(name, value);
    }
    
    let request = request_builder.body(Body::from(body))?;
    let response = app.clone().oneshot(request).await?;
    
    // Should be properly authenticated
    assert!(
        response.status().is_success() || 
        response.status() == StatusCode::ACCEPTED ||
        response.status() == StatusCode::NOT_FOUND,
        "Request with query parameters failed authentication, got {}",
        response.status()
    );
    
    Ok(())
}

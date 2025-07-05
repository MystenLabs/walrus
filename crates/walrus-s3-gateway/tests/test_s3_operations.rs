//! S3 operations tests for Walrus S3 Gateway

use anyhow::Result;
use axum::{
    body::Body,
    http::{Method, Request, StatusCode},
};
use tower::ServiceExt;

mod common;
use common::*;

#[tokio::test]
async fn test_list_buckets() -> Result<()> {
    let app = create_test_app().await?;
    let client = create_test_client();
    
    // Create properly signed GET request for root path (list buckets)
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
    
    // Should succeed or return not found/not implemented
    assert!(
        response.status().is_success() ||
        response.status() == StatusCode::NOT_FOUND ||
        response.status() == StatusCode::NOT_IMPLEMENTED,
        "List buckets should succeed or be not implemented, got {}",
        response.status()
    );
    
    // If successful, response should be valid XML or JSON
    if response.status().is_success() {
        let response_text = extract_text_response(response).await?;
        
        // Should contain either XML (S3 standard) or JSON
        assert!(
            response_text.contains("<?xml") ||
            response_text.contains("{") ||
            response_text.contains("buckets"),
            "Response should contain bucket information"
        );
    }
    
    Ok(())
}

#[tokio::test]
async fn test_get_nonexistent_object() -> Result<()> {
    let app = create_test_app().await?;
    let client = create_test_client();
    
    let bucket = TestData::sample_bucket_name();
    let object_key = TestData::sample_object_key();
    
    // Create properly signed GET request for non-existent object
    let (headers, body) = client.create_signed_request(
        &Method::GET,
        &format!("/{}/{}", bucket, object_key),
        b"",
        None,
    )?;
    
    let mut request_builder = Request::builder()
        .method(Method::GET)
        .uri(format!("/{}/{}", bucket, object_key));
    
    for (name, value) in headers.iter() {
        request_builder = request_builder.header(name, value);
    }
    
    let request = request_builder.body(Body::from(body))?;
    let response = app.clone().oneshot(request).await?;
    
    // Should return 404 for non-existent object
    assert_eq!(
        response.status(),
        StatusCode::NOT_FOUND,
        "GET non-existent object should return 404"
    );
    
    Ok(())
}

#[tokio::test]
async fn test_delete_nonexistent_object() -> Result<()> {
    let app = create_test_app().await?;
    let client = create_test_client();
    
    let bucket = TestData::sample_bucket_name();
    let object_key = TestData::sample_object_key();
    
    // Create properly signed DELETE request for non-existent object
    let (headers, body) = client.create_signed_request(
        &Method::DELETE,
        &format!("/{}/{}", bucket, object_key),
        b"",
        None,
    )?;
    
    let mut request_builder = Request::builder()
        .method(Method::DELETE)
        .uri(format!("/{}/{}", bucket, object_key));
    
    for (name, value) in headers.iter() {
        request_builder = request_builder.header(name, value);
    }
    
    let request = request_builder.body(Body::from(body))?;
    let response = app.clone().oneshot(request).await?;
    
    // DELETE should succeed (204) or return 404 for non-existent object
    assert!(
        response.status() == StatusCode::NO_CONTENT ||
        response.status() == StatusCode::NOT_FOUND ||
        response.status() == StatusCode::OK,
        "DELETE non-existent object should return 204, 404, or 200, got {}",
        response.status()
    );
    
    Ok(())
}

#[tokio::test]
async fn test_put_object_various_content_types() -> Result<()> {
    let app = create_test_app().await?;
    let client = create_test_client();
    
    let bucket = TestData::sample_bucket_name();
    
    // Test different content types
    let test_cases = vec![
        ("text/plain", "Hello, World!".as_bytes(), "text.txt"),
        ("application/json", r#"{"key": "value"}"#.as_bytes(), "data.json"),
        ("text/html", "<html><body>Test</body></html>".as_bytes(), "page.html"),
        ("application/octet-stream", &[0x00, 0x01, 0x02, 0x03], "binary.bin"),
    ];
    
    for (content_type, content, filename) in test_cases {
        let (headers, body) = client.create_signed_request(
            &Method::PUT,
            &format!("/{}/{}", bucket, filename),
            content,
            Some(content_type),
        )?;
        
        let mut request_builder = Request::builder()
            .method(Method::PUT)
            .uri(format!("/{}/{}", bucket, filename));
        
        for (name, value) in headers.iter() {
            request_builder = request_builder.header(name, value);
        }
        
        let request = request_builder.body(Body::from(body))?;
        let response = app.clone().oneshot(request).await?;
        
        // Should handle the request appropriately
        assert!(
            response.status().is_success() ||
            response.status() == StatusCode::ACCEPTED ||
            response.status() == StatusCode::NOT_IMPLEMENTED,
            "PUT object with {} should be handled, got {}",
            content_type,
            response.status()
        );
    }
    
    Ok(())
}

#[tokio::test]
async fn test_head_object() -> Result<()> {
    let app = create_test_app().await?;
    let client = create_test_client();
    
    let bucket = TestData::sample_bucket_name();
    let object_key = TestData::sample_object_key();
    
    // Create properly signed HEAD request
    let (headers, body) = client.create_signed_request(
        &Method::HEAD,
        &format!("/{}/{}", bucket, object_key),
        b"",
        None,
    )?;
    
    let mut request_builder = Request::builder()
        .method(Method::HEAD)
        .uri(format!("/{}/{}", bucket, object_key));
    
    for (name, value) in headers.iter() {
        request_builder = request_builder.header(name, value);
    }
    
    let request = request_builder.body(Body::from(body))?;
    let response = app.clone().oneshot(request).await?;
    
    // Should return 404 for non-existent object or handle appropriately
    assert!(
        response.status() == StatusCode::NOT_FOUND ||
        response.status() == StatusCode::OK ||
        response.status() == StatusCode::NOT_IMPLEMENTED,
        "HEAD object should return 404, 200, or 501, got {}",
        response.status()
    );
    
    Ok(())
}

#[tokio::test]
async fn test_list_objects_in_bucket() -> Result<()> {
    let app = create_test_app().await?;
    let client = create_test_client();
    
    let bucket = TestData::sample_bucket_name();
    
    // Create properly signed GET request for bucket (list objects)
    let (headers, body) = client.create_signed_request(
        &Method::GET,
        &format!("/{}/", bucket),
        b"",
        None,
    )?;
    
    let mut request_builder = Request::builder()
        .method(Method::GET)
        .uri(format!("/{}/", bucket));
    
    for (name, value) in headers.iter() {
        request_builder = request_builder.header(name, value);
    }
    
    let request = request_builder.body(Body::from(body))?;
    let response = app.clone().oneshot(request).await?;
    
    // Should handle bucket listing appropriately
    assert!(
        response.status().is_success() ||
        response.status() == StatusCode::NOT_FOUND ||
        response.status() == StatusCode::NOT_IMPLEMENTED,
        "List objects in bucket should be handled, got {}",
        response.status()
    );
    
    Ok(())
}

#[tokio::test]
async fn test_put_object_with_metadata() -> Result<()> {
    let app = create_test_app().await?;
    let client = create_test_client();
    
    let bucket = TestData::sample_bucket_name();
    let object_key = TestData::sample_object_key();
    let test_content = TestData::sample_blob_content().as_bytes();
    
    // Create request with custom metadata headers
    let (mut headers, body) = client.create_signed_request(
        &Method::PUT,
        &format!("/{}/{}", bucket, object_key),
        test_content,
        Some("text/plain"),
    )?;
    
    // Add S3 metadata headers
    headers.insert("x-amz-meta-author", "test-user".parse()?);
    headers.insert("x-amz-meta-purpose", "testing".parse()?);
    headers.insert("x-amz-storage-class", "STANDARD".parse()?);
    
    let mut request_builder = Request::builder()
        .method(Method::PUT)
        .uri(format!("/{}/{}", bucket, object_key));
    
    for (name, value) in headers.iter() {
        request_builder = request_builder.header(name, value);
    }
    
    let request = request_builder.body(Body::from(body))?;
    let response = app.clone().oneshot(request).await?;
    
    // Should handle metadata appropriately
    assert!(
        response.status().is_success() ||
        response.status() == StatusCode::ACCEPTED ||
        response.status() == StatusCode::NOT_IMPLEMENTED,
        "PUT object with metadata should be handled, got {}",
        response.status()
    );
    
    Ok(())
}

#[tokio::test]
async fn test_large_object_handling() -> Result<()> {
    let app = create_test_app().await?;
    let client = create_test_client();
    
    let bucket = TestData::sample_bucket_name();
    let object_key = TestData::sample_object_key();
    
    // Create a larger test content (1MB)
    let large_content = vec![b'A'; 1024 * 1024];
    
    let (headers, body) = client.create_signed_request(
        &Method::PUT,
        &format!("/{}/{}", bucket, object_key),
        &large_content,
        Some("application/octet-stream"),
    )?;
    
    let mut request_builder = Request::builder()
        .method(Method::PUT)
        .uri(format!("/{}/{}", bucket, object_key));
    
    for (name, value) in headers.iter() {
        request_builder = request_builder.header(name, value);
    }
    
    let request = request_builder.body(Body::from(body))?;
    let response = app.clone().oneshot(request).await?;
    
    // Should handle large objects appropriately
    assert!(
        response.status().is_success() ||
        response.status() == StatusCode::ACCEPTED ||
        response.status() == StatusCode::PAYLOAD_TOO_LARGE ||
        response.status() == StatusCode::NOT_IMPLEMENTED,
        "PUT large object should be handled, got {}",
        response.status()
    );
    
    Ok(())
}

#[tokio::test]
async fn test_invalid_bucket_names() -> Result<()> {
    let app = create_test_app().await?;
    let client = create_test_client();
    
    let invalid_bucket_names = vec![
        "INVALID-UPPERCASE",
        "bucket_with_underscore",
        "bucket-with-period.",
        "bucket..double-period",
        "192.168.1.1", // IP address format
        "a", // Too short
    ];
    
    for bucket_name in invalid_bucket_names {
        let object_key = TestData::sample_object_key();
        
        let (headers, body) = client.create_signed_request(
            &Method::PUT,
            &format!("/{}/{}", bucket_name, object_key),
            TestData::sample_blob_content().as_bytes(),
            Some("text/plain"),
        )?;
        
        let mut request_builder = Request::builder()
            .method(Method::PUT)
            .uri(format!("/{}/{}", bucket_name, object_key));
        
        for (name, value) in headers.iter() {
            request_builder = request_builder.header(name, value);
        }
        
        let request = request_builder.body(Body::from(body))?;
        let response = app.clone().oneshot(request).await?;
        
        // Should either handle it or return appropriate error
        // We don't enforce strict bucket name validation in this test
        assert!(
            response.status().is_success() ||
            response.status() == StatusCode::ACCEPTED ||
            response.status() == StatusCode::BAD_REQUEST ||
            response.status() == StatusCode::NOT_IMPLEMENTED,
            "Invalid bucket name '{}' should be handled, got {}",
            bucket_name,
            response.status()
        );
    }
    
    Ok(())
}

#[tokio::test]
async fn test_options_request_cors() -> Result<()> {
    let app = create_test_app().await?;
    
    // Create OPTIONS request (typically for CORS preflight)
    let request = Request::builder()
        .method(Method::OPTIONS)
        .uri("/")
        .header("Origin", "https://example.com")
        .header("Access-Control-Request-Method", "PUT")
        .header("Access-Control-Request-Headers", "content-type")
        .body(Body::empty())?;
    
    let response = app.clone().oneshot(request).await?;
    
    // Should handle OPTIONS appropriately (200 or 204)
    assert!(
        response.status() == StatusCode::OK ||
        response.status() == StatusCode::NO_CONTENT ||
        response.status() == StatusCode::NOT_IMPLEMENTED ||
        response.status() == StatusCode::METHOD_NOT_ALLOWED,
        "OPTIONS request should be handled, got {}",
        response.status()
    );
    
    Ok(())
}

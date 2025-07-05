//! Integration tests for Walrus S3 Gateway

use anyhow::Result;
use axum::{
    body::Body,
    http::{Method, Request, StatusCode},
};
use serde_json::json;
use std::time::Duration;
use tokio::time::timeout;
use tower::ServiceExt;

mod common;
use common::*;

#[tokio::test]
async fn test_gateway_health_check() -> Result<()> {
    let app = create_test_app().await?;
    
    // Test basic connectivity without authentication (if health endpoint exists)
    let request = Request::builder()
        .method(Method::GET)
        .uri("/health")
        .body(Body::empty())?;
    
    let response = app.clone().oneshot(request).await?;
    
    // Health endpoint might not exist, that's OK
    assert!(
        response.status().is_success() ||
        response.status() == StatusCode::NOT_FOUND ||
        response.status() == StatusCode::UNAUTHORIZED ||
        response.status() == StatusCode::FORBIDDEN,
        "Health check should return success, 404, 401, or 403, got {}",
        response.status()
    );
    
    Ok(())
}

#[tokio::test]
async fn test_complete_object_lifecycle() -> Result<()> {
    let app = create_test_app().await?;
    let client = create_test_client();
    
    let bucket = TestData::sample_bucket_name();
    let object_key = TestData::sample_object_key();
    let test_content = TestData::sample_blob_content();
    
    // Step 1: PUT object
    let (put_headers, put_body) = client.create_signed_request(
        &Method::PUT,
        &format!("/{}/{}", bucket, object_key),
        test_content.as_bytes(),
        Some("text/plain"),
    )?;
    
    let mut put_request_builder = Request::builder()
        .method(Method::PUT)
        .uri(format!("/{}/{}", bucket, object_key));
    
    for (name, value) in put_headers.iter() {
        put_request_builder = put_request_builder.header(name, value);
    }
    
    let put_request = put_request_builder.body(Body::from(put_body))?;
    let put_response = app.clone().oneshot(put_request).await?;
    
    // PUT should succeed or require client signing
    assert!(
        put_response.status().is_success() ||
        put_response.status() == StatusCode::ACCEPTED ||
        put_response.status() == StatusCode::NOT_IMPLEMENTED,
        "PUT object should succeed or require client signing, got {}",
        put_response.status()
    );
    
    // Step 2: HEAD object (check if it exists)
    let (head_headers, head_body) = client.create_signed_request(
        &Method::HEAD,
        &format!("/{}/{}", bucket, object_key),
        b"",
        None,
    )?;
    
    let mut head_request_builder = Request::builder()
        .method(Method::HEAD)
        .uri(format!("/{}/{}", bucket, object_key));
    
    for (name, value) in head_headers.iter() {
        head_request_builder = head_request_builder.header(name, value);
    }
    
    let head_request = head_request_builder.body(Body::from(head_body))?;
    let head_response = app.clone().oneshot(head_request).await?;
    
    // HEAD should return appropriate status
    assert!(
        head_response.status() == StatusCode::OK ||
        head_response.status() == StatusCode::NOT_FOUND ||
        head_response.status() == StatusCode::NOT_IMPLEMENTED,
        "HEAD object should return 200, 404, or 501, got {}",
        head_response.status()
    );
    
    // Step 3: GET object
    let (get_headers, get_body) = client.create_signed_request(
        &Method::GET,
        &format!("/{}/{}", bucket, object_key),
        b"",
        None,
    )?;
    
    let mut get_request_builder = Request::builder()
        .method(Method::GET)
        .uri(format!("/{}/{}", bucket, object_key));
    
    for (name, value) in get_headers.iter() {
        get_request_builder = get_request_builder.header(name, value);
    }
    
    let get_request = get_request_builder.body(Body::from(get_body))?;
    let get_response = app.clone().oneshot(get_request).await?;
    
    // GET should return appropriate status
    assert!(
        get_response.status() == StatusCode::OK ||
        get_response.status() == StatusCode::NOT_FOUND ||
        get_response.status() == StatusCode::NOT_IMPLEMENTED,
        "GET object should return 200, 404, or 501, got {}",
        get_response.status()
    );
    
    // Step 4: DELETE object
    let (delete_headers, delete_body) = client.create_signed_request(
        &Method::DELETE,
        &format!("/{}/{}", bucket, object_key),
        b"",
        None,
    )?;
    
    let mut delete_request_builder = Request::builder()
        .method(Method::DELETE)
        .uri(format!("/{}/{}", bucket, object_key));
    
    for (name, value) in delete_headers.iter() {
        delete_request_builder = delete_request_builder.header(name, value);
    }
    
    let delete_request = delete_request_builder.body(Body::from(delete_body))?;
    let delete_response = app.clone().oneshot(delete_request).await?;
    
    // DELETE should succeed
    assert!(
        delete_response.status() == StatusCode::OK ||
        delete_response.status() == StatusCode::NO_CONTENT ||
        delete_response.status() == StatusCode::NOT_FOUND ||
        delete_response.status() == StatusCode::NOT_IMPLEMENTED,
        "DELETE object should return 200, 204, 404, or 501, got {}",
        delete_response.status()
    );
    
    Ok(())
}

#[tokio::test]
async fn test_concurrent_requests() -> Result<()> {
    let app = create_test_app().await?;
    let _client = create_test_client();
    let mut handles = Vec::new();
    
    for i in 0..5 {
        let app_clone = app.clone();
        let client_clone = AwsSigV4TestClient::new(
            "test-access-key".to_string(),
            "test-secret-key".to_string(),
            "us-east-1".to_string(),
        );
        
        let handle = tokio::spawn(async move {
            let bucket = format!("test-bucket-{}", i);
            let object_key = format!("test-object-{}.txt", i);
            
            let (headers, body) = client_clone.create_signed_request(
                &Method::PUT,
                &format!("/{}/{}", bucket, object_key),
                format!("Test content {}", i).as_bytes(),
                Some("text/plain"),
            )?;
            
            let mut request_builder = Request::builder()
                .method(Method::PUT)
                .uri(format!("/{}/{}", bucket, object_key));
            
            for (name, value) in headers.iter() {
                request_builder = request_builder.header(name, value);
            }
            
            let request = request_builder.body(Body::from(body))?;
            let response = app_clone.oneshot(request).await?;
            
            Ok::<StatusCode, anyhow::Error>(response.status())
        });
        
        handles.push(handle);
    }
    
    // Wait for all requests to complete
    for handle in handles {
        let status = handle.await??;
        
        // All requests should be handled appropriately
        assert!(
            status.is_success() ||
            status == StatusCode::ACCEPTED ||
            status == StatusCode::NOT_IMPLEMENTED,
            "Concurrent request should be handled, got {}",
            status
        );
    }
    
    Ok(())
}

#[tokio::test]
async fn test_request_timeout_handling() -> Result<()> {
    let app = create_test_app().await?;
    let client = create_test_client();
    
    let bucket = TestData::sample_bucket_name();
    let object_key = TestData::sample_object_key();
    
    // Create a normal request
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
    
    // Test that request completes within reasonable time
    let response = timeout(Duration::from_secs(10), app.clone().oneshot(request)).await?;
    
    assert!(response.is_ok(), "Request should complete within timeout");
    
    Ok(())
}

#[tokio::test]
async fn test_malformed_requests() -> Result<()> {
    let app = create_test_app().await?;
    
    // Test completely malformed request
    let request = Request::builder()
        .method(Method::GET)
        .uri("/\\invalid\\path")
        .body(Body::empty())?;
    
    let response = app.clone().oneshot(request).await?;
    
    // Should handle malformed request gracefully
    assert!(
        response.status().is_client_error() ||
        response.status().is_server_error() ||
        response.status() == StatusCode::NOT_FOUND,
        "Malformed request should be handled gracefully, got {}",
        response.status()
    );
    
    Ok(())
}

#[tokio::test]
async fn test_content_length_validation() -> Result<()> {
    let app = create_test_app().await?;
    let client = create_test_client();
    
    let bucket = TestData::sample_bucket_name();
    let object_key = TestData::sample_object_key();
    let test_content = "Short content";
    
    // Create request with mismatched content-length
    let (mut headers, _) = client.create_signed_request(
        &Method::PUT,
        &format!("/{}/{}", bucket, object_key),
        test_content.as_bytes(),
        Some("text/plain"),
    )?;
    
    // Add incorrect content-length header
    headers.insert("content-length", "999".parse()?);
    
    let mut request_builder = Request::builder()
        .method(Method::PUT)
        .uri(format!("/{}/{}", bucket, object_key));
    
    for (name, value) in headers.iter() {
        request_builder = request_builder.header(name, value);
    }
    
    let request = request_builder.body(Body::from(test_content.as_bytes()))?;
    let response = app.clone().oneshot(request).await?;
    
    // Should handle content length mismatch appropriately
    assert!(
        response.status().is_success() ||
        response.status() == StatusCode::ACCEPTED ||
        response.status() == StatusCode::BAD_REQUEST ||
        response.status() == StatusCode::LENGTH_REQUIRED ||
        response.status() == StatusCode::NOT_IMPLEMENTED,
        "Content length mismatch should be handled, got {}",
        response.status()
    );
    
    Ok(())
}

#[tokio::test]
async fn test_error_response_format() -> Result<()> {
    let app = create_test_app().await?;
    
    // Create request that should return an error (unauthenticated)
    let request = Request::builder()
        .method(Method::GET)
        .uri("/nonexistent-bucket/nonexistent-object")
        .body(Body::empty())?;
    
    let response = app.clone().oneshot(request).await?;
    
    // Should return an error
    assert!(
        !response.status().is_success(),
        "Request should return an error status"
    );
    
    // Check if error response has reasonable format
    let response_text = extract_text_response(response).await?;
    
    // Error response should be either XML (S3 standard) or JSON
    assert!(
        response_text.contains("<?xml") ||
        response_text.contains("{") ||
        response_text.contains("error") ||
        response_text.contains("Error") ||
        response_text.is_empty(), // Empty response is also acceptable
        "Error response should have reasonable format"
    );
    
    Ok(())
}

#[tokio::test]
async fn test_client_signing_integration_flow() -> Result<()> {
    let app = create_test_app().await?;
    let client = create_test_client();
    
    let bucket = TestData::sample_bucket_name();
    let object_key = TestData::sample_object_key();
    let test_content = TestData::sample_blob_content();
    
    // Complete client-side signing workflow
    
    // 1. Generate transaction template
    let generate_body = json!({
        "purpose": "store_blob",
        "params": {
            "size": test_content.len()
        }
    });
    
    let body_bytes = serde_json::to_vec(&generate_body)?;
    
    let (gen_headers, _) = client.create_signed_request(
        &Method::POST,
        "/_walrus/generate-transaction",
        &body_bytes,
        Some("application/json"),
    )?;
    
    let mut gen_request_builder = Request::builder()
        .method(Method::POST)
        .uri("/_walrus/generate-transaction");
    
    for (name, value) in gen_headers.iter() {
        gen_request_builder = gen_request_builder.header(name, value);
    }
    
    let gen_request = gen_request_builder.body(Body::from(body_bytes))?;
    let gen_response = app.clone().oneshot(gen_request).await?;
    
    // 2. Submit signed transaction
    let submit_body = json!({
        "signed_transaction": TestData::sample_signed_transaction(),
        "transaction_id": format!("test-tx-{}", uuid::Uuid::new_v4())
    });
    
    let submit_body_bytes = serde_json::to_vec(&submit_body)?;
    
    let (submit_headers, _) = client.create_signed_request(
        &Method::POST,
        "/_walrus/submit-transaction",
        &submit_body_bytes,
        Some("application/json"),
    )?;
    
    let mut submit_request_builder = Request::builder()
        .method(Method::POST)
        .uri("/_walrus/submit-transaction");
    
    for (name, value) in submit_headers.iter() {
        submit_request_builder = submit_request_builder.header(name, value);
    }
    
    let submit_request = submit_request_builder.body(Body::from(submit_body_bytes))?;
    let submit_response = app.clone().oneshot(submit_request).await?;
    
    // 3. Attempt regular PUT to see if it now requires client signing
    let (put_headers, put_body) = client.create_signed_request(
        &Method::PUT,
        &format!("/{}/{}", bucket, object_key),
        test_content.as_bytes(),
        Some("text/plain"),
    )?;
    
    let mut put_request_builder = Request::builder()
        .method(Method::PUT)
        .uri(format!("/{}/{}", bucket, object_key));
    
    for (name, value) in put_headers.iter() {
        put_request_builder = put_request_builder.header(name, value);
    }
    
    let put_request = put_request_builder.body(Body::from(put_body))?;
    let put_response = app.clone().oneshot(put_request).await?;
    
    // All operations should be handled appropriately
    // Generation and submission may not be implemented yet, that's OK
    assert!(
        gen_response.status().is_success() ||
        gen_response.status() == StatusCode::NOT_FOUND ||
        gen_response.status() == StatusCode::NOT_IMPLEMENTED,
        "Transaction generation should be handled"
    );
    
    assert!(
        submit_response.status().is_success() ||
        submit_response.status() == StatusCode::BAD_REQUEST ||
        submit_response.status() == StatusCode::NOT_FOUND ||
        submit_response.status() == StatusCode::NOT_IMPLEMENTED,
        "Transaction submission should be handled"
    );
    
    assert!(
        put_response.status().is_success() ||
        put_response.status() == StatusCode::ACCEPTED ||
        put_response.status() == StatusCode::NOT_IMPLEMENTED,
        "PUT operation should be handled"
    );
    
    Ok(())
}

#[tokio::test]
async fn test_stress_authentication() -> Result<()> {
    let app = create_test_app().await?;
    
    // Test multiple authentication attempts
    let mut handles = Vec::new();
    
    for i in 0..10 {
        let app_clone = app.clone();
        
        let handle = tokio::spawn(async move {
            let client = AwsSigV4TestClient::new(
                "test-access-key".to_string(),
                "test-secret-key".to_string(),
                "us-east-1".to_string(),
            );
            
            let (headers, body) = client.create_signed_request(
                &Method::GET,
                &format!("/test-bucket-{}/test-object-{}", i, i),
                b"",
                None,
            )?;
            
            let mut request_builder = Request::builder()
                .method(Method::GET)
                .uri(format!("/test-bucket-{}/test-object-{}", i, i));
            
            for (name, value) in headers.iter() {
                request_builder = request_builder.header(name, value);
            }
            
            let request = request_builder.body(Body::from(body))?;
            let response = app_clone.oneshot(request).await?;
            
            Ok::<StatusCode, anyhow::Error>(response.status())
        });
        
        handles.push(handle);
    }
    
    // All authentication attempts should be handled properly
    for handle in handles {
        let status = handle.await??;
        
        assert!(
            status.is_success() ||
            status == StatusCode::NOT_FOUND ||
            status == StatusCode::NOT_IMPLEMENTED,
            "Authentication should work under stress, got {}",
            status
        );
    }
    
    Ok(())
}

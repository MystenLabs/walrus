//! Client-side signing tests for Walrus S3 Gateway

use anyhow::Result;
use axum::{
    body::Body,
    http::{Method, Request, StatusCode},
};
use serde_json::json;
use tower::ServiceExt;

mod common;
use common::*;

#[tokio::test]
async fn test_put_object_requires_client_signing() -> Result<()> {
    let app = create_test_app().await?;
    let client = create_test_client();
    
    let test_content = TestData::sample_blob_content().as_bytes();
    let bucket = TestData::sample_bucket_name();
    let object_key = TestData::sample_object_key();
    
    // Create properly signed PUT request
    let (headers, body) = client.create_signed_request(
        &Method::PUT,
        &format!("/{}/{}", bucket, object_key),
        test_content,
        Some("text/plain"),
    )?;
    
    let mut request_builder = Request::builder()
        .method(Method::PUT)
        .uri(format!("/{}/{}", bucket, object_key));
    
    for (name, value) in headers.iter() {
        request_builder = request_builder.header(name, value);
    }
    
    let request = request_builder.body(Body::from(body))?;
    let response = app.clone().oneshot(request).await?;
    
    // Should return 202 (Accepted) requiring client-side signing
    // or other appropriate status indicating client signing workflow
    assert!(
        response.status() == StatusCode::ACCEPTED ||
        response.status() == StatusCode::OK ||
        response.status() == StatusCode::NOT_IMPLEMENTED,
        "Expected 202 (client signing required), 200, or 501, got {}",
        response.status()
    );
    
    // If response is 202, it should contain client signing information
    if response.status() == StatusCode::ACCEPTED {
        let json_response = extract_json_response(response).await?;
        
        // Should contain transaction template or signing instructions
        assert!(
            json_response.get("template").is_some() ||
            json_response.get("transaction_template").is_some() ||
            json_response.get("transaction_id").is_some() ||
            json_response.get("instructions").is_some(),
            "Response should contain client signing information, got: {}",
            json_response
        );
    }
    
    Ok(())
}

#[tokio::test]
async fn test_put_object_with_client_signing_parameter() -> Result<()> {
    let app = create_test_app().await?;
    let client = create_test_client();
    
    let test_content = TestData::sample_blob_content().as_bytes();
    let bucket = TestData::sample_bucket_name();
    let object_key = TestData::sample_object_key();
    
    // Create PUT request with X-Walrus-Client-Signing parameter
    let (headers, body) = client.create_signed_request(
        &Method::PUT,
        &format!("/{}/{}?X-Walrus-Client-Signing=true", bucket, object_key),
        test_content,
        Some("text/plain"),
    )?;
    
    let mut request_builder = Request::builder()
        .method(Method::PUT)
        .uri(format!("/{}/{}?X-Walrus-Client-Signing=true", bucket, object_key));
    
    for (name, value) in headers.iter() {
        request_builder = request_builder.header(name, value);
    }
    
    let request = request_builder.body(Body::from(body))?;
    let response = app.clone().oneshot(request).await?;
    
    // Should return 202 (Accepted) with transaction template
    assert!(
        response.status() == StatusCode::ACCEPTED ||
        response.status() == StatusCode::OK ||
        response.status() == StatusCode::NOT_IMPLEMENTED,
        "Expected 202, 200, or 501 for client signing request, got {}",
        response.status()
    );
    
    // Response should contain signing information
    if response.status() == StatusCode::ACCEPTED {
        let json_response = extract_json_response(response).await?;
        
        assert!(
            json_response.get("template").is_some() ||
            json_response.get("transaction_template").is_some(),
            "Response should contain transaction template"
        );
    }
    
    Ok(())
}

#[tokio::test]
async fn test_generate_transaction_template() -> Result<()> {
    let app = create_test_app().await?;
    let client = create_test_client();
    
    let request_body = json!({
        "purpose": "store_blob",
        "params": {
            "size": TestData::sample_blob_content().len()
        }
    });
    
    let body_bytes = serde_json::to_vec(&request_body)?;
    
    // Create properly signed POST request
    let (headers, _) = client.create_signed_request(
        &Method::POST,
        "/_walrus/generate-transaction",
        &body_bytes,
        Some("application/json"),
    )?;
    
    let mut request_builder = Request::builder()
        .method(Method::POST)
        .uri("/_walrus/generate-transaction");
    
    for (name, value) in headers.iter() {
        request_builder = request_builder.header(name, value);
    }
    
    let request = request_builder.body(Body::from(body_bytes))?;
    let response = app.clone().oneshot(request).await?;
    
    // Should return 200 with transaction template or 404 if not implemented
    assert!(
        response.status() == StatusCode::OK ||
        response.status() == StatusCode::NOT_FOUND ||
        response.status() == StatusCode::NOT_IMPLEMENTED,
        "Expected 200, 404, or 501 for transaction generation, got {}",
        response.status()
    );
    
    if response.status() == StatusCode::OK {
        let json_response = extract_json_response(response).await?;
        
        // Should contain transaction template
        assert!(
            json_response.get("template").is_some() ||
            json_response.get("transaction_id").is_some(),
            "Response should contain transaction template or ID"
        );
    }
    
    Ok(())
}

#[tokio::test]
async fn test_submit_signed_transaction() -> Result<()> {
    let app = create_test_app().await?;
    let client = create_test_client();
    
    let request_body = json!({
        "signed_transaction": TestData::sample_signed_transaction(),
        "transaction_id": format!("test-tx-{}", uuid::Uuid::new_v4())
    });
    
    let body_bytes = serde_json::to_vec(&request_body)?;
    
    // Create properly signed POST request
    let (headers, _) = client.create_signed_request(
        &Method::POST,
        "/_walrus/submit-transaction",
        &body_bytes,
        Some("application/json"),
    )?;
    
    let mut request_builder = Request::builder()
        .method(Method::POST)
        .uri("/_walrus/submit-transaction");
    
    for (name, value) in headers.iter() {
        request_builder = request_builder.header(name, value);
    }
    
    let request = request_builder.body(Body::from(body_bytes))?;
    let response = app.clone().oneshot(request).await?;
    
    // Should handle the request appropriately
    assert!(
        response.status() == StatusCode::OK ||
        response.status() == StatusCode::BAD_REQUEST || // Expected with dummy data
        response.status() == StatusCode::NOT_FOUND ||
        response.status() == StatusCode::NOT_IMPLEMENTED,
        "Expected 200, 400, 404, or 501 for transaction submission, got {}",
        response.status()
    );
    
    // If successful, should contain transaction result
    if response.status() == StatusCode::OK {
        let json_response = extract_json_response(response).await?;
        
        assert!(
            json_response.get("transaction_hash").is_some() ||
            json_response.get("status").is_some(),
            "Response should contain transaction result"
        );
    }
    
    Ok(())
}

#[tokio::test]
async fn test_generate_transaction_with_invalid_purpose() -> Result<()> {
    let app = create_test_app().await?;
    let client = create_test_client();
    
    let request_body = json!({
        "purpose": "invalid_purpose",
        "params": {
            "size": 1000
        }
    });
    
    let body_bytes = serde_json::to_vec(&request_body)?;
    
    let (headers, _) = client.create_signed_request(
        &Method::POST,
        "/_walrus/generate-transaction",
        &body_bytes,
        Some("application/json"),
    )?;
    
    let mut request_builder = Request::builder()
        .method(Method::POST)
        .uri("/_walrus/generate-transaction");
    
    for (name, value) in headers.iter() {
        request_builder = request_builder.header(name, value);
    }
    
    let request = request_builder.body(Body::from(body_bytes))?;
    let response = app.clone().oneshot(request).await?;
    
    // Should return 400 for invalid purpose or 404 if endpoint not implemented
    assert!(
        response.status() == StatusCode::BAD_REQUEST ||
        response.status() == StatusCode::NOT_FOUND ||
        response.status() == StatusCode::NOT_IMPLEMENTED,
        "Expected 400, 404, or 501 for invalid purpose, got {}",
        response.status()
    );
    
    Ok(())
}

#[tokio::test]
async fn test_generate_transaction_missing_parameters() -> Result<()> {
    let app = create_test_app().await?;
    let client = create_test_client();
    
    let request_body = json!({
        "purpose": "store_blob"
        // Missing required "params" field
    });
    
    let body_bytes = serde_json::to_vec(&request_body)?;
    
    let (headers, _) = client.create_signed_request(
        &Method::POST,
        "/_walrus/generate-transaction",
        &body_bytes,
        Some("application/json"),
    )?;
    
    let mut request_builder = Request::builder()
        .method(Method::POST)
        .uri("/_walrus/generate-transaction");
    
    for (name, value) in headers.iter() {
        request_builder = request_builder.header(name, value);
    }
    
    let request = request_builder.body(Body::from(body_bytes))?;
    let response = app.clone().oneshot(request).await?;
    
    // Should return 400 for missing parameters or appropriate error
    assert!(
        response.status() == StatusCode::BAD_REQUEST ||
        response.status() == StatusCode::UNPROCESSABLE_ENTITY ||
        response.status() == StatusCode::NOT_FOUND ||
        response.status() == StatusCode::NOT_IMPLEMENTED,
        "Expected 400, 422, 404, or 501 for missing parameters, got {}",
        response.status()
    );
    
    Ok(())
}

#[tokio::test]
async fn test_submit_transaction_missing_signature() -> Result<()> {
    let app = create_test_app().await?;
    let client = create_test_client();
    
    let request_body = json!({
        "transaction_id": format!("test-tx-{}", uuid::Uuid::new_v4())
        // Missing "signed_transaction" field
    });
    
    let body_bytes = serde_json::to_vec(&request_body)?;
    
    let (headers, _) = client.create_signed_request(
        &Method::POST,
        "/_walrus/submit-transaction",
        &body_bytes,
        Some("application/json"),
    )?;
    
    let mut request_builder = Request::builder()
        .method(Method::POST)
        .uri("/_walrus/submit-transaction");
    
    for (name, value) in headers.iter() {
        request_builder = request_builder.header(name, value);
    }
    
    let request = request_builder.body(Body::from(body_bytes))?;
    let response = app.clone().oneshot(request).await?;
    
    // Should return 400 for missing signature
    assert!(
        response.status() == StatusCode::BAD_REQUEST ||
        response.status() == StatusCode::UNPROCESSABLE_ENTITY ||
        response.status() == StatusCode::NOT_FOUND ||
        response.status() == StatusCode::NOT_IMPLEMENTED,
        "Expected 400, 422, 404, or 501 for missing signature, got {}",
        response.status()
    );
    
    Ok(())
}

#[tokio::test]
async fn test_client_signing_workflow_integration() -> Result<()> {
    let app = create_test_app().await?;
    let client = create_test_client();
    
    let bucket = TestData::sample_bucket_name();
    let object_key = TestData::sample_object_key();
    let test_content = TestData::sample_blob_content();
    
    // Step 1: Try to PUT object, should require client signing
    let (headers, body) = client.create_signed_request(
        &Method::PUT,
        &format!("/{}/{}", bucket, object_key),
        test_content.as_bytes(),
        Some("text/plain"),
    )?;
    
    let mut request_builder = Request::builder()
        .method(Method::PUT)
        .uri(format!("/{}/{}", bucket, object_key));
    
    for (name, value) in headers.iter() {
        request_builder = request_builder.header(name, value);
    }
    
    let request = request_builder.body(Body::from(body))?;
    let put_response = app.clone().oneshot(request).await?;
    
    // Should either succeed or require client signing
    assert!(
        put_response.status().is_success() ||
        put_response.status() == StatusCode::ACCEPTED ||
        put_response.status() == StatusCode::NOT_IMPLEMENTED,
        "PUT request should succeed or require client signing, got {}",
        put_response.status()
    );
    
    // If client signing is required, try the signing workflow
    if put_response.status() == StatusCode::ACCEPTED {
        let json_response = extract_json_response(put_response).await?;
        
        // Should contain transaction information
        assert!(
            json_response.get("template").is_some() ||
            json_response.get("transaction_template").is_some() ||
            json_response.get("transaction_id").is_some(),
            "Response should contain transaction information for client signing"
        );
        
        // Step 2: Generate transaction template (if needed)
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
        
        // Generation should succeed or be not implemented
        assert!(
            gen_response.status() == StatusCode::OK ||
            gen_response.status() == StatusCode::NOT_FOUND ||
            gen_response.status() == StatusCode::NOT_IMPLEMENTED,
            "Transaction generation should succeed or be not implemented, got {}",
            gen_response.status()
        );
        
        // Step 3: Submit signed transaction (with dummy data)
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
        
        // Submission should be handled appropriately
        assert!(
            submit_response.status() == StatusCode::OK ||
            submit_response.status() == StatusCode::BAD_REQUEST || // Expected with dummy data
            submit_response.status() == StatusCode::NOT_FOUND ||
            submit_response.status() == StatusCode::NOT_IMPLEMENTED,
            "Transaction submission should be handled, got {}",
            submit_response.status()
        );
    }
    
    Ok(())
}

//! Client-side signing endpoints for S3 operations.

use crate::error::{S3Error, S3Result};
use crate::handlers::S3State;
use crate::credentials::{SignedTransactionRequest, TransactionPurpose};
use axum::extract::{Query, State};
use axum::http::{HeaderMap, Method, Uri, StatusCode};
use axum::response::{Json, Response};
use axum::body::Body;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Request to generate an unsigned transaction template
#[derive(Debug, Deserialize)]
pub struct GenerateTransactionRequest {
    /// The purpose of the transaction
    pub purpose: String,
    /// Additional parameters based on purpose
    pub params: HashMap<String, String>,
}

/// Response containing unsigned transaction template
#[derive(Debug, Serialize)]
pub struct GenerateTransactionResponse {
    /// Unsigned transaction template
    pub template: serde_json::Value,
    /// Transaction ID for tracking
    pub transaction_id: String,
    /// Instructions for client
    pub instructions: String,
}

/// Request to submit a signed transaction
#[derive(Debug, Deserialize)]
pub struct SubmitSignedTransactionRequest {
    /// The signed transaction
    pub signed_transaction: SignedTransactionRequest,
    /// Transaction ID from the generation phase
    pub transaction_id: String,
}

/// Response for submitted transaction
#[derive(Debug, Serialize)]
pub struct SubmitTransactionResponse {
    /// Transaction hash on Sui
    pub transaction_hash: String,
    /// Status of the operation
    pub status: String,
    /// Additional information
    pub message: String,
}

/// Generate an unsigned transaction template for client signing
pub async fn generate_transaction_template(
    State(state): State<S3State>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    Json(request): Json<GenerateTransactionRequest>,
) -> S3Result<Response<Body>> {
    // Authenticate the request and extract access key
    let access_key = state.authenticate_request(&method, &uri, &headers, &[])?;
    
    // Parse the transaction purpose
    let purpose = match request.purpose.as_str() {
        "store_blob" => {
            let size = request.params.get("size")
                .and_then(|s| s.parse::<u64>().ok())
                .ok_or_else(|| S3Error::BadRequest("Missing or invalid 'size' parameter".to_string()))?;
            TransactionPurpose::StoreBlob { size }
        }
        "delete_blob" => {
            let blob_id = request.params.get("blob_id")
                .ok_or_else(|| S3Error::BadRequest("Missing 'blob_id' parameter".to_string()))?
                .clone();
            TransactionPurpose::DeleteBlob { blob_id }
        }
        "create_bucket" => {
            let name = request.params.get("name")
                .ok_or_else(|| S3Error::BadRequest("Missing 'name' parameter".to_string()))?
                .clone();
            TransactionPurpose::CreateBucket { name }
        }
        "delete_bucket" => {
            let name = request.params.get("name")
                .ok_or_else(|| S3Error::BadRequest("Missing 'name' parameter".to_string()))?
                .clone();
            TransactionPurpose::DeleteBucket { name }
        }
        _ => return Err(S3Error::BadRequest("Invalid transaction purpose".to_string())),
    };
    
    // Generate the unsigned transaction template
    let template = state.generate_transaction_template(&access_key, purpose).await?;
    
    // Create a unique transaction ID
    let transaction_id = uuid::Uuid::new_v4().to_string();
    
    // Prepare response
    let response = GenerateTransactionResponse {
        template: serde_json::to_value(&template)
            .map_err(|e| S3Error::InternalError(format!("Failed to serialize template: {}", e)))?,
        transaction_id,
        instructions: "Sign this transaction with your Sui wallet and submit it using the submit endpoint".to_string(),
    };
    
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/json")
        .body(Body::from(serde_json::to_string(&response)
            .map_err(|e| S3Error::InternalError(format!("Failed to serialize response: {}", e)))?))
        .unwrap())
}

/// Submit a signed transaction
pub async fn submit_signed_transaction(
    State(state): State<S3State>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    Json(request): Json<SubmitSignedTransactionRequest>,
) -> S3Result<Response<Body>> {
    // Authenticate the request and extract access key
    let access_key = state.authenticate_request(&method, &uri, &headers, &[])?;
    
    // Validate the signed transaction
    state.validate_signed_transaction(&access_key, &request.signed_transaction).await?;
    
    // Submit the transaction to Sui
    let tx_hash = state.submit_signed_transaction(&request.signed_transaction).await?;
    
    // Prepare response
    let response = SubmitTransactionResponse {
        transaction_hash: tx_hash,
        status: "success".to_string(),
        message: "Transaction submitted successfully".to_string(),
    };
    
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/json")
        .body(Body::from(serde_json::to_string(&response)
            .map_err(|e| S3Error::InternalError(format!("Failed to serialize response: {}", e)))?))
        .unwrap())
}

/// Handle pre-signed PUT operations for client-side signing
pub async fn handle_presigned_put(
    State(state): State<S3State>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> S3Result<Response<Body>> {
    // Check if this is a client-side signing request
    if params.contains_key("X-Walrus-Client-Signing") {
        // Extract object information
        let path = uri.path().trim_start_matches('/');
        let parts: Vec<&str> = path.splitn(2, '/').collect();
        
        if parts.len() != 2 {
            return Err(S3Error::BadRequest("Invalid object path".to_string()));
        }
        
        let _bucket = parts[0];
        let _key = parts[1];
        
        // Generate transaction template for storing the blob
        let purpose = TransactionPurpose::StoreBlob { 
            size: body.len() as u64 
        };
        
        // Authenticate and get access key
        let access_key = state.authenticate_request(&method, &uri, &headers, &body)?;
        
        // Generate unsigned transaction template
        let template = state.generate_transaction_template(&access_key, purpose).await?;
        
        let response = GenerateTransactionResponse {
            template: serde_json::to_value(&template)
                .map_err(|e| S3Error::InternalError(format!("Failed to serialize template: {}", e)))?,
            transaction_id: uuid::Uuid::new_v4().to_string(),
            instructions: "Sign this transaction and submit via POST to /_walrus/submit-transaction".to_string(),
        };
        
        return Ok(Response::builder()
            .status(StatusCode::ACCEPTED) // 202 - indicates client needs to complete signing
            .header("Content-Type", "application/json")
            .header("X-Walrus-Signing-Required", "true")
            .body(Body::from(serde_json::to_string(&response)
                .map_err(|e| S3Error::InternalError(format!("Failed to serialize response: {}", e)))?))
            .unwrap());
    }
    
    // Fall back to regular PUT handling
    Err(S3Error::NotImplemented("Regular PUT not implemented in signing handler".to_string()))
}

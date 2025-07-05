// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Client-Side Signing credential management for secure Walrus S3 Gateway.

use crate::error::{S3Error, S3Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use sui_types::base_types::SuiAddress;
use sui_types::transaction::Transaction;

/// Client-side signing credential strategy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientSigningConfig {
    /// Whether to require transaction signatures for write operations
    pub require_signatures: bool,
    /// Allowed Sui addresses for operations (None = allow all)
    pub allowed_addresses: Option<Vec<SuiAddress>>,
    /// Maximum transaction size in bytes
    pub max_transaction_size: Option<u64>,
    /// Rate limiting per address (transactions per minute)
    pub rate_limit_per_address: Option<u32>,
    /// Whether to validate signature authenticity
    pub validate_signatures: bool,
}

impl Default for ClientSigningConfig {
    fn default() -> Self {
        Self {
            require_signatures: true,
            allowed_addresses: None,
            max_transaction_size: Some(1_000_000), // 1MB
            rate_limit_per_address: Some(100), // 100 tx/min
            validate_signatures: true,
        }
    }
}

/// Client credential information extracted from S3 requests
#[derive(Debug, Clone)]
pub struct ClientCredential {
    /// S3 access key identifier
    pub access_key: String,
    /// Sui address associated with this credential
    pub sui_address: SuiAddress,
    /// Permissions for this credential
    pub permissions: Vec<Permission>,
    /// Rate limiting state
    pub rate_limit_state: RateLimitState,
}

/// Permissions that can be granted to a credential
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Permission {
    Read,
    Write,
    Delete,
    ListBuckets,
    CreateBucket,
}

/// Rate limiting state for a credential
#[derive(Debug, Clone)]
pub struct RateLimitState {
    /// Number of requests in current window
    pub current_requests: u32,
    /// Window start time
    pub window_start: std::time::Instant,
    /// Requests per minute limit
    pub limit: u32,
}

impl RateLimitState {
    pub fn new(limit: u32) -> Self {
        Self {
            current_requests: 0,
            window_start: std::time::Instant::now(),
            limit,
        }
    }

    /// Check if the request is within rate limits
    pub fn check_rate_limit(&mut self) -> bool {
        let now = std::time::Instant::now();
        
        // Reset window if more than 1 minute has passed
        if now.duration_since(self.window_start).as_secs() >= 60 {
            self.current_requests = 0;
            self.window_start = now;
        }

        // Check if under limit
        if self.current_requests < self.limit {
            self.current_requests += 1;
            true
        } else {
            false
        }
    }
}

/// Signed transaction from client
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignedTransactionRequest {
    /// The signed transaction
    pub transaction: Transaction,
    /// Additional metadata
    pub metadata: TransactionMetadata,
}

/// Metadata for signed transactions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionMetadata {
    /// Client identifier
    pub client_id: String,
    /// Transaction purpose
    pub purpose: TransactionPurpose,
    /// Timestamp
    pub timestamp: u64,
}

/// Purpose of the transaction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TransactionPurpose {
    StoreBlob { size: u64 },
    DeleteBlob { blob_id: String },
    CreateBucket { name: String },
    DeleteBucket { name: String },
}

/// Client-side signing credential manager
pub struct ClientSigningManager {
    config: ClientSigningConfig,
    /// Mapping from S3 access key to client credentials
    credentials: HashMap<String, ClientCredential>,
    /// Rate limiting state per address
    rate_limits: HashMap<SuiAddress, RateLimitState>,
}

impl ClientSigningManager {
    pub fn new(config: ClientSigningConfig) -> Self {
        Self {
            config,
            credentials: HashMap::new(),
            rate_limits: HashMap::new(),
        }
    }

    /// Register a new client credential
    pub fn register_credential(
        &mut self,
        access_key: String,
        sui_address: SuiAddress,
        permissions: Vec<Permission>,
    ) -> S3Result<()> {
        // Check if address is allowed
        if let Some(ref allowed) = self.config.allowed_addresses {
            if !allowed.contains(&sui_address) {
                return Err(S3Error::AccessDenied("Address not allowed".to_string()));
            }
        }

        let credential = ClientCredential {
            access_key: access_key.clone(),
            sui_address,
            permissions,
            rate_limit_state: RateLimitState::new(
                self.config.rate_limit_per_address.unwrap_or(100)
            ),
        };

        self.credentials.insert(access_key, credential);
        Ok(())
    }

    /// Get credential by access key
    pub fn get_credential(&self, access_key: &str) -> Option<&ClientCredential> {
        self.credentials.get(access_key)
    }

    /// Validate a signed transaction request
    pub fn validate_signed_transaction(
        &mut self,
        access_key: &str,
        request: &SignedTransactionRequest,
    ) -> S3Result<()> {
        // Get credential
        let credential = self.get_credential(access_key)
            .ok_or_else(|| S3Error::AccessDenied("Invalid access key".to_string()))?;

        // Check rate limit
        let rate_limit = self.rate_limits
            .entry(credential.sui_address)
            .or_insert_with(|| RateLimitState::new(
                self.config.rate_limit_per_address.unwrap_or(100)
            ));

        if !rate_limit.check_rate_limit() {
            return Err(S3Error::ServiceUnavailable("Rate limit exceeded".to_string()));
        }

        // Check transaction size
        if let Some(max_size) = self.config.max_transaction_size {
            let tx_size = bcs::to_bytes(&request.transaction)
                .map_err(|_| S3Error::BadRequest("Invalid transaction format".to_string()))?
                .len() as u64;
            
            if tx_size > max_size {
                return Err(S3Error::BadRequest("Transaction too large".to_string()));
            }
        }

        // Validate signature if required
        if self.config.validate_signatures {
            self.validate_transaction_signature(&request.transaction, &credential.sui_address)?;
        }

        // Check permissions based on transaction purpose
        self.validate_permissions(&credential.permissions, &request.metadata.purpose)?;

        Ok(())
    }

    /// Validate transaction signature
    fn validate_transaction_signature(
        &self,
        transaction: &Transaction,
        expected_signer: &SuiAddress,
    ) -> S3Result<()> {
        // This would validate the transaction signature against the expected signer
        // For now, we'll implement a basic check
        // In a real implementation, you'd verify the cryptographic signature
        
        // Extract signer from transaction (simplified)
        let tx_signer = self.extract_transaction_signer(transaction)?;
        
        if tx_signer != *expected_signer {
            return Err(S3Error::AccessDenied("Invalid transaction signer".to_string()));
        }

        Ok(())
    }

    /// Extract the signer address from a transaction
    fn extract_transaction_signer(&self, transaction: &Transaction) -> S3Result<SuiAddress> {
        // This is a simplified implementation
        // In reality, you'd extract the signer from the transaction's signature
        // For demonstration purposes, we'll return a placeholder
        
        // Get the first signature from the transaction
        if let Some(signature) = transaction.tx_signatures().first() {
            // Extract the address from the signature
            // This would involve cryptographic verification
            // For now, we'll use a placeholder implementation
            return Ok(SuiAddress::default()); // Replace with actual signature verification
        }

        Err(S3Error::BadRequest("No signature found in transaction".to_string()))
    }

    /// Validate permissions for a transaction purpose
    fn validate_permissions(
        &self,
        permissions: &[Permission],
        purpose: &TransactionPurpose,
    ) -> S3Result<()> {
        let required_permission = match purpose {
            TransactionPurpose::StoreBlob { .. } => Permission::Write,
            TransactionPurpose::DeleteBlob { .. } => Permission::Delete,
            TransactionPurpose::CreateBucket { .. } => Permission::CreateBucket,
            TransactionPurpose::DeleteBucket { .. } => Permission::Delete,
        };

        if !permissions.contains(&required_permission) {
            return Err(S3Error::AccessDenied(
                format!("Missing permission: {:?}", required_permission)
            ));
        }

        Ok(())
    }

    /// Generate an unsigned transaction template for the client to sign
    pub fn generate_transaction_template(
        &self,
        purpose: TransactionPurpose,
        sui_address: SuiAddress,
    ) -> S3Result<UnsignedTransactionTemplate> {
        Ok(UnsignedTransactionTemplate {
            purpose,
            signer: sui_address,
            gas_budget: 10_000_000, // 0.01 SUI
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        })
    }
}

/// Unsigned transaction template for client signing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnsignedTransactionTemplate {
    pub purpose: TransactionPurpose,
    pub signer: SuiAddress,
    pub gas_budget: u64,
    pub timestamp: u64,
}

/// JWT claims for token-based authentication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserClaims {
    /// Subject (user ID)
    pub sub: String,
    /// User's Sui address
    pub sui_address: String,
    /// Expiration timestamp
    pub exp: u64,
    /// Issuer
    pub iss: String,
    /// Permissions
    pub permissions: Option<Vec<String>>,
}

/// Credential manager handles different authentication strategies
pub struct CredentialManager {
    strategy: CredentialStrategy,
}

impl CredentialManager {
    /// Create a new credential manager with the specified strategy
    pub fn new(strategy: CredentialStrategy) -> Self {
        Self { strategy }
    }

    /// Resolve S3 credentials to Walrus wallet configuration
    pub async fn resolve_credentials(
        &self,
        access_key: &str,
        secret_key: &str,
        authorization_header: Option<&str>,
    ) -> S3Result<ResolvedCredential> {
        match &self.strategy {
            CredentialStrategy::DirectMapping { mapping } => {
                self.resolve_direct_mapping(access_key, secret_key, mapping).await
            }
            CredentialStrategy::JwtBased { jwt_secret, issuer } => {
                self.resolve_jwt_based(authorization_header, jwt_secret, issuer).await
            }
            CredentialStrategy::ClientSigning { require_signatures } => {
                self.resolve_client_signing(access_key, *require_signatures).await
            }
            CredentialStrategy::DelegatedSigning { signing_service_url, api_key } => {
                self.resolve_delegated_signing(access_key, signing_service_url, api_key).await
            }
        }
    }

    async fn resolve_direct_mapping(
        &self,
        access_key: &str,
        _secret_key: &str,
        mapping: &HashMap<String, UserCredential>,
    ) -> S3Result<ResolvedCredential> {
        let user_cred = mapping.get(access_key)
            .ok_or_else(|| S3Error::InvalidAccessKeyId)?;

        let wallet_config = if let Some(wallet_config) = &user_cred.wallet_config {
            Some(wallet_config.clone())
        } else if let Some(keystore_env) = &user_cred.keystore_env {
            // Load from environment variable
            let keystore_path = std::env::var(keystore_env)
                .map_err(|_| S3Error::InternalError(format!("Environment variable {} not set", keystore_env)))?;
            
            Some(WalletConfig::OptionalPathWithOverride {
                path: Some(keystore_path.into()),
                active_env: Some("testnet".to_string()),
                active_address: None,
            })
        } else {
            None
        };

        Ok(ResolvedCredential {
            sui_address: user_cred.sui_address.clone(),
            wallet_config: wallet_config.clone(),
            signing_mode: if wallet_config.is_some() {
                SigningMode::ServerSide
            } else {
                SigningMode::ClientSide
            },
            permissions: user_cred.permissions.clone(),
        })
    }

    async fn resolve_jwt_based(
        &self,
        authorization_header: Option<&str>,
        _jwt_secret: &str,
        _issuer: &str,
    ) -> S3Result<ResolvedCredential> {
        let _auth_header = authorization_header
            .ok_or_else(|| S3Error::AccessDenied)?;

        // TODO: Implement JWT validation
        // For now, return an error indicating this is not implemented
        Err(S3Error::InternalError("JWT authentication not yet implemented".to_string()))
    }

    async fn resolve_client_signing(
        &self,
        access_key: &str,
        _require_signatures: bool,
    ) -> S3Result<ResolvedCredential> {
        // For client signing, we don't need wallet config on server
        // The client will sign transactions locally
        Ok(ResolvedCredential {
            sui_address: format!("client-managed-{}", access_key),
            wallet_config: None,
            signing_mode: SigningMode::ClientSide,
            permissions: vec!["read".to_string(), "write".to_string()],
        })
    }

    async fn resolve_delegated_signing(
        &self,
        _access_key: &str,
        _signing_service_url: &str,
        _api_key: &str,
    ) -> S3Result<ResolvedCredential> {
        // TODO: Implement delegated signing service integration
        Err(S3Error::InternalError("Delegated signing not yet implemented".to_string()))
    }
}

/// Resolved credential information
#[derive(Debug, Clone)]
pub struct ResolvedCredential {
    /// User's Sui address
    pub sui_address: String,
    /// Wallet configuration (if available)
    pub wallet_config: Option<WalletConfig>,
    /// How transactions should be signed
    pub signing_mode: SigningMode,
    /// User permissions
    pub permissions: Vec<String>,
}

/// How transactions are signed
#[derive(Debug, Clone)]
pub enum SigningMode {
    /// Server signs transactions using wallet config
    ServerSide,
    /// Client signs transactions, server only validates
    ClientSide,
    /// External service signs transactions
    Delegated,
}

impl ResolvedCredential {
    /// Check if user has the specified permission
    pub fn has_permission(&self, permission: &str) -> bool {
        self.permissions.contains(&permission.to_string())
    }

    /// Check if this credential supports server-side signing
    pub fn supports_server_signing(&self) -> bool {
        matches!(self.signing_mode, SigningMode::ServerSide) && self.wallet_config.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_direct_mapping() {
        let mut mapping = HashMap::new();
        mapping.insert("alice".to_string(), UserCredential {
            sui_address: "0x123".to_string(),
            wallet_config: None,
            keystore_env: Some("ALICE_KEYSTORE".to_string()),
            permissions: vec!["read".to_string(), "write".to_string()],
        });

        let strategy = CredentialStrategy::DirectMapping { mapping };
        let manager = CredentialManager::new(strategy);

        // This would fail in practice because ALICE_KEYSTORE env var is not set
        // But it demonstrates the structure
        assert!(manager.resolve_credentials("alice", "secret", None).await.is_err());
    }

    #[tokio::test]
    async fn test_client_signing() {
        let strategy = CredentialStrategy::ClientSigning {
            require_signatures: true,
        };
        let manager = CredentialManager::new(strategy);

        let result = manager.resolve_credentials("bob", "secret", None).await.unwrap();
        assert_eq!(result.sui_address, "client-managed-bob");
        assert!(matches!(result.signing_mode, SigningMode::ClientSide));
        assert!(!result.supports_server_signing());
    }
}

// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Advanced credential management strategies for production deployments.

use crate::error::{S3Error, S3Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use walrus_sui::config::WalletConfig;

/// Strategy for credential mapping and authentication.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CredentialStrategy {
    /// Direct keystore mapping (development only)
    DirectMapping {
        mapping: HashMap<String, UserCredential>,
    },
    /// External JWT-based authentication
    JwtBased {
        jwt_secret: String,
        issuer: String,
    },
    /// Client-side signing with unsigned transaction flow
    ClientSigning {
        /// Whether to require client signatures for all operations
        require_signatures: bool,
    },
    /// Delegated signing service
    DelegatedSigning {
        signing_service_url: String,
        api_key: String,
    },
}

/// User credential configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserCredential {
    /// User's Sui address
    pub sui_address: String,
    /// Wallet configuration (optional for client signing)
    pub wallet_config: Option<WalletConfig>,
    /// Environment variable for keystore (more secure than direct path)
    pub keystore_env: Option<String>,
    /// Permissions/capabilities
    pub permissions: Vec<String>,
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

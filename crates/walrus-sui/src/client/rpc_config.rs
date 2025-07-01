// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Configuration for the RPC client.

use std::{collections::HashMap, time::Duration};

use serde::{Deserialize, Serialize};
use walrus_utils::backoff::ExponentialBackoffConfig;

/// Authentication configuration for a single RPC endpoint
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct RpcEndpointAuth {
    /// Basic auth username
    pub username: Option<String>,
    /// Basic auth password
    pub password: Option<String>,
    /// Bearer token
    pub bearer_token: Option<String>,
}

impl RpcEndpointAuth {
    /// Check if any authentication is configured
    pub fn has_auth(&self) -> bool {
        self.username.is_some() || self.bearer_token.is_some()
    }

    /// Get authentication type for logging
    pub fn auth_type(&self) -> &'static str {
        if self.bearer_token.is_some() {
            "Bearer Token"
        } else if self.username.is_some() {
            "Basic Auth"
        } else {
            "None"
        }
    }
}

/// Authentication configuration for RPC clients supporting multiple endpoints
#[derive(Debug, Clone, Default)]
pub struct RpcAuthConfig {
    /// Per-endpoint authentication configuration
    /// Key: RPC URL, Value: Authentication config for that endpoint
    endpoints: HashMap<String, RpcEndpointAuth>,
}

impl RpcAuthConfig {
    /// Load authentication configuration from environment variables
    /// Supports per-endpoint configuration using URL encoding
    ///
    /// Format: WALRUS_RPC_AUTH_{URL_ENCODED}_{FIELD}
    /// where URL_ENCODED is the URL with percent encoding (URL encoding)
    ///
    /// Examples:
    /// - WALRUS_RPC_AUTH_https%3A%2F%2Frpc1.sui.io_USERNAME=user1
    /// - WALRUS_RPC_AUTH_https%3A%2F%2Frpc1.sui.io_PASSWORD=pass1
    /// - WALRUS_RPC_AUTH_https%3A%2F%2Frpc2.sui.io_BEARER_TOKEN=token123
    /// - WALRUS_RPC_AUTH_https%3A%2F%2Frpc.sui.io%3A443%2Fapi_BEARER_TOKEN=token456
    ///
    pub fn from_environment() -> Self {
        let mut config = Self::default();

        for (key, value) in std::env::vars() {
            if let Some(rest) = key.strip_prefix("WALRUS_RPC_AUTH_") {
                if let Some((url_encoded, auth_field)) = rest.rsplit_once('_') {
                    if let Ok(decoded_url) = urlencoding::decode(url_encoded) {
                        let url = decoded_url.into_owned();
                        let endpoint_auth = config.endpoints.entry(url).or_default();

                        match auth_field {
                            "USERNAME" => endpoint_auth.username = Some(value),
                            "PASSWORD" => endpoint_auth.password = Some(value),
                            "BEARER_TOKEN" => endpoint_auth.bearer_token = Some(value),
                            _ => {}
                        }
                    } else {
                        tracing::warn!(
                            "Failed to URL-decode key in environment variable: \
                            {} (encoded part: {})",
                            key,
                            url_encoded
                        );
                    }
                }
            }
        }

        config
    }

    /// Get authentication config for a specific RPC URL
    pub fn get_auth_for_url(&self, url: &str) -> Option<&RpcEndpointAuth> {
        self.endpoints.get(url).filter(|auth| auth.has_auth())
    }

    /// Check if any authentication is configured
    pub fn has_any_auth(&self) -> bool {
        self.endpoints.values().any(|auth| auth.has_auth())
    }

    /// Get all configured endpoint URLs
    pub fn configured_endpoints(&self) -> Vec<&String> {
        self.endpoints.keys().collect()
    }
}

/// Configuration for the RPC endpoint fallback.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RpcFallbackConfig {
    /// The endpoint of the checkpoint bucket that will be
    /// used to download the checkpoint if the RPC endpoint
    /// is not available.
    pub checkpoint_bucket: reqwest::Url,
    /// The configuration for the backoff strategy for the RPC
    /// endpoint before falling back to the checkpoint bucket.
    #[serde(default = "RpcFallbackConfig::default_quick_retry_config")]
    pub quick_retry_config: ExponentialBackoffConfig,

    /// The minimum number of failures to start the fallback.
    #[serde(default = "RpcFallbackConfig::default_min_failures_to_start_fallback")]
    pub min_failures_to_start_fallback: usize,

    /// The duration of the failure window to start the fallback.
    #[serde(default = "RpcFallbackConfig::default_failure_window_to_start_fallback_duration")]
    pub failure_window_to_start_fallback_duration: Duration,

    /// The duration to skip the RPC for checkpoint download if fallback is configured.
    #[serde(default = "RpcFallbackConfig::default_skip_rpc_for_checkpoint_duration")]
    pub skip_rpc_for_checkpoint_duration: Duration,

    /// The maximum number of consecutive failures to tolerate for a single checkpoint
    /// before falling back to the checkpoint bucket.
    #[serde(default = "RpcFallbackConfig::default_max_consecutive_failures")]
    pub max_consecutive_failures: usize,
}

impl RpcFallbackConfig {
    fn default_quick_retry_config() -> ExponentialBackoffConfig {
        ExponentialBackoffConfig {
            min_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_millis(300),
            max_retries: None,
        }
    }

    fn default_min_failures_to_start_fallback() -> usize {
        10
    }

    fn default_failure_window_to_start_fallback_duration() -> Duration {
        Duration::from_secs(300)
    }

    fn default_skip_rpc_for_checkpoint_duration() -> Duration {
        Duration::from_secs(300)
    }

    fn default_max_consecutive_failures() -> usize {
        10
    }
}

/// Command line arguments for the RPC fallback configuration.
#[derive(Debug, Clone, clap::Args)]
pub struct RpcFallbackConfigArgs {
    /// The fallback checkpoint bucket URL.
    #[arg(long)]
    pub checkpoint_bucket: Option<reqwest::Url>,

    /// The minimum backoff interval in milliseconds
    /// to retry the fullnode RPC before falling back
    /// to the checkpoint bucket when available.
    #[arg(long)]
    pub min_backoff: Option<u64>,

    /// The maximum backoff interval in milliseconds
    /// to retry the fullnode RPC before falling back
    /// to the checkpoint bucket when available.
    #[arg(long)]
    pub max_backoff: Option<u64>,

    /// The maximum number of retries
    /// to retry the fullnode RPC before falling back
    /// to the checkpoint bucket when available.
    #[arg(long)]
    pub max_retries: Option<u32>,
}

impl RpcFallbackConfigArgs {
    /// Converts the command line arguments to a [`RpcFallbackConfig`].
    pub fn to_config(&self) -> Option<RpcFallbackConfig> {
        self.checkpoint_bucket.as_ref().map(|url| {
            let backoff = ExponentialBackoffConfig {
                min_backoff: self
                    .min_backoff
                    .map(Duration::from_millis)
                    .unwrap_or(Duration::from_millis(100)),
                max_backoff: self
                    .max_backoff
                    .map(Duration::from_millis)
                    .unwrap_or(Duration::from_millis(300)),
                max_retries: self.max_retries,
            };
            RpcFallbackConfig {
                checkpoint_bucket: url.clone(),
                quick_retry_config: backoff,
                min_failures_to_start_fallback:
                    RpcFallbackConfig::default_min_failures_to_start_fallback(),
                failure_window_to_start_fallback_duration:
                    RpcFallbackConfig::default_failure_window_to_start_fallback_duration(),
                skip_rpc_for_checkpoint_duration:
                    RpcFallbackConfig::default_skip_rpc_for_checkpoint_duration(),
                max_consecutive_failures: RpcFallbackConfig::default_max_consecutive_failures(),
            }
        })
    }
}

/// Encode URL for use as environment variable key using URL encoding (percent encoding)
/// This is deterministic, reversible, and follows web standards
///
/// Example:
/// ```
/// use walrus_sui::client::rpc_config::encode_url_for_env_key;
/// assert_eq!(encode_url_for_env_key("https://rpc1.sui.io"), "https%3A%2F%2Frpc1.sui.io");
/// ```
#[cfg(test)]
pub fn encode_url_for_env_key(url: &str) -> String {
    urlencoding::encode(url).into_owned()
}

#[cfg(test)]
mod tests {
    use std::env;

    use super::*;

    /// Helper to set environment variables for testing and ensure cleanup
    struct TestEnv {
        vars_to_cleanup: Vec<String>,
    }

    impl TestEnv {
        fn new() -> Self {
            Self {
                vars_to_cleanup: Vec::new(),
            }
        }

        fn set_var(&mut self, key: &str, value: &str) {
            unsafe {
                env::set_var(key, value);
            }
            self.vars_to_cleanup.push(key.to_string());
        }
    }

    impl Drop for TestEnv {
        fn drop(&mut self) {
            for var in &self.vars_to_cleanup {
                unsafe {
                    env::remove_var(var);
                }
            }
        }
    }

    #[test]
    fn test_rpc_auth_config_from_environment_basic_auth() {
        let mut test_env = TestEnv::new();

        // Set up environment variables for basic auth
        test_env.set_var(
            "WALRUS_RPC_AUTH_https%3A%2F%2Frpc1.sui.io_USERNAME",
            "testuser",
        );
        test_env.set_var(
            "WALRUS_RPC_AUTH_https%3A%2F%2Frpc1.sui.io_PASSWORD",
            "testpass",
        );

        let config = RpcAuthConfig::from_environment();

        assert!(config.has_any_auth());
        assert_eq!(config.configured_endpoints().len(), 1);

        let auth = config.get_auth_for_url("https://rpc1.sui.io").unwrap();
        assert_eq!(auth.username, Some("testuser".to_string()));
        assert_eq!(auth.password, Some("testpass".to_string()));
        assert!(auth.bearer_token.is_none());
        assert_eq!(auth.auth_type(), "Basic Auth");
    }

    #[test]
    fn test_rpc_auth_config_from_environment_bearer_token() {
        let mut test_env = TestEnv::new();

        // Set up environment variables for bearer token
        test_env.set_var(
            "WALRUS_RPC_AUTH_https%3A%2F%2Fapi.sui.io_BEARER_TOKEN",
            "test-token-123",
        );

        let config = RpcAuthConfig::from_environment();

        assert!(config.has_any_auth());

        let auth = config.get_auth_for_url("https://api.sui.io").unwrap();
        assert!(auth.username.is_none());
        assert!(auth.password.is_none());
        assert_eq!(auth.bearer_token, Some("test-token-123".to_string()));
        assert_eq!(auth.auth_type(), "Bearer Token");
    }

    #[test]
    fn test_rpc_auth_config_multiple_endpoints() {
        let mut test_env = TestEnv::new();

        // Set up multiple endpoints with different auth types
        test_env.set_var(
            "WALRUS_RPC_AUTH_https%3A%2F%2Frpc1.sui.io_USERNAME",
            "user1",
        );
        test_env.set_var(
            "WALRUS_RPC_AUTH_https%3A%2F%2Frpc1.sui.io_PASSWORD",
            "pass1",
        );
        test_env.set_var(
            "WALRUS_RPC_AUTH_https%3A%2F%2Fapi.sui.io_BEARER_TOKEN",
            "token123",
        );
        test_env.set_var(
            "WALRUS_RPC_AUTH_http%3A%2F%2Flocalhost%3A8080_USERNAME",
            "localuser",
        );

        let config = RpcAuthConfig::from_environment();

        assert!(config.has_any_auth());
        assert_eq!(config.configured_endpoints().len(), 3);

        // Test first endpoint (basic auth)
        let auth1 = config.get_auth_for_url("https://rpc1.sui.io").unwrap();
        assert_eq!(auth1.username, Some("user1".to_string()));
        assert_eq!(auth1.password, Some("pass1".to_string()));

        // Test second endpoint (bearer token)
        let auth2 = config.get_auth_for_url("https://api.sui.io").unwrap();
        assert_eq!(auth2.bearer_token, Some("token123".to_string()));

        // Test third endpoint (basic auth with username only)
        let auth3 = config.get_auth_for_url("http://localhost:8080").unwrap();
        assert_eq!(auth3.username, Some("localuser".to_string()));
        assert!(auth3.password.is_none());

        // Test non-existent endpoint
        assert!(config.get_auth_for_url("https://nonexistent.com").is_none());
    }
}

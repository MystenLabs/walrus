// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Configuration for the RPC client.

use std::{collections::HashMap, time::Duration};

use serde::{Deserialize, Serialize};
use walrus_utils::backoff::ExponentialBackoffConfig;

/// Authentication configuration for a single RPC endpoint
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum RpcEndpointAuth {
    /// Basic authentication with username and password
    BasicAuth {
        /// The username for basic authentication
        username: String,
        /// The password for basic authentication
        password: String,
    },
    /// Bearer token authentication
    BearerToken(String),
    /// No authentication
    #[default]
    None,
}
impl RpcEndpointAuth {
    /// Check if any authentication is configured
    pub fn has_auth(&self) -> bool {
        matches!(self, Self::BasicAuth { .. } | Self::BearerToken(_))
    }

    /// Get authentication type for logging
    #[cfg(test)]
    pub fn auth_type(&self) -> &'static str {
        if let Self::BearerToken(_) = self {
            "Bearer Token"
        } else if let Self::BasicAuth { .. } = self {
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
            let Some(rest) = key.strip_prefix("WALRUS_RPC_AUTH_") else {
                continue;
            };
            const SUFFIXES: [&str; 3] = ["_BEARER_TOKEN", "_PASSWORD", "_USERNAME"];
            let value = value.clone();
            for suffix in SUFFIXES {
                let Some(url_encoded) = rest.strip_suffix(suffix) else {
                    continue;
                };
                let Ok(decoded_url) = urlencoding::decode(url_encoded) else {
                    continue;
                };
                let url = decoded_url.into_owned();
                let endpoint_auth = config.endpoints.entry(url).or_default();
                match suffix {
                    "_USERNAME" => match endpoint_auth {
                        RpcEndpointAuth::BasicAuth { username, .. } => *username = value.clone(),
                        _ => {
                            *endpoint_auth = RpcEndpointAuth::BasicAuth {
                                username: value.clone(),
                                password: String::new(),
                            };
                        }
                    },
                    "_PASSWORD" => match endpoint_auth {
                        RpcEndpointAuth::BasicAuth { password, .. } => *password = value.clone(),
                        _ => {
                            *endpoint_auth = RpcEndpointAuth::BasicAuth {
                                username: String::new(),
                                password: value.clone(),
                            };
                        }
                    },
                    "_BEARER_TOKEN" => *endpoint_auth = RpcEndpointAuth::BearerToken(value.clone()),
                    _ => unreachable!(),
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

#[cfg(test)]
mod tests {
    use std::{
        env,
        sync::{Mutex, MutexGuard, OnceLock},
    };

    use super::*;

    // Prevent tests running simultaneously to avoid interferences with the environment variables.
    fn test_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(Mutex::default)
    }

    /// Helper to set environment variables for testing and ensure cleanup
    struct TestEnv {
        vars_to_cleanup: Vec<String>,
        _lock: MutexGuard<'static, ()>,
    }

    impl TestEnv {
        fn new() -> Self {
            Self {
                vars_to_cleanup: Vec::new(),
                _lock: test_lock().lock().unwrap(),
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
        assert_eq!(
            *auth,
            RpcEndpointAuth::BasicAuth {
                username: "testuser".to_string(),
                password: "testpass".to_string(),
            }
        );
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
        assert_eq!(
            *auth,
            RpcEndpointAuth::BearerToken("test-token-123".to_string())
        );
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
        assert_eq!(
            *auth1,
            RpcEndpointAuth::BasicAuth {
                username: "user1".to_string(),
                password: "pass1".to_string(),
            }
        );

        // Test second endpoint (bearer token)
        let auth2 = config.get_auth_for_url("https://api.sui.io").unwrap();
        assert_eq!(*auth2, RpcEndpointAuth::BearerToken("token123".to_string()));

        // Test third endpoint (basic auth with username only)
        let auth3 = config.get_auth_for_url("http://localhost:8080").unwrap();
        assert_eq!(
            *auth3,
            RpcEndpointAuth::BasicAuth {
                username: "localuser".to_string(),
                password: String::new(),
            }
        );

        // Test non-existent endpoint
        assert!(config.get_auth_for_url("https://nonexistent.com").is_none());
    }
}

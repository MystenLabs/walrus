// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Configuration for async blob upload feature.

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Configuration for async blob upload feature.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct AsyncUploadConfig {
    /// Feature flag to enable async blob uploads.
    /// When enabled, the client returns immediately after reaching quorum (2f+1 shard weight)
    /// and continues uploading to additional nodes in the background.
    #[serde(default)]
    pub enabled: bool,

    /// Maximum number of concurrent background upload tasks.
    #[serde(default = "default_max_concurrent_tasks")]
    pub max_concurrent_tasks: usize,

    /// Timeout for individual background upload tasks in seconds.
    #[serde(
        default = "default_task_timeout",
        rename = "task_timeout_seconds",
        serialize_with = "serialize_duration_as_secs",
        deserialize_with = "deserialize_duration_from_secs"
    )]
    pub task_timeout: Duration,

    /// Whether to wait for background tasks to complete on client shutdown.
    #[serde(default = "default_wait_on_shutdown")]
    pub wait_on_shutdown: bool,
}

impl Default for AsyncUploadConfig {
    fn default() -> Self {
        Self {
            enabled: false, // Feature flag OFF by default for safety
            max_concurrent_tasks: default_max_concurrent_tasks(),
            task_timeout: default_task_timeout(),
            wait_on_shutdown: default_wait_on_shutdown(),
        }
    }
}

fn default_max_concurrent_tasks() -> usize {
    10
}

fn default_task_timeout() -> Duration {
    Duration::from_secs(60)
}

fn default_wait_on_shutdown() -> bool {
    true
}

fn serialize_duration_as_secs<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_u64(duration.as_secs())
}

fn deserialize_duration_from_secs<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let secs = u64::deserialize(deserializer)?;
    Ok(Duration::from_secs(secs))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = AsyncUploadConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.max_concurrent_tasks, 10);
        assert_eq!(config.task_timeout, Duration::from_secs(60));
        assert!(config.wait_on_shutdown);
    }

    #[test]
    fn test_serde_roundtrip() {
        let config = AsyncUploadConfig {
            enabled: true,
            max_concurrent_tasks: 20,
            task_timeout: Duration::from_secs(120),
            wait_on_shutdown: false,
        };

        let yaml = serde_yaml::to_string(&config).unwrap();
        let parsed: AsyncUploadConfig = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(config, parsed);
    }

    #[test]
    fn test_partial_config() {
        let yaml = "enabled: true";
        let config: AsyncUploadConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.enabled);
        assert_eq!(config.max_concurrent_tasks, 10);
        assert_eq!(config.task_timeout, Duration::from_secs(60));
        assert!(config.wait_on_shutdown);
    }
}

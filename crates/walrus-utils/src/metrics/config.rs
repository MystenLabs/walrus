// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Metrics configuration.

use std::{collections::HashMap, time::Duration};

use serde::{Deserialize, Serialize};
use serde_with::{DurationSeconds, serde_as};

/// Configuration for metric push.
#[serde_as]
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct MetricsPushConfig {
    /// The interval of time we will allow to elapse before pushing metrics.
    #[serde_as(as = "DurationSeconds<u64>")]
    #[serde(
        rename = "push_interval_secs",
        default = "defaults::push_interval",
        skip_serializing_if = "defaults::is_push_interval_default"
    )]
    pub push_interval: Duration,
    /// The URL that we will push metrics to.
    pub push_url: String,
    /// Static labels to provide to the push process.
    #[serde(default, skip_serializing_if = "defaults::is_none")]
    pub labels: Option<HashMap<String, String>>,
}

impl MetricsPushConfig {
    /// Creates a new `MetricsPushConfig` with the provided URL and otherwise default values.
    pub fn new_for_url(url: String) -> Self {
        Self {
            push_interval: defaults::push_interval(),
            push_url: url,
            labels: None,
        }
    }

    /// Sets the 'name' label to `name` and the 'host' label to the machine's hostname; if the
    /// hostname cannot be determined, `name` is used as a fallback.
    pub fn set_name_and_host_label(&mut self, name: &str) {
        self.labels_mut()
            .entry("name".into())
            .or_insert_with(|| name.to_owned());

        let host =
            hostname::get().map_or_else(|_| name.into(), |v| v.to_string_lossy().to_string());
        self.set_host(host);
    }

    /// Sets the role associated with the service, overwrites any previously set value.
    pub fn set_role_label(&mut self, role: ServiceRole) {
        if let Some(prior) = self
            .labels_mut()
            .insert("role".to_owned(), role.to_string())
        {
            tracing::warn!(%prior, %role, "overwrote a prior role value");
        }
    }

    /// Sets the 'host' label to `host`.
    fn set_host(&mut self, host: String) {
        self.labels_mut().entry("host".to_owned()).or_insert(host);
    }

    fn labels_mut(&mut self) -> &mut HashMap<String, String> {
        self.labels.get_or_insert_default()
    }
}

/// Default values for metrics configuration.
mod defaults {
    use std::time::Duration;

    /// Configure the default push interval for metrics.
    pub fn push_interval() -> Duration {
        Duration::from_secs(60)
    }

    /// Returns true if the `duration` is equal to the default push interval for metrics.
    pub fn is_push_interval_default(duration: &Duration) -> bool {
        duration == &push_interval()
    }

    /// Returns true iff the value is `None` and we don't run in test mode.
    pub fn is_none<T>(t: &Option<T>) -> bool {
        // The `cfg!(test)` check is there to allow serializing the full configuration, specifically
        // to generate the example configuration files.
        !cfg!(test) && t.is_none()
    }
}

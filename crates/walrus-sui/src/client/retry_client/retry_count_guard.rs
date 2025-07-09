// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! A guard that records the number of retries and the result of an RPC call.
use std::sync::Arc;

use super::ToErrorType;
use crate::client::SuiClientMetricSet;

/// A guard that records the number of retries and the result of an RPC call.
#[derive(Debug)]
#[must_use]
pub(crate) struct RetryCountGuard {
    pub(crate) method: String,
    pub(crate) count: u64,
    pub(crate) metrics: Arc<SuiClientMetricSet>,
    pub(crate) status: String,
}

impl RetryCountGuard {
    pub(crate) fn new(metrics: Arc<SuiClientMetricSet>, method: &str) -> Self {
        Self {
            method: method.to_string(),
            count: 0,
            metrics,
            status: "success".to_string(),
        }
    }

    pub fn record_result<T, E>(&mut self, value: Result<&T, &E>)
    where
        E: ToErrorType,
    {
        match value {
            Ok(_) => self.status = "success".to_string(),
            Err(e) => self.status = e.to_error_type(),
        }
        self.count += 1;
    }
}

impl Drop for RetryCountGuard {
    fn drop(&mut self) {
        if self.count > 1 {
            #[cfg(msim)]
            tracing::debug!(
                "RPC call {} failed {} times, status: {}",
                self.method,
                self.count,
                self.status
            );
            self.metrics
                .record_rpc_retry_count(self.method.as_str(), self.count, &self.status);
        }
    }
}

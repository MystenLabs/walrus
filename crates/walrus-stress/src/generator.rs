// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Generate writes and reads for stress tests.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use blob::WriteBlobConfig;
use futures::future::try_join_all;
use rand::{Rng, thread_rng};
use sui_sdk::types::base_types::SuiAddress;
use tokio::{
    sync::mpsc::{self, Receiver, Sender, error::TryRecvError},
    time::{Interval, MissedTickBehavior},
};
use walrus_core::{BlobId, EpochCount, encoding::Primary};
use walrus_sdk::{
    client::{
        Client,
        metrics::{self, ClientMetrics},
    },
    config::ClientConfig,
    error::ClientResult,
};
use walrus_service::client::{RefillHandles, Refiller};
use walrus_sui::{
    client::{SuiReadClient, retry_client::RetriableSuiClient},
    utils::SuiNetwork,
};

/// Minimum burst duration in milliseconds.
const MIN_BURST_DURATION_MS: u64 = 100;
/// Minimum burst duration.
const MIN_BURST_DURATION: Duration = Duration::from_millis(MIN_BURST_DURATION_MS);
/// Number of seconds per load period.
const SECS_PER_LOAD_PERIOD: u64 = 60;

pub(crate) mod blob;

pub mod write_client;
use walrus_utils::backoff::{BackoffStrategy, ExponentialBackoffConfig};
use write_client::WriteClient;

use crate::generator::blob::QuiltStoreBlobConfig;

/// A load generator for Walrus writes.
#[derive(Debug)]
pub struct LoadGenerator {
    write_client: WriteClient,
    metrics: Arc<ClientMetrics>,
    quilt_percentage: f64,
}

fn burst_load(load: u64) -> (u64, Interval) {
    if load == 0 {
        // Set the interval to ~100 years. `Duration::MAX` causes an overflow in tokio.
        return (
            0,
            tokio::time::interval(Duration::from_secs(100 * 365 * 24 * 60 * 60)),
        );
    }
    let duration_per_op = Duration::from_secs_f64(SECS_PER_LOAD_PERIOD as f64 / (load as f64));
    let (load_per_burst, burst_duration) = if duration_per_op < MIN_BURST_DURATION {
        (
            load / (SECS_PER_LOAD_PERIOD * 1_000 / MIN_BURST_DURATION_MS),
            MIN_BURST_DURATION,
        )
    } else {
        (1, duration_per_op)
    };

    let mut interval = tokio::time::interval(burst_duration);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    (load_per_burst, interval)
}

// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! An actor that manages a rayon ThreadPool and executes compute-intensive jobs.

use std::{
    any::Any,
    fmt::{self, Debug},
};

use rayon::ThreadPool;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

/// Configuration for the RayonPool actor.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RayonPoolConfig {
    /// Number of threads in the pool.
    pub num_threads: usize,
    /// Size of the channel for job requests.
    pub channel_size: usize,
}

impl RayonPoolConfig {
    /// Builds a RayonPoolActor from the config.
    pub fn build(&self) -> (RayonPoolActor, RayonPoolHandle) {
        let (actor, handle) = RayonPoolActor::new(self.channel_size, self.num_threads);
        (actor, handle)
    }

    /// Builds a RayonPoolActor and runs it in a new tokio task.
    pub fn build_and_run(&self) -> RayonPoolHandle {
        let (mut actor, handle) = self.build();
        tokio::spawn(async move {
            actor.run().await;
        });
        handle
    }
}

impl Default for RayonPoolConfig {
    fn default() -> Self {
        Self {
            num_threads: default::num_threads(),
            channel_size: default::channel_size(),
        }
    }
}

mod default {
    pub(crate) fn num_threads() -> usize {
        // Default to number of CPUs.
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
    }

    pub(crate) fn channel_size() -> usize {
        100
    }
}

pub type OpaqueReturn = Box<dyn Any + Send>;

/// A request to the rayon thread pool containing the job to execute and the channel
/// to send the result back on.
pub struct PoolRequest {
    job: Box<dyn FnOnce() -> OpaqueReturn + Send>,
    response_tx: oneshot::Sender<OpaqueReturn>,
}

impl Debug for PoolRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PoolRequest").finish()
    }
}

/// The actor that manages the rayon ThreadPool.
#[derive(Debug)]
pub struct RayonPoolActor {
    pool: ThreadPool,
    job_rx: mpsc::Receiver<PoolRequest>,
}

/// Handle to communicate with the RayonPoolActor.
#[derive(Debug, Clone)]
pub struct RayonPoolHandle {
    job_tx: mpsc::Sender<PoolRequest>,
}

impl RayonPoolActor {
    /// Creates a new RayonPoolActor.
    pub fn new(channel_size: usize, num_threads: usize) -> (Self, RayonPoolHandle) {
        let (job_tx, job_rx) = mpsc::channel(channel_size);
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .build()
            .expect("Failed to create thread pool");

        (Self { pool, job_rx }, RayonPoolHandle { job_tx })
    }

    /// Runs the actor, processing job requests.
    pub async fn run(&mut self) {
        while let Some(PoolRequest { job, response_tx }) = self.job_rx.recv().await {
            self.pool.spawn(move || {
                let result = job();
                let _ = response_tx.send(result);
            });
        }

        tracing::info!("RayonPool channel closed, stopping actor");
    }
}

impl RayonPoolHandle {
    /// Submits a function to be executed in the thread pool
    pub async fn submit<F, T>(&self, job: F) -> Result<T, RayonPoolError>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let (response_tx, response_rx) = oneshot::channel();
        let job = Box::new(move || Box::new(job()) as OpaqueReturn);
        self.job_tx.send(PoolRequest { job, response_tx }).await?;

        let result = response_rx.await?;
        Ok(*result
            .downcast()
            .expect("the actor will only reply with the correct type"))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RayonPoolError {
    #[error("failed to send job to pool: {0}")]
    Send(#[from] mpsc::error::SendError<PoolRequest>),
    #[error("failed to receive result from pool: {0}")]
    Receive(#[from] oneshot::error::RecvError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_rayon_pool() {
        let handle = RayonPoolConfig::default().build_and_run();
        let result = handle.submit(|| 1 + 2).await.unwrap();
        assert_eq!(result, 3);
    }

    #[tokio::test]
    async fn test_multi_large_comp_rayon_pool() {
        let handle = RayonPoolConfig::default().build_and_run();
        let create_job = |num| {
            move || {
                let mut sum = 0;
                for i in 0..num {
                    sum += i;
                }
                sum
            }
        };

        let nums: Vec<u64> = vec![1_000, 10_000, 100_000];
        let results = nums
            .iter()
            .map(|num| handle.submit(create_job(*num)))
            .collect::<Vec<_>>();

        let results = futures::future::try_join_all(results).await.unwrap();
        assert_eq!(
            results,
            nums.iter()
                .map(|num| num * (num - 1) / 2)
                .collect::<Vec<_>>()
        );
    }
}

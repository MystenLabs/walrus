// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/// A simple auto-tune mechanism for the data-in-flight limit.
///
/// The auto-tune mechanism is based on a an algorithm which works in three phases which are:
/// 1. Searching: The auto-tune mechanism is searching for the peak throughput. It'll increase
///    the permits until the peak throughput is found.
/// 2. Verifying: The auto-tune mechanism is verifying if the peak throughput is real. It'll hold
///    the permits at the peak throughput until the throughput drops below the peak throughput.
/// 3. Locked: The auto-tune mechanism is locked in the peak throughput. It'll hold the permits
///    at the peak throughput.
///
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use tokio::sync::{
    Semaphore,
    mpsc::{UnboundedSender, unbounded_channel},
};
use walrus_core::SliverType;

use crate::config::communication_config::DataInFlightAutoTuneConfig;

/// Handle used by write tasks to report progress to the auto tuner.
#[derive(Clone, Debug)]
pub(crate) struct AutoTuneHandle {
    inner: Arc<AutoTuneInner>,
    semaphore: Arc<Semaphore>,
    primary_weight: f64,
    secondary_weight: f64,
}

impl AutoTuneHandle {
    pub fn new(config: &DataInFlightAutoTuneConfig, allowed_permits: usize) -> Option<Self> {
        if !config.enabled {
            return None;
        }

        let allowed_permits = allowed_permits.max(1);

        let (config_min, config_max) = config.permit_bounds();
        let max_permits = config_max.min(allowed_permits).max(1);
        let min_permits = config_min.min(max_permits).max(1);

        if max_permits <= 1 {
            tracing::warn!(
                target: "walrus::auto_tune",
                max_permits,
                "auto tune disabled because at most one concurrent write is allowed"
            );
            return None;
        }

        debug_assert!(
            min_permits < max_permits,
            "min_permits should be < max_permits"
        );

        let sample_target = config.sample_target();
        let window_timeout = config.timeout();
        let increase_factor = config.increase_factor();
        let lock_factor = config.lock_factor();
        let primary_weight = 1.0_f64;
        let secondary_weight = config.secondary_sliver_weight().min(primary_weight);

        let initial_permits = min_permits;
        let state = AutoTuneState::new(
            initial_permits,
            min_permits,
            max_permits,
            sample_target,
            window_timeout,
            increase_factor,
            lock_factor,
        );
        let semaphore = Arc::new(Semaphore::new(initial_permits));

        Some(Self {
            inner: Arc::new(AutoTuneInner::new(semaphore.clone(), state)),
            semaphore,
            primary_weight,
            secondary_weight,
        })
    }

    pub fn semaphore(&self) -> Arc<Semaphore> {
        self.semaphore.clone()
    }

    pub fn record_success(&self, bytes: usize, sliver_type: SliverType, completed_at: Instant) {
        let weight = self.weight_for(sliver_type);
        let mut state = self
            .inner
            .state
            .lock()
            .expect("mutex should not be poisoned");

        state.observe(bytes, weight, completed_at);
        let adjustment = self.inner.compute_adjustment(&mut state);

        drop(state);
        self.inner.apply_adjustment(adjustment);
    }

    fn weight_for(&self, sliver_type: SliverType) -> f64 {
        match sliver_type {
            SliverType::Primary => self.primary_weight,
            SliverType::Secondary => self.secondary_weight,
        }
    }
}

#[derive(Debug)]
struct AutoTuneInner {
    state: Mutex<AutoTuneState>,
    adjust_tx: UnboundedSender<AdjustmentKind>,
}

impl AutoTuneInner {
    fn new(semaphore: Arc<Semaphore>, state: AutoTuneState) -> Self {
        let (adjust_tx, mut adjust_rx) = unbounded_channel::<AdjustmentKind>();
        tokio::spawn(async move {
            while let Some(kind) = adjust_rx.recv().await {
                match kind {
                    AdjustmentKind::Increase { permits } => {
                        if permits > 0 {
                            semaphore.add_permits(permits);
                        }
                    }
                    AdjustmentKind::Decrease { permits } => {
                        for _ in 0..permits {
                            match semaphore.clone().acquire_owned().await {
                                Ok(permit) => {
                                    drop(permit);
                                }
                                Err(_) => {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        });

        Self {
            state: Mutex::new(state),
            adjust_tx,
        }
    }

    /// Checks if the desired number of permits has changed and returns the
    /// necessary adjustment.
    fn compute_adjustment(&self, state: &mut AutoTuneState) -> Option<AdjustmentKind> {
        if state.desired_permits > state.current_permits {
            let needed = state.desired_permits - state.current_permits;
            state.current_permits = state.desired_permits;
            Some(AdjustmentKind::Increase { permits: needed })
        } else if state.desired_permits < state.current_permits {
            let excess = state.current_permits - state.desired_permits;
            state.current_permits = state.desired_permits;
            Some(AdjustmentKind::Decrease { permits: excess })
        } else {
            None
        }
    }

    fn apply_adjustment(&self, adjustment: Option<AdjustmentKind>) {
        if let Some(kind) = adjustment
            && self.adjust_tx.send(kind).is_err()
        {
            tracing::debug!(target: "walrus::auto_tune", "auto tune channel closed");
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum AdjustmentKind {
    Increase { permits: usize },
    Decrease { permits: usize },
}

/// The state machine for the "Find and Lock" algorithm.
#[derive(Debug)]
enum Phase {
    /// Aggressively increasing permits to find the peak.
    Searching,
    /// Paused at a potential peak to confirm if it's real.
    Verifying,
    /// Peak found, permits are locked for the duration of the upload.
    Locked,
}

#[derive(Debug)]
struct AutoTuneState {
    phase: Phase,
    desired_permits: usize,
    current_permits: usize,
    min_permits: usize,
    max_permits: usize,
    window_sample_target: usize,
    window_timeout: Duration,
    window_start_time: Instant,
    window_bytes: u128,
    window_effective_slivers: f64,
    window_slivers: usize,
    last_throughput: f64,
    max_observed_throughput: f64,
    permits_at_max: usize,
    increase_factor: f64,
    lock_factor: f64,
}

impl AutoTuneState {
    fn new(
        initial_permits: usize,
        min_permits: usize,
        max_permits: usize,
        window_sample_target: usize,
        window_timeout: Duration,
        increase_factor: f64,
        lock_factor: f64,
    ) -> Self {
        tracing::debug!(
            target: "walrus::auto_tune",
            initial_permits,
            min_permits,
            max_permits,
            "Auto-tuner initialized in 'Searching' phase."
        );
        Self {
            phase: Phase::Searching,
            desired_permits: initial_permits,
            current_permits: initial_permits,
            min_permits,
            max_permits,
            window_sample_target,
            window_timeout,
            window_start_time: Instant::now(),
            window_bytes: 0,
            window_effective_slivers: 0.0,
            window_slivers: 0,
            last_throughput: 0.0,
            max_observed_throughput: 0.0,
            permits_at_max: initial_permits,
            increase_factor,
            lock_factor,
        }
    }

    /// Main entry point for the state machine.
    fn observe(&mut self, bytes: usize, weight: f64, completed_at: Instant) {
        self.window_bytes = self.window_bytes.saturating_add(bytes as u128);
        self.window_effective_slivers =
            (self.window_effective_slivers + weight).clamp(0.0, f64::MAX);
        self.window_slivers = self.window_slivers.saturating_add(1);
        let elapsed = completed_at.duration_since(self.window_start_time);

        if self.window_effective_slivers < (self.window_sample_target as f64)
            && elapsed < self.window_timeout
        {
            return;
        }

        let current_throughput =
            (self.window_bytes as f64) / elapsed.as_secs_f64().max(f64::EPSILON);

        if self.window_effective_slivers >= (self.window_sample_target as f64) {
            tracing::trace!(
                phase = "Searching",
                action = "sample_window_completed",
                window_slivers = self.window_slivers,
                effective_slivers = self.window_effective_slivers,
                elapsed_millis = elapsed.as_millis(),
                current_throughput,
                last_throughput = self.last_throughput,
                "Sample window completed."
            );
        } else if elapsed >= self.window_timeout {
            tracing::trace!(
                phase = "Searching",
                action = "sample_window_timeout",
                window_slivers = self.window_slivers,
                effective_slivers = self.window_effective_slivers,
                elapsed_millis = elapsed.as_millis(),
                current_throughput,
                last_throughput = self.last_throughput,
                "Sample window timed out."
            );
        }

        match self.phase {
            Phase::Searching => self.search(current_throughput),
            Phase::Verifying => self.verify(current_throughput),
            Phase::Locked => { /* Nothing to do */ }
        }

        self.window_start_time = Instant::now();
        self.window_bytes = 0;
        self.window_effective_slivers = 0.0;
        self.window_slivers = 0;
        self.last_throughput = current_throughput;
    }

    fn search(&mut self, current_throughput: f64) {
        if current_throughput >= self.last_throughput {
            if current_throughput > self.max_observed_throughput {
                self.max_observed_throughput = current_throughput;
                self.permits_at_max = self.current_permits;
            }

            // This window performed better than the last. Aggressively increase permits.
            #[allow(clippy::cast_possible_truncation)]
            let new_target = ((self.desired_permits as f64) * self.increase_factor).ceil() as usize;
            self.desired_permits = new_target.clamp(self.min_permits, self.max_permits);

            tracing::debug!(
                phase = "Searching",
                action = "increase",
                throughput_sps = current_throughput,
                new_permits = self.desired_permits,
                "Throughput increasing, continuing aggressive ramp-up."
            );
        } else {
            // Enter verifying state to confirm before locking permits.
            self.phase = Phase::Verifying;
            tracing::debug!(
                phase = "Searching",
                action = "hold",
                throughput_sps = current_throughput,
                "Throughput dropped. Entering 'Verifying' phase."
            );
        }
    }

    fn verify(&mut self, current_throughput: f64) {
        if current_throughput >= self.last_throughput {
            // Throughput recovered. Resume searching.
            self.phase = Phase::Searching;
            tracing::debug!(
                phase = "Verifying",
                action = "resume_search",
                throughput_sps = current_throughput,
                "Throughput recovered. Resuming search."
            );
        } else {
            // We've found the peak. Lock it in.
            self.phase = Phase::Locked;
            #[allow(clippy::cast_possible_truncation)]
            let final_permits = ((self.permits_at_max as f64) * self.lock_factor).round() as usize;
            self.desired_permits = final_permits.clamp(self.min_permits, self.max_permits);

            tracing::debug!(
                phase = "Verifying",
                action = "lock",
                throughput_sps = self.max_observed_throughput,
                permits_at_max = self.permits_at_max,
                final_permits = self.desired_permits,
                "Peak confirmed. Locking permits."
            );
        }
    }
}

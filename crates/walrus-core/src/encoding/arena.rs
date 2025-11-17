// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Shared arena allocator scaffolding for symbol encoding.

use core::{
    fmt,
    ops::Range,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};
use memvec::{MemVec, MmapAnon};
use std::io;
use thiserror::Error;
use tracing::debug;

const DEFAULT_SYMBOL_ARENA_CAPACITY_BYTES: usize = 4 * 1024 * 1024 * 1024; // 4 GiB
const DEFAULT_SYMBOL_ARENA_SPILL_BYTES: usize = 512 * 1024 * 1024; // 512 MiB
const MIN_SYMBOL_ARENA_CAPACITY_BYTES: usize = 1024 * 1024; // 1 MiB safety floor

type ArenaMemVec = MemVec<'static, u8, MmapAnon>;

static ARENA_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Identifier for arena sessions to simplify logging and telemetry.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SymbolArenaId(u64);

impl SymbolArenaId {
    fn next() -> Self {
        Self(ARENA_ID_COUNTER.fetch_add(1, Ordering::Relaxed))
    }
}

impl fmt::Display for SymbolArenaId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "arena-{}", self.0)
    }
}

/// User configurable bounds for the shared symbol arena.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SymbolArenaConfig {
    pub capacity_bytes: usize,
    pub spill_capacity_bytes: usize,
}

impl SymbolArenaConfig {
    /// Creates a new arena configuration, clamping obviously invalid inputs.
    pub const fn new(capacity_bytes: usize, spill_capacity_bytes: usize) -> Self {
        Self {
            capacity_bytes,
            spill_capacity_bytes,
        }
    }

    fn sanitized(self) -> Self {
        let capacity = self.capacity_bytes.max(MIN_SYMBOL_ARENA_CAPACITY_BYTES);
        let spill = self.spill_capacity_bytes.min(capacity);
        Self {
            capacity_bytes: capacity,
            spill_capacity_bytes: spill,
        }
    }
}

impl Default for SymbolArenaConfig {
    fn default() -> Self {
        Self {
            capacity_bytes: DEFAULT_SYMBOL_ARENA_CAPACITY_BYTES,
            spill_capacity_bytes: DEFAULT_SYMBOL_ARENA_SPILL_BYTES,
        }
    }
}

/// Snapshot of arena usage for telemetry/logging.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SymbolArenaStats {
    pub arena_capacity_bytes: usize,
    pub arena_used_bytes: usize,
    pub high_watermark_bytes: usize,
    pub spill_bytes: usize,
    pub spill_count: u64,
    pub duration: Duration,
}

/// Errors that can occur when manipulating the shared arena.
#[derive(Debug, Error)]
pub enum SymbolArenaError {
    #[error(
        "requested {requested} bytes but only {remaining} bytes remaining (capacity {capacity})"
    )]
    CapacityExceeded {
        requested: usize,
        remaining: usize,
        capacity: usize,
    },
    #[error("failed to allocate arena backing pool: {0}")]
    AllocationFailed(#[from] io::Error),
    #[error("reservation does not belong to this arena session")]
    WrongSession,
}

/// Reservation handle returned by [`SymbolArenaSession::reserve_slice`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SymbolArenaReservation {
    session_id: SymbolArenaId,
    range: Range<usize>,
}

impl SymbolArenaReservation {
    fn new(session_id: SymbolArenaId, range: Range<usize>) -> Self {
        Self { session_id, range }
    }

    /// Returns the number of bytes reserved.
    pub fn len(&self) -> usize {
        self.range.end.saturating_sub(self.range.start)
    }

    /// Returns `true` when the reservation is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn session_id(&self) -> SymbolArenaId {
        self.session_id
    }

    pub(crate) fn range(&self) -> Range<usize> {
        self.range.clone()
    }
}

#[derive(Debug)]
struct MemvecBackingPool {
    mem: ArenaMemVec,
    resize_events: u32,
}

impl MemvecBackingPool {
    fn new(capacity: usize) -> Result<Self, io::Error> {
        let mmap = MmapAnon::with_size(capacity.max(1))?;
        let mem = match unsafe { MemVec::try_from_memory(mmap) } {
            Ok(memvec) => memvec,
            Err((_mmap, layout_error)) => {
                return Err(io::Error::new(io::ErrorKind::Other, layout_error))
            }
        };
        Ok(Self {
            mem,
            resize_events: 0,
        })
    }

    fn capacity(&self) -> usize {
        self.mem.capacity()
    }

    fn extend_to(&mut self, len: usize) {
        unsafe {
            self.mem.set_len(len);
        }
    }

    fn slice_mut(&mut self, range: Range<usize>) -> &mut [u8] {
        &mut self.mem.as_mut_slice()[range]
    }

    fn release(&mut self) {
        self.mem.truncate(0);
    }
}

/// RAII session that tracks arena usage and exposes allocation helpers.
#[derive(Debug)]
pub struct SymbolArenaSession {
    id: SymbolArenaId,
    config: SymbolArenaConfig,
    pool: MemvecBackingPool,
    stats: SymbolArenaStats,
    pending_bytes: usize,
    started_at: Instant,
}

impl SymbolArenaSession {
    /// Starts a new session with the provided configuration.
    pub fn start(config: SymbolArenaConfig) -> Result<Self, SymbolArenaError> {
        let config = config.sanitized();
        let pool = MemvecBackingPool::new(config.capacity_bytes)?;
        let id = SymbolArenaId::next();
        debug!(
            %idid,
            capacity_bytes = config.capacity_bytes,
            spill_bytes = config.spill_capacity_bytes,
            "allocating symbol arena session"
        );
        Ok(Self {
            id,
            stats: SymbolArenaStats {
                arena_capacity_bytes: config.capacity_bytes,
                ..Default::default()
            },
            config,
            pool,
            pending_bytes: 0,
            started_at: Instant::now(),
        })
    }

    /// Returns the session identifier.
    pub fn id(&self) -> SymbolArenaId {
        self.id
    }

    /// Returns the current statistics snapshot.
    pub fn stats(&self) -> SymbolArenaStats {
        self.stats
    }

    /// Reserves a mutable slice within the arena, returning a reservation handle.
    pub fn reserve_slice(
        &mut self,
        len_bytes: usize,
    ) -> Result<SymbolArenaReservation, SymbolArenaError> {
        if len_bytes == 0 {
            let cursor = self.stats.arena_used_bytes + self.pending_bytes;
            return Ok(SymbolArenaReservation::new(self.id, cursor..cursor));
        }
        let cursor = self.stats.arena_used_bytes + self.pending_bytes;
        let new_len = cursor
            .checked_add(len_bytes)
            .expect("arena length must not overflow usize");
        if new_len > self.config.capacity_bytes {
            let remaining = self
                .config
                .capacity_bytes
                .saturating_sub(self.stats.arena_used_bytes + self.pending_bytes);
            return Err(SymbolArenaError::CapacityExceeded {
                requested: len_bytes,
                remaining,
                capacity: self.config.capacity_bytes,
            });
        }
        self.pool.extend_to(new_len);
        self.pending_bytes += len_bytes;
        self.stats.high_watermark_bytes = self.stats.high_watermark_bytes.max(new_len);
        Ok(SymbolArenaReservation::new(self.id, cursor..new_len))
    }

    /// Provides mutable access to the slice described by the reservation.
    pub fn slice_mut<'a>(
        &'a mut self,
        reservation: &SymbolArenaReservation,
    ) -> Result<&'a mut [u8], SymbolArenaError> {
        self.ensure_reservation_session(reservation)?;
        Ok(self.pool.slice_mut(reservation.range()))
    }

    /// Commits the reserved bytes, updating usage counters.
    pub fn commit_usage(
        &mut self,
        reservation: SymbolArenaReservation,
    ) -> Result<(), SymbolArenaError> {
        self.ensure_reservation_session(&reservation)?;
        let len = reservation.len();
        if len > 0 {
            self.stats.arena_used_bytes = self
                .stats
                .arena_used_bytes
                .checked_add(len)
                .expect("arena usage fits in usize");
            self.pending_bytes = self.pending_bytes.saturating_sub(len);
        }
        Ok(())
    }

    /// Resets the session so it can be reused for a subsequent store run.
    pub fn release_all(&mut self) {
        self.pool.release();
        self.pending_bytes = 0;
        self.stats.arena_used_bytes = 0;
        self.stats.spill_bytes = 0;
        self.stats.spill_count = 0;
        self.stats.high_watermark_bytes = 0;
        self.started_at = Instant::now();
    }

    /// Consumes the session and returns the final stats snapshot.
    pub fn finish(mut self) -> SymbolArenaStats {
        let mut stats = self.stats;
        stats.duration = self.started_at.elapsed();
        self.release_all();
        stats
    }

    fn ensure_reservation_session(
        &self,
        reservation: &SymbolArenaReservation,
    ) -> Result<(), SymbolArenaError> {
        if reservation.session_id() != self.id {
            return Err(SymbolArenaError::WrongSession);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserve_and_commit_updates_usage() {
        let mut session = SymbolArenaSession::start(SymbolArenaConfig::new(1024, 256)).unwrap();
        let reservation = session.reserve_slice(128).unwrap();
        assert_eq!(reservation.len(), 128);
        session.commit_usage(reservation).unwrap();
        assert_eq!(session.stats().arena_used_bytes, 128);
        assert_eq!(session.stats().high_watermark_bytes, 128);
    }

    #[test]
    fn release_all_resets_usage() {
        let mut session = SymbolArenaSession::start(SymbolArenaConfig::new(1024, 256)).unwrap();
        let reservation = session.reserve_slice(64).unwrap();
        session.commit_usage(reservation).unwrap();
        assert_eq!(session.stats().arena_used_bytes, 64);
        session.release_all();
        assert_eq!(session.stats().arena_used_bytes, 0);
        assert_eq!(session.stats().high_watermark_bytes, 0);
    }

    #[test]
    fn finish_returns_snapshot() {
        let mut session = SymbolArenaSession::start(SymbolArenaConfig::new(256, 64)).unwrap();
        let reservation = session.reserve_slice(32).unwrap();
        session.commit_usage(reservation).unwrap();
        let stats = session.finish();
        assert_eq!(stats.arena_used_bytes, 32);
        assert!(stats.duration >= Duration::default());
    }
}

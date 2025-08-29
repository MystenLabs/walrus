// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Advisory file lock to ensure only one Publisher instance runs at a time.

use std::{
    fs::OpenOptions,
    io::{Seek, SeekFrom, Write},
    net::SocketAddr,
    path::PathBuf,
};

use anyhow::{Context, Result};
use thiserror::Error;

/// Errors that can occur when acquiring the publisher lock.
#[derive(Error, Debug)]
pub enum PublisherLockError {
    #[error("Another publisher instance is already running")]
    AlreadyLocked,
    #[error("Failed to create lock file directory: {0}")]
    DirectoryCreation(std::io::Error),
    #[error("Failed to access lock file: {0}")]
    FileAccess(std::io::Error),
    #[error("Failed to determine home directory: {0}")]
    HomeDirectory(anyhow::Error),
}

impl From<std::io::Error> for PublisherLockError {
    fn from(err: std::io::Error) -> Self {
        PublisherLockError::FileAccess(err)
    }
}

/// Advisory file lock to prevent multiple Publisher instances from running simultaneously.
///
/// This lock uses the operating system's advisory file locking mechanism to ensure
/// that only one Publisher daemon can run at a time per user. The lock is automatically
/// released when the PublisherLock instance is dropped (e.g., when the process exits).
pub struct PublisherLock {
    _file: std::fs::File,
}

impl PublisherLock {
    /// Attempts to acquire an exclusive lock for the Publisher.
    ///
    /// # Arguments
    ///
    /// * `bind_address` - The network address the publisher will bind to
    ///
    /// # Returns
    ///
    /// * `Ok(PublisherLock)` - Lock acquired successfully
    /// * `Err(PublisherLockError::AlreadyLocked)` - Another publisher is already running
    /// * `Err(PublisherLockError::FileAccess)` - I/O error accessing the lock file
    /// * `Err(PublisherLockError::DirectoryCreation)` - Failed to create lock directory
    pub fn try_acquire(bind_address: SocketAddr) -> Result<Self, PublisherLockError> {
        let lock_path = get_lock_file_path()?;

        // Ensure the parent directory exists
        if let Some(parent) = lock_path.parent() {
            std::fs::create_dir_all(parent).map_err(PublisherLockError::DirectoryCreation)?;
        }

        // Open/create lock file WITHOUT truncating if it exists
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true) // Create if doesn't exist
            .truncate(false) // Don't truncate existing file
            .open(&lock_path)?;

        // Try to acquire exclusive lock (non-blocking)
        file.try_lock()
            .map_err(|_| PublisherLockError::AlreadyLocked)?;

        // NOW truncate and write process info (since we have the lock)
        file.set_len(0)?; // Truncate file content
        file.seek(SeekFrom::Start(0))?;

        writeln!(file, "PID: {}", std::process::id())?;
        writeln!(file, "Address: {}", bind_address)?;
        writeln!(file, "Started: {}", chrono::Utc::now().to_rfc3339())?;
        file.flush()?;

        tracing::info!(
            lock_file = %lock_path.display(),
            pid = std::process::id(),
            %bind_address,
            "Publisher lock acquired successfully"
        );

        Ok(PublisherLock { _file: file })
    }
}

impl Drop for PublisherLock {
    fn drop(&mut self) {
        tracing::info!("Publisher lock released");
    }
}

/// Returns the path to the publisher lock file.
///
/// Uses the user's home directory under `.walrus/publisher.lock`.
/// This ensures per-user singleton behavior.
fn get_lock_file_path() -> Result<PathBuf, PublisherLockError> {
    let home_dir = home::home_dir()
        .context("Failed to determine home directory")
        .map_err(PublisherLockError::HomeDirectory)?;

    let lock_path = home_dir.join(".walrus").join("publisher.lock");
    Ok(lock_path)
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

    use super::*;

    // Test version that uses a unique lock file per test to avoid conflicts
    struct TestPublisherLock {
        _file: std::fs::File,
    }

    impl TestPublisherLock {
        fn try_acquire_with_path(
            bind_address: SocketAddr,
            lock_path: PathBuf,
        ) -> Result<Self, PublisherLockError> {
            // Ensure the parent directory exists
            if let Some(parent) = lock_path.parent() {
                std::fs::create_dir_all(parent).map_err(PublisherLockError::DirectoryCreation)?;
            }

            // Open/create lock file WITHOUT truncating if it exists
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true) // Create if doesn't exist
                .truncate(false) // Don't truncate existing file
                .open(&lock_path)?;

            // Try to acquire exclusive lock (non-blocking)
            file.try_lock()
                .map_err(|_| PublisherLockError::AlreadyLocked)?;

            // NOW truncate and write process info (since we have the lock)
            file.set_len(0)?; // Truncate file content
            file.seek(SeekFrom::Start(0))?;

            writeln!(file, "PID: {}", std::process::id())?;
            writeln!(file, "Address: {}", bind_address)?;
            writeln!(file, "Started: {}", chrono::Utc::now().to_rfc3339())?;
            file.flush()?;

            Ok(TestPublisherLock { _file: file })
        }
    }

    #[test]
    fn test_lock_file_path() {
        let path = get_lock_file_path().unwrap();
        assert!(path.to_string_lossy().contains(".walrus"));
        assert!(path.to_string_lossy().ends_with("publisher.lock"));
    }

    #[test]
    fn test_single_lock_acquisition() {
        let temp_dir = tempfile::tempdir().unwrap();
        let lock_path = temp_dir.path().join("test1.lock");

        let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 8080));
        let _lock = TestPublisherLock::try_acquire_with_path(addr, lock_path).unwrap();
        // Lock should be held until _lock is dropped
    }

    #[test]
    fn test_multiple_lock_acquisition_fails() {
        let temp_dir = tempfile::tempdir().unwrap();
        let lock_path = temp_dir.path().join("test2.lock");

        let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 8080));
        let _lock1 = TestPublisherLock::try_acquire_with_path(addr, lock_path.clone()).unwrap();

        // Second lock attempt should fail
        match TestPublisherLock::try_acquire_with_path(addr, lock_path) {
            Err(PublisherLockError::AlreadyLocked) => {
                // Expected behavior
            }
            _ => panic!("Second lock acquisition should have failed with AlreadyLocked"),
        }
    }
}

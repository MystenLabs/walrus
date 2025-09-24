// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Walrus Indexer Library - Skeleton

pub mod storage;

// Re-export storage types
pub use storage::{BlobIdentity, IndexTarget, ObjectIndexValue, Patch, WalrusIndexStore};

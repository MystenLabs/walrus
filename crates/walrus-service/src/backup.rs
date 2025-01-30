// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Walrus blob backup service.

mod config;
pub use config::BackupNodeConfig;

#[cfg(feature = "backup")]
mod models;

#[cfg(feature = "backup")]
mod schema;

#[cfg(feature = "backup")]
mod service;

#[cfg(feature = "backup")]
pub use service::start_backup_node;

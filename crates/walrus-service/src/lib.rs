// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Walrus client, server, and associated utilities.

#[cfg(feature = "client")]
pub mod client;

pub mod events;

#[cfg(feature = "node")]
pub mod node;

#[cfg(feature = "deploy")]
pub mod testbed;

#[cfg(any(feature = "client", feature = "node"))]
pub(crate) mod common;
#[cfg(any(feature = "client", feature = "node"))]
pub use common::utils;

#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

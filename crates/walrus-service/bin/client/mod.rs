// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The Walrus client.

use anyhow::Result;

mod cli;
mod client;
mod utils;

#[tokio::main]
/// The main function for the Walrus client.
async fn main() -> Result<()> {
    cli::main().await
}

// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The Walrus client.

use std::path::PathBuf;

use clap::Parser;

/// CLI options for the Walrus client.
#[derive(Parser)]
#[command(author, version, about = "Walrus client", long_about = None)]
pub struct Opts {
    /// The path to the Sui wallet file.
    #[clap(long, value_name = "FILE")]
    wallet_path: PathBuf,

    /// The path to the blob the user wants to store on Walrus.
    #[clap(long, value_name = "FILE")]
    blob_path: PathBuf,

    /// The duration for which the user wants to store the blob (in Walrus epochs).
    #[clap(long, value_name = "INT", default_value_t = 2)]
    duration: usize,
}

/// The main function for the Walrus client.
fn main() {
    let _opts: Opts = Opts::parse();
    todo!("Implement the client")
}

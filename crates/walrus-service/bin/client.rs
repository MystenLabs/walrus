//! The Walrus client.

use std::{num::ParseIntError, time::Duration};

use clap::Parser;

/// CLI options for the Walrus client.
#[derive(Parser)]
#[command(author, version, about = "Walrus client", long_about = None)]
pub struct Opts {
    /// The path to the Sui wallet file.
    #[clap(long, value_name = "FILE")]
    wallet_path: PathBuf,

    /// The path to the blob the user wants to store on Walrus.
    #[clap(long, value_name = "FILE", global = true)]
    blob_path: String,

    /// The duration for which the user wants to store the blob (in days).
    #[clap(long, value_parser = parse_duration, default_value = "30", global = true)]
    duration: Duration,
}

/// Parses a duration from a string representing the number of days.
fn parse_duration(arg: &str) -> Result<Duration, ParseIntError> {
    let days: u64 = arg.parse()?;
    let seconds = days * 24 * 60 * 60;
    Ok(Duration::from_secs(seconds))
}

/// The main function for the Walrus client.
fn main() {
    let _opts: Opts = Opts::parse();
    todo!("Implement the client")
}

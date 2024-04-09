// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Orchestrator entry point.

use clap::Parser;
use client::{aws::AwsClient, vultr::VultrClient, ServerProviderClient};
use eyre::Context;
use settings::{CloudProvider, Settings};
use testbed::Testbed;

mod client;
mod display;
mod error;
mod settings;
mod ssh;
mod testbed;

/// The orchestrator command line options.
#[derive(Parser, Debug)]
#[command(author, version, about = "Testbed orchestrator", long_about = None)]
#[clap(rename_all = "kebab-case")]
pub struct Opts {
    /// The path to the settings file. This file contains basic information to deploy testbeds
    /// and run benchmarks such as the url of the git repo, the commit to deploy, etc.
    #[clap(
        long,
        value_name = "FILE",
        default_value = "orchestrator/assets/settings-template.yaml",
        global = true
    )]
    settings_path: String,

    /// The type of operation to run.
    #[clap(subcommand)]
    operation: Operation,
}

/// The type of operation to run.
#[derive(Parser, Debug)]
#[clap(rename_all = "kebab-case")]
pub enum Operation {
    /// Read or modify the status of the testbed.
    Testbed {
        /// The action to perform on the testbed.
        #[clap(subcommand)]
        action: TestbedAction,
    },
    /// Deploy nodes and run a benchmark on the specified testbed.
    Benchmark,
}

/// The action to perform on the testbed.
#[derive(Parser, Debug)]
#[clap(rename_all = "kebab-case")]
pub enum TestbedAction {
    /// Display the testbed status.
    Status,

    /// Deploy the specified number of instances in all regions specified by in the setting file.
    Deploy {
        /// Number of instances to deploy.
        #[clap(long)]
        instances: usize,

        /// The region where to deploy the instances. If this parameter is not specified, the
        /// command deploys the specified number of instances in all regions listed in the
        /// setting file.
        #[clap(long)]
        region: Option<String>,
    },

    /// Start at most the specified number of instances per region on an existing testbed.
    Start {
        /// Number of instances to deploy.
        #[clap(long, default_value_t = 10)]
        instances: usize,
    },

    /// Stop an existing testbed (without destroying the instances).
    Stop,

    /// Destroy the testbed and terminate all instances.
    Destroy,
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    color_eyre::install()?;
    let opts: Opts = Opts::parse();

    // Load the settings files.
    let settings = Settings::load(&opts.settings_path).wrap_err("Failed to load settings")?;

    match &settings.cloud_provider {
        CloudProvider::Aws => {
            // Create the client for the cloud provider.
            let client = AwsClient::new(settings.clone()).await;

            // Execute the command.
            run(settings, client, opts).await
        }
        CloudProvider::Vultr => {
            // Create the client for the cloud provider.
            let token = settings
                .load_token()
                .wrap_err("Failed to load cloud provider's token")?;
            let client = VultrClient::new(token, settings.clone());

            // Execute the command.
            run(settings, client, opts).await
        }
    }
}

async fn run<C: ServerProviderClient>(
    settings: Settings,
    client: C,
    opts: Opts,
) -> eyre::Result<()> {
    // Create a new testbed.
    let mut testbed = Testbed::new(settings.clone(), client)
        .await
        .wrap_err("Failed to crate testbed")?;

    match opts.operation {
        Operation::Testbed { action } => match action {
            // Display the current status of the testbed.
            TestbedAction::Status => testbed.status(),

            // Deploy the specified number of instances on the testbed.
            TestbedAction::Deploy { instances, region } => testbed
                .deploy(instances, region)
                .await
                .wrap_err("Failed to deploy testbed")?,

            // Start the specified number of instances on an existing testbed.
            TestbedAction::Start { instances } => testbed
                .start(instances)
                .await
                .wrap_err("Failed to start testbed")?,

            // Stop an existing testbed.
            TestbedAction::Stop => testbed.stop().await.wrap_err("Failed to stop testbed")?,

            // Destroy the testbed and terminal all instances.
            TestbedAction::Destroy => testbed
                .destroy()
                .await
                .wrap_err("Failed to destroy testbed")?,
        },

        // Run benchmarks.
        Operation::Benchmark => {
            todo!("Alberto: Implement the benchmark operation. (#222)")
        }
    }
    Ok(())
}

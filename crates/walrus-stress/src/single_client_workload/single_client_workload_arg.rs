// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Command line arguments for the single client workload.

use clap::Parser;

use super::single_client_workload_config::{
    RequestTypeDistributionConfig,
    SizeDistributionConfig,
    StoreLengthDistributionConfig,
};

#[derive(Parser, Debug, Clone)]
#[command(rename_all = "kebab-case")]
pub struct SingleClientWorkloadArgs {
    /// Target requests per minute
    #[arg(long, default_value_t = 5)]
    pub target_requests_per_minute: u64,
    /// Check read result
    #[arg(long, default_value_t = true)]
    pub check_read_result: bool,
    // Define the distribution of request types: read, write, delete, extend
    #[command(flatten)]
    pub request_type_distribution: RequestTypeDistributionArgs,
    // Define the workload configuration including size and store length distributions
    #[command(subcommand)]
    pub workload_config: WorkloadConfig,
}

#[derive(clap::Args, Debug, Clone)]
pub struct RequestTypeDistributionArgs {
    /// Weight for read requests
    #[arg(long, default_value_t = 20)]
    pub read_weight: u32,
    /// Weight for permanent write requests
    #[arg(long, default_value_t = 5)]
    pub write_permanent_weight: u32,
    /// Weight for deletable write requests
    #[arg(long, default_value_t = 5)]
    pub write_deletable_weight: u32,
    /// Weight for delete requests
    #[arg(long, default_value_t = 1)]
    pub delete_weight: u32,
    /// Weight for extend requests
    #[arg(long, default_value_t = 1)]
    pub extend_weight: u32,
    // TODO: add inconsistent blob weight.
}

impl RequestTypeDistributionArgs {
    /// Convert to configuration for use by the load generator
    pub fn to_config(&self) -> RequestTypeDistributionConfig {
        RequestTypeDistributionConfig {
            read_weight: self.read_weight,
            write_permanent_weight: self.write_permanent_weight,
            write_deletable_weight: self.write_deletable_weight,
            delete_weight: self.delete_weight,
            extend_weight: self.extend_weight,
        }
    }
}

#[derive(clap::Subcommand, Debug, Clone)]
pub enum WorkloadConfig {
    /// Configure workload with uniform size distribution
    UniformSize {
        /// Minimum size in bytes (log2)
        #[arg(long, default_value_t = 1024)]
        min_size_bytes: u64,
        /// Maximum size in bytes
        #[arg(long, default_value_t = 20 * 1024 * 1024)]
        max_size_bytes: u64,
        /// Store length distribution configuration
        #[command(subcommand)]
        store_length_distribution: RequestStoreLengthDistributionArgs,
    },
    /// Configure workload with Poisson size distribution
    PoissonSize {
        /// Lambda parameter for Poisson distribution
        #[arg(long, default_value_t = 100.0)]
        lambda: f64,
        /// Size multiplier to scale the Poisson values
        #[arg(long, default_value_t = 1024)]
        size_multiplier: u32,
        /// Store length distribution configuration
        #[command(subcommand)]
        store_length_distribution: RequestStoreLengthDistributionArgs,
    },
}

impl WorkloadConfig {
    /// Get the size distribution configuration
    pub fn get_size_config(&self) -> SizeDistributionConfig {
        match self {
            WorkloadConfig::UniformSize {
                min_size_bytes,
                max_size_bytes,
                ..
            } => SizeDistributionConfig::Uniform {
                min_size_bytes: *min_size_bytes as usize,
                max_size_bytes: *max_size_bytes as usize,
            },
            WorkloadConfig::PoissonSize {
                lambda,
                size_multiplier,
                ..
            } => SizeDistributionConfig::Poisson {
                lambda: *lambda,
                size_multiplier: *size_multiplier,
            },
        }
    }

    /// Get the store length distribution configuration
    pub fn get_store_length_config(&self) -> StoreLengthDistributionConfig {
        match self {
            WorkloadConfig::UniformSize {
                store_length_distribution,
                ..
            }
            | WorkloadConfig::PoissonSize {
                store_length_distribution,
                ..
            } => store_length_distribution.get_store_length_config(),
        }
    }
}

#[derive(clap::Subcommand, Debug, Clone)]
pub enum RequestStoreLengthDistributionArgs {
    /// Use uniform store length distribution with min and max epoch bounds
    Uniform {
        /// Minimum store length in epochs
        #[arg(long, default_value_t = 1)]
        min_store_epochs: u32,
        /// Maximum store length in epochs
        #[arg(long, default_value_t = 10)]
        max_store_epochs: u32,
    },
    /// Use Poisson distribution for store lengths
    Poisson {
        /// Lambda parameter for Poisson distribution of store lengths
        #[arg(long, default_value_t = 5.0)]
        store_lambda: f64,
        /// Minimum store length to add to Poisson result
        #[arg(long, default_value_t = 1)]
        store_base_epochs: u32,
    },
}

impl RequestStoreLengthDistributionArgs {
    /// Get the configuration for this store length distribution
    pub fn get_store_length_config(&self) -> StoreLengthDistributionConfig {
        match self {
            RequestStoreLengthDistributionArgs::Uniform {
                min_store_epochs,
                max_store_epochs,
            } => StoreLengthDistributionConfig::Uniform {
                min_epochs: *min_store_epochs,
                max_epochs: *max_store_epochs,
            },
            RequestStoreLengthDistributionArgs::Poisson {
                store_lambda,
                store_base_epochs,
            } => StoreLengthDistributionConfig::Poisson {
                lambda: *store_lambda,
                base_epochs: *store_base_epochs,
            },
        }
    }
}

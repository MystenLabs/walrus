// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Command line arguments for the single client workload.

use clap::Parser;

use super::single_client_workload_config::{
    RequestTypeDistributionConfig, SizeDistributionConfig, StoreLengthDistributionConfig,
};

/// Arguments for the single client workload.
#[derive(Parser, Debug, Clone)]
#[command(rename_all = "kebab-case")]
pub struct SingleClientWorkloadArgs {
    /// Target requests per minute
    #[arg(long, default_value_t = 5)]
    pub target_requests_per_minute: u64,
    /// Check read result
    #[arg(long, default_value_t = true)]
    pub check_read_result: bool,
    /// The percentage of write operations that will write the same data.
    #[arg(long, default_value = "0.1")]
    pub write_same_data_ratio: f64,
    /// The maximum number of blobs to keep in the blob pool. The limit needs to be set in a way
    /// that the pool will hold blob data in memory.
    #[arg(long, default_value_t = 10000)]
    pub max_blobs_in_pool: usize,
    /// The initial number of blobs to pre-create in the blob pool. This is useful for read-heavy
    /// workloads to ensure there are enough blobs to read from at the start.
    #[arg(long, default_value_t = 0)]
    pub initial_blobs_in_pool: usize,
    /// Define the distribution of request types: read, write, delete, extend
    #[command(flatten)]
    pub request_type_distribution: RequestTypeDistributionArgs,
    /// Define the workload configuration including size and store length distributions
    #[command(subcommand)]
    pub workload_config: WorkloadConfig,
}

/// Arguments for the request type distribution.
///
/// The ratio of each request type is the weight divided by the sum of all weights.
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
    // TODO(WAL-938): allow sending inconsistent blob data.
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

/// Arguments for the workload configuration.
#[derive(clap::Subcommand, Debug, Clone)]
pub enum WorkloadConfig {
    /// Configure workload with uniform size distribution.
    UniformSize {
        /// Minimum size in bytes (log2)
        #[arg(long, default_value_t = 1024)]
        min_size_bytes: usize,
        /// Maximum size in bytes
        #[arg(long, default_value_t = 20 * 1024 * 1024)]
        max_size_bytes: usize,
        /// Store length distribution configuration
        #[command(subcommand)]
        store_length_distribution: RequestStoreLengthDistributionArgs,
    },
    /// Configure workload with Poisson size distribution.
    ///
    PoissonSize {
        /// Lambda parameter for Poisson distribution
        #[arg(long, default_value_t = 100.0)]
        lambda: f64,
        /// Size multiplier to scale the Poisson values
        #[arg(long, default_value_t = 1024)]
        size_multiplier: usize,
        /// Store length distribution configuration
        #[command(subcommand)]
        store_length_distribution: RequestStoreLengthDistributionArgs,
    },
}

impl WorkloadConfig {
    /// Get the size distribution configuration.
    pub fn get_size_config(&self) -> SizeDistributionConfig {
        match self {
            WorkloadConfig::UniformSize {
                min_size_bytes,
                max_size_bytes,
                ..
            } => SizeDistributionConfig::Uniform {
                min_size_bytes: *min_size_bytes,
                max_size_bytes: *max_size_bytes,
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

/// Arguments for the store length distribution.
#[derive(clap::Subcommand, Debug, Clone)]
pub enum RequestStoreLengthDistributionArgs {
    /// Use uniform store length distribution with min and max epoch bounds
    UniformStoreLength {
        /// Minimum store length in epochs
        #[arg(long, default_value_t = 1)]
        min_store_epochs: u32,
        /// Maximum store length in epochs
        #[arg(long, default_value_t = 10)]
        max_store_epochs: u32,
    },
    /// Use Poisson distribution for store lengths
    PoissonStoreLength {
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
            RequestStoreLengthDistributionArgs::UniformStoreLength {
                min_store_epochs,
                max_store_epochs,
            } => StoreLengthDistributionConfig::Uniform {
                min_epochs: *min_store_epochs,
                max_epochs: *max_store_epochs,
            },
            RequestStoreLengthDistributionArgs::PoissonStoreLength {
                store_lambda,
                store_base_epochs,
            } => StoreLengthDistributionConfig::Poisson {
                lambda: *store_lambda,
                base_epochs: *store_base_epochs,
            },
        }
    }
}

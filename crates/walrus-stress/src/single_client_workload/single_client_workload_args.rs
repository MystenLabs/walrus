// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Arguments for the single client workload.

use clap::Parser;

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
}

impl RequestTypeDistributionArgs {
    /// Calculate the total weight across all request types
    pub fn total_weight(&self) -> u32 {
        self.read_weight
            + self.write_permanent_weight
            + self.write_deletable_weight
            + self.delete_weight
            + self.extend_weight
    }

    /// Get the probability/ratio for read requests
    pub fn read_ratio(&self) -> f64 {
        self.read_weight as f64 / self.total_weight() as f64
    }

    /// Get the probability/ratio for permanent write requests
    pub fn write_permanent_ratio(&self) -> f64 {
        self.write_permanent_weight as f64 / self.total_weight() as f64
    }

    /// Get the probability/ratio for deletable write requests
    pub fn write_deletable_ratio(&self) -> f64 {
        self.write_deletable_weight as f64 / self.total_weight() as f64
    }

    /// Get the probability/ratio for delete requests
    pub fn delete_ratio(&self) -> f64 {
        self.delete_weight as f64 / self.total_weight() as f64
    }

    /// Get the probability/ratio for extend requests
    pub fn extend_ratio(&self) -> f64 {
        self.extend_weight as f64 / self.total_weight() as f64
    }

    /// Validate that at least one weight is non-zero
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.total_weight() == 0 {
            anyhow::bail!("At least one request type weight must be greater than 0");
        }
        Ok(())
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

    /// Validate both size and store length distribution parameters
    pub fn validate(&self) -> anyhow::Result<()> {
        // Validate size distribution
        match self {
            WorkloadConfig::UniformSize {
                min_size_bytes,
                max_size_bytes,
                ..
            } => {
                if min_size_bytes > max_size_bytes {
                    anyhow::bail!("min_size_bytes must be less or equal to max_size_bytes");
                }
            }
            WorkloadConfig::PoissonSize { lambda, .. } => {
                if *lambda <= 0.0 {
                    anyhow::bail!("lambda must be positive for Poisson distribution");
                }
            }
        }

        // Validate store length distribution
        match self {
            WorkloadConfig::UniformSize {
                store_length_distribution,
                ..
            }
            | WorkloadConfig::PoissonSize {
                store_length_distribution,
                ..
            } => {
                store_length_distribution.validate()?;
            }
        }

        Ok(())
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

    /// Validate the distribution parameters based on the selected type
    pub fn validate(&self) -> anyhow::Result<()> {
        match self {
            RequestStoreLengthDistributionArgs::Uniform {
                min_store_epochs,
                max_store_epochs,
            } => {
                if min_store_epochs > max_store_epochs {
                    anyhow::bail!("min_store_epochs must be less or equal to max_store_epochs");
                }
                if *min_store_epochs == 0 {
                    anyhow::bail!("min_store_epochs must be at least 1");
                }
            }
            RequestStoreLengthDistributionArgs::Poisson {
                store_lambda,
                store_base_epochs,
            } => {
                if *store_lambda <= 0.0 {
                    anyhow::bail!("store_lambda must be positive for Poisson distribution");
                }
                if *store_base_epochs == 0 {
                    anyhow::bail!("store_base_epochs must be at least 1");
                }
            }
        }
        Ok(())
    }
}

/// Configuration for size distribution that can be used by the load generator
#[derive(Debug, Clone)]
pub enum SizeDistributionConfig {
    /// Use uniform size distribution with min and max size bounds
    Uniform {
        min_size_bytes: u64,
        max_size_bytes: u64,
    },
    /// Use Poisson distribution for request sizes
    Poisson { lambda: f64, size_multiplier: u32 },
}

/// Configuration for store length distribution that can be used by the load generator
#[derive(Debug, Clone)]
pub enum StoreLengthDistributionConfig {
    Uniform {
        min_epochs: u32,
        max_epochs: u32,
    },
    Poisson {
        lambda: f64,
        base_epochs: u32, // Added to Poisson result to ensure minimum store length
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_failure_with_all_weights_zero() {
        let args = RequestTypeDistributionArgs {
            read_weight: 0,
            write_permanent_weight: 0,
            write_deletable_weight: 0,
            delete_weight: 0,
            extend_weight: 0,
        };
        let result = args.validate();
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "At least one request type weight must be greater than 0"
        );
    }

    #[test]
    fn test_ratio_calculations() {
        let args = RequestTypeDistributionArgs {
            read_weight: 10,
            write_permanent_weight: 5,
            write_deletable_weight: 3,
            delete_weight: 1,
            extend_weight: 1,
        };

        assert_eq!(args.read_ratio(), 0.5);
        assert_eq!(args.write_permanent_ratio(), 0.25);
        assert_eq!(args.write_deletable_ratio(), 0.15);
        assert_eq!(args.delete_ratio(), 0.05);
        assert_eq!(args.extend_ratio(), 0.05);
    }

    #[test]
    fn test_uniform_size_validate_success() {
        let args = WorkloadConfig::UniformSize {
            min_size_bytes: 1024,
            max_size_bytes: 2048,
            store_length_distribution: RequestStoreLengthDistributionArgs::Uniform {
                min_store_epochs: 1,
                max_store_epochs: 10,
            },
        };
        assert!(args.validate().is_ok());
    }

    #[test]
    fn test_uniform_size_validate_success_equal_sizes() {
        let args = WorkloadConfig::UniformSize {
            min_size_bytes: 1024,
            max_size_bytes: 1024,
            store_length_distribution: RequestStoreLengthDistributionArgs::Uniform {
                min_store_epochs: 1,
                max_store_epochs: 10,
            },
        };
        assert!(args.validate().is_ok());
    }

    #[test]
    fn test_uniform_size_validate_failure_min_greater_than_max() {
        let args = WorkloadConfig::UniformSize {
            min_size_bytes: 2048,
            max_size_bytes: 1024,
            store_length_distribution: RequestStoreLengthDistributionArgs::Uniform {
                min_store_epochs: 1,
                max_store_epochs: 10,
            },
        };
        let result = args.validate();
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "min_size_bytes must be less or equal to max_size_bytes"
        );
    }

    #[test]
    fn test_poisson_size_validate_success() {
        let args = WorkloadConfig::PoissonSize {
            lambda: 5.0,
            size_multiplier: 1024,
            store_length_distribution: RequestStoreLengthDistributionArgs::Poisson {
                store_lambda: 3.0,
                store_base_epochs: 1,
            },
        };
        assert!(args.validate().is_ok());
    }

    #[test]
    fn test_poisson_size_validate_failure_zero_lambda() {
        let args = WorkloadConfig::PoissonSize {
            lambda: 0.0,
            size_multiplier: 1024,
            store_length_distribution: RequestStoreLengthDistributionArgs::Poisson {
                store_lambda: 3.0,
                store_base_epochs: 1,
            },
        };
        let result = args.validate();
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "lambda must be positive for Poisson distribution"
        );
    }

    #[test]
    fn test_workload_config_validates_store_length_distribution() {
        // Test that WorkloadConfig.validate() correctly propagates store length validation errors
        let args = WorkloadConfig::UniformSize {
            min_size_bytes: 1024,
            max_size_bytes: 2048,
            store_length_distribution: RequestStoreLengthDistributionArgs::Uniform {
                min_store_epochs: 10,
                max_store_epochs: 5, // Invalid: min > max
            },
        };
        let result = args.validate();
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "min_store_epochs must be less or equal to max_store_epochs"
        );
    }

    #[test]
    fn test_uniform_validate_success_equal_epochs() {
        let args = RequestStoreLengthDistributionArgs::Uniform {
            min_store_epochs: 5,
            max_store_epochs: 5,
        };
        assert!(args.validate().is_ok());
    }

    #[test]
    fn test_uniform_validate_failure_min_greater_than_max() {
        let args = RequestStoreLengthDistributionArgs::Uniform {
            min_store_epochs: 10,
            max_store_epochs: 5,
        };
        let result = args.validate();
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "min_store_epochs must be less or equal to max_store_epochs"
        );
    }

    #[test]
    fn test_uniform_validate_failure_zero_min_epochs() {
        let args = RequestStoreLengthDistributionArgs::Uniform {
            min_store_epochs: 0,
            max_store_epochs: 10,
        };
        let result = args.validate();
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "min_store_epochs must be at least 1"
        );
    }

    #[test]
    fn test_poisson_validate_failure_zero_base_epochs() {
        let args = RequestStoreLengthDistributionArgs::Poisson {
            store_lambda: 3.0,
            store_base_epochs: 0,
        };
        let result = args.validate();
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "store_base_epochs must be at least 1"
        );
    }
}

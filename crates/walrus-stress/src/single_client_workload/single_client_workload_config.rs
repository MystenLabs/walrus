// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Configuration for the single client workload.

/// Configuration for request type distribution that can be used by the load generator
#[derive(Debug, Clone)]
pub struct RequestTypeDistributionConfig {
    pub read_weight: u32,
    pub write_permanent_weight: u32,
    pub write_deletable_weight: u32,
    pub delete_weight: u32,
    pub extend_weight: u32,
}

impl RequestTypeDistributionConfig {
    /// Calculate the total weight across all request types
    pub fn total_weight(&self) -> u32 {
        self.read_weight
            + self.write_permanent_weight
            + self.write_deletable_weight
            + self.delete_weight
            + self.extend_weight
    }

    /// Validate that at least one weight is greater than 0
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.total_weight() == 0 {
            anyhow::bail!("At least one request type weight must be greater than 0");
        }
        Ok(())
    }

    /// Get the probability ratio for each request type
    pub fn read_ratio(&self) -> f64 {
        self.read_weight as f64 / self.total_weight() as f64
    }

    pub fn write_permanent_ratio(&self) -> f64 {
        self.write_permanent_weight as f64 / self.total_weight() as f64
    }

    pub fn write_deletable_ratio(&self) -> f64 {
        self.write_deletable_weight as f64 / self.total_weight() as f64
    }

    pub fn delete_ratio(&self) -> f64 {
        self.delete_weight as f64 / self.total_weight() as f64
    }

    pub fn extend_ratio(&self) -> f64 {
        self.extend_weight as f64 / self.total_weight() as f64
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

impl SizeDistributionConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        match self {
            SizeDistributionConfig::Uniform {
                min_size_bytes,
                max_size_bytes,
            } => {
                if min_size_bytes > max_size_bytes {
                    anyhow::bail!("min_size_bytes must be less or equal to max_size_bytes");
                }
                Ok(())
            }
            SizeDistributionConfig::Poisson {
                lambda,
                size_multiplier,
            } => {
                if *lambda <= 0.0 {
                    anyhow::bail!("lambda must be positive for Poisson distribution");
                }
                if *size_multiplier == 0 {
                    anyhow::bail!("size_multiplier must be at least 1");
                }
                Ok(())
            }
        }
    }
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

impl StoreLengthDistributionConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        match self {
            StoreLengthDistributionConfig::Uniform {
                min_epochs,
                max_epochs,
            } => {
                if min_epochs > max_epochs {
                    anyhow::bail!("min_epochs must be less or equal to max_epochs");
                }
                if *min_epochs == 0 {
                    anyhow::bail!("min_epochs must be at least 1");
                }
                Ok(())
            }
            StoreLengthDistributionConfig::Poisson {
                lambda,
                base_epochs,
            } => {
                if *lambda <= 0.0 {
                    anyhow::bail!("lambda must be positive for Poisson distribution");
                }
                if *base_epochs == 0 {
                    anyhow::bail!("base_epochs must be at least 1");
                }
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_failure_with_all_weights_zero() {
        let config = RequestTypeDistributionConfig {
            read_weight: 0,
            write_permanent_weight: 0,
            write_deletable_weight: 0,
            delete_weight: 0,
            extend_weight: 0,
        };
        let result = config.validate();
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "At least one request type weight must be greater than 0"
        );
    }

    #[test]
    fn test_ratio_calculations() {
        let config = RequestTypeDistributionConfig {
            read_weight: 10,
            write_permanent_weight: 5,
            write_deletable_weight: 3,
            delete_weight: 1,
            extend_weight: 1,
        };

        assert_eq!(config.read_ratio(), 0.5);
        assert_eq!(config.write_permanent_ratio(), 0.25);
        assert_eq!(config.write_deletable_ratio(), 0.15);
        assert_eq!(config.delete_ratio(), 0.05);
        assert_eq!(config.extend_ratio(), 0.05);
    }

    #[test]
    fn test_request_type_distribution_config_validate_success() {
        let config = RequestTypeDistributionConfig {
            read_weight: 10,
            write_permanent_weight: 5,
            write_deletable_weight: 3,
            delete_weight: 1,
            extend_weight: 1,
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_request_type_distribution_config_validate_failure() {
        let config = RequestTypeDistributionConfig {
            read_weight: 0,
            write_permanent_weight: 0,
            write_deletable_weight: 0,
            delete_weight: 0,
            extend_weight: 0,
        };
        let result = config.validate();
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "At least one request type weight must be greater than 0"
        );
    }

    #[test]
    fn test_request_type_distribution_config_ratios() {
        let config = RequestTypeDistributionConfig {
            read_weight: 10,
            write_permanent_weight: 5,
            write_deletable_weight: 3,
            delete_weight: 1,
            extend_weight: 1,
        };

        assert_eq!(config.read_ratio(), 0.5);
        assert_eq!(config.write_permanent_ratio(), 0.25);
        assert_eq!(config.write_deletable_ratio(), 0.15);
        assert_eq!(config.delete_ratio(), 0.05);
        assert_eq!(config.extend_ratio(), 0.05);
    }

    #[test]
    fn test_uniform_size_validate_success() {
        let config = SizeDistributionConfig::Uniform {
            min_size_bytes: 1024,
            max_size_bytes: 2048,
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_uniform_size_validate_success_equal_sizes() {
        let config = SizeDistributionConfig::Uniform {
            min_size_bytes: 1024,
            max_size_bytes: 1024,
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_uniform_size_validate_failure_min_greater_than_max() {
        let config = SizeDistributionConfig::Uniform {
            min_size_bytes: 2048,
            max_size_bytes: 1024,
        };
        let result = config.validate();
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "min_size_bytes must be less or equal to max_size_bytes"
        );
    }

    #[test]
    fn test_poisson_size_validate_success() {
        let config = SizeDistributionConfig::Poisson {
            lambda: 5.0,
            size_multiplier: 1024,
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_poisson_size_validate_failure_zero_lambda() {
        let config = SizeDistributionConfig::Poisson {
            lambda: 0.0,
            size_multiplier: 1024,
        };
        let result = config.validate();
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "lambda must be positive for Poisson distribution"
        );
    }

    #[test]
    fn test_workload_config_validates_store_length_distribution() {
        // Test that WorkloadConfig.validate() correctly propagates store length validation errors
        let config = StoreLengthDistributionConfig::Uniform {
            min_epochs: 10,
            max_epochs: 5,
        };
        let result = config.validate();
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "min_epochs must be less or equal to max_epochs"
        );
    }

    #[test]
    fn test_uniform_validate_success_equal_epochs() {
        let config = StoreLengthDistributionConfig::Uniform {
            min_epochs: 5,
            max_epochs: 5,
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_uniform_validate_failure_min_greater_than_max() {
        let config = StoreLengthDistributionConfig::Uniform {
            min_epochs: 10,
            max_epochs: 5,
        };
        let result = config.validate();
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "min_epochs must be less or equal to max_epochs"
        );
    }

    #[test]
    fn test_uniform_validate_failure_zero_min_epochs() {
        let config = StoreLengthDistributionConfig::Uniform {
            min_epochs: 0,
            max_epochs: 10,
        };
        let result = config.validate();
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "min_epochs must be at least 1"
        );
    }

    #[test]
    fn test_poisson_validate_failure_zero_base_epochs() {
        let config = StoreLengthDistributionConfig::Poisson {
            lambda: 3.0,
            base_epochs: 0,
        };
        let result = config.validate();
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "base_epochs must be at least 1"
        );
    }
}

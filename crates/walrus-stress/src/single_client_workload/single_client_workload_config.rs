// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Configuration for the single client workload.

use rand::Rng;
use walrus_sui::test_utils::system_setup::DEFAULT_MAX_EPOCHS_AHEAD;

/// Configuration for request type distribution that can be used by the load generator
#[derive(Debug, Clone)]
pub struct RequestTypeDistributionConfig {
    /// The weight for read requests.
    pub read_weight: u32,
    /// The weight for write permanent requests.
    pub write_permanent_weight: u32,
    /// The weight for write deletable requests.
    pub write_deletable_weight: u32,
    /// The weight for delete requests.
    pub delete_weight: u32,
    /// The weight for extend requests.
    pub extend_weight: u32,
}

/// The type of request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestType {
    /// Read request.
    Read,
    /// Write permanent request.
    WritePermanent,
    /// Write deletable request.
    WriteDeletable,
    /// Delete request.
    Delete,
    /// Extend request.
    Extend,
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
            anyhow::bail!("at least one request type weight must be greater than 0");
        }
        Ok(())
    }

    /// Sample a request type based on the weight distribution.
    pub fn sample<R: Rng>(&self, rng: &mut R) -> RequestType {
        let total_weight = self.total_weight();
        let random_value = rng.gen_range(0..total_weight);
        if random_value < self.read_weight {
            RequestType::Read
        } else if random_value < self.read_weight + self.write_permanent_weight {
            RequestType::WritePermanent
        } else if random_value
            < self.read_weight + self.write_permanent_weight + self.write_deletable_weight
        {
            RequestType::WriteDeletable
        } else if random_value
            < self.read_weight
                + self.write_permanent_weight
                + self.write_deletable_weight
                + self.delete_weight
        {
            RequestType::Delete
        } else {
            RequestType::Extend
        }
    }
}

/// Configuration for size distribution that can be used by the load generator
#[derive(Debug, Clone)]
pub enum SizeDistributionConfig {
    /// Use uniform size distribution with min and max size bounds
    Uniform {
        /// The minimum size in bytes.
        min_size_bytes: usize,
        /// The maximum size in bytes.
        max_size_bytes: usize,
    },
    /// Use Poisson distribution for request sizes
    Poisson {
        /// The lambda parameter for Poisson distribution of request sizes.
        lambda: f64,
        /// The size multiplier for Poisson distribution of request sizes.
        size_multiplier: u32,
    },
}

impl SizeDistributionConfig {
    /// Validates the size distribution configuration.
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
    /// Use uniform store length distribution with min and max epoch bounds
    Uniform {
        /// The minimum store length in epochs.
        min_epochs: u32,
        /// The maximum store length in epochs.
        max_epochs: u32,
    },
    /// Use Poisson distribution for store lengths
    Poisson {
        /// The lambda parameter for Poisson distribution of store lengths.
        lambda: f64,
        /// The minimum store length to add to Poisson result.
        base_epochs: u32,
    },
}

impl StoreLengthDistributionConfig {
    /// Validates the store length distribution configuration.
    ///
    /// Returns an error if the configuration is invalid.
    pub fn validate(&self) -> anyhow::Result<()> {
        match self {
            StoreLengthDistributionConfig::Uniform {
                min_epochs,
                max_epochs,
            } => {
                if min_epochs > max_epochs {
                    anyhow::bail!("min_epochs must be less or equal to max_epochs");
                }
                if *min_epochs < 1 {
                    anyhow::bail!("min_epochs must be at least 1");
                }
                if *max_epochs > DEFAULT_MAX_EPOCHS_AHEAD {
                    anyhow::bail!(
                        "max_epochs must be less or equal to {}",
                        DEFAULT_MAX_EPOCHS_AHEAD
                    );
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
                if *lambda > f64::from(DEFAULT_MAX_EPOCHS_AHEAD) {
                    anyhow::bail!(
                        "lambda must be less or equal to {}",
                        DEFAULT_MAX_EPOCHS_AHEAD
                    );
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
            "at least one request type weight must be greater than 0"
        );
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
            "at least one request type weight must be greater than 0"
        );
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

    // max_epoch must be less than max epochs ahead
    #[test]
    fn test_uniform_validate_failure_max_epoch_greater_than_max_epochs_ahead() {
        let config = StoreLengthDistributionConfig::Uniform {
            min_epochs: 1,
            max_epochs: 100,
        };
        let result = config.validate();
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "max_epochs must be less or equal to 53"
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

    #[test]
    fn test_poisson_validate_failure_lambda_greater_than_max_epochs_ahead() {
        let config = StoreLengthDistributionConfig::Poisson {
            lambda: 100.0,
            base_epochs: 1,
        };
        let result = config.validate();
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "lambda must be less or equal to 53"
        );
    }
}

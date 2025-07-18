// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Blob generator.

use rand::Rng;
use rand_distr::{Distribution, Poisson};
use walrus_test_utils::random_data_from_rng;

use super::single_client_workload_config::SizeDistributionConfig;

/// A trait for generating random blobs.
pub trait RandomBlobGenerator {
    fn generate_blob<R: Rng>(&self, rng: &mut R) -> Vec<u8>;
}

pub enum BlobGenerator {
    Uniform(UniformBlobGenerator),
    Poisson(PoissonBlobGenerator),
}

impl BlobGenerator {
    pub fn new(size_distribution_config: SizeDistributionConfig) -> Self {
        match size_distribution_config {
            SizeDistributionConfig::Uniform {
                min_size_bytes,
                max_size_bytes,
            } => Self::Uniform(UniformBlobGenerator {
                min_size_bytes,
                max_size_bytes,
            }),
            SizeDistributionConfig::Poisson {
                lambda,
                size_multiplier,
            } => Self::Poisson(PoissonBlobGenerator::new(lambda, size_multiplier as usize)),
        }
    }

    pub fn generate_blob<R: Rng>(&self, rng: &mut R) -> Vec<u8> {
        match self {
            Self::Uniform(generator) => generator.generate_blob(rng),
            Self::Poisson(generator) => generator.generate_blob(rng),
        }
    }
}

pub(crate) struct UniformBlobGenerator {
    min_size_bytes: usize,
    max_size_bytes: usize,
}

impl RandomBlobGenerator for UniformBlobGenerator {
    fn generate_blob<R: Rng>(&self, rng: &mut R) -> Vec<u8> {
        let size = rng.gen_range(self.min_size_bytes..=self.max_size_bytes);
        random_data_from_rng(size, rng)
    }
}

pub(crate) struct PoissonBlobGenerator {
    poisson: Poisson<f64>,
    size_multiplier: usize,
}

impl PoissonBlobGenerator {
    pub fn new(lambda: f64, size_multiplier: usize) -> Self {
        let poisson = Poisson::new(lambda).expect("lambda must be positive");
        Self {
            poisson,
            size_multiplier,
        }
    }
}

impl RandomBlobGenerator for PoissonBlobGenerator {
    fn generate_blob<R: Rng>(&self, rng: &mut R) -> Vec<u8> {
        let size_base = (self.poisson.sample(rng).round() as usize).max(1);
        let size = size_base * self.size_multiplier;
        random_data_from_rng(size, rng)
    }
}

// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Epoch length generator.

use rand::Rng;
use rand_distr::{Distribution, Poisson};
use walrus_core::EpochCount;
use walrus_sui::test_utils::system_setup::DEFAULT_MAX_EPOCHS_AHEAD;

use super::single_client_workload_config::StoreLengthDistributionConfig;

pub trait RandomEpochLengthGenerator {
    fn generate_epoch_length<R: Rng>(&self, rng: &mut R) -> EpochCount;
}

pub enum EpochLengthGenerator {
    Uniform(UniformEpochLengthGenerator),
    Poisson(PoissonEpochLengthGenerator),
}

impl EpochLengthGenerator {
    pub fn new(epoch_length_config: StoreLengthDistributionConfig) -> Self {
        match epoch_length_config {
            StoreLengthDistributionConfig::Uniform {
                min_epochs,
                max_epochs,
            } => Self::Uniform(UniformEpochLengthGenerator {
                min_epochs,
                max_epochs,
            }),
            StoreLengthDistributionConfig::Poisson {
                lambda,
                base_epochs,
            } => Self::Poisson(PoissonEpochLengthGenerator::new(lambda, base_epochs)),
        }
    }

    pub fn generate_epoch_length<R: Rng>(&self, rng: &mut R) -> EpochCount {
        match self {
            Self::Uniform(generator) => generator.generate_epoch_length(rng),
            Self::Poisson(generator) => generator.generate_epoch_length(rng),
        }
    }
}

pub(crate) struct UniformEpochLengthGenerator {
    min_epochs: EpochCount,
    max_epochs: EpochCount,
}

impl RandomEpochLengthGenerator for UniformEpochLengthGenerator {
    fn generate_epoch_length<R: Rng>(&self, rng: &mut R) -> EpochCount {
        let epoch_length = rng.gen_range(self.min_epochs..=self.max_epochs);
        epoch_length.max(1).min(DEFAULT_MAX_EPOCHS_AHEAD)
    }
}

pub(crate) struct PoissonEpochLengthGenerator {
    poisson: Poisson<f64>,
    base_epochs: EpochCount,
}

impl PoissonEpochLengthGenerator {
    pub fn new(lambda: f64, base_epochs: EpochCount) -> Self {
        let poisson = Poisson::new(lambda).expect("lambda must be positive");
        Self {
            poisson,
            base_epochs,
        }
    }
}

impl RandomEpochLengthGenerator for PoissonEpochLengthGenerator {
    fn generate_epoch_length<R: Rng>(&self, rng: &mut R) -> EpochCount {
        let epoch_length = self.base_epochs + self.poisson.sample(rng).round() as EpochCount;
        epoch_length.max(1).min(DEFAULT_MAX_EPOCHS_AHEAD)
    }
}

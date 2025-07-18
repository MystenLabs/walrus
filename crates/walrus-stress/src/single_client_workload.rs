// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Single client workload.

use single_client_workload_config::{
    RequestTypeDistributionConfig,
    SizeDistributionConfig,
    StoreLengthDistributionConfig,
};

pub(crate) mod single_client_workload_arg;
pub(crate) mod single_client_workload_config;

pub struct SingleClientWorkload {
    data_size_config: SizeDistributionConfig,
    store_length_config: StoreLengthDistributionConfig,
    request_type_distribution: RequestTypeDistributionConfig,
}

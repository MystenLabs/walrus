// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! The logic for the fan-out proxy tipping system.

mod check;
mod config;
mod error;

pub(crate) use check::TipChecker;
pub(crate) use config::TipConfig;
pub(crate) use error::TipError;

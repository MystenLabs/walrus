// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use core::fmt;
use std::{fmt::Display, ops::Div};

/// A wrapper around byte counts which allows printing the value as `<x>Ki`, `<x>Mi`, or `<x>Gi`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct ByteSize(usize);

impl ByteSize {
    pub const fn byte_value(self) -> usize {
        self.0
    }

    pub const fn kibi(value: usize) -> Self {
        Self(value << 10)
    }

    pub const fn mebi(value: usize) -> Self {
        Self(value << 20)
    }

    pub const fn gibi(value: usize) -> Self {
        Self(value << 30)
    }
}

impl Display for ByteSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (shift, suffix) = if self.0.trailing_zeros() >= 30 {
            (30, "Gi")
        } else if self.0.trailing_zeros() >= 20 {
            (20, "Mi")
        } else if self.0.trailing_zeros() >= 10 {
            (10, "Ki")
        } else {
            (0, "")
        };
        write!(f, "{}{}", self.0 >> shift, suffix)
    }
}

impl Div<usize> for ByteSize {
    type Output = Self;

    fn div(self, rhs: usize) -> Self::Output {
        Self(self.0 / rhs)
    }
}

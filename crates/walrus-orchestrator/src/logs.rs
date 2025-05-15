// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::cmp::max;

use crate::display;

/// A simple log analyzer counting the number of errors and panics.
#[derive(Default, PartialEq, Eq)]
pub struct LogsAnalyzer {
    /// The number of errors in the nodes' log files.
    pub node_errors: usize,
    /// Whether a node panicked.
    pub node_panic: bool,
    /// The number of errors in the clients' log files.
    pub client_errors: usize,
    /// Whether a client panicked.
    pub client_panic: bool,
}

impl PartialOrd for LogsAnalyzer {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for LogsAnalyzer {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        if self.node_panic
            || self.client_panic
            || self.client_errors > other.client_errors
            || self.node_errors > other.node_errors
        {
            std::cmp::Ordering::Greater
        } else {
            std::cmp::Ordering::Less
        }
    }
}

impl LogsAnalyzer {
    /// Deduce the number of clients errors from the logs.
    pub fn set_client_errors(&mut self, log: &str) {
        self.client_errors = max(self.client_errors, log.matches(" ERROR").count());
        self.client_panic = log.contains("panic");
    }

    /// Print a summary of the errors.
    pub fn print_summary(&self) {
        if self.node_panic {
            display::error("Node(s) panicked!");
        } else if self.client_panic {
            display::error("Client(s) panicked!");
        } else if self.node_errors != 0 || self.client_errors != 0 {
            display::newline();
            display::warn(format!(
                "Logs contain errors (node: {}, client: {})",
                self.node_errors, self.client_errors
            ));
        }
    }
}

// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Build script for the walrus-service crate.

use std::env;

use chrono::{DateTime, Utc};

fn main() {
    #[cfg(feature = "backup")]
    println!("cargo:rerun-if-changed=migrations");

    // Check the SOURCE_DATE_EPOCH environment variable to enable reproducible builds, see
    // https://reproducible-builds.org/docs/source-date-epoch/.
    let build_time = if let Ok(timestamp) = env::var("SOURCE_DATE_EPOCH") {
        DateTime::from_timestamp(
            timestamp
                .parse::<i64>()
                .expect("SOURCE_DATE_EPOCH must be set to a valid UNIX timestamp"),
            0,
        )
        .expect("SOURCE_DATE_EPOCH must be set to a valid UNIX timestamp")
    } else {
        Utc::now()
    };
    println!("cargo:rerun-if-env-changed=SOURCE_DATE_EPOCH");
    println!("cargo::rustc-env=BUILD_TIME={}", build_time.timestamp());
}

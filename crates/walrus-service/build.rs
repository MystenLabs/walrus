// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Build script for the walrus-service crate.
fn main() {
    #[cfg(feature = "backup")]
    println!("cargo:rerun-if-changed=migrations");
}

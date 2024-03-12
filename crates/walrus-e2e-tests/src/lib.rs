// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;

use sui_move_build::{
    build_from_resolution_graph,
    gather_published_ids,
    BuildConfig,
    PackageDependencies,
};

pub fn compile_package(package_path: PathBuf) -> (PackageDependencies, Vec<Vec<u8>>) {
    let mut build_config = BuildConfig::default();
    build_config.config.install_dir = Some(package_path.clone());
    build_config.config.lock_file = Some(package_path.join("Move.lock"));
    let resolution_graph = build_config
        .resolution_graph(&package_path)
        .expect("Resolution failed");
    let (_, dependencies) = gather_published_ids(&resolution_graph);
    let compiled_package =
        build_from_resolution_graph(package_path, resolution_graph, false, false).expect("");
    let compiled_modules = compiled_package.get_package_bytes(false);
    (dependencies, compiled_modules)
}

pub fn contract_path(contract: &str) -> anyhow::Result<PathBuf> {
    Ok(std::env::current_dir()?
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("contracts")
        .join(contract))
}

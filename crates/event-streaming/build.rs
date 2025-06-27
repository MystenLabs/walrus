// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{fs, path::PathBuf, process::Command};

const SUI_REPO_URL: &str = "https://github.com/MystenLabs/sui.git";
const SUI_TAG: &str = "testnet-v1.50.1";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let sui_proto_source = ensure_sui_proto_source()?;

    // Get the manifest directory (crate root) to resolve proto files relative to it
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")?;
    let crate_root = PathBuf::from(manifest_dir);

    let proto_file_names = vec!["proto/walrus/event/v1alpha/service.proto"];

    // Convert to full paths and check that our proto files exist
    let mut proto_files = Vec::new();
    for proto_file in &proto_file_names {
        let full_proto_path = crate_root.join(proto_file);
        if !full_proto_path.exists() {
            return Err(format!(
                "Proto file not found: {} (resolved to: {})",
                proto_file,
                full_proto_path.display()
            )
            .into());
        }
        proto_files.push(full_proto_path.to_string_lossy().to_string());
    }

    // Use protox to compile proto files without requiring system protoc
    let include_paths = vec![
        crate_root.join("proto").to_string_lossy().to_string(),
        sui_proto_source.to_string_lossy().to_string(),
    ];

    let file_descriptor_set = protox::compile(&proto_files, &include_paths)?;

    // Generate Rust code using prost-build
    let mut prost_config = prost_build::Config::new();
    prost_config.include_file("mod.rs");

    // Compile the file descriptor set
    prost_config.compile_fds(file_descriptor_set)?;

    // Tell cargo to rerun if proto files change
    println!(
        "cargo:rerun-if-changed={}",
        crate_root.join("proto").display()
    );

    Ok(())
}

fn ensure_sui_proto_source() -> Result<PathBuf, Box<dyn std::error::Error>> {
    // Always download Sui proto files from GitHub to ensure we have the exact version we need
    // This eliminates potential version mismatches and simplifies the build process
    download_sui_repo()
}

fn download_sui_repo() -> Result<PathBuf, Box<dyn std::error::Error>> {
    let out_dir = std::env::var("OUT_DIR")?;
    let sui_clone_dir = PathBuf::from(&out_dir).join(format!("sui_clone_{}", SUI_TAG));
    let sui_proto_path = sui_clone_dir.join("crates/sui-rpc-api/proto");

    cleanup_old_sui_caches(&out_dir)?;

    // Check if already cloned and up to date
    if sui_proto_path.exists() {
        println!(
            "cargo:warning=Using cached Sui proto files from: {} (tag: {})",
            sui_proto_path.display(),
            SUI_TAG
        );
        return Ok(sui_proto_path);
    }

    println!(
        "cargo:warning=Downloading Sui repository from GitHub (tag: {})...",
        SUI_TAG
    );

    // Remove existing clone if it exists but is incomplete
    if sui_clone_dir.exists() {
        fs::remove_dir_all(&sui_clone_dir)?;
    }

    // Clone the specific tag with shallow depth for faster download
    let output = Command::new("git")
        .args([
            "clone",
            "--depth=1",
            "--branch",
            SUI_TAG,
            SUI_REPO_URL,
            &sui_clone_dir.to_string_lossy(),
        ])
        .output()?;

    if !output.status.success() {
        return Err(format!(
            "Failed to clone Sui repository: {}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }

    // Verify the proto files exist
    if !sui_proto_path.exists() {
        return Err("Cloned Sui repository doesn't contain expected proto files".into());
    }

    println!(
        "cargo:warning=Successfully downloaded Sui proto files to: {} (tag: {})",
        sui_proto_path.display(),
        SUI_TAG
    );
    Ok(sui_proto_path)
}

fn cleanup_old_sui_caches(out_dir: &str) -> Result<(), Box<dyn std::error::Error>> {
    let out_path = PathBuf::from(out_dir);
    let current_cache_name = format!("sui_clone_{}", SUI_TAG);

    // Read the directory and remove old sui_clone_* directories
    if let Ok(entries) = fs::read_dir(&out_path) {
        for entry in entries.flatten() {
            if let Some(name) = entry.file_name().to_str() {
                if name.starts_with("sui_clone_") && name != current_cache_name {
                    if let Err(e) = fs::remove_dir_all(entry.path()) {
                        println!("cargo:warning=Failed to clean up old cache {}: {}", name, e);
                    } else {
                        println!("cargo:warning=Cleaned up old Sui cache: {}", name);
                    }
                }
            }
        }
    }
    Ok(())
}

// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Walrus Sui build script. Currently only used to generate error definitions based on the
//! Move errors.

use std::{collections::HashSet, convert::identity, env, fs, path::Path};

use inflections::Inflect;
use regex::Regex;
use walkdir::WalkDir;

struct ModuleErrorDefs {
    fully_qualified_module: String,
    error_defs: Vec<(String, u64)>,
}

impl ModuleErrorDefs {
    fn module_name(&self) -> String {
        self.fully_qualified_module
            .split("::")
            .last()
            .expect("module name should have at least one component")
            .to_string()
    }

    fn error_kind_name(&self) -> String {
        format!("{}Error", self.enum_variant_name())
    }

    fn enum_variant_name(&self) -> String {
        self.module_name().to_pascal_case()
    }
}

fn main() {
    let out_dir = env::var_os("OUT_DIR").expect("OUT_DIR should be set automatically by cargo");
    let contracts_dir = "../../contracts";
    let contracts_dir_path = Path::new(contracts_dir);

    if !(fs::exists(contracts_dir_path).is_ok_and(identity)) {
        println!(
            "cargo::error=The directory {} (relative to crates/walrus-sui) \
            is required to generate Move errors.",
            contracts_dir_path.display()
        );
        return;
    }

    generate_error_defs_for_contracts(contracts_dir_path, Path::new(&out_dir));
    // Tell cargo to rerun if relevant files change
    println!("cargo::rerun-if-changed={}", contracts_dir);
}

fn generate_error_defs_for_contracts(contracts_dir: &Path, out_dir: &Path) {
    let dest_path = out_dir.join(format!(
        "{}_error_defs.rs",
        contracts_dir
            .file_name()
            .expect("contracts dir should have a name")
            .to_string_lossy()
    ));
    // Collect error definitions from Move files
    let error_defs = collect_error_definitions(contracts_dir);

    // Generate Rust code
    let error_defs_code = generate_all_error_defs(&error_defs);

    // Write to file
    fs::write(&dest_path, error_defs_code).expect("should be able to write to file");
}

/// Collect the error names and codes from the Move source code for each module.
fn collect_error_definitions(contracts_dir: &Path) -> Vec<ModuleErrorDefs> {
    let module_pattern =
        Regex::new(r"module\s+([a-z_]+::[a-z_]+)\s*;").expect("should be able to compile regex");
    let error_pattern = Regex::new(r"const\s+(E(?:[A-Z][a-z]+)*)\s*:\s*u64\s*=\s*(\d+)")
        .expect("should be able to compile regex");
    let mut module_error_defs = Vec::new();

    for entry in WalkDir::new(contracts_dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| {
            let components = e
                .path()
                .components()
                .filter_map(|component| component.as_os_str().to_str())
                .collect::<HashSet<_>>();
            e.path().extension().is_some_and(|ext| ext == "move")
                && components.contains("sources")
                && !components.contains("build")
        })
    {
        let content = fs::read_to_string(entry.path())
            .expect("should be able to read file in contracts directory");

        let Some(module_captures) = module_pattern.captures(&content) else {
            continue;
        };

        let module_name = module_captures[1].to_owned();

        let mut error_defs = Vec::new();
        for cap in error_pattern.captures_iter(&content) {
            let error_name = cap[1].to_owned();
            let error_code = cap[2]
                .parse::<u64>()
                .expect("error code captures digits only");
            error_defs.push((error_name, error_code));
        }

        if error_defs.is_empty() {
            continue;
        }

        error_defs.sort_by_key(|e| e.1);

        module_error_defs.push(ModuleErrorDefs {
            fully_qualified_module: module_name,
            error_defs,
        });
    }

    module_error_defs
}

/// Generate the definitions for the `MoveExecutionError` enum and all error kinds.
fn generate_all_error_defs(module_error_defs: &[ModuleErrorDefs]) -> String {
    let mut code = generate_move_error_enum(module_error_defs);
    code.push_str(&generate_move_error_from_impl(module_error_defs));
    code.push_str(&generate_error_kind_defs(module_error_defs));
    code
}

/// Generate the Rust code for the error definitions using the `move_error_kind!` macro.
fn generate_error_kind_defs(module_error_defs: &[ModuleErrorDefs]) -> String {
    let mut code = String::new();

    for module_error in module_error_defs {
        code.push_str(&format!(
            "move_error_kind!(\n    {},\n    {},\n",
            module_error.error_kind_name(),
            module_error.fully_qualified_module
        ));
        for (error_name, error_code) in module_error.error_defs.iter() {
            code.push_str(&format!("    {}, {},\n", error_name, error_code));
        }
        code.push_str(");\n\n");
    }

    code
}

fn generate_move_error_enum(module_error_defs: &[ModuleErrorDefs]) -> String {
    let mut code = String::new();
    code.push_str(
        r#"#[non_exhaustive]
#[derive(Debug, Clone, thiserror::Error)]
/// An error occurring during a Move contract execution.
pub enum MoveExecutionError {
"#,
    );
    for module_error in module_error_defs {
        code.push_str(&format!(
            "    /// Error in `{}`.\n",
            module_error.fully_qualified_module
        ));
        code.push_str("    #[error(\"{0}\")]\n");
        code.push_str(&format!(
            "    {}({}),\n",
            module_error.enum_variant_name(),
            module_error.error_kind_name()
        ));
    }
    code.push_str(
        r#"    /// Error in other move modules.
    #[error("Unknown move error occurred: {0}")]
    OtherMoveModule(RawMoveError),
    /// A non-parsable move error.
    #[error("unparsable move error occurred: {0}")]
    NotParsable(String),
}"#,
    );
    code.push_str("\n\n");
    code
}

fn generate_move_error_from_impl(module_error_defs: &[ModuleErrorDefs]) -> String {
    let mut code = String::new();

    code.push_str("impl From<RawMoveError> for MoveExecutionError {\n");
    code.push_str("    fn from(error: RawMoveError) -> Self {\n");
    code.push_str("        match error.module.as_str() {\n");

    // Generate match arms for each module.
    for module_error in module_error_defs {
        code.push_str(&format!(
            "            \"{}\" => {}::try_from(error).map(Self::{}),\n",
            module_error.module_name(),
            module_error.error_kind_name(),
            module_error.enum_variant_name(),
        ));
    }

    // Add the fallback case.
    code.push_str("            _ => Ok(Self::OtherMoveModule(error)),\n");

    code.push_str("        }\n");
    code.push_str("        .unwrap_or_else(|e: ConversionError| Self::OtherMoveModule(e.0))\n");
    code.push_str("    }\n");
    code.push_str("}\n\n");

    code
}

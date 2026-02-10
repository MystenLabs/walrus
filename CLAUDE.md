# CLAUDE.md

This file provides guidance for Claude Code when working on the Walrus repository.

## Project Overview

Walrus is a decentralized storage protocol built on Sui. It consists of storage nodes that store
data using erasure coding, a client for interacting with the network, and Move smart contracts on
Sui.

## Repository Structure

```
walrus/
├── crates/                     # Rust crates
│   ├── walrus-service/         # Main storage node and client implementation
│   │   ├── bin/                # Binaries: node.rs, client.rs, deploy.rs, backup.rs
│   │   └── src/
│   │       ├── node/           # Storage node implementation
│   │       │   ├── config.rs   # Node configuration
│   │       │   ├── contract_service.rs  # Sui contract interactions
│   │       │   ├── config_synchronizer.rs  # Config sync with on-chain state
│   │       │   └── ...
│   │       ├── client/         # Client implementation
│   │       └── event/          # Event processing
│   ├── walrus-sdk/             # High-level SDK for blob operations
│   │   └── src/
│   │       ├── client.rs       # Main client for upload/download
│   │       ├── uploader.rs     # Distributed upload orchestration
│   │       ├── active_committees.rs  # Committee tracking per epoch
│   │       ├── config.rs       # SDK configuration
│   │       └── upload_relay.rs # Upload relay client
│   ├── walrus-storage-node-client/  # Low-level storage node HTTP client
│   │   └── src/
│   │       ├── client.rs       # StorageNodeClient implementation
│   │       ├── api.rs          # REST API types (BlobStatus, ServiceResponse)
│   │       └── error.rs        # Error types (NodeError, ClientBuildError)
│   ├── walrus-sui/             # Sui blockchain integration
│   │   ├── src/client.rs       # Sui client for contract calls
│   │   └── src/types.rs        # Move struct representations
│   ├── walrus-core/            # Core types and cryptography
│   ├── walrus-simtest/         # Simulation tests
│   └── ...
├── contracts/                  # Move smart contracts
│   ├── walrus/                 # Main Walrus system contract
│   ├── wal/                    # WAL token contract
│   └── subsidies/              # Subsidies contract
├── scripts/                    # Utility scripts
├── docs/                       # Documentation
└── setup/                      # Setup configurations
```

## Key Crates

- **walrus-service**: Main crate containing storage node (`walrus-node`) and client (`walrus`) binaries
- **walrus-sui**: Sui blockchain integration, contract client, Move type definitions
- **walrus-core**: Core types, cryptographic primitives, erasure coding
- **walrus-sdk**: High-level SDK for blob upload/download operations
- **walrus-storage-node-client**: Low-level HTTP client for storage node REST API
- **walrus-simtest**: Deterministic simulation tests using `msim`

### Linting and Formatting

```bash
# Format code
cargo fmt

# Run clippy
cargo clippy --all-targets --all-features

# Run pre-commit hooks
pre-commit run --all-files
```

## Code Conventions

### Error Handling
- Never use `unwrap()` in production code; only in tests
- Use `expect()` with explanation when value cannot be None/Err
- Prefer explicit error handling with `Result` types

### Type Conversions
- Avoid `as` casts on numeric types (can silently truncate)
- Use `from`/`into` or `try_from`/`try_into` instead

### Logging
- Use `tracing` crate for logging
- Log messages: lowercase, no trailing period
- Use metadata fields instead of string interpolation
- Use `#[instrument]` attribute for async functions

### Naming
- Full words over abbreviations (except common ones like `min`, `max`, `id`)
- Module files use modern naming: `some_module.rs` with `some_module/` directory

### Commit Messages
- Follow [conventional commits](https://www.conventionalcommits.org/) format
- Examples: `feat: add new feature`, `fix: resolve bug`, `refactor: improve code structure`

## Configuration Files

- `StorageNodeConfig`: Node configuration in `crates/walrus-service/src/node/config.rs`
- Example config: `crates/walrus-service/node_config_example.yaml`
- Serde is used for YAML serialization; use `#[serde(default)]` for backward compatibility

## Key Patterns

### Adding New Config Fields
1. Add field to config struct with `#[serde(default, skip_serializing_if = "...")]` for backward compatibility
2. Update `generate_update_params()` if field affects on-chain state
3. Add CLI argument if needed in `bin/node.rs`
4. Write tests for serialization/deserialization

### Contract Interactions
- `SystemContractService` trait in `contract_service.rs` defines contract operations
- `SuiContractClient` in `walrus-sui` handles transaction building and execution
- On-chain types are in `walrus-sui/src/types.rs`

### Testing Patterns
- Unit tests in same file as implementation
- Use `#[tokio::test]` for async tests
- Mock traits using `mockall` crate (e.g., `MockSystemContractService`)
- Simulation tests in `walrus-simtest` for distributed system testing

## Useful Files to Reference

### Storage Node
- `crates/walrus-service/src/node/config.rs` - Node configuration structure
- `crates/walrus-service/src/node/contract_service.rs` - Contract service trait and implementation
- `crates/walrus-service/bin/node.rs` - CLI argument definitions and commands
- `crates/walrus-service/src/test_utils.rs` - Test utilities and mocks

### SDK and Client
- `crates/walrus-sdk/src/client.rs` - High-level client for blob operations
- `crates/walrus-sdk/src/uploader.rs` - Distributed upload orchestration
- `crates/walrus-sdk/src/config.rs` - SDK configuration
- `crates/walrus-storage-node-client/src/client.rs` - Low-level storage node HTTP client
- `crates/walrus-storage-node-client/src/api.rs` - REST API types and responses

### Sui Integration
- `crates/walrus-sui/src/types.rs` - Move type representations
- `crates/walrus-sui/src/client.rs` - Sui contract client

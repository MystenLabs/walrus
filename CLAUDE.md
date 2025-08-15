# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Build Commands
```bash
# Build all crates
cargo build

# Build optimized release version
cargo build --release

# Build specific binary
cargo run --bin walrus -- [args]
cargo run --bin walrus-node -- [args]

# Build documentation
cargo doc --workspace --open
```

### Testing Commands
```bash
# Install nextest for running tests
cargo install cargo-nextest

# Run all tests
cargo nextest run

# Run specific test
cargo nextest run [test_name]

# Run simulation tests
cargo test --package walrus-simtest

# Run Move contract tests
cd contracts/walrus && sui move test

# Run local testbed (requires local Sui network)
./scripts/local-testbed.sh
```

### Linting and Formatting
```bash
# Format code
cargo fmt

# Run clippy lints
cargo clippy --all-targets --all-features

# Check for build warnings
cargo check --workspace
```

## High-Level Architecture

Walrus is a decentralized storage system combining off-chain storage nodes with Sui blockchain
coordination. The system uses Reed-Solomon erasure coding for fault-tolerant storage.

### Core Components

1. **Storage Nodes** (`crates/walrus-service/src/node.rs`): Manage shards containing
erasure-coded data slivers. Each node participates in BLS signature aggregation for availability
certificates.

2. **Sui Integration** (`crates/walrus-sui/`): On-chain contracts coordinate storage epochs,
manage storage resources, and track blob metadata. Key contracts are in `contracts/walrus/sources/`.

3. **Encoding System** (`crates/walrus-core/src/encoding.rs`): Two-dimensional Reed-Solomon
erasure coding with 4.5-5x expansion factor. Primary encoding enables fast random access while
secondary encoding provides additional redundancy.

4. **Client SDK** (`crates/walrus-sdk/`): High-level API for blob storage/retrieval, committee management, and resource allocation.

### Key Data Flow

**Write Path**:
1. Client acquires storage resources on Sui
2. Registers blob ID with resource (emits blockchain event)
3. Erasure-codes blob and distributes slivers to storage nodes
4. Collects signatures from nodes to form availability certificate
5. Submits certificate to Sui contract

**Read Path**:
1. Retrieves blob metadata from any storage node
2. Requests slivers from f+1 nodes in parallel
3. Reconstructs blob using erasure decoding
4. Verifies blob matches expected cryptographic ID

### Storage Organization

- **BlobId**: 32-byte cryptographic identifier from Merkle root
- **Slivers**: Erasure-coded pieces distributed across shards
- **Storage Resources**: On-chain tokenized capacity with epoch bounds
- **Committees**: Storage nodes organized in 2-week epochs with BFT assumptions (tolerates 1/3 Byzantine nodes)

### Key Contracts

- `contracts/walrus/sources/system.move`: Main coordination contract
- `contracts/walrus/sources/staking.move`: Node staking and rewards
- `contracts/walrus/sources/system/blob.move`: Blob lifecycle management
- `contracts/walrus/sources/system/storage_resource.move`: Storage resource tokens

### Important Crates

- `walrus-core`: Core types, encoding, and cryptographic primitives
- `walrus-service`: Storage node and client implementations
- `walrus-sui`: Sui blockchain integration
- `walrus-sdk`: High-level client SDK
- `walrus-simtest`: Simulation tests for concurrent operations
- `walrus-storage-node-client`: HTTP client for storage node APIs

## Development Notes

- The codebase uses `unwrap()` only in tests; production code uses explicit error handling
- Type conversions prefer `from/into` over `as` casts to avoid truncation
- Logging uses the `tracing` crate with structured fields
- Pre-commit hooks enforce code formatting and linting
- Commit messages follow conventional commit format (e.g., `feat:`, `fix:`, `docs:`)
- Storage nodes use RocksDB for persistent storage
- The system assumes AVX2/SSSE3 on x86 and Neon on ARM for efficient encoding


## Node format
- Each line should be less than 100 characters, for *.rs files, note that explicit line break
  should be used.
- Each comment should end with a period.

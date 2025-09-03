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

# Run indexer tests (use sequential execution to avoid metrics conflicts)
RUST_TEST_THREADS=1 cargo test -p walrus-indexer --lib
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

4. **Client SDK** (`crates/walrus-sdk/`): High-level API for blob storage/retrieval,
committee management, and resource allocation.

5. **Indexer (Octopus Index)** (`crates/walrus-indexer/`): Dual-index system for
blob discovery. Maps bucket_id/identifier → blob metadata and object_id → blob
metadata, processing Sui events to maintain synchronized indices.

### core concepts
1. Blob ID is determined by the blob data, the same data result in the same ID, as well as the
same sliver pairs.

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

### Indexer System (Octopus Index)

The indexer provides efficient blob discovery through a dual-index system:

**Data Structures**:
- **BlobIdentity**: Contains both `blob_id` and `object_id` for complete blob identification
- **Primary Index**: Maps `bucket_id/identifier` → `BlobIdentity`
- **Object Index**: Maps `object_id` → `bucket_id/identifier` (reverse lookup)

**Key Operations**:
- `IndexMutation::Insert`: Adds entry to both indices atomically
- `IndexMutation::Delete`: Removes entry from both indices atomically
- Processes Sui events to maintain index synchronization

**API Endpoints**:
- `GET /v1/blobs/{bucket_id}/{primary_key}`: Retrieve blob by bucket and identifier
- `GET /v1/object/{object_id}`: Retrieve blob by object ID
- `GET /v1/bucket/{bucket_id}`: List all blobs in bucket
- `GET /v1/bucket/{bucket_id}/{prefix}`: List blobs with prefix
- `GET /v1/bucket/{bucket_id}/stats`: Get bucket statistics

**Storage Backend**:
- Uses RocksDB with two column families: `octopus_index_primary` and `octopus_index_object`
- Atomic batch operations ensure consistency between indices
- In-memory cache for frequently accessed entries

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
- `walrus-indexer`: Octopus Index implementation for blob discovery

## Development Notes

- The codebase uses `unwrap()` only in tests; production code uses explicit error handling
- Type conversions prefer `from/into` over `as` casts to avoid truncation
- Logging uses the `tracing` crate with structured fields
- Pre-commit hooks enforce code formatting and linting
- Commit messages follow conventional commit format (e.g., `feat:`, `fix:`, `docs:`)
- Storage nodes use RocksDB for persistent storage
- The system assumes AVX2/SSSE3 on x86 and Neon on ARM for efficient encoding


## Code Format and Style

### General Formatting
- Each line should be less than 100 characters, for *.rs files, note that explicit line break
  should be used.
- Each comment should end with a period.
- Avoid fully qualified type names (e.g., `walrus_storage_node_client::api::MultiPutBundle`).
  Instead, import the types first and use simple names (e.g., `use ... ::api::MultiPutBundle;`
  then just `MultiPutBundle`).

### Walrus Indexer Specific Style
Based on recent refactoring (2025-09-03):

**Method Organization**:
- Public methods should be grouped together, followed by private helper methods.
- The `run()` method should be self-contained, calling helper methods for initialization.
- Helper methods like `start_event_processor()` and `start_rest_api()` should be private.

**Import Organization**:
- Use `tokio::{select, task::JoinHandle}` style for multiple imports from same module.
- Group imports: std library, external crates, then internal modules.

**Documentation Comments**:
- Use triple-slash `///` for public API documentation.
- Multi-line doc comments should have clear formatting:
  ```rust
  /// Run the indexer as a full-featured service.
  /// This method:
  /// 1. Initializes the event processor (if configured).
  /// 2. Starts the REST API server (if configured).
  /// 3. Processes events from Sui blockchain (if configured).
  ```

**Error Messages**:
- Multi-line error/warning messages should use backslash for continuation:
  ```rust
  warn!(
      "Event processor config provided but sui config is missing. \
       Event processor will not be started."
  );
  ```

**Async Method Patterns**:
- Extract initialization logic into separate async methods when the `run()` method becomes complex.
- Use `self: Arc<Self>` for methods that need to spawn tasks with self.
- Use `self: &Arc<Self>` for methods that need to clone Arc but don't consume it.


## Manual confirmation
- Ask me everytime before making changes to the code for manual review.

## Recent Changes

### Indexer Architecture Refactoring (2025-09-03)

Refactored the Walrus Indexer to separate library and service concerns:

**Key Architectural Changes**:
- Moved event processor initialization from `new_and_start()` to `run()` method
- Database opening logic encapsulated in `storage::OctopusIndexStore::open()`
- Clear separation: without `run()`, indexer is a simple KV store; with `run()`, it's a full service
- Helper methods `start_event_processor()` and `start_rest_api()` extracted for cleaner code

**Pattern Changes**:
- `new()` now returns `Arc<WalrusIndexer>` directly (eliminated redundant `new_and_start()`)
- `run()` method handles all service initialization (event processor, REST API)
- Indexer can be used as library (with `apply_mutations()`) or service (with `run()`)

### Event Processing Integration (2025-09-02)

Integrated with existing EventProcessor from walrus-service:

**Key Changes**:
- Uses `EventProcessorRuntime::start_async()` from walrus-service
- Implements `EventStreamCursor` pattern for event resumption
- Stores last processed event index in DBMap for persistence
- Removed custom event processor implementation

### Indexer Implementation (2025-08-31)

Updated the Walrus Indexer to implement dual-index system per design specification:

**Modified Files**:
- `crates/walrus-indexer/src/storage/mod.rs`: Implemented dual-index storage layer
  with atomic batch operations. Added `BlobIdentity` struct and updated
  `IndexMutation` enum to match specification.
  
- `crates/walrus-indexer/src/lib.rs`: Updated database initialization for dual
  indices. Modified `IndexOperation` enum to support removal by object_id or
  identifier. Added `get_blob_by_object_id` method.
  
- `crates/walrus-indexer/src/routes.rs`: Added `/v1/object/{object_id}` endpoint.
  Updated response format to return both `blob_id` and `object_id` fields.
  
- `crates/walrus-indexer/src/bin/indexer.rs`: Updated documentation to include
  new object_id endpoint.

**Key Changes**:
- Bidirectional mapping between `object_id` and `bucket_id/identifier`
- Atomic updates to both indices using RocksDB batch operations
- Cache consistency maintained across operations
- All tests updated and passing with sequential execution

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Walrus is a decentralized storage protocol built on Sui. It uses Reed-Solomon erasure coding to
distribute blob data across a network of storage nodes, with smart contracts on Sui managing
registration, certification, and economics.

## Architecture

### Three-Layer Design

1. **Client Layer** (`walrus-sdk`): Encodes blobs into slivers via erasure coding, orchestrates
   parallel uploads to storage nodes, and interacts with Sui contracts for registration/certification.
2. **Storage Node Layer** (`walrus-service`): Axum-based REST API servers that store slivers in
   RocksDB, process blob events from Sui, manage epoch transitions, and participate in shard sync.
3. **Blockchain Layer** (`contracts/`): Move smart contracts on Sui managing blob lifecycle
   (register, certify, delete), committee state, staking, and WAL token economics.

### Blob Upload Flow

1. `BlobEncoder` splits raw data into primary/secondary sliver pairs using RS2 erasure coding
2. Merkle tree over sliver pairs produces the blob ID (committed on-chain)
3. `DistributedUploader` sends slivers in parallel to storage nodes based on committee shard assignments
4. Client calls Sui contract to register blob; nodes see `BlobRegistered` event and finalize storage
5. After quorum confirmation, client can certify the blob on-chain

### Epoch & Committee Model

- `ActiveCommittees` tracks current, previous, and next epoch committees
- Epoch changes reshuffle shard-to-node assignments; `EpochChangeDriver` manages transitions
- During transitions, both current and previous committees serve reads

### Key Crate Relationships

- **walrus-sdk** → uses **walrus-storage-node-client** (HTTP client to nodes) + **walrus-sui** (chain interactions)
- **walrus-service** → storage node binary + publisher/aggregator daemon; uses **walrus-core** (encoding/types) + **walrus-sui** (event processing)
- **walrus-core** → encoding primitives, cryptographic types, slivers, metadata (no network/chain deps)
- **walrus-upload-relay** → alternative upload path where clients pay tips; validates transactions and delegates to SDK
- **walrus-proxy** → metrics aggregation relay for Prometheus/Mimir
- **walrus-stress** → stress testing workloads against Walrus clusters
- **walrus-indexer** → indexes Walrus on-chain data

## Build & Development Commands

### Prerequisites

System packages: `libssl-dev pkg-config zlib1g-dev libpq-dev build-essential cmake`
Rust toolchain: 1.93 (specified in `rust-toolchain.toml`)

### Building

```bash
cargo build                          # Debug build
cargo build --release                # Release build
```

### Running Tests

```bash
# Run all tests (uses nextest)
cargo nextest run

# Run a single test by name
cargo nextest run my_test_name

# Run tests in a specific crate
cargo nextest run -p walrus-core

# Run ignored tests (require external Sui cluster)
cargo nextest run --run-ignored ignored-only

# Run doctests (nextest doesn't run these)
cargo test --doc
```

### Linting & Formatting

```bash
# Format (note: custom rustfmt config via CLI args, not rustfmt.toml)
cargo fmt -- --config group_imports=StdExternalCrate,imports_granularity=Crate,imports_layout=HorizontalVertical

# Clippy
cargo clippy --all-features --tests -- -D warnings

# All pre-commit hooks
prek run --all-files # or pre-commit run --all-files if using pre-commit
```

### Simulation Tests (msim)

Deterministic simulation tests for distributed system scenarios. Uses a patched Rust stdlib for
controlled scheduling and networking.

```bash
# One-time install
./scripts/simtest/install.sh

# Run all simtests
MSIM_TEST_SEED=1 cargo simtest simtest --profile simtest

# Run a specific simtest
MSIM_TEST_SEED=1 cargo simtest simtest <test_name> --profile simtest

# Run all seeds (CI runs seeds 1-5)
./scripts/run-all-simtests.sh
```

Key env vars: `MSIM_TEST_SEED` (default: 1), `WALRUS_GRPC_MIGRATION_LEVEL` (0 or 100).

### Move Contracts

```bash
# Run all Move tests
./scripts/move_tests.sh

# With coverage (minimum 70%)
./scripts/move_tests.sh -c

# Specific directory
./scripts/move_tests.sh -d testnet-contracts

# Manual (from contract dir)
sui move test --allow-dirty -e testnet
```

The correct `sui` binary version must match the Sui tag in `Cargo.toml` (currently `testnet-v1.66.1`).

### Binaries

```bash
cargo run --bin walrus -- <args>        # Client CLI
cargo run --bin walrus-node -- <args>   # Storage node
```

### Local Testbed

```bash
./scripts/local-testbed.sh              # Start local network (4 nodes, 10 shards)
./scripts/local-testbed.sh -c 8 -s 20   # Custom committee size and shards
```

## Code Conventions

### Error Handling
- No `unwrap()` in production code (only tests); use `expect()` with explanation
- Prefer explicit `Result` types

### Type Conversions
- No `as` casts on numeric types (clippy `cast_possible_truncation` is enforced)
- Use `from`/`into` or `try_from`/`try_into`

### Logging
- `tracing` crate; lowercase messages, no trailing period
- Structured fields over string interpolation: `tracing::info!(blob_id = %id, "stored blob")`
- `#[instrument]` on async functions

### Naming
- Full words over abbreviations (except `min`, `max`, `id`)
- Module files: `some_module.rs` with `some_module/` directory (modern style)

### Commit Messages
- [Conventional commits](https://www.conventionalcommits.org/): `feat:`, `fix:`, `refactor:`, `chore:`, `docs:`

## Workspace Lints

Enforced via `[workspace.lints]` in root `Cargo.toml`:
- `clippy::unwrap_used = "warn"` — no unwrap in non-test code
- `clippy::cast_possible_truncation = "warn"` — no lossy numeric casts
- `clippy::unused_async = "warn"` — don't mark functions async unnecessarily
- `missing_docs = "warn"` — public items need doc comments
- `unexpected_cfgs = "allow"` for `cfg(msim)` — simulation test conditional compilation

## Nextest Profiles

Configured in `.config/nextest.toml`:
- **default**: excludes `walrus-performance-tests`, 1m slow timeout
- **ci**: 1 retry, 10m timeout; `walrus-e2e-tests` and `walrus-sui` get 2m slow period
- **simtest**: `num-cpus` threads, 30m timeout
- **performance-test**: single-threaded, 15m timeout

## Storage Resources

### Overview

A `Storage` resource is the fundamental economic primitive in Walrus — a Sui object representing a
reservation of `storage_size` bytes for epoch range `[start_epoch, end_epoch)`. It is defined in
`contracts/walrus/sources/system/storage_resource.move` and mirrored in Rust as `StorageResource`
in `crates/walrus-sui/src/types/move_structs.rs`.

### Lifecycle

1. **Purchase** (`system::reserve_space`): User pays WAL tokens. Cost =
   `ceil(encoded_size / 1 MiB) * storage_price_per_unit * num_epochs`. Payment is split per-epoch
   into the `FutureAccountingRingBuffer`. Capacity is checked against `total_capacity_size` for
   each epoch.
2. **Blob Registration** (`system::register_blob`): The `Storage` object is consumed (embedded into
   the `Blob` object). A separate write fee is paid. The contract validates the blob's encoded size
   fits within `storage_size` and the current epoch is within the storage's range.
3. **Certification** (`system::certify_blob`): Records a quorum BLS signature. Does not modify the
   storage resource.
4. **Extension** (`system::extend_blob`): Pays for additional epochs, extends `end_epoch` in-place.
   Alternatively `extend_blob_with_resource` fuses a separate `Storage` object into the blob.
5. **Deletion** (`system::delete_blob`): For deletable blobs only. Destroys the `Blob` and returns
   the `Storage` object for reuse.
6. **Split/Merge**: `split_by_epoch`, `split_by_size`, `fuse_periods`, `fuse_amount` allow flexible
   resource management. The SDK uses `split_storage_by_size` to right-size resources before
   registration.

### Pricing & Economics

- **Two prices** (WAL per MiB, set by quorum voting of storage nodes):
  - `storage_price_per_unit_size` — per-epoch reservation cost
  - `write_price_per_unit_size` — one-time registration cost
- **Price voting**: Nodes call `set_storage_price_vote`/`set_write_price_vote`. The `quorum_below`
  algorithm selects the lowest price that 2/3+ of the committee (by shard weight) voted for or
  above. Takes effect immediately.
- **FutureAccountingRingBuffer** (`contracts/walrus/sources/system/storage_accounting.move`): One
  slot per epoch, each holding `used_capacity` and `rewards_to_distribute`. At epoch end,
  `ring_pop_expand()` pops the current slot. 3% of rewards are burned, the rest distributed to
  nodes proportional to `weight * (used_capacity - deny_list_size)`.
- **Subsidies**: `add_subsidy` distributes WAL across future epochs. Client-side `Credits` objects
  can reduce buyer cost via `buyer_subsidy_rate`/`system_subsidy_rate`.

### SDK Resource Manager

`ResourceManager` in `crates/walrus-sdk/src/client/resource.rs` optimizes cost by choosing:
- `ReuseRegistration` — blob already registered with sufficient lifetime
- `ReuseStorage` — existing `StorageResource` in wallet fits (by size and epoch range)
- `RegisterFromScratch` — buy new storage + register
- `ReuseAndExtend` — blob exists but lifetime too short, extend it

It queries owned resources, sorts by best fit (smallest sufficient), and allocates largest blobs
first.

### Interaction with Storage Nodes

Storage nodes **never directly inspect `Storage` objects on Sui**. They learn about storage
resources through on-chain events:

**Event pipeline** (Sui → EventProcessor → Storage Node):
1. `EventProcessor` (`walrus-service/src/event/event_processor/processor.rs`) downloads Sui
   checkpoints and extracts Walrus events.
2. Events are streamed to the node via the `SystemEventProvider` trait.
3. The node dispatches events in `process_event_impl()` (`walrus-service/src/node.rs`).

**Key events** (`contracts/walrus/sources/system/events.move`):
- `BlobRegistered` — includes `blob_id`, `size`, `end_epoch` (from storage resource), `deletable`
- `BlobCertified` — includes `end_epoch`, `is_extension` flag
- `BlobDeleted` — blob was deleted, storage reclaimed

**When `BlobRegistered` arrives**:
1. `update_blob_info()` writes to the `BlobInfoTable` (RocksDB merge operator), recording
   `end_epoch`, deletable/permanent status, reference counts.
2. `notify_registration()` wakes clients that may have started uploading before registration.
3. `flush_pending_caches()` persists pre-uploaded metadata and slivers from memory to disk.

**Sliver acceptance** (`store_sliver()` in `walrus-service/src/node.rs`):
- If the blob is registered (metadata persisted) → store directly
- If not yet registered → buffer in pending sliver cache; flush when registration event arrives
- `is_blob_registered()` checks `end_epoch > current_epoch` (the storage resource's lifetime)

**Capacity tracking is on-chain only**: When `reserve_space()` succeeds and emits `BlobRegistered`,
the contract has already validated capacity. Nodes trust the chain events.

**Epoch transitions and expiry**:
1. Node receives `EpochChangeStart` → waits for pending events, locks shards, executes epoch change.
2. Garbage collection iterates per-object blob info, deletes entries where `end_epoch <= current_epoch`.
3. When no non-expired references remain (`can_data_be_deleted()`), actual sliver data is deleted.

**Full flow**:
```
User pays WAL → reserve_space() → Storage created (capacity++ in ring buffer)
User registers → register_blob() → Storage consumed into Blob → BlobRegistered event
  → Node: update blob_info, notify clients, flush pending caches
Client uploads slivers → store_sliver() → node checks is_blob_registered() or buffers
Client certifies → certify_blob() → BlobCertified event → node syncs missing slivers
Epoch ends → advance_epoch() → pop ring buffer, burn 3%, distribute rewards
  → Node: garbage collect expired blobs, delete slivers for expired storage
```

## Key Patterns

### Adding Config Fields
1. Add field with `#[serde(default, skip_serializing_if = "...")]` for backward compatibility
2. Update `generate_update_params()` if it affects on-chain state
3. Add CLI argument in `bin/node.rs` if needed

### Publisher/Aggregator Daemon
- Lives in `walrus-service/src/client/`: Axum HTTP API for storing (publisher) and reading (aggregator) blobs
- `ClientMultiplexer` manages a `WriteClientPool` of `WalrusNodeClient<SuiContractClient>` instances for parallel writes
- Each pool client uses a separate sub-wallet (created/loaded from disk) with automatic gas/WAL refilling via `Refiller`
- `WalrusWriteClient` / `WalrusReadClient` traits in `daemon.rs` define the interface; `ClientMultiplexer` and `WalrusNodeClient` both implement them
- HTTP routes are in `daemon/routes.rs`; query parameters control encoding type, epochs, persistence, etc.

### Contract Interactions
- `SystemContractService` trait in `contract_service.rs` defines node-side contract operations
- `SuiContractClient` in `walrus-sui` builds and executes Sui transactions
- On-chain types mapped in `walrus-sui/src/types.rs`

### Conditional Compilation
- `#[cfg(msim)]` gates simulation-specific code (fake clocks, deterministic RNG, controlled networking)
- `cfg(msim)` is allowed via workspace lint config; do not remove the `unexpected_cfgs` allow

### Testing
- Unit tests in same file; `#[tokio::test]` for async
- `mockall` for trait mocking (e.g., `MockSystemContractService`)
- Simulation tests in `walrus-simtest` for end-to-end distributed scenarios
- `walrus-e2e-tests` requires 4 threads per test (`threads-required = 4` in nextest config)

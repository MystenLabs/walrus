> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

Track what's new, changed, and fixed in `memwal` (Python).

For the latest version, see the [PyPI project page](https://pypi.org/project/memwal/).

### Added

- Added `RecallParams` for object-style `recall(...)` calls.

### Changed

- Changed the default `restore()` limit from `50` to `10` to match the relayer and TypeScript SDK.
- Documented `restore()` response fields, default limit, pagination behavior, and performance expectations.

### Added

- Added `max_distance` to async and sync `recall()`.
- Added credential verification helper.

### Changed

- Updated docs/examples to use `MEMWAL_PRIVATE_KEY`.

### Fixed

- Made `401` relayer errors more actionable.

### Added

- Added relayer `env` presets.
- Added compatibility checks and `compatibility()` helpers.

### Initial release

- `MemWal` async client and `MemWalSync` sync wrapper
- Memory APIs: `remember`, `recall`, `analyze`, `ask`, `restore`, `health`
- Async job helpers for remember, bulk remember, and analyze
- LangChain/OpenAI middleware and delegate-key utilities
- Ed25519 delegate-key auth with namespace-scoped memory isolation
> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

Track what's new, changed, and fixed in `memwal` (Python).

For the latest version, see the [PyPI project page](https://pypi.org/project/memwal/).

### Added

- Added a runnable [Walrus Memory Python SDK Colab](https://colab.research.google.com/drive/1SaKjkSp0DXnM_nktWSiEC-l9qGtVr6ph) covering installation, secure `staging` configuration, optional `prod`, `MemWalSync`, health/compatibility checks, delegate public-key/address derivation, `remember`, `remember_async`, async job waiting, `recall`, bulk remember, `remember_bulk_async`, `remember_bulk_and_wait`, optional `ask`, `analyze`, `analyze_and_wait`, `embed`, manual methods with scoring weights, `restore`, optional OpenAI/LangChain middleware, OpenAI-compatible provider settings such as `OPENAI_BASE_URL`, and troubleshooting.

### Fixed

- Fixed `MemWalSync` reuse inside notebooks so repeated calls do not reuse an HTTP transport from a closed event loop.

### Added

- Added optional `occurred_at` to `analyze()` and `analyze_and_wait()` (both async and sync) for temporal anchoring of extracted facts. When supplied, the server resolves in-turn relative references ("last Friday", "yesterday") into absolute dates inside the extracted fact text before embedding and encryption.
- Accepts `datetime` or RFC-3339 string. Wire format is RFC-3339 UTC with millisecond precision (for example, `"2023-05-25T17:50:00.000Z"`), byte-identical to the TypeScript SDK.
- Field is omitted from the request body when not supplied.

### Changed

- `occurred_at` validates input at the SDK boundary rather than forwarding malformed values to the server: naive `datetime` instances raise `ValueError` (silently assuming UTC would mis-anchor by N hours for callers outside UTC), and malformed RFC-3339 strings raise `ValueError` with a diagnostic message instead of surfacing as opaque 400s.

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
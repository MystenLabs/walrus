> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

### Added

- Added `RecallParams` for object-style `recall(...)` calls.

### Changed

- Marked the positional `recall(...)` overload as deprecated in favor of `recall({ query, limit, namespace })`.
- Documented `restore()` response fields, default limit, pagination behavior, and performance expectations.

### Added

- Added relayer compatibility metadata checks before protected requests.
- Added `compatibility()` and exported compatibility types/errors so callers can inspect SDK/relayer support explicitly.
- Added `RecallOptions` for `topK`, namespace override, and `maxDistance`.

### Changed

- Prefer Sui gRPC for SEAL sessions, with JSON-RPC fallback.
- Updated docs/examples for `MEMWAL_PRIVATE_KEY` and hosted relayer defaults.

### Fixed

- Made `401` relayer errors more actionable.

### Added

- Added `getRememberStatus(jobId)` so clients can poll and display the full async remember state machine.
- Added `SealServerConfig` and `sealServerConfigs` for manual-mode SEAL committee aggregator configuration.

### Changed

- Manual mode now normalizes full SEAL server configs, validates optional API key pairs, and caps the default threshold to configured server weight.
- Manual mode keeps Testnet defaults on the legacy independent key servers for compatibility with hosted Testnet relayer data.

### Changed

- Updated `remember()` for the relayer's async `/api/remember` flow. It now returns the accepted job payload immediately.
- Added `rememberAsync()`, `waitForRememberJob()`, and `rememberAndWait()` for callers that need the final `blob_id`.
- Added bulk remember helpers: `rememberBulk()`, `rememberBulkAsync()`, `waitForRememberJobs()`, and `rememberBulkAndWait()`.
- Updated `analyze()` for async fact storage and added `analyzeAndWait()`.

### Compatibility

- `recall()` and `restore()` remain wire-compatible with the existing relayer responses.
- The SDK continues to use `x-seal-session` for relayer-mode decrypt credentials.

### Security

- Added per-request `x-nonce` signing to block replay within the timestamp window.
- Added `x-account-id` to the canonical signed message so account hints cannot be rebound in transit.
- Replaced relayer-mode `x-delegate-key` transport with ephemeral `x-seal-session`; manual-mode requests no longer send delegate private key material.
- SDK versions that do not send `x-nonce` are no longer supported by the server and receive `426 Upgrade Required`.

### Initial release

- `MemWal` default client, relayer-handled embedding, SEAL encryption, Walrus upload, vector search
- `MemWalManual` manual client, client-side embedding and SEAL operations
- `withMemWal` Vercel AI SDK middleware, automatic memory recall and save
- Account management utilities, `createAccount`, `addDelegateKey`, `removeDelegateKey`, `generateDelegateKey`
- Ed25519 delegate key authentication
- Namespace-scoped memory isolation
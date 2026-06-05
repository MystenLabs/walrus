> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

### Fixed

- Accept HTTPS dashboard sign-in callbacks to the local `127.0.0.1` MCP listener.
- Reload credentials after `memwal_login` so memory tools work without restarting the MCP client.

### Changed

- Rebranded package metadata and documentation from Walrus Memory to Walrus Memory.

### Added

- Added relayer compatibility metadata checks before opening the MCP bridge.

### Initial release

- Stdio MCP server for Walrus Memory with browser-based wallet login.
- Inline `memwal_login` and `memwal_logout` session tools.
- Memory tools for remember, recall, analyze, and restore through the Walrus Memory relayer.
- Environment presets for production, dev, staging, and local relayers.
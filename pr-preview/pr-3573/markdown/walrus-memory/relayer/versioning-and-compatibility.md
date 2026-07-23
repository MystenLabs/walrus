> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

The Walrus Memory relayer is the public protocol/API layer for SDKs, MCP clients, and self-hosted deployments. Treat every route, signed header, response field, runtime config field, and documented environment variable on this page as a versioned contract.

## Relayer SemVer

The relayer package version follows SemVer, and the public API contract is exposed separately as `apiVersion`.

| Change | Required bump | Examples |
| --- | --- | --- |
| Compatible public behavior | patch | bug fix, clearer error body, additive docs |
| Additive public surface | minor | new response field, new route, new optional env var, new feature flag |
| Breaking public surface | major | removing or renaming a header, route, env var, response field, auth canonical format, or changing field meaning |

Public surfaces include:

- HTTP routes under `/api/*`, `/health`, `/version`, `/config`, `/sponsor`, and `/api/mcp*`
- signed auth headers and the canonical signature format
- JSON request/response field names and status-code semantics
- runtime config and environment variables used by self-hosted relayers
- MCP transport behavior and bearer credential expectations

## Compatibility matrix

| Relayer API | Relayer package | TypeScript SDK | Python SDK | MCP package | Notes |
| --- | --- | --- | --- | --- | --- |
| `1.x` | `0.1.x` | `>=0.0.4` | `>=0.1.0` | `>=0.0.1` | Requires `x-nonce`; TypeScript and Python SDKs use `x-seal-session` for relayer-mode decrypt flows; MCP uses bearer delegate credentials for SSE |

SDKs and MCP clients read `/version` before protected requests and fail with an explicit compatibility error when the relayer API major or minimum SDK version is unsupported.

## Runtime metadata

Modern relayers expose compatibility metadata at `GET /version` and include the same block in `GET /health`.

[Source: relayer/versioning-and-compatibility.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/relayer/versioning-and-compatibility.md)

```json
{
  "relayerVersion": "0.1.0",
  "apiVersion": "1.0.0",
  "minSupportedSdk": {
    "typescript": "0.0.4",
    "python": "0.1.0",
    "mcp": "0.0.1"
  },
  "featureFlags": {
    "auth.accountBoundNonce": true,
    "auth.sealSessionHeader": true,
    "runtime.versionEndpoint": true
  },
  "deprecations": [
    {
      "surface": "header:x-delegate-key",
      "deprecatedSince": "1.0.0",
      "removalApiVersion": "2.0.0",
      "guidance": "Use x-seal-session for relayer-managed SEAL decrypt flows; manual-mode requests should send no decrypt credential."
    }
  ],
  "build": {
    "commit": "optional-git-sha",
    "buildTimestamp": "optional-build-time"
  }
}
```

`/health` keeps the legacy `version` field for older monitoring checks. New automation should prefer `relayerVersion`.

## Deprecation process

Breaking changes must follow this process unless there is an active security incident:

1. Add a deprecation notice to `/version.deprecations`.
2. Document the replacement path and removal `apiVersion`.
3. Keep the old surface working for the rest of the current API major.
4. Add or update contract tests and docs in the same PR.
5. Remove the surface only in the next API major.

This process applies to headers, routes, environment variables, response fields, feature flags, and MCP transport behavior.

## Environment variables

Environment variables documented in [Environment Variables](/walrus-memory/reference/environment-variables) are public contract items for self-hosted deployments. Renaming, removing, or changing their meaning is a breaking relayer API change.

Additive env vars might ship in a minor release. Deprecating an env var requires a `/version.deprecations` entry, docs update, and a replacement path. `SEAL_KEY_SERVERS` is currently deprecated in favor of `SEAL_SERVER_CONFIGS` and remains supported through relayer API `1.x`.

## Contract checks

CI runs `pnpm check:compatibility`, which verifies that:

- relayer API and minimum supported SDK/MCP constants are valid SemVer contract values
- SDK/MCP compatibility baselines match the relayer's minimum supported versions
- SDK/MCP package versions are not older than the compatibility baseline they advertise
- this policy document contains the current API version and compatibility matrix values

If a public compatibility value changes, update the implementation, this document, and the relevant changelog/deprecation notes together.
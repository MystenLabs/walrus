> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

Use this page to pick the right config shape quickly.

## `MemWalConfig`

Used by:

- `MemWal.create(config)`
- `withMemWal(model, options)`

| Field | Required | Notes |
| --- | --- | --- |
| `key` | yes | Delegate private key in hex |
| `accountId` | yes | MemWalAccount object ID on Sui |
| `serverUrl` | no | Relayer URL. Default: `https://relayer.memwal.ai` |
| `namespace` | no | Default memory boundary. Default: `"default"` |

## `MemWalManualConfig`

Used by:

- `MemWalManual.create(config)`

Core fields:

| Field | Required | Notes |
| --- | --- | --- |
| `key` | yes | Delegate private key in hex |
| `serverUrl` | no | Relayer URL |
| `embeddingApiKey` | yes | OpenAI/OpenRouter-compatible embedding key |
| `embeddingApiBase` | no | Default: `https://api.openai.com/v1` |
| `embeddingModel` | no | Default: `text-embedding-3-small` |
| `packageId` | yes | Walrus Memory package ID on Sui |
| `accountId` | yes | `MemWalAccount` object ID |
| `namespace` | no | Default namespace |

Sui signer fields:

| Field | Required | Notes |
| --- | --- | --- |
| `suiPrivateKey` | one of two | Use for local signing |
| `walletSigner` | one of two | Use a connected browser wallet instead |
| `suiClient` | no | Optional pre-configured Sui client |

Walrus and network fields:

| Field | Required | Notes |
| --- | --- | --- |
| `suiNetwork` | no | `testnet` or `mainnet`. Default: `mainnet` |
| `sealServerConfigs` | no | Full SEAL configs for independent or committee servers. Committee entries require `aggregatorUrl` |
| `sealKeyServers` | no | Legacy override for independent SEAL key server object IDs |
| `sealThreshold` | no | Default: `2`, capped to total configured server weight |
| `walrusEpochs` | no | Default: `50` |
| `walrusAggregatorUrl` | no | Walrus download endpoint. Defaults follow `suiNetwork` |
| `walrusPublisherUrl` | no | Walrus upload endpoint. Defaults follow `suiNetwork` |

## `WithMemWalOptions`

`withMemWal(model, options)` accepts all `MemWalConfig` fields plus:

| Field | Required | Notes |
| --- | --- | --- |
| `maxMemories` | no | Default: `5` |
| `autoSave` | no | Default: `true` |
| `minRelevance` | no | Default: `0.3` |
| `debug` | no | Default: `false` |

## Rules that matter

- `namespace` defaults to `"default"` when omitted.
- `MemWal` is the default relayer-handled path.
- `MemWalManual` is the manual client path, but it still uses the relayer for registration, search, and restore.
- `withMemWal` builds on top of `MemWal`, so it uses the same relayer-backed config shape.
- `MemWalManual` now defaults to `mainnet` network settings unless you pass `suiNetwork: "testnet"`.
- `sealServerConfigs` takes priority over `sealKeyServers`; `sealKeyServers` remains supported for legacy independent key server lists.
- Relayer/sidecar SEAL defaults use Mysten's initial committee aggregator on `testnet`. `mainnet` keeps the legacy independent key server pair until an official Mainnet committee aggregator is available.
- `MemWalManual` keeps the legacy independent Testnet default for compatibility. Pass `sealServerConfigs` to use a committee aggregator manually.
- Use `sealServerConfigs` to override the built-in default with another committee by providing `objectId`, `weight`, and `aggregatorUrl`.
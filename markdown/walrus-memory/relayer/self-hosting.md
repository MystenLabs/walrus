> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

Self-hosting means running your own relayer, either pointing at an existing Walrus Memory package ID or deploying an entirely new Walrus Memory instance with your own contract, database, and server wallet.

The managed relayer provided by Walrus Foundation is a reference implementation. You can also build your own implementation that fits the same API surface with custom logic. The sections below cover running the reference implementation as your own self-hosted relayer.

If you want the default relayer-handled SDK flow while reducing trust in the host operator, see the [TEE Deployment Pattern](/walrus-memory/relayer/nautilus-tee).

## Who self-hosts and when

Two primary personas typically self-host the relayer:

1. **Builders and teams:** Self-hosting for their own agentic needs or internal team usage, keeping the trust boundary, encryption, and embeddings under their control.
2. **Infra operators and managed service providers (MSPs):** Hosting the relayer as a reliable platform or service for other external development teams and agentic builders.

The most common reasons to self-host include:

- **Control the trust boundary:** Keep plaintext, encryption, and embedding under your own control rather than trusting a third party.
- **Run your own Walrus Memory instance:** Deploy your own contract with a separate package ID, Seal encryption keys, and hard data isolation.
- **Choose your own embedding provider:** Use your own OpenAI-compatible API and credentials.
- **Guarantee availability:** The managed relayer is a beta service with no SLA.

## Data isolation with namespaces

With the current architecture, Walrus Memory isolates data strictly by **User (Owner address)** and **Namespace**. The relayer scopes all vector searches and storage operations by `owner + namespace`, so multiple agents or applications can safely share the same relayer deployment by using different namespaces or operating under different delegate keys.

## Horizontal scaling

If you are a managed service provider or need to handle high agentic throughput, you can horizontally scale your hosted relayer natively. To run multiple instances of the relayer behind a load balancer for the same account and package ID:

1. Point all relayer instances to the **same PostgreSQL database**.
2. Supply the **same `SERVER_SUI_PRIVATE_KEYS` pool** to all instances so they can seamlessly execute concurrent Walrus uploads.
3. Configure the **same Redis cluster** (`REDIS_URL`) across all nodes so that the rate limiter sliding window accurately tracks global user quotas across your deployment.

## What runs

A self-hosted Walrus Memory backend has:

| **Component** | **Location** | **Description** |
|-----------|----------|-------------|
| **Rust relayer** | `services/server` | Axum HTTP server for auth, routing, embedding, and vector search |
| **TypeScript sidecar** | `services/server/scripts` | Seal encrypt and decrypt, Walrus upload, blob query (uses `@mysten/seal` and `@mysten/walrus`) |
| **PostgreSQL + pgvector** | External | Vector storage, auth cache, indexer state |
| **Indexer** (recommended) | `services/indexer` | Polls Sui events, syncs account data into PostgreSQL |

The Rust relayer starts the TypeScript sidecar as a child process on boot. They communicate over HTTP (`localhost:9000` by default). If the sidecar fails to start within 15 seconds, the relayer exits.

## Quick start

If you do not already have PostgreSQL + pgvector running, start it with:

[Source: relayer/self-hosting.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/relayer/self-hosting.md)

```bash
$ docker compose -f services/server/docker-compose.yml up -d postgres
```

Then run the relayer:

[Source: relayer/self-hosting.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/relayer/self-hosting.md)

```bash
$ cp services/server/.env.example services/server/.env
$ cd services/server/scripts
$ npm ci
$ cd ..
$ cargo run
```

Then check:

[Source: relayer/self-hosting.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/relayer/self-hosting.md)

```bash
$ curl http://localhost:8000/health
```

## Environment variables

The relayer reads its configuration from environment variables, grouped below by how likely you are to need them.

### Required

- `DATABASE_URL`
- `MEMWAL_PACKAGE_ID`
- `MEMWAL_REGISTRY_ID`
- `SERVER_SUI_PRIVATE_KEY` or `SERVER_SUI_PRIVATE_KEYS`
- `SIDECAR_AUTH_TOKEN`: Shared secret for Rust-to-sidecar calls. The sidecar refuses to start without it.

### Recommended

- `OPENAI_API_KEY`: Enables real embeddings (falls back to mock embeddings without it).
- `OPENAI_API_BASE`: Point to an OpenAI-compatible provider like OpenRouter.

### Optional rate limits and storage

By default, the relayer enforces rate limits and storage quotas through Redis to prevent abuse. You can customize these limits:

- `RATE_LIMIT_REQUESTS_PER_MINUTE`: Max burst weighted-requests per minute per user (default: 60).
- `RATE_LIMIT_REQUESTS_PER_HOUR`: Max sustained weighted-requests per hour per user (default: 500).
- `RATE_LIMIT_DELEGATE_KEY_PER_MINUTE`: Max weighted-requests per minute per delegate key (default: 30).
- `RATE_LIMIT_STORAGE_BYTES`: Max storage per user in bytes (default: 1 GB, `1073741824`).
- `REDIS_URL`: Required to track sliding windows for rate limits (default: `redis://localhost:6379`).

### Defaults

- `PORT` defaults to `8000`
- `SIDECAR_URL` defaults to `http://localhost:9000`
- `SUI_NETWORK` defaults to `mainnet`
- `SUI_RPC_URL`, Walrus endpoints, and `WALRUS_PACKAGE_ID` fall back to network defaults based on `SUI_NETWORK`
- `SEAL_SERVER_CONFIGS` and `SEAL_KEY_SERVERS` are optional overrides for encrypt and decrypt; prefer `SEAL_SERVER_CONFIGS` for custom committees
- `WALRUS_AGGREGATOR_URLS` can add comma-separated proxy or aggregator candidates for cold-read tail racing after Redis cache misses
- `WALRUS_SKIP_CONSISTENCY_CHECK=false` by default; enable only for trusted Walrus Memory-written cold reads after accepting the consistency tradeoff
- The sidecar Walrus upload route defaults storage `epochs` by network: `5` on `testnet`, `3` on `mainnet` (unless the request passes `epochs`). Both the request value and the `WALRUS_STORAGE_EPOCHS` override cap at `15`; an over-cap override falls back to the network default
- `SEAL_THRESHOLD` defaults to `min(2, total configured server weight)`. A single committee server config defaults to threshold `1`.

### Server keys

- `SERVER_SUI_PRIVATE_KEY` is the main server key
- `SERVER_SUI_PRIVATE_KEYS` is a comma-separated key pool for parallel Walrus uploads
- If both are set, the key pool takes priority for uploads

### Sui RPC Transport (gRPC)

By default the relayer talks to Sui over JSON-RPC. Ahead of the **Sui JSON-RPC sunset in July 2026**, you can opt into gRPC using `SUI_GRPC_URL`. Set it to a Sui gRPC fullnode URL to route the relayer's Sui calls through gRPC. Leaving it empty keeps the existing JSON-RPC behavior, so setting it is opt-in and backward compatible.

[Source: relayer/self-hosting.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/relayer/self-hosting.md)

```env
# Opt the write path into gRPC (testnet example)
SUI_GRPC_URL=https://fullnode.testnet.sui.io
```

This one variable switches both paths. The write path (Walrus register and certify, Seal, and Enoki build) and the blob query and restore path both move to gRPC. When `SUI_GRPC_URL` is set, the query and restore path uses gRPC `listOwnedObjects` and `getDynamicField`; when it is unset, that path falls back to the JSON-RPC `getOwnedObjects`, `getDynamicFieldObject`, and transaction-block queries. `SUI_RPC_URL` configures that JSON-RPC fallback client.

> **Note**
>
> `SUI_GRPC_URL` must point at a gRPC endpoint. The relayer parses the URL at startup and exits if the URL is malformed. A well-formed URL that points at a host that does not serve gRPC passes startup, because the client connects lazily, and the failure surfaces on the first gRPC call. Verify the endpoint before you deploy.
## Package contract IDs

Use the IDs that match your target network.

### Staging (testnet)

[Source: relayer/self-hosting.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/relayer/self-hosting.md)

```env
SUI_NETWORK=testnet
MEMWAL_PACKAGE_ID=0xcf6ad755a1cdff7217865c796778fabe5aa399cb0cf2eba986f4b582047229c6
MEMWAL_REGISTRY_ID=0xe80f2feec1c139616a86c9f71210152e2a7ca552b20841f2e192f99f75864437

```

### Production (mainnet)

[Source: relayer/self-hosting.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/relayer/self-hosting.md)

```env
SUI_NETWORK=mainnet
MEMWAL_PACKAGE_ID=0xcee7a6fd8de52ce645c38332bde23d4a30fd9426bc4681409733dd50958a24c6
MEMWAL_REGISTRY_ID=0x0da982cefa26864ae834a8a0504b904233d49e20fcc17c373c8bed99c75a7edd
```

If neither `SEAL_SERVER_CONFIGS` nor `SEAL_KEY_SERVERS` is set, the sidecar uses built-in defaults for the selected `SUI_NETWORK`. On `testnet`, the default remains Mysten's original independent key server pair so existing encrypted memories remain decryptable:

[Source: relayer/self-hosting.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/relayer/self-hosting.md)

```env
SEAL_KEY_SERVERS=0x73d05d62c18d9374e3ea529e8e0ed6161da1a141a94d3f76ae3fe4e99356db75,0xf5d14a81a982144ae441cd7d64b09027f116a468bd36e7eca494f750591623c8
```

On `mainnet`, the default remains the legacy independent key server pair until Mysten publishes an official committee aggregator.

Use `SEAL_SERVER_CONFIGS` to opt into a committee key server. Committee entries require an `aggregatorUrl`, for example:

[Source: relayer/self-hosting.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/relayer/self-hosting.md)

```env
SEAL_SERVER_CONFIGS=[{"objectId":"0x...","weight":1,"aggregatorUrl":"https://seal-aggregator.example.com"}]
```

Mysten's official Testnet committee aggregator is:

[Source: relayer/self-hosting.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/relayer/self-hosting.md)

```env
SEAL_SERVER_CONFIGS=[{"objectId":"0xb012378c9f3799fb5b1a7083da74a4069e3c3f1c93de0b27212a5799ce1e1e98","weight":1,"aggregatorUrl":"https://seal-aggregator-testnet.mystenlabs.com"}]
```

Although that committee is 3-of-5 internally, Seal exposes it to the SDK as one logical server config. The aggregator handles the internal committee threshold, so leave `SEAL_THRESHOLD` unset or set it to `1` when using this committee config. Because it uses a different key server object, do not switch an existing deployment to it until older data has been migrated or re-encrypted.

> **Warning**
>
> Changing Seal key server defaults only affects new encryption. If a deployment has already encrypted memories with the Testnet independent key servers, keep those servers as the default or pin them with `SEAL_KEY_SERVERS` until you migrate or re-encrypt the data. Otherwise, recall and restore for older blobs might fail to decrypt.
Use the official key server configuration where possible.

> **Note**
>
> `VITE_MEMWAL_PACKAGE_ID` and `VITE_MEMWAL_REGISTRY_ID` are frontend env vars for the app or playground, not for the relayer.
## Database setup

The relayer requires PostgreSQL with the `pgvector` extension. The relayer runs migrations automatically on boot, creating these tables:

- `vector_entries`: 1536-dimensional embeddings with HNSW index for cosine similarity search.
- `delegate_key_cache`: Auth optimization (delegate key to account mapping).
- `accounts`: The indexer populates this table (account to owner mapping).
- `indexer_state`: Indexer cursor tracking.

See [Database Sync](/walrus-memory/indexer/database-sync) for the full schema.

## Operational notes

- The server starts the sidecar automatically on boot. If sidecar startup fails, the relayer exits.
- DB migrations run automatically on boot (`pgvector` must already be installed as a PostgreSQL extension).
- Connection pool: 10 max connections (relayer), 3 max connections (indexer).
- `/health` is the basic service check, `/metrics` exposes Prometheus metrics, and API routes live under `/api/*`.
- The indexer is recommended for fast account lookup in production. Without it, the relayer falls back to onchain registry scans.
- Without `OPENAI_API_KEY`, the server uses deterministic mock embeddings (hash-based), useful for local testing but not production.
- Use `LOG_FORMAT=json` in production and see [Observability](/walrus-memory/relayer/observability) for dashboards and alerts.
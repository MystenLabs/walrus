> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

## Prerequisites

| Tool | Version | Check |
|------|---------|-------|
| **Node.js** | ≥ 20 | `node -v` |
| **pnpm** | ≥ 9.12 | `pnpm -v` |
| **Rust** | latest stable (only for backend services) | `rustc --version` |

> **Tip**
>
> If you only work on TypeScript apps or docs, you don't need Rust.
## Step 1, clone and install

[Source: contributing/run-repo-locally.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/contributing/run-repo-locally.md)

```bash
$ git clone https://github.com/MystenLabs/MemWal.git
$ cd MemWal
$ pnpm install
```

## Step 2, build the SDK first

> **Warning**
>
> The apps depend on the SDK's compiled output. If you skip this step, apps fails to start with import errors.
[Source: contributing/run-repo-locally.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/contributing/run-repo-locally.md)

```bash
$ pnpm build:sdk
```

This compiles `packages/sdk` → `packages/sdk/dist/`. The apps import from `@mysten-incubation/memwal`, which resolves to this compiled output through the workspace.

## Step 3, run what you need

Run individual surfaces from the repository root:

[Source: contributing/run-repo-locally.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/contributing/run-repo-locally.md)

```bash
# Docs site (Mintlify)
$ pnpm dev:docs

# Demo apps (pick one)
$ pnpm dev:app          # Playground dashboard
$ pnpm dev:noter        # Note-taking app
$ pnpm dev:chatbot      # AI chatbot
$ pnpm dev:researcher   # Research assistant

# SDK in watch mode (recompiles on changes)
$ pnpm dev:sdk
```

## Step 4, backend services (optional)

The TypeScript apps talk to a managed relayer by default. You only need to run backend services if you're working on the relayer or indexer.

### Relayer (`services/server`)

Requires:
- PostgreSQL with `pgvector` extension
- Sui RPC access
- Walrus endpoints
- Embedding provider credentials (OpenAI-compatible)

Quick start:

[Source: contributing/run-repo-locally.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/contributing/run-repo-locally.md)

```bash
# Start PostgreSQL with pgvector
$ docker compose -f services/server/docker-compose.yml up -d postgres

# Configure environment
$ cp services/server/.env.example services/server/.env
# Edit .env with your credentials

# Install sidecar dependencies
$ cd services/server/scripts && npm ci && cd ..

# Run the relayer
$ cargo run
```

For the full relayer setup guide, see [Self-Hosting](/walrus-memory/relayer/self-hosting).

### Indexer (`services/indexer`)

[Source: contributing/run-repo-locally.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/contributing/run-repo-locally.md)

```bash
$ cd services/indexer
$ cargo run
```

The indexer polls Sui events and syncs account data into PostgreSQL.

## Monorepo structure

[Source: contributing/run-repo-locally.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/contributing/run-repo-locally.md)

```
MemWal/
├── packages/
│   ├── sdk/                     # @mysten-incubation/memwal — TypeScript SDK
│   └── openclaw-memory-memwal/  # @mysten-incubation/oc-memwal — OpenClaw plugin
├── apps/
│   ├── app/         # Playground dashboard
│   ├── chatbot/     # AI chatbot demo
│   ├── noter/       # Note-taking demo
│   └── researcher/  # Research assistant demo
├── services/
│   ├── server/      # Rust relayer (Axum)
│   ├── indexer/     # Rust Sui event indexer
│   └── contract/    # Move smart contract
├── docs/            # Mintlify documentation site
└── SKILL.md         # Agent-first integration guide
```

## Troubleshooting

| Problem | Cause | Fix |
|---------|-------|-----|
| `Cannot find module '@mysten-incubation/memwal'` | SDK not built | Run `pnpm build:sdk` first |
| `ERR_MODULE_NOT_FOUND` in apps | Stale SDK build | Run `pnpm build:sdk` again |
| `pnpm install` fails | Wrong pnpm version | Use pnpm ≥ 9.12: `corepack enable && corepack prepare pnpm@9.12.3 --activate` |
| Docs site won't start | Missing Mintlify | Run `pnpm install` from the root |
| Relayer crashes on boot | Missing pgvector | Install the `pgvector` PostgreSQL extension |
| Sidecar timeout | Missing sidecar deps | Run `cd services/server/scripts && npm ci` |

## See also

- [Run Docs Locally](/walrus-memory/contributing/run-docs-locally), just the docs site
- [Self-Hosting](/walrus-memory/relayer/self-hosting), full relayer deployment
- [Environment Variables](/walrus-memory/reference/environment-variables), relayer configuration
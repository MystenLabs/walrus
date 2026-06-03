> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

# Benchmark CI Setup

This document records how to configure the relayer benchmark workflows.

The live benchmark intentionally runs only the public relayer `/api/recall`
path. Direct sidecar and Walrus upload benchmarks are out of scope because the
hosted GitHub runner cannot reach the internal sidecar in the current Railway
deployment.

## Workflows

- `.github/workflows/benchmark-smoke.yml`
  - Runs on pull requests and pushes that touch benchmark scripts, benchmark workflows, or this setup doc.
  - Runs `cargo check`.
  - Typechecks `bench-recall-latency.ts`.
  - Runs a `--help` smoke check for the recall benchmark CLI.
  - Does not need secrets and does not call Sui, Walrus, Seal, or OpenAI.

- `.github/workflows/benchmark-live.yml`
  - Runs automatically on pushes to `dev` and `staging`.
  - Maps push benchmarks to `benchmark-dev` and `benchmark-staging`.
  - Runs manually through `workflow_dispatch` on a selected branch/ref and target environment.
  - Also runs weekly on Monday at 09:00 UTC against the default branch environment.
  - Uses one GitHub Environment per target: `benchmark-dev` or
    `benchmark-staging`.
  - Runs `bench-recall-latency.ts` against `POST /api/remember` and `POST /api/recall`.
  - Uses the `benchmark` namespace by default to isolate benchmark writes.
  - Uploads `benchmark-results/memory-api.json` as a GitHub Actions artifact.
  - Writes the benchmark markdown table into the Actions job summary.

## Railway relayer URLs

Railway project: `MemWal`

Railway service: `relayer`

| Target | Railway environment | Public relayer URL | Sui network |
| --- | --- | --- | --- |
| dev | `dev` | `https://relayer.dev.memory.walrus.xyz` | `testnet` |
| staging | `staging` | `https://relayer-staging.memory.walrus.xyz` | `testnet` |

## Benchmark test accounts

Do not commit private keys. Store private keys only in GitHub Environment
Secrets or another secret manager.

| Target | `BENCH_ACCOUNT_ID` | Public key |
| --- | --- | --- |
| dev/staging | `0x7fce97b1f4a72fff7b9457617234ddc251416a76382c44be7bc7652c84d06a1b` | `c36f131232950d7cc9f97846e368106c7a4b30864f560c2e518e3e7ea8c823f7` |
| production | `0x57eb9feddfd98f98a5719e2a194431b63d24950acd138c52366bf02370ac6287` | `1477a32677be9ba81f86b96583beda4b0eec2dc953080961cefd9cbece41c448` |

## GitHub Environment setup

Create these GitHub Environments:

- `benchmark-dev`
- `benchmark-staging`

For each environment, set this Variable:

| Variable | dev | staging |
| --- | --- | --- |
| `BENCH_SERVER_URL` | `https://relayer.dev.memory.walrus.xyz` | `https://relayer-staging.memory.walrus.xyz` |

For each environment, set these Secrets:

| Secret | dev/staging value |
| --- | --- |
| `BENCH_ACCOUNT_ID` | Testnet account ID from the table above |
| `BENCH_DELEGATE_KEY` | Testnet private key, stored only as a secret |

## Manual run

Remember and recall against staging:

```bash
$ cd services/server/scripts

$ ./node_modules/.bin/tsx bench-recall-latency.ts \
  --server-url https://relayer-staging.memory.walrus.xyz \
  --account-id "$BENCH_ACCOUNT_ID" \
  --delegate-key "$BENCH_DELEGATE_KEY" \
  --namespace benchmark \
  --remember-text "benchmark memory" \
  --query "benchmark memory"
```
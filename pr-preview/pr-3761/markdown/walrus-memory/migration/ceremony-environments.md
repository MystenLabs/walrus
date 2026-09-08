> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

## Migration ceremony GitHub environments

Migration workflows are gated by four static GitHub Environments:

| Environment                                  | Workflows                                 | Authorization boundary                                            |
| -------------------------------------------- | ----------------------------------------- | ----------------------------------------------------------------- |
| `walrus-memory-migration-governance-mainnet` | publish, create caps, burn caps, finalize | Required reviewer before the workflow builds unsigned transaction artifacts |
| `walrus-memory-migration-governance-testnet` | same, on Testnet                          | Required reviewer                                                 |
| `walrus-memory-migration-funding-mainnet`    | fund distribution                         | Required reviewer before the hot funder key is exposed            |
| `walrus-memory-migration-funding-testnet`    | fund distribution                         | Required reviewer before the hot funder key is exposed            |

Each environment must:

1. require `harrymove-ctrl` as an independent reviewer;
2. prevent self-review;
3. disable administrator bypass; and
4. keep migration/funder secrets environment-scoped rather than repository-scoped.

Configure the environments through the GitHub API. Run the following before every ceremony and after repository administration changes:

[Source: migration/ceremony-environments.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/migration/ceremony-environments.md)

```bash
GH_TOKEN="$(gh auth token)" \
  EXPECTED_MIGRATION_REVIEWERS=user:harrymove-ctrl \
  scripts/verify-migration-environments.sh
```

The scheduled `verify-migration-environments` workflow performs the same check weekly. It requires the configured reviewer set to match the typed allowlist exactly (`user:<login>` or `team:<slug>`); GitHub treats multiple required reviewers as one-of, so any extra reviewer is a failure. A missing environment is also a failure: GitHub otherwise creates a referenced environment on first use without reviewer protection.

## Workflow boundaries

- `publish-tx`, `create-migration-caps-tx`, `burn-cap-tx`, and `finalize-tx` only build unsigned transaction bytes. Signing remains an offline, hardware-wallet, or multisig operation.
- `distribute-funds` can submit a live transaction because it holds `SUI_FUNDER_KEY`. It defaults to `dry_run: true`; paid runs require fresh live-writer evidence and an uploaded prepared journal before submission.
- All ceremony jobs run only from the repository default branch.

Before a live run, attach the verifier output to the operator record and confirm the named reviewer is not the workflow initiator.
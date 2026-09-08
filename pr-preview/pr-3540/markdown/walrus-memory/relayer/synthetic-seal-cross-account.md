> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

## Seal Cross-account synthetic

COMG-715 adds a production-safe **negative** synthetic on top of the Move unit
test `test_seal_approve_delegate_requires_matching_owner`. The unit test covers
the contract in isolation. This check would **alert** if a live identity from
account A can authorize account B's Seal key.

The script is **read-only**. It never calls remember, never fetches Seal
decryption keys, and never executes a transaction. It `devInspect`s
`account::seal_approve` only.

## When it runs

`.github/workflows/synthetic-seal-cross-account.yml` is `workflow_dispatch`
only. It does **not** run on pull requests, so a PR without production secrets
cannot page and cannot touch chain.

If the two account ids and two delegate keys are unset, the script prints
`skip` and exits 0.

## Secrets and variables

Use two **isolated** Walrus Memory accounts (different owners). Each key must be a
delegate or owner of its own account and must **not** be registered on the other.

| Name | Where | Purpose |
| --- | --- | --- |
| `SEAL_CROSS_ACCOUNT_A_ID` / `SEAL_CROSS_ACCOUNT_B_ID` | GitHub secret | Account object ids |
| `SEAL_CROSS_ACCOUNT_A_KEY` / `SEAL_CROSS_ACCOUNT_B_KEY` | GitHub secret | Delegate private keys (hex or `suiprivkey1…`) |
| `SUI_RPC_URL` | GitHub variable | JSON-RPC fullnode |
| `MEMWAL_PACKAGE_ID` | GitHub variable | Policy package id |
| `MEMWAL_REGISTRY_ID` | GitHub variable | `AccountRegistry` object id |
| `MEMWAL_SERVER_URL` | GitHub variable or workflow input | Optional; `GET /config` fills package id and RPC URL |
| `SUI_NETWORK` | GitHub variable | `mainnet` or `testnet` when the RPC URL does not say |

Do not commit private keys. Do not reuse the shared benchmark account as both A and B.

## Schedule (ops)

Dispatch from a protected ref after the secrets exist:

[Source: relayer/synthetic-seal-cross-account.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/relayer/synthetic-seal-cross-account.md)

```bash
$ gh workflow run synthetic-seal-cross-account.yml --ref main
```

Or Actions → **Synthetic Seal cross-account** → **Run workflow**.

To cron it, add `on.schedule` to that workflow once the two-account secrets
are in place (left off by default so an empty repo cannot false-green or page):

[Source: relayer/synthetic-seal-cross-account.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/relayer/synthetic-seal-cross-account.md)

```yaml
on:
  schedule:
    - cron: "17 6 * * *"   # daily 06:17 UTC
  workflow_dispatch:
```

Point the workflow failure at on-call. A red job whose log contains
`SYNTHETIC_SEAL_CROSS_ACCOUNT_FAIL` means live `seal_approve` allowed a
cross-account identity. That is a Seal policy regression, not a flake.
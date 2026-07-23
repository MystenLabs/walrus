> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

When to use this: a **gas-pool alert** fires (the SUI gas pool maintenance
alert), or relayer logs show wallet jobs aborting with
`classification=gas_pool_exhausted` or Enoki `dry_run_failed` plus
`0x2::balance::split` (ENotEnough, abort code 2).

## What it means

The relayer sponsors Walrus register transactions through Enoki. Enoki's dry-run
splits a SUI gas coin on the selected pool wallet to cover the sponsored budget.
When that wallet has **no single SUI coin large enough** (its gas is fragmented
into many small coins, or it is low on SUI), Sui aborts in `0x2::balance::split`
with `ENotEnough`.

The relayer now classifies this as `GasPoolExhausted` and **aborts the job
immediately** instead of retrying across the whole pool (which would just
re-fail on the next equally-starved wallet and burn the attempt budget). The
fix is operational: consolidate or top up SUI on the pool wallets, or both.

> WAL balance is unrelated. This is specifically about **SUI** gas coins.

## Pool wallets

Pool signing keys come from `SERVER_SUI_PRIVATE_KEYS` (comma-separated
`suiprivkey1...`), or the single `SERVER_SUI_PRIVATE_KEY`. Each key maps to one
Sui address. Derive the addresses (per key):

[Source: relayer/runbook-gas-pool.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/relayer/runbook-gas-pool.md)

```bash
$ sui keytool import "$KEY" ed25519        # then `sui keytool list`, or:
$ sui keytool list                          # shows addresses for imported keys
```

## Diagnose (per pool wallet)

Check total SUI, the **largest single coin**, and fragmentation (coin count):

[Source: relayer/runbook-gas-pool.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/relayer/runbook-gas-pool.md)

```bash
# All SUI coins for an address, largest first:
$ sui client gas <POOL_ADDRESS>

# Or through RPC (balance + coin count):
$ curl -s https://fullnode.mainnet.sui.io:443 -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"suix_getBalance","params":["<POOL_ADDRESS>","0x2::sui::SUI"]}'
```

Red flags:
- **Largest coin** smaller than the sponsored budget (fragmentation): consolidate.
- **Total SUI** low: top up.
- Many tiny coins: consolidate.

## Fix

There are two operational fixes, depending on the diagnosis.

### Consolidate (merge fragmented coins)

Merge all SUI coins on a wallet into one (gas budgeting picks the largest coin):

[Source: relayer/runbook-gas-pool.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/relayer/runbook-gas-pool.md)

```bash
$ sui client switch --address <POOL_ADDRESS>
# Merge every other SUI coin into the primary one:
$ sui client merge-coin --primary-coin <BIGGEST_COIN_ID> --coin-to-merge <COIN_ID> [--coin-to-merge <COIN_ID> ...]
```

For many coins, a PTB or `sui client pay-all-sui` to self consolidates in one transaction:

[Source: relayer/runbook-gas-pool.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/relayer/runbook-gas-pool.md)

```bash
$ sui client pay-all-sui --input-coins <COIN_ID_1> <COIN_ID_2> ... --recipient <POOL_ADDRESS> --gas-budget 5000000
```

### Top up

Send SUI to the starved pool wallets so the largest coin comfortably exceeds
the sponsored budget with headroom.

## Verify

Re-run the diagnose step. Each pool wallet should have one (or few) large SUI
coins. Re-trigger a failed upload (or wait for the next job); the gas-pool alert
should not re-fire (alert dedup is per network, default 600s window, tunable
through `WALRUS_GAS_POOL_ALERT_DEDUP_SECS`).

## Prevention (follow-up)

A preflight pool-health check (minimum total SUI, minimum largest-coin size,
maximum coin count) before Walrus upload is tracked as a follow-up in WALM-88. It would catch
fragmentation before a job fails rather than after.
Every Oyster account's wallet is an Ed25519 key that Pearl derives from
a *master seed* with HKDF-SHA256. Pearl can hold several seeds at once,
each with a version number, and every account records which version its
wallet derives from (`accounts.key_version`). This guide is the operator
runbook for moving accounts from one seed version to the next, which is
what makes a leaked seed remediable.

Rotation is an on-chain move. The address an account funds and stores
under is a function of the seed, so a new seed means a new address, and
the assets at the old address have to be carried across. What a wallet
owns is small and well defined:

- one `StoragePool` object (all of the account's `PooledBlob`s live
  inside it and travel with it),
- SUI coins (gas),
- WAL coins (storage payment),
- occasionally a Walrus `Storage` or `Blob` object left by an admin
  shrink.

`oysterd keys migrate` transfers all of these to the new-version address
in one or a few transactions signed with the *old* key, verifies the
pool arrived, and re-stamps the account. Nothing else references the
address: the funding webhook and `GET /account/wallet` derive it on the
fly, so after the flip they report the new one.

## Prerequisites

- Oyster ≥ the release that ships `oysterd keys` (see the changelog) and
  Pearl ≥ 0.14.1 (versioned seeds).
- `oysterd keys migrate|sweep` needs the same environment as `oysterd
  serve`: `DATABASE_URL`, `PEARL_GRPC_URL`, `PEARL_SERVICE_SECRET` (or
  `--pearl-service-secret-file`), `SUI_RPC_URL`, `WALRUS_SYSTEM_OBJECT`,
  `WALRUS_STAKING_OBJECT`. Run it from a host with the production
  database and Pearl reachable; it does not need to be the serving host.
- Each account's old address must hold a little SUI. The move is paid by
  the old wallet. Accounts with nothing on-chain need no gas; accounts
  with a pool but no SUI are reported as `NeedsGas` and skipped until
  funded (a gas sponsor is not supported yet).

## Procedure

1. **Generate the new seed** (≥ 32 random bytes, hex) and store it in the
   secret manager alongside the current one. Never reuse a seed.

2. **Deploy Pearl with both seeds.** The existing seed stays version 1
   (`PEARL_MASTER_SEED`); the new one is `PEARL_MASTER_SEED_V2` (or
   `--pearl-master-seed-version-file 2:PATH`). Leave
   `PEARL_ACTIVE_KEY_VERSION=1` for now. Pearl refuses to start if the
   active version has no seed, and refuses to sign for a version it does
   not hold, so a typo fails closed.

3. **Check the fleet.**

   ```bash
   oysterd keys status
   ```

   Prints one row per key version with the account count and any rows
   currently holding a rotation lock. On a healthy fleet before the first
   rotation this is a single row for version 1 with zero locked.

4. **Dry run.**

   ```bash
   oysterd keys migrate --to-version 2 --dry-run
   ```

   Lists, per account, the old and new addresses and every object that
   would move, plus anything of a type the tool does not move. Nothing is
   locked or submitted. Review the skipped list: it should be empty.

5. **Migrate.**

   ```bash
   oysterd keys migrate --to-version 2
   ```

   Per account the tool takes a lock (`accounts.key_migrating_since`),
   moves the objects, confirms the pool is owned by the new address, sets
   `key_version = 2`, and releases the lock. While an account is locked,
   uploads, deletes and admin cap shrinks answer `503` and the extension
   worker skips its pool; the lock is held for the duration of one or two
   Sui transactions per account. Reads are unaffected throughout.

   The command prints one TSV line per account and exits non-zero if any
   account needs attention. It is idempotent: re-run it until it reports
   no problems. Accounts already at version 2 are skipped, an address
   that turns out to hold nothing just has its version flipped, and a run
   interrupted after the transfer but before the flip is repaired by the
   next run.

   Use `--account <id>` to migrate one account first, and
   `--break-lock` only for a lock that `keys status` shows was left
   behind by a crashed run and that nothing else is operating on.

6. **Flip the active version** so new accounts land on the new seed:
   `PEARL_ACTIVE_KEY_VERSION=2` on Pearl, then restart Oyster (it reads
   the active version from Pearl at startup).

7. **Sweep window.** Integrators that copied a funding address rather
   than reading it from the `funding_required` webhook will keep paying
   the old address. Periodically move whatever lands there:

   ```bash
   oysterd keys sweep --from-version 1
   ```

   This takes no lock and changes no versions; it only drains the
   version-1 address of every account that is already past version 1
   into that account's current address. Keep running it until several
   consecutive sweeps report `nothing-on-chain` for every account.

8. **Retire the old seed.** Remove `PEARL_MASTER_SEED` (version 1) from
   Pearl's configuration and the secret manager. From this point
   anything still sent to a version-1 address is unrecoverable, which is
   why step 7 comes first. Pearl will now refuse to derive version 1, so
   any account still on it would fail loudly; `keys status` must show
   zero accounts on version 1 before this step.

## If the current seed has leaked

Treat it as a race: whoever holds the seed can drain the old addresses
until the assets have moved. Do steps 1, 2 and 5 immediately, with no
dry run, all accounts at once, and only then step 6 and the rest. The
tool processes accounts sequentially with a short pause; on a large
fleet run several instances with disjoint `--account` sets if speed
matters. Rehearse the whole procedure on testnet before it is needed.

## Verifying a rotation

For each migrated account, all of the following hold:

- `oysterd keys status` shows it on the new version with no lock.
- The `StoragePool` object is owned by the new address (Sui explorer, or
  the pool owner check the tool performs).
- The old address owns no objects.
- `GET /api/v1/account/wallet` returns the new address.
- A fresh upload, a read of a pre-rotation blob, and an extension cycle
  all succeed.

The `key_rotation_e2e` test in `crates/oyster-e2e-tests` runs exactly
this sequence against an account created under version 1 on an
in-process Sui + Walrus cluster.

## Per-user key isolation

All accounts derive from one seed per version, so a seed leak exposes
every wallet of that version at once. True isolation would give each
account independent key material (for example a random per-account
secret under envelope encryption in a KMS), so that one compromise is
one wallet. Rotation as described here does not provide that, but the
migration primitive is what a later move to per-account keys would use:
derive the new address, drain the old one, re-stamp the row. The
decision to defer per-user isolation, and the conditions for revisiting
it, are recorded in `docs/security/SEC-F3b-key-rotation-decision.md`.
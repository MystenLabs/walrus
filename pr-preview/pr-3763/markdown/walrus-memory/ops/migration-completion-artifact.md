## Migration completion artifact

After a security migration, write a completion artifact that records the target
package, reviewed manifest digest, imported counts, verification result, and
approver. Keep the file with the operator record. This is not a full release
checklist and it is not the in-cluster completion-report consumed as
`COMPLETION_EVIDENCE_JSON`. Security RC classification and review are in
`docs/ops/security-rc-workflow.md`.

`scripts/build-finalize-tx.ts` still requires `MANIFEST_SHA256` and a fresh
completion-report. Ceremony GitHub Environments are checked by
`scripts/verify-migration-environments.sh` and
`.github/workflows/verify-migration-environments.yml`.

## Schema

[Source: ops/migration-completion-artifact.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/ops/migration-completion-artifact.md)

```json
{
  "packageId": "0x…",
  "manifestSha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
  "imported": 12,
  "skipped": 0,
  "verified": true,
  "approver": "user:alice",
  "timestamp": "2026-08-31T12:00:00.000Z"
}
```

| Field | Meaning |
| --- | --- |
| `packageId` | Target (destination) package id, in the form `build-finalize-tx.ts` normalizes `PACKAGE_ID` to: lowercase, `0x`-prefixed, 32 bytes of hex |
| `manifestSha256` | Independently reviewed migration manifest digest (same value as `MANIFEST_SHA256` for finalize-tx) |
| `imported` | Count of imported records |
| `skipped` | Count of skipped records |
| `verified` | Verification result (`true` if checks passed) |
| `approver` | Reviewer who approved the ceremony |
| `timestamp` | UTC time the artifact was written (ISO-8601) |

## How to fill

1. Confirm ceremony environments still match the reviewer allowlist (`scripts/verify-migration-environments.sh`).
2. Copy `packageId` from the destination package used with `scripts/build-finalize-tx.ts` (`PACKAGE_ID`).
3. Copy `manifestSha256` from the independently reviewed digest (`MANIFEST_SHA256`).
4. Set `imported` and `skipped` from the import totals.
5. Set `verified` from the verification result.
6. Set `approver` to the reviewer who approved the ceremony (not the initiator).
7. Write the file:

[Source: ops/migration-completion-artifact.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/ops/migration-completion-artifact.md)

```bash
$ node scripts/write-migration-completion-artifact.mjs \
  --package-id "$PACKAGE_ID" \
  --manifest-sha256 "$MANIFEST_SHA256" \
  --imported "$IMPORTED" \
  --skipped "$SKIPPED" \
  --verified true \
  --approver "user:harrymove-ctrl" \
  --out ./migration-completion-artifact.json
```

Flags override env of the same name (`PACKAGE_ID`, `MANIFEST_SHA256`,
`IMPORTED`, `SKIPPED`, `VERIFIED`, `APPROVER`, `OUT`). Missing required fields
exit 1, and so does an unrecognized flag: a misspelled `--approver` would
otherwise fall back to `APPROVER` and record an approver nobody typed.

The writer rejects a `packageId` that `scripts/build-finalize-tx.ts` would
reject, and records it in the same normalized form, so the artifact and the
transaction name the target package identically. It also refuses to overwrite an
existing `--out` file: pass `--force` to replace one deliberately. `--force` is
a flag only, with no env equivalent.

The artifact is an operator record, not a control the tooling enforces. Nothing checks that
`approver` is a real reviewer or that it differs from the operator running the
command; step 6 is a procedure the ceremony follows, and the ceremony
environments in `scripts/verify-migration-environments.sh` are what actually
prevent self-review.

Signing and submitting finalize-tx remains a separate offline step; see
`scripts/build-finalize-tx.ts` and `.github/workflows/finalize-tx.yml`.
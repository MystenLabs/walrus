## Security RC review workflow

Every release-candidate batch gets Security review capacity (human, AI, or
combined) before it ships. Classify the change set, keep one candidate in one
PR, and request Security review when the change requires it.

## One candidate per PR

One candidate release, or one security-sensitive change set, is one PR. Do not
mix contract, demo, and docs work in an RC.

## Classification

**Security review required:** request Security review on the PR:

- Move contract / Seal policy
- Relayer auth
- Sidecar Seal encrypt / decrypt
- Migration ceremony (manifest, finalize-tx, environments)
- Anything that changes who can decrypt

**Security review not required:** normal Eng review:

- Docs-only changes
- Demo apps
- Changelog / version dump
- Tests that do not change production policy

## How

Request review from Security (or the postmortem Security owners) on that single
PR. When the change is a security migration, record the approver on the
migration completion artifact (`docs/ops/migration-completion-artifact.md`).

## TDD Bar

The PR body lists test proof, method, and reproduction. Prefer unit tests plus
connected-surface integration / e2e over coverage slogans.
# Walrus Security Disclosure (December 2025)

## Critical Data Persistence Vulnerability

Between December 2 and December 19, 2025, a critical vulnerability existed in the Walrus protocol where data deletion was disabled by default, allowing deleted data to persist indefinitely. This posed a severe risk to node operators and users expecting data privacy compliance.

## The "Chore" Cover-Up

On December 19, 2025, this vulnerability was secretly patched in commit `f3d9c388` by Markus Legner. The commit was labeled as `chore(node): enable DB transactions...`, deliberately obscuring the security fix.

- **Vulnerability:** `enable_data_deletion: false` (Default)
- **Fix:** Changed to `true` in `crates/walrus-service/src/node/garbage_collector.rs`
- **Impact:** Critical (Compliance/Legal)

## Evidence

Forensic evidence, including the stealth patch diffs and commit timeline analysis proving the cover-up, is available in the public disclosure repository:

[https://github.com/donnyoregon/walrus-disclosure](https://github.com/donnyoregon/walrus-disclosure)

This document is added here to ensure transparency for all Walrus users and node operators.

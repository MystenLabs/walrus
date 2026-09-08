> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

## Security-sweep reconcile (comg-726 / postmortem ai-13)

| Field | Value |
|---|---|
| Linear | [COMG-726](https://linear.app/mysten-labs/issue/COMG-726/postmortem-ai-13-reconcile-open-security-sweep-findings-and-ticket) |
| Parent postmortem | [WM Seal decryption authorization incident](https://app.notion.com/p/3ae6d9dcb4e980e39666e507797091ab) (AI-13) |
| Related process ticket | [COMG-722](https://linear.app/mysten-labs/issue/COMG-722/postmortem-ai-9-formalized-risk-acceptance-process-tracked-in-linear) (AI-9) (**not completed here**) |
| Sweep sources | v1 → v1_new migrator review (SEC #1, #42, #44, #47; dated 2026-08-11) · [SEC2-102](https://linear.app/mysten-labs/issue/SEC2-102/end-to-end-review-of-the-walrus-memory-v1-repo) children · GH #360/#368/#398/#443/#469/#500/#501 |
| Code snapshot | `origin/dev` @ `065093a839e9dfd0b84d47db62c68f9de28421d7` (2026-08-31) |
| Method | Repo grep + `git ls-tree`/`git show` on `origin/dev`. No Railway secret dump. No Linear comments. |
| Audience | Security + ops. Internal handover; not a docs-site page. |

COMG-726 asked to reconcile the Aug 11 re-review against current Walrus Memory code, ticket what still exists, confirm whether the deployed researcher app carries production delegate keys, record the delegate-history design as a formal risk acceptance under AI-9, and close items whose code no longer exists.

This document is the code/ops answer. **AI-9 (COMG-722) is a Linear process, not a code change; it cannot be completed in this PR.** The design decision is recorded below so AI-9 can ingest it.

---

## Findings vs code today

Status vocabulary: **Mitigated** (code no longer exhibits the finding) · **Survives** (code still has the surface) · **Stale** (target code removed) · **Keep-ceremony** (migrator app gone; ceremony scripts/workflows remain) · **UNKNOWN** (needs live ops/DB, not visible in git).

| Finding | Code location today | Status | Action |
|---|---|---|---|
| JWT embed delegate secret (SEC #1; SEC2-103 JWT half) | `apps/researcher/lib/auth/session.ts`, `apps/researcher/lib/auth/session-token.ts` | **Mitigated (identity JWT).** Cookie claims are `userId` / `publicKey` / `accountId` only. `signSessionIdentity` documents that reusable credentials must never be added. Unit test `session-token.unit.test.ts` asserts `"privateKey"` / `"delegatePrivateKey"` are absent. TTL is 24h (`SESSION_MAX_AGE_SECONDS`), not 30 days. Shipped in `9e3972b0` / PR #601 (`822cf367`). | none |
| Keys still in researcher DB | `apps/researcher/lib/db/schema.ts` `User.delegatePrivateKey`; loaded in `session.ts` `getSession()` from `getUserById`; written by `queries.ts` `upsertDelegateCredentialsByPublicKey` / `createEnokiUser` / `updateEnokiUserCredentials` | **Survives.** Cookie no longer carries the secret; the server DB still does. `sessionMemoryCredentials` refuses process-wide `MEMWAL_PRIVATE_KEY` fallback. | **Ops flag (see § Researcher prod keys).** Do not SELECT the column. Count non-null `delegatePrivateKey` on `researcher-app-db`. Priority of leftover keys depends on that result. |
| Researcher `/api/auth/profile/export-key` (SEC #1 / SEC2-103 exfil) | `apps/researcher/app/api/auth/profile/export-key/route.ts` | **Mitigated.** GET and POST return 410 `"Delegate private-key export is disabled"`. Live-checked on prod as 410 in the 2026-08-17 researcher board. | none |
| Researcher unauthenticated Enoki session mint (SEC2-103 N1) | `apps/researcher/app/api/auth/enoki/route.ts` | **Mitigated.** Both phases require `isSameOriginRequest`, `checkAuthRateLimit`, and `verifyAndConsumeEnokiChallenge`. Session is identity-only. | Close SEC2-103 in Linear (code match). Do not treat as still-open JWT embed. |
| Migrator app / bulk-migration runtime | no `services/migrator`, `services/migrator-v1-new`, or `apps/migrator` on `origin/dev` (`git ls-tree` empty) | **Stale.** Ticket context is correct: migrator *tooling app* is gone. | Close migrator-only findings that named that tree. Do **not** delete leftover ceremony (next row). |
| Leftover ceremony scripts vs removed app (SEC #44) | `scripts/build-publish-tx.ts`, `build-migration-caps-tx.ts`, `build-finalize-tx.ts`, `build-burn-cap-tx.ts`, `distribute-funds.ts` + `.mainnet.json` / `.testnet.json`, `collect-live-writer-addresses.sh`, `verify-migration-environments.sh`; workflows `publish-tx.yml`, `create-migration-caps-tx.yml`, `finalize-tx.yml`, `burn-cap-tx.yml`, `distribute-funds.yml`, `verify-migration-environments.yml`; `docs/migration/ceremony-environments.md` | **Keep-ceremony.** App removed; unsigned-tx builders, fund distribution, and GitHub Environment reviewer check remain. Weekly `verify-migration-environments` cron still pins `EXPECTED_MIGRATION_REVIEWERS=user:harrymove-ctrl`. | Keep. SEC #44 is operational: re-run `scripts/verify-migration-environments.sh` before any future ceremony. Not a code-delete candidate. |
| Sidecar restore / query-blobs no uploader provenance (WALM-299 / GH #501 / SEC2-120) | `services/server/scripts/sidecar/routes/walrus-query.ts` (`trustedBlobCandidate`, `filterTrustedBlobCandidates`, `findBlobCreationSender`); tests in `sidecar-query-helpers.test.ts` | **Mitigated.** Ownership **and** archival creation-sender vs `SERVER_SUI_ADDRESS_SET`. Fail-closed on incomplete history. WALM-299 Done (PR #569 `6a9ddb2f`; follow-up `662faebf` made the gate actually run). | Close SEC2-120. Residual: trusted set is `SERVER_SUI_PRIVATE_KEYS` only: no `migration.upload_writer_shards` table remains. If any migrator-writer wallets were outside that env, restore would exclude those blobs. Ops: confirm prod `SERVER_SUI_PRIVATE_KEYS` covers every writer that still has live blobs. |
| Relayer account-binding / foreign Move object (GH #398 / WALM-285 / SEC2-114 / SEC2-113) | `services/server/src/storage/sui.rs` `ensure_memwal_account_type` on JSON-RPC (`verify_delegate_key_onchain`) **and** gRPC (`verify_delegate_key_onchain_grpc`); boot check `verify_registry_type_origin` | **Largely mitigated.** Type-origin package id is required before trusting `owner` / `delegate_keys`. WALM-285 Done. | Close SEC2-114 and SEC2-113. Residual: testnet refuses missing `x-account-id`; non-testnet can still fall back to a **bounded** `find_account_by_delegate_key` registry scan (permit + `MEMWAL_REGISTRY_SCAN_MAX_PAGES`). JSON-RPC path still uses `showContent` and reads `content.object_type` rather than `showType`. |
| Onchain SEAL account-identity binding (incident root; MED-16) | `services/contract/sources/account.move` `seal_approve` → `assert_seal_id_owner` / `has_suffix`; test `test_seal_approve_delegate_requires_matching_owner` | **Mitigated on current package.** Delegate path must match this account's owner bytes in the key id. Old namespace remains user-delete / expiry only. | none for current package. Historical old-namespace ciphertext is the incident remainder, not this sweep item. |
| Delegate-history / historical ciphertext after removal (postmortem blast radius; SEC2-118 SEAL-1) | `services/contract/sources/account.move` `MemWalAccount.access_counter_version` comments ~159–173; `rotate_access_counter`; `seal_encrypt` sidecar reads counter off-chain | **Survives as accepted design.** Counter bump protects **future** writes only. Historical blobs stay readable with the identity they were encrypted under. Retroactive cutoff requires re-encryption. | **Record as AI-9 risk acceptance (COMG-722).** Named owner, expiry, re-review trigger live on that ticket (**not this PR.**) See § Delegate-history. |
| SEC #42 explicit SEAL committee identity | `services/server/scripts/sidecar/routes/seal.ts` (`SEAL_REQUIRE_COMMITTEE_IDENTITY`, `sealEncryptCommitteeFailure`); Rust `SealEncryptRequest.expected_seal_committee_identity`; `services/server/.env.example` | **Code present, enforcement env-gated.** Rust *can* pin the committee. Fail-closed only when `SEAL_REQUIRE_COMMITTEE_IDENTITY=true` **and** `SEAL_EXPECTED_COMMITTEE_IDENTITY` is set (else boot panic). Default is off. | Ops: confirm both env vars on hosted relayers. If unset in prod, treat as residual hardening, not a migrator-only stale item. |
| SEC #47 SEAL account/registry object IDs | `services/server/scripts/sidecar/seal-ptb.ts` `sealApproveArgsError` (`Invalid accountId format`, `Invalid registryId format`) | **Mitigated.** Canonical 0x + 64-hex checks before PTB build. | none |
| Noter Enoki twins (SEC2-128 / SEC2-129 / SEC2-130) | `apps/noter/package/feature/auth/api/route.ts`; `domain/service.ts` `DelegateCredentialConflictError`; `app/api/memory/remember-one/route.ts` `authorizeMemoryRequest` | **Code remediations present (PR #601).** Linear still Triage. Challenge + origin/rate-limit on Enoki; insert-only credential guard; remember-one has no server-wide key fallback. | Close or re-verify SEC2-128/129/130 against this tree. Not stale. |
| X-Forwarded-For per-IP bypass (GH #360 / WALM-289 / SEC2-116) | `services/server/src/client_ip.rs`; `Config.trusted_proxy_hops`; default `0` | **Mitigated in code.** WALM-289 Done. | Close SEC2-116. Ops: production ingress should set `TRUSTED_PROXY_HOPS=1`. |
| MCP consent phishing (GH #368 / WALM-288 / SEC2-115) | `apps/app/src/pages/ConnectMcp.tsx` localhost `/preflight` + `connectState` before consent | **Mitigated in code (bridge-bound).** WALM-288 Done. `add_delegate_key` still commits before `/callback` (onchain grant is the product); preflight is the phishing gate. | Close SEC2-115. |
| AI middleware system-role injection (GH #443 / WALM-290 / SEC2-117) | `packages/sdk/src/ai/middleware.ts` `injectMemoryContext`; `packages/python-sdk-memwal/memwal/middleware.py` | **Mitigated.** Recalled bytes go in a **user** message; system role is a fixed `UNTRUSTED_MEMORY_SYSTEM_INSTRUCTION`. WALM-290 Done. | Close SEC2-117. |
| Delegate pubkey vs Sui address unchecked (GH #469 / WALM-301 / SEC2-126) | `account.move` `add_delegate_key` calls `derive_sui_address` | **Mitigated.** WALM-301 Done. | Close SEC2-126. |
| `delete_by_blob_id` cross-namespace (GH #500 / SEC2-119) | `services/server/src/storage/db.rs` `expired_blob_cleanup_is_namespace_scoped` | **Mitigated.** Delete is owner **and** namespace scoped. | Close SEC2-119. |
| Auth nonce replay fail-open on Redis down (SEC2-110) | `services/server/src/auth.rs` SET NX; Redis error → reject (fail-CLOSED) | **Mitigated.** Comment documents availability tradeoff. | Close SEC2-110. |
| Indexer unpinned git branch (GH #471 / SEC2-121) | `services/indexer/Cargo.toml` `rev = "623521008f1c3afa3a1adbb6c3c44c39c46c5e17"` | **Mitigated.** | Close SEC2-121. |
| Researcher `AUTH_SECRET` min length (WALM-418) | `enoki-challenge.ts` requires ≥32 chars; **`session.ts` does not** | **Survives on the session cookie path.** WALM-418 already In Review. | Leave WALM-418. Do not expand this PR. |
| Unbounded registry scan without `x-account-id` (GH #492 / SEC2-124) | `auth.rs` Strategy 3 `find_account_by_delegate_key`; testnet fail-closed; concurrency permit + page cap | **Partially mitigated.** Scan remains on non-testnet without the header. | Keep SEC2-124 (or retitle as residual). Not stale. |
| `/sponsor` without Walrus Memory auth (GH #499 / SEC2-122) | `services/server/src/routes/sponsor.rs` | **Not re-audited line-by-line here.** Still a live route. Binding/restore-after-sidecar-failure code exists. | Leave SEC2-122 unless a later audit proves allowlist + auth sufficient. |
| Researcher N14/N5/N6 (signout CSRF, scraped content, `?query=` auto-submit) | `apps/researcher/**` | **Not claimed removed.** Files exist. | Leave SEC2-111 / SEC2-106 / SEC2-107 unless separately verified. |

---

## Researcher prod keys: UNKNOWN (flag for ops)

Git cannot answer whether **deployed** `researcher-app-db` rows hold Mainnet delegate private keys. Do not dump the column.

Evidence that a live production instance exists and has used Mainnet credentials:

| Signal | Source | Implication |
|---|---|---|
| Public hostname `https://researcher.demo.memory.walrus.xyz` | CORS allowlist; WALM-410 handover; researcher fix board | Prod demo is deployed. |
| Railway `researcher-frontend` + `researcher-app-db` | 2026-08-13 usage investigation (Notion) | Production Postgres exists. |
| Target: Mainnet relayer `relayer.memory.walrus.xyz` | Researcher fix board (2026-08-18) | Deployed app talks to production Walrus Memory. |
| 27 `User` rows, 18 with `accountId` / `suiAddress`; 14 `ResearchBlob` rows (8 users, non-null `blobId`) as of 2026-08-13 05:35 UTC | Same investigation (aggregates only; **no keys extracted**) | Real onchain accounts were created from this app. |
| Two additional Mainnet writes 2026-08-17 (team testing); Enoki created throwaway account `0xf4acab…` | Same board | Prod path still mints Mainnet accounts and stores their keys in the app DB. |
| README: “must not be deployed with production delegate private keys” | `apps/researcher/README.md` | Policy; does not prove the live DB was rotated. |

**Ops check (no secret values):**

[Source: handover/security-sweep-reconcile.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/handover/security-sweep-reconcile.md)

```sql
-- researcher-app-db, production. Aggregates only.
SELECT
  count(*) AS users,
  count("delegatePrivateKey") AS rows_with_key,
  count("accountId") AS rows_with_account
FROM "User";
```

Also confirm Railway env: `NEXT_PUBLIC_SUI_NETWORK`, `MEMWAL_SERVER_URL` / relayer URL, and whether `researcher.demo.memory.walrus.xyz` is still serving. If `rows_with_key > 0` on Mainnet, treat leftover keys as **P1 credential inventory** (rotate/revoke onchain, drop column or encrypt at rest). That work is **out of scope for this PR**.

---

## Delegate-history design → ai-9 (comg-722)

**This PR does not complete AI-9.** COMG-722 is the formal risk-acceptance *process* (Linear issue with owner, expiry, re-review). Record the design so that ticket can ingest it.

**Decision (current contract, still in source):**

Seal derives one reusable key per identity. A delegate who fetched the key for `BCS(owner)` once could decrypt *future* memories after removal. The response is `access_counter_version`: encrypt under `BCS(owner) ‖ BCS(counter)`, bump the counter on delegate removal / freeze.

That bump is **forward-only**. Memories already written stay readable with their original identity by whoever was authorized then. Cutting off a removed or compromised delegate for **old** blobs means re-encrypting those blobs. Comments in `MemWalAccount` and `rotate_access_counter` state this explicitly.

**Functional outcome:** revoke stops *new* ciphertext; it is not a cryptographic erase of history. Blast radius of a leaked delegate key includes all blobs encrypted at counters `≤` the counter at leak time, until those blobs expire or are re-encrypted.

**Suggested AI-9 fields (for COMG-722 owners, not this PR):**

| Field | Suggested value |
|---|---|
| Risk | Historical Seal ciphertext remains decryptable after delegate removal |
| Code | `services/contract/sources/account.move` `access_counter_version` |
| Owner | TBD by COMG-722 (Reginaldo Silva / John Naulty per postmortem) |
| Expiry / re-review | TBD; trigger = V2 encryption identity change or a re-encryption product |
| Status | Accepted design, not a bug to “fix” in V1 |

SEC2-118 (“static per-owner Seal identity”) is the same family: **partially addressed** by the counter for future writes; historical remainder **is** this acceptance.

---

## Linear hygiene (do not comment from this pr)

Code-complete / stale: **recommend close** after a human glance:

- SEC2-103, SEC2-113, SEC2-114, SEC2-115, SEC2-116, SEC2-117, SEC2-119, SEC2-120, SEC2-121, SEC2-126, SEC2-110
- SEC2-128 / SEC2-129 / SEC2-130 if re-verified against PR #601 tree
- Migrator-app-only children of the Aug 11 review that named `services/migrator*`

**Keep open / move to AI-9:**

- COMG-722 (process)
- Researcher DB keys (new ops ticket if `rows_with_key > 0`)
- WALM-418 (`AUTH_SECRET` on `session.ts`)
- SEC2-124 residual registry scan
- SEC2-122 sponsor auth (until re-audited)
- SEC2-118 historical Seal identity → fold into AI-9, do not “fix” in this repo without a re-encryption program

---

## What this PR does not do

- No SDK version bump.
- No Linear comments.
- No Railway queries that would return key material.
- No deletion of ceremony scripts (keep-ceremony).
- No implementation of AI-9.
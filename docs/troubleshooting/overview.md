---
title: "Troubleshooting & FAQ"
description: "Symptom → cause → fix for common Walrus Memory issues: delegate-key auth (401 / AUTH_REJECTED), MCP connection problems, and save/analyze timeouts."
keywords: [Walrus Memory, MemWal, troubleshooting, 401, AUTH_REJECTED, delegate key, MCP, timeout, memwal_analyze]
---

<!--
MAINTAINER NOTE (remove before publishing if desired)
Seeded from BEDU-612 (Discord support thread: Daniel Lam, Dan D, Kenton).
Two real cases drove this page:
  1. 401 AUTH_REJECTED on a fresh dashboard delegate key (SDK ~0.0.7).
     The reporter SELF-RESOLVED but did NOT record the actual fix
     ("user does not know what the solution was, only used Claude to solve it"
     — FAQ tracking sheet). So the root cause for THAT specific case is unconfirmed.
     The "401 / AUTH_REJECTED" entry below therefore enumerates every documented
     cause from the auth model (relayer api-reference + delegate-key-management),
     ordered by likelihood for the reported fingerprint (fresh dashboard key +
     account ID from localStorage). Leading suspects: (a) delegate not yet
     registered/indexed on-chain, (b) account-ID mismatch, (c) env/relayer mismatch.
     TODO: confirm the actual fix with the reporter / Dio and tighten the ordering.
  2. memwal_analyze timed out twice while recall worked. Grounded in the async
     job model (relayer api-reference) + MCP reference.
Grounding sources (MystenLabs/MemWal @ dev):
  docs/contract/delegate-key-management.md, docs/contract/ownership-and-permissions.md,
  docs/relayer/api-reference.md, docs/sdk/api-reference.md, docs/mcp/reference.md,
  docs/getting-started/quick-start.md
-->

This page collects the support questions we see most often and the fastest way to resolve them. Each entry follows the same shape so you can scan to the symptom that matches yours:

**Symptom** (what you observe) → **Cause** (why it happens) → **Fix** (what to do).

If your issue isn't here, set `MEMWAL_MCP_DEBUG=1` (MCP) or enable `debug` in the SDK config to get verbose logs, and open an issue on the repo with that output.

---

## How authentication works (read this first)

Most auth problems make sense once the model is clear. Walrus Memory never uses your owner wallet for day-to-day calls. Instead:

1. You **generate a delegate keypair** (an Ed25519 key) — for example from the [dashboard](https://memory.walrus.xyz).
2. The delegate's **public key is registered on-chain** in your `MemWalAccount` by the account owner.
3. The SDK or MCP client signs each request with the delegate **private key**.
4. On every request, the relayer verifies the signature and then **looks up that public key in your account's on-chain `delegate_keys` list** to resolve the owner.

The key word is *registered*. A delegate key that exists locally but has not been added to your account on-chain — or has been added to a *different* account, or on a *different* network — will be rejected. That single fact explains the large majority of `401 / AUTH_REJECTED` reports.

Every signed request also carries a timestamp with a **5-minute validity window** and a one-time nonce (replay protection), which is the second most common source of surprise rejections.

---

## Authentication & delegate keys

### 401 / `AUTH_REJECTED` — requests rejected even with a fresh delegate key

**Symptom.** You generated a delegate key (often from the dashboard), confirmed your account ID, and every authenticated call still returns `401` / `AUTH_REJECTED`. `memwal_health` (or `MemWal.health()`) succeeds, because the health endpoint is unauthenticated — so the relayer is reachable, but your *signed* requests are not being accepted.

**Cause.** The relayer could not match your signed request to an **active, registered delegate key on the account you named**. It verifies the Ed25519 signature, then resolves the owner by finding your public key in that account's on-chain `delegate_keys`. Anything that breaks that lookup produces the same `401`. In order of likelihood for a freshly generated key:

| # | Cause | How to check | Fix |
| --- | --- | --- | --- |
| 1 | **Delegate key not registered on-chain yet** (or the registration transaction hasn't landed/been indexed). Generating a keypair is *not* the same as adding it to the account. | Confirm the key appears under your account's delegate keys in the [dashboard](https://memory.walrus.xyz). If you scripted setup, confirm `addDelegateKey` actually executed and the tx succeeded. | Register the public key on-chain (owner-signed) via the dashboard or `addDelegateKey(...)`. If you just registered it, wait a few seconds for indexing and retry. |
| 2 | **Account-ID mismatch.** The `accountId` you're passing (e.g. copied from `localStorage`) isn't the account the key is registered under — a common result of stale `localStorage` from a previous account or copying the wrong field. | Compare the `accountId` in your config against the account that owns the delegate key in the dashboard. | Use the account ID that *you* generated and that owns this delegate key. Don't reuse an account ID copied from docs or another project. |
| 3 | **Environment / relayer mismatch.** The key + account live on one network but you're pointed at another relayer — e.g. registered on staging (testnet) but calling the production relayer, or vice versa. The relayer only sees on-chain state for its own network. | Check `serverUrl` (SDK) or your preset (`--prod` / `--staging`) against where the account was created. Prod = `https://relayer.memory.walrus.xyz`; staging = `https://relayer-staging.memory.walrus.xyz`. | Point the client at the relayer for the network where the account/key live, or re-create the account/key in the environment you intend to use. For MCP, `--logout` then `login --<env>`. |
| 4 | **Clock skew / expired request.** The signed timestamp must be within a **5-minute window**. A client clock that's off by more than that gets rejected. | Check that the machine's clock is accurate (NTP synced). | Sync the system clock and retry. |
| 5 | **Key revoked or account frozen.** The delegate was removed, or the owner deactivated (froze) the account — which denies access for *all* keys. | Check the dashboard for the key's presence and the account's active state. | Re-add the delegate key, or reactivate the account (owner only). |

<Note>
SDK version is rarely the cause here. The relayer reports its `minSupportedSdk` (TypeScript `0.0.4` at time of writing), so `0.0.7` is supported. A genuine version mismatch surfaces as `MemWalCompatibilityError`, not `AUTH_REJECTED`.
</Note>

<Tip>
Quick triage order: (1) is the key listed under the right account in the dashboard? (2) does your `accountId` match that account exactly? (3) is your relayer/env the same one the account was created in? Those three resolve almost every case.
</Tip>

---

## MCP connection issues

### Only `memwal_login` shows up (or memory tools error until you sign in)

**Symptom.** After connecting the MCP server, the agent only sees `memwal_login`, or memory tools return an "auth required" error.

**Cause.** No credentials file exists at `~/.memwal/credentials.json`. When launched by an MCP host without credentials, the package starts in an auth-required mode that intentionally exposes `memwal_login` first.

**Fix.** Ask the agent to call `memwal_login` (it returns a one-time sign-in URL valid for **5 minutes**), or run `npx -y @mysten-incubation/memwal-mcp login --prod` in your terminal. Make sure your browser is logged into the wallet you intend to use before clicking. If the URL expires, call the tool again to mint a fresh one.

### The MCP tools aren't visible to the agent at all

**Symptom.** None of the `memwal_*` tools appear.

**Cause.** MCP servers are loaded only at client startup, so a server added mid-session won't appear.

**Fix.** Fully quit and relaunch the MCP client. If you registered via `claude mcp add`, run `claude mcp list` to confirm `memwal` is registered before restarting.

---

## Saving & timeouts

### `memwal_analyze` (or a milestone save) times out, but recall works

**Symptom.** Recall is fine, but saving via `memwal_analyze` times out — sometimes on repeated attempts — while `memwal_recall` returns quickly.

**Cause.** Recall is a single search round-trip, so it's fast. Saving is much heavier, and `analyze` is the heaviest save of all: it runs an **LLM extraction** over your text and then enqueues **each extracted fact as its own background job**, where every job embeds → SEAL-encrypts → uploads to Walrus → indexes. Under load this fan-out can exceed the MCP host's tool-call timeout even though the work is proceeding normally on the relayer.

<Warning>
**A client-side timeout does not mean the save failed.** The relayer accepts the work as background jobs (HTTP `202 Accepted`) and keeps processing after your client gives up. Blindly re-running the same `analyze` can create **duplicate** memories. Verify before retrying.
</Warning>

**Fix.**

1. **Verify instead of resubmitting.** Wait a few seconds (indexing can lag briefly under load), then run `memwal_recall` for the fact you tried to save. If it's there, the save succeeded despite the timeout.
2. **Use the right tool for the job.** To save a single discrete fact (a "milestone"), prefer `memwal_remember` — it's one write, not an extract-and-fan-out. Reserve `analyze` for longer passages where you actually want multiple facts extracted, and keep those passages reasonably sized.
3. **Confirm connectivity first.** Run `memwal_health` — it hits the unauthenticated `/health` endpoint and is the fastest way to confirm the relayer is reachable (use this rather than a full `recall` round-trip).
4. **For SDK callers**, remember that `remember()` and `analyze()` return immediately with job IDs; do the waiting yourself with `waitForRememberJob` / `analyzeAndWait` and a sensible timeout, or poll the job status, rather than wrapping the whole thing in one short blocking call.
5. If it persists, set `MEMWAL_MCP_DEBUG=1` (MCP) or `debug: true` (SDK) and capture the per-request logs.

### Recall returns "no matching memories" right after a save

**Symptom.** You save something and an immediate recall finds nothing.

**Cause.** The write returns once the Walrus upload completes, but the embedding/indexing step can lag a few seconds behind under load, so the memory isn't searchable for a moment.

**Fix.** Wait briefly and retry the recall. If a memory is genuinely missing from the search index later (e.g. after a re-index need), `memwal_restore` / `restore(namespace)` rebuilds the index for that namespace from Walrus.

---

## Quick reference

| Thing | Value |
| --- | --- |
| Production relayer | `https://relayer.memory.walrus.xyz` |
| Staging relayer (testnet) | `https://relayer-staging.memory.walrus.xyz` |
| Dashboard (accounts, delegate keys) | `https://memory.walrus.xyz` |
| MCP env presets | `--prod`, `--staging`, `--local` |
| Local credentials file | `~/.memwal/credentials.json` |
| Verbose MCP logs | `MEMWAL_MCP_DEBUG=1` |
| Unauthenticated health check | `memwal_health` / `MemWal.health()` |
| Delegate keys per account (max) | 20 |
| Request timestamp window | 5 minutes |

---

## FAQ (for the product page)

These are written for a lighter-weight, product-page audience. Lift them as-is or trim.

**Do I need a registration step after generating a delegate key?**
Yes. Generating a delegate key creates the keypair; it only works once its public key has been **registered on your account on-chain** (the dashboard does this when you create the key there). The relayer checks for that registration on every request, so a key that was generated but not registered — or registered on a different account or network — will be rejected.

**I'm getting `401 / AUTH_REJECTED` with a brand-new key. What's wrong?**
Almost always one of three things: the key isn't registered on the account yet (or the registration is still settling), your account ID doesn't match the account the key belongs to, or your client is pointed at a different environment (prod vs. staging) than where the account was created. Check those in that order. (A skewed system clock can also do it, since requests are time-stamped with a 5-minute window.)

**Is the dashboard's testnet account the same as my production account?**
No. Accounts and delegate keys are per-network. A key created on staging/testnet won't authenticate against the production relayer, and vice versa. Make sure your relayer URL (or `--prod` / `--staging` preset) matches where you created the account.

**Saving a memory timed out — did I lose it?**
Probably not. Saves are processed as background jobs and the server keeps working after the client times out. Don't immediately re-save (that can duplicate the memory) — wait a few seconds and recall it to confirm. For a single fact, use the lighter "remember" rather than "analyze," which extracts and stores many facts at once and is the slowest operation.

**Why does recall work instantly but saving feel slow?**
Recall is one search request. Saving embeds, encrypts, uploads to Walrus, and indexes — and "analyze" does that for every fact it extracts. It's normal for saves to take longer; they run asynchronously in the background.

**How do I check whether the service is up?**
Run a health check (`memwal_health`, or `MemWal.health()`). It doesn't require auth, so it isolates "is the relayer reachable?" from "are my credentials valid?"

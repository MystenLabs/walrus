> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

<!--
Source: BEDU-612, seeded from a Walrus Memory Discord support thread.
The 401 reporter self-resolved without recording the exact fix, so the AUTH_REJECTED
entry documents all known causes from the authentication model rather than a single
confirmed one. Confirm the specific fix with the reporter before treating that entry
as final. Grounded in: contract/delegate-key-management, relayer/api-reference,
sdk/api-reference, mcp/reference, getting-started/quick-start.
-->

This page collects the support questions that come up most often and the fastest way to resolve each one. Every entry lists the symptom you observe, the cause behind it, and the fix to apply.

If your issue is not here, set `MEMWAL_MCP_DEBUG=1` for the Model Context Protocol (MCP) server, or, if you use the `withMemWal()` AI middleware, pass `debug: true` in its options, to get verbose logs, then open an issue on the repository with that output.

## How authentication works

Read this section first, because most authentication problems make sense once the model is clear. Walrus Memory does not use your owner wallet for day-to-day calls. Instead:

1. You generate a delegate key pair, an Ed25519 key, for example from the [Walrus Memory dashboard](https://memory.walrus.xyz).
2. The account owner registers the delegate public key onchain in your `MemWalAccount`. See [Delegate Key Management](/walrus-memory/contract/delegate-key-management).
3. The SDK or MCP client signs each request with the delegate private key.
4. On every request, the relayer verifies the signature, then looks up that public key in your account's onchain `delegate_keys` list to resolve the owner.

The key word is **registered**. A delegate key that exists locally but is not added to your account onchain, or is added to a different account, or is added on a different network, fails authentication. That single fact explains most `AUTH_REJECTED` reports.

Every signed request also carries a timestamp with a 5-minute validity window and a one-time nonce for replay protection, which is the second most common source of unexpected rejections.

## Authentication and delegate keys

This section covers credential and authorization errors.

### 401 `AUTH_REJECTED` errors

**Symptom:** You generated a delegate key, often from the dashboard, confirmed your account ID, and every authenticated call returns `AUTH_REJECTED` with HTTP status 401. A health check still succeeds, because the health endpoint needs no authentication, so the relayer is reachable but your signed requests are not accepted.

**Cause:** The relayer cannot match your signed request to an active, registered delegate key on the account you named. It verifies the Ed25519 signature, then resolves the owner by finding your public key in that account's onchain `delegate_keys`. Anything that breaks that lookup returns the same 401. The following causes are ordered by how often they apply to a freshly generated key, and each row pairs the check with its fix:

| **Cause** | **How to check** | **How to fix** |
| --- | --- | --- |
| The delegate key is not registered onchain yet, or its registration transaction is not indexed | Confirm the key appears under your account in the dashboard, and that any `addDelegateKey` call succeeded. | Register the public key onchain through the dashboard or `addDelegateKey`, then wait a few seconds for indexing and retry. |
| The account ID does not match the account that owns the key | Compare the `accountId` in your configuration against the account that lists this delegate key in the dashboard. A stale value in `localStorage` is a common source. | Use the account ID that you generated and that owns this delegate key, and do not reuse an account ID copied from another project. |
| The client points at a different network than the one where the account lives | Confirm your `serverUrl` or environment preset matches where you created the account. Production is `https://relayer.memory.walrus.xyz` and staging is `https://relayer-staging.memory.walrus.xyz`. | Point the client at the relayer for the correct network, or recreate the account and key in the environment you intend to use. |
| The system clock is outside the 5-minute signing window | Confirm the machine clock is accurate and synchronized using the Network Time Protocol (NTP). | Synchronize the clock and retry. |
| The delegate key is revoked, or the account is deactivated | Confirm the key is present and the account is active in the dashboard. | Re-add the delegate key, or reactivate the account, which only the owner can do. |

> **Tip**
>
> To triage quickly, confirm 3 things in order: the key is listed under the correct account in the dashboard, your account ID matches that account exactly, and your relayer environment matches where you created the account.
> **Info**
>
> A version mismatch is rarely the cause here. The relayer reports its minimum supported SDK version, which is TypeScript 0.0.4 at the time of writing, so 0.0.7 is supported. A true version mismatch surfaces as `MemWalCompatibilityError` rather than `AUTH_REJECTED`.
## MCP Connection issues

This section covers problems that appear before the memory tools work.

### Only `memwal_login` appears

**Symptom:** After you connect the MCP server, the agent sees only `memwal_login`, or the memory tools return an authentication-required error.

**Cause:** No credentials file exists at `~/.memwal/credentials.json`. When an MCP host launches the package without credentials, it starts in an authentication-required mode. The tool list still advertises the core memory tools alongside `memwal_login`, but only `memwal_login` runs until you authenticate; the other tools return an authentication-required error when called, which is why it can look like `memwal_login` is the only tool available.

**Fix:** Ask the agent to call `memwal_login`, which returns a one-time sign-in URL valid for 5 minutes, or run `npx -y @mysten-incubation/memwal-mcp login --prod` in your terminal. Sign in to the wallet you intend to use before you open the URL. If the URL expires, call the tool again to get a fresh one.

### The MCP tools do not appear

**Symptom:** None of the `memwal_*` tools are available to the agent.

**Cause:** MCP servers load only when the client starts, so a server added during a session does not appear.

**Fix:** Quit the MCP client fully, then start it again. If you registered the server with `claude mcp add`, run `claude mcp list` to confirm `memwal` is registered before you restart.

## Saving and timeouts

This section covers slow or timed-out writes.

### `memwal_analyze` times out while recall works

**Symptom:** Recall returns quickly, but a save through `memwal_analyze` times out, sometimes on repeated attempts.

**Cause:** Recall is a single search request, so it is fast. A save is heavier, and `memwal_analyze` is the heaviest save: it runs a large language model (LLM) extraction over your text, then embeds and encrypts each extracted fact and enqueues each as its own background job that uploads to Walrus and indexes the result. Under load, this fan-out can exceed the MCP host's tool-call timeout even though the relayer keeps working normally.

> **Warning**
>
> A client-side timeout does not mean the save failed. The relayer accepts the work as background jobs and keeps processing after the client stops waiting. Running the same `memwal_analyze` call again can create duplicate memories, so confirm the result before you retry.
**Fix:**

1. Confirm the result instead of resubmitting. Wait a few seconds, because indexing can lag briefly under load, then run `memwal_recall` for the fact you tried to save. If the fact is present, the save succeeded despite the timeout.
2. Match the tool to the task. To save one discrete fact, such as a milestone, use `memwal_remember`, which performs a single write. Reserve `memwal_analyze` for longer passages where you want several facts extracted, and keep those passages a reasonable size.
3. Confirm connectivity first. Run `memwal_health`, which calls the unauthenticated health endpoint and is the fastest way to confirm the relayer is reachable.
4. Wait correctly in the SDK. The `remember` and `analyze` methods return immediately with job IDs, so wait with `waitForRememberJob` or `analyzeAndWait` and a sensible timeout, or poll the job status, rather than wrapping the whole operation in one short blocking call.
5. Collect logs if the problem persists. Set `MEMWAL_MCP_DEBUG=1` for the MCP server, or pass `debug: true` to the `withMemWal()` AI middleware, and capture the per-request output. The core SDK (`MemWal.create()`) has no built-in debug flag.

### Recall returns no results immediately after a save

**Symptom:** You save a memory, and an immediate recall finds nothing.

**Cause:** A save returns once the Walrus upload completes, but the embedding and indexing step can lag a few seconds behind under load, so the memory is briefly unsearchable.

**Fix:** Wait a moment, then retry the recall. If a memory is missing from the search index later, `memwal_restore` rebuilds the index for that namespace from Walrus.

## Quick reference

Use these values for quick configuration and triage:

| **Item** | **Value** |
| --- | --- |
| Production relayer | `https://relayer.memory.walrus.xyz` |
| Staging relayer (Testnet) | `https://relayer-staging.memory.walrus.xyz` |
| Dashboard for accounts and delegate keys | `https://memory.walrus.xyz` |
| MCP environment presets | `--prod`, `--staging`, `--local` |
| Local credentials file | `~/.memwal/credentials.json` |
| Verbose MCP logs | `MEMWAL_MCP_DEBUG=1` |
| Unauthenticated health check | `memwal_health` |
| Maximum delegate keys per account | 20 |
| Request timestamp window | 5 minutes |

## Frequently asked questions

These answers are written for the product page.

### Is there a registration step after i generate a delegate key?

Yes. Generating a delegate key creates the key pair, and it works only after the owner registers its public key on your account onchain, which the dashboard does when you create the key there. The relayer checks for that registration on every request, so a key that is generated but not registered, or registered on a different account or network, fails authentication.

### Why do i get `AUTH_REJECTED` with a brand-new key?

The cause is almost always one of 3 things: the key is not registered on the account yet or is still settling, your account ID does not match the account that owns the key, or your client points at a different network than the one where you created the account. Check those in that order. A skewed system clock can also cause it, because each request carries a timestamp with a 5-minute window.

### Is my Testnet account the same as my production account?

No. Accounts and delegate keys are specific to each network. A key created on staging does not authenticate against the production relayer, and the reverse is also true. Confirm that your relayer environment matches where you created the account.

### My save timed out, so did i lose the memory?

Probably not. The relayer processes saves as background jobs and keeps working after the client times out. Do not save again right away, because that can duplicate the memory. Wait a few seconds and recall it to confirm. To save a single fact, use `memwal_remember` rather than `memwal_analyze`, which extracts and stores many facts at once and is the slowest operation.

### Why does recall feel instant while saving feels slow?

Recall is one search request. A save embeds, encrypts, uploads to Walrus, and indexes the result, and `memwal_analyze` does that for every fact it extracts. Saves are expected to take longer, and they run in the background.

### How do i check whether the service is reachable?

Run `memwal_health`. It does not require authentication, so it separates the question of whether the relayer is reachable from whether your credentials are valid.
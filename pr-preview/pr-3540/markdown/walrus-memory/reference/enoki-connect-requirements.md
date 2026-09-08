> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

# Enoki Connect Requirements

## Overview

Enoki Connect lets an external application (a "connecting app," for example, Console) authenticate its users through Walrus Memory's Enoki-hosted identity flow. This requires configuration on two sides: WM's own Enoki app settings, and each connecting app's origin being allowlisted. This document exists because that configuration surface is not obvious from the Enoki Developer Portal UI, and one gap in it (a missing Allowed Origin) produced an opaque `NOT_FOUND` failure that took live debugging with Console's team to diagnose. Enoki itself is closed, Mysten-hosted infrastructure, WM has no visibility into its source, so everything below is inferred from observed behavior (decoded request payloads, browser stack traces), not from reading Enoki's implementation.

## What enoki connect requires from wm's side

WM's Enoki app is registered in the Enoki Developer Portal under the team **CommandOSS** (just the Portal account/team name WM's app happens to live under, not a separate product or system) with the app name **Walrus Memory**. For Enoki Connect to work at all, that app must have:

1. **Enoki Connect enabled** as a feature on the app.
2. **A Public App Slug set.** This slug becomes part of the hosted connect page's domain: `<slug>.connect.enoki.mystenlabs.com`.

The Public App Slug is **immutable once set**, the Enoki Portal does not offer a way to change it after creation. WM's current production slug is `lorem`, chosen as a placeholder before this immutability constraint was understood. It is not going to change; it is now a permanent, non-ideal, but load-bearing part of WM's Enoki configuration (it appears in the Allowed Origins entry below and in every OAuth provider's redirect URI). Treat it as fixed infrastructure, not as a value worth trying to "fix" later.

**Recommendation for any future Enoki Connect app:** pick the Public App Slug deliberately, as if it were a permanent subdomain (because it is), not as a temporary placeholder.

## Default salt confirmation

Enoki Connect only works if WM and Console both use Enoki's **default salt** for zkLogin, that is, neither app supplies a custom salt, because Enoki Connect does not support custom-salt (custom zkLogin) apps at all. This was verified by code inspection on both sides, not by an Enoki Portal setting (Enoki does not surface an explicit "using default salt: yes/no" indicator; absence of custom-salt code is the only observable signal):

- **WM (`apps/app/src/App.tsx`):** `registerEnokiWallets({ apiKey, providers: { google: { clientId, redirectUrl } }, client, network })`, no `salt`/custom-salt parameter anywhere in the call, and a repo-wide search of `apps/app/src` for "salt" returns no custom salt service or override.
- **Console (`api/src/infra/clients/enoki/enoki.client.ts`, `api/src/domain/auth/auth.service.ts`):** Console always obtains its salt *from* Enoki through `enokiClient.getZkLogin({ jwt })`, never supplies one, and `auth.service.ts` explicitly depends on this for a security check: *"The Enoki-managed salt makes local re-derivation the only proof that this token belongs to the caller"* (used to prevent id_token substitution attacks). Console's own security model would break under a custom salt, which is independent, corroborating evidence.
- Enoki Connect itself would fail to register the wallet at all for a custom-salt app (per Enoki's own documentation), so the fact that WM's Enoki Connect setup above works at all is a third, functional confirmation.

No custom salt exists on either side, by code inspection and by the functional precondition that Enoki Connect requires it. The one thing this doesn't cover: an explicit walkthrough of the Enoki Portal's own app configuration to positively confirm there's no server-side custom salt backend registered outside of what the client SDKs above would exercise, if a Portal-level indicator exists, checking it directly remains a stronger source of truth than the client-code inference above.

*(This confirmation was tracked as part of WALM-298's acceptance criteria, see `docs/reference/console-identity-link.md` for the endpoint that ticket covers.)*

## Allowed origins requirement (the actual bug found)

The Enoki Developer Portal's app-level **Settings** page has an **Allowed Origins** field. This lists the website origins permitted to access the Enoki app, and critically, it must include the origin of every **connecting app** that will initiate an Enoki Connect flow against WM's app, not just WM's own frontend origins.

This was confirmed through live debugging with Console's team: when Console attempted to connect from `http://localhost:3000` (their local dev origin) and that origin was not yet in WM's Allowed Origins list, the Enoki-hosted connect page, 

[Source: reference/enoki-connect-requirements.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/reference/enoki-connect-requirements.md)

```
https://<slug>.connect.enoki.mystenlabs.com/dapp-request/connect
```

, threw a client-side `RpcError: NOT_FOUND`. The error surfaced inside a component named `scam-protection-guard` in Enoki's hosted page bundle, and rendered to the user simply as "Something went wrong" / "NOT_FOUND", with no indication of what was actually missing. Decoding the `dapp-request` payload passed to that page showed `appUrl` / origin-shaped fields matching the connecting app's URL, which is what led to checking the Allowed Origins list as the likely gate, adding Console's origin to Allowed Origins resolved the failure.

**Currently allowlisted for WM's Enoki app:**

- `http://localhost:3000`, Console local dev
- `https://lorem.connect.enoki.mystenlabs.com`, the Enoki-hosted domain itself (WM's own slug)

**Any new environment Console (or another connecting app) deploys to, staging, production, a new preview domain, needs its origin added to this list**, or it will reproduce the same `NOT_FOUND` failure. This is not automatic; it has to be added manually in the Enoki Developer Portal ahead of that environment going live.

## Redirect URL requirement

Separately from Allowed Origins, each authentication provider used with Enoki Connect (for example, Google) requires its own one-time OAuth console setup: the Enoki Portal's own UI displays a banner instructing that

[Source: reference/enoki-connect-requirements.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/reference/enoki-connect-requirements.md)

```
https://<slug>.connect.enoki.mystenlabs.com/auth/callback
```

be added as an authorized redirect URI in that provider's OAuth configuration (for example, the Google Cloud Console credentials page for the OAuth client Enoki uses). This is a per-provider setup step, done once when a new provider is wired into Enoki Connect, it is not something a connecting app or WM's application code can work around at runtime.

## Evidence basis

The findings above come from a live debugging session with Console's team, working from directly observable evidence:

- The `NOT_FOUND` error and its stack trace, which resolved through a `scam-protection-guard` component in the Enoki-hosted connect page bundle.
- The decoded `dapp-request` payload sent to `/dapp-request/connect`, which contains `appUrl` / origin-shaped fields corresponding to the connecting app.
- Reproducing and resolving the failure by adding the missing origin to WM's Allowed Origins list in the Enoki Developer Portal.

None of this is confirmed against Enoki's own source, it is closed, Mysten-operated infrastructure. The causal link between "origin missing from Allowed Origins" and "`scam-protection-guard` throws `NOT_FOUND`" is inferred from behavior, not verified from Enoki's implementation. Treat it as the best available explanation, not a guarantee of exactly how Enoki enforces this check internally.

## Troubleshooting checklist: not_found on the enoki-hosted connect page

If a connecting app hits "Something went wrong" / `NOT_FOUND` on `<slug>.connect.enoki.mystenlabs.com/dapp-request/connect`, check in this order:

1. **Allowed Origins**, In the Enoki Developer Portal, under WM's app ("Walrus Memory," team CommandOSS) → Settings, confirm the connecting app's exact origin (scheme + host + port) is present in Allowed Origins. This is the most common cause and was the root cause the one time this was diagnosed.
2. **Redirect URI**, Confirm `https://<slug>.connect.enoki.mystenlabs.com/auth/callback` is registered as an authorized redirect URI in the relevant OAuth provider's console (for example, Google Cloud Console) for the provider being used.
3. **If neither resolves it**, This is likely an Enoki-side issue, because the failure occurs inside Enoki's own hosted page, not in WM's or Console's code. Escalate to Mysten/Enoki support rather than continuing to debug it from WM's or Console's side.
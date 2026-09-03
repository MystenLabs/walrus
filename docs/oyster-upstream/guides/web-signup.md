# Web Signup

Oyster can serve a self-serve signup page at `/signup`. A user signs in
with Google, passes a Cloudflare Turnstile anti-bot check, and receives
an **app** plus its first **admin key**, the same kind of key an
operator would otherwise issue with `oysterd app issue-admin-key`. A
small dashboard at `/signup/keys` lets them issue (capped), revoke, and
rotate keys.

The feature is entirely opt-in. When its configuration is absent, the
routes are not mounted at all.

## Prerequisites

Two external accounts, both free:

1. **Cloudflare Turnstile**: create a widget in the Cloudflare
   dashboard (Turnstile → Add widget), listing your hostnames (plus
   `localhost` for dev). This yields the sitekey and secret key. Your
   site does *not* need to be behind Cloudflare.
2. **Google OAuth**: in a Google Cloud project, configure the OAuth
   consent screen (External; only non-sensitive scopes are used, so no
   verification review is needed) and create an **OAuth 2.0 Client ID**
   of type *Web application* with the redirect URI
   `<OYSTER_PUBLIC_BASE_URL>/signup/callback`. Publish the consent
   screen when you are ready for users outside your test list.

## Configuration

Signup is enabled only when **all five** of the following are set. Setting only
some is a startup error.

| Variable | Meaning |
| --- | --- |
| `OYSTER_PUBLIC_BASE_URL` | Public base URL, for example `https://oyster.example.com`; used to build the OAuth redirect URI |
| `GOOGLE_OAUTH_CLIENT_ID` | Google OAuth web client ID |
| `GOOGLE_OAUTH_CLIENT_SECRET` | Google OAuth web client secret |
| `TURNSTILE_SITE_KEY` | Turnstile sitekey (public, rendered into the page) |
| `TURNSTILE_SECRET_KEY` | Turnstile secret key (server-side verification) |

Behavior knobs (optional):

| Variable | Default | Meaning |
| --- | --- | --- |
| `OYSTER_SIGNUP_MODE` | `closed` | `open` (anyone signs up), `waitlist` (operator approves), `closed` (no new signups; existing users still sign in) |
| `OYSTER_SIGNUP_ALLOWED_DOMAINS` | — | Comma-separated email domains that skip the waitlist (Google-verified emails only) |
| `OYSTER_SIGNUP_ALLOWED_EMAILS` | — | Comma-separated individual emails that skip the waitlist (Google-verified). Pre-authorizes named people without opening their whole domain, and works before they've ever signed in |
| `OYSTER_MAX_ADMIN_KEYS_PER_APP` | `5` | Active-key cap on web issuance (the operator CLI bypasses it) |
| `OYSTER_SIGNUP_ENV_LABEL` | — | Badge ("Testnet", "Mainnet", and so on) on the signup pages; set it when running multiple deployments so users can tell them apart |

See `.env.example` at the repo root for a copy-paste template.

## How it maps to the data model

Google account → `users` row (keyed by the stable `sub` claim in
`user_identities`) → owns one `apps` row → holds `app_admin_keys`.
Admin keys issued through the web are indistinguishable from CLI-issued
ones. Google only authenticates the *management* of keys, never API
calls themselves. Raw keys are shown exactly once and stored only as
Blake2s-256 hashes. A lost key cannot be recovered, only replaced.

## Waitlist review

In `waitlist` mode, a new user's first sign-in files a pending request
and shows a "request received" page. Review requests with the server
CLI (direct database access, the same as `oysterd app`):

```bash
oysterd signup list             # pending requests (TSV)
oysterd signup list --all       # include decided ones
oysterd signup approve <id-or-email>
oysterd signup reject <id-or-email>
```

No email is sent on approval. The user simply signs in again, and that
sign-in completes signup and shows their admin key.

`oysterd signup approve` only flips an **existing** request, one filed
when the person first signed in, because approval is matched on the
Google `sub`, which you do not have until they authenticate. To
pre-authorize someone who has *not* signed in yet, add their address to
`OYSTER_SIGNUP_ALLOWED_EMAILS` (or their domain to
`OYSTER_SIGNUP_ALLOWED_DOMAINS`). The gate matches the Google-verified
email at sign-in time, so no prior request row or `sub` is needed. These
lists are read from the environment, so changes take effect on restart.

## Local development

Cloudflare publishes dummy Turnstile keys that require no account and skip
real challenges:

```
TURNSTILE_SITE_KEY=1x00000000000000000000AA
TURNSTILE_SECRET_KEY=1x0000000000000000000000000000000AA
```

Google has no equivalent dummy mode. For a fully offline flow, use the
signup testbed script, which boots Oyster with a mock Google OAuth
server (`scripts/signup-testbed.sh`). To manually exercise real
Google OAuth, register `http://localhost:3000/signup/callback` as a
redirect URI on a dev OAuth client and set
`OYSTER_PUBLIC_BASE_URL=http://localhost:3000`.

## Operational notes

- Browser sessions live 8 hours in the `web_sessions` table (hashed
  tokens); an hourly sweep prunes expired rows.
- Web key issuance and revocation is recorded in `audit_events`
  (`admin_key.issued_via_web` and `admin_key.revoked_via_web`).
- `closed` mode is the abuse kill-switch: it stops new signups without
  affecting existing users' sign-in or their keys.

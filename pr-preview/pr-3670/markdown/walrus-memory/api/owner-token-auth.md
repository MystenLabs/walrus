> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

Issuance route: `POST /v1/owner-tokens`.

## Overview

Lets Console call WM's owner-scoped read API without ever holding
a delegate key. Console proves control of a WM owner address `Y` entirely on
its own side (its own identity-link flow, Enoki Connect or a wallet-signature
challenge; WM is never involved in that proof). It then calls `POST /v1/owner-tokens`,
authenticating itself as a legitimate client through a single static **service
credential** WM issues to Console out of band, to mint a short-lived bearer
token scoped to `Y`. Console presents that token as `Authorization: Bearer
<token>` on subsequent read calls.

This is additive: it does not replace the existing Ed25519 signed-request
scheme (`x-public-key`/`x-signature`/...) every other protected route and
the owner-scoped read routes already use. The two schemes are separate, parallel
auth mechanisms, mirroring how `security-delete.md`'s Bearer-token flow
coexists with it today.

**Phase 1 scope:** only the `memories.read` permission is ever granted. No
extend/renew/write scope exists yet.

## Client authentication: the service credential

WM generates one static secret and shares it with Console out of band (not
over an unauthenticated channel). Console sends it on every issuance request:

[Source: api/owner-token-auth.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/api/owner-token-auth.md)

```http
POST /v1/owner-tokens
x-service-credential: <the shared secret>
Content-Type: application/json

{"owner": "0x<64 hex chars>"}
```

Missing header, wrong value, or the secret being unconfigured on WM's side
(`OWNER_TOKEN_SERVICE_CREDENTIAL` unset) all collapse to a bare `401` with no
body, no detail is leaked about which of these it was.

**Trust boundary, stated explicitly:** this credential is the *only* thing
gating who can request a token, and a request's `owner` is taken as a plain
request parameter, trusted because the caller already proved it holds the
credential. If the credential leaks, whoever holds it can mint a
`memories.read` token for **any** owner address, there is no secondary check
tying a specific token request back to a specific proven identity-link event.
This is an accepted Phase-1 trade-off, a deliberate choice of a shared
service credential over mTLS/signed-client-assertion for cost/complexity
reasons, not an oversight. Treat the credential with the
same handling rigor as a database password: unique per environment, rotated
if any Console-side compromise is suspected, never logged, never in a repo.

## Issuing a token

[Source: api/owner-token-auth.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/api/owner-token-auth.md)

```http
POST /v1/owner-tokens
x-service-credential: <shared secret>
Content-Type: application/json

{"owner": "0xffebf969cf2f9ec94089be20387633b3ffaf8a3c8e6caade6c47e8814852c4e7"}
```

Validation order: per-IP rate limit (outer middleware, throttles credential
guessing regardless of validity) → service credential (middleware) →
per-credential rate limit (middleware) → Sui address format (handler,
cheapest handler-level check) → per-owner rate limit → `MemWalAccount`
existence → mint. `owner` is canonicalized to lowercase before every
downstream check and before it's embedded in the token's claims.

Response (`200 OK`):

[Source: api/owner-token-auth.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/api/owner-token-auth.md)

```json
{
  "token": "<opaque bearer token>",
  "expires_at": "2026-08-04T12:15:00Z",
  "permissions": ["memories.read"]
}
```

`token` is an opaque `base64url(claims json).base64url(HMAC-SHA256
signature)` string, treat it as a black box, do not attempt to parse it
client-side. Its claims (for WM's own reference, not something Console needs
to decode): `subject` (constant `"console"`), `owner_address` (canonical
lowercase), `audience` (constant `"memwal"`), `issued_at`/`expires_at` (Unix
seconds), `nonce` (a UUID identifying this specific token, see "Replay and
revocation" below), `permissions` (`["memories.read"]` in Phase 1).

TTL defaults to 900s (15 minutes), configurable through `OWNER_TOKEN_TTL_SECS`.
**Refresh path:** there is no separate refresh/rotate endpoint in Phase 1, 
when a token is at or near expiry, call `POST /v1/owner-tokens` again with
the same `owner` to mint a fresh one. There is no reuse or extension of an
existing token's lifetime.

### Error responses

| Status | Body | Cause |
|---|---|---|
| `400` | `{"error": "owner must be a 0x-prefixed 32-byte Sui address (66 characters)"}` | Malformed `owner` |
| `400` | `{"error": "owner has no MemWalAccount"}` | `owner` is a well-formed address but has never created a `MemWalAccount`. Use `GET /api/accounts/:owner/exists` first. |
| `400` | `{"error": "Phase 1 only grants memories.read"}` | Request body included `scope` or `permissions` other than `memories.read`. |
| `401` | *(bare, no body)* | Missing/wrong `x-service-credential`, or the credential is unconfigured on this deployment |
| `429` | See "Rate limiting": **two different shapes**, do not assume one | Per-owner or per-credential budget exceeded |
| `503` | See "Availability failures": **two different shapes**, do not assume one | `OWNER_TOKEN_SECRET` unconfigured, or the rate limiter's Redis backend is unreachable |

## Using a token

[Source: api/owner-token-auth.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/api/owner-token-auth.md)

```http
GET /v1/owners/{owner}/memories
Authorization: Bearer <token>
```

`GET /v1/owners/{owner}/namespaces`, `.../memories`, and `.../agents`
all accept this same bearer token through `auth::verify_read_api_auth`
(`services/server/src/auth.rs`), which dispatches between it and the
existing Ed25519 signed-request scheme based on whether the request carries
an `Authorization` header, see `docs/api/memory-read-api.md`'s
Authentication section for the combined contract. `GET
/v1/owners/{owner}/_token_probe` still exists alongside them as the
original dev-only smoke-test route this mechanism was proven against before
the real handlers existed; it is redundant now and a candidate for removal
in a follow-up, not something Console should call.

`{owner}` in the path must exactly equal the token's `owner_address` claim
(canonical-address comparison, not raw string equality) or the request is
rejected `403`. The token's `permissions` must include the scope the route
requires (`memories.read` for all three read routes) or the request is
rejected `403`. An expired, tampered, wrongly-signed, or wrong-audience token
is rejected `401`, with no distinction in the response between those causes.

## Rate limiting

Three **independent** budgets apply to `POST /v1/owner-tokens`, enforced by
three different code paths that do **not** all share a response shape:

**0. Per-IP** (outermost middleware, runs *before* the service-credential
check, regardless of whether the credential is valid): default 30/min,
300/hour per source IP (`OWNER_TOKEN_RATE_LIMIT_IP_PER_MINUTE`/`_PER_HOUR`).
This is the layer that actually bounds someone guessing the shared service
credential, the per-credential budget below is keyed by the *value* of the
supplied credential, so a wrong guess never accumulates against it, and a
varying guess gets a fresh bucket every time; without a per-IP layer,
credential-guessing had no throttling anywhere. Uses the same response shape
as the per-credential budget below (`layer: "owner_token_ip_burst"` /
`"owner_token_ip_sustained"`).

**1. Per-owner** (checked inside the handler, after the service-credential
gate): default 5/min, 30/hour per canonical owner address
(`OWNER_TOKEN_RATE_LIMIT_OWNER_PER_MINUTE`/`_PER_HOUR`). Exceeding it:

[Source: api/owner-token-auth.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/api/owner-token-auth.md)

```json
HTTP 429
{"error": "too many owner-token requests for this owner; retry later"}
```

No `Retry-After` header, no `layer`/`limit`/`retry_after_seconds` fields, 
this is the crate's plain `AppError` envelope, not the shared rate-limiter
response builder.

**2. Per-service-credential** (middleware, runs *before* the handler and
before the per-owner check, a wrong credential never reaches the per-owner
budget, confirmed by test): default 120/min, 3000/hour across all requests
using this credential regardless of which owner they target
(`OWNER_TOKEN_RATE_LIMIT_PER_MINUTE`/`_PER_HOUR`). Exceeding it:

[Source: api/owner-token-auth.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/api/owner-token-auth.md)

```json
HTTP 429
Retry-After: 60
{
  "error": "Rate limit exceeded",
  "layer": "owner_token_credential_burst",
  "limit": "120 weighted-requests/min",
  "retry_after_seconds": 60
}
```

(`layer` is `"owner_token_credential_sustained"` and `retry_after_seconds`/
`Retry-After` are `300` for the hourly budget instead of the per-minute one.)
This is the same response shape every other rate limiter in this crate uses
(`rate_limit_response`), **do not** confuse it with the per-owner shape
above; they are genuinely different envelopes from different limiters
guarding the same endpoint.

## Availability failures

Also two different shapes, both `503`, both meaning "try again later" but
from different failure points:

**Feature not configured** (`OWNER_TOKEN_SECRET` unset on this deployment, 
checked in the handler, before the per-owner rate-limit check runs):

[Source: api/owner-token-auth.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/api/owner-token-auth.md)

```json
HTTP 503
{"error": "Upstream temporarily unavailable (traceId: <uuid>)"}
```

**Per-owner rate limiter's Redis backend unreachable** (same handler-level
check, fails closed rather than allowing the request through unmetered):

[Source: api/owner-token-auth.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/api/owner-token-auth.md)

```json
HTTP 503
{"error": "Upstream temporarily unavailable (traceId: <uuid>)"}
```

(Both of the above share one shape, the crate's generic `AppError`
envelope.)

**Per-credential rate limiter's Redis backend unreachable** (the middleware,
a separate code path from the two above):

[Source: api/owner-token-auth.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/api/owner-token-auth.md)

```json
HTTP 503
Retry-After: 30
{"error": "Rate limiter temporarily unavailable", "retry_after_seconds": 30}
```

This third shape is distinct from the other two `503`s, it has a
`retry_after_seconds` field and a `Retry-After` header; they don't.
Integrators should treat `503` generically (retry with backoff) rather than
branching on these exact shapes, but the shapes are documented here precisely
because they differ, so nothing downstream mis-parses one expecting the
other.

## Configuration

| Env var | Default | Purpose |
|---|---|---|
| `OWNER_TOKEN_SECRET` | *(required, empty disables the feature)* | HMAC signing key for minted tokens |
| `OWNER_TOKEN_SERVICE_CREDENTIAL` | *(required, empty rejects everything)* | The shared WM↔Console client-auth secret |
| `OWNER_TOKEN_TTL_SECS` | `900` | Token lifetime in seconds |
| `OWNER_TOKEN_RATE_LIMIT_PER_MINUTE` | `120` | Per-credential issuance budget |
| `OWNER_TOKEN_RATE_LIMIT_PER_HOUR` | `3000` | Per-credential issuance budget |
| `OWNER_TOKEN_RATE_LIMIT_OWNER_PER_MINUTE` | `5` | Per-owner issuance budget |
| `OWNER_TOKEN_RATE_LIMIT_OWNER_PER_HOUR` | `30` | Per-owner issuance budget |
| `OWNER_TOKEN_RATE_LIMIT_IP_PER_MINUTE` | `30` | Per-source-IP issuance budget (throttles credential guessing) |
| `OWNER_TOKEN_RATE_LIMIT_IP_PER_HOUR` | `300` | Per-source-IP issuance budget (throttles credential guessing) |

## Replay and revocation

Each token's `nonce` claim is a unique UUID generated at mint time. It is
**not** a single-use, replay-preventing nonce the way the signed-request
scheme's `x-nonce` is, a bearer token is meant to be reused across many read
requests during its TTL window, so per-request single-use tracking would
break normal usage. Its purpose in Phase 1 is forward-compatibility
(identifying a specific issued token, in case a revocation list is added
later) and observability (logging which token served a given request). A
stolen, unexpired token is fully usable for its entire remaining TTL, the
short default TTL (15 minutes) is the primary mitigation, not the nonce.
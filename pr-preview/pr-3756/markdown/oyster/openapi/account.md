> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

# Account Endpoints

Account and API key management

Base URL: `https://oyster.testnet.mystenlabs.com/api/v1`

> **Info**
>
> This page is auto-generated from the [OpenAPI spec](/oyster/openapi.json).
> For an interactive explorer, see the [Interactive API Reference](../api-reference.mdx).
## PUT `/account/billing`

**Update billing information for the authenticated account. Not yet implemented.**

**Authentication:** Required

**Responses:**

- **501**: Not implemented

---

## POST `/account/extend`

**Request an immediate storage-pool extension retry.**

Call this after funding the account's wallet (see `/account/wallet` or
the `account.funding_required` webhook): it clears the extension
worker's retry backoff so the pool is re-attempted on the next worker
cycle instead of waiting out the exponential backoff. Issues no chain
transactions itself — the background worker performs the extension.

**Authentication:** Required

**Responses:**

- **202**: Extension retry scheduled
- **401**: Unauthorized
- **404**: Account has no storage pool

---

## GET `/account/report`

**Retrieve a usage report for the authenticated account. Not yet implemented.**

**Authentication:** Required

**Responses:**

- **501**: Not implemented

---

## POST `/account/transfer`

**Transfer ownership of resources to another account. Not yet implemented.**

**Authentication:** Required

**Responses:**

- **501**: Not implemented

---

## GET `/account/wallet`

**Get wallet information for the authenticated account.**

**Authentication:** Required

**Responses:**

- **200**: Wallet information
- **401**: Unauthorized
- **503**: Wallet service unavailable

---
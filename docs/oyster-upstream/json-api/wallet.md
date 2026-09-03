# Wallet

Each Oyster account has an associated **Sui wallet address**, derived from
the account's identity by the Pearl custodial wallet service. This wallet
is used for onchain operations when blobs are stored on Walrus.

## Get Wallet Address

```
GET /api/v1/account/wallet
```

Returns the Sui wallet address associated with your account.

**Example:**

```bash
curl -s -H "Authorization: Bearer $API_KEY" \
  "$OYSTER_URL/api/v1/account/wallet" | jq
```

**Response** (`200 OK`):

```json
{
  "address": "0x1a2b3c4d5e6f7890abcdef1234567890abcdef1234567890abcdef1234567890"
}
```

| Field | Type | Description |
|-------|------|-------------|
| `address` | string | Sui wallet address (hex-encoded) |

**Errors:**

| Status | Condition |
|--------|-----------|
| `401` | Missing or invalid API key |
| `503` | Wallet service unavailable (Pearl not configured or unreachable) |

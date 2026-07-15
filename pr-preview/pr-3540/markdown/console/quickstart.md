> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

This quickstart takes you from sign-up to a working encrypted upload: create an account, mint an API key, then create a Seal-encrypted bucket and upload, download, and decrypt a file.

> **Info**
>
> Walrus Console is in a closed, invite-only beta on Mainnet. This quickstart uses the current Testnet preview, hosted under the Harbor name at `testnet.harbor.walrus.xyz`, with its API at `https://api.testnet.harbor.walrus.xyz`. The external API is in alpha and its endpoint shapes might change before Mainnet GA. All bucket creation goes through the private, Seal-encrypted flow; public bucket creation is disabled at the API boundary. For the full endpoint surface, see the [API reference](./api-reference). For the product model, see the [concepts and overview](./overview).
## Prerequisites

- [x] A Google account for sign-in.
- [x] Node.js with [`@mysten/sui`](https://www.npmjs.com/package/@mysten/sui) and [`@mysten/seal`](https://www.npmjs.com/package/@mysten/seal) installed, for the signing and encryption steps.

Every request below carries your API key as a bearer token:

```http
Authorization: Bearer hbr_…
```

> **Info**
>
> The code in this guide is pulled directly from the runnable [`walrus-harbor-quickstart`](https://github.com/MystenLabs/walrus-harbor-quickstart) example, so the package IDs and Seal key-server IDs stay current with the source. Clone it and run the `pnpm` scripts to try the flow end to end.
## Sign up and create an API key

1. Visit the [Walrus Console app](https://testnet.harbor.walrus.xyz/) and sign in with Google. zkLogin provisions your account and a Personal Space automatically.
2. Open **Settings → API Keys → New API key**, give it a name, pick a role, and submit.
   - `read_write` is required for any state change: create and delete buckets, upload, rename, and delete files, and finalize private buckets.
   - `read_only` covers listing, status, and download only. Every write endpoint returns `403` with code `read_only_api_key` for these keys. Use this role when you hand a key to a downstream consumer that should not change your data.
3. On the reveal screen, copy the `hbr_…` key. Console shows it once and cannot recover it afterward. Store it like a cloud secret access key.

Pick `read_write` if you intend to follow the encrypted flow below end to end.

## Create an encrypted bucket and upload a file

Private buckets are encrypted client-side, so Console stores ciphertext only and never sees your plaintext or decryption material. Creation goes through a reserve, sign, and finalize handshake, with a one-time service-key setup beforehand and a local decrypt step on download:

```
service key setup → get space → reserve → sign → finalize →
encrypt → upload → poll → download → decrypt
```

> **Warning**
>
> You end this section holding two secrets: an `hbr_…` API key, sent as `Authorization: Bearer …` on every request, and a `suiprivkey1…` service private key, kept locally and used to sign the finalize transaction and to authenticate decrypt sessions with Seal. Console shows both once. Store them like cloud access keys.
### Step 1: Create an encrypted-capable API key

In **Settings → API Keys → New API key**, pick role `read_write` and tick **Create**. The reveal screen now exposes two secrets:

- `hbr_…`, the API key.
- `suiprivkey1…`, the service private key: an Ed25519 secret in Sui keytool format. It is bound to this API key, and Console stores only the derived public address. It does not need a token balance.

Paste them into your `.env` as `HARBOR_SERVICE_PRIVKEY` and your bearer token, or into Postman.

### Step 2: Get your space ID

```http
GET /api/v1/spaces
```

The response's `data[]` array holds your spaces. Copy the `id` of the Personal Space created at sign-up.

### Step 3: Reserve the bucket

```http
POST /api/v1/spaces/{spaceId}/buckets
Content-Type: application/json

{ "name": "secrets", "scope": "private" }
```

Response (`201`):

```json
{
  "bucket_id": "…",
  "bytes": "<base64 sponsored Sui transaction>",
  "digest": "…",
  "state": "pending_policy"
}
```

`bytes` is the sponsored Sui transaction that creates the bucket's [Seal](/docs/data-security) access policy, with your service key's address as the sender. Console has already attached the gas sponsor's signature, so your service key only needs to add its own. `digest` is the sponsor digest, which Console uses at finalize to look up the sponsored transaction. The bucket stays in `pending_policy` and accepts no uploads until finalize succeeds.

### The example code

The signing, encryption, and decryption steps below come from the runnable [`walrus-harbor-quickstart`](https://github.com/MystenLabs/walrus-harbor-quickstart) example. Two files hold everything: `config.ts` pins the Testnet package and Seal key-server IDs, and `lib/seal.ts` wraps the signing and Seal encrypt/decrypt helpers. Clone the repo, copy `app/.env.example` to `app/.env`, and set `HARBOR_SERVICE_PRIVKEY` to your service private key.

<!-- ImportContent: GitHub source — resolve at export time or visit https://github.com/MystenLabs/walrus-harbor-quickstart/blob/main/app/src/config.ts -->

<!-- ImportContent: GitHub source — resolve at export time or visit https://github.com/MystenLabs/walrus-harbor-quickstart/blob/main/app/src/lib/seal.ts -->

### Step 4: Sign the bytes with the service key

Sign the reserved `bytes` with your service key. `sign-reserve.ts` loads the key with `loadKeypair` and calls `signReserveBytes`, returning the base64 signature you pass to finalize. Run it with `pnpm sign-reserve <base64-reserve-bytes>`:

<!-- ImportContent: GitHub source — resolve at export time or visit https://github.com/MystenLabs/walrus-harbor-quickstart/blob/main/app/src/scripts/sign-reserve.ts -->

### Step 5: Finalize

```http
POST /api/v1/buckets/{bucketId}/finalize
Content-Type: application/json

{ "signature": "<base64 signature from step 4>" }
```

Console combines your signature with the gas-sponsor signature and broadcasts the transaction. Response (`200`):

```json
{ "bucket_id": "…", "seal_policy_id": "…", "state": "active" }
```

`seal_policy_id` is the onchain bucket-policy object ID that Seal uses for access checks. The bucket is now usable.

### Step 6: Encrypt the file with Seal

Encrypt locally against the `seal_policy_id` from finalize. `encryptBytes` (in `lib/seal.ts`) derives a per-file Seal identity and encrypts against the bucket policy using `ORIGINAL_PACKAGE_ID` from `config.ts`. Seal pins identity derivation to the original package ID, so this value must stay fixed across package upgrades, otherwise an upgrade would invalidate every previously encrypted blob's key. `encrypt-file.ts` wires it up; run it with `pnpm encrypt-file <plaintextPath> <sealPolicyId>`:

<!-- ImportContent: GitHub source — resolve at export time or visit https://github.com/MystenLabs/walrus-harbor-quickstart/blob/main/app/src/scripts/encrypt-file.ts -->

It writes `<plaintextPath>.enc`, the byte stream you upload next.

### Step 7: Upload

```http
POST /api/v1/buckets/{bucketId}/files
Content-Type: multipart/form-data

file=@<encryptedObject>
```

Just after finalize, the onchain access grant needs a few seconds to propagate into Console's access index. Until it does, this endpoint returns `403` with code `mirror_missing_grant`. Retry every few seconds; usually 20 attempts is plenty in practice. Once the grant mirrors, the response is `202` with `data.id`.

### Step 8: Poll status

```http
GET /api/v1/buckets/{bucketId}/files/{fileId}/status
```

Returns `data.state`, one of `queued`, `active`, `completed`, or `failed`. Poll every second or two until the state is `completed`. Completion is typically under 30 seconds on Testnet.

### Step 9: Download and decrypt

```http
GET /api/v1/buckets/{bucketId}/files/{fileId}/download
```

The response is the raw Seal ciphertext. `decryptBytes` (in `lib/seal.ts`) builds the `seal_approve` access-check transaction, signs it with a short-lived session key, and decrypts client-side. `decrypt-file.ts` runs the download-to-plaintext round trip and checks the result against the original; run it with `pnpm decrypt-file <ciphertextPath> <sealPolicyId>`:

<!-- ImportContent: GitHub source — resolve at export time or visit https://github.com/MystenLabs/walrus-harbor-quickstart/blob/main/app/src/scripts/decrypt-file.ts -->

Decryption is fully client-side and never touches Console's backend.

## Get help

- Open an issue at [github.com/MystenLabs/harbor/issues](https://github.com/MystenLabs/harbor/issues) with the `developer-docs` label. Include the endpoint and method, the HTTP status, and the `code` field from any error response.
- Ask the team and other developers in the [Walrus Discord](https://discord.gg/walrusprotocol).
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
> The TypeScript snippets in this guide mirror the hosted quickstart. The canonical version, including the live package IDs and Seal key-server object IDs, lives in the [Harbor repository](https://github.com/MystenLabs/harbor); prefer copying from there so your values stay current across contract upgrades.
## Sign up and create an API key

1. Visit [testnet.harbor.walrus.xyz](https://testnet.harbor.walrus.xyz/) and sign in with Google. zkLogin provisions your account and a Personal Space automatically.
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

### Step 4: Sign the bytes with the service key

The signing step uses `@mysten/sui`, which handles the Bech32 private-key decode and the Sui signature envelope for you:

```ts
import { decodeSuiPrivateKey } from '@mysten/sui/cryptography';
import { Ed25519Keypair } from '@mysten/sui/keypairs/ed25519';
import { fromBase64 } from '@mysten/sui/utils';

const { secretKey } = decodeSuiPrivateKey(process.env.HARBOR_SERVICE_PRIVKEY);
const keypair = Ed25519Keypair.fromSecretKey(secretKey);
const { signature } = await keypair.signTransaction(fromBase64(bytes));
```

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

Encrypt locally against the `seal_policy_id` returned by finalize. `ORIGINAL` here means the original ID of the upgradeable package. Seal pins identity derivation to the original package ID, so encryption must use this value even after the package upgrades, otherwise an upgrade would invalidate every previously encrypted blob's key.

```ts
import { SealClient } from '@mysten/seal';
import { SuiGrpcClient } from '@mysten/sui/grpc';
import { bcs } from '@mysten/sui/bcs';

const HARBOR_ORIGINAL_PACKAGE_ID =
  '0x8b2429358e9b0f005b69fe8ad3cbd1268ad87f35047a21612e082c64824faf8d';
const SEAL_KEY_SERVER_OBJECT_IDS = [
  '0x6068c0acb197dddbacd4746a9de7f025b2ed5a5b6c1b1ab44dade4426d141da2',
  '0x164ac3d2b3b8694b8181c13f671950004765c23f270321a45fdd04d40cccf0f2',
  '0x9c949e53c36ab7a9c484ed9e8b43267a77d4b8d70e79aa6b39042e3d4c434105',
];

const sui = new SuiGrpcClient({
  network: 'testnet',
  baseUrl: 'https://fullnode.testnet.sui.io:443',
});
const seal = new SealClient({
  suiClient: sui,
  serverConfigs: SEAL_KEY_SERVER_OBJECT_IDS.map((objectId) => ({ objectId, weight: 1 })),
  verifyKeyServers: false,
});

// Each file's Seal ID = (bucket policy ID, 32 random bytes).
const SealIdentity = bcs.struct('SealIdentity', {
  policyObjectId: bcs.Address,
  nonce: bcs.fixedArray(32, bcs.u8()),
});
const nonce = Array.from(crypto.getRandomValues(new Uint8Array(32)));
const id = SealIdentity.serialize({ policyObjectId: sealPolicyId, nonce }).toHex();

const { encryptedObject } = await seal.encrypt({
  threshold: 2,
  packageId: HARBOR_ORIGINAL_PACKAGE_ID,
  id,
  data: plaintextBytes, // Uint8Array
});
```

`encryptedObject` is the byte stream you upload next.

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

The response is the raw Seal ciphertext. Reusing the `sui` and `seal` clients from step 6, decrypt by building the bucket's access-check transaction, signing it with a session key, and passing both the ciphertext and the transaction to `SealClient.decrypt`:

```ts
import { EncryptedObject, SessionKey } from '@mysten/seal';
import { Transaction } from '@mysten/sui/transactions';
import { fromHex } from '@mysten/sui/utils';

// Latest bucket-policy package, host of the seal_approve move call.
const HARBOR_LATEST_PACKAGE_ID =
  '0xc11d875481544e9b6c616f7d6704266e1633b4034eab7ed76626dc25ebfcd506';

// ciphertext = bytes from GET /download
const parsed = EncryptedObject.parse(ciphertext);
const idBytes = fromHex(parsed.id.startsWith('0x') ? parsed.id : '0x' + parsed.id);

// 1. Build the access-check transaction (transaction kind only, never broadcast).
const tx = new Transaction();
tx.moveCall({
  target: `${HARBOR_LATEST_PACKAGE_ID}::bucket_policy::seal_approve`,
  arguments: [tx.pure.vector('u8', idBytes), tx.object(sealPolicyId)],
});
const txBytes = await tx.build({ client: sui, onlyTransactionKind: true });

// 2. A session key lets the Seal key servers verify the caller without re-signing per request.
const sessionKey = await SessionKey.create({
  address: keypair.toSuiAddress(),
  packageId: HARBOR_ORIGINAL_PACKAGE_ID,
  ttlMin: 10,
  suiClient: sui,
  signer: keypair,
});

// 3. Decrypt. SealClient fetches threshold key shares and reconstructs the key locally.
const plaintext = await seal.decrypt({ data: ciphertext, sessionKey, txBytes });
```

Decryption is fully client-side and never touches Console's backend.

## Get help

- Open an issue at [github.com/MystenLabs/harbor/issues](https://github.com/MystenLabs/harbor/issues) with the `developer-docs` label. Include the endpoint and method, the HTTP status, and the `code` field from any error response.
- Ask the team and other developers in the [Walrus Discord](https://discord.gg/walrusprotocol).
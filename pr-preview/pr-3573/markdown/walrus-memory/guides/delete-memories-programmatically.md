> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

## Overview

Use the Security Delete API to find memories owned by a wallet, select memories older
than a cutoff date, and permanently delete them. The API prepares and sponsors the Sui
transaction; your wallet authenticates the requests and signs the prepared transaction.

> **Warning**
>
> Deletion is permanent. Start with the dry run in this guide, review every blob ID, and
>   only then enable deletion.
The deletion flow uses these endpoints:

| Step | Endpoint | Purpose |
| --- | --- | --- |
| 1 | `POST /api/security-delete-auth/challenge` | Request a single-use wallet challenge. |
| 2 | `POST /api/security-delete-auth/verify` | Exchange the signed challenge for a short-lived Bearer token. |
| 3 | `GET /api/security-deletable-blobs` | List tracked blobs that the wallet can delete. |
| 4 | `POST /api/security-deletions` | Prepare a sponsored deletion transaction. |
| 5 | `POST /api/security-deletions/{batchId}/submit` | Submit the wallet signature and execute the deletion. |

## Before you begin

You need:

- Node.js 20 or later.
- The base URL of a Walrus Memory deployment with the Security Delete API enabled.
- The Sui private key for the wallet that owns the memories.

The example uses an Ed25519 key. Keep the key in a secret manager or environment variable;
never commit it to source control. For an interactive application, use the connected
wallet's `signPersonalMessage` and `signTransaction` methods instead.

Create a small Node.js project and install the Sui SDK:

[Source: guides/delete-memories-programmatically.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/guides/delete-memories-programmatically.md)

```bash
$ npm install @mysten/sui
$ npm install --save-dev tsx
```

## Create the deletion script

Create `delete-old-memories.ts`:

[Source: guides/delete-memories-programmatically.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/guides/delete-memories-programmatically.md)

```ts
import { decodeSuiPrivateKey } from '@mysten/sui/cryptography'
import { Ed25519Keypair } from '@mysten/sui/keypairs/ed25519'
import { fromBase64 } from '@mysten/sui/utils'

type Challenge = {
  challengeId: string
  challenge: string
  expiresInSecs: number
}

type BlobItem = {
  blobId: string
  objectId: string | null
  createdAt: string
  state: string
}

type BlobPage = {
  items: BlobItem[]
  limits: { deleteBatchMax: number }
  nextCursor: string | null
}

type PreparedBatch = {
  batchId: string | null
  txBytes: string | null
  included: number
  excluded: Array<{ blobId: string; reason: string }>
  expiresAt: string | null
}

type BatchStatus = {
  state: 'awaiting_signature' | 'executing' | 'completed' | 'failed' | 'rolled_back'
  blobCount: number
  digest: string | null
  resolvedAt: string | null
}

const baseUrl = requireEnv('MEMWAL_URL').replace(/\/$/, '')
const privateKey = requireEnv('SUI_PRIVATE_KEY')
const maxAgeDays = positiveNumber(process.env.MEMORY_MAX_AGE_DAYS ?? '90')
const confirmed = process.env.CONFIRM_DELETE === 'true'

const decoded = decodeSuiPrivateKey(privateKey)
if (decoded.scheme !== 'ED25519') {
  throw new Error(`This example requires an Ed25519 key, received ${decoded.scheme}`)
}
const signer = Ed25519Keypair.fromSecretKey(decoded.secretKey)
const address = signer.toSuiAddress()

function requireEnv(name: string): string {
  const value = process.env[name]?.trim()
  if (!value) throw new Error(`${name} is required`)
  return value
}

function positiveNumber(value: string): number {
  const parsed = Number(value)
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error('MEMORY_MAX_AGE_DAYS must be a positive number')
  }
  return parsed
}

async function api<T>(path: string, init: RequestInit = {}, token?: string): Promise<T> {
  const headers = new Headers(init.headers)
  headers.set('accept', 'application/json')
  if (init.body) headers.set('content-type', 'application/json')
  if (token) headers.set('authorization', `Bearer ${token}`)

  const response = await fetch(`${baseUrl}${path}`, { ...init, headers })
  const body = await response.json().catch(() => null)
  if (!response.ok) {
    const error = body?.error
    throw new Error(
      `${response.status} ${error?.code ?? 'UNKNOWN'}: ${error?.message ?? 'Request failed'}`,
    )
  }
  return body as T
}

async function authenticate(): Promise<string> {
  const challenge = await api<Challenge>('/api/security-delete-auth/challenge', {
    method: 'POST',
    body: JSON.stringify({ address }),
  })

  // Sign the exact UTF-8 bytes returned by the server. Do not rebuild the message.
  const signed = await signer.signPersonalMessage(
    new TextEncoder().encode(challenge.challenge),
  )

  const verified = await api<{ token: string }>('/api/security-delete-auth/verify', {
    method: 'POST',
    body: JSON.stringify({
      challengeId: challenge.challengeId,
      address,
      signature: signed.signature,
    }),
  })
  return verified.token
}

async function listDeletable(token: string): Promise<{
  items: BlobItem[]
  batchMax: number
}> {
  const items: BlobItem[] = []
  let cursor: string | null = null
  let batchMax = 900

  do {
    const query = new URLSearchParams({ state: 'deletable', limit: '200' })
    if (cursor) query.set('cursor', cursor)
    const page = await api<BlobPage>(
      `/api/security-deletable-blobs?${query.toString()}`,
      {},
      token,
    )
    items.push(...page.items)
    batchMax = Math.min(batchMax, page.limits.deleteBatchMax)
    cursor = page.nextCursor
  } while (cursor)

  return { items, batchMax }
}

async function main() {
  const token = await authenticate()
  const { items, batchMax } = await listDeletable(token)
  const cutoff = new Date(Date.now() - maxAgeDays * 24 * 60 * 60 * 1000)
  const candidates = items
    .filter((item) => new Date(item.createdAt) < cutoff)
    .sort((a, b) => a.createdAt.localeCompare(b.createdAt))

  console.log(`Wallet: ${address}`)
  console.log(`Cutoff: ${cutoff.toISOString()}`)
  console.table(
    candidates.map(({ blobId, objectId, createdAt }) => ({ blobId, objectId, createdAt })),
  )

  if (candidates.length === 0) {
    console.log('No deletable memories are older than the cutoff.')
    return
  }
  if (!confirmed) {
    console.log('Dry run only. Set CONFIRM_DELETE=true after reviewing the list.')
    return
  }

  let deleted = 0
  for (let offset = 0; offset < candidates.length; offset += batchMax) {
    const blobIds = candidates.slice(offset, offset + batchMax).map((item) => item.blobId)
    const prepared = await api<PreparedBatch>(
      '/api/security-deletions',
      {
        method: 'POST',
        body: JSON.stringify({ mode: 'selection', blobIds }),
      },
      token,
    )

    if (!prepared.batchId || !prepared.txBytes) {
      console.log('The batch contained nothing that remained deletable.')
      continue
    }
    if (prepared.excluded.length > 0) {
      console.table(prepared.excluded)
    }

    // txBytes is the exact sponsored TransactionData prepared by the server.
    const signed = await signer.signTransaction(fromBase64(prepared.txBytes))

    try {
      const result = await api<{ deleted: number; digest: string }>(
        `/api/security-deletions/${encodeURIComponent(prepared.batchId)}/submit`,
        {
          method: 'POST',
          body: JSON.stringify({ signature: signed.signature }),
        },
        token,
      )
      deleted += result.deleted
      console.log(`Deleted ${result.deleted} memories in transaction ${result.digest}`)
    } catch (submitError) {
      // A transport error can leave execution outcome uncertain. Check the batch before
      // preparing a replacement transaction for the same blobs.
      const status = await api<BatchStatus>(
        `/api/security-deletions/${encodeURIComponent(prepared.batchId)}`,
        {},
        token,
      )
      console.error(`Batch ${prepared.batchId} is ${status.state}.`, submitError)
      throw new Error('Deletion stopped; resolve this batch before retrying.')
    }
  }

  console.log(`Deleted ${deleted} memories in total.`)
}

await main()
```

## Run a dry run

Set the API URL, wallet key, and age cutoff. The script only lists candidates unless
`CONFIRM_DELETE` is exactly `true`.

[Source: guides/delete-memories-programmatically.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/guides/delete-memories-programmatically.md)

```bash
$ export MEMWAL_URL="https://your-memwal-deployment.example"
$ export SUI_PRIVATE_KEY="suiprivkey1..."
$ export MEMORY_MAX_AGE_DAYS="90"

$ npx tsx delete-old-memories.ts
```

Review the wallet address, cutoff timestamp, blob IDs, object IDs, and creation dates in
the output.

## Delete the reviewed memories

After you confirm the dry-run output, enable deletion and run the same script again:

[Source: guides/delete-memories-programmatically.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/guides/delete-memories-programmatically.md)

```bash
$ export CONFIRM_DELETE="true"
$ npx tsx delete-old-memories.ts
```

The script chunks the selection using the server's `deleteBatchMax` value. Each batch
requires a newly prepared transaction and wallet signature.

## Verify the deletion

Run the script again without `CONFIRM_DELETE`. Successfully deleted blobs no longer appear
in the default `deletable` list. To inspect terminal states directly, request them with the
same Bearer token:

[Source: guides/delete-memories-programmatically.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/guides/delete-memories-programmatically.md)

```http
GET /api/security-deletable-blobs?state=deleted,deleted_external&limit=200
Authorization: Bearer <token>
```

If submitting a batch times out or returns `RPC_UNAVAILABLE`, query
`GET /api/security-deletions/{batchId}` until it reaches a terminal state. Do not prepare a
replacement batch for the same blobs while its state is `executing`.

To delete memories through the dashboard instead, see
[Delete old memories](/walrus-memory/guides/delete-old-memories).
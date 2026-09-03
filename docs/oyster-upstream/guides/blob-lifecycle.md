# Blob Lifecycle

Blobs progress through a well-defined lifecycle from upload to expiration.
Oyster's automatic extension service keeps your data alive.

## Upload and expiration

Walrus storage is epoch-scoped, not time-scoped. When you upload a blob,
Oyster registers it under your account's `StoragePool`, a single
onchain object whose `end_epoch` defines the lifetime of every blob it
holds. The first upload from an account lazily creates the pool with
`POOL_INITIAL_EPOCHS_AHEAD` of runway (default `5`). Subsequent uploads
share that same expiration.

The pool's `end_epoch` is surfaced on the account as
`pool_end_epoch`. To inspect remaining runway, compare it against
the network's current epoch. Blob responses do not carry a per-blob
expiration. Every blob in the account shares the pool's lifetime.

## Automatic extension

Oyster runs a **background extension worker** (`oysterd extend`) that
keeps every account's `StoragePool` ahead of expiration. As long as the
worker is running and the account's Pearl-derived wallet has WAL and
SUI to spend, your blobs persist indefinitely.

### What the worker guarantees

While the worker is running:

- Any account whose `pool_end_epoch` falls within
  `current_epoch + POOL_EXTEND_LOOKAHEAD_EPOCHS` is picked up and its
  pool extended by `POOL_EXTEND_EPOCHS` Walrus epochs, provided the
  Pearl-derived wallet has the WAL and SUI to cover it.
- Each pool is extended at most once per
  `EXTENSION_CLAIM_COOLDOWN_SECS` window, so retries and
  `account.funding_required` webhook deliveries are naturally rate-
  limited per account. The same cooldown doubles as webhook-spam
  suppression. A row that just emitted `account.funding_required`
  cannot re-emit until the cooldown expires.
- Latency between an account becoming eligible and its pool being
  extended is bounded above by `EXTENSION_IDLE_SLEEP_SECS` plus the
  RPC time of one extension.

### Horizontal scaling

The worker is safe to run as multiple replicas against the same
database. Each pool is claimed by exactly one replica per cycle, so
two extenders never double-extend the same pool. The public Oyster
Testnet runs 2 extender replicas behind a shared DB.

### Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `POOL_EXTEND_LOOKAHEAD_EPOCHS` | `7` | Claim any pool expiring within `current_epoch + this`. Leave default unless your network's epoch length is unusual. |
| `POOL_EXTEND_EPOCHS` | `5` | Walrus epochs each `extend_storage_pool` PTB extends by. Tune per network: Testnet ≈ 1 day/epoch → `30`; Mainnet ≈ 14 days/epoch → `4`. |
| `EXTENSION_IDLE_SLEEP_SECS` | `30` | Sleep when a cycle finds zero work. Leave default unless tuning latency vs. RPC load. |
| `EXTENSION_BUSY_SLEEP_MS` | `250` | Sleep between cycles while there's still work to drain. Leave default. |
| `EXTENSION_CLAIM_BATCH_SIZE` | `100` | Max pool rows claimed per cycle. Leave default unless DB round-trip latency dominates. |
| `EXTENSION_CLAIM_COOLDOWN_SECS` | `60` | Per-row claim TTL; also the webhook re-notify backoff for the same account. Leave default. |

### Insufficient funds

If the Pearl-derived wallet for an account is short on WAL or SUI, the
`extend_storage_pool` PTB fails with an insufficient-funds error.
Oyster then takes these steps:

1. Logs the failure.
2. POSTs an `account.funding_required` webhook to the owning app's
   configured receiver URL (if any).
3. Leaves the cooldown TTL stamped on the row so the same account
   does not re-trigger the webhook for `EXTENSION_CLAIM_COOLDOWN_SECS`.

The next cycle re-claims the row once the cooldown expires. If the
wallet is still underfunded, another webhook fires. See
[Webhooks](webhooks.md) for the full payload schema, retry policy,
circuit-breaker behavior, and receiver examples.

## Auto-grow

The pool's encoded-bytes reservation grows on demand. The first
upload that does not fit in the current reservation submits a
`register_pooled_blobs` PTB whose `grow_by` reserves the missing
capacity in the same transaction.

### When auto-grow retries

In a horizontally scaled Oyster deployment, two replicas can each
compute `grow_by` against the same onchain snapshot, then race to
submit their register PTBs. The replica that lands second sees its
`storage_pool::add_blob` Move call abort with `EInsufficientCapacity`
(code `6`) because the first replica already consumed the reserve.

Oyster handles this by:

1. Refreshing the onchain `StoragePoolInnerV1` through the Sui RPC's
   gRPC `StateService.ListDynamicFields`.
2. Reconciling the DB's `pool_reserved_encoded_bytes` and
   `pool_used_encoded_bytes` counters against onchain truth (the
   chain is authoritative, and a stale DB counter is overwritten).
3. Recomputing `grow_by` from the reconciled state.
4. Resubmitting the register PTB exactly once.

If the resubmit also aborts with `EInsufficientCapacity`, the error
is surfaced to the caller. There is no second self-heal. A steady-state of
cross-replica thrash is not a normal failure mode and the operator
should investigate.

### Interaction with the per-account cap

Auto-grow runs *after* the per-account
[`max_unencoded_bytes` cap](../json-api/blobs.md#store-blob) is
checked. The cap pre-check uses the same forward encoder
(`f = encoded_blob_length_for_n_shards`) the upload path uses to
project the post-upload encoded total, so a successful cap check
already accounts for the would-be `grow_by`. Auto-grow can never
push the account's encoded-bytes usage past the threshold the cap
implies. (On small-shard / large-cap testbeds where the forward
encoder would overflow `i64`, the pre-check falls back to a
saturating comparison rather than a `500`.)

By default the cap is an *upper* bound on storable unencoded bytes
(each blob's fixed metadata overhead is paid per blob, so many small
blobs hit the cap early). Setting a non-zero per-account
[`avg_blob_size`](../json-api/admin.md#lower-bound-semantics-avg_blob_size)
inflates the admission ceiling by the per-blob expansion factor
`f(s)/s`, turning the cap into a *lower* bound: at least
`max_unencoded_bytes` unencoded bytes are guaranteed storable when the
account's blobs average ≥ `s`. New accounts default to a 10 MB
`avg_blob_size`; `avg_blob_size = 0` preserves the upper-bound
behavior.

### Why there is no per-process lock

Oyster scales horizontally: a `Mutex` inside one replica cannot
coordinate with another replica's process, and chain-side state
would still drift under concurrent uploads. The onchain
`StoragePoolInnerV1` is the source of truth. The one-shot
reconcile-and-retry above absorbs the inevitable drift without
serializing uploads.

## Blob states

A blob's lifetime is bound to its account's `StoragePool`:

```
Upload → Active → Pool Approaching Expiry → Pool Extended → Active → ...
                                         ↘ (if wallet underfunded)
                                           Funding Required webhook
```

| State | Description |
|-------|-------------|
| **Active** | Blob is registered in a pool with `pool_end_epoch > current_epoch` |
| **Pool Approaching Expiry** | `pool_end_epoch < current_epoch + POOL_EXTEND_LOOKAHEAD_EPOCHS`; the worker claims and extends |
| **Pool Extended** | `extend_storage_pool` PTB succeeded; `pool_end_epoch` advanced |
| **Funding Required** | PTB failed insufficient-funds; webhook fired; cooldown TTL active |

## Deletion

Blobs can be explicitly deleted at any time through the API:

- **JSON API:** `DELETE /api/v1/buckets/{bucket}/blobs/{key}`
- **S3 API:** `DeleteObject`
- **CLI:** `oyster delete <key> --bucket <bucket>`

Deletion is reference-counted at the content-addressed level. The
onchain `delete_pooled_blob` PTB fires only when the last reference
to a given `blob_id` is removed from the account (see
[Content Addressing](content-addressing.md)).

## Local vs. onchain storage

| Aspect | Local (filesystem) | Onchain (Walrus) |
|--------|-------------------|-------------------|
| Expiration tracked | Not applicable | `accounts.pool_end_epoch` (Walrus epochs) |
| Auto-renewal | Not applicable | Yes (extension worker, multi-instance safe) |
| `pooled_blob_object_id` | `null` | Sui object ID of the registered `PooledBlob` |
| Storage scope | Per-blob file on disk | Pool-scoped capacity reservation |

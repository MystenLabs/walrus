# BlobManager

```admonish warning title="Preview Feature"
BlobManager is currently under active development and is not yet available on Mainnet or Testnet.
The APIs and behavior described in this document may change before the final release. This
documentation serves as a preview and request for comments (RFC) on the design.
```

BlobManager is a shared storage container for managing multiple blobs with unified lifecycle
management, with additional features for community-funded storage extensions and flexible access
control.

## Overview

Unlike regular Walrus blobs where each blob has its own storage resource and end epoch, a
BlobManager maintains a **shared storage pool** that all managed blobs draw from. This simplifies
lifecycle management: instead of tracking dozens of individual blob expirations, you manage a single
storage allocation that covers all blobs in the manager.

Key features:

- **Unified storage pool**: All blobs share storage capacity and expiration epoch
- **Community-funded extensions**: Anyone can extend storage lifetime (with configurable policies)
- **Tip incentives**: Reward community members who help extend your storage
- **Flexible access control**: Separate permissions for owners, writers, and the public

```admonish warning title="Blob ID Uniqueness"
Only one blob with a given blob ID can exist in a BlobManager at any time. A blob is either
**deletable** or **permanent**—not both. If you store the same content twice, the second store
will update the existing blob's persistence: a deletable blob can be converted to permanent by
re-storing as permanent, but a permanent blob cannot be made deletable.
```

## Sui structures

The BlobManager is implemented as a Sui shared object using a **versioned design pattern** for upgradability.
The outer `BlobManager` struct serves as a stable interface, while the actual implementation is stored
in `BlobManagerInnerV1` as a dynamic field. This allows the implementation to be upgraded without
breaking external contracts.

```move
/// The stable interface object.
public struct BlobManager has key, store {
    id: UID,
    /// Version number (currently 1) used to access the inner implementation.
    version: u64,
}

/// The implementation stored as a dynamic field of BlobManager.
public struct BlobManagerInnerV1 has store {
    /// Unified storage pool for all managed blobs.
    storage: BlobStorage,
    /// Coin stash for community funding (WAL for storage, SUI for tips).
    coin_stash: BlobManagerCoinStash,
    /// Policy controlling who can extend storage, when, and tip amount.
    extension_policy: ExtensionPolicy,
    /// Policy controlling how much storage can be purchased from the coin stash.
    storage_purchase_policy: StoragePurchasePolicy,
    /// Maps capability IDs to their info, including revocation status.
    caps_info: Table<ID, CapInfo>,
}
```

The inner implementation is accessed via dynamic field lookup using the version number as the key.
This pattern follows the same design as the Walrus `System` / `SystemStateInnerV1` structs.

Access to the BlobManager is controlled through capability objects, the owner of a cap object has
the corresponding permissions.

```move
public struct BlobManagerCap has key, store {
    id: UID,
    manager_id: ID,
    can_delegate: bool,
    can_withdraw_funds: bool,
}
```

Capabilities determine what operations an address can perform:

| Permission           | Can do                                                    |
|----------------------|-----------------------------------------------------------|
| Any cap              | Store, delete, and manage blobs; buy storage              |
| `can_withdraw_funds` | Configure policies; withdraw funds                        |
| `can_delegate`       | Create and revoke capabilities                            |

The initial creator receives a capability with all permissions. Capabilities can be revoked by
delegate caps, which prevents the revoked capability from performing any operations.

## Creating a BlobManager

Create a new BlobManager with an initial storage allocation and WAL deposit:

```rust
// Create manager with 1GB storage for 10 epochs, with 10 WAL initial deposit
let (manager_id, admin_cap) = sui_client
    .create_blob_manager(
        1_000_000_000,   // 1GB capacity
        10,              // 10 epochs ahead
        10_000_000_000,  // 10 WAL initial deposit for coin stash
    )
    .await?;
```

The initial WAL deposit funds the coin stash, which is used for storage operations like registering
blobs. Additional funds can be deposited at any time—**anyone** can deposit to a BlobManager's coin
stash without requiring a capability:

```rust
// Deposit more WAL for storage purchases and extensions (anyone can call)
sui_client.deposit_wal_to_blob_manager(manager_id, 10_000_000_000).await?;  // 10 WAL

// Deposit SUI for tips to community extenders (anyone can call)
sui_client.deposit_sui_to_blob_manager(manager_id, 1_000_000_000).await?;   // 1 SUI
```

## Storing and managing blobs

### Storing blobs

Store blobs through the BlobManagerClient:

```rust
// Store a permanent blob (cannot be deleted)
let result = blob_manager_client
    .store_blob(data, BlobPersistence::Permanent)
    .await?;

// Store a deletable blob (can be removed to reclaim storage)
let result = blob_manager_client
    .store_blob(data, BlobPersistence::Deletable)
    .await?;
```

### Reading blobs

Reading managed blobs works the same as regular blobs:

```rust
let data = client.read_blob(&blob_id).await?;
```

### Deleting blobs

Only deletable blobs can be removed. Deleting a blob returns its storage capacity to the pool:

```rust
blob_manager_client.delete_blob(blob_id).await?;
```

### Moving existing blobs

You can import an existing regular blob into a BlobManager:

```admonish warning title="One-Way Operation"
Moving a blob into a BlobManager is a **one-way operation**. Once imported, the blob cannot be
moved back out to become a regular blob again. The blob's lifecycle becomes tied to the manager.
```

```rust
sui_client
    .move_blob_into_manager(manager_id, cap_id, blob_object)
    .await?;
```

The blob's storage is consumed and its capacity is added to the manager's pool.

```admonish warning title="Epoch Alignment Requirements"
Regular blobs can only be moved into a BlobManager if their `end_epoch` is **less than or equal to**
the BlobManager's `end_epoch`. This validation prevents blobs from outliving their manager's storage
allocation.

- **Blobs expiring before manager**: The system automatically purchases extension storage from the
  coin stash to align the blob's storage with the manager's end epoch.
- **Blobs expiring after manager**: These are rejected with error `ERegularBlobEndEpochTooLarge`.
  You must first extend the BlobManager's storage period before importing such blobs.
- **Already aligned blobs**: Move in directly without any storage adjustments.
```

### Blob attributes

Attach metadata to managed blobs:

```rust
// Set an attribute
blob_manager_client
    .set_attribute(blob_id, "content-type".to_string(), "image/png".to_string())
    .await?;

// Remove an attribute
blob_manager_client.remove_attribute(blob_id, "content-type".to_string()).await?;

// Clear all attributes
blob_manager_client.clear_attributes(blob_id).await?;
```

## Storage management

### Buying additional storage

Increase the storage pool capacity using funds from the coin stash. Any capability holder (writer,
fund manager, or admin) can purchase additional storage:

```rust
// Buy 500MB more storage (any cap holder can call)
blob_manager_client.buy_storage_from_stash(500_000_000).await?;
```

#### Storage purchase policy

The BlobManager implements a **storage purchase policy** system to prevent abuse and ensure fair
access to storage resources. This policy controls when and how much storage can be purchased from
the coin stash.

**Policy types:**

1. **Unlimited**: No restrictions on storage purchases (not recommended for production)
2. **FixedCap**: Hard limit on the maximum storage that can be purchased in a single transaction
3. **ConditionalPurchase**: Only allows purchases when available storage is below a threshold

**Default policy (ConditionalPurchase):**

The BlobManager defaults to a conditional purchase policy with a 15GB threshold:
- Storage can only be purchased when available storage < 15GB
- Maximum purchase amount is capped at 15GB per transaction
- Prevents malicious actors from draining funds by purchasing excessive storage

```move
/// Default configuration in BlobManager
storage_purchase_policy: storage_purchase_policy::conditional_purchase(15_000_000_000), // 15GB
```

**How it works:**

When calling `buy_storage_from_stash()`, the system:
1. Checks the current available storage in the pool
2. If using ConditionalPurchase policy:
   - Verifies available storage is below the threshold (15GB)
   - Caps the requested amount to the threshold maximum
3. Processes the purchase with the allowed amount

**Security benefits:**
- **Prevents fund draining**: Limits how much storage a malicious actor can purchase
- **Ensures availability**: Keeps storage accessible for legitimate users
- **Market balance**: Prevents hoarding of storage resources
- **Configurable**: Administrators can adjust policy based on market conditions

### Extending storage with rewards

The extension policy controls who can extend storage and when. See
[Extension policies](#extension-policies) for details.

```rust
// Anyone can call this (subject to policy)
client.extend_blob_manager_storage(manager_id, 5).await?;  // Extend by 5 epochs
```

### Querying storage info

```rust
let info = blob_manager_client.get_storage_info().await?;

let balances = blob_manager_client.get_coin_stash_balances().await?;
```

## Extension policies

Extension policies control how and when community members can extend the BlobManager's storage
lifetime.

### Disabled policy

No one can extend storage.

```rust
blob_manager_client.set_extension_policy_disabled().await?;
```

### Constrained policy

Allow extensions within specified constraints with optional tipping:

```rust
// Allow extensions when within 5 epochs of expiry, max 10 epochs per extension,
// tip 0.1 SUI (100_000_000 MIST) to extenders
blob_manager_client
    .set_extension_policy_constrained(5, 10, 100_000_000)
    .await?;
```

Parameters:
- `expiry_threshold_epochs`: Extensions only allowed when current epoch is within this many epochs
  of the storage end epoch
- `max_extension_epochs`: Maximum epochs that can be added in a single extension
- `tip_amount`: SUI tip in MIST to reward community extenders (1 SUI = 1,000,000,000 MIST)

When a community member calls `extend_blob_manager_storage()`, they:
1. Pay the gas fee of the transaction
2. Pay nothing for storage (WAL comes from the coin stash)
3. Receive the configured SUI tip

This creates a **keeper incentive**: community members can monitor BlobManagers approaching expiry
and extend them to earn tips.

### Admin storage adjustment

For administrators with `can_withdraw_funds` permission, `adjust_storage()` provides direct control
over storage capacity and duration, bypassing all policy constraints:

```rust
// Increase capacity to 10GB and extend to epoch 200
blob_manager_client
    .adjust_storage(10_000_000_000, 200)
    .await?;
```

This function:
- Bypasses extension policy (works even when disabled)
- Bypasses storage_purchase_policy (no limits)
- Can only increase values (not decrease capacity or end_epoch)
- Uses WAL from the coin stash for purchases

## Access control

### Creating capabilities

Share access by creating new capabilities:

```rust
// Create a writer-only capability (can store/delete blobs, cannot configure policies)
let writer_cap = blob_manager_client
    .create_cap(false, false)  // no delegate, no withdraw_funds
    .await?;

// Create a can_withdraw_funds capability (can configure policies and withdraw)
let withdraw_funds_cap = blob_manager_client
    .create_cap(false, true)  // no delegate, can_withdraw_funds
    .await?;

// Transfer the capability to another address
// (use standard Sui transfer)
```

### Revoking capabilities

Delegate caps can revoke capabilities to prevent them from performing any operations:

```rust
// Revoke a capability (requires can_delegate permission)
blob_manager_client
    .revoke_cap(cap_id_to_revoke)
    .await?;
```

Revocation is permanent and cannot be undone. The revoked capability object still exists but cannot
be used for any BlobManager operations.

### Withdrawing funds

Caps with can_withdraw_funds can withdraw from the coin stash, note the fund will go to the
withdrawer's address. A blob manager's delegate cap could revoke all can_withdraw_funds caps, so
that the community funds cannot be withdrawn by anyone.

```rust
blob_manager_client.withdraw_wal(1_000_000_000).await?;  // Withdraw 1 WAL
blob_manager_client.withdraw_sui(500_000_000).await?;    // Withdraw 0.5 SUI
```

## Community operations

These operations can be performed by anyone, without a capability:

### Depositing funds

Anyone can contribute to a BlobManager's coin stash:

```rust
// Deposit WAL (used for storage operations)
blob_manager_client.deposit_wal_to_coin_stash(1_000_000_000).await?;

// Deposit SUI (used for tips)
blob_manager_client.deposit_sui_to_coin_stash(100_000_000).await?;
```

### Extending storage

Anyone can extend storage (subject to the extension policy):

```rust
// Extend storage and receive tip (if configured)
client.extend_blob_manager_storage(manager_id, 5).await?;
```

## API reference

```admonish note title="SDK and API Availability"
We aim to provide the same set of BlobManager APIs across the Rust SDK, TypeScript SDK, and
publisher/aggregator HTTP API. Note that when using the publisher for BlobManager operations, it
should be deployed as a **private publisher** since it requires wallet access for signing
transactions. Do not expose a BlobManager-enabled publisher publicly without proper authentication.
```

### Rust SDK

#### Can_withdraw_funds operations

| Operation | Description |
|-----------|-------------|
| `create_blob_manager(capacity, epochs, initial_wal)` | Create a new BlobManager with initial storage and WAL deposit |
| `create_cap(can_delegate, can_withdraw_funds)` | Create a new capability (requires can_delegate) |
| `revoke_cap(cap_id)` | Revoke a capability (requires can_delegate) |
| `set_extension_policy_disabled()` | Disable community extensions |
| `set_extension_policy_constrained(threshold, max, tip)` | Set constrained extension policy with tip |
| `adjust_storage(capacity, end_epoch)` | Admin storage adjustment (increase only) |
| `withdraw_wal(amount)` | Withdraw WAL from coin stash |
| `withdraw_sui(amount)` | Withdraw SUI from coin stash |

#### Writer operations

| Operation | Description |
|-----------|-------------|
| `store_blob(data, persistence)` | Store a new blob |
| `delete_blob(blob_id)` | Delete a deletable blob |
| `move_blob_into_manager(blob)` | Import an existing blob |
| `buy_storage_from_stash(bytes)` | Purchase additional storage |
| `set_attribute(blob_id, key, value)` | Set blob metadata |
| `remove_attribute(blob_id, key)` | Remove blob metadata |
| `clear_attributes(blob_id)` | Remove all blob metadata |

#### Community operations

| Operation | Description |
|-----------|-------------|
| `read_blob(blob_id)` | Read blob data |
| `deposit_wal_to_coin_stash(amount)` | Donate WAL for storage |
| `deposit_sui_to_coin_stash(amount)` | Donate SUI for tips |
| `extend_blob_manager_storage(manager_id, epochs)` | Extend storage (earn tip) |
| `get_blob_manager_info()` | Query storage and balance info |
| `get_managed_blob(blob_id)` | Query a specific blob |

### HTTP API (Publisher/Aggregator)

The following HTTP endpoints are proposed for BlobManager operations. These mirror the Rust SDK
APIs and will be available on private publishers configured with a wallet.

```admonish warning title="Private Publisher Required"
BlobManager HTTP APIs require a private publisher with wallet access. These endpoints should NOT
be exposed publicly without authentication, as they can modify on-chain state and spend funds.
```

#### Fund manager endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/v1/blob-managers` | Create a new BlobManager (with initial WAL) |
| `POST` | `/v1/blob-managers/{manager_id}/cap` | Create a new capability |
| `DELETE` | `/v1/blob-managers/{manager_id}/cap/{cap_id}` | Revoke a capability |
| `PUT` | `/v1/blob-managers/{manager_id}/extension-policy` | Set extension policy |
| `PUT` | `/v1/blob-managers/{manager_id}/tip-policy` | Set tip policy |
| `POST` | `/v1/blob-managers/{manager_id}/withdraw-wal` | Withdraw WAL |
| `POST` | `/v1/blob-managers/{manager_id}/withdraw-sui` | Withdraw SUI |

#### Writer endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `PUT` | `/v1/blob-managers/{manager_id}/blobs` | Store a blob |
| `DELETE` | `/v1/blob-managers/{manager_id}/blobs/{blob_id}` | Delete a blob |
| `POST` | `/v1/blob-managers/{manager_id}/import-blob` | Import existing blob |
| `POST` | `/v1/blob-managers/{manager_id}/buy-storage` | Buy additional storage |
| `PUT` | `/v1/blob-managers/{manager_id}/blobs/{blob_id}/attributes/{key}` | Set attribute |
| `DELETE` | `/v1/blob-managers/{manager_id}/blobs/{blob_id}/attributes/{key}` | Remove attribute |
| `DELETE` | `/v1/blob-managers/{manager_id}/blobs/{blob_id}/attributes` | Clear all attributes |

#### Community endpoints (Aggregator + Publisher)

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/v1/blobs/{blob_id}` | Read blob (same as regular blobs) |
| `POST` | `/v1/blob-managers/{manager_id}/deposit-wal` | Deposit WAL |
| `POST` | `/v1/blob-managers/{manager_id}/deposit-sui` | Deposit SUI |
| `POST` | `/v1/blob-managers/{manager_id}/extend` | Extend storage (earn tip) |
| `GET` | `/v1/blob-managers/{manager_id}` | Get manager info |
| `GET` | `/v1/blob-managers/{manager_id}/blobs/{blob_id}` | Get managed blob info |

#### HTTP API examples

**Create a BlobManager:**

```sh
curl -X POST "$PUBLISHER/v1/blob-managers?capacity=1000000000&epochs=10&initial_wal=10000000000"
```

**Store a blob in a BlobManager:**

```sh
curl -X PUT "$PUBLISHER/v1/blob-managers/$MANAGER_ID/blobs?deletable=true" \
  --upload-file "some/file"
```

**Read a managed blob (same as regular blobs):**

```sh
curl "$AGGREGATOR/v1/blobs/$BLOB_ID" -o output.file
```

**Extend storage (anyone can call):**

```sh
curl -X POST "$PUBLISHER/v1/blob-managers/$MANAGER_ID/extend?epochs=5"
```

**Get BlobManager info:**

```sh
curl "$AGGREGATOR/v1/blob-managers/$MANAGER_ID"
```

Response:
```json
{
  "managerId": "0x...",
  "storage": {
    "totalCapacity": 1000000000,
    "usedCapacity": 250000000,
    "availableCapacity": 750000000,
    "endEpoch": 42
  },
  "coinStash": {
    "walBalance": 5000000000,
    "suiBalance": 100000000
  },
  "extensionPolicy": {
    "type": "constrained",
    "expiryThresholdEpochs": 5,
    "maxExtensionEpochs": 10
  },
  "tipPolicy": {
    "type": "fixedAmount",
    "amount": 10000000
  },
  "storagePurchasePolicy": {
    "type": "conditionalPurchase",
    "thresholdBytes": 15000000000
  }
}
```

**Set extension policy:**

```sh
curl -X PUT "$PUBLISHER/v1/blob-managers/$MANAGER_ID/extension-policy" \
  -H "Content-Type: application/json" \
  -d '{"type": "constrained", "expiryThresholdEpochs": 5, "maxExtensionEpochs": 10}'
```

**Deposit WAL to coin stash:**

```sh
curl -X POST "$PUBLISHER/v1/blob-managers/$MANAGER_ID/deposit-wal?amount=1000000000"
```

## Comparison with regular blobs

| Feature | Regular Blob | BlobManager |
|---------|--------------|-------------|
| Storage | Per-blob resource | Shared pool |
| Expiration | Per-blob | All blobs share same epoch |
| Extension | Owner only | Anyone (with policy) |
| Deletion | Owner only (if deletable) | Capability holders |
| Community funding | No | Yes (coin stash) |
| Tipped extensions | No | Yes |

**Use BlobManager when**: You have many related blobs, want community-funded storage, or need
shared access control.

**Use regular blobs when**: You have independent blobs with different lifetimes, or want simple
per-blob ownership.

## Future work

### Sponsored transactions for public write APIs

The current design requires a **private publisher** with wallet access for write operations, as the
publisher must sign transactions on behalf of capability holders. This limits deployment options
and requires trust between writers and publisher operators.

A future enhancement could leverage Sui's **sponsored transactions** to enable public write APIs:

1. **Writer prepares transaction**: The capability owner constructs and signs a transaction
   (e.g., store blob) but doesn't provide gas.
2. **Publisher sponsors gas**: The publisher adds a gas coin and co-signs as the sponsor.
3. **Dual-signed execution**: Sui executes the transaction with both signatures, where the writer
   authorizes the operation and the publisher pays for gas.

This would enable:
- **Public write APIs**: Publishers could expose write endpoints without wallet risk
- **Writer autonomy**: Only capability owners can authorize writes to their BlobManager
- **Flexible fee models**: Publishers could charge fees, require deposits, or subsidize operations

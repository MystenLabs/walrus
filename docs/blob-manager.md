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

The BlobManager owns a UnifiedStorage instance which manages the actual storage pool:

- **UnifiedStorage** (in Walrus package): Core storage abstraction with `key, store` abilities
- **BlobManager** (in separate package): Owns UnifiedStorage, enforces policies
- **BlobV2**: References UnifiedStorage via `storage_id` field

```move
/// The stable interface object.
public struct BlobManager has key, store {
    id: UID,
    /// Version number (currently 1) used to access the inner implementation.
    version: u64,
}

/// The implementation stored as a dynamic field of BlobManager.
public struct BlobManagerInnerV1 has store {
    /// Unified storage pool (from Walrus package).
    storage: UnifiedStorage,
    /// Coin stash for community funding (WAL for storage, tips).
    coin_stash: BlobManagerCoinStash,
    /// Combined policy controlling both capacity purchases and time extensions.
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

The BlobManager implements a **unified storage purchase policy** that controls both capacity
purchases and time extensions. This policy prevents abuse and ensures fair access to storage resources.
The policy module is part of the BlobManager package (not the Walrus package), giving BlobManager
full control over its storage management policies.

**Capacity Policy Types:**

1. **Unlimited**: No restrictions on storage purchases (not recommended for production)
2. **Constrained**: Only allows purchases when available storage is below a threshold with a maximum purchase limit

**Default policy:**

The BlobManager defaults to a constrained capacity policy with a 15GB threshold:
- Storage can only be purchased when available storage < 15GB
- Maximum purchase amount is capped at 15GB per transaction
- Extensions allowed within 2 epochs of expiry, max 5 epochs per extension
- Base tip of 10 WAL with 2x multiplier in last epoch

```move
/// Default configuration in BlobManager
storage_purchase_policy: storage_purchase_policy::default(),
// Includes:
// - Capacity: constrained with 15GB threshold and max
// - Extensions: allowed within 2 epochs, max 5 epochs
// - Tip: 10 WAL base with 2x last epoch multiplier
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

Extension policies (part of the unified storage purchase policy) control how and when community
members can extend the BlobManager's storage lifetime. The policy is configured with four parameters:

- `expiry_threshold_epochs`: Extensions only allowed when current epoch is within this many epochs
  of the storage end epoch
- `max_extension_epochs`: Maximum epochs that can be added in a single extension. Set to 0 to
  disable extensions entirely.
- `tip_amount`: Base tip amount in FROST (WAL) for community extenders
- `last_epoch_multiplier`: Multiplier applied to tip in the last epoch before expiry (e.g., 2 = 2x)

### Setting extension parameters

```rust
// Allow extensions when within 5 epochs of expiry, max 10 epochs per extension,
// with a base tip of 10 WAL and 2x multiplier in last epoch
blob_manager_client
    .set_extension_params(5, 10, 10_000_000_000, 2)  // 10 WAL in FROST, 2x last epoch
    .await?;

// Flat tip (no multiplier): 20 WAL always
blob_manager_client
    .set_extension_params(5, 10, 20_000_000_000, 1)  // 20 WAL, no multiplier (1x)
    .await?;

// Disable extensions by setting max_extension_epochs to 0
blob_manager_client
    .set_extension_params(0, 0, 0, 1)
    .await?;
```

When a community member calls `extend_blob_manager_storage()`, they:
1. Pay the gas fee of the transaction
2. Pay nothing for storage (WAL comes from the coin stash)
3. Receive the configured WAL tip

This creates a **keeper incentive**: community members can monitor BlobManagers approaching expiry
and extend them to earn tips.

## Tip Policy

The BlobManager uses a simple tip policy to incentivize community members to extend storage. Tips are paid in WAL from the coin stash.

### How It Works

The tip policy has two components:
- **Base tip**: A fixed amount in WAL paid for extensions
- **Last-epoch multiplier**: Increases the tip when extending in the last epoch before expiry (default: 2x)

### Configuration

```rust
// Default policy: 10 WAL base tip with 2x multiplier in last epoch
blob_manager_client.set_extension_params(
    2,                  // expiry_threshold_epochs
    5,                  // max_extension_epochs
    10_000_000_000,     // base_tip: 10 WAL in FROST
    2,                  // last_epoch_multiplier: 2x
).await?;

// Custom multiplier: 5 WAL base with 3x in last epoch
blob_manager_client.set_extension_params(
    2,                  // expiry_threshold_epochs
    5,                  // max_extension_epochs
    5_000_000_000,      // base_tip: 5 WAL in FROST
    3,                  // last_epoch_multiplier: 3x
).await?;

// Flat tip (no multiplier): 20 WAL always
blob_manager_client.set_extension_params(
    2,                  // expiry_threshold_epochs
    5,                  // max_extension_epochs
    20_000_000_000,     // base_tip: 20 WAL in FROST
    1,                  // last_epoch_multiplier: 1x (no multiplier)
).await?;
```

### Examples

| Policy | Normal Epochs | Last Epoch Before Expiry |
|--------|---------------|--------------------------|
| Default (10 WAL, 2x) | 10 WAL | 20 WAL |
| Custom (5 WAL, 3x) | 5 WAL | 15 WAL |
| Flat (20 WAL, 1x) | 20 WAL | 20 WAL |

This design provides predictable costs while incentivizing timely extensions when storage is about to expire.

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
- Bypasses extension policy (works even when extensions are disabled)
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

### Certifying blobs

Anyone can certify a registered blob—no capability required. Since the blob must already be
registered (which requires a capability), certification simply confirms that the blob data was
successfully stored with cryptographic proof. This allows flexible publisher architectures where
less-trusted parties can complete the certification step:

```rust
// Anyone can certify a registered blob
client.certify_managed_blob(manager_id, blob_id, deletable, &certificate).await?;
```

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
| `set_extension_policy(threshold, max, tip)` | Set extension policy (0 for max disables extensions) |
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
| `certify_blob(blob_id, certificate)` | Certify a registered blob |
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
| `PUT` | `/v1/blob-managers/{manager_id}/extension-policy` | Set extension policy (includes tip) |
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
| `POST` | `/v1/blob-managers/{manager_id}/certify/{blob_id}` | Certify a registered blob |
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
    "maxExtensionEpochs": 10,
    "tipAmount": 10000000
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
  -d '{"type": "constrained", "expiryThresholdEpochs": 5, "maxExtensionEpochs": 10, "tipAmount": 100000000}'
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
| Attributes| Access by blob object ID | Accessed by blob ID |

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

> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

The smart contract (`memwal::account`) defines the onchain account model for Walrus Memory. It is a Move module deployed on Sui.

## Network IDs

These are the onchain IDs for the current public Walrus Memory deployments:

### Staging (testnet)

```env
SUI_NETWORK=testnet
MEMWAL_PACKAGE_ID=0xcf6ad755a1cdff7217865c796778fabe5aa399cb0cf2eba986f4b582047229c6
MEMWAL_REGISTRY_ID=0xe80f2feec1c139616a86c9f71210152e2a7ca552b20841f2e192f99f75864437
```

### Production (mainnet)

```env
SUI_NETWORK=mainnet
MEMWAL_PACKAGE_ID=0xcee7a6fd8de52ce645c38332bde23d4a30fd9426bc4681409733dd50958a24c6
MEMWAL_REGISTRY_ID=0x0da982cefa26864ae834a8a0504b904233d49e20fcc17c373c8bed99c75a7edd
```

For relayer setup and environment variable usage, see [Self-Hosting](/walrus-memory/relayer/self-hosting) and [Environment Variables](/walrus-memory/reference/environment-variables).

## What it manages

- **Ownership**, who owns a Walrus Memory account
- **Delegate keys**, which Ed25519 keys are authorized to act through the relayer
- **SEAL access control**, who can decrypt encrypted memories through `seal_approve`
- **Account lifecycle**, activation and deactivation (freeze/unfreeze)

The contract does not store memory content, it only manages identity, permissions, and access control.

## Key objects

### `AccountRegistry`

A shared object created at module publish time. It tracks all MemWalAccount objects and prevents duplicate account creation (one account per Sui address).

### `MemWalAccount`

A shared object representing a single user's account. It stores:

| Field | Type | Description |
|-------|------|-------------|
| `owner` | `address` | The Sui wallet address that owns this account |
| `delegate_keys` | `vector<DelegateKey>` | List of authorized Ed25519 delegate keys |
| `created_at` | `u64` | Timestamp when the account was created (epoch ms) |
| `active` | `bool` | Whether the account is active (false = frozen) |

### `DelegateKey`

A struct stored inside `MemWalAccount.delegate_keys`:

| Field | Type | Description |
|-------|------|-------------|
| `public_key` | `vector<u8>` | Ed25519 public key (32 bytes) |
| `sui_address` | `address` | Sui address derived from this Ed25519 key |
| `label` | `String` | Human-readable label (for example, , "MacBook Pro") |
| `created_at` | `u64` | Timestamp when the key was added (epoch ms) |

## Limits

- **Maximum delegate keys per account**: 20

## Error codes

| Code | Name | Description |
|------|------|-------------|
| 0 | `EDelegateKeyAlreadyExists` | Key already registered in this account |
| 1 | `EDelegateKeyNotFound` | Key not found when trying to remove |
| 2 | `ETooManyDelegateKeys` | Account has reached the 20-key limit |
| 3 | `EAccountAlreadyExists` | Address already has an account |
| 4 | `ENotOwner` | Caller is not the account owner |
| 5 | `EInvalidPublicKeyLength` | Public key is not exactly 32 bytes |
| 6 | `EAccountDeactivated` | Account is frozen, operation denied |
| 100 | `ENoAccess` | SEAL access denied, caller is neither owner nor delegate |

## Entry functions

| Function | Description |
|----------|-------------|
| `create_account(registry, clock)` | Create a new MemWalAccount (one per address) |
| `add_delegate_key(account, public_key, sui_address, label, clock)` | Add a delegate key (owner only) |
| `remove_delegate_key(account, public_key)` | Remove a delegate key (owner only) |
| `deactivate_account(account)` | Freeze the account, SEAL access denied, keys locked (owner only) |
| `reactivate_account(account)` | Unfreeze the account (owner only) |
| `seal_approve(id, account)` | SEAL policy, authorizes owner or delegate key holder to decrypt |

## View functions

| Function | Description |
|----------|-------------|
| `is_delegate(account, public_key)` | Check if a public key is an authorized delegate |
| `is_delegate_address(account, addr)` | Check if a Sui address is an authorized delegate |
| `owner(account)` | Get the owner address |
| `delegate_count(account)` | Get the number of delegate keys |
| `has_account(registry, addr)` | Check if an address already has an account |
| `is_active(account)` | Check if the account is active |

## Events

| Event | Emitted when |
|-------|-------------|
| `AccountCreated` | A new account is created |
| `DelegateKeyAdded` | A delegate key is added to an account |
| `DelegateKeyRemoved` | A delegate key is removed from an account |
| `AccountDeactivated` | An account is frozen |
| `AccountReactivated` | A frozen account is unfrozen |
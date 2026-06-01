> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

## Owner

The owner is the Sui wallet address that created the `MemWalAccount`. The owner has full control:

- Add and remove delegate keys
- Deactivate (freeze) and reactivate the account
- Decrypt any memory encrypted under their address through Seal

Each Sui address can only create **one** MemWalAccount (enforced by the `AccountRegistry`).

## Delegate

A delegate key authenticates API calls through the relayer. Delegates can:

- Store memories (`remember`, `analyze`)
- Recall memories (`recall`)
- Restore namespaces (`restore`)
- Decrypt Seal-encrypted content (through `seal_approve`)

Delegates **cannot**:

- Add or remove other delegate keys
- Deactivate or reactivate the account
- Transfer ownership

## Seal Access control

The contract's `seal_approve` function is the Seal policy that controls who can decrypt memories. Access is granted if the caller is:

1. **The data owner**, the key ID ends with the BCS-encoded owner address and the caller is the account owner
2. **A registered delegate**, the caller's Sui address is in the account's `delegate_keys` list

The account must also be **active** (not frozen). If the account is deactivated, all Seal access is denied.

## Permission boundary

These are separate layers that work together:

| Layer | Controls | Enforced by |
|-------|----------|-------------|
| **Owner** | Account control, keys, activation, ownership | Sui smart contract |
| **Delegate** | Application access, read/write memory | Sui smart contract + relayer verification |
| **Relayer** | Backend execution, encryption, storage, search | Server-side auth middleware |

The relayer verifies every request against the onchain contract before executing any operation. Even if the relayer is compromised, it cannot forge delegate permissions or change ownership, those are cryptographically enforced onchain.
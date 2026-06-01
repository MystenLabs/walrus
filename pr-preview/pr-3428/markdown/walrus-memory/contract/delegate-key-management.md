> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

Delegate keys are lightweight Ed25519 keys used for SDK authentication. They are registered onchain in a `MemWalAccount` and verified by the relayer on every request.

## Why they exist

- Apps need a usable key for API calls without exposing the owner wallet
- Users should not hand over the owner wallet for day-to-day memory access
- Different apps or devices can each have their own delegate key with a descriptive label

### 1. Generate a delegate keypair

Use the SDK's `generateDelegateKey()` helper to create a new Ed25519 keypair:

```ts
import { generateDelegateKey } from "@mysten-incubation/memwal/account";

const delegate = await generateDelegateKey();
// delegate.privateKey — hex string, store securely
// delegate.publicKey — 32-byte Uint8Array
// delegate.suiAddress — derived Sui address (0x...)
```

### 2. Register the public key onchain

Only the account owner can add delegate keys:

```ts
import { addDelegateKey } from "@mysten-incubation/memwal/account";

await addDelegateKey({
  packageId: "0x...",
  accountId: "0x...",
  publicKey: delegate.publicKey,
  label: "MacBook Pro",
  suiPrivateKey: "suiprivkey1...", // or walletSigner
});
```

### 3. Use the private key in the SDK

```ts
import { MemWal } from "@mysten-incubation/memwal";

const memwal = MemWal.create({
  key: delegate.privateKey,
  accountId: "0x...",
});
```

### 4. Revoke the delegate key

Removing a delegate key prevents future relayer access from that key:

```ts
import { removeDelegateKey } from "@mysten-incubation/memwal/account";

await removeDelegateKey({
  packageId: "0x...",
  accountId: "0x...",
  publicKey: delegate.publicKey,
  suiPrivateKey: "suiprivkey1...", // or walletSigner
});
```

## Limits

- Each account supports up to **20 delegate keys**
- Each delegate key must be a valid 32-byte Ed25519 public key
- Duplicate keys are rejected (error code 0)
- Only the account owner can add or remove delegate keys

## Account deactivation

An account owner can deactivate (freeze) their account. When deactivated:

- SEAL decryption access is denied for all keys (owner and delegates)
- Delegate keys cannot be added or removed
- The owner can reactivate the account at any time

This is useful as an emergency kill switch if a key is compromised.
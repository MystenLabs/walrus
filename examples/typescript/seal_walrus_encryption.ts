// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/**
 * End-to-end encryption with Seal and Walrus on Testnet.
 *
 * This example shows the full "encrypt before you store" loop:
 *
 *   1. Encrypt plaintext with Seal under an onchain access policy.
 *   2. Store the resulting ciphertext as a blob on Walrus.
 *   3. Read the ciphertext blob back from Walrus.
 *   4. Decrypt it with Seal after the key servers verify the policy.
 *
 * Walrus never sees the plaintext: only the Seal ciphertext is stored, and
 * only addresses that satisfy the onchain `seal_approve` policy can decrypt it.
 *
 * Prerequisites:
 *   - A Move package that defines a Seal access policy with a `seal_approve`
 *     function. This example uses the allowlist pattern from
 *     https://github.com/MystenLabs/seal/tree/main/move/patterns/sources/whitelist.move
 *   - The package ID, the shared allowlist (policy) object ID, and an address
 *     that is on the allowlist.
 *   - A funded Testnet keypair (SUI for gas, WAL for storage).
 *
 * Run with:
 *   pnpm dlx tsx seal_walrus_encryption.ts
 */

import { SealClient, SessionKey } from '@mysten/seal';
import { SuiClient, getFullnodeUrl } from '@mysten/sui/client';
import { Ed25519Keypair } from '@mysten/sui/keypairs/ed25519';
import { Transaction } from '@mysten/sui/transactions';
import { fromHex, toHex } from '@mysten/sui/utils';
import { WalrusClient } from '@mysten/walrus';

// docs::#config
// Replace these with the values from your deployed Seal policy package.
const PACKAGE_ID = '0x...'; // package that defines `seal_approve`
const ALLOWLIST_ID = '0x...'; // shared allowlist (policy) object
const MODULE_NAME = 'whitelist';

const suiClient = new SuiClient({ url: getFullnodeUrl('testnet') });

const walrusClient = new WalrusClient({
  network: 'testnet',
  suiClient,
});

// Load a funded Testnet keypair. The address must be on the allowlist above so
// that the `seal_approve` policy grants it decryption access.
const keypair = Ed25519Keypair.fromSecretKey(process.env.SUI_SECRET_KEY!);
const address = keypair.toSuiAddress();

// Pick two verified Testnet key servers and require a 2-of-2 threshold. Reusing
// a single SealClient instance lets it cache key-server objects and keys. For
// the current verified server object IDs, see
// https://seal-docs.wal.app/Pricing#verified-key-servers
const sealClient = new SealClient({
  suiClient,
  serverConfigs: [
    { objectId: '0x73d05d62c18d9374e3ea529e8e0ed6161da1a141a94d3f76ae3fe4e99356db75', weight: 1 },
    { objectId: '0xf5d14a81a982144ae441cd7d64b09027f116a468bd36e7eca494f750591623c8', weight: 1 },
  ],
  verifyKeyServers: false,
});
// docs::/#config

// docs::#encrypt
async function encryptAndStore(plaintext: Uint8Array): Promise<string> {
  // The identity is [allowlist object ID][random nonce]. The allowlist prefix
  // ties the ciphertext to your onchain policy; the nonce keeps each blob unique.
  const nonce = crypto.getRandomValues(new Uint8Array(5));
  const id = toHex(new Uint8Array([...fromHex(ALLOWLIST_ID), ...nonce]));

  const { encryptedObject } = await sealClient.encrypt({
    threshold: 2,
    packageId: PACKAGE_ID,
    id,
    data: plaintext,
  });

  // Store the Seal ciphertext on Walrus. Walrus only ever sees encrypted bytes.
  const { blobId } = await walrusClient.writeBlob({
    blob: encryptedObject,
    deletable: false,
    epochs: 3,
    signer: keypair,
  });

  console.log('stored encrypted blob:', blobId);
  return blobId;
}
// docs::/#encrypt

// docs::#decrypt
async function readAndDecrypt(blobId: string): Promise<Uint8Array> {
  // Read the ciphertext back from Walrus.
  const encryptedBytes = await walrusClient.readBlob({ blobId });

  // Create a session key. The user signs a personal message once to grant the
  // app time-limited access to decryption keys for this package.
  const sessionKey = await SessionKey.create({
    address,
    packageId: PACKAGE_ID,
    ttlMin: 10,
    suiClient,
  });
  const { signature } = await keypair.signPersonalMessage(sessionKey.getPersonalMessage());
  sessionKey.setPersonalMessageSignature(signature);

  // Build a PTB that calls the policy's `seal_approve` function. The key servers
  // dry-run this transaction to confirm the caller is allowed to decrypt.
  const tx = new Transaction();
  tx.moveCall({
    target: `${PACKAGE_ID}::${MODULE_NAME}::seal_approve`,
    arguments: [
      tx.pure.vector('u8', fromHex(ALLOWLIST_ID)),
      tx.object(ALLOWLIST_ID),
    ],
  });
  // The sender must match the address that holds decryption access, otherwise
  // the policy check fails with "Transaction was not signed by the correct sender".
  tx.setSender(address);
  const txBytes = await tx.build({ client: suiClient, onlyTransactionKind: true });

  const decrypted = await sealClient.decrypt({
    data: new Uint8Array(encryptedBytes),
    sessionKey,
    txBytes,
  });

  return decrypted;
}
// docs::/#decrypt

async function main() {
  const message = new TextEncoder().encode('private data for the allowlist only');

  const blobId = await encryptAndStore(message);
  const recovered = await readAndDecrypt(blobId);

  console.log('decrypted:', new TextDecoder().decode(recovered));
}

main().catch(console.error);

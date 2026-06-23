// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

// Step 1 of the tutorial: encrypt a message with Seal, then store the
// ciphertext on Walrus. Run with:
//   SUI_PRIVATE_KEY=suiprivkey1... npx tsx encrypt-and-store.ts

import { fromHex, toHex } from '@mysten/sui/utils';

import { ALLOWLIST_ID, PACKAGE_ID, THRESHOLD, sealClient, suiClient } from './config';
import { getFundedKeypair } from './funded-keypair';

async function encryptAndStore() {
  const keypair = await getFundedKeypair();

  const message = new TextEncoder().encode('This message is encrypted end-to-end with Seal.');

  // The Seal identity must start with the policy's namespace so that the
  // `allowlist::seal_approve` prefix check passes. For the allowlist pattern the
  // namespace is the allowlist object's ID; we append a random nonce so each
  // encryption gets a distinct identity under the same policy.
  const nonce = crypto.getRandomValues(new Uint8Array(5));
  const id = toHex(new Uint8Array([...fromHex(ALLOWLIST_ID), ...nonce]));

  // `encrypt` returns the encrypted object (store this) and a symmetric backup
  // key (optional disaster-recovery handle that we ignore here).
  const { encryptedObject } = await sealClient.encrypt({
    threshold: THRESHOLD,
    packageId: PACKAGE_ID,
    id,
    data: message,
  });

  // Store the ciphertext on Walrus. Only the encrypted bytes ever touch Walrus.
  const { blobId } = await suiClient.walrus.writeBlob({
    blob: encryptedObject,
    deletable: true,
    epochs: 3,
    signer: keypair,
  });

  console.log('Stored encrypted blob:', blobId);
  console.log('Seal identity:', id);
  return { blobId };
}

encryptAndStore().catch((error) => {
  console.error(error);
  process.exit(1);
});

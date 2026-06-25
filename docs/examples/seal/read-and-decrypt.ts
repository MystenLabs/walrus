// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

// Step 2 of the tutorial: read the ciphertext back from Walrus and decrypt it
// with Seal. The caller must satisfy the onchain policy (be on the allowlist).
// Run with:
//   SUI_PRIVATE_KEY=suiprivkey1... npx tsx read-and-decrypt.ts <blobId>

import { EncryptedObject, SessionKey } from '@mysten/seal';
import { Transaction } from '@mysten/sui/transactions';
import { fromHex } from '@mysten/sui/utils';

import {
  ALLOWLIST_ID,
  MODULE_NAME,
  PACKAGE_ID,
  THRESHOLD,
  sealClient,
  suiClient,
} from './config';
import { getFundedKeypair } from './funded-keypair';

// Builds the PTB that Seal evaluates as the access check. It must call only
// `seal_approve*` functions on PACKAGE_ID. The transaction's sender is set to
// the requester, because `allowlist::seal_approve` checks `ctx.sender()`.
function buildApproveTx(id: string, sender: string): Transaction {
  const tx = new Transaction();
  // Set the sender before building, or decryption fails with
  // "Transaction was not signed by the correct sender".
  tx.setSender(sender);
  tx.moveCall({
    target: `${PACKAGE_ID}::${MODULE_NAME}::seal_approve`,
    arguments: [tx.pure.vector('u8', fromHex(id)), tx.object(ALLOWLIST_ID)],
  });
  return tx;
}

async function readAndDecrypt(blobId: string) {
  const keypair = await getFundedKeypair();
  const address = keypair.toSuiAddress();

  // 1. Read the ciphertext back from Walrus.
  const encryptedBytes = await suiClient.walrus.readBlob({ blobId });

  // 2. Recover the Seal identity that was embedded at encryption time.
  const id = EncryptedObject.parse(encryptedBytes).id;

  // 3. Create a session key and authorize it once with a personal-message
  //    signature. In a browser this is a wallet pop-up; here we sign locally.
  const sessionKey = await SessionKey.create({
    address,
    packageId: PACKAGE_ID,
    ttlMin: 10,
    suiClient,
  });
  const { signature } = await keypair.signPersonalMessage(sessionKey.getPersonalMessage());
  await sessionKey.setPersonalMessageSignature(signature);

  // 4. Build the access-check PTB and fetch decryption shares from key servers.
  const tx = buildApproveTx(id, address);
  const txBytes = await tx.build({ client: suiClient, onlyTransactionKind: true });
  await sealClient.fetchKeys({ ids: [id], txBytes, sessionKey, threshold: THRESHOLD });

  // 5. Decrypt locally using the fetched shares.
  const decrypted = await sealClient.decrypt({ data: encryptedBytes, sessionKey, txBytes });

  console.log('Decrypted message:', new TextDecoder().decode(decrypted));
  return decrypted;
}

const blobId = process.argv[2];
if (!blobId) {
  console.error('Usage: npx tsx read-and-decrypt.ts <blobId>');
  process.exit(1);
}

readAndDecrypt(blobId).catch((error) => {
  console.error(error);
  process.exit(1);
});

// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

// Returns a keypair that has SUI (for gas) and WAL (to pay for storage) on
// Testnet. In a browser app you would instead use a wallet via @mysten/dapp-kit;
// here we sign with a local keypair so the example can run end-to-end in Node.

import { getFaucetHost, requestSuiFromFaucetV2 } from '@mysten/sui/faucet';
import { Ed25519Keypair } from '@mysten/sui/keypairs/ed25519';
import { MIST_PER_SUI } from '@mysten/sui/utils';

import { NETWORK, suiClient } from './config';

export async function getFundedKeypair(): Promise<Ed25519Keypair> {
  // Load your key from the environment. Generate one with:
  //   sui client new-address ed25519
  // then export it as SUI_PRIVATE_KEY (a `suiprivkey1...` string).
  const secret = process.env.SUI_PRIVATE_KEY;
  if (!secret) {
    throw new Error('Set SUI_PRIVATE_KEY to a testnet keypair secret (suiprivkey1...).');
  }
  const keypair = Ed25519Keypair.fromSecretKey(secret);
  const address = keypair.toSuiAddress();

  // Top up SUI from the faucet if the balance is low.
  const { totalBalance } = await suiClient.getBalance({ owner: address });
  if (BigInt(totalBalance) < MIST_PER_SUI) {
    await requestSuiFromFaucetV2({
      host: getFaucetHost(NETWORK),
      recipient: address,
    });
  }

  // You also need WAL to pay for storage. Acquire it with the Walrus CLI
  // (`walrus get-wal`) or by swapping SUI for WAL; see the Walrus docs.
  return keypair;
}

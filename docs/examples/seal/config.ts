// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

// Shared configuration for the end-to-end Seal + Walrus example.
//
// These IDs come from the access-control policy you deploy yourself. The
// example uses the `allowlist` pattern from the Seal repository; deploy it and
// fill in the two values below before running the scripts. See the tutorial
// (Encrypting data with Seal) for the deployment steps and links.

import { getJsonRpcFullnodeUrl, SuiJsonRpcClient } from '@mysten/sui/jsonRpc';
import { SealClient } from '@mysten/seal';
import { walrus } from '@mysten/walrus';

// The network all three services run on. Seal key servers are network-specific:
// a Testnet key server will not serve keys for a Mainnet package and vice versa.
export const NETWORK = 'testnet' as const;

// The Move package that contains your `seal_approve` function (here, the
// `allowlist` module). Replace with the package ID printed when you publish.
export const PACKAGE_ID = '0xYOUR_PACKAGE_ID';

// The shared policy object that gates access (the `Allowlist` object). Replace
// with the object ID created when you call `create_allowlist_entry`.
export const ALLOWLIST_ID = '0xYOUR_ALLOWLIST_OBJECT_ID';

// The Move module inside PACKAGE_ID that defines `seal_approve`.
export const MODULE_NAME = 'allowlist';

// One Sui client, extended with the Walrus SDK, reused everywhere. The same
// client instance is passed to Seal and to SessionKey so that policy checks and
// blob reads/writes all target the same network.
export const suiClient = new SuiJsonRpcClient({
  network: NETWORK,
  url: getJsonRpcFullnodeUrl(NETWORK),
}).$extend(walrus());

// A Seal client configured against a single Testnet key server. For threshold
// configurations across multiple servers and for current key-server object IDs,
// see https://seal-docs.wal.app/UsingSeal#configure-a-seal-client
// (the SDK's removed `getAllowlistedKeyServers` helper is no longer used;
// list the key-server object IDs explicitly instead).
export const sealClient = new SealClient({
  suiClient,
  serverConfigs: [
    {
      objectId: '0xb35a7228d8cf224ad1e828c0217c95a5153bafc2906d6f9c178197dce26fbcf8',
      weight: 1,
    },
  ],
  verifyKeyServers: false,
});

// Threshold = how many key servers must each return a share to decrypt. With a
// single configured server this must be 1.
export const THRESHOLD = 1;

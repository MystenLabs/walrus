// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

// Store a blob on Walrus and read it back with plain HTTP calls.
//
// A publisher accepts uploads through PUT requests and an aggregator serves
// downloads through GET requests, so the built-in fetch API is all you need.
// The endpoints below are the public Testnet ones.

const PUBLISHER = "https://publisher.walrus-testnet.walrus.space";
const AGGREGATOR = "https://aggregator.walrus-testnet.walrus.space";

// Send the raw bytes as the body of a PUT request. `data` accepts a File,
// Blob, ArrayBuffer, Uint8Array, or string.
export async function storeBlob(data, epochs = 1) {
  const response = await fetch(`${PUBLISHER}/v1/blobs?epochs=${epochs}`, {
    method: "PUT",
    body: data,
  });
  if (!response.ok) {
    throw new Error(`Store failed with status ${response.status}`);
  }
  return response.json();
}

// A successful store returns one of two shapes. `newlyCreated` describes a
// blob Walrus stored for the first time; `alreadyCertified` describes one
// another user already stored and certified. They nest the blob ID
// differently, so handle both.
export function parseStoreResponse(info) {
  if ("alreadyCertified" in info) {
    return {
      status: "Already certified",
      blobId: info.alreadyCertified.blobId,
      endEpoch: info.alreadyCertified.endEpoch,
    };
  }
  if ("newlyCreated" in info) {
    return {
      status: "Newly created",
      blobId: info.newlyCreated.blobObject.blobId,
      endEpoch: info.newlyCreated.blobObject.storage.endEpoch,
      suiObjectId: info.newlyCreated.blobObject.id,
    };
  }
  throw new Error("Unexpected store response");
}

// Read the blob back by blob ID. Call response.text() or response.json()
// instead when you stored text or JSON.
export async function readBlob(blobId) {
  const response = await fetch(`${AGGREGATOR}/v1/blobs/${blobId}`);
  if (!response.ok) {
    throw new Error(`Read failed with status ${response.status}`);
  }
  return response.arrayBuffer();
}

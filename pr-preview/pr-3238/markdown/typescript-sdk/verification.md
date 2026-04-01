The Walrus TypeScript SDK includes mechanisms to verify the integrity of uploaded and downloaded data. These checks run automatically during uploads and downloads, but you can also perform them manually.

During upload, the TypeScript SDK validates content during the encoding process by checking that the blob size is within limits and verifying the encoding process completes successfully. It also ensures that the metadata matches the encoded data.

During download, the SDK validates content during reconstruction by verifying each sliver's integrity, ensuring the reconstructed blob matches the root hash. It also verifies the reconstructed blob size matches the metadata.

When you route uploads through an upload relay, the SDK performs the same integrity steps it uses for direct-to-node uploads.

The following example shows how to perform an additional hash comparison after download:

```typescript
const download = await client.walrus.readBlob({ blobId });
const reconstructedHash = await crypto.subtle.digest('SHA-256', download);

if (!Buffer.from(reconstructedHash).equals(Buffer.from(expectedHash))) {
  throw new Error('Blob content mismatch');
}
```

You only need to compare the bytes (or hash) against your expectation because `readBlob` uses `decodePrimarySlivers` and the metadata hashes. 

You can also verify blob metadata using the metadata's hashes for verification:

```typescript
interface BlobMetadata {
  encodingType: 'RS2';
  hashes: Array<{
    primaryHash: string;
    secondaryHash: string;
  }>;
  unencodedLength: number;
}
```

## Blob ID validation

The blob ID is derived from the blob's metadata and serves as a unique identifier. The same data produces the same blob ID, different data produces different blob IDs therefore you can verify the blob's data based on recomputing the blob ID from the blob's metadata.

The following example computes metadata and extracts the blob ID:

```ts
const { blobId, metadata } = await client.walrus.computeBlobMetadata({
  bytes: blob,
});

console.log('Blob ID', blobId);
console.log('Encoding type', metadata.encodingType); // Relay enforces RS2 today.
```

## Root hash verification

Every blob has a root hash that represents the Merkle tree root of the encoded data. The TypeScript SDK computes this hash during encoding, stores it onchain in the blob registration transaction, then verifies it during download.

The following example shows how to compute the root hash and use it when sending an upload relay tip:

```ts
const { rootHash, blobDigest, nonce } = await client.walrus.computeBlobMetadata({
  bytes: blob,
});

console.log('Root hash', Buffer.from(rootHash).toString('hex'));
await client.walrus.sendUploadRelayTip({
  size: blob.length,
  blobDigest: await blobDigest(),
  nonce,
});
```

Any corruption in the data changes the hash. The hash can be verified without downloading the entire blob.

## Certificate verification

Whether you upload directly or through an upload relay, you ultimately rely on the same certificate to prove a quorum of storage nodes persisted the blob. The relay generates the certificate server-side, while the direct path has the SDK gather confirmations and build the certificate locally.

The certificate contains the following fields:

```typescript
interface ProtocolMessageCertificate {
  signers: number[];              // Indices of nodes that signed
  serializedMessage: Uint8Array;  // The message that was signed
  signature: Uint8Array;          // Aggregated BLS signature
}
```

### Building a certificate from confirmations

After `writeBlob` gathers per-node confirmations, call this helper to aggregate them:

```ts
const confirmations = await client.walrus.writeEncodedBlobToNodes({ ... });
const certificate = await client.walrus.certificateFromConfirmations({
  confirmations,
  blobId,
  blobObjectId,
  deletable: true,
});
```

Once you have a certificate (regardless of upload path), it is verified twice onchain during the certification transaction: first by storage nodes before they confirm storage, then by clients that can verify the certificate signature.

### Using a relay-provided certificate

The following example shows how to receive a certificate from an upload relay and submit it for onchain certification:

```ts
const { certificate, blobId } = await client.walrus.writeBlobToUploadRelay({
  blob,
  blobId: metadata.blobId,
  nonce: metadata.nonce,
  txDigest: registerResult.digest,
  deletable: true,
  blobObjectId: registerResult.blob.id.id,
});

await client.walrus.executeCertifyBlobTransaction({
  blobId,
  blobObjectId: registerResult.blob.id.id,
  certificate,
  signer,
  deletable: true,
});
```

If you bypass the relay, use `client.walrus.certificateFromConfirmations` to aggregate node confirmations yourself. It wraps the same BLS helpers the relay uses internally.

## Signature verification

Storage node confirmations include BLS signatures that you can verify. The `certificateFromConfirmations` helper re-verifies each node's signature against the committee's public keys and fails if you do not have a quorum.

```ts
const certificate = await client.walrus.certificateFromConfirmations({
  confirmations,
  blobId,
  blobObjectId,
  deletable: true,
});
```

The TypeScript SDK verifies that each node's signature is valid, the signed message matches the blob metadata, and a quorum of valid signatures exists.

## Manual integrity verification

You can manually verify blob integrity by comparing downloaded data against expected data:

<summary>Manual blob verification function</summary>

```typescript
async function verifyBlobIntegrity(blobId: string, expectedData: Uint8Array) {
  // Download the blob
  const downloadedData = await client.walrus.readBlob({ blobId });
  
  // Compare sizes
  if (downloadedData.length !== expectedData.length) {
    throw new Error('Blob size mismatch');
  }
  
  // Compare content (for small blobs)
  if (downloadedData.length < 1024 * 1024) { // 1MB
    const match = downloadedData.every((byte, i) => byte === expectedData[i]);
    if (!match) {
      throw new Error('Blob content mismatch');
    }
  } else {
    // For larger blobs, compute and compare hashes
    const downloadedHash = await crypto.subtle.digest('SHA-256', downloadedData);
    const expectedHash = await crypto.subtle.digest('SHA-256', expectedData);
    
    const match = new Uint8Array(downloadedHash).every(
      (byte, i) => byte === new Uint8Array(expectedHash)[i]
    );
    
    if (!match) {
      throw new Error('Blob hash mismatch');
    }
  }
  
  return true;
}
```
// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

// @ts-check
import http from 'k6/http';

/** A Walrus blob ID, which is a base64 encoded string */
type BlobId = string;
/** A Walrus QuiltId which is blob-id followed by the filename. */
type QuiltId = `${BlobId}/${string}`;
/** A Walrus patch ID, which is a base64 encoded string */
type PatchId = string;

/**
 * Retrieves a blob via an aggregator.
 * @param aggregatorUrl - The URL of the aggregator to contact for the blob.
 * @param blobId - The blob ID of the blob to requests.
 * @param params - Any additional parameters to pass to the `http.get` call.
 */
export async function getBlob(
    aggregatorUrl: string,
    blobId: BlobId,
    params?: any,
) {
    return http.get(http.url`${aggregatorUrl}/v1/blobs/${blobId}`, params);
}

/**
 * Retrieves a quilt patch via an aggregator.
 * @param aggregatorUrl - The URL of the aggregator to contact for the blob.
 * @param quiltPatch - The quilt or patch ID to requests. They are differentiated by the presence
 * of a '/' in the ID.
 * @param params - Any additional parameters to pass to the `http.get` call.
 */
export async function getQuiltPatch(
    aggregatorUrl: string,
    quiltPatch: QuiltId | PatchId,
    params?: any,
) {
    const url = quiltPatch.includes('/')
        ? http.url`${aggregatorUrl}/v1/blobs/by-quilt-id/${quiltPatch}`
        : http.url`${aggregatorUrl}/v1/blobs/by-quilt-patch-id/${quiltPatch}`;

    return http.get(url, params);
}

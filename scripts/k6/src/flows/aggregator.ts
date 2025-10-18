// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

// @ts-check
import http from 'k6/http';

type BlobId = string;
type QuiltId = `${BlobId}/${string}`;
type PatchId = string;

export async function getBlob(
    aggregatorUrl: string,
    blobId: BlobId,
    timeout: string,
) {
    return http.get(http.url`${aggregatorUrl}/v1/blobs/${blobId}`, { timeout: timeout });
}

export async function getQuiltPatch(
    aggregatorUrl: string,
    quiltPatch: QuiltId | PatchId,
    timeout: string,
) {
    const url = quiltPatch.includes('/')
        ? http.url`${aggregatorUrl}/v1/blobs/by-quilt-id/${quiltPatch}`
        : http.url`${aggregatorUrl}/v1/blobs/by-quilt-patch-id/${quiltPatch}`;

    return http.get(url, { timeout: timeout });
}

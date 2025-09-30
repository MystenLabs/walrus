// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

// @ts-check
import http from 'k6/http';

export async function getBlob(
    aggregatorUrl: string,
    blobId: string,
    timeout: string,
) {
    return http.get(http.url`${aggregatorUrl}/v1/blobs/${blobId}`, { timeout: timeout });
}

// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0
//
// Helpers for warming up client-side state (e.g., TCP/TLS handshakes) in k6 scripts.
//
// Note: k6 executes each VU in its own JS runtime, so module-level state is per-VU.

import http from "k6/http";

let hasWarmedUp = false;

/**
 * Establishes a connection to the publisher once per VU so that subsequent requests can reuse
 * the TCP/TLS session (when keep-alives are supported).
 *
 * The request is tagged with `phase=warmup` so that thresholds can exclude it.
 */
export function warmupPublisherConnectionOncePerVu(publisherUrl: string) {
    if (hasWarmedUp) {
        return;
    }

    // Use an endpoint on the same origin as the real test traffic; status code is irrelevant for
    // warming up the connection.
    http.get(`${publisherUrl}/v1/blobs`, {
        timeout: "30s",
        tags: { phase: "warmup" },
    });

    hasWarmedUp = true;
}

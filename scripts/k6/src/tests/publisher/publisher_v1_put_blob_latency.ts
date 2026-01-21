// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0
//
// Measures the latency of putting file via a publisher on Walrus, by storing a
// fixed number of files of a given size.
//
// if WALRUS_K6_REDIS_URL is specified, then the stored blob IDs are written to a redis list with
// payload size as the key.
//
import { loadEnvironment } from '../../config/environment.ts'
import { putBlob } from '../../flows/publisher.ts'
import * as fs from 'k6/experimental/fs';
import { parseHumanFileSize, loadParameters, logObject, recordEndTime, recordStartTime }
    from "../../lib/utils.ts"
import { BlobHistory } from "../../lib/blob_history.ts"
import exec from 'k6/execution';
import { Trend } from "k6/metrics";
import { expect }
    // @ts-ignore
    from 'https://jslib.k6.io/k6-testing/0.5.0/index.js';


/**
 * Test parameters read from the environment in UPPER_CASE with a WALRUS_K6_ prefix.
 */
interface TestParameters {
    /** Number of blobs to store. */
    blobCount: number,
    /** The number of virtual users that will repeatedly store blobs. */
    maxConcurrency: number,
    /** The payload size of each request with a Ki or Mi or Gi suffix. */
    payloadSize: string,
    /** Number of warmup iterations to run before recording measurements. */
    warmupCount: number,
    /** Value in milliseconds that will be used to set a threshold on http request duration */
    httpDurationThreshold: string,
    /** The timeout for storing each blob. */
    timeout: string,
}

/** Parameters from __ENV with defaults. */
const params = loadParameters<TestParameters>({
    blobCount: 10,
    maxConcurrency: 3,
    payloadSize: "1Ki",
    warmupCount: 0,
    httpDurationThreshold: "",
    timeout: "15m",
});
/** Common environment setting like URLs, loaded from __ENV with defaults. */
const env = loadEnvironment();
/** File providing random bytes to be used for blob files. */
const dataFile = await fs.open(env.payloadSourceFile);
/** Handle for storing the IDs of blobs written to the network. */
const blobHistory = new BlobHistory(env.redisUrl);
const measuredReqDuration = new Trend('measured_req_duration', /*isTime=*/true);

export const options = {
    scenarios: {
        "putBlobs": {
            executor: 'shared-iterations',
            vus: params.maxConcurrency,
            iterations: params.blobCount + params.warmupCount,
            maxDuration: "60m",
        }
    },
    thresholds: !params.httpDurationThreshold ? {} : {
        http_req_duration: [`p(99) < ${params.httpDurationThreshold}`]
    },
    insecureSkipTLSVerify: true,
};

export function setup(): number {
    recordStartTime();
    logObject(params, env);
    return parseHumanFileSize(params.payloadSize);
}

export default async function (fileSizeBytes: number) {
    const isWarmup = params.warmupCount > 0
        && exec.scenario.iterationInTest < params.warmupCount;
    const startTimeMs = isWarmup ? 0 : Date.now();
    const response = await putBlob(
        dataFile,
        env.publisherUrl,
        fileSizeBytes,
        { timeout: params.timeout, tags: { warmup: isWarmup ? "true" : "false" } }
    );

    expect(response.status).toBe(200);
    expect(response.json()).toHaveProperty('newlyCreated');

    if (!isWarmup) {
        measuredReqDuration.add(Date.now() - startTimeMs);
    }

    await blobHistory.maybeRecordFromResponse(`blob_ids:${params.payloadSize}`, response);
}

export function teardown() {
    recordEndTime();
}

// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

import redis from 'k6/experimental/redis';

/**
 * Enables storing and retrieving blob IDs that have been successfully stored.
 *
 * If no `redisUrl` is provided, then the class does nothing.
 */
export class BlobHistory {
    private client: redis.Client | undefined

    constructor(redisUrl?: `redis://${string}`) {
        if (!redisUrl) {
            this.client = undefined
        } else {
            this.client = new redis.Client(redisUrl);
        }
    }

    /**
     * Record the given blob ID under the specified key.
     *
     * @returns The number of blob IDs currently recorded under that key,
     * or null if not connected to a redis instance.
     */
    async record(groupKey: string, blobId: string): Promise<number | null> {
        if (this.client == undefined) {
            return null;
        } else {
            return await this.client.rpush(groupKey, blobId);
        }
    }

    /**
     * Record the given blob ID from `newlyCreated.blobObject.blobId` in the json response if
     * the response's status code is 200, otherwise do nothing.
     *
     * @returns The number of blob IDs currently recorded under that key,
     * or null if not connected to a redis instance or the response was not a success.
     */
    async maybeRecordFromResponse(groupKey: string, response: any): Promise<number | null> {
        if (response.status == 200) {
            const blobId = response.json('newlyCreated.blobObject.blobId')?.toString();
            if (blobId) {
                return await this.record(groupKey, blobId);
            }
        }
        return null
    }

    /**
     * Get the blob at a given index in the list pointed to by the key.
     *
     * @returns The blob ID or null if not connected to a redis instance.
     */
    async blobIdAtIndex(groupKey: string, index: number): Promise<string | null> {
        if (this.client == undefined) {
            return null;
        } else {
            return await this.client.lindex(groupKey, index);
        }
    }

    /**
     * Returns the list of blob Ids stored under the provided key,
     * or an empty list if not connected to a Redis instance.
     */
    async list(groupKey: string): Promise<string[]> {
        if (this.client == undefined) {
            return [];
        } else {
            return await this.client.lrange(groupKey, 0, -1);
        }
    }

    /**
     * Returns the number of blob IDs in the list stored under the provided key,
     * or null if not connected to a Redis instance.
     */
    async len(groupKey: string): Promise<number | null> {
        if (this.client == undefined) {
            return null;
        } else {
            return await this.client.llen(groupKey);
        }
    }
}

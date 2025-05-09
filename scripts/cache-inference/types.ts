// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

export type Network = "mainnet" | "testnet";

export type AggregatorData = {
    cache?: boolean;
    operator?: string;
    [key: string]: unknown;
}

export type PublisherData = {
    operator?: string
    [key: string]: unknown;
};

export type NetworkData = {
    aggregators: Record<string, AggregatorData>;
    publishers?: Record<string, PublisherData>;
};

export type Operators = Record<Network, NetworkData>

type NotExisting = undefined;
type NullHeaderValue = null;
export type HeaderValue = string | NotExisting | NullHeaderValue

// Includes more info on the measured request for debugging purposes
// Enabled using `--verbose`
export type AggregatorDataVerbose = AggregatorData & {
    cacheHeaders?: Record<string, [HeaderValue, HeaderValue]>;
    cacheSpeedupMs?: [number, [number, number]];
};

export type NetworkDataVerbose = {
    aggregators?: Record<string, AggregatorDataVerbose>;
    publishers?: Record<string, PublisherData>;
};

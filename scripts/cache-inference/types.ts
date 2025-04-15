
export type Network = "mainnet" | "testnet";

export type AggregatorData = {
    cache?: {
        hasCache: boolean;
        speedupMs?: number | [number, [number, number]]
    },
    operator?: string;
    [key: string]: unknown;
}

export type PublisherData = {
    operator?: string
    [key: string]: unknown;
};

export type NetworkData = {
    aggregators?: Record<string, AggregatorData>;
    publishers?: Record<string, PublisherData>;
};

export type Operators = Record<Network, NetworkData>

type NotExisting = undefined;
type NullHeaderValue = null;
export type HeaderValue = string | NotExisting | NullHeaderValue

export type AggregatorDataVerbose = AggregatorData & {
    cache?: {
        headers?: Record<string, [HeaderValue, HeaderValue]>;
    };
};

export type NetworkDataVerbose = {
    aggregators?: Record<string, AggregatorDataVerbose>;
    publishers?: Record<string, PublisherData>;
};

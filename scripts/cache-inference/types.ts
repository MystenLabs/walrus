
export type Network = "mainnet" | "testnet";

export type AggregatorData = {
    cache?: {
        hasCache: boolean;
        // TODO
        speedup?: any
    }
}

export type PublisherData = Object;

export type NetworkData = {
    aggregators?: Record<string, AggregatorData>;
    publishers?: Record<string, PublisherData>;
};

export type Operators = Record<Network, NetworkData>

export type AggregatorDataVerbose = AggregatorData & {
    cache?: {
        headers?: Record<string, [string, string]>;
    };
};

export type NetworkDataVerbose = {
    aggregators?: Record<string, AggregatorDataVerbose>;
    publishers?: Record<string, PublisherData>;
};

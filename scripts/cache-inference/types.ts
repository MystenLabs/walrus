
export type Network = "mainnet" | "testnet";

export type AggregatorData = {
    cache?: {
        hasCache: boolean;
        headers: {
            key: string;
            value: string | null;
        }[];
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


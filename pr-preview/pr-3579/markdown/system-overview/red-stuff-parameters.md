> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

This page covers the properties, Walrus-specific parameters, and authentication mechanisms of the [RedStuff encoding algorithm](red-stuff).

## Properties and observations

### Why the rectangular layout?

The rectangular layout of the message matrix is an optimization for the Byzantine setting. When storing the blob, a client can only await $N - f$ responses, as the remaining $f$ shards might be Byzantine. Yet, $f$ of these $N-f$ might be the Byzantine ones, and the $f$ that did not reply were only slow due to network asynchrony. Therefore, the blob needs to be encoded such that $N-2f$ symbols are sufficient to recover the original blob. This is achieved by the primary encoding.

After the initial sharing phase, the honest shards share and reconstruct the missing slivers from each other. At a steady state, you can always assume that $N - f$ honest shards are in possession of their slivers. The secondary encoding can therefore have a higher rate, $(N-f)/N$, decreasing the storage overhead while maintaining the same fault tolerance properties.

### Worst case initial sharing

The following describes how the $N-f$ honest shards can obtain their sliver pairs in the worst case outlined above, where the client shares the slivers with $N-f$ shards, $f$ of which are Byzantine and drop them.

1. The $N-2f$ honest shards receive the sliver pairs.
2. The remaining $f$ honest shards are notified of the stored blob, such as through the chain, and start the process to recover their sliver pairs.
3. First, they recover their secondary slivers, as they can be decoded from $N-2f$ recovery symbols.
4. Then, once all $N-f$ honest shards have their secondary slivers, they can start recovering the primary slivers, which require $N-f$ recovery symbols.
5. All honest shards have their sliver pairs.

### Storage overhead

Assume for simplicity that $N=3f+1$. Then, the original blob is divided into roughly $f \cdot 2f = 2f^2$ symbols. The system stores $N \cdot 2f$ primary sliver symbols and $N \cdot f$ secondary sliver symbols, for a total storage of about $9f^2$ symbols.

Therefore, the storage overhead due to RedStuff encoding is about $9f^2 / 2f^2 = 4.5$ times the original blob size.

### Differences with the Twin-Code framework

The key modifications in RedStuff, compared to the original Twin-Code framework, are the following:

- RedStuff uses the RaptorQ fountain code for both the Type 0 and Type 1 encoding, as they are called in the paper. The rates are about $(N-2f)/N$ and $(N-f)/N$ respectively. The Type 0 encoding is called the primary encoding, and the Type 1 encoding the secondary encoding.
- The blob is not laid out in a square message matrix, but in a rectangular one. This is an optimization for the specific BFT setting described here.
- Both Type 0 and Type 1 encodings are stored on each shard. These are called slivers, and the 2 together form a sliver pair.

## Walrus-specific parameters and considerations

### Sliver-pair-to-shard mapping

The previous sections assumed that sliver pair $i$ is stored on shard $i$. In practice, sliver pairs are mapped to shards in a pseudo-random fashion to ensure that the systematic slivers, which contain the original data, are not always stored on the same shard.

This is important because systematic slivers are the most frequently accessed, as they allow you to access the data without any decoding.

The mapping works as follows: each encoded blob is assigned a 32-byte pseudo-random blob ID. This ID is interpreted as an unsigned big-endian integer, and its remainder modulo $N$ is used as a rotation offset, such that sliver pair $i$ is stored on shard $(i + \text{offset}) \mod N$.

### Decoding probability and decoding safety limit

As mentioned above, the reconstruction failure probability of the RaptorQ code is $O(256^{-(H+1)})$, where $H$ is the number of extra symbols received. Therefore, in a system with $f$ Byzantine shards, the number of source symbols for the primary encoding should be slightly below $N-2f$, and for the secondary encoding slightly below $N-f$. This ensures that whenever a validity or quorum threshold of messages is received, there is always a positive $H$ for a low failure probability.

The following parameters are used in the encoding configuration:

- $f$, the maximum number of Byzantine shards, is $\lfloor (N-1) / 3 \rfloor$.
- The safety limit for the encoding, $\sigma$, is set as a function of $N$ to ensure high reconstruction probability (see table below).
- The number of primary source symbols (equivalent to the number of symbols in a secondary sliver) is $N - 2f -\sigma$.
- The number of secondary source symbols (equivalent to the number of symbols in a primary sliver) is $N - f -\sigma$.

Currently, $\sigma$ is selected depending on the number of shards as follows:

| N shards from | N shards to (incl) | $\sigma$ | Prob. failure |
|--------------:|-------------------:|---------:|--------------:|
|             0 |                 15 |        0 |       0.00391 |
|            16 |                 30 |        1 |      1.53e-05 |
|            31 |                 45 |        2 |      5.96e-08 |
|            46 |                 60 |        3 |      2.33e-10 |
|            61 |                 75 |        4 |      9.09e-13 |
|            76 |                inf |        5 |      3.55e-15 |

For example, the following settings apply:

| N shards |   f | $\sigma$ | Num primary | Num secondary |
|---------:|----:|---------:|------------:|--------------:|
|        7 |   2 |        0 |           3 |             5 |
|       10 |   3 |        0 |           4 |             7 |
|       31 |  10 |        2 |           9 |            19 |
|      100 |  33 |        5 |          29 |            62 |
|      300 |  99 |        5 |          97 |           196 |
|     1000 | 333 |        5 |         329 |           662 |

## Blob size limits

In RaptorQ, the size of a symbol is encoded as a 16-bit integer. Therefore, the maximum size of a blob that can be encoded is $2^{16} - 1 = 65535$ bytes. At a minimum, a symbol must be at least 1 byte.

Because the blob is encoded in the rectangular message matrix, the blob size is upperbound by `source_symbols_primary * source_symbols_secondary * u16::MAX` and lowerbound by `source_symbols_primary * source_symbols_secondary`. A few examples for the same configurations as above:

| N shards | Min blob size | Max blob size | Min encoded blob size | Max encoded blob size |
|---------:|--------------:|--------------:|----------------------:|----------------------:|
|        7 |        15.0 B |        983 KiB |                56.0 B |               3.67 MiB |
|       10 |        28.0 B |       1.83 MiB |                 110 B |               7.21 MiB |
|       31 |         171 B |       11.2 MiB |                 868 B |               56.9 MiB |
|      100 |       1.80 KiB |        118 MiB |               9.10 KiB |                596 MiB |
|      300 |       19.0 KiB |       1.25 GiB |               87.9 KiB |               5.76 GiB |
|     1000 |        218 KiB |       14.3 GiB |                991 KiB |               64.9 GiB |

## Sliver authentication, blob metadata, and the blob ID

Alongside the efficient encoding performed by RedStuff, shards need to be able to authenticate that the slivers and encoding symbols they receive belong to the blob they requested. This section briefly outlines how this is achieved.

For each sliver, primary or secondary, a Merkle tree is constructed.

![](./images/sliver-hash.png)

The tree is constructed over all $N$ symbols of the fully expanded sliver. The root node of the Merkle tree (the sliver hash) is included in the metadata for the blob. To prove that a symbol is part of a sliver, the prover supplies the symbol alongside the Merkle path to the root hash, which every node has as part of the metadata.

A Merkle tree over the sliver hashes is then computed to obtain a blob hash. This is computed by concatenating primary and secondary sliver hashes for each sliver pair, and then constructing the Merkle tree over the concatenations ($c_i$ in the figure). This construction reduces the number of hashing operations compared to hashing each sliver Merkle root individually.

![](./images/blob-hash.png)

To prove that a sliver is part of a blob, it is sufficient to provide the Merkle path to the root.

Finally, the encoding type tag (representing the RedStuff version or alternative encoding), the length of the blob before encoding, and the Merkle root of the tree over the slivers are hashed together to obtain the blob ID.

### Metadata overhead

Each storage node stores the full metadata for the blob. The metadata consists of:

- A `32 B` Merkle root hash for each primary and secondary sliver.
- The `32 B` blob ID, computed as above.
- The erasure code type (`1 B`).
- The length of the unencoded blob size (`8 B`).

The hashes for the primary and secondary slivers can be a considerable overhead when the number of shards is high. The following table shows the cumulative size of the hashes stored on the system, depending on the number of nodes and shards.

| N shards | One node | floor(N/floor(log2(N))) nodes | N nodes |
|---------:|---------:|------------------------------:|--------:|
|        7 |    448 B |                       1.34 KiB | 3.14 KiB |
|       10 |    640 B |                       1.92 KiB | 6.40 KiB |
|       31 |  1.98 KiB |                       13.9 KiB | 61.5 KiB |
|      100 |  6.40 KiB |                        102 KiB |  640 KiB |
|      300 |  19.2 KiB |                        710 KiB | 5.76 MiB |
|     1000 |  64.0 KiB |                       7.10 MiB | 64.0 MiB |

The cumulative size of the hashes in the case of 1000 nodes (1 node per shard) is 64 KiB per node, or 64 MiB for a system of 1000 nodes. The number of shards is fixed and constant, while the number of nodes might vary—each node has 1 or more shards—potentially lowering the overhead on the system. The following table shows the ratio between the size of the hashes stored on the system to the minimum and maximum blob sizes, for `N=1000` shards and different numbers of nodes (1 node, floor(N/floor(log2(N))) = 111, and 1000).

| N = 1000                      | Total metadata size | Factor min blob | Factor max blob | Factor min encoded blob | Factor max encoded blob |
| ----------------------------- | ------------------: | --------------: | --------------: | ----------------------: | ----------------------: |
| Single node                   |             64.0 KiB |           0.294 |        4.48e-06 |                  0.0646 |                9.85e-07 |
| floor(N/floor(log2(N))) nodes |             7.10 MiB |            32.6 |        0.000498 |                    7.17 |                0.000109 |
| N nodes                       |             64.0 MiB |             294 |         0.00448 |                    64.6 |                0.000985 |

For realistic node counts and small blob sizes, the total metadata overhead can be a multiple of the size of the initial unencoded blob. For larger blob sizes, the overhead is negligible.

---

**Reference**

[1] K. V. Rashmi, N. B. Shah and P. V. Kumar, "Enabling node repair in any erasure code for distributed storage," 2011 IEEE International Symposium on Information Theory Proceedings, St. Petersburg, Russia, 2011, pp. 1235-1239, doi: [10.1109/ISIT.2011.6033732](https://doi.org/10.1109/ISIT.2011.6033732).
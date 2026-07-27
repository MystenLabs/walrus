> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

The RedStuff encoding algorithm used in Walrus is an adaptation of the Twin-Code framework presented by Rashmi et al. [[1]](https://doi.org/10.1109/ISIT.2011.6033732).

## Goals and overview

The goal of the Walrus system is to provide a distributed storage infrastructure, where a decentralized set of entities—the storage nodes—collaborate to store and serve files (blobs of data). When it comes to storage properties, Walrus has 3 key goals:

1. To support extremely high availability and durability of the data.
2. To have low storage overhead compared to full replication, meaning you do not store each blob on every storage node.
3. To gracefully support node failures, and in particular to allow for efficient node recovery (more on this later).

Given these requirements, one good option is to erasure encode the blobs across the storage nodes. At a high level, erasure encoding (or erasure coding) allows you to encode the data into $N$ parts, such that the aggregate size of the $N$ blobs is a small multiple of the original blob size, and a subset $k$ of these parts is sufficient to recover the original blob. The next section formalizes these concepts, but note that erasure coding already allows you to achieve goals 1 and 2 above, because:

1. Erasure coding allows you to recover a blob even if $N - k$ storage nodes fail, providing high availability and durability.
2. The overall storage overhead is much smaller than for full replication. For a blob of size $S$, the total storage used in the system is $S \cdot c$ instead of $S \cdot N$, where $c \ll N$ is a small constant (4.5 in Walrus's case).

To achieve the third requirement, however, simple erasure coding is insufficient. A failed node that wants to reconstruct its part of the encoding needs to first fetch at least $k$ other parts, reconstruct the blob, and then re-encode its own part. Therefore, the communication overhead for recovery is on the order of the size of the whole blob, $S$. With RedStuff, you can instead reconstruct the encoded part of a failed node by fetching only $O(S/N)$ data, meaning only in the order of the size of the lost part. This achieves goal 3.

## Background

This section provides the essential background on the coding schemes used in RedStuff.

### Erasure codes

Erasure coding addresses the problem of error correction in the case of bit erasures, where some bits in the message are lost, as in the case of a lossy channel. An erasure code divides a blob (or message) of $S$ bytes into $k$ symbols (bitstrings of fixed length $\sim S/k$), which are then encoded to form a longer message of $N$ symbols, such that the original blob can be recovered from any subset $k'$ of the $N$ symbols. The ratio $k/N$ is called the code rate.

### Fountain codes

Fountain codes are a class of erasure codes. The key property of fountain codes is that the encoding process is rateless, meaning the encoder can produce an arbitrary number of encoded parts without knowing the total number of parts that will be produced. This is useful for the RedStuff use case, as it allows you to specify the rate of the encoder. For example, by encoding $f+1$ source symbols into $N$ recovery symbols, you guarantee that any subset of $f+1$ symbols can reconstruct the source. Fountain codes are also extremely efficient as they typically require only XOR operations to encode and decode data.

### RaptorQ

RedStuff is based on the RaptorQ fountain code. RaptorQ is one of the fastest and most efficient fountain codes, and has the following properties:

1. It is systematic, meaning the first $k$ symbols of the encoded message correspond to the original message.
2. It is a linear code, meaning the encoding process is a linear transformation of the input symbols, or in other words, the encoded symbols are linear combinations of the input symbols.
3. It is almost optimal, meaning that $k' \approx k$. Specifically, the probability of decoding failure for $k' = k + H$ symbols received is $\lt 1/256^{H+1}$.

## RedStuff encoding

An established approach in distributed storage is to use an erasure code to encode blobs of data across multiple storage nodes. By using a $k/N$ rate erasure code for $N$ nodes and $k$ source symbols, the system can tolerate $N - k$ node failures, with just an $N/k$ factor of storage overhead. However, in the case of a node failure, the recovery process is inefficient: the failed node needs to fetch $k$ other parts, reconstruct the blob, and then re-encode its own part. Therefore, the communication overhead for recovery is on the order of the size of the whole blob, $S$.

The Twin-Code framework aims to solve this issue by allowing for efficient node recovery. This section briefly describes how the framework is used in RedStuff. For specific details, refer to the original paper. The RedStuff encoding algorithm is an adaptation of the Twin-Code framework, which allows for efficient node recovery in erasure-coded storage systems.

Consider a scenario in which a blob of data is encoded and stored across $N$ shards—multiple shards can be mapped to the same storage node—in a Byzantine setting. Up to $f$ of the shards can be corrupted by an adversary, with $f \lt 1/3 N$, and the remaining $N - f$ shards are honest.

### Encoding and recovery

The RedStuff encoding and recovery process works as follows:

- First, the data blob of size $S$ is divided into symbols and arranged in a rectangular message matrix of up to $N - 2f$ rows and $N - f$ columns of symbols. The number of rows ($n_R$) and columns ($n_C$) is fixed, and determines the symbol size $s$ as follows:

$$
s = \left\lceil S / (n_R \cdot n_C) \right\rceil
$$

- Then, the columns and the rows of the message matrix are encoded separately with RaptorQ.
  - The primary encoding, performed on columns, expands the $n_R$ symbols of each column to $N$ symbols. The rateless nature of RaptorQ allows you to choose the number of encoded symbols.
  - The secondary encoding, performed on rows, expands the $n_C$ symbols of each row to $N$ symbols.
- $n_R$ is also called the number of primary source symbols, and $n_C$ the number of secondary source symbols. The primary encoding has rate $n_R / N$, and the secondary encoding has rate $n_C / N$.
- The encoded rows and columns are then used to obtain primary and secondary slivers, which are distributed to shards and used for blob reconstruction and sliver recovery:
  - Primary slivers are the rows of the matrix of size $N \times n_C$ obtained with the primary encoding of the message matrix. Each primary sliver is therefore composed of $n_C$ symbols.
  - Secondary slivers are the columns of the matrix of size $n_R \times N$ obtained with the primary encoding of the message matrix. Each secondary sliver is therefore composed of $n_R$ symbols.
- Each shard receives a primary and a secondary sliver, based on the shard number and the row and column numbers of the slivers. See the [section on sliver-to-shard mapping](red-stuff-parameters#sliver-pair-to-shard-mapping) for more details.
- The fundamental property achieved with this construction, thanks to the linearity of RaptorQ, is that encoding the primary slivers (as rows) with the secondary encoding and the secondary slivers (as columns) with the primary encoding results in the same $N \times N$ expanded message matrix. This property enables lost sliver recovery:
  - To reconstruct a lost primary sliver, a shard can request $N-f$ symbols from the encodings of the secondary slivers of other shards. Because the primary encoding of secondary slivers results in the symbols for primary slivers, and the secondary encoding has $n_C$ source symbols where $n_C \leq N-2f$, the shard can decode the original primary sliver from the obtained recovery symbols with high probability. See the [discussion on recovery probability](red-stuff-parameters#decoding-probability-and-decoding-safety-limit) for more details.
  - The reconstruction of secondary slivers is identical, but with the roles of primary and secondary slivers and encodings inverted.

For a concrete worked example, see [RedStuff encoding example](red-stuff-details) and [RedStuff recovery example](red-stuff-recovery). For Walrus-specific parameters, see [RedStuff properties and parameters](red-stuff-parameters).
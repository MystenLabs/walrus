> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

This page continues from the [RedStuff encoding algorithm overview](red-stuff) with a concrete worked example.

## Worked example

### Encoding

Consider a Walrus instance with $N = 7 = 3f + 1$ shards. This means the number of primary source symbols is $N - 2f = 3$, and secondary $N - f = 5$. A blob of size $S = 15 \cdot s$ can therefore be divided into 15 symbols of size $s$, and arranged in the matrix as follows.

$$
\left[
\begin{array}{ccccc}
s_{0,0} & s_{0,1} & s_{0,2} & s_{0,3} & s_{0,4} \\
s_{1,0} & s_{1,1} & s_{1,2} & s_{1,3} & s_{1,4} \\
s_{2,0} & s_{2,1} & s_{2,2} & s_{2,3} & s_{2,4} \\
\end{array}
\right]
$$

Then, the primary encoding acts on the columns of the matrix, expanding them such that each column is composed of 4 source symbols and 6 recovery symbols ($s_{i,j}$ indicates source symbols, while $r_{i,j}$ indicates recovery symbols).

$$
\left[
\begin{array}{c|c|c|c|c}
s_{0,0} & s_{0,1} & s_{0,2} & s_{0,3} & s_{0,4} \\
s_{1,0} & s_{1,1} & s_{1,2} & s_{1,3} & s_{1,4} \\
s_{2,0} & s_{2,1} & s_{2,2} & s_{2,3} & s_{2,4} \\
\color{blue} r_{3,0} & \color{blue} r_{3,1} & \color{blue} r_{3,2} & \color{blue} r_{3,3} & \color{blue} r_{3,4} \\
\color{blue} r_{4,0} & \color{blue} r_{4,1} & \color{blue} r_{4,2} & \color{blue} r_{4,3} & \color{blue} r_{4,4} \\
\color{blue} r_{5,0} & \color{blue} r_{5,1} & \color{blue} r_{5,2} & \color{blue} r_{5,3} & \color{blue} r_{5,4} \\
\color{blue} r_{6,0} & \color{blue} r_{6,1} & \color{blue} r_{6,2} & \color{blue} r_{6,3} & \color{blue} r_{6,4} \\
\end{array}
\right]
$$

Each of the rows of this column expansion is a primary sliver. For example, $[r_{5,0}, r_{5,1}, r_{5,2}, r_{5,3}, r_{5,4}, r_{5,5}, r_{5,6}]$.

Similarly, the secondary encoding on the rows of the matrix produces the expanded rows.

$$
\left[
\begin{array}{ccccccc}
s_{0,0} & s_{0,1} & s_{0,2} & s_{0,3} & s_{0,4} & \color{blue} r_{0,5} & \color{blue} r_{0,6} \\
\hline
s_{1,0} & s_{1,1} & s_{1,2} & s_{1,3} & s_{1,4} & \color{blue} r_{1,5} & \color{blue} r_{1,6} \\
\hline
s_{2,0} & s_{2,1} & s_{2,2} & s_{2,3} & s_{2,4} & \color{blue} r_{2,5} & \color{blue} r_{2,6} \\
\end{array}
\right]
$$

Each of the columns of this row expansion is a secondary sliver. For example, $[r_{0,6}, r_{1,6}, r_{2,6}]$.

The $i$th sliver pair is composed of the $i$th primary and $i$th secondary slivers. For simplicity, consider that the $i$th sliver pair is stored on shard $i$. The [sliver-pair-to-shard mapping section](#sliver-pair-to-shard-mapping) discusses the full mapping.

Thanks to the linearity of RaptorQ, the expansion of:

- the recovery secondary slivers (columns 5 and 6) with the primary encoding, and
- the recovery primary slivers (rows 3, 4, 5, and 6) with the secondary encoding,

results in the same set of symbols, which is essential for recovery. These symbols can be represented as the lower-right quadrant of what is called the fully expanded message matrix.

$$
\left[
\begin{array}{ccccc|cc}
s_{0,0} & s_{0,1} & s_{0,2} & s_{0,3} & s_{0,4} & r_{0,5} & r_{0,6} \\
s_{1,0} & s_{1,1} & s_{1,2} & s_{1,3} & s_{1,4} & r_{1,5} & r_{1,6} \\
s_{2,0} & s_{2,1} & s_{2,2} & s_{2,3} & s_{2,4} & r_{2,5} & r_{2,6} \\
\hline
r_{3,0} & r_{3,1} & r_{3,2} & r_{3,3} & r_{3,4} & \color{blue} r_{3,5} & \color{blue} r_{3,6} \\
r_{4,0} & r_{4,1} & r_{4,2} & r_{4,3} & r_{4,4} & \color{blue} r_{4,5} & \color{blue} r_{4,6} \\
r_{5,0} & r_{5,1} & r_{5,2} & r_{5,3} & r_{5,4} & \color{blue} r_{5,5} & \color{blue} r_{5,6} \\
r_{6,0} & r_{6,1} & r_{6,2} & r_{6,3} & r_{6,4} & \color{blue} r_{6,5} & \color{blue} r_{6,6} \\
\end{array}
\right]
$$

These symbols do not need to be stored on any node because they can always be recomputed by expanding either a primary or secondary symbol. For example, $r_{4,5}$ can be obtained by:

- the secondary-encoding expansion of the 4th primary sliver: $[r_{4,0}, r_{4,1}, r_{4,2}, r_{4,3}, r_{4,4}, \color{blue} r_{4,5}, r_{4,6}]$, or
- the primary-encoding expansion of the 5th secondary sliver: $[r_{0,5}, r_{1,5}, r_{2,5}, r_{3,5}, \color{blue} r_{4,5}, r_{5,5}, r_{6,5}]$.

### Recovery

Consider that shard 3 fails, losing its slivers, and needs to recover them. In the following, the symbols of the lost slivers are highlighted in red (the lower quadrant is never stored).

$$
\left[
\begin{array}{ccccc|cc}
s_{0,0} & s_{0,1} & s_{0,2} & \color{red} s_{0,3} & s_{0,4} & r_{0,5} & r_{0,6} \\
s_{1,0} & s_{1,1} & s_{1,2} & \color{red} s_{1,3} & s_{1,4} & r_{1,5} & r_{1,6} \\
s_{2,0} & s_{2,1} & s_{2,2} & \color{red} s_{2,3} & s_{2,4} & r_{2,5} & r_{2,6} \\
\hline
\color{red} r_{3,0} & \color{red} r_{3,1} & \color{red} r_{3,2} & \color{red} r_{3,3} & \color{red} r_{3,4} & & \\
r_{4,0} & r_{4,1} & r_{4,2} & r_{4,3} & r_{4,4} & & \\
r_{5,0} & r_{5,1} & r_{5,2} & r_{5,3} & r_{5,4} & & \\
r_{6,0} & r_{6,1} & r_{6,2} & r_{6,3} & r_{6,4} & & \\
\end{array}
\right]
$$

To recover the primary sliver, the node contacts 5 other shards and requests the recovery symbols for the 3rd primary slivers. Because the symbols of the sliver are recovery symbols, the shards need to encode their secondary slivers (highlighted as columns) to obtain them. For example, shards 0, 1, 2, 4, and 6 provide the symbols:

$$
\left[
\begin{array}{c|c|c|c|c|c|c}
s_{0,0} & s_{0,1} & s_{0,2} & \color{red} s_{0,3} & s_{0,4} & r_{0,5} & r_{0,6} \\
s_{1,0} & s_{1,1} & s_{1,2} & \color{red} s_{1,3} & s_{1,4} & r_{1,5} & r_{1,6} \\
s_{2,0} & s_{2,1} & s_{2,2} & \color{red} s_{2,3} & s_{2,4} & r_{2,5} & r_{2,6} \\
\color{green} r_{3,0} & \color{green} r_{3,1} & \color{green} r_{3,2} &  & \color{green} r_{3,4} &  & \color{green} r_{3,6}\\
\end{array}
\right]
$$

To recover the secondary sliver, the node contacts at least 3 other shards to obtain recovery symbols. In this case, the recovery symbols are already part of the primary slivers (highlighted as rows) stored by the other shards, so no re-encoding is necessary. For example, shards 0, 1, and 5 provide the recovery symbols:

$$
\left[
\begin{array}{ccccc}
s_{0,0} & s_{0,1} & s_{0,2} & \color{green} s_{0,3} & s_{0,4} \\
\hline
s_{1,0} & s_{1,1} & s_{1,2} & \color{green} s_{1,3} & s_{1,4} \\
\hline
s_{2,0} & s_{2,1} & s_{2,2} & s_{2,3} & s_{2,4} \\
\hline
\color{red} r_{3,0} & \color{red} r_{3,1} & \color{red} r_{3,2} & \color{red} r_{3,3} & \color{red} r_{3,4} \\
\hline
r_{4,0} & r_{4,1} & r_{4,2} & r_{4,3} & r_{4,4} \\
\hline
r_{5,0} & r_{5,1} & r_{5,2} & \color{green} r_{5,3} & r_{5,4} \\
\hline
r_{6,0} & r_{6,1} & r_{6,2} & r_{6,3} & r_{6,4} \\
\end{array}
\right]
$$

In this case, the symbols $s_{0,3}$, $s_{1,3}$, and $s_{2,3}$ are already stored in the primary slivers of shards 0, 1, and 2 directly. Therefore, by requesting these from those shards, shard 3 does not need to decode the symbols to recover its secondary sliver.

For properties, Walrus-specific parameters, blob size limits, and sliver authentication details, see [RedStuff properties and parameters](red-stuff-parameters).
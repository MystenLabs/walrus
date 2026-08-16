> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

The following Move example showcases how to import and use Walrus onchain objects.

A certified blob exists on Sui as a `Blob` object, so Move code can hold one the same way it holds any other object. The module below imports `walrus::blob::Blob`, defines a `WrappedBlob` struct that owns one, and exposes a `wrap` function that moves a caller's `Blob` into it.

That pattern is the basis for building on stored data: once your own struct owns the `Blob`, your package controls the blob's lifecycle, and it can attach whatever application state it needs alongside the stored content. The full example package, including the `Move.toml` that declares the Walrus dependency, lives in [`docs/examples/move/walrus_dep`](https://github.com/MystenLabs/walrus/tree/main/docs/examples/move/walrus_dep).

<!-- ImportContent: GitHub source — resolve at export time or visit https://github.com/MystenLabs/walrus/blob/main/docs/examples/move/walrus_dep/sources/wrapped_blob.move -->
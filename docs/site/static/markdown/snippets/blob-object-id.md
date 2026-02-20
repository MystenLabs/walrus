After you upload a blob to Walrus, it has 2 identifiers:

```sh
Blob ID: oehkoh0352bRGNPjuwcy0nye3OLKT649K62imdNAlXg
Sui object ID: 0x1c086e216c4d35bf4c1ea493aea701260ffa5b0070622b17271e4495a030fe83
```

- Blob ID: A way to reference the blob on Walrus. The system generates the blob ID based on the blob's contents, meaning any file you upload to the network twice results in the same blob ID.

- Sui Object ID: The blob's corresponding newly created Sui object identifier, as the system binds all blobs to one or more Sui objects.

You use blob IDs to read blob data, while you use Sui object IDs to make modifications to the blob's metadata, such as its storage duration. You might also use them to read blob data.
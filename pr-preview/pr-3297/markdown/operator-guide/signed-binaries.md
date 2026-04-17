Walrus release binaries and Docker images are signed using [Cosign](https://github.com/sigstore/cosign) with a GCP KMS key. Each binary has a corresponding `.sig` file containing its signature. Docker image signatures are stored in the registry.

<Tabs>
<TabItem value="prereq" label="Prerequisites">

- [x] Install [Cosign](https://docs.sigstore.dev/cosign/system_config/installation/)
- [x] Install the [gcloud CLI](https://cloud.google.com/sdk/docs/install) (for downloading binaries)

</TabItem>
</Tabs>

## Use the verification script

The easiest way to download and verify binaries is to use the provided script. See also the [Storage Node Setup](/docs/operator-guide/storage-nodes/storage-node-setup#binaries) guide for standard binary downloads without verification.

```bash
# Download the script
curl -sSfL https://raw.githubusercontent.com/MystenLabs/walrus/main/scripts/download_and_verify_binary.sh \
  -o download_and_verify_binary.sh
chmod +x download_and_verify_binary.sh

# Download and verify a binary
./download_and_verify_binary.sh <ref> <binary_name>
```

The command takes 2 arguments:

- `ref`: A git reference (branch, tag, or commit SHA)
- `binary_name`: The full binary name with platform suffix

For example:

```bash
./download_and_verify_binary.sh v1.0.0 walrus-ubuntu-x86_64
```

This downloads the binary and signature, then verifies the signature using the public key.

### Available platforms

| **Platform** | **Suffix** |
|----------|--------|
| Linux x86_64 | `-ubuntu-x86_64` |
| Linux ARM64 | `-ubuntu-aarch64` |
| macOS x86_64 | `-macos-x86_64` |
| macOS ARM64 | `-macos-arm64` |
| Windows x86_64 | `-windows-x86_64.exe` |

### Available binaries

| **Binary** | **Description** |
|--------|-------------|
| `walrus` | The Walrus CLI client |
| `walrus-node` | The Walrus storage node |
| `walrus-upload-relay` | The Walrus upload relay service |

## Verify manually

If you prefer to verify manually without the script, first download the signed binaries from Google Cloud Storage:

```bash
# Download the binary
gcloud storage cp gs://mysten-walrus-binaries/signed/<ref>/walrus-ubuntu-x86_64 ./walrus

# Download the signature
gcloud storage cp gs://mysten-walrus-binaries/signed/<ref>/walrus-ubuntu-x86_64.sig ./walrus.sig
```

Then verify them using the public key:

```bash
# Download the public key
curl -sSfL https://docs.walrus.site/walrus-signing.pub -o walrus-signing.pub

# Verify the binary
cosign verify-blob \
  --key walrus-signing.pub \
  --signature walrus.sig \
  --insecure-ignore-tlog \
  walrus
```

If successful, the output displays:

```
Verified OK
```

:::info

The `--insecure-ignore-tlog` flag is required because signatures are not uploaded to the Sigstore transparency log. Verification is still cryptographically secure using the signing key.

:::

### Verify using the GCP KMS key directly

If you have GCP access:

```bash
# Authenticate with GCP
gcloud auth application-default login

# Verify the binary
cosign verify-blob \
  --key gcpkms://projects/walrus-infra/locations/global/keyRings/walrus-signing/cryptoKeys/release-sign \
  --signature walrus.sig \
  --insecure-ignore-tlog \
  walrus
```

## Verify Docker images

Signed Docker images are published to Docker Hub with the `-signed` suffix.

### Available images

| **Image** | **Description** |
|-------|-------------|
| `mysten/walrus-service` | Walrus storage node service |
| `mysten/walrus-upload-relay` | Walrus upload relay service |

### Image tags

Each image is tagged in 2 ways:

- **By commit SHA:** `mysten/walrus-service:FULL_SHA-signed`
- **By version:** `mysten/walrus-service:vVERSION-signed`

### Pull a signed image

```bash
# Pull by version
docker pull mysten/walrus-service:v1.0.0-signed

# Pull by commit SHA
docker pull mysten/walrus-service:a5a6dc7b12345678-signed
```

### Verify a Docker image

```bash
# Download the public key
curl -sSfL https://docs.walrus.site/walrus-signing.pub -o walrus-signing.pub

# Verify the image
cosign verify --key walrus-signing.pub mysten/walrus-service:v1.0.0-signed
```

You can also verify using the GCP KMS key directly:

```bash
cosign verify \
  --key gcpkms://projects/walrus-infra/locations/global/keyRings/walrus-signing/cryptoKeys/release-sign \
  mysten/walrus-service:v1.0.0-signed
```

### Verify the image digest matches

To ensure the image you pulled matches what was signed:

```bash
# Get the digest of your local image
docker inspect --format='{{index .RepoDigests 0}}' mysten/walrus-service:v1.0.0-signed

# Compare with the digest shown in the cosign verify output
```

## Troubleshoot common errors

### Permission denied when verifying

If you get a permission denied error when using the GCP KMS key directly, try one of the following:

- Authenticate with GCP by running `gcloud auth application-default login`.
- Use the public key method instead.

### Signature verification failed

If verification fails:

1. Ensure you downloaded both the binary and signature for the same version.
1. Ensure the files were not corrupted during download.
1. Try re-downloading both files.

### No matching signatures error for Docker images

If you see a no matching signatures error:

1. Ensure the image tag includes the `-signed` suffix.
1. Verify the commit SHA is correct.
1. Check that the image was actually signed.

### Image not found

If the Docker image is not found:

1. Verify the commit SHA exists in the repository.
1. Check that the signed binaries workflow ran successfully for that commit.
1. Ensure you are using the correct image name.

## Security considerations

The signing infrastructure has the following properties:

- The signing key is stored in GCP KMS and cannot be exported.
- Only authorized CI/CD workflows can sign binaries and images.
- Each binary is signed individually, not just the archive.
- Docker image signatures are stored in the registry alongside the image.
#!/bin/bash
# Copyright (c) Walrus Foundation
# SPDX-License-Identifier: Apache-2.0
#
# Downloads and verifies a signed Walrus binary.
#
# Usage: ./download_and_verify_binary.sh <ref> <binary_name>
#
# Arguments:
#   ref          - Git reference (branch, tag, or commit SHA)
#   binary_name  - Full binary name with platform suffix (e.g., walrus-ubuntu-x86_64)
#
# Available binaries:
#   walrus-ubuntu-x86_64, walrus-ubuntu-aarch64, walrus-macos-x86_64, walrus-macos-arm64
#   walrus-node-ubuntu-x86_64, walrus-node-ubuntu-aarch64, etc.
#   walrus-upload-relay-ubuntu-x86_64, etc.
#
# Example:
#   ./download_and_verify_binary.sh v1.0.0 walrus-ubuntu-x86_64

set -euo pipefail

if ! cosign version &> /dev/null; then
    echo "cosign is not installed. Please install cosign for binary verification."
    echo "https://docs.sigstore.dev/cosign/installation"
    exit 1
fi

if ! gcloud version &> /dev/null; then
    echo "gcloud is not installed. Please install the Google Cloud SDK."
    echo "https://cloud.google.com/sdk/docs/install"
    exit 1
fi

if [ $# -lt 2 ]; then
    echo "Usage: $0 <ref> <binary_name>"
    echo ""
    echo "Example: $0 v1.0.0 walrus-ubuntu-x86_64"
    exit 1
fi

ref=$1
binary_name=$2
pub_key_url=https://docs.walrus.site/walrus-signing.pub
gcs_bucket=gs://mysten-walrus-binaries/signed

echo "[+] Downloading binary '$binary_name' for $ref ..."
gcloud storage cp "$gcs_bucket/$ref/$binary_name" "./$binary_name"
gcloud storage cp "$gcs_bucket/$ref/$binary_name.sig" "./$binary_name.sig"
chmod +x "./$binary_name"

echo "[+] Downloading public key ..."
curl -sSfL "$pub_key_url" -o walrus-signing.pub

echo "[+] Verifying binary '$binary_name' for $ref ..."
cosign verify-blob \
    --insecure-ignore-tlog \
    --key walrus-signing.pub \
    --signature "./$binary_name.sig" \
    "./$binary_name"

echo "[+] Verification successful!"

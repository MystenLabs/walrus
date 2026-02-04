#!/usr/bin/env bash
# Copyright (c) Walrus Foundation
# SPDX-License-Identifier: Apache-2.0
#
# This script verifies signed Walrus binaries and Docker images using Cosign.
# It can download binaries from GCS and verify their signatures.

set -euo pipefail

# Default values
GCS_BUCKET="gs://mysten-walrus-binaries/signed"
KMS_KEY="gcpkms://projects/walrus-infra/locations/global/keyRings/walrus-signing/cryptoKeys/release-sign"
PUBLIC_KEY_URL="https://docs.walrus.site/walrus-signing.pub"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

usage() {
    cat <<EOF
Usage: $(basename "$0") <command> [options]

Commands:
  download    Download a binary and its signature from GCS
  verify      Verify a binary signature
  docker      Verify a Docker image signature
  list        List available binaries for a given ref

Options:
  -r, --ref <ref>           Git reference (branch, tag, or commit SHA)
  -b, --binary <name>       Binary name (walrus, walrus-node, walrus-upload-relay)
  -p, --platform <platform> Platform suffix (ubuntu-x86_64, ubuntu-aarch64, macos-x86_64, macos-arm64, windows-x86_64.exe)
  -o, --output <path>       Output path for downloaded binary (default: ./<binary>)
  -k, --key <path>          Path to public key file (optional, will download if not specified)
  -i, --image <image>       Docker image to verify (e.g., mysten/walrus-service:abc123-signed)
  --use-kms                 Use GCP KMS key directly (requires GCP auth)
  -h, --help                Show this help message

Examples:
  # List available binaries for a release
  $(basename "$0") list -r v1.0.0

  # Download and verify a binary
  $(basename "$0") download -r v1.0.0 -b walrus -p ubuntu-x86_64 -o ./walrus
  $(basename "$0") verify -b ./walrus

  # Verify using GCP KMS key directly
  $(basename "$0") verify -b ./walrus --use-kms

  # Verify a Docker image
  $(basename "$0") docker -i mysten/walrus-service:abc123-signed

Platforms:
  ubuntu-x86_64     Linux x86_64
  ubuntu-aarch64    Linux ARM64
  macos-x86_64      macOS x86_64
  macos-arm64       macOS ARM64
  windows-x86_64    Windows x86_64

Binaries:
  walrus              The Walrus CLI client
  walrus-node         The Walrus storage node
  walrus-upload-relay The Walrus upload relay service
EOF
    exit 0
}

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

check_dependencies() {
    local deps=("$@")
    for cmd in "${deps[@]}"; do
        if ! command -v "$cmd" >/dev/null 2>&1; then
            log_error "Required command '$cmd' not found in PATH."
            exit 1
        fi
    done
}

get_public_key() {
    local key_path="$1"

    if [[ -f "$key_path" ]]; then
        log_info "Using existing public key: $key_path"
        return 0
    fi

    log_info "Downloading public key from $PUBLIC_KEY_URL"
    if curl -sSfL "$PUBLIC_KEY_URL" -o "$key_path"; then
        log_info "Public key saved to: $key_path"
    else
        log_error "Failed to download public key"
        exit 1
    fi
}

cmd_list() {
    local ref=""

    while [[ $# -gt 0 ]]; do
        case "$1" in
            -r|--ref)
                ref="$2"
                shift 2
                ;;
            *)
                log_error "Unknown option: $1"
                usage
                ;;
        esac
    done

    if [[ -z "$ref" ]]; then
        log_error "Missing required option: --ref"
        usage
    fi

    check_dependencies gcloud

    log_info "Listing binaries for ref: $ref"
    gcloud storage ls "${GCS_BUCKET}/${ref}/"
}

cmd_download() {
    local ref=""
    local binary=""
    local platform=""
    local output=""

    while [[ $# -gt 0 ]]; do
        case "$1" in
            -r|--ref)
                ref="$2"
                shift 2
                ;;
            -b|--binary)
                binary="$2"
                shift 2
                ;;
            -p|--platform)
                platform="$2"
                shift 2
                ;;
            -o|--output)
                output="$2"
                shift 2
                ;;
            *)
                log_error "Unknown option: $1"
                usage
                ;;
        esac
    done

    if [[ -z "$ref" ]] || [[ -z "$binary" ]] || [[ -z "$platform" ]]; then
        log_error "Missing required options: --ref, --binary, --platform"
        usage
    fi

    check_dependencies gcloud

    local binary_name="${binary}-${platform}"
    local gcs_path="${GCS_BUCKET}/${ref}/${binary_name}"
    output="${output:-"./${binary}"}"

    log_info "Downloading binary: $gcs_path"
    if ! gcloud storage cp "$gcs_path" "$output"; then
        log_error "Failed to download binary"
        exit 1
    fi

    log_info "Downloading signature: ${gcs_path}.sig"
    if ! gcloud storage cp "${gcs_path}.sig" "${output}.sig"; then
        log_error "Failed to download signature"
        exit 1
    fi

    chmod +x "$output"
    log_info "Binary downloaded and made executable: $output"
    log_info "Signature downloaded: ${output}.sig"
    echo ""
    log_info "To verify the binary, run:"
    echo "  $(basename "$0") verify -b $output"
}

cmd_verify() {
    local binary=""
    local key_path=""
    local use_kms=false

    while [[ $# -gt 0 ]]; do
        case "$1" in
            -b|--binary)
                binary="$2"
                shift 2
                ;;
            -k|--key)
                key_path="$2"
                shift 2
                ;;
            --use-kms)
                use_kms=true
                shift
                ;;
            *)
                log_error "Unknown option: $1"
                usage
                ;;
        esac
    done

    if [[ -z "$binary" ]]; then
        log_error "Missing required option: --binary"
        usage
    fi

    check_dependencies cosign

    if [[ ! -f "$binary" ]]; then
        log_error "Binary file not found: $binary"
        exit 1
    fi

    local sig_file="${binary}.sig"
    if [[ ! -f "$sig_file" ]]; then
        log_error "Signature file not found: $sig_file"
        exit 1
    fi

    if [[ "$use_kms" == true ]]; then
        log_info "Verifying using GCP KMS key..."
        if cosign verify-blob \
            --key "$KMS_KEY" \
            --signature "$sig_file" \
            --insecure-ignore-tlog \
            "$binary"; then
            log_info "Signature verification successful!"
        else
            log_error "Signature verification failed!"
            exit 1
        fi
    else
        key_path="${key_path:-"./walrus-signing.pub"}"
        get_public_key "$key_path"

        log_info "Verifying using public key: $key_path"
        if cosign verify-blob \
            --key "$key_path" \
            --signature "$sig_file" \
            --insecure-ignore-tlog \
            "$binary"; then
            log_info "Signature verification successful!"
        else
            log_error "Signature verification failed!"
            exit 1
        fi
    fi
}

cmd_docker() {
    local image=""
    local key_path=""
    local use_kms=false

    while [[ $# -gt 0 ]]; do
        case "$1" in
            -i|--image)
                image="$2"
                shift 2
                ;;
            -k|--key)
                key_path="$2"
                shift 2
                ;;
            --use-kms)
                use_kms=true
                shift
                ;;
            *)
                log_error "Unknown option: $1"
                usage
                ;;
        esac
    done

    if [[ -z "$image" ]]; then
        log_error "Missing required option: --image"
        usage
    fi

    check_dependencies cosign

    if [[ "$use_kms" == true ]]; then
        log_info "Verifying Docker image using GCP KMS key: $image"
        if cosign verify \
            --key "$KMS_KEY" \
            "$image"; then
            log_info "Docker image signature verification successful!"
        else
            log_error "Docker image signature verification failed!"
            exit 1
        fi
    else
        key_path="${key_path:-"./walrus-signing.pub"}"
        get_public_key "$key_path"

        log_info "Verifying Docker image using public key: $image"
        if cosign verify \
            --key "$key_path" \
            "$image"; then
            log_info "Docker image signature verification successful!"
        else
            log_error "Docker image signature verification failed!"
            exit 1
        fi
    fi
}

# Main entry point
if [[ $# -eq 0 ]]; then
    usage
fi

command="$1"
shift

case "$command" in
    download)
        cmd_download "$@"
        ;;
    verify)
        cmd_verify "$@"
        ;;
    docker)
        cmd_docker "$@"
        ;;
    list)
        cmd_list "$@"
        ;;
    -h|--help|help)
        usage
        ;;
    *)
        log_error "Unknown command: $command"
        usage
        ;;
esac

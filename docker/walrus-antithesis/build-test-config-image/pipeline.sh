#!/bin/bash
# Copyright (c) Walrus Foundation
# SPDX-License-Identifier: Apache-2.0
# shellcheck disable=SC2155

msg() {
  echo "$0: note: $*" >&2
}

die() {
  echo "$0: error: $*" >&2
  exit 1
}

build_dir="$(realpath "$(dirname "$0")")"
git_root=$(git -C "$(dirname "$0")" rev-parse --show-toplevel) || die "Failed to get git root"

cleanup-docker-compose() {
  docker compose -f "$1"/docker-compose.yaml down
}

run-pipeline() {
  set -v
  # chdir to git root
  cd "$git_root" || die "Failed to chdir to git root"

  sui_version="$(cargo tree --package sui-rpc-api | grep sui-rpc-api | grep -Eo 'testnet-[^#)]*')"
  msg "Using SUI version: $sui_version"
  # Manually start local registry.
  # docker run -d -p 5000:5000 --restart always --name registry registry:2
  # TODO: check that local registry is running.

  local_sui_image_name=sui-tools:"$sui_version"

  (
    # Assume SUI is in ../sui.
    cd ../sui || die "no sui dir?"
    git fetch origin || die "Failed to fetch SUI"
    git checkout "$sui_version" || die "Failed to checkout SUI version '$sui_version'"
    cd docker/sui-tools
    ./build.sh -t "$local_sui_image_name" || die "Failed to build SUI image"
    # Get SUI image and push to local registry.
  ) || die "failed to build SUI image"

  local_walrus_image="walrus-antithesis:$sui_version"

  export WALRUS_IMAGE_NAME="$local_walrus_image"
  export SUI_IMAGE_NAME="mysten/sui-tools:$sui_version"

  export SUI_PLATFORM=linux/"$(uname -m)"
  export WALRUS_PLATFORM=linux/"$(uname -m)"

  # Build walrus-antithesis image.
  msg "Running walrus-antithesis build"
  docker/walrus-antithesis/build-walrus-image-for-antithesis/build.sh \
    --build-arg RUSTFLAGS= \
    --build-arg LD_LIBRARY_PATH= \
    -t "$local_walrus_image" || die "Failed to build walrus-antithesis image"

  # Kill docker compose on exit.
  trap 'cleanup-docker-compose '"$build_dir" EXIT

  msg "Running docker compose"
  cd "$build_dir" || die "Failed to chdir to build dir"
  docker compose \
    --env-file <(
      echo WALRUS_IMAGE_NAME="$WALRUS_IMAGE_NAME"
      echo SUI_IMAGE_NAME="$SUI_IMAGE_NAME"
    ) \
    -f "$build_dir"/docker-compose.yaml \
    up \
      --pull never \
      --force-recreate \
      --abort-on-container-failure
}

run-pipeline 2>&1 | tee "$git_root"/pipeline.log

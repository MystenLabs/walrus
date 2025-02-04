#!/bin/sh
# Copyright (c) Mysten Labs, Inc.
# SPDX-License-Identifier: Apache-2.0

set -e

DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(git rev-parse --show-toplevel)"
DOCKERFILE="$DIR/Dockerfile"
GIT_REVISION="$(git describe --always --abbrev=12 --dirty --exclude '*')"
BUILD_DATE="$(date -u +'%Y-%m-%d')"

if [ "$1" = "--debug-symbols" ]; then
    PROFILE="bench"
    echo "Building with debug symbols... (Warning: larger binaries)"
    shift
else
    PROFILE="release"
fi

echo
echo "Building walrus-node-prober docker image"
echo "Dockerfile: \t$DOCKERFILE"
echo "Context: \t$REPO_ROOT"
echo "Build date: \t$BUILD_DATE"
echo "Git revision: \t$GIT_REVISION"
echo

docker build -f "$DOCKERFILE" "$REPO_ROOT" \
    --build-arg GIT_REVISION="$GIT_REVISION" \
    --build-arg BUILD_DATE="$BUILD_DATE" \
    --build-arg PROFILE="$PROFILE" \
    --target walrus-node-prober \
    "$@"

#!/bin/sh
# Copyright (c) Walrus Foundation
# SPDX-License-Identifier: Apache-2.0

# fast fail.
set -e

DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(git rev-parse --show-toplevel)"
DOCKERFILE="$DIR/Dockerfile"
GIT_REVISION="$(git describe --always --abbrev=12 --dirty --exclude '*')"
BUILD_DATE="$(date -u +'%Y-%m-%d')"

# option to build using debug symbols
if [ "$1" = "--debug-symbols" ]; then
  PROFILE="bench"
  echo "Building with full debug info enabled ... WARNING: binary size might significantly increase"
  shift
else
  PROFILE="release"
fi

echo
echo "Building walrus-upload-relay docker images"
echo "Dockerfile: \t$DOCKERFILE"
echo "docker context: $REPO_ROOT"
echo "build date: \t$BUILD_DATE"
echo "git revision: \t$GIT_REVISION"
echo

docker build \
  --progress plain \
  -f "$DOCKERFILE" "$REPO_ROOT" \
  --build-arg GIT_REVISION="$GIT_REVISION" \
  --build-arg BUILD_DATE="$BUILD_DATE" \
  --build-arg PROFILE="$PROFILE" \
  --target walrus-upload-relay \
  "$@"

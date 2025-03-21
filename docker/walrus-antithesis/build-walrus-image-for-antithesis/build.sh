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

echo
printf "Building '%s' docker images\n" "$WALRUS_IMAGE_NAME"
printf "Dockerfile: \t%s\n" "$DOCKERFILE"
printf "docker context: %s\n" "$REPO_ROOT"
printf "build date: \t%s\n" "$BUILD_DATE"
printf "git revision: \t%s\n" "$GIT_REVISION"
echo

docker build \
  --progress plain \
  -f "$DOCKERFILE" "$REPO_ROOT" \
  --build-arg GIT_REVISION="$GIT_REVISION" \
  --build-arg BUILD_DATE="$BUILD_DATE" \
  --platform linux/"$(uname -m)" \
  "$@"

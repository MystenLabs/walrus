#!/bin/bash
# Copyright (c) Walrus Foundation
# SPDX-License-Identifier: Apache-2.0

PLATFORM=linux/arm64
GIT_REVISION=$(git rev-parse HEAD)
IMAGE_NAME=local-testbed_walrus-service:${GIT_REVISION}
DOCKER_COMPOSE_FILE=docker/local-testbed/docker-compose.yaml

cd ../.. # go to the root directory (walrus)

docker build -t ${IMAGE_NAME} \
  -f docker/walrus-service/Dockerfile \
  --target walrus-service \
  --platform $PLATFORM .

# echo the IMAGE_NAME and PLATFORM to the .env file
rm -f docker/local-testbed/.env
echo "WALRUS_IMAGE_NAME=${IMAGE_NAME}" >>docker/local-testbed/.env
echo "WALRUS_PLATFORM=${PLATFORM}" >>docker/local-testbed/.env

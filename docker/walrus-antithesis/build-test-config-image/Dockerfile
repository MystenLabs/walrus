# This is the same as debian:bookworm-slim but we use the GCP registry to avoid rate limiting.
FROM us-central1-docker.pkg.dev/cryptic-bolt-398315/base-images/debian:bookworm-slim-amd64 AS setup

RUN apt update
RUN apt install python3 python3-pip -y

WORKDIR /

# Walrus commit picked to run the stress test.
ARG WALRUS_COMMIT_HASH
ARG WALRUS_RUST_LOG

# Set SUI_VERSION to the version.toml value
COPY ./docker/walrus-antithesis/sui_version.toml .

RUN echo "WALRUS_RUST_LOG=$WALRUS_RUST_LOG" >> /.env && echo "WALRUS_RUST_LOG: $WALRUS_RUST_LOG"
RUN echo "WALRUS_IMAGE_NAME=walrus-service:$WALRUS_COMMIT_HASH" >> /.env && \
    echo "WALRUS_IMAGE_NAME: walrus-service:$WALRUS_COMMIT_HASH"
RUN echo "WALRUS_PLATFORM=linux/amd64" >> /.env && echo "WALRUS_PLATFORM: linux/amd64"

# Set SUI_VERSION to the version.toml value so that it can pull the correct sui image.
# Note that we have to run all of these in one command to make sure that SUI_VERSION is set.
RUN export SUI_VERSION=$(grep SUI_VERSION sui_version.toml | cut -d'"' -f2) && \
    echo "SUI_VERSION: $SUI_VERSION" && \
    echo "SUI_IMAGE_NAME=mysten/sui-tools:$SUI_VERSION" >> /.env && \
    echo "SUI_IMAGE_NAME: mysten/sui-tools:$SUI_VERSION"

COPY ./docker/walrus-antithesis/build-test-config-image/docker-compose.yaml /docker-compose.yaml
COPY ./docker/walrus-antithesis/build-test-config-image/files /files

FROM scratch

COPY --from=setup /docker-compose.yaml /docker-compose.yaml
COPY --from=setup /.env /.env
COPY --from=setup /files /files

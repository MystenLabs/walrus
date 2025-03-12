FROM debian:bookworm-slim AS setup

RUN apt update
RUN apt install python3 python3-pip -y

WORKDIR /

ARG WALRUS_IMAGE_NAME

# Set SUI_VERSION to the version.toml value
COPY sui_version.toml .
RUN export SUI_VERSION=$(grep SUI_VERSION sui_version.toml | cut -d'"' -f2) && \
    echo "SUI_VERSION: $SUI_VERSION"

RUN echo "WALRUS_RUST_LOG=$WALRUS_RUST_LOG" >> /.env
RUN echo "WALRUS_IMAGE_NAME=walrus-service:$WALRUS_COMMIT_HASH" >> /.env
RUN echo "WALRUS_PLATFORM=linux/amd64" >> /.env
RUN echo "SUI_IMAGE_NAME=mysten/sui-tools:$SUI_VERSION" >> /.env

COPY ./docker-compose.yaml /docker-compose.yaml
COPY ./files /files

FROM scratch

COPY --from=setup /docker-compose.yaml /docker-compose.yaml
COPY --from=setup /.env /.env
COPY --from=setup /files /files

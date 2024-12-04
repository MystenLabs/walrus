# Build application
#
# Copy in all crates, Cargo.toml and Cargo.lock unmodified,
# and build the application.
FROM rust:1.81-bullseye AS builder
ARG PROFILE=release
ARG GIT_REVISION
ENV GIT_REVISION=$GIT_REVISION
WORKDIR "$WORKDIR/walrus"
RUN apt-get update && apt-get install -y cmake clang

COPY Cargo.toml Cargo.lock ./
COPY crates crates

RUN cargo build --profile $PROFILE \
    --features deploy\
    --bin walrus \
    --bin walrus-node \
    --bin walrus-deploy

# Production Image for all binaries under walrus-service
FROM debian:bullseye-slim AS walrus-service
RUN apt-get update && apt-get install -y ca-certificates curl git
ARG PROFILE=release
WORKDIR "$WORKDIR/walrus"
# Both bench and release profiles copy from release dir
COPY --from=builder /walrus/target/release/walrus /opt/walrus/bin/walrus
COPY --from=builder /walrus/target/release/walrus-node /opt/walrus/bin/walrus-node
COPY --from=builder /walrus/target/release/walrus-deploy /opt/walrus/bin/walrus-deploy

ARG BUILD_DATE
ARG GIT_REVISION
LABEL build-date=$BUILD_DATE
LABEL git-revision=$GIT_REVISION

# Production Image for walrus CLI
FROM debian:bullseye-slim AS walrus-cli
RUN apt-get update && apt-get install -y ca-certificates curl
ARG PROFILE=release
WORKDIR "$WORKDIR/walrus"
# Both bench and release profiles copy from release dir
COPY --from=builder /walrus/target/release/walrus /opt/walrus/bin/walrus

ARG BUILD_DATE
ARG GIT_REVISION
LABEL build-date=$BUILD_DATE
LABEL git-revision=$GIT_REVISION

# Production Image for walrus node
FROM debian:bullseye-slim AS walrus-node
RUN apt-get update && apt-get install -y ca-certificates curl
ARG PROFILE=release
WORKDIR "$WORKDIR/walrus"
# Both bench and release profiles copy from release dir
COPY --from=builder /walrus/target/release/walrus-node /opt/walrus/bin/walrus-node

ARG BUILD_DATE
ARG GIT_REVISION
LABEL build-date=$BUILD_DATE
LABEL git-revision=$GIT_REVISION

# Production Image for walrus deployment CLI
FROM debian:bullseye-slim AS walrus-deploy
RUN apt-get update && apt-get install -y ca-certificates curl
ARG PROFILE=release
WORKDIR "$WORKDIR/walrus"
# Both bench and release profiles copy from release dir
COPY --from=builder /walrus/target/release/walrus-deploy /opt/walrus/bin/walrus-deploy

ARG BUILD_DATE
ARG GIT_REVISION
LABEL build-date=$BUILD_DATE
LABEL git-revision=$GIT_REVISION

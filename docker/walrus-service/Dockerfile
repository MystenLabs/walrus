# Build application
#
# Copy in all crates, Cargo.toml and Cargo.lock unmodified,
# and build the application.

FROM rust:1.93-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends cmake clang \
 && rm -rf /var/lib/apt/lists/*
ARG PROFILE=release
WORKDIR "$WORKDIR/walrus"

# COPYs ordered least- to most-frequently-changing for deeper layer cache hits.
COPY rust-toolchain.toml ./
COPY Cargo.toml Cargo.lock ./
COPY setup setup
COPY contracts ./contracts
COPY contracts /contracts
COPY crates crates

# GIT_REVISION declared late so SHA changes don't invalidate the COPYs above.
ARG GIT_REVISION
ENV GIT_REVISION=$GIT_REVISION
RUN cargo build --profile $PROFILE \
    --bin walrus \
    --bin walrus-node \
    --bin walrus-deploy

# Production Image for all binaries under walrus-service
FROM debian:bookworm-slim AS walrus-service

RUN apt-get update && apt-get install -y ca-certificates curl git
ARG PROFILE=release
WORKDIR "$WORKDIR/walrus"
# Both bench and release profiles copy from release dir
COPY --from=builder /walrus/target/release/walrus /opt/walrus/bin/walrus
COPY --from=builder /walrus/target/release/walrus-node /opt/walrus/bin/walrus-node
COPY --from=builder /walrus/target/release/walrus-deploy /opt/walrus/bin/walrus-deploy
COPY --from=builder /contracts /opt/walrus/contracts

ARG BUILD_DATE
ARG GIT_REVISION
LABEL build-date=$BUILD_DATE
LABEL git-revision=$GIT_REVISION

# Production Image for walrus CLI
FROM debian:bookworm-slim AS walrus-cli

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
FROM debian:bookworm-slim AS walrus-node
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
FROM debian:bookworm-slim AS walrus-deploy
RUN apt-get update && apt-get install -y ca-certificates curl
ARG PROFILE=release
WORKDIR "$WORKDIR/walrus"
# Both bench and release profiles copy from release dir
COPY --from=builder /walrus/target/release/walrus-deploy /opt/walrus/bin/walrus-deploy
COPY --from=builder /contracts /opt/walrus/contracts

ARG BUILD_DATE
ARG GIT_REVISION
LABEL build-date=$BUILD_DATE
LABEL git-revision=$GIT_REVISION

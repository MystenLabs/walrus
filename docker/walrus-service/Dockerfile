# Build application
#
# Copy in all crates, Cargo.toml and Cargo.lock unmodified,
# and build the application.

# This is the same as rust:1.88-bookworm but we use the GCP registry to avoid rate limiting.
FROM us-central1-docker.pkg.dev/cryptic-bolt-398315/base-images/rust:1.88-bookworm-amd64 AS builder

ARG PROFILE=release
ARG GIT_REVISION
ENV GIT_REVISION=$GIT_REVISION
WORKDIR "$WORKDIR/walrus"
RUN apt-get update && apt-get install -y cmake clang

COPY Cargo.toml Cargo.lock ./
COPY crates crates
COPY contracts ./contracts
COPY contracts /contracts

RUN cargo build --profile $PROFILE \
    --bin walrus \
    --bin walrus-node \
    --bin walrus-deploy

# Production Image for all binaries under walrus-service
# This is the same as debian:bookworm-slim but we use the GCP registry to avoid rate limiting.
FROM us-central1-docker.pkg.dev/cryptic-bolt-398315/base-images/debian:bookworm-slim-amd64 AS walrus-service

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
# This is the same as debian:bookworm-slim but we use the GCP registry to avoid rate limiting.
FROM us-central1-docker.pkg.dev/cryptic-bolt-398315/base-images/debian:bookworm-slim-amd64 AS walrus-cli

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
# This is the same as debian:bookworm-slim but we use the GCP registry to avoid rate limiting.
FROM us-central1-docker.pkg.dev/cryptic-bolt-398315/base-images/debian:bookworm-slim-amd64 AS walrus-node
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
# This is the same as debian:bookworm-slim but we use the GCP registry to avoid rate limiting.
FROM us-central1-docker.pkg.dev/cryptic-bolt-398315/base-images/debian:bookworm-slim-amd64 AS walrus-deploy
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

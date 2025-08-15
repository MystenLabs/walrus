# Build application
#
# Copy in all crates, Cargo.toml and Cargo.lock unmodified,
# and build the application.

# This is the same as rust:1.89-bookworm but we use the GCP registry to avoid rate limiting.
FROM us-central1-docker.pkg.dev/cryptic-bolt-398315/base-images/rust:1.89-bookworm-amd64 AS builder

ARG PROFILE=release
ARG GIT_REVISION
ENV GIT_REVISION=$GIT_REVISION
WORKDIR "$WORKDIR/walrus"
RUN apt-get update && apt-get install -y cmake clang

COPY Cargo.toml Cargo.lock ./
COPY crates crates

RUN cargo build --profile $PROFILE --bin walrus-orchestrator

# Production Image for walrus orchestrator
# This is the same as debian:bookworm-slim but we use the GCP registry to avoid rate limiting.
FROM us-central1-docker.pkg.dev/cryptic-bolt-398315/base-images/debian:bookworm-slim-amd64 AS walrus-orchestrator

RUN apt-get update && apt-get install -y ca-certificates curl
ARG PROFILE=release
WORKDIR "$WORKDIR/walrus"
# Both bench and release profiles copy from release dir
COPY --from=builder /walrus/target/release/walrus-orchestrator /opt/walrus/bin/walrus-orchestrator

ARG BUILD_DATE
ARG GIT_REVISION
LABEL build-date=$BUILD_DATE
LABEL git-revision=$GIT_REVISION

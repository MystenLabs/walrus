# Build application
#
# Copy in all crates, Cargo.toml and Cargo.lock unmodified,
# and build the application.

FROM rust:1.92-bookworm AS builder

ARG PROFILE=release
ARG GIT_REVISION
ENV GIT_REVISION=$GIT_REVISION
WORKDIR "$WORKDIR/walrus"
RUN apt-get update && apt-get install -y cmake clang

COPY Cargo.toml Cargo.lock ./
COPY crates crates
COPY setup setup
COPY contracts contracts

RUN cargo build --profile $PROFILE --bin walrus-stress --bin walrus

# Production Image for walrus stress
FROM debian:bookworm-slim AS walrus-stress

RUN apt-get update && apt-get install -y ca-certificates curl
ARG PROFILE=release
WORKDIR "$WORKDIR/walrus"
# Both bench and release profiles copy from release dir
COPY --from=builder /walrus/target/release/walrus-stress /opt/walrus/bin/walrus-stress
COPY --from=builder /walrus/target/release/walrus /opt/walrus/bin/walrus

ARG BUILD_DATE
ARG GIT_REVISION
LABEL build-date=$BUILD_DATE
LABEL git-revision=$GIT_REVISION

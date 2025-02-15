# Build application
#
# Copy in all crates, Cargo.toml and Cargo.lock unmodified,
# and build the application.
FROM rust:1.84-bookworm AS base
ARG PROFILE=release
ARG GIT_REVISION
ENV GIT_REVISION=$GIT_REVISION
WORKDIR "$WORKDIR/walrus"
RUN apt-get update && apt-get install -y cmake clang

COPY Cargo.toml Cargo.lock ./
COPY crates crates
COPY contracts ./contracts
COPY contracts /contracts
COPY docker/walrus-antithesis/build-walrus-image/update_move_toml.sh /tmp/

# Clone Sui repository at specific tag
# TODO(zhewu): make this configurable to use the branch matching the current tag
RUN git clone --depth 1 --branch testnet-v1.41.0 https://github.com/MystenLabs/sui /sui

# We need to replace all the Sui dependencies with the local Sui repository.
# When testing in antithesis, we cannot pull the Sui repository from the internet.
RUN chmod +x /tmp/update_move_toml.sh
RUN /tmp/update_move_toml.sh

FROM base AS builder
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
COPY --from=builder /sui /opt/sui

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
COPY --from=builder /sui /opt/sui

ARG BUILD_DATE
ARG GIT_REVISION
LABEL build-date=$BUILD_DATE
LABEL git-revision=$GIT_REVISION

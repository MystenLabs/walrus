# Multi-stage Dockerfile for building and packaging Walrus components with cargo
# instrumentation for antithesis testing.
# This file creates multiple production images:
# - walrus-service: Complete service with all binaries
# - walrus-cli: CLI tool only
# - walrus-node: Node binary only
# - walrus-deploy: Deployment tool with contracts

# Stage 1: Base Build Environment
# -------------------------------------
FROM rust:1.84-bookworm AS base
ARG PROFILE=release-antithesis
ARG GIT_REVISION
ENV GIT_REVISION=$GIT_REVISION
WORKDIR "$WORKDIR/walrus"

# Install build dependencies
RUN apt-get update && apt-get install -y cmake clang

# Copy project files
COPY Cargo.toml Cargo.lock ./
COPY crates crates
COPY contracts ./contracts
# Note: Duplicate contracts copy can be removed
COPY contracts /contracts
COPY docker/walrus-antithesis/build-walrus-image-for-antithesis/update_move_toml.sh /tmp/

# Clone specific Sui version for offline access
RUN git clone --depth 1 --branch testnet-v1.41.0 https://github.com/MystenLabs/sui /sui

# Update Move.toml to use local Sui repository
RUN chmod +x /tmp/update_move_toml.sh
RUN /tmp/update_move_toml.sh

# Add Antithesis instrumentation
RUN wget -O /usr/lib/libvoidstar.so https://antithesis.com/assets/instrumentation/libvoidstar.so

# Configure Rust flags for Antithesis instrumentation and coverage
ENV RUSTFLAGS="-C target-feature=-crt-static -C codegen-units=1 -C passes=sancov-module \
-C llvm-args=-sanitizer-coverage-level=3 \
-C llvm-args=-sanitizer-coverage-trace-pc-guard \
-C link-args=-Wl,--build-id -Ccodegen-units=1 \
-L/usr/lib/libvoidstar.so -lvoidstar"

# Stage 2: Builder
# -------------------------------------
FROM base AS builder
# Build all binaries with Antithesis instrumentation
RUN LD_LIBRARY_PATH=/usr/lib/libvoidstar.so \
    cargo build --profile $PROFILE \
    --bin walrus \
    --bin walrus-node \
    --bin walrus-deploy

# Stage 3: Production Images
# -------------------------------------

# walrus-service: Complete service image
FROM debian:bookworm-slim AS walrus-service
RUN apt-get update && apt-get install -y ca-certificates curl git
ARG PROFILE=release
WORKDIR "$WORKDIR/walrus"
COPY --from=builder /walrus/target/release/walrus /opt/walrus/bin/walrus
COPY --from=builder /walrus/target/release/walrus-node /opt/walrus/bin/walrus-node
COPY --from=builder /walrus/target/release/walrus-deploy /opt/walrus/bin/walrus-deploy
RUN mkdir -p /symbols
RUN ln -s /opt/walrus/bin/walrus /symbols/walrus
RUN ln -s /opt/walrus/bin/walrus-node /symbols/walrus-node
RUN ln -s /opt/walrus/bin/walrus-deploy /symbols/walrus-deploy
# COPY --from=builder /walrus/target/release/walrus /symbols/walrus
# COPY --from=builder /walrus/target/release/walrus-node /symbols/walrus-node
# COPY --from=builder /walrus/target/release/walrus-deploy /symbols/walrus-deploy
COPY --from=builder /usr/lib/libvoidstar.so /usr/lib/libvoidstar.so
COPY --from=builder /contracts /opt/walrus/contracts
COPY --from=builder /sui /opt/sui
ARG BUILD_DATE
ARG GIT_REVISION
LABEL build-date=$BUILD_DATE
LABEL git-revision=$GIT_REVISION

# walrus-cli: CLI-only image
FROM debian:bookworm-slim AS walrus-cli
RUN apt-get update && apt-get install -y ca-certificates curl
ARG PROFILE=release
WORKDIR "$WORKDIR/walrus"
COPY --from=builder /walrus/target/release/walrus /opt/walrus/bin/walrus
RUN mkdir -p /symbols
RUN ln -s /opt/walrus/bin/walrus /symbols/walrus
# COPY --from=builder /walrus/target/release/walrus /symbols/walrus
COPY --from=builder /usr/lib/libvoidstar.so /usr/lib/libvoidstar.so
ARG BUILD_DATE
ARG GIT_REVISION
LABEL build-date=$BUILD_DATE
LABEL git-revision=$GIT_REVISION

# walrus-node: Node-only image
FROM debian:bookworm-slim AS walrus-node
RUN apt-get update && apt-get install -y ca-certificates curl
ARG PROFILE=release
WORKDIR "$WORKDIR/walrus"
COPY --from=builder /walrus/target/release/walrus-node /opt/walrus/bin/walrus-node
RUN mkdir -p /symbols
RUN ln -s /opt/walrus/bin/walrus-node /symbols/walrus-node
# COPY --from=builder /walrus/target/release/walrus-node /symbols/walrus-node
COPY --from=builder /usr/lib/libvoidstar.so /usr/lib/libvoidstar.so
ARG BUILD_DATE
ARG GIT_REVISION
LABEL build-date=$BUILD_DATE
LABEL git-revision=$GIT_REVISION

# walrus-deploy: Deployment tool image
FROM debian:bookworm-slim AS walrus-deploy
RUN apt-get update && apt-get install -y ca-certificates curl
ARG PROFILE=release
WORKDIR "$WORKDIR/walrus"
COPY --from=builder /walrus/target/release/walrus-deploy /opt/walrus/bin/walrus-deploy
RUN mkdir -p /symbols
RUN ln -s /opt/walrus/bin/walrus-deploy /symbols/walrus-deploy
# COPY --from=builder /walrus/target/release/walrus-deploy /symbols/walrus-deploy
COPY --from=builder /usr/lib/libvoidstar.so /usr/lib/libvoidstar.so
COPY --from=builder /contracts /opt/walrus/contracts
COPY --from=builder /sui /opt/sui
ARG BUILD_DATE
ARG GIT_REVISION
LABEL build-date=$BUILD_DATE
LABEL git-revision=$GIT_REVISION

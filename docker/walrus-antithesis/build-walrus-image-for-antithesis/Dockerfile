# Multi-stage Dockerfile for building and packaging Walrus components with cargo
# instrumentation for antithesis testing.
# This file creates multiple production images:
# - walrus-service: Complete service with all binaries
# - walrus-cli: CLI tool only
# - walrus-node: Node binary only
# - walrus-deploy: Deployment tool with contracts

# Stage 1: Base Build Environment + Builder
# -------------------------------------
FROM rust:1.88-bookworm AS builder
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

COPY docker/walrus-antithesis/sui_version.toml .
RUN export SUI_VERSION=$(grep SUI_VERSION sui_version.toml | cut -d'"' -f2) && \
    echo "SUI_VERSION: $SUI_VERSION" && \
    git clone --depth 1 --branch $SUI_VERSION https://github.com/MystenLabs/sui /sui

# Update Move.toml to use local Sui repository
RUN chmod +x /tmp/update_move_toml.sh
RUN /tmp/update_move_toml.sh

# Add Antithesis instrumentation
RUN wget -O /usr/lib/libvoidstar.so https://antithesis.com/assets/instrumentation/libvoidstar.so

# Configure Rust flags for Antithesis instrumentation and coverage
ARG RUSTFLAGS="-C target-feature=-crt-static -C codegen-units=1 -C passes=sancov-module \
-C llvm-args=-sanitizer-coverage-level=3 \
-C llvm-args=-sanitizer-coverage-trace-pc-guard \
-C link-args=-Wl,--build-id -Ccodegen-units=1 \
-L/usr/lib/libvoidstar.so -lvoidstar"
ENV RUSTFLAGS="$RUSTFLAGS"

# Build all binaries with Antithesis instrumentation
ARG LD_LIBRARY_PATH="/usr/lib/libvoidstar.so"
ENV LD_LIBRARY_PATH="$LD_LIBRARY_PATH"
RUN LD_LIBRARY_PATH=$LD_LIBRARY_PATH \
    cargo build \
    --profile release-antithesis \
    --target-dir=/walrus/target \
    --bin walrus \
    --bin walrus-node \
    --bin walrus-deploy \
    --bin walrus-stress
RUN find /walrus/target

FROM debian:bookworm-slim AS setup

RUN apt-get update && apt-get install -y ca-certificates curl git
WORKDIR "$WORKDIR/walrus"
COPY --from=builder /walrus/target/release-antithesis/walrus /opt/walrus/bin/walrus
COPY --from=builder /walrus/target/release-antithesis/walrus-node /opt/walrus/bin/walrus-node
COPY --from=builder /walrus/target/release-antithesis/walrus-deploy /opt/walrus/bin/walrus-deploy
COPY --from=builder /walrus/target/release-antithesis/walrus-stress /opt/walrus/bin/walrus-stress
RUN mkdir -p /symbols
RUN ln -s /opt/walrus/bin/walrus /symbols/walrus
RUN ln -s /opt/walrus/bin/walrus-node /symbols/walrus-node
RUN ln -s /opt/walrus/bin/walrus-deploy /symbols/walrus-deploy
RUN ln -s /opt/walrus/bin/walrus-stress /symbols/walrus-stress
COPY --from=builder /usr/lib/libvoidstar.so /usr/lib/libvoidstar.so
COPY --from=builder /contracts /opt/walrus/contracts
COPY --from=builder /sui /opt/sui
ARG BUILD_DATE
ARG GIT_REVISION
LABEL build-date=$BUILD_DATE
LABEL git-revision=$GIT_REVISION

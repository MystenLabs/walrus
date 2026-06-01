# Build application
#
# Copy in all crates, Cargo.toml and Cargo.lock unmodified,
# and build the application.
FROM rust:1.96-bookworm AS builder
RUN apt-get update && apt-get install -y --no-install-recommends cmake clang \
    && rm -rf /var/lib/apt/lists/*
ARG PROFILE=release
WORKDIR "/tmp/walrus"

# COPYs ordered least- to most-frequently-changing for deeper layer cache hits.
COPY rust-toolchain.toml ./
COPY Cargo.toml Cargo.lock ./
COPY setup setup
COPY contracts contracts
COPY crates crates

# GIT_REVISION declared late so SHA changes don't invalidate the COPYs above.
ARG GIT_REVISION
ENV GIT_REVISION=$GIT_REVISION
RUN cargo build --locked --profile ${PROFILE} --bin walrus-upload-relay

# Production Image for walrus walrus-upload-relay
FROM debian:bookworm-slim AS walrus-upload-relay
RUN apt-get update && apt-get install -y ca-certificates curl
ARG PROFILE=release
WORKDIR "/tmp/walrus"
COPY --from=builder /tmp/walrus/target/release/walrus-upload-relay /opt/walrus/bin/walrus-upload-relay

ARG BUILD_DATE
ARG GIT_REVISION
LABEL build-date=$BUILD_DATE
LABEL git-revision=$GIT_REVISION
ENV PATH="/opt/walrus/bin:$PATH"
ENTRYPOINT ["/opt/walrus/bin/walrus-upload-relay"]

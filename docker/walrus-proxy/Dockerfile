FROM rust:1.93-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends cmake clang \
 && rm -rf /var/lib/apt/lists/*
ARG PROFILE=release
WORKDIR /work

# COPYs ordered least- to most-frequently-changing for deeper layer cache hits.
COPY rust-toolchain.toml ./
COPY Cargo.toml Cargo.lock ./
COPY setup setup
COPY contracts contracts
COPY crates crates

# GIT_REVISION declared late so SHA changes don't invalidate the COPYs above.
ARG GIT_REVISION
ENV GIT_REVISION=$GIT_REVISION
RUN cargo build --profile ${PROFILE} --bin walrus-proxy

FROM gcr.io/distroless/cc-debian12 as deploy

COPY --from=builder --chmod=755 /work/target/release/walrus-proxy /opt/walrus/bin/walrus-proxy

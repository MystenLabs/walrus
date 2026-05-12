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
# Required: `bin_version!()` previously fell back to `git describe` from
# `.git/`, which we dropped for cache hygiene. Fail loudly if the build arg
# is missing rather than embedding an empty revision string.
ARG GIT_REVISION
RUN test -n "$GIT_REVISION" || \
    (echo "GIT_REVISION build arg is required (e.g. --build-arg GIT_REVISION=\$(git rev-parse HEAD))" >&2 && exit 1)
ENV GIT_REVISION=$GIT_REVISION
RUN cargo build --profile ${PROFILE} --bin walrus-proxy

FROM gcr.io/distroless/cc-debian12 as deploy

COPY --from=builder --chmod=755 /work/target/release/walrus-proxy /opt/walrus/bin/walrus-proxy

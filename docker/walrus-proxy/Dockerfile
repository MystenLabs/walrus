# This is the same as rust:1.90-bookworm but we use the GCP registry to avoid rate limiting.
FROM us-central1-docker.pkg.dev/cryptic-bolt-398315/base-images/rust:1.90-bookworm-amd64 as builder

ARG PROFILE=release
WORKDIR /work

RUN apt-get update && apt-get install -y cmake clang

COPY .git/ .git/
COPY Cargo.toml Cargo.lock ./
COPY crates crates
COPY contracts contracts

RUN cargo build --profile ${PROFILE} --bin walrus-proxy

FROM gcr.io/distroless/cc-debian12 as deploy

COPY --from=builder --chmod=755 /work/target/release/walrus-proxy /opt/walrus/bin/walrus-proxy

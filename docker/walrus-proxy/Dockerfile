FROM rust:1.92-bookworm as builder

ARG PROFILE=release
WORKDIR /work

RUN apt-get update && apt-get install -y cmake clang

COPY .git/ .git/
COPY Cargo.toml Cargo.lock ./
COPY crates crates
COPY contracts contracts
COPY setup setup

RUN cargo build --profile ${PROFILE} --bin walrus-proxy

FROM gcr.io/distroless/cc-debian12 as deploy

COPY --from=builder --chmod=755 /work/target/release/walrus-proxy /opt/walrus/bin/walrus-proxy

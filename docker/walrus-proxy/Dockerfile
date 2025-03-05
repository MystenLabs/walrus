FROM rust:1.84-bookworm as build

ARG PROFILE=release
WORKDIR /work

RUN apt-get update && apt-get install -y cmake clang

COPY .git/ .git/
COPY Cargo.toml Cargo.lock ./
COPY crates crates
COPY contracts contracts

RUN cargo build --profile ${PROFILE} --bin walrus-proxy

FROM gcr.io/distroless/cc-debian12 as deploy

COPY --from=build --chmod=755 /work/target/release/walrus-proxy /opt/walrus/bin/walrus-proxy

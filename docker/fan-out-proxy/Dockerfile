# Build application
#
# Copy in all crates, Cargo.toml and Cargo.lock unmodified,
# and build the application.
FROM rust:1.88-bookworm AS builder
ARG PROFILE=release
ARG GIT_REVISION
ENV GIT_REVISION=$GIT_REVISION
WORKDIR "/tmp/walrus"
RUN apt-get update && apt-get install -y cmake clang

COPY Cargo.toml Cargo.lock ./
COPY crates crates
COPY contracts contracts

RUN cargo build --profile $PROFILE --bin fan-out-proxy

# Production Image for walrus fan-out-proxy
FROM debian:bookworm-slim AS fan-out-proxy
RUN apt-get update && apt-get install -y ca-certificates curl
ARG PROFILE=release
WORKDIR "/tmp/walrus"
COPY --from=builder /tmp/walrus/target/release/fan-out-proxy /opt/walrus/bin/fan-out-proxy

ARG BUILD_DATE
ARG GIT_REVISION
LABEL build-date=$BUILD_DATE
LABEL git-revision=$GIT_REVISION
ENV PATH="/opt/walrus/bin:$PATH"
ENTRYPOINT ["/opt/walrus/bin/fan-out-proxy"]

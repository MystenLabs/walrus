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

RUN cargo build --profile $PROFILE --bin walrus-upload-relay

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
